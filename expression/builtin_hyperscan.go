// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// +build hyperscan

/*
 * Hyperscan integration for builtin functions
 *
 * Hyperscan can be use for multi-pattern-match.
 *
 * Function list:
 *   hs_match(input, patterns, [format])
 *   hs_match_json(input, jsonPatterns)
 *   hs_match_all(input, patterns, [format])
 *   hs_match_all_json(input, jsonPatterns)
 *   hs_match_ids(input, patterns, [format])
 *   hs_match_ids_json(input, jsonPatterns)
 *   hs_build_db_json(jsonPatterns, [encodeType])
 *
 * Functions:
 * hs_match(input, patterns, [format])
 *   match any pattern in patterns
 * params:
 *    input: string, required, table column or string for matching
 *    patterns: string, required, patterns format see Patterns Format
 *    format: string, optional, patterns format type see Patterns Format, default is `lines`
 *
 * hs_match_json(input, jsonPatterns)
 *    match any pattern in patterns
 * params:
 *    input: string, required, table column or string for matching
 *    jsonPatterns: string, required, patterns format in json, see Patterns Format
 *
 * hs_match_all(input, patterns, [format])
 *    match all pattern in patterns
 * params:
 *    input: string, required, table column or string for matching
 *    patterns: string, required, patterns format see Patterns Format
 *    format: string, optional, patterns format type see Patterns Format, default is `lines`
 *
 * hs_match_all_json(input, jsonPatterns)
 *    match all pattern in patterns
 * params:
 *    input: string, required, table column or string for matching
 *    jsonPatterns: string, required, patterns format in json, see Patterns Format
 *
 * hs_match_ids(input, patterns, [format])
 *    return matched pattern's id given in patterns, all id is separate by `,`
 * params:
 *    input: string, required, table column or string for matching
 *    patterns: string, required, patterns format see Patterns Format
 *    format: string, optional, patterns format type see Patterns Format, default is `lines`
 *
 * hs_match_ids_json(input, jsonPatterns)
 *    return matched pattern's id given in patterns, all id is separate by `,`
 * params:
 *    input: string, required, table column or string for matching
 *    jsonPatterns: string, required, patterns format in json, see Patterns Format
 *
 * hs_build_db_json(jsonPatterns, [encodeFormat])
 *    build hyperscan database and marshal into encodeFormat
 * params:
 *    jsonPatterns: string, required, patterns format in json, see Patterns Format
 *    encodeFormat: string, optional, marshal data encode format should be `hex` or `base64`, default is `hex`
 *
 * Patterns Format
 *   1. `lines`
 *      line split patterns, example:
 *      ```
 *      pattern1
 *      pattern2
 *      /pattern3/i
 *      ```
 *   2. `json`
 *      json array for pattern, example:
 *      ```
 *      [
 *        {"id": 1, "pattern": "pattern1"},
 *        {"id": 2, "pattern": "pattern2"},
 *        {"id": 3, "pattern": "/pattern3/i"}
 *      ]
 *      ```
 *      `pattern` field is required contain regexp pattern, `id` field can be ignored, if ignored id will assigned as array index plus 1.
 *  3. `hex`
 *     hex encoded marshaled hyperscan database which generated by `hs_build_db_json` function
 *
 *  4. `base64`
 *     base64 encoded marshaled hyperscan database which generated by `hs_build_db_json` function
 *
 * Special for `hs_match_all`
 *   In `hs_match_all` function if patterns format is `hex` or `base64`, function cannot find out how many patterns in database, so `hs_match_all` function only support `lines` and `json` patterns format.
 */

package expression

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"

	hs "github.com/flier/gohs/hyperscan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	// Build DB functions
	_ functionClass = &hsBuildDbJSONFunctionClass{}

	// Match any functions
	_ functionClass = &hsMatchFunctionClass{}
	_ functionClass = &hsMatchJSONFunctionClass{}

	// Match all functions
	_ functionClass = &hsMatchAllFunctionClass{}
	_ functionClass = &hsMatchAllJSONFunctionClass{}

	// Match all ids functions
	_ functionClass = &hsMatchIdsFunctionClass{}
	_ functionClass = &hsMatchIdsJSONFunctionClass{}
)

var (
	_ builtinFunc = &builtinHsMatchSig{}
	_ builtinFunc = &builtinHsBuildDbSig{}
)

var (
	// HSBuildDBJSON is function name of hs_build_db_json
	HSBuildDBJSON = "hs_build_db_json"

	// HSMatch is function name of hs_match
	HSMatch = "hs_match"

	// HSMatchJSON is function name of hs_match_json
	HSMatchJSON = "hs_match_json"

	// HSMatchAll is function name of hs_match_all
	HSMatchAll = "hs_match_all"

	// HSMatchAllJSON is function name of hs_match_all_json
	HSMatchAllJSON = "hs_match_all_json"

	// HSMatchIds is function name of hs_match_ids
	HSMatchIds = "hs_match_ids"

	// HSMatchIdsJSON is function name of hs_match_ids_json
	HSMatchIdsJSON = "hs_match_ids_json"

	errAlreadyMatched                     = fmt.Errorf("Already Matched")
	errInvalidEncodeType                  = fmt.Errorf("Invalid hpyerscan database encode type should be (hex, base64)")
	errInvalidHSSourceType                = fmt.Errorf("Invalid hyperscan database source type should be (lines, json, hex, base64)")
	errInvalidHSSourceTypeWithoutDBSource = fmt.Errorf("Invalid hyperscan database source type should be (lines, json)")
)

const (
	// Build DB functions
	hsScalarFuncSigHsBuildDbJSON tipb.ScalarFuncSig = 4320

	// Match functions
	hsScalarFuncSigHsMatch        tipb.ScalarFuncSig = 4331
	hsScalarFuncSigHsMatchJSON    tipb.ScalarFuncSig = 4332
	hsScalarFuncSigHsMatchAll     tipb.ScalarFuncSig = 4333
	hsScalarFuncSigHsMatchAllJSON tipb.ScalarFuncSig = 4334
	hsScalarFuncSigHsMatchIds     tipb.ScalarFuncSig = 4335
	hsScalarFuncSigHsMatchIdsJSON tipb.ScalarFuncSig = 4336

	hsSourceTypeLines  = 1
	hsSourceTypeJSON   = 2
	hsSourceTypeHex    = 3
	hsSourceTypeBase64 = 4

	hsEncodeTypeHex    = 1
	hsEncodeTypeBase64 = 2

	hsMatchTypeAny = 1
	hsMatchTypeAll = 2
)

type baseBuiltinHsSig struct {
	baseBuiltinFunc
	sourceType      int
	matchType       int
	numPatterns     int
	supportDBSource bool
	db              hs.BlockDatabase
	scratch         *hs.Scratch
}

// Sig classes
// for match series functions
type builtinHsMatchSig struct {
	baseBuiltinHsSig
}

// for build db series functions
type builtinHsBuildDbSig struct {
	baseBuiltinFunc
	sourceType int
	encodeType int
}

// End Sig classes

// Function classes
// hs_build_db_json(source, [encodeFormat]
type hsBuildDbJSONFunctionClass struct {
	baseFunctionClass
}

// Match functions
// hs_match(input, patterns, [format])
type hsMatchFunctionClass struct {
	baseFunctionClass
}

// hs_match_json(input, jsonPatterns)
type hsMatchJSONFunctionClass struct {
	baseFunctionClass
}

// hs_match_ids(input, patterns, [format])
type hsMatchIdsFunctionClass struct {
	baseFunctionClass
}

// hs_match_ids_json(input, jsonPatterns)
type hsMatchIdsJSONFunctionClass struct {
	baseFunctionClass
}

// hs_match_all(input, patterns, [format])
type hsMatchAllFunctionClass struct {
	baseFunctionClass
}

// hs_match_all(input, jsonPatterns)
type hsMatchAllJSONFunctionClass struct {
	baseFunctionClass
}

// End Function classes

// Add hyperscan builtin functions
func init() {
	funcs[HSBuildDBJSON] = &hsBuildDbJSONFunctionClass{baseFunctionClass{HSBuildDBJSON, 1, 2}}
	funcs[HSMatch] = &hsMatchFunctionClass{baseFunctionClass{HSMatch, 2, 3}}
	funcs[HSMatchJSON] = &hsMatchJSONFunctionClass{baseFunctionClass{HSMatchJSON, 2, 2}}
	funcs[HSMatchAll] = &hsMatchAllFunctionClass{baseFunctionClass{HSMatchAll, 2, 3}}
	funcs[HSMatchAllJSON] = &hsMatchAllJSONFunctionClass{baseFunctionClass{HSMatchAllJSON, 2, 2}}
	funcs[HSMatchIds] = &hsMatchIdsFunctionClass{baseFunctionClass{HSMatchIds, 2, 3}}
	funcs[HSMatchIdsJSON] = &hsMatchIdsJSONFunctionClass{baseFunctionClass{HSMatchIdsJSON, 2, 2}}
}

func (c *hsBuildDbJSONFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString}
	if len(args) == 2 {
		argTp = append(argTp, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTp...)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 1
	sig := &builtinHsBuildDbSig{bf, hsSourceTypeJSON, 0}
	sig.setPbCode(hsScalarFuncSigHsBuildDbJSON)
	return sig, nil
}

// Match functions
func (c *hsMatchFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	if len(args) == 3 {
		argTp = append(argTp, types.ETString)
	}
	base, err := newBaseBuiltinHsSig(c.funcName, ctx, args, argTp, types.ETInt, hsSourceTypeLines, hsMatchTypeAny)
	if err != nil {
		return nil, err
	}
	sig := &builtinHsMatchSig{base}
	sig.setPbCode(hsScalarFuncSigHsMatch)
	return sig, nil
}

func (c *hsMatchJSONFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	base, err := newBaseBuiltinHsSig(c.funcName, ctx, args, argTp, types.ETInt, hsSourceTypeJSON, hsMatchTypeAny)
	if err != nil {
		return nil, err
	}
	sig := &builtinHsMatchSig{base}
	sig.setPbCode(hsScalarFuncSigHsMatchJSON)
	return sig, nil
}

func (c *hsMatchIdsFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	if len(args) == 3 {
		argTp = append(argTp, types.ETString)
	}
	base, err := newBaseBuiltinHsSig(c.funcName, ctx, args, argTp, types.ETString, hsSourceTypeLines, hsMatchTypeAll)
	if err != nil {
		return nil, err
	}
	sig := &builtinHsMatchSig{base}
	sig.setPbCode(hsScalarFuncSigHsMatch)
	return sig, nil
}

func (c *hsMatchIdsJSONFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	base, err := newBaseBuiltinHsSig(c.funcName, ctx, args, argTp, types.ETString, hsSourceTypeJSON, hsMatchTypeAll)
	if err != nil {
		return nil, err
	}
	sig := &builtinHsMatchSig{base}
	sig.setPbCode(hsScalarFuncSigHsMatchJSON)
	return sig, nil
}

func (c *hsMatchAllFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	if len(args) == 3 {
		argTp = append(argTp, types.ETString)
	}
	base, err := newBaseBuiltinHsSig(c.funcName, ctx, args, argTp, types.ETInt, hsSourceTypeLines, hsMatchTypeAll)
	if err != nil {
		return nil, err
	}
	base.supportDBSource = false
	sig := &builtinHsMatchSig{base}
	sig.setPbCode(hsScalarFuncSigHsMatchAll)
	return sig, nil
}

func (c *hsMatchAllJSONFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	base, err := newBaseBuiltinHsSig(c.funcName, ctx, args, argTp, types.ETInt, hsSourceTypeJSON, hsMatchTypeAll)
	if err != nil {
		return nil, err
	}
	sig := &builtinHsMatchSig{base}
	sig.setPbCode(hsScalarFuncSigHsMatchAllJSON)
	return sig, nil
}

func newBaseBuiltinHsSig(name string, ctx sessionctx.Context, args []Expression, argType []types.EvalType, retType types.EvalType, sourceType, matchType int) (baseBuiltinHsSig, error) {
	bf, err := newBaseBuiltinFuncWithTp(ctx, name, args, retType, argType...)
	if err != nil {
		return baseBuiltinHsSig{}, err
	}
	bf.tp.Flen = 1
	return baseBuiltinHsSig{bf, sourceType, matchType, 0, true, nil, nil}, nil
}

// initScratch must call after Hyperscan database is created.
// And all expression function will not execute parallel, so
// no need to create a pool or lock to make call `Scan` function
// serialize.
func (b *baseBuiltinHsSig) initScratch() error {
	if b.scratch != nil {
		return nil
	}
	scratch, err := hs.NewScratch(b.db)
	if err != nil {
		return err
	}
	b.scratch = scratch
	runtime.SetFinalizer(scratch, func(hsScratch *hs.Scratch) {
		hsScratch.Free()
	})
	return nil
}

func (b *baseBuiltinHsSig) cloneDb() hs.BlockDatabase {
	var (
		ret  hs.BlockDatabase
		err  error
		data []byte
	)
	data, err = b.db.Marshal()
	if err != nil {
		return nil
	}
	ret, err = hs.UnmarshalBlockDatabase(data)
	if err != nil {
		return nil
	}

	// expression function cannot know when query finished, so call SetFinalizer is
	// the only way to free CGO allocated memory.
	runtime.SetFinalizer(ret, func(hsdb hs.BlockDatabase) {
		hsdb.Close()
	})
	return ret
}

func (b *baseBuiltinHsSig) hsMatch(val string) bool {
	switch b.matchType {
	case hsMatchTypeAll:
		return b.hsMatchAll(val)
	case hsMatchTypeAny:
		_, ret := b.hsMatchAny(val)
		return ret
	}
	return false
}

func (b *baseBuiltinHsSig) hsMatchAll(val string) bool {
	matchCount := 0
	if b.db == nil {
		return false
	}
	handler := func(id uint, from, to uint64, flags uint, context interface{}) error {
		matchCount++
		return nil
	}
	err := b.db.Scan([]byte(val), b.scratch, handler, nil)
	if err != nil && err.(hs.HsError) != hs.ErrScanTerminated {
		return false
	}
	return matchCount >= b.numPatterns
}

func (b *baseBuiltinHsSig) hsMatchAny(val string) (int, bool) {
	matched := false
	matchedID := 0
	if b.db == nil {
		return matchedID, matched
	}
	handler := func(id uint, from, to uint64, flags uint, context interface{}) error {
		if !matched {
			matched = true
			matchedID = int(id)
			return errAlreadyMatched
		}
		return nil
	}
	err := b.db.Scan([]byte(val), b.scratch, handler, nil)
	if err != nil && err.(hs.HsError) != hs.ErrScanTerminated {
		return 0, false
	}
	return matchedID, matched
}

func (b *baseBuiltinHsSig) hsMatchIds(val string) []string {
	matched := false
	matchedIds := make([]string, 0)
	if b.db == nil {
		return matchedIds
	}
	handler := func(id uint, from, to uint64, flags uint, context interface{}) error {
		matchedIds = append(matchedIds, fmt.Sprintf("%d", id))
		matched = true
		// If match any just return the first match pattern ID
		if matched && b.matchType == hsMatchTypeAny {
			return errAlreadyMatched
		}
		return nil
	}
	err := b.db.Scan([]byte(val), b.scratch, handler, nil)
	if err != nil && err.(hs.HsError) != hs.ErrScanTerminated {
		return nil
	}
	return matchedIds
}

func buildHsBlockDB(patterns []*hs.Pattern) (hs.BlockDatabase, int, error) {
	if len(patterns) == 0 {
		return nil, 0, nil
	}
	builder := hs.DatabaseBuilder{
		Patterns: patterns,
		Mode:     hs.BlockMode,
		Platform: hs.PopulatePlatform(),
	}
	db, err := builder.Build()
	if err != nil {
		return nil, 0, err
	}

	// expression function cannot know when query finished, so call SetFinalizer is
	// the only way to free CGO allocated memory.
	runtime.SetFinalizer(db.(hs.BlockDatabase), func(hsdb hs.BlockDatabase) {
		hsdb.Close()
	})
	return db.(hs.BlockDatabase), len(patterns), err
}

type hsPatternObj struct {
	ID      int    `json:"id,omitempty"`
	Pattern string `json:"pattern"`
}

func buildBlockDBFromJSON(patternsJSON string) (hs.BlockDatabase, int, error) {
	patterns := make([]hsPatternObj, 0)
	err := json.Unmarshal([]byte(patternsJSON), &patterns)
	if err != nil {
		return nil, 0, err
	}

	pats := make([]*hs.Pattern, 0, len(patterns))
	pid := 1
	for _, reg := range patterns {
		if reg.Pattern == "" {
			continue
		}
		pat, err := hs.ParsePattern(reg.Pattern)
		if err != nil {
			return nil, 0, err
		}
		pat.Id = reg.ID
		if reg.ID == 0 {
			pat.Id = pid
		}
		pats = append(pats, pat)
		pid++
	}
	return buildHsBlockDB(pats)
}

func buildBlockDBFromLines(patterns string) (hs.BlockDatabase, int, error) {
	lines := strings.Split(patterns, "\n")
	pats := make([]*hs.Pattern, 0, len(lines))
	pid := 1
	for _, reg := range lines {
		if reg == "" {
			continue
		}
		pat, err := hs.ParsePattern(reg)
		if err != nil {
			return nil, 0, err
		}
		pat.Id = pid
		pats = append(pats, pat)
		pid++
	}
	return buildHsBlockDB(pats)
}

func buildBlockDBFromHex(hexData string) (hs.BlockDatabase, int, error) {
	data, err := hex.DecodeString(hexData)
	if err != nil {
		return nil, 0, err
	}
	db, err := hs.UnmarshalBlockDatabase(data)
	return db, 0, err
}

func buildBlockDBFromBase64(base64Data string) (hs.BlockDatabase, int, error) {
	data, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		return nil, 0, err
	}
	db, err := hs.UnmarshalBlockDatabase(data)
	return db, 0, err
}

func buildBlockDB(source string, sourceTp int) (hs.BlockDatabase, int, error) {
	switch sourceTp {
	case hsSourceTypeJSON:
		return buildBlockDBFromJSON(source)
	case hsSourceTypeHex:
		return buildBlockDBFromHex(source)
	case hsSourceTypeBase64:
		return buildBlockDBFromBase64(source)
	default:
		return buildBlockDBFromLines(source)
	}
}

func (b *baseBuiltinHsSig) buildBlockDB(source string) (hs.BlockDatabase, error) {
	db, num, err := buildBlockDB(source, b.sourceType)
	b.numPatterns = num
	return db, err
}

func (b *baseBuiltinHsSig) updateSourceType(row chunk.Row) (bool, error) {
	if len(b.args) == 3 {
		sourceType, isNull, err := b.args[2].EvalString(b.ctx, row)
		if isNull || err != nil {
			return isNull, err
		}
		switch strings.ToLower(sourceType) {
		case "base64":
			if !b.supportDBSource {
				return false, errInvalidHSSourceTypeWithoutDBSource
			}
			b.sourceType = hsSourceTypeBase64
		case "hex":
			if !b.supportDBSource {
				return false, errInvalidHSSourceTypeWithoutDBSource
			}
			b.sourceType = hsSourceTypeHex
		case "json":
			b.sourceType = hsSourceTypeJSON
		case "lines":
			b.sourceType = hsSourceTypeLines
		default:
			return false, errInvalidHSSourceType
		}
	}
	return false, nil
}

func (b *baseBuiltinHsSig) evalInt(row chunk.Row) (int64, bool, error) {
	valStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	patternSource, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	isNull, err = b.updateSourceType(row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if b.db == nil {
		db, err := b.buildBlockDB(patternSource)
		if err != nil {
			return 0, true, ErrRegexp.GenWithStackByArgs(err.Error())
		}
		b.db = db
	}

	if err := b.initScratch(); err != nil {
		return 0, false, err
	}

	matched := b.hsMatch(valStr)
	return boolToInt64(matched), false, nil
}

func (b *builtinHsMatchSig) evalString(row chunk.Row) (string, bool, error) {
	valStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	patternSource, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	isNull, err = b.updateSourceType(row)
	if isNull || err != nil {
		return "", isNull, err
	}

	if b.db == nil {
		db, err := b.buildBlockDB(patternSource)
		if err != nil {
			return "", true, ErrRegexp.GenWithStackByArgs(err.Error())
		}
		b.db = db
	}

	if err := b.initScratch(); err != nil {
		return "", false, err
	}

	matchedIds := b.hsMatchIds(valStr)
	if len(matchedIds) == 0 {
		return "", false, nil
	}
	return strings.Join(matchedIds, ","), false, nil
}

func (b *baseBuiltinHsSig) cloneFromBaseHsSig(source *baseBuiltinHsSig) {
	b.sourceType = source.sourceType
	b.matchType = source.matchType
	b.numPatterns = source.numPatterns
	b.scratch = nil
	if source.db != nil {
		b.db = b.cloneDb()
	}
}

func (b *builtinHsMatchSig) Clone() builtinFunc {
	newSig := &builtinHsMatchSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.cloneFromBaseHsSig(&b.baseBuiltinHsSig)
	return newSig
}

func (b *builtinHsBuildDbSig) Clone() builtinFunc {
	newSig := &builtinHsBuildDbSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.sourceType = b.sourceType
	newSig.encodeType = b.encodeType
	return newSig
}

func (b *builtinHsBuildDbSig) evalString(row chunk.Row) (string, bool, error) {
	patternSource, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	encTp := "hex"
	if len(b.args) == 2 {
		encodeType, isNull, err := b.args[1].EvalString(b.ctx, row)
		if err != nil {
			return "", isNull, err
		}

		encTp = strings.ToLower(encodeType)
	}

	switch encTp {
	case "hex":
		b.encodeType = hsEncodeTypeHex
	case "base64":
		b.encodeType = hsEncodeTypeBase64
	default:
		return "", false, errInvalidEncodeType
	}

	db, err := b.buildBlockDB(patternSource)
	if err != nil {
		return "", true, ErrRegexp.GenWithStackByArgs(err.Error())
	}
	data, err := db.Marshal()
	if err != nil {
		return "", true, ErrRegexp.GenWithStackByArgs(err.Error())
	}
	switch b.encodeType {
	case hsEncodeTypeBase64:
		return base64.StdEncoding.EncodeToString(data), false, nil
	default:
		return hex.EncodeToString(data), false, nil
	}
}

func (b *builtinHsBuildDbSig) buildBlockDB(source string) (hs.BlockDatabase, error) {
	db, _, err := buildBlockDB(source, b.sourceType)
	return db, err
}
