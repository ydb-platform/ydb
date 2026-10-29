#include <ydb/core/formats/arrow/program/pattern_hints.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

using namespace NKikimr::NArrow::NSSA;

namespace {

TString HintText(const TPatternIndexHint& hint) {
    const auto* caseMode = hint.Operation.GetCaseSensitive() ? "case_sensitive" : "case_insensitive";
    return TStringBuilder() << ::ToString(hint.Operation.GetOperation()) << ":" << hint.Value << ":" << caseMode;
}

TString JoinHints(const TStringBuf pattern, const bool caseSensitive, const TStringBuf escape = {}, const bool allowEscape = false) {
    TPatternHintPolicy policy;
    policy.AllowEscape = allowEscape;
    TString result;
    for (const auto& hint : BuildLikeIndexHints(pattern, escape, caseSensitive, 0, policy)) {
        if (!result.empty()) {
            result += ",";
        }
        result += HintText(hint);
    }
    return result;
}

}   // namespace

Y_UNIT_TEST_SUITE(PatternHints) {
    Y_UNIT_TEST(LiteralRuns) {
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%aaa%bbb%", true), "Contains:aaa:case_sensitive,Contains:bbb:case_sensitive");
        // Suffixes are Contains. EndsWith does not match a larger ngram's stored suffix window.
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("aaa%bbb", true), "StartsWith:aaa:case_sensitive,Contains:bbb:case_sensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("aaa%bbbbb", true), "StartsWith:aaa:case_sensitive,Contains:bbbbb:case_sensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("aaa%bbb%", true), "StartsWith:aaa:case_sensitive,Contains:bbb:case_sensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%aaa_bbb%", true), "Contains:aaa:case_sensitive,Contains:bbb:case_sensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("aaa", true), "StartsWith:aaa:case_sensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%ab%cde%", true), "Contains:cde:case_sensitive");
        // A one-byte suffix does not match the stored suffix window, so it is not a probe.
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%aaa%b", true), "Contains:aaa:case_sensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%b", true), "");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("aaa%bb", true), "StartsWith:aaa:case_sensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%aaa%aaa%", true), "Contains:aaa:case_sensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%", true), "");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%_%", true), "");
    }

    Y_UNIT_TEST(CaseAndEscape) {
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%Err%", false), "Contains:Err:case_insensitive");
        // ILIKE drops k/K/s/S: RE2 also matches U+212A and U+017F, and the bloom folds ASCII only.
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%kkk%bbb%", false), "Contains:bbb:case_insensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%kkk%", false), "");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%sss%", false), "");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("xxxk%", false), "StartsWith:xxx:case_insensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%kxxx", false), "Contains:xxx:case_insensitive");
        // The piece left of k is not anchored, and two bytes are below the contains minimum.
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%xxk", false), "");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%xxxk", false), "Contains:xxx:case_insensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%kkk%bbb%", true), "Contains:kkk:case_sensitive,Contains:bbb:case_sensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%Ошибка%", false), "");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%Ошибка%", true), "Contains:Ошибка:case_sensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%ошибка timeout%", false), "Contains: timeout:case_insensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("ErrОшибка", false), "StartsWith:Err:case_insensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("ErrОшибка", true), "StartsWith:ErrОшибка:case_sensitive");
        TPatternHintPolicy wide;
        wide.MinContainsLength = 8;
        TString wideHints;
        for (const auto& hint : BuildLikeIndexHints("%aaa%bbbbbbbb%", {}, true, 0, wide)) {
            if (!wideHints.empty()) {
                wideHints += ",";
            }
            wideHints += HintText(hint);
        }
        UNIT_ASSERT_VALUES_EQUAL(wideHints, "Contains:bbbbbbbb:case_sensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("100#%%", true, "#"), "");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("100#%%", true, "#", true), "StartsWith:100%:case_sensitive");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("abc#", true, "#"), "");
    }
}
