#include <ydb/core/formats/arrow/program/pattern_hints.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NArrow::NSSA;

namespace {

TString HintText(const TPatternIndexHint& hint) {
    TString operation;
    switch (hint.Operation.GetOperation()) {
        case TIndexCheckOperation::EOperation::StartsWith:
            operation = "StartsWith";
            break;
        case TIndexCheckOperation::EOperation::EndsWith:
            operation = "EndsWith";
            break;
        case TIndexCheckOperation::EOperation::Contains:
            operation = "Contains";
            break;
        default:
            operation = "Other";
            break;
    }
    return TStringBuilder() << operation << ":" << hint.Value << ":" << (hint.Operation.GetCaseSensitive() ? "cs" : "ci");
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
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%aaa%bbb%", true), "Contains:aaa:cs,Contains:bbb:cs");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("aaa%bbb", true), "StartsWith:aaa:cs,EndsWith:bbb:cs");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("aaa%bbb%", true), "StartsWith:aaa:cs,Contains:bbb:cs");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%aaa_bbb%", true), "Contains:aaa:cs,Contains:bbb:cs");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("aaa", true), "StartsWith:aaa:cs");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%ab%cde%", true), "Contains:cde:cs");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%", true), "");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%_%", true), "");
    }

    Y_UNIT_TEST(CaseAndEscape) {
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%Err%", false), "Contains:Err:ci");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%Ошибка%", false), "");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%Ошибка%", true), "Contains:Ошибка:cs");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("%ошибка timeout%", false), "Contains: timeout:ci");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("ErrОшибка", false), "StartsWith:Err:ci");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("ErrОшибка", true), "StartsWith:ErrОшибка:cs");
        TPatternHintPolicy wide;
        wide.MinContainsLength = 8;
        TString wideHints;
        for (const auto& hint : BuildLikeIndexHints("%aaa%bbbbbbbb%", {}, true, 0, wide)) {
            if (!wideHints.empty()) {
                wideHints += ",";
            }
            wideHints += HintText(hint);
        }
        UNIT_ASSERT_VALUES_EQUAL(wideHints, "Contains:bbbbbbbb:cs");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("100#%%", true, "#"), "");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("100#%%", true, "#", true), "StartsWith:100%:cs");
        UNIT_ASSERT_VALUES_EQUAL(JoinHints("abc#", true, "#"), "");
    }
}
