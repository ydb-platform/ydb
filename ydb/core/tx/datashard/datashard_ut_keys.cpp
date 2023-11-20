#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/scheme/scheme_tabledefs.h>


namespace NKikimr {

using namespace NKikimr::NDataShard;

Y_UNIT_TEST_SUITE(TxKeys) {

static TString ProgSelectRow(TString key) {
    return Sprintf(R"((
        (let select_ (SelectRow 'table2 '(%s) '('value)))
        (return (AsList (SetResult 'Result select_)))
    ))", key.data());
}

static TString ProgSelectRange(TString key) {
    return Sprintf(R"((
        (let select_ (SelectRange 'table2 '(%s) '('value) '()))
        (return (AsList (SetResult 'Result select_)))
    ))", key.data());
}

static void ComparePointsStr(TTester& t, TString xKey, TString yKey, int cmpExpected) {
    TKeyExtractor xExtr(t, ProgSelectRow(xKey));
    TKeyExtractor yExtr(t, ProgSelectRow(yKey));

    auto& xKeys = xExtr.GetKeys();
    auto& yKeys = yExtr.GetKeys();

    UNIT_ASSERT_EQUAL(xKeys.size(), 1);
    UNIT_ASSERT_EQUAL(yKeys.size(), 1);

    int cmpActual = ComparePointKeys(*xKeys[0], *yKeys[0]);

    UNIT_ASSERT((cmpActual < 0 && cmpExpected < 0) ||
                (cmpActual == 0 && cmpExpected == 0) ||
                (cmpActual > 0 && cmpExpected > 0));
}

static void ComparePointAndRangeStr(TTester& t, TString pointKey, TString rangeKey, int cmpExpected) {
    TKeyExtractor pointExtr(t, ProgSelectRow(pointKey));
    TKeyExtractor rangeExtr(t, ProgSelectRange(rangeKey));

    auto& pointKeys = pointExtr.GetKeys();
    auto& rangeKeys = rangeExtr.GetKeys();

    UNIT_ASSERT_EQUAL(pointKeys.size(), 1);
    UNIT_ASSERT_EQUAL(rangeKeys.size(), 1);

    int cmpActual = ComparePointAndRangeKeys(*pointKeys[0], *rangeKeys[0]);

    UNIT_ASSERT((cmpActual < 0 && cmpExpected < 0) ||
                (cmpActual == 0 && cmpExpected == 0) ||
                (cmpActual > 0 && cmpExpected > 0));
}

static TString ToPointKey(std::pair<ui32, TString> x) {
    const char * keyPattern = "'('key1 (Uint32 '%u)) '('key2 (Utf8 '@@%s@@))";
    return Sprintf(keyPattern, x.first, x.second.data());
}

static TString ToRangeKey(std::pair<ui32, TString> begin, std::pair<ui32, TString> end, TString includes) {
    const char * keyPattern = "%s '('key1 (Uint32 '%u) (Uint32 '%u)) '('key2 (Utf8 '@@%s@@) (Utf8 '@@%s@@))";
    return Sprintf(keyPattern, includes.data(), begin.first, end.first, begin.second.data(), end.second.data());
}

static void ComparePoints(TTester& t, std::pair<ui32, TString> x, std::pair<ui32, TString> y, int cmpExpected) {
    ComparePointsStr(t, ToPointKey(x), ToPointKey(y), cmpExpected);
}

static void ComparePointAndRange(TTester& t,
                                 std::pair<ui32, TString> point, std::pair<ui32, TString> rBegin,
                                 std::pair<ui32, TString> rEnd, TString includes, int cmpExpected) {
    ComparePointAndRangeStr(t, ToPointKey(point), ToRangeKey(rBegin, rEnd, includes), cmpExpected);
}

static void ComparePointAndRange(TTester& t, std::pair<ui32, TString> point, TString rangeKey, int cmpExpected) {
    ComparePointAndRangeStr(t, ToPointKey(point), rangeKey, cmpExpected);
}

//

Y_UNIT_TEST(ComparePointKeys) {
    TTester t(TTester::ESchema_DoubleKV, TTester::TOptions());

    ComparePoints(t, {0, ""}, {0, ""}, 0);
    ComparePoints(t, {0, "a"}, {0, "a"}, 0);
    ComparePoints(t, {Max<ui32>(), ""}, {Max<ui32>(), ""}, 0);

    ComparePoints(t, {0, "a"}, {1, "a"}, -1);
    ComparePoints(t, {0, "a"}, {Max<ui32>(), "a"}, -1);
    ComparePoints(t, {1, "a"}, {Max<ui32>(), "a"}, -1);
    ComparePoints(t, {Max<ui32>()-1, "a"}, {Max<ui32>(), "a"}, -1);

    ComparePoints(t, {1, "a"}, {0, "a"}, 1);
    ComparePoints(t, {Max<ui32>(), "a"}, {0, "a"}, 1);
    ComparePoints(t, {Max<ui32>(), "a"}, {1, "a"}, 1);
    ComparePoints(t, {Max<ui32>(), "a"}, {Max<ui32>()-1, "a"}, 1);

    ComparePoints(t, {0, "a"}, {0, "b"}, -1);
    ComparePoints(t, {0, "a"}, {0, ""}, 1);
    ComparePoints(t, {1, "a"}, {0, "b"}, 1);
    ComparePoints(t, {0, "a"}, {0, "aa"}, -1);
    ComparePoints(t, {0, "a"}, {0, "ba"}, -1);
    ComparePoints(t, {1, "a"}, {0, "ba"}, 1);
}

Y_UNIT_TEST(ComparePointKeysWithNull) {
    TTester t(TTester::ESchema_DoubleKV, TTester::TOptions());

    const char * nullNull = "'('key1 (Null)) '('key2 (Null))";
    const char * null = "'('key1 (Uint32 '%u)) '('key2 (Null))";

    ComparePointsStr(t, nullNull, nullNull, 0);
    ComparePointsStr(t, Sprintf(null, 0), Sprintf(null, 0), 0);

    ComparePointsStr(t, Sprintf(null, 0), Sprintf(null, 1), -1);
    ComparePointsStr(t, Sprintf(null, 1), Sprintf(null, 0), 1);

    ComparePointsStr(t, nullNull, Sprintf(null, 0), -1);
    ComparePointsStr(t, Sprintf(null, 0), nullNull, 1);

    ComparePointsStr(t, nullNull, ToPointKey({0, ""}), -1);
    ComparePointsStr(t, ToPointKey({0, ""}), nullNull, 1);

    ComparePointsStr(t, Sprintf(null, 0), ToPointKey({0, ""}), -1);
    ComparePointsStr(t, ToPointKey({0, ""}), Sprintf(null, 0), 1);
}

Y_UNIT_TEST(ComparePointAndRange) {
    TTester t(TTester::ESchema_DoubleKV, TTester::TOptions());

    ComparePointAndRange(t, {0, ""}, {0, ""}, {0, ""}, "'IncFrom 'IncTo", 0);
    ComparePointAndRange(t, {0, "a"}, {0, "a"}, {0, "a"}, "'IncFrom 'IncTo", 0);

    ComparePointAndRange(t, {0, ""}, {0, ""}, {1, ""}, "'IncFrom 'IncTo", 0);
    ComparePointAndRange(t, {0, ""}, {0, ""}, {1, ""}, "'IncFrom 'ExcTo", 0);
    ComparePointAndRange(t, {0, ""}, {0, ""}, {1, ""}, "'ExcFrom 'IncTo", -1);
    ComparePointAndRange(t, {0, ""}, {0, ""}, {1, ""}, "'ExcFrom 'ExcTo", -1);

    ComparePointAndRange(t, {1, ""}, {0, ""}, {1, ""}, "'IncFrom 'IncTo", 0);
    ComparePointAndRange(t, {1, ""}, {0, ""}, {1, ""}, "'IncFrom 'ExcTo", 1);
    ComparePointAndRange(t, {1, ""}, {0, ""}, {1, ""}, "'ExcFrom 'IncTo", 0);
    ComparePointAndRange(t, {1, ""}, {0, ""}, {1, ""}, "'ExcFrom 'ExcTo", 1);

    ComparePointAndRange(t, {1, "a"}, {0, "b"}, {2, ""}, "'IncFrom 'IncTo", 0);
    ComparePointAndRange(t, {1, "a"}, {0, "b"}, {2, ""}, "'IncFrom 'ExcTo", 0);
    ComparePointAndRange(t, {1, "a"}, {0, "b"}, {2, ""}, "'ExcFrom 'IncTo", 0);
    ComparePointAndRange(t, {1, "a"}, {0, "b"}, {2, ""}, "'ExcFrom 'ExcTo", 0);

    ComparePointAndRange(t, {0, "a"}, {1, ""}, {2, ""}, "'IncFrom 'IncTo", -1);
    ComparePointAndRange(t, {0, "a"}, {1, ""}, {2, ""}, "'IncFrom 'ExcTo", -1);
    ComparePointAndRange(t, {0, "a"}, {1, ""}, {2, ""}, "'ExcFrom 'IncTo", -1);
    ComparePointAndRange(t, {0, "a"}, {1, ""}, {2, ""}, "'ExcFrom 'ExcTo", -1);

    ComparePointAndRange(t, {2, ""}, {0, ""}, {1, "a"}, "'IncFrom 'IncTo", 1);
    ComparePointAndRange(t, {2, ""}, {0, ""}, {1, "a"}, "'IncFrom 'ExcTo", 1);
    ComparePointAndRange(t, {2, ""}, {0, ""}, {1, "a"}, "'ExcFrom 'IncTo", 1);
    ComparePointAndRange(t, {2, ""}, {0, ""}, {1, "a"}, "'ExcFrom 'ExcTo", 1);

    ComparePointAndRange(t, {Max<ui32>()-1, ""}, {Max<ui32>()-1, ""}, {Max<ui32>(), ""}, "'IncFrom 'IncTo", 0);
    ComparePointAndRange(t, {Max<ui32>()-1, ""}, {Max<ui32>()-1, ""}, {Max<ui32>(), ""}, "'IncFrom 'ExcTo", 0);
    ComparePointAndRange(t, {Max<ui32>()-1, ""}, {Max<ui32>()-1, ""}, {Max<ui32>(), ""}, "'ExcFrom 'IncTo", -1);
    ComparePointAndRange(t, {Max<ui32>()-1, ""}, {Max<ui32>()-1, ""}, {Max<ui32>(), ""}, "'ExcFrom 'ExcTo", -1);

    ComparePointAndRange(t, {Max<ui32>(), ""}, {Max<ui32>()-1, ""}, {Max<ui32>(), ""}, "'IncFrom 'IncTo", 0);
    ComparePointAndRange(t, {Max<ui32>(), ""}, {Max<ui32>()-1, ""}, {Max<ui32>(), ""}, "'IncFrom 'ExcTo", 1);
    ComparePointAndRange(t, {Max<ui32>(), ""}, {Max<ui32>()-1, ""}, {Max<ui32>(), ""}, "'ExcFrom 'IncTo", 0);
    ComparePointAndRange(t, {Max<ui32>(), ""}, {Max<ui32>()-1, ""}, {Max<ui32>(), ""}, "'ExcFrom 'ExcTo", 1);

    ComparePointAndRange(t, {0, "a"}, {0, "a"}, {0, "aa"}, "'IncFrom 'IncTo", 0);
    ComparePointAndRange(t, {0, "a"}, {0, "a"}, {0, "aa"}, "'IncFrom 'ExcTo", 0);
    ComparePointAndRange(t, {0, "a"}, {0, "a"}, {0, "aa"}, "'ExcFrom 'IncTo", -1);
    ComparePointAndRange(t, {0, "a"}, {0, "a"}, {0, "aa"}, "'ExcFrom 'ExcTo", -1);

    ComparePointAndRange(t, {0, "aa"}, {0, "a"}, {0, "aa"}, "'IncFrom 'IncTo", 0);
    ComparePointAndRange(t, {0, "aa"}, {0, "a"}, {0, "aa"}, "'IncFrom 'ExcTo", 1);
    ComparePointAndRange(t, {0, "aa"}, {0, "a"}, {0, "aa"}, "'ExcFrom 'IncTo", 0);
    ComparePointAndRange(t, {0, "aa"}, {0, "a"}, {0, "aa"}, "'ExcFrom 'ExcTo", 1);

    ComparePointAndRange(t, {0, "aa"}, {0, "aa"}, {0, "ab"}, "'IncFrom 'IncTo", 0);
    ComparePointAndRange(t, {0, "aa"}, {0, "aa"}, {0, "ab"}, "'IncFrom 'ExcTo", 0);
    ComparePointAndRange(t, {0, "aa"}, {0, "aa"}, {0, "ab"}, "'ExcFrom 'IncTo", -1);
    ComparePointAndRange(t, {0, "aa"}, {0, "aa"}, {0, "ab"}, "'ExcFrom 'ExcTo", -1);

    ComparePointAndRange(t, {0, "ab"}, {0, "aa"}, {0, "ab"}, "'IncFrom 'IncTo", 0);
    ComparePointAndRange(t, {0, "ab"}, {0, "aa"}, {0, "ab"}, "'IncFrom 'ExcTo", 1);
    ComparePointAndRange(t, {0, "ab"}, {0, "aa"}, {0, "ab"}, "'ExcFrom 'IncTo", 0);
    ComparePointAndRange(t, {0, "ab"}, {0, "aa"}, {0, "ab"}, "'ExcFrom 'ExcTo", 1);

    ComparePointAndRange(t, {0, "a"}, {0, "b"}, {0, "c"}, "'IncFrom 'IncTo", -1);
    ComparePointAndRange(t, {0, "a"}, {0, "b"}, {0, "c"}, "'IncFrom 'ExcTo", -1);
    ComparePointAndRange(t, {0, "a"}, {0, "b"}, {0, "c"}, "'ExcFrom 'IncTo", -1);
    ComparePointAndRange(t, {0, "a"}, {0, "b"}, {0, "c"}, "'ExcFrom 'ExcTo", -1);

    ComparePointAndRange(t, {0, "b"}, {0, "a"}, {0, "c"}, "'IncFrom 'IncTo", 0);
    ComparePointAndRange(t, {0, "b"}, {0, "a"}, {0, "c"}, "'IncFrom 'ExcTo", 0);
    ComparePointAndRange(t, {0, "b"}, {0, "a"}, {0, "c"}, "'ExcFrom 'IncTo", 0);
    ComparePointAndRange(t, {0, "b"}, {0, "a"}, {0, "c"}, "'ExcFrom 'ExcTo", 0);

    ComparePointAndRange(t, {0, "c"}, {0, "a"}, {0, "b"}, "'IncFrom 'IncTo", 1);
    ComparePointAndRange(t, {0, "c"}, {0, "a"}, {0, "b"}, "'IncFrom 'ExcTo", 1);
    ComparePointAndRange(t, {0, "c"}, {0, "a"}, {0, "b"}, "'ExcFrom 'IncTo", 1);
    ComparePointAndRange(t, {0, "c"}, {0, "a"}, {0, "b"}, "'ExcFrom 'ExcTo", 1);
}

Y_UNIT_TEST(ComparePointAndRangeWithNull) {
    TTester t(TTester::ESchema_DoubleKV, TTester::TOptions());

    const char * nullNull = "'('key1 (Null)) '('key2 (Null))";
    const char * null = "'('key1 (Uint32 '%u)) '('key2 (Null))";

    ComparePointAndRangeStr(t, nullNull, "'IncFrom 'IncTo '('key1 (Uint32 '0) (Uint32 '0)) '('key2 (Null) (Null))", -1);
    ComparePointAndRangeStr(t, nullNull, "'IncFrom 'IncTo '('key1 (Null) (Uint32 '0)) '('key2 (Null) (Null))", 0);
    ComparePointAndRangeStr(t, nullNull, "'ExcFrom 'IncTo '('key1 (Null) (Uint32 '0)) '('key2 (Null) (Null))", -1);
    ComparePointAndRangeStr(t, nullNull, "'IncFrom 'IncTo '('key1 (Null) (Null)) '('key2 (Null) (Utf8 'a))", 0);
    ComparePointAndRangeStr(t, nullNull, "'ExcFrom 'IncTo '('key1 (Null) (Null)) '('key2 (Null) (Utf8 'a))", -1);

    ComparePointAndRangeStr(t, Sprintf(null, 0), "'IncFrom 'IncTo '('key1 (Uint32 '0) (Uint32 '0)) '('key2 (Null) (Null))", 0);
    ComparePointAndRangeStr(t, Sprintf(null, 1), "'IncFrom 'IncTo '('key1 (Uint32 '0) (Uint32 '0)) '('key2 (Null) (Null))", 1);
    ComparePointAndRangeStr(t, Sprintf(null, 0), "'IncFrom 'IncTo '('key1 (Uint32 '1) (Uint32 '1)) '('key2 (Null) (Null))", -1);

    ComparePointAndRangeStr(t, Sprintf(null, 1), "'IncFrom 'IncTo '('key1 (Uint32 '0) (Uint32 '1)) '('key2 (Null) (Null))", 0);
    ComparePointAndRangeStr(t, Sprintf(null, 1), "'IncFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '1)) '('key2 (Null) (Null))", 1);
    ComparePointAndRangeStr(t, Sprintf(null, 0), "'ExcFrom 'IncTo '('key1 (Uint32 '0) (Uint32 '1)) '('key2 (Null) (Null))", -1);
    ComparePointAndRangeStr(t, Sprintf(null, 1), "'ExcFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '2)) '('key2 (Null) (Null))", 0);

    ComparePointAndRangeStr(t, Sprintf(null, 0), "'IncFrom 'IncTo '('key1 (Null) (Uint32 '0)) '('key2 (Null) (Null))", 0);
    ComparePointAndRangeStr(t, Sprintf(null, 0), "'IncFrom 'ExcTo '('key1 (Null) (Uint32 '0)) '('key2 (Null) (Null))", 1);
    ComparePointAndRangeStr(t, Sprintf(null, 0), "'ExcFrom 'ExcTo '('key1 (Null) (Uint32 '0)) '('key2 (Null) (Utf8 'a))", 0);
}

Y_UNIT_TEST(ComparePointAndRangeWithInf) {
    TTester t(TTester::ESchema_DoubleKV, TTester::TOptions());

    const char * nullNull = "'('key1 (Null)) '('key2 (Null))";
    const char * null = "'('key1 (Uint32 '%u)) '('key2 (Null))";

    ComparePointAndRange(t, {0, ""}, "'ExcFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '0)) '('key2 (Null) (Void))", 0);
    ComparePointAndRange(t, {0, ""}, "'ExcFrom 'ExcTo '('key1 (Uint32 '1) (Uint32 '1)) '('key2 (Null) (Void))", -1);
    ComparePointAndRange(t, {0, ""}, "'ExcFrom 'ExcTo '('key1 (Uint32 '1) (Uint32 '1)) '('key2 (Null) (Null))", -1);
    ComparePointAndRange(t, {0, ""}, "'IncFrom 'ExcTo '('key1 (Uint32 '1) (Uint32 '1))", -1);
    ComparePointAndRange(t, {1, ""}, "'ExcFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '0)) '('key2 (Null) (Void))", 1);
    ComparePointAndRange(t, {1, ""}, "'ExcFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '1)) '('key2 (Null) (Void))", 0);
    ComparePointAndRange(t, {1, ""}, "'ExcFrom 'ExcTo '('key1 (Uint32 '0) (Void))", 0);

    ComparePointAndRangeStr(t, Sprintf(null, 0), "'IncFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '0)) '('key2 (Null) (Void))", 0);
    ComparePointAndRangeStr(t, Sprintf(null, 0), "'ExcFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '0)) '('key2 (Null) (Void))", -1);
    ComparePointAndRangeStr(t, Sprintf(null, 0), "'IncFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '0))", 0);
    ComparePointAndRangeStr(t, Sprintf(null, 0), "'ExcFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '0))", -1);
    ComparePointAndRangeStr(t, Sprintf(null, 0), "'ExcFrom 'IncTo '('key1 (Null) (Uint32 '0)) '('key2 (Null) (Null))", 0);
    ComparePointAndRangeStr(t, Sprintf(null, 0), "'ExcFrom 'ExcTo '('key1 (Null) (Uint32 '0)) '('key2 (Null) (Null))", 1);
    ComparePointAndRangeStr(t, Sprintf(null, 0), "'ExcFrom 'ExcTo '('key1 (Null) (Uint32 '0))", 0);
    ComparePointAndRangeStr(t, Sprintf(null, 0), "'IncFrom 'ExcTo '('key1 (Null) (Uint32 '0))", 0);
    ComparePointAndRangeStr(t, Sprintf(null, 0), "'ExcFrom 'IncTo '('key1 (Null) (Uint32 '0))", 0);

    ComparePointAndRangeStr(t, Sprintf(null, 1), "'IncFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '0)) '('key2 (Null) (Void))", 1);
    ComparePointAndRangeStr(t, Sprintf(null, 1), "'ExcFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '1)) '('key2 (Null) (Void))", 0);
    ComparePointAndRangeStr(t, Sprintf(null, 1), "'IncFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '0))", 1);
    ComparePointAndRangeStr(t, Sprintf(null, 1), "'ExcFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '1))", 0);
    ComparePointAndRangeStr(t, Sprintf(null, 1), "'ExcFrom 'ExcTo '('key1 (Null) (Uint32 '0))", 1);
    ComparePointAndRangeStr(t, Sprintf(null, 1), "'IncFrom 'ExcTo '('key1 (Null) (Uint32 '1))", 0);
    ComparePointAndRangeStr(t, Sprintf(null, 1), "'ExcFrom 'IncTo '('key1 (Uint32 '2) (Uint32 '2))", -1);

    ComparePointAndRangeStr(t, nullNull, "'IncFrom 'ExcTo '('key1 (Null) (Uint32 '0)) '('key2 (Null) (Void))", 0);
    ComparePointAndRangeStr(t, nullNull, "'ExcFrom 'ExcTo '('key1 (Null) (Uint32 '0)) '('key2 (Null) (Void))", -1);
    ComparePointAndRangeStr(t, nullNull, "'IncFrom 'ExcTo '('key1 (Null) (Null)) '('key2 (Null) (Void))", 0);
    ComparePointAndRangeStr(t, nullNull, "'ExcFrom 'ExcTo '('key1 (Null) (Null)) '('key2 (Null) (Void))", -1);
    ComparePointAndRangeStr(t, nullNull, "'IncFrom 'ExcTo '('key1 (Null) (Null))", 0);
    ComparePointAndRangeStr(t, nullNull, "'ExcFrom 'ExcTo '('key1 (Null) (Null))", -1);

    ComparePointAndRangeStr(t, nullNull, "'IncFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '0)) '('key2 (Null) (Void))", -1);
    ComparePointAndRangeStr(t, nullNull, "'ExcFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '0)) '('key2 (Null) (Void))", -1);
    ComparePointAndRangeStr(t, nullNull, "'IncFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '0))", -1);
    ComparePointAndRangeStr(t, nullNull, "'ExcFrom 'ExcTo '('key1 (Uint32 '0) (Uint32 '0))", -1);
}

}}
