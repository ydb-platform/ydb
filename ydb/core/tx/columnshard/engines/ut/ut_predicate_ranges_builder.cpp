#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/columnshard/engines/predicate/filter.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cstring>

#include <util/generic/vector.h>

namespace NKikimr {

namespace {

using NOlap::TNameTypeInfo;

std::vector<TNameTypeInfo> MakePairTsUtf8Pk() {
    return {
        {"timestamp", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)},
        {"resource_type", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)},
    };
}

TVector<NScheme::TTypeInfo> PairTsUtf8KeyTypes() {
    return {
        NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp),
        NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
    };
}

TTableRange MakeTableRange(TVector<TCell>& from, bool incFrom, TVector<TCell>& to, bool incTo) {
    TConstArrayRef<TCell> fromRef(from);
    TConstArrayRef<TCell> toRef(to);
    return TTableRange(fromRef, incFrom, toRef, incTo);
}

bool PrefixEqual(TConstArrayRef<TCell> x, TConstArrayRef<TCell> y) {
    if (x.size() != y.size()) {
        return false;
    }
    const TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
    if (x.size() > types.size()) {
        return false;
    }
    for (size_t i = 0; i < x.size(); ++i) {
        if (x[i].IsNull() || y[i].IsNull()) {
            if (!(x[i].IsNull() && y[i].IsNull())) {
                return false;
            }
            continue;
        }
        if (CompareTypedCells(x[i], y[i], types[i]) != 0) {
            return false;
        }
    }
    return true;
}

bool TableRangesEqual(const TTableRange& a, const TTableRange& b) {
    return a.InclusiveFrom == b.InclusiveFrom && a.InclusiveTo == b.InclusiveTo && a.Point == b.Point
        && PrefixEqual(a.From, b.From) && PrefixEqual(a.To, b.To);
}

TConstArrayRef<NScheme::TTypeInfo> KeyTypesRef(const TVector<NScheme::TTypeInfo>& v) {
    return TConstArrayRef<NScheme::TTypeInfo>(v.data(), v.size());
}

}   // namespace

// scheme_tabledefs.h: TTableRange / TSerializedTableRange semantics (partial keys, null, inclusive, IsAmbiguous, IsEmpty).
Y_UNIT_TEST_SUITE(TColumnShardPredicateRangesBuilder) {

    Y_UNIT_TEST(TRangesBuilder_ExclusiveOpenToWithNullSecondKeyColumn) {
        using NOlap::TRangesBuilder;

        constexpr ui64 TsStartUs = 1776309661000000ULL;
        constexpr ui64 TsEndUs = 1776310561000000ULL;
        static constexpr const char ResourceType[] = "a1";

        auto ydbPk = MakePairTsUtf8Pk();
        auto pkSchema = NArrow::MakeArrowSchema(ydbPk, {}).ValueOrDie();

        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(TsStartUs));
        fromCells.push_back(TCell(ResourceType, sizeof(ResourceType) - 1));

        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(TsEndUs));
        toCells.emplace_back();

        const TString fromBuf = TSerializedCellVec::Serialize(fromCells);
        const TString toBuf = TSerializedCellVec::Serialize(toCells);

        TRangesBuilder builder(ydbPk, pkSchema);
        builder.AddRange(TSerializedTableRange(fromBuf, toBuf, false, false));
        auto filter = builder.Finish();
        UNIT_ASSERT_C(filter.IsSuccess(), filter.GetErrorMessage());
        UNIT_ASSERT_VALUES_EQUAL(filter->Size(), 1);
        const TString dbg = filter->DebugString();
        UNIT_ASSERT_C(!dbg.empty(), dbg);
        // Явный NULL во втором ключе должен входить в префикс предиката (старый код давал NumColumns==1 для To).
        const NOlap::TPKRangeFilter& pkRange = *filter->begin();
        UNIT_ASSERT_VALUES_EQUAL(pkRange.GetPredicateFrom().NumColumns(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(pkRange.GetPredicateTo().NumColumns(), 2u);
        UNIT_ASSERT_C(dbg.Contains("resource_type"), dbg);
    }

    // scheme_tabledefs.h: "probably most used case" — full prefix on both sides, inclusive.
    Y_UNIT_TEST(TRangesBuilder_InclusiveClosedInterval) {
        using NOlap::TRangesBuilder;

        constexpr ui64 T0 = 100ULL;
        constexpr ui64 T1 = 200ULL;
        static constexpr const char K0[] = "a";
        static constexpr const char K1[] = "z";

        auto ydbPk = MakePairTsUtf8Pk();
        auto pkSchema = NArrow::MakeArrowSchema(ydbPk, {}).ValueOrDie();

        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(T0));
        fromCells.push_back(TCell(K0, sizeof(K0) - 1));
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(T1));
        toCells.push_back(TCell(K1, sizeof(K1) - 1));

        const TString fromBuf = TSerializedCellVec::Serialize(fromCells);
        const TString toBuf = TSerializedCellVec::Serialize(toCells);

        TSerializedTableRange ser(fromBuf, toBuf, true, true);
        UNIT_ASSERT(!ser.ToTableRange().IsEmptyRange(PairTsUtf8KeyTypes()));
        UNIT_ASSERT(!ser.ToTableRange().IsAmbiguousReason(2));

        TRangesBuilder builder(ydbPk, pkSchema);
        builder.AddRange(std::move(ser));
        auto filter = builder.Finish();
        UNIT_ASSERT_C(filter.IsSuccess(), filter.GetErrorMessage());
        UNIT_ASSERT_VALUES_EQUAL(filter->Size(), 1);
    }

    // Incomplete To must be inclusive (IsAmbiguousReason); TRangesBuilder accepts short To + padding.
    Y_UNIT_TEST(TRangesBuilder_PartialToOneColumn_InclusiveTo) {
        using NOlap::TRangesBuilder;

        constexpr ui64 T0 = 50ULL;
        constexpr ui64 T1 = 300ULL;
        static constexpr const char K0[] = "m";

        auto ydbPk = MakePairTsUtf8Pk();
        auto pkSchema = NArrow::MakeArrowSchema(ydbPk, {}).ValueOrDie();

        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(T0));
        fromCells.push_back(TCell(K0, sizeof(K0) - 1));
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(T1)); // open suffix on resource_type (missing tail in serialization)

        const TString fromBuf = TSerializedCellVec::Serialize(fromCells);
        const TString toBuf = TSerializedCellVec::Serialize(toCells);

        TSerializedTableRange ser(fromBuf, toBuf, false, true);
        UNIT_ASSERT(!ser.ToTableRange().IsAmbiguousReason(2));

        TRangesBuilder builder(ydbPk, pkSchema);
        builder.AddRange(std::move(ser));
        auto filter = builder.Finish();
        UNIT_ASSERT_C(filter.IsSuccess(), filter.GetErrorMessage());
    }

    // Incomplete From must be exclusive (non-inclusive); short From + full To.
    Y_UNIT_TEST(TRangesBuilder_PartialFromOneColumn_ExclusiveFrom) {
        using NOlap::TRangesBuilder;

        constexpr ui64 T0 = 100ULL;
        constexpr ui64 T1 = 200ULL;
        static constexpr const char K1[] = "z";

        auto ydbPk = MakePairTsUtf8Pk();
        auto pkSchema = NArrow::MakeArrowSchema(ydbPk, {}).ValueOrDie();

        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(T0));
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(T1));
        toCells.push_back(TCell(K1, sizeof(K1) - 1));

        const TString fromBuf = TSerializedCellVec::Serialize(fromCells);
        const TString toBuf = TSerializedCellVec::Serialize(toCells);

        TSerializedTableRange ser(fromBuf, toBuf, false, true);
        UNIT_ASSERT(!ser.ToTableRange().IsAmbiguousReason(2));

        TRangesBuilder builder(ydbPk, pkSchema);
        builder.AddRange(std::move(ser));
        auto filter = builder.Finish();
        UNIT_ASSERT_C(filter.IsSuccess(), filter.GetErrorMessage());
    }

    // scheme_tabledefs.h TTableRange::IsEmptyRange: upper bound strictly below lower → empty.
    Y_UNIT_TEST(TableRange_IsEmpty_WhenExclusiveUpperBelowLower) {
        constexpr ui64 TLo = 10ULL;
        constexpr ui64 THi = 5ULL;
        static constexpr const char K[] = "k";

        // From = (10,k), To = (5,k) — верхняя граница строго ниже нижней по первому столбцу.
        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(TLo));
        fromCells.push_back(TCell(K, sizeof(K) - 1));
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(THi));
        toCells.push_back(TCell(K, sizeof(K) - 1));

        TSerializedTableRange ser(TSerializedCellVec::Serialize(fromCells), TSerializedCellVec::Serialize(toCells), false, false);
        UNIT_ASSERT(ser.ToTableRange().IsEmptyRange(PairTsUtf8KeyTypes()));
    }

    // IsAmbiguousReason: incomplete From with inclusive from is ambiguous.
    Y_UNIT_TEST(TableRange_IsAmbiguous_PartialFromInclusive) {
        constexpr ui64 T0 = 1ULL;
        static constexpr const char K[] = "x";

        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(T0));
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(T0));
        toCells.push_back(TCell(K, sizeof(K) - 1));

        TSerializedTableRange ser(TSerializedCellVec::Serialize(fromCells), TSerializedCellVec::Serialize(toCells), true, true);
        UNIT_ASSERT(ser.ToTableRange().IsAmbiguousReason(2) != nullptr);
    }

    // IsAmbiguousReason: incomplete To with exclusive to is ambiguous.
    Y_UNIT_TEST(TableRange_IsAmbiguous_PartialToExclusive) {
        constexpr ui64 T0 = 1ULL;
        constexpr ui64 T1 = 2ULL;
        static constexpr const char K[] = "x";

        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(T0));
        fromCells.push_back(TCell(K, sizeof(K) - 1));
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(T1));

        TSerializedTableRange ser(TSerializedCellVec::Serialize(fromCells), TSerializedCellVec::Serialize(toCells), true, false);
        UNIT_ASSERT(ser.ToTableRange().IsAmbiguousReason(2) != nullptr);
    }

    // (-inf, +inf): (null,null) inclusive From, empty To, inclusive To false — IsFullRange.
    Y_UNIT_TEST(TableRange_IsFullRange_AllNullFromEmptyTo) {
        TVector<TCell> fromCells;
        fromCells.emplace_back();
        fromCells.emplace_back();

        TSerializedTableRange ser(TSerializedCellVec::Serialize(fromCells), TString(), true, false);
        UNIT_ASSERT(ser.ToTableRange().IsFullRange(2));
    }

    // IsFullRange negative cases (scheme_tabledefs.cpp).
    Y_UNIT_TEST(TableRange_IsFullRange_NotWhenExclusiveFrom) {
        TVector<TCell> fromCells;
        fromCells.emplace_back();
        fromCells.emplace_back();
        TSerializedTableRange ser(TSerializedCellVec::Serialize(fromCells), TString(), false, false);
        UNIT_ASSERT(!ser.ToTableRange().IsFullRange(2));
    }

    Y_UNIT_TEST(TableRange_IsFullRange_NotWhenToNonEmpty) {
        TVector<TCell> fromCells;
        fromCells.emplace_back();
        fromCells.emplace_back();
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(1ULL));
        toCells.emplace_back();
        TSerializedTableRange ser(
            TSerializedCellVec::Serialize(fromCells), TSerializedCellVec::Serialize(toCells), true, true);
        UNIT_ASSERT(!ser.ToTableRange().IsFullRange(2));
    }

    Y_UNIT_TEST(TableRange_IsFullRange_NotWhenFromWrongLength) {
        TVector<TCell> fromCells;
        fromCells.emplace_back();
        TSerializedTableRange ser(TSerializedCellVec::Serialize(fromCells), TString(), true, false);
        UNIT_ASSERT(!ser.ToTableRange().IsFullRange(2));
    }

    Y_UNIT_TEST(TableRange_IsFullRange_NotWhenFromHasValue) {
        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(1ULL));
        fromCells.emplace_back();
        TSerializedTableRange ser(TSerializedCellVec::Serialize(fromCells), TString(), true, false);
        UNIT_ASSERT(!ser.ToTableRange().IsFullRange(2));
    }

    // TSerializedTableRange::IsEmpty must match TTableRange::IsEmptyRange (same CompareBorders call).
    Y_UNIT_TEST(SerializedTableRange_IsEmpty_MatchesToTableRange) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        constexpr ui64 TLo = 100ULL;
        constexpr ui64 THi = 50ULL;
        static constexpr const char K[] = "k";
        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(TLo));
        fromCells.push_back(TCell(K, sizeof(K) - 1));
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(THi));
        toCells.push_back(TCell(K, sizeof(K) - 1));
        TSerializedTableRange ser(
            TSerializedCellVec::Serialize(fromCells), TSerializedCellVec::Serialize(toCells), false, false);
        UNIT_ASSERT_EQUAL(ser.IsEmpty(KeyTypesRef(types)), ser.ToTableRange().IsEmptyRange(KeyTypesRef(types)));
        UNIT_ASSERT(ser.IsEmpty(KeyTypesRef(types)));
    }

    // CompareBorders<true,false> at equal full keys: both exclusive → empty; both inclusive → not empty.
    Y_UNIT_TEST(TableRange_IsEmpty_SameFullKeyBothExclusive) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        constexpr ui64 T0 = 7ULL;
        static constexpr const char K[] = "q";
        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(T0));
        fromCells.push_back(TCell(K, sizeof(K) - 1));
        TVector<TCell> toCells = fromCells;
        TSerializedTableRange ser(
            TSerializedCellVec::Serialize(fromCells), TSerializedCellVec::Serialize(toCells), false, false);
        UNIT_ASSERT(ser.ToTableRange().IsEmptyRange(KeyTypesRef(types)));
    }

    Y_UNIT_TEST(TableRange_NotEmpty_SameFullKeyBothInclusive) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        constexpr ui64 T0 = 7ULL;
        static constexpr const char K[] = "q";
        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(T0));
        fromCells.push_back(TCell(K, sizeof(K) - 1));
        TVector<TCell> toCells = fromCells;
        TSerializedTableRange ser(
            TSerializedCellVec::Serialize(fromCells), TSerializedCellVec::Serialize(toCells), true, true);
        UNIT_ASSERT(!ser.ToTableRange().IsEmptyRange(KeyTypesRef(types)));
    }

    // scheme_tabledefs.h: [{a, null}, {b, null}) for 2-col key — (ts, null) to (ts, null), inclusive/exclusive ends.
    Y_UNIT_TEST(TRangesBuilder_DoubleNullSecondKey_OpenInterval) {
        using NOlap::TRangesBuilder;

        constexpr ui64 Ta = 11ULL;
        constexpr ui64 Tb = 99ULL;

        auto ydbPk = MakePairTsUtf8Pk();
        auto pkSchema = NArrow::MakeArrowSchema(ydbPk, {}).ValueOrDie();

        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(Ta));
        fromCells.emplace_back();
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(Tb));
        toCells.emplace_back();

        TSerializedTableRange ser(
            TSerializedCellVec::Serialize(fromCells), TSerializedCellVec::Serialize(toCells), true, false);
        UNIT_ASSERT(!ser.ToTableRange().IsAmbiguousReason(2));

        TRangesBuilder builder(ydbPk, pkSchema);
        builder.AddRange(std::move(ser));
        auto filter = builder.Finish();
        UNIT_ASSERT_C(filter.IsSuccess(), filter.GetErrorMessage());
        const NOlap::TPKRangeFilter& pkRange = *filter->begin();
        UNIT_ASSERT_VALUES_EQUAL(pkRange.GetPredicateFrom().NumColumns(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(pkRange.GetPredicateTo().NumColumns(), 2u);
        UNIT_ASSERT(filter->DebugString().Contains("resource_type"));
    }

    // 2-column PK only: reject 3-column serialized prefix (IsAmbiguousReason).
    Y_UNIT_TEST(TableRange_IsAmbiguous_FromTooManyCells) {
        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(1ULL));
        fromCells.push_back(TCell("x", 1));
        fromCells.push_back(TCell::Make(2ULL));
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(3ULL));
        toCells.push_back(TCell("y", 1));
        TSerializedTableRange ser(
            TSerializedCellVec::Serialize(fromCells), TSerializedCellVec::Serialize(toCells), false, true);
        const char* reason = ser.ToTableRange().IsAmbiguousReason(2);
        UNIT_ASSERT_C(reason, reason);
        UNIT_ASSERT(strstr(reason, "From is too large") != nullptr);
    }

    Y_UNIT_TEST(TableRange_IsAmbiguous_ToTooManyCells) {
        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(1ULL));
        fromCells.push_back(TCell("x", 1));
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(3ULL));
        toCells.push_back(TCell("y", 1));
        toCells.push_back(TCell::Make(4ULL));
        TSerializedTableRange ser(
            TSerializedCellVec::Serialize(fromCells), TSerializedCellVec::Serialize(toCells), false, true);
        const char* reason = ser.ToTableRange().IsAmbiguousReason(2);
        UNIT_ASSERT_C(reason, reason);
        UNIT_ASSERT(strstr(reason, "To is too large") != nullptr);
    }

    Y_UNIT_TEST(TableRange_IsAmbiguous_PointWrongColumnCount) {
        TVector<TCell> oneCol;
        oneCol.push_back(TCell::Make(1ULL));
        TConstArrayRef<TCell> ref(oneCol);
        TTableRange point(ref);
        TSerializedTableRange ser(point);
        UNIT_ASSERT(ser.Point);
        const char* reason = ser.ToTableRange().IsAmbiguousReason(2);
        UNIT_ASSERT_C(reason, reason);
        UNIT_ASSERT(strstr(reason, "key columns count") != nullptr);
    }

    // With RelaxEmptyKeys, empty From + non-inclusive From is still unambiguous.
    Y_UNIT_TEST(TableRange_NotAmbiguous_EmptyFromNonInclusive) {
        TVector<TCell> emptyFrom;
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(5ULL));
        toCells.push_back(TCell("z", 1));
        TSerializedTableRange ser(
            TSerializedCellVec::Serialize(emptyFrom), TSerializedCellVec::Serialize(toCells), false, true);
        UNIT_ASSERT(!ser.ToTableRange().IsAmbiguousReason(2));
    }

    Y_UNIT_TEST(TableRange_NotAmbiguous_EmptyToNonInclusive) {
        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(1ULL));
        fromCells.push_back(TCell("a", 1));
        TVector<TCell> emptyTo;
        TSerializedTableRange ser(
            TSerializedCellVec::Serialize(fromCells), TSerializedCellVec::Serialize(emptyTo), true, false);
        UNIT_ASSERT(!ser.ToTableRange().IsAmbiguousReason(2));
    }

    // Same first key; upper bound strictly below lower on Utf8 column → empty.
    Y_UNIT_TEST(TableRange_IsEmpty_WhenSecondKeyUpperBelowLower) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        constexpr ui64 T0 = 100ULL;
        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(T0));
        fromCells.push_back(TCell("z", 1));
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(T0));
        toCells.push_back(TCell("a", 1));
        TSerializedTableRange ser(
            TSerializedCellVec::Serialize(fromCells), TSerializedCellVec::Serialize(toCells), false, false);
        UNIT_ASSERT(ser.ToTableRange().IsEmptyRange(KeyTypesRef(types)));
    }

    Y_UNIT_TEST(TableRange_Intersect_IsCommutativeForOverlappingPair) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        TVector<TCell> aFrom;
        aFrom.push_back(TCell::Make(1ULL));
        aFrom.push_back(TCell("a", 1));
        TVector<TCell> aTo;
        aTo.push_back(TCell::Make(10ULL));
        aTo.push_back(TCell("z", 1));
        TVector<TCell> bFrom;
        bFrom.push_back(TCell::Make(5ULL));
        bFrom.push_back(TCell("a", 1));
        TVector<TCell> bTo;
        bTo.push_back(TCell::Make(15ULL));
        bTo.push_back(TCell("z", 1));
        TTableRange ra = MakeTableRange(aFrom, true, aTo, true);
        TTableRange rb = MakeTableRange(bFrom, true, bTo, true);
        auto ab = Intersect(KeyTypesRef(types), ra, rb);
        auto ba = Intersect(KeyTypesRef(types), rb, ra);
        UNIT_ASSERT(TableRangesEqual(ab, ba));
    }

    Y_UNIT_TEST(SerializedTableRange_CellCtorMatchesSerializedBuffers) {
        constexpr ui64 T0 = 2ULL;
        constexpr ui64 T1 = 4ULL;
        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(T0));
        fromCells.push_back(TCell("c", 1));
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(T1));
        toCells.push_back(TCell("d", 1));
        TSerializedTableRange viaBuf(
            TSerializedCellVec::Serialize(fromCells), TSerializedCellVec::Serialize(toCells), true, false);
        TConstArrayRef<TCell> fromRef(fromCells);
        TConstArrayRef<TCell> toRef(toCells);
        TSerializedTableRange viaCells(fromRef, true, toRef, false);
        UNIT_ASSERT(PrefixEqual(viaBuf.From.GetCells(), viaCells.From.GetCells()));
        UNIT_ASSERT(PrefixEqual(viaBuf.To.GetCells(), viaCells.To.GetCells()));
        UNIT_ASSERT_VALUES_EQUAL(viaBuf.FromInclusive, viaCells.FromInclusive);
        UNIT_ASSERT_VALUES_EQUAL(viaBuf.ToInclusive, viaCells.ToInclusive);
    }

    Y_UNIT_TEST(TableRange_Intersect_IdenticalRanges) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        TVector<TCell> aFrom;
        aFrom.push_back(TCell::Make(1ULL));
        aFrom.push_back(TCell("a", 1));
        TVector<TCell> aTo;
        aTo.push_back(TCell::Make(10ULL));
        aTo.push_back(TCell("z", 1));
        TTableRange a = MakeTableRange(aFrom, true, aTo, true);
        auto got = Intersect(KeyTypesRef(types), a, a);
        UNIT_ASSERT(TableRangesEqual(got, a));
    }

    Y_UNIT_TEST(TableRange_Intersect_InnerRange) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        TVector<TCell> outerFrom;
        outerFrom.push_back(TCell::Make(1ULL));
        outerFrom.push_back(TCell("a", 1));
        TVector<TCell> outerTo;
        outerTo.push_back(TCell::Make(100ULL));
        outerTo.push_back(TCell("z", 1));
        TVector<TCell> innerFrom;
        innerFrom.push_back(TCell::Make(5ULL));
        innerFrom.push_back(TCell("m", 1));
        TVector<TCell> innerTo;
        innerTo.push_back(TCell::Make(20ULL));
        innerTo.push_back(TCell("n", 1));
        TTableRange outer = MakeTableRange(outerFrom, true, outerTo, true);
        TTableRange inner = MakeTableRange(innerFrom, true, innerTo, true);
        auto got = Intersect(KeyTypesRef(types), outer, inner);
        UNIT_ASSERT(TableRangesEqual(got, inner));
    }

    Y_UNIT_TEST(TableRange_Intersect_FirstEmpty_ReturnsFirst) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        TVector<TCell> hiFrom;
        hiFrom.push_back(TCell::Make(10ULL));
        hiFrom.push_back(TCell("k", 1));
        TVector<TCell> loTo;
        loTo.push_back(TCell::Make(5ULL));
        loTo.push_back(TCell("k", 1));
        TTableRange emptyR = MakeTableRange(hiFrom, false, loTo, false);
        UNIT_ASSERT(emptyR.IsEmptyRange(KeyTypesRef(types)));

        TVector<TCell> gFrom;
        gFrom.push_back(TCell::Make(0ULL));
        gFrom.push_back(TCell("a", 1));
        TVector<TCell> gTo;
        gTo.push_back(TCell::Make(50ULL));
        gTo.push_back(TCell("z", 1));
        TTableRange good = MakeTableRange(gFrom, true, gTo, true);
        auto got = Intersect(KeyTypesRef(types), emptyR, good);
        UNIT_ASSERT(TableRangesEqual(got, emptyR));
    }

    Y_UNIT_TEST(CompareRanges_OverlapVsDisjoint) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        TVector<TCell> aFrom;
        aFrom.push_back(TCell::Make(1ULL));
        aFrom.push_back(TCell("a", 1));
        TVector<TCell> aTo;
        aTo.push_back(TCell::Make(5ULL));
        aTo.push_back(TCell("z", 1));
        TVector<TCell> bFrom;
        bFrom.push_back(TCell::Make(3ULL));
        bFrom.push_back(TCell("a", 1));
        TVector<TCell> bTo;
        bTo.push_back(TCell::Make(10ULL));
        bTo.push_back(TCell("z", 1));
        TTableRange ra = MakeTableRange(aFrom, true, aTo, true);
        TTableRange rb = MakeTableRange(bFrom, true, bTo, true);
        UNIT_ASSERT_VALUES_EQUAL(CompareRanges(ra, rb, KeyTypesRef(types)), 0);

        TVector<TCell> cFrom;
        cFrom.push_back(TCell::Make(100ULL));
        cFrom.push_back(TCell("a", 1));
        TVector<TCell> cTo;
        cTo.push_back(TCell::Make(200ULL));
        cTo.push_back(TCell("z", 1));
        TTableRange rc = MakeTableRange(cFrom, true, cTo, true);
        const int ab = CompareRanges(ra, rc, KeyTypesRef(types));
        UNIT_ASSERT(ab != 0);
    }

    Y_UNIT_TEST(CheckRangesOverlap_TwoIntervals) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        TVector<TCell> aFrom;
        aFrom.push_back(TCell::Make(1ULL));
        aFrom.push_back(TCell("a", 1));
        TVector<TCell> aTo;
        aTo.push_back(TCell::Make(5ULL));
        aTo.push_back(TCell("z", 1));
        TVector<TCell> bFrom;
        bFrom.push_back(TCell::Make(4ULL));
        bFrom.push_back(TCell("a", 1));
        TVector<TCell> bTo;
        bTo.push_back(TCell::Make(6ULL));
        bTo.push_back(TCell("z", 1));
        TTableRange ra = MakeTableRange(aFrom, true, aTo, true);
        TTableRange rb = MakeTableRange(bFrom, true, bTo, true);
        UNIT_ASSERT(CheckRangesOverlap(ra, rb, KeyTypesRef(types), KeyTypesRef(types)));
    }

    Y_UNIT_TEST(IsAllowedKeyType_JsonDisallowed_Uint64Allowed) {
        UNIT_ASSERT(!IsAllowedKeyType(NScheme::TTypeInfo(NScheme::NTypeIds::Json)));
        UNIT_ASSERT(!IsAllowedKeyType(NScheme::TTypeInfo(NScheme::NTypeIds::Float)));
        UNIT_ASSERT(IsAllowedKeyType(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)));
        UNIT_ASSERT(IsAllowedKeyType(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)));
    }

    Y_UNIT_TEST(SerializedTableRange_KeyRangeRoundTrip_NonPoint) {
        constexpr ui64 T0 = 3ULL;
        constexpr ui64 T1 = 9ULL;
        TVector<TCell> fromCells;
        fromCells.push_back(TCell::Make(T0));
        fromCells.push_back(TCell("a", 1));
        TVector<TCell> toCells;
        toCells.push_back(TCell::Make(T1));
        toCells.push_back(TCell("b", 1));
        TSerializedTableRange ser(
            TSerializedCellVec::Serialize(fromCells), TSerializedCellVec::Serialize(toCells), false, true);

        NKikimrTx::TKeyRange kr;
        ser.Serialize(kr);
        TSerializedTableRange loaded;
        loaded.Load(kr);

        UNIT_ASSERT_VALUES_EQUAL(loaded.FromInclusive, ser.FromInclusive);
        UNIT_ASSERT_VALUES_EQUAL(loaded.ToInclusive, ser.ToInclusive);
        UNIT_ASSERT(!loaded.Point);
        UNIT_ASSERT(PrefixEqual(loaded.From.GetCells(), ser.From.GetCells()));
        UNIT_ASSERT(PrefixEqual(loaded.To.GetCells(), ser.To.GetCells()));
    }

    Y_UNIT_TEST(TRangesBuilder_TwoRanges) {
        using NOlap::TRangesBuilder;

        auto ydbPk = MakePairTsUtf8Pk();
        auto pkSchema = NArrow::MakeArrowSchema(ydbPk, {}).ValueOrDie();

        TVector<TCell> f0;
        f0.push_back(TCell::Make(1ULL));
        f0.push_back(TCell("a", 1));
        TVector<TCell> t0;
        t0.push_back(TCell::Make(2ULL));
        t0.push_back(TCell("b", 1));

        TVector<TCell> f1;
        f1.push_back(TCell::Make(100ULL));
        f1.push_back(TCell("c", 1));
        TVector<TCell> t1;
        t1.push_back(TCell::Make(200ULL));
        t1.push_back(TCell("d", 1));

        TRangesBuilder builder(ydbPk, pkSchema);
        builder.AddRange(TSerializedTableRange(
            TSerializedCellVec::Serialize(f0), TSerializedCellVec::Serialize(t0), true, true));
        builder.AddRange(TSerializedTableRange(
            TSerializedCellVec::Serialize(f1), TSerializedCellVec::Serialize(t1), true, true));
        auto filter = builder.Finish();
        UNIT_ASSERT_C(filter.IsSuccess(), filter.GetErrorMessage());
        UNIT_ASSERT_VALUES_EQUAL(filter->Size(), 2);
    }

    // Point: same key prefix, Point flag (TTableRange point ctor via TSerializedTableRange copy).
    Y_UNIT_TEST(TRangesBuilder_PointRange) {
        using NOlap::TRangesBuilder;

        constexpr ui64 T0 = 42ULL;
        static constexpr const char K[] = "p";

        auto ydbPk = MakePairTsUtf8Pk();
        auto pkSchema = NArrow::MakeArrowSchema(ydbPk, {}).ValueOrDie();

        TVector<TCell> cells;
        cells.push_back(TCell::Make(T0));
        cells.push_back(TCell(K, sizeof(K) - 1));

        TConstArrayRef<TCell> cellsRef(cells);
        TTableRange pointRange(cellsRef);
        TSerializedTableRange ser(pointRange);
        UNIT_ASSERT(ser.Point);
        UNIT_ASSERT(!ser.ToTableRange().IsAmbiguousReason(2));

        TRangesBuilder builder(ydbPk, pkSchema);
        builder.AddRange(std::move(ser));
        auto filter = builder.Finish();
        UNIT_ASSERT_C(filter.IsSuccess(), filter.GetErrorMessage());
        UNIT_ASSERT_VALUES_EQUAL(filter->Size(), 1);
    }

    // TTableRange::IsEmptyRange: для Point всегда false (scheme_tabledefs.cpp).
    Y_UNIT_TEST(Point_IsEmptyRange_AlwaysFalse) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        TVector<TCell> cells;
        cells.push_back(TCell::Make(1ULL));
        cells.push_back(TCell("k", 1));
        TConstArrayRef<TCell> ref(cells);
        TTableRange point(ref);
        UNIT_ASSERT(point.Point);
        UNIT_ASSERT(!point.IsEmptyRange(KeyTypesRef(types)));
    }

    // Точка (null, null) на полном ключе — не ambiguous при columnCount=2.
    Y_UNIT_TEST(Point_IsAmbiguousReason_NullFullKey_Ok) {
        TVector<TCell> cells;
        cells.emplace_back();
        cells.emplace_back();
        TConstArrayRef<TCell> ref(cells);
        TTableRange point(ref);
        TSerializedTableRange ser(point);
        UNIT_ASSERT(ser.Point);
        UNIT_ASSERT(!ser.ToTableRange().IsAmbiguousReason(2));
    }

    Y_UNIT_TEST(ComparePointAndRange_InsideClosedInterval) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        TVector<TCell> pCells;
        pCells.push_back(TCell::Make(15ULL));
        pCells.push_back(TCell("m", 1));
        TConstArrayRef<TCell> pref(pCells);

        TVector<TCell> rFrom;
        rFrom.push_back(TCell::Make(10ULL));
        rFrom.push_back(TCell("a", 1));
        TVector<TCell> rTo;
        rTo.push_back(TCell::Make(20ULL));
        rTo.push_back(TCell("z", 1));
        TTableRange interval = MakeTableRange(rFrom, true, rTo, true);

        UNIT_ASSERT_VALUES_EQUAL(ComparePointAndRange(pref, interval, KeyTypesRef(types), KeyTypesRef(types)), 0);
    }

    Y_UNIT_TEST(ComparePointAndRange_BelowExclusiveFrom) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        TVector<TCell> pCells;
        pCells.push_back(TCell::Make(10ULL));
        pCells.push_back(TCell("a", 1));
        TConstArrayRef<TCell> pref(pCells);

        TVector<TCell> rFrom = pCells;
        TVector<TCell> rTo;
        rTo.push_back(TCell::Make(20ULL));
        rTo.push_back(TCell("z", 1));
        TTableRange interval = MakeTableRange(rFrom, false, rTo, true);

        UNIT_ASSERT_VALUES_EQUAL(ComparePointAndRange(pref, interval, KeyTypesRef(types), KeyTypesRef(types)), -1);
    }

    Y_UNIT_TEST(ComparePointAndRange_AboveExclusiveTo) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        TVector<TCell> pCells;
        pCells.push_back(TCell::Make(20ULL));
        pCells.push_back(TCell("z", 1));
        TConstArrayRef<TCell> pref(pCells);

        TVector<TCell> rFrom;
        rFrom.push_back(TCell::Make(10ULL));
        rFrom.push_back(TCell("a", 1));
        TVector<TCell> rTo = pCells;
        TTableRange interval = MakeTableRange(rFrom, true, rTo, false);

        UNIT_ASSERT_VALUES_EQUAL(ComparePointAndRange(pref, interval, KeyTypesRef(types), KeyTypesRef(types)), 1);
    }

    Y_UNIT_TEST(CheckRangesOverlap_TwoEqualPoints) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        TVector<TCell> cells;
        cells.push_back(TCell::Make(7ULL));
        cells.push_back(TCell("x", 1));
        TConstArrayRef<TCell> ref(cells);
        TTableRange p1(ref);
        TTableRange p2(ref);
        UNIT_ASSERT(CheckRangesOverlap(p1, p2, KeyTypesRef(types), KeyTypesRef(types)));
    }

    Y_UNIT_TEST(CheckRangesOverlap_TwoDistinctPoints) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        TVector<TCell> c1;
        c1.push_back(TCell::Make(1ULL));
        c1.push_back(TCell("a", 1));
        TVector<TCell> c2;
        c2.push_back(TCell::Make(2ULL));
        c2.push_back(TCell("b", 1));
        TConstArrayRef<TCell> r1(c1);
        TConstArrayRef<TCell> r2(c2);
        TTableRange p1(r1);
        TTableRange p2(r2);
        UNIT_ASSERT(!CheckRangesOverlap(p1, p2, KeyTypesRef(types), KeyTypesRef(types)));
    }

    Y_UNIT_TEST(CheckRangesOverlap_PointInsideInterval) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        TVector<TCell> pCells;
        pCells.push_back(TCell::Make(5ULL));
        pCells.push_back(TCell("m", 1));
        TConstArrayRef<TCell> prefInside(pCells);
        TTableRange point(prefInside);

        TVector<TCell> rFrom;
        rFrom.push_back(TCell::Make(1ULL));
        rFrom.push_back(TCell("a", 1));
        TVector<TCell> rTo;
        rTo.push_back(TCell::Make(10ULL));
        rTo.push_back(TCell("z", 1));
        TTableRange interval = MakeTableRange(rFrom, true, rTo, true);

        UNIT_ASSERT(CheckRangesOverlap(point, interval, KeyTypesRef(types), KeyTypesRef(types)));
        UNIT_ASSERT(CheckRangesOverlap(interval, point, KeyTypesRef(types), KeyTypesRef(types)));
    }

    Y_UNIT_TEST(CheckRangesOverlap_PointOutsideInterval) {
        TVector<NScheme::TTypeInfo> types = PairTsUtf8KeyTypes();
        TVector<TCell> pCells;
        pCells.push_back(TCell::Make(100ULL));
        pCells.push_back(TCell("m", 1));
        TConstArrayRef<TCell> prefOutside(pCells);
        TTableRange point(prefOutside);

        TVector<TCell> rFrom;
        rFrom.push_back(TCell::Make(1ULL));
        rFrom.push_back(TCell("a", 1));
        TVector<TCell> rTo;
        rTo.push_back(TCell::Make(10ULL));
        rTo.push_back(TCell("z", 1));
        TTableRange interval = MakeTableRange(rFrom, true, rTo, true);

        UNIT_ASSERT(!CheckRangesOverlap(point, interval, KeyTypesRef(types), KeyTypesRef(types)));
    }

    Y_UNIT_TEST(SerializedTableRange_Point_SerializeKeyRange_ToEqualsFrom) {
        TVector<TCell> cells;
        cells.push_back(TCell::Make(99ULL));
        cells.push_back(TCell("q", 1));
        TConstArrayRef<TCell> cellsRef(cells);
        TTableRange asPoint(cellsRef);
        TSerializedTableRange ser(asPoint);

        NKikimrTx::TKeyRange kr;
        ser.Serialize(kr);
        UNIT_ASSERT_VALUES_EQUAL(kr.GetFrom(), kr.GetTo());
        UNIT_ASSERT(kr.GetFromInclusive());
        UNIT_ASSERT(kr.GetToInclusive());
    }

    // Load() не восстанавливает флаг Point; ячейки From/To в protobuf совпадают для точки.
    Y_UNIT_TEST(SerializedTableRange_Point_SerializeLoad_CellsPreserved) {
        TVector<TCell> cells;
        cells.push_back(TCell::Make(8ULL));
        cells.push_back(TCell("w", 1));
        TConstArrayRef<TCell> cellsRefW(cells);
        TTableRange asPointW(cellsRefW);
        TSerializedTableRange ser(asPointW);

        NKikimrTx::TKeyRange kr;
        ser.Serialize(kr);
        TSerializedTableRange loaded;
        loaded.Load(kr);

        UNIT_ASSERT(!loaded.Point);
        UNIT_ASSERT(PrefixEqual(loaded.From.GetCells(), ser.From.GetCells()));
        UNIT_ASSERT(PrefixEqual(loaded.To.GetCells(), ser.From.GetCells()));
        UNIT_ASSERT(loaded.ToInclusive);
    }

    // Второй point после первого даёт «not sorted sequence»: To-предикат первой точки
    // пересекается с From второй (см. TPKRangesFilter::Add / CrossRanges).
    Y_UNIT_TEST(TRangesBuilder_TwoPointsSecondFailsNotSortedSequence) {
        using NOlap::TRangesBuilder;

        auto ydbPk = MakePairTsUtf8Pk();
        auto pkSchema = NArrow::MakeArrowSchema(ydbPk, {}).ValueOrDie();

        TVector<TCell> c0;
        c0.push_back(TCell::Make(1ULL));
        c0.push_back(TCell("a", 1));
        TVector<TCell> c1;
        c1.push_back(TCell::Make(1000ULL));
        c1.push_back(TCell("z", 1));

        TConstArrayRef<TCell> ref0(c0);
        TConstArrayRef<TCell> ref1(c1);
        TTableRange pt0(ref0);
        TTableRange pt1(ref1);
        TRangesBuilder builder(ydbPk, pkSchema);
        builder.AddRange(TSerializedTableRange(pt0));
        builder.AddRange(TSerializedTableRange(pt1));
        auto filter = builder.Finish();
        UNIT_ASSERT(!filter.IsSuccess());
        UNIT_ASSERT(strstr(filter.GetErrorMessage().c_str(), "not sorted sequence") != nullptr);
    }
}

}   // namespace NKikimr
