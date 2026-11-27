#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/columnshard/engines/predicate/filter.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/loaded_key_ranges.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

namespace {

using NOlap::TLoadedKeyRanges;

std::vector<NOlap::TNameTypeInfo> MakePk() {
    return {
        { "timestamp", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp) },
        { "resource_type", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) },
    };
}

std::shared_ptr<arrow::Schema> MakePkSchema() {
    return NArrow::MakeArrowSchema(MakePk()).ValueOrDie();
}

NArrow::TSimpleRow MakeKey(const ui64 timestamp, const TString& resourceType) {
    const auto schema = MakePkSchema();
    auto builders = NArrow::MakeBuilders(schema);
    UNIT_ASSERT(static_cast<arrow::TimestampBuilder&>(*builders[0]).Append(timestamp).ok());
    UNIT_ASSERT(static_cast<arrow::StringBuilder&>(*builders[1]).Append(resourceType.data(), resourceType.size()).ok());
    return NArrow::TSimpleRow(arrow::RecordBatch::Make(schema, 1, NArrow::Finish(std::move(builders))), 0);
}

struct TBorder {
    std::vector<TCell> Cells;
    bool Inclusive = true;
};

TCell MakeTimestamp(const ui64 timestamp) {
    return TCell::Make<ui64>(timestamp);
}

TCell MakeResourceType(const TString& resourceType) {
    return TCell(resourceType.data(), resourceType.size());
}

NOlap::TPKRangesFilter MakeRanges(const std::vector<std::pair<TBorder, TBorder>>& ranges) {
    NOlap::TRangesBuilder builder(MakePk(), MakePkSchema());
    for (const auto& [from, to] : ranges) {
        const TString fromBuf = TSerializedCellVec::Serialize(from.Cells);
        const TString toBuf = TSerializedCellVec::Serialize(to.Cells);
        builder.AddRange(TSerializedTableRange(fromBuf, toBuf, from.Inclusive, to.Inclusive));
    }
    auto result = builder.Finish();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetErrorMessage());
    return result.DetachResult();
}

NOlap::TPKRangesFilter MakeRange(const TBorder& from) {
    return MakeRanges({ { from, TBorder{ { MakeTimestamp(1000) } } } });
}

// every portion with the last key >= (100, "m") is loaded
TLoadedKeyRanges MakeLoadedFrom100M() {
    return TLoadedKeyRanges::BuildFrom(MakeKey(100, "m"));
}

}   // namespace

Y_UNIT_TEST_SUITE(TLoadedKeyRangesCovers) {
    Y_UNIT_TEST(EverythingLoadedCoversFullScan) {
        UNIT_ASSERT(TLoadedKeyRanges::BuildEverything().Covers(NOlap::TPKRangesFilter::BuildEmpty()));
    }

    Y_UNIT_TEST(NothingLoadedCoversNoRead) {
        UNIT_ASSERT(!TLoadedKeyRanges::BuildNothing().Covers(MakeRange(TBorder{ { MakeTimestamp(150) } })));
    }

    Y_UNIT_TEST(FullScan) {
        UNIT_ASSERT(!MakeLoadedFrom100M().Covers(NOlap::TPKRangesFilter::BuildEmpty()));
    }

    Y_UNIT_TEST(OpenLeftBorder) {
        UNIT_ASSERT(!MakeLoadedFrom100M().Covers(MakeRange(TBorder{})));
    }

    Y_UNIT_TEST(PrefixAboveBound) {
        UNIT_ASSERT(MakeLoadedFrom100M().Covers(MakeRange(TBorder{ { MakeTimestamp(150) } })));
    }

    Y_UNIT_TEST(PrefixBelowBound) {
        UNIT_ASSERT(!MakeLoadedFrom100M().Covers(MakeRange(TBorder{ { MakeTimestamp(50) } })));
    }

    Y_UNIT_TEST(FullKeyEqualToBound) {
        UNIT_ASSERT(MakeLoadedFrom100M().Covers(MakeRange(TBorder{ { MakeTimestamp(100), MakeResourceType("m") }, true })));
        UNIT_ASSERT(MakeLoadedFrom100M().Covers(MakeRange(TBorder{ { MakeTimestamp(100), MakeResourceType("m") }, false })));
    }

    Y_UNIT_TEST(FullKeyBelowBound) {
        UNIT_ASSERT(!MakeLoadedFrom100M().Covers(MakeRange(TBorder{ { MakeTimestamp(100), MakeResourceType("a") }, false })));
    }

    // timestamp >= 100 selects (100, "a"), which is below the bound
    Y_UNIT_TEST(PrefixEqualToBoundIncluded) {
        UNIT_ASSERT(!MakeLoadedFrom100M().Covers(MakeRange(TBorder{ { MakeTimestamp(100) }, true })));
    }

    Y_UNIT_TEST(PrefixEqualToBoundExcluded) {
        UNIT_ASSERT(MakeLoadedFrom100M().Covers(MakeRange(TBorder{ { MakeTimestamp(100) }, false })));
    }

    // NULL is below every value
    Y_UNIT_TEST(NullAfterPrefixEqualToBound) {
        UNIT_ASSERT(!MakeLoadedFrom100M().Covers(MakeRange(TBorder{ { MakeTimestamp(100), TCell() }, true })));
    }

    Y_UNIT_TEST(OneOfRangesBelowBound) {
        const auto ranges = MakeRanges({
            { TBorder{ { MakeTimestamp(50) } }, TBorder{ { MakeTimestamp(60) } } },
            { TBorder{ { MakeTimestamp(150) } }, TBorder{ { MakeTimestamp(160) } } },
        });
        UNIT_ASSERT(!MakeLoadedFrom100M().Covers(ranges));
    }
}

Y_UNIT_TEST_SUITE(TLoadedKeyRangesCoversFrom) {
    Y_UNIT_TEST(EverythingLoaded) {
        UNIT_ASSERT(TLoadedKeyRanges::BuildEverything().CoversFrom(MakeKey(50, "a")));
    }

    Y_UNIT_TEST(NothingLoaded) {
        UNIT_ASSERT(!TLoadedKeyRanges::BuildNothing().CoversFrom(MakeKey(150, "a")));
    }

    Y_UNIT_TEST(KeyAboveBound) {
        UNIT_ASSERT(MakeLoadedFrom100M().CoversFrom(MakeKey(100, "n")));
    }

    Y_UNIT_TEST(KeyEqualToBound) {
        UNIT_ASSERT(MakeLoadedFrom100M().CoversFrom(MakeKey(100, "m")));
    }

    Y_UNIT_TEST(KeyBelowBound) {
        UNIT_ASSERT(!MakeLoadedFrom100M().CoversFrom(MakeKey(100, "a")));
    }
}

}   // namespace NKikimr
