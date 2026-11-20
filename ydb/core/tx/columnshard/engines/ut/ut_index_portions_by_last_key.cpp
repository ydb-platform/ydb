#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/index_portions_by_last_key.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/join.h>

namespace NKikimr::NOlap {

namespace {

struct TBatch {
    std::vector<ui64> PortionIds;
    std::optional<TOwnedCellVec> LastKey;
};

TOwnedCellVec MakeLastKey(const ui64 ts, const TString& id) {
    return TOwnedCellVec::Make({ TCell::Make(ts), TCell(id.data(), id.size()) });
}

void AssertLastKey(const std::optional<TOwnedCellVec>& key, const ui64 ts, const TString& id) {
    UNIT_ASSERT(key);
    UNIT_ASSERT_VALUES_EQUAL(key->size(), 2);
    UNIT_ASSERT_VALUES_EQUAL((*key)[0].AsValue<ui64>(), ts);
    UNIT_ASSERT_VALUES_EQUAL((*key)[1].AsBuf(), id);
}

// last key (ts Uint64, id Utf8)
class TTestIndex {
private:
    NTable::NTest::TDbExec Db;

    NTable::TDatabase& Database() {
        return *Db.operator->();
    }

public:
    TTestIndex() {
        Db.Begin();
        TIndexPortionsByLastKey::Create(
            Database(), { { "ts", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64) }, { "id", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } });
        Db.Commit();
    }

    void Add(const ui64 pathId, const ui64 ts, const TString& id, const ui64 portionId) {
        const TVector<TRawTypeValue> key = {
            TRawTypeValue(&pathId, sizeof(pathId), NScheme::NTypeIds::Uint64),
            TRawTypeValue(&ts, sizeof(ts), NScheme::NTypeIds::Uint64),
            TRawTypeValue(id.data(), id.size(), NScheme::NTypeIds::Utf8),
            TRawTypeValue(&portionId, sizeof(portionId), NScheme::NTypeIds::Uint64),
        };
        Db.Begin();
        Db->Update(NColumnShard::Schema::PortionsByLastKeyTableId, NTable::ERowOp::Upsert, key, {});
        Db.Commit();
    }

    TBatch Read(const ui64 pathId, const std::optional<TOwnedCellVec>& below, const ui32 rowsLimit) {
        TBatch batch;
        Db.Begin();
        UNIT_ASSERT(TIndexPortionsByLastKey::ReadBatch(
            Database(), TInternalPathId::FromRawValue(pathId), below, rowsLimit, batch.PortionIds, batch.LastKey));
        Db.Commit();
        return batch;
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TIndexPortionsByLastKeyReadBatch) {
    Y_UNIT_TEST(FromTopToEnd) {
        TTestIndex index;
        index.Add(1, 10, "a", 3);
        index.Add(1, 30, "a", 1);
        index.Add(1, 20, "a", 2);

        const TBatch batch = index.Read(1, std::nullopt, 10);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", batch.PortionIds), "1,2,3");
        UNIT_ASSERT(!batch.LastKey);
    }

    Y_UNIT_TEST(LimitReachedAtTheEnd) {
        TTestIndex index;
        index.Add(1, 20, "a", 1);
        index.Add(1, 10, "a", 2);

        const TBatch batch = index.Read(1, std::nullopt, 2);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", batch.PortionIds), "1,2");
        UNIT_ASSERT(!batch.LastKey);
    }

    Y_UNIT_TEST(BatchEndsAtKeyBoundary) {
        TTestIndex index;
        index.Add(1, 30, "b", 1);
        index.Add(1, 30, "a", 2);
        index.Add(1, 30, "a", 3);
        index.Add(1, 30, "a", 4);
        index.Add(1, 20, "a", 5);

        const TBatch batch = index.Read(1, std::nullopt, 2);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", batch.PortionIds), "1,4,3,2");
        AssertLastKey(batch.LastKey, 30, "a");
    }

    Y_UNIT_TEST(NextBatchStartsBelowLastKey) {
        TTestIndex index;
        index.Add(1, 30, "b", 1);
        index.Add(1, 30, "a", 2);
        index.Add(1, 30, "a", 3);
        index.Add(1, 20, "a", 4);

        const TBatch batch = index.Read(1, MakeLastKey(30, "a"), 2);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", batch.PortionIds), "4");
        UNIT_ASSERT(!batch.LastKey);
    }

    Y_UNIT_TEST(OtherPathIdIsNotRead) {
        TTestIndex index;
        index.Add(1, 99, "a", 1);
        index.Add(2, 30, "a", 2);
        index.Add(2, 10, "a", 3);
        index.Add(3, 1, "a", 4);

        const TBatch fromTop = index.Read(2, std::nullopt, 10);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", fromTop.PortionIds), "2,3");
        UNIT_ASSERT(!fromTop.LastKey);

        const TBatch belowLowestKey = index.Read(2, MakeLastKey(10, "a"), 10);
        UNIT_ASSERT(belowLowestKey.PortionIds.empty());
        UNIT_ASSERT(!belowLowestKey.LastKey);
    }
}

}   // namespace NKikimr::NOlap
