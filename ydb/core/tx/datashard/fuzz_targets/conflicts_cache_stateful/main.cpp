#include <ydb/core/tx/datashard/conflicts_cache.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/system/yassert.h>

namespace {

using namespace NKikimr;
using namespace NKikimr::NDataShard;
using namespace NKikimr::NTable;

constexpr ui32 TableId = 1;
constexpr size_t KeySpace = 24;
constexpr size_t MaxSteps = 220;

TAlter MakeAlter() {
    TAlter alter;
    alter
        .AddTable("conflicts", TableId)
        .AddColumn(TableId, "key", 1, NScheme::NTypeIds::Uint64, false, false)
        .AddColumn(TableId, "value", 2, NScheme::NTypeIds::Uint64, false, false)
        .AddColumnToKey(TableId, 1);
    return alter;
}

TOwnedCellVec MakeKey(ui64 key) {
    TCell cell = TCell::Make(key);
    return TOwnedCellVec(TConstArrayRef<TCell>(&cell, 1));
}

struct TModel {
    THashMap<ui64, THashSet<ui64>> DistributedByKey;
    THashMap<ui64, THashSet<ui64>> UncommittedByKey;

    TModel Snapshot() const {
        return *this;
    }

    void Register(ui64 distTx, ui64 key) {
        DistributedByKey[key].insert(distTx);
    }

    void Unregister(ui64 distTx) {
        TVector<ui64> keysToDrop;
        for (auto& [key, txs] : DistributedByKey) {
            txs.erase(distTx);
            if (txs.empty()) {
                keysToDrop.push_back(key);
            }
        }
        for (ui64 key : keysToDrop) {
            DistributedByKey.erase(key);
            UncommittedByKey.erase(key);
        }
    }

    void AddUncommitted(ui64 txId, ui64 key) {
        if (DistributedByKey.contains(key)) {
            UncommittedByKey[key].insert(txId);
        }
    }

    void RemoveKey(ui64 key) {
        UncommittedByKey[key].clear();
    }

    void RemoveTx(ui64 txId) {
        for (auto& [_, txs] : UncommittedByKey) {
            txs.erase(txId);
        }
    }

    void RemoveAll() {
        for (auto& [_, txs] : UncommittedByKey) {
            txs.clear();
        }
    }

    THashSet<ui64> Expected(ui64 key) const {
        if (!DistributedByKey.contains(key)) {
            return {};
        }
        auto it = UncommittedByKey.find(key);
        return it == UncommittedByKey.end() ? THashSet<ui64>{} : it->second;
    }
};

void Check(TTableConflictsCache& cache, const TModel& model) {
    for (ui64 key = 0; key < KeySpace; ++key) {
        auto expected = model.Expected(key);
        const auto ownedKey = MakeKey(key);
        const auto* actual = cache.FindUncommittedWrites(ownedKey);
        if (expected.empty()) {
            Y_ABORT_UNLESS(actual == nullptr || actual->empty());
            continue;
        }
        Y_ABORT_UNLESS(actual != nullptr);
        Y_ABORT_UNLESS(actual->size() == expected.size());
        for (ui64 txId : expected) {
            Y_ABORT_UNLESS(actual->contains(txId));
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    NTable::NTest::TDbExec db;
    db.To(1).Begin().Apply(*MakeAlter().Flush()).Commit();
    NTable::TDatabase& rawDb = *db.operator->();

    TTableConflictsCache cache(TableId);
    TModel model;
    std::optional<TModel> beforeRollbackableChange;
    bool inTx = false;
    ui64 step = 2;

    const auto begin = [&]() {
        if (!inTx) {
            db.To(step++).Begin();
            beforeRollbackableChange = model.Snapshot();
            inTx = true;
        }
    };
    const auto finish = [&](bool commit) {
        if (!inTx) {
            return;
        }
        if (commit) {
            db.Commit();
        } else {
            db.Reject();
            model = *beforeRollbackableChange;
        }
        beforeRollbackableChange.reset();
        inTx = false;
    };

    const size_t steps = provider.ConsumeIntegralInRange<size_t>(0, MaxSteps);
    for (size_t i = 0; i < steps && provider.remaining_bytes() > 0; ++i) {
        const ui64 key = provider.ConsumeIntegralInRange<ui64>(0, KeySpace - 1);
        const ui64 txId = provider.ConsumeIntegralInRange<ui64>(1, 32);
        switch (provider.ConsumeIntegralInRange<unsigned>(0, 7)) {
            case 0: {
                begin();
                const auto ownedKey = MakeKey(key);
                const bool registered = cache.RegisterDistributedWrite(txId, ownedKey, rawDb);
                Y_ABORT_UNLESS(registered);
                model.Register(txId, key);
                break;
            }
            case 1:
                finish(true);
                cache.UnregisterDistributedWrites(txId);
                model.Unregister(txId);
                break;
            case 2: {
                begin();
                const auto ownedKey = MakeKey(key);
                cache.AddUncommittedWrite(ownedKey, txId, rawDb);
                model.AddUncommitted(txId, key);
                break;
            }
            case 3: {
                begin();
                const auto ownedKey = MakeKey(key);
                cache.RemoveUncommittedWrites(ownedKey, rawDb);
                model.RemoveKey(key);
                break;
            }
            case 4:
                begin();
                cache.RemoveUncommittedWrites(txId, rawDb);
                model.RemoveTx(txId);
                break;
            case 5:
                begin();
                cache.RemoveAllUncommittedWrites(rawDb);
                model.RemoveAll();
                break;
            case 6:
                finish(true);
                break;
            case 7:
                finish(false);
                break;
        }
        Check(cache, model);
    }

    finish(true);
    Check(cache, model);

    return 0;
}
