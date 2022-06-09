#include "change_collector.h"
#include "change_collector_async_index.h"
#include "change_collector_cdc_stream.h"
#include "datashard_impl.h"

#include <util/generic/vector.h>

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TChangeCollectorProxy: public IChangeCollector {
public:
    void AddUnderlying(THolder<IChangeCollector> collector) {
        Underlying.emplace_back(std::move(collector));
    }

    bool NeedToReadKeys() const override {
        for (const auto& collector : Underlying) {
            if (collector->NeedToReadKeys()) {
                return true;
            }
        }

        return false;
    }

    void SetReadVersion(const TRowVersion& readVersion) override {
        for (auto& collector : Underlying) {
            collector->SetReadVersion(readVersion);
        }
    }

    void SetWriteVersion(const TRowVersion& writeVersion) override {
        for (auto& collector : Underlying) {
            collector->SetWriteVersion(writeVersion);
        }
    }

    bool Collect(const TTableId& tableId, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates) override
    {
        for (auto& collector : Underlying) {
            if (!collector->Collect(tableId, rop, key, updates)) {
                return false;
            }
        }

        return true;
    }

    const TVector<TChange>& GetCollected() const override {
        CollectedBuf.clear();

        for (const auto& collector : Underlying) {
            const auto& collected = collector->GetCollected();
            std::copy(collected.begin(), collected.end(), std::back_inserter(CollectedBuf));
        }

        return CollectedBuf;
    }

    TVector<TChange>&& GetCollected() override {
        CollectedBuf.clear();

        for (auto& collector : Underlying) {
            auto collected = std::move(collector->GetCollected());
            std::move(collected.begin(), collected.end(), std::back_inserter(CollectedBuf));
        }

        return std::move(CollectedBuf);
    }

    void Reset() override {
        for (auto& collector : Underlying) {
            collector->Reset();
        }

        CollectedBuf.clear();
    }

private:
    TVector<THolder<IChangeCollector>> Underlying;
    mutable TVector<TChange> CollectedBuf;

}; // TChangeCollectorProxy

IChangeCollector* CreateChangeCollector(TDataShard& dataShard, NTable::TDatabase& db, const TUserTable& table, bool isImmediateTx) {
    const bool hasAsyncIndexes = table.HasAsyncIndexes();
    const bool hasCdcStreams = table.HasCdcStreams();

    if (!hasAsyncIndexes && !hasCdcStreams) {
        return nullptr;
    }

    auto proxy = MakeHolder<TChangeCollectorProxy>();

    if (hasAsyncIndexes) {
        proxy->AddUnderlying(MakeHolder<TAsyncIndexChangeCollector>(&dataShard, db, isImmediateTx));
    }

    if (hasCdcStreams) {
        proxy->AddUnderlying(MakeHolder<TCdcStreamChangeCollector>(&dataShard, db, isImmediateTx));
    }

    return proxy.Release();
}

IChangeCollector* CreateChangeCollector(TDataShard& dataShard, NTable::TDatabase& db, ui64 tableId, bool isImmediateTx) {
    Y_VERIFY(dataShard.GetUserTables().contains(tableId));
    const TUserTable& tableInfo = *dataShard.GetUserTables().at(tableId);
    return CreateChangeCollector(dataShard, db, tableInfo, isImmediateTx);
}

} // NDataShard
} // NKikimr
