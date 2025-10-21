#pragma once

#include "scheme.h"

#include <ydb/core/kqp/runtime/kqp_write_table.h>

namespace NKikimr::NReplication::NTransfer {

class ITableKindState {
public:
    using TPtr = std::unique_ptr<ITableKindState>;

    ITableKindState(const TActorId& selfId, const TAutoPtr<NSchemeCache::TSchemeCacheNavigate>& result)
        : SelfId(selfId)
        , Scheme(BuildScheme(result))
    {}

    virtual ~ITableKindState() = default;

    [[nodiscard]] bool AddData(TString&& table, const NMiniKQL::TUnboxedValueBatch& data, size_t estimateSize) {
        auto& batcher = Batchers[std::move(table)];
        if (!batcher) {
            batcher = CreateDataBatcher();
        }

        static constexpr size_t MaxBatchSize = 512_MB; // error on 2147483646

        if (batcher->GetMemory() + estimateSize > MaxBatchSize) {
            return false;
        }
        batcher->AddData(data);
        return true;
    }

    ui64 BatchSize() const {
        ui64 size = 0;
        for (auto& [_, batcher] : Batchers) {
            size += std::max<i64>(0, batcher->GetMemory());
        }
        return size;
    }

    virtual NKqp::IDataBatcherPtr CreateDataBatcher() = 0;
    virtual bool Flush() = 0;

    const TScheme::TPtr GetScheme() const {
        return Scheme;
    }

    void PassAway() {
        if (UploaderActorId) {
            TActivationContext::AsActorContext().Send(UploaderActorId, new TEvents::TEvPoison());
        }
    }

protected:
    const TActorId SelfId;
    const TScheme::TPtr Scheme;

    std::map<TString, NKqp::IDataBatcherPtr> Batchers;
    TActorId UploaderActorId;
};


std::unique_ptr<ITableKindState> CreateColumnTableState(const TActorId& selfId, TAutoPtr<NSchemeCache::TSchemeCacheNavigate>& result);
std::unique_ptr<ITableKindState> CreateRowTableState(const TActorId& selfId, TAutoPtr<NSchemeCache::TSchemeCacheNavigate>& result);

}
