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

    void AddData(TString&& table, const NMiniKQL::TUnboxedValueBatch &data) {
        auto it = Batchers.find(table);
        if (it != Batchers.end()) {
            it->second->AddData(data);
            return;
        }

        auto& batcher = Batchers[std::move(table)] = CreateDataBatcher();
        batcher->AddData(data);
    }

    i64 BatchSize() const {
        i64 size = 0;
        for (auto& [_, batcher] : Batchers) {
            size += batcher->GetMemory();
        }
        return size;
    }

    virtual NKqp::IDataBatcherPtr CreateDataBatcher() = 0;
    virtual bool Flush() = 0;

    const TScheme& GetScheme() const {
        return Scheme;
    }

protected:
    const TActorId SelfId;
    const TScheme Scheme;

    std::map<TString, NKqp::IDataBatcherPtr> Batchers;
};


std::unique_ptr<ITableKindState> CreateRowTableState(const TActorId& selfId, TAutoPtr<NSchemeCache::TSchemeCacheNavigate>& result);

}
