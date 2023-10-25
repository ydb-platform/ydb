#pragma once

#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/library/accessor/accessor.h>

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>

namespace NKikimr::NOlap {

    class TNormalizationContext {
        YDB_READONLY_DEF(TActorId, ResourceSubscribeActor);
        YDB_READONLY_DEF(TActorId, ColumnshardActor);

        const NOlap::NResourceBroker::NSubscribe::TTaskContext& InsertTaskSubscription;
        std::shared_ptr<IStoragesManager> StoragesManager;

    public:
        TNormalizationContext(const NOlap::NResourceBroker::NSubscribe::TTaskContext& its)
            : InsertTaskSubscription(its)
        {}

        IStoragesManager& GetStoragesManager() {
            AFL_VERIFY(!!StoragesManager);
            return *StoragesManager;
        }

        const NOlap::NResourceBroker::NSubscribe::TTaskContext& GetInsertTaskSubscription() const {
            return InsertTaskSubscription;
        }
    };

    enum class ENormalizerResult {
        Ok,
        Wait,
        Failed,
        Skip
    };

    class INormalizerComponent {
    public:
        using TPtr = std::shared_ptr<INormalizerComponent>;

        virtual ~INormalizerComponent() {}

        virtual bool NormalizationRequired() const {
            return false;
        }

        virtual const TString& GetName() const = 0;

        virtual ENormalizerResult NormalizeData(TNormalizationContext& nCtx, NTabletFlatExecutor::TTransactionContext& txc) = 0;
    };

}
