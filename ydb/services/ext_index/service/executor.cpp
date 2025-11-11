#include "executor.h"

#include <ydb/services/ext_index/metadata/fetcher.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/ext_index/common/service.h>
#include "add_data.h"
#include "activation.h"
#include "deleting.h"

namespace NKikimr::NCSIndex {

void TExecutor::Handle(TEvAddData::TPtr& ev) {
    if (IndexesSnapshot) {
        std::vector<NMetadata::NCSIndex::TObject> indexes = IndexesSnapshot->GetIndexes(ev->Get()->GetTablePath());
        if (indexes.empty()) {
            ev->Get()->GetExternalController()->OnAllIndexesUpserted();
        } else {
            std::shared_ptr<TDataUpserter> upserter = std::make_shared<TDataUpserter>(std::move(indexes), ev->Get()->GetExternalController(), ev->Get()->GetData());
            upserter->Start(upserter);
        }
    } else {
        if (!DeferredEventsOnAddData.Add(*ev)) {
            ev->Get()->GetExternalController()->OnAllIndexesUpsertionFailed("too big queue for index construction");
        }
    }
}

class TIndexesController: public IActivationExternalController, public IDeletingExternalController {
public:
    virtual void OnActivationFailed(Ydb::StatusIds::StatusCode /*status*/, const TString& errorMessage, const TString& requestId) override {
        ALS_ERROR(NKikimrServices::EXT_INDEX) << "cannot activate index for " << requestId << ": " << errorMessage;
    }
    virtual void OnActivationSuccess(const TString& requestId) override {
        ALS_NOTICE(NKikimrServices::EXT_INDEX) << "index activated " << requestId;
    }
    virtual void OnDeletingFailed(Ydb::StatusIds::StatusCode /*status*/, const TString& errorMessage, const TString& requestId) override {
        ALS_ERROR(NKikimrServices::EXT_INDEX) << "cannot remove index for " << requestId << ": " << errorMessage;
    }
    virtual void OnDeletingSuccess(const TString& requestId) override {
        ALS_NOTICE(NKikimrServices::EXT_INDEX) << "index deleted " << requestId;
    }

};

void TExecutor::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    {
        auto snapshot = ev->Get()->GetSnapshotPtrAs<NMetadata::NCSIndex::TSnapshot>();
        if (snapshot) {
            IndexesSnapshot = snapshot;
            DeferredEventsOnAddData.ResendAll(SelfId());
            std::vector<NMetadata::NCSIndex::TObject> inactiveIndexes;
            std::vector<NMetadata::NCSIndex::TObject> deletingIndexes;
            IndexesSnapshot->GetObjectsForActivity(inactiveIndexes, deletingIndexes);
            auto controller = std::make_shared<TIndexesController>();
            for (auto&& i : inactiveIndexes) {
                auto activation = std::make_shared<TActivation>(i, controller, i.GetUniqueId(), Config);
                activation->Start(activation);
            }
            for (auto&& i : deletingIndexes) {
                auto deleting = std::make_shared<TDeleting>(i, controller, i.GetUniqueId(), Config);
                deleting->Start(deleting);
            }
            return;
        }
    }
    Y_ABORT_UNLESS(false, "unexpected snapshot");
}

void TExecutor::Bootstrap() {
    Become(&TExecutor::StateMain);
    Y_ABORT_UNLESS(NMetadata::NProvider::TServiceOperator::IsEnabled(), "metadata service not active");

    auto managerIndexes = std::make_shared<NMetadata::NCSIndex::TFetcher>();
    Sender<NMetadata::NProvider::TEvSubscribeExternal>(managerIndexes).SendTo(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()));
}

NActors::IActor* CreateService(const TConfig& config) {
    return new TExecutor(config);
}

}
