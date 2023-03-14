#include "executor.h"

#include <ydb/services/ext_index/metadata/fetcher.h>
#include <ydb/services/metadata/initializer/fetcher.h>
#include <ydb/services/metadata/initializer/manager.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/ext_index/common/service.h>
#include "add_data.h"
#include "activation.h"
#include "deleting.h"

namespace NKikimr::NCSIndex {

void TExecutor::Handle(TEvAddData::TPtr& ev) {
    if (CheckActivity()) {
        std::vector<NMetadata::NCSIndex::TObject> indexes = IndexesSnapshot->GetIndexes(ev->Get()->GetTablePath());
        std::shared_ptr<TDataUpserter> upserter = std::make_shared<TDataUpserter>(std::move(indexes), ev->Get()->GetExternalController(), ev->Get()->GetData());
        upserter->Start(upserter);
    } else {
        if (!DeferredEventsOnIntialization.Add(*ev)) {
            Send(ev->Sender, new TEvAddDataResult("too big queue for index construction"));
        }
    }
}

void TExecutor::Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr& /*ev*/) {
    ActivityState = EActivity::Active;
    if (IndexesSnapshot) {
        DeferredEventsOnIntialization.ResendAll(SelfId());
    }
}

class TIndexesController: public IActivationExternalController, public IDeletingExternalController {
public:
    virtual void OnActivationFailed(const TString& errorMessage, const TString& requestId) override {
        ALS_ERROR(NKikimrServices::EXT_INDEX) << "cannot activate index for " << requestId << ": " << errorMessage;
    }
    virtual void OnActivationSuccess(const TString& requestId) override {
        ALS_NOTICE(NKikimrServices::EXT_INDEX) << "index activated " << requestId;
    }
    virtual void OnDeletingFailed(const TString& errorMessage, const TString& requestId) override {
        ALS_ERROR(NKikimrServices::EXT_INDEX) << "cannot remove index for " << requestId << ": " << errorMessage;
    }
    virtual void OnDeletingSuccess(const TString& requestId) override {
        ALS_NOTICE(NKikimrServices::EXT_INDEX) << "index deleted " << requestId;
    }

};

void TExecutor::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    {
        auto snapshot = ev->Get()->GetSnapshotPtrAs<NMetadata::NInitializer::TSnapshot>();
        if (snapshot) {
            if (snapshot->HasComponent("ext_index")) {
                CheckActivity();
            }
            return;
        }
    }
    {
        auto snapshot = ev->Get()->GetSnapshotPtrAs<NMetadata::NCSIndex::TSnapshot>();
        if (snapshot) {
            IndexesSnapshot = snapshot;
            if (CheckActivity()) {
                DeferredEventsOnIntialization.ResendAll(SelfId());
            }
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
    Y_VERIFY(false, "unexpected snapshot");
}

void TExecutor::Bootstrap() {
    Become(&TExecutor::StateMain);
    auto managerInitializer = std::make_shared<NMetadata::NInitializer::TFetcher>();
    Y_VERIFY(NMetadata::NProvider::TServiceOperator::IsEnabled(), "metadata service not active");
    Sender<NMetadata::NProvider::TEvSubscribeExternal>(managerInitializer).SendTo(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()));

    auto managerIndexes = std::make_shared<NMetadata::NCSIndex::TFetcher>();
    Sender<NMetadata::NProvider::TEvSubscribeExternal>(managerIndexes).SendTo(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()));
}

bool TExecutor::CheckActivity() {
    switch (ActivityState) {
        case EActivity::Created:
            ActivityState = EActivity::Preparation;
            Sender<NMetadata::NProvider::TEvPrepareManager>(NMetadata::NCSIndex::TObject::GetBehaviour()).SendTo(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()));
            break;
        case EActivity::Preparation:
            break;
        case EActivity::Active:
            if (!IndexesSnapshot) {
                return false;
            }
            return true;
    }
    return false;
}

NActors::IActor* CreateService(const TConfig& config) {
    return new TExecutor(config);
}

}
