#include "accessor_refresh.h"

namespace NKikimr::NMetadata::NProvider {

void TDSAccessorRefresher::OnBootstrap() {
    TBase::OnBootstrap();
    UnsafeBecome(&TDSAccessorRefresher::StateMain);
    Sender<TEvRefresh>().SendTo(SelfId());
}

void TDSAccessorRefresher::OnNewEnrichedSnapshot(NFetcher::ISnapshot::TPtr snapshot) {
    FetchingRequestIsRunning = false;
    Schedule(Config.GetRefreshPeriod(), new TEvRefresh());
    CurrentSnapshot = snapshot;
    *CurrentSelection.mutable_result_sets() = std::move(*ProposedProto.mutable_result_sets());
    OnSnapshotModified();
    OnSnapshotRefresh();
}

void TDSAccessorRefresher::OnNewParsedSnapshot(Ydb::Table::ExecuteQueryResult&& qResult, NFetcher::ISnapshot::TPtr snapshot) {
    *ProposedProto.mutable_result_sets() = std::move(*qResult.mutable_result_sets());
    if (!CurrentSnapshot || CurrentSelection.SerializeAsString() != ProposedProto.SerializeAsString()) {
        ALS_INFO(NKikimrServices::METADATA_PROVIDER) << "New refresher data: " << ProposedProto.DebugString();
        SnapshotConstructor->EnrichSnapshotData(snapshot, InternalController);
    } else {
        FetchingRequestIsRunning = false;
        CurrentSnapshot->SetActuality(GetRequestedActuality());
        OnSnapshotRefresh();
        Schedule(Config.GetRefreshPeriod(), new TEvRefresh());
    }
}

void TDSAccessorRefresher::OnConstructSnapshotError(const TString& errorMessage) {
    FetchingRequestIsRunning = false;
    TBase::OnConstructSnapshotError(errorMessage);
    Schedule(Config.GetRefreshPeriod(), new TEvRefresh());
}

void TDSAccessorRefresher::Handle(TEvRefresh::TPtr& /*ev*/) {
    if (!FetchingRequestIsRunning) {
        FetchingRequestIsRunning = true;
        TBase::StartSnapshotsFetching();
    }
}

}
