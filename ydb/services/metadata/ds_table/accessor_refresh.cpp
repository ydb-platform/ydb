#include "accessor_refresh.h"
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <library/cpp/actors/core/log.h>

#include <util/string/escape.h>

namespace NKikimr::NMetadata::NProvider {

void TDSAccessorRefresher::Handle(TEvYQLResponse::TPtr& ev) {
    auto& currentFullReply = ev->Get()->GetResponse();
    Ydb::Table::ExecuteQueryResult qResult;
    currentFullReply.operation().result().UnpackTo(&qResult);
    Y_VERIFY((size_t)qResult.result_sets().size() == SnapshotConstructor->GetManagers().size());
    *ProposedProto.mutable_result_sets() = std::move(*qResult.mutable_result_sets());
    auto parsedSnapshot = SnapshotConstructor->ParseSnapshot(ProposedProto, RequestedActuality);
    if (!parsedSnapshot) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot parse current snapshot";
    }

    if (!!parsedSnapshot && CurrentSelection.SerializeAsString() != ProposedProto.SerializeAsString()) {
        ALS_INFO(NKikimrServices::METADATA_PROVIDER) << "New refresher data: " << ProposedProto.DebugString();
        SnapshotConstructor->EnrichSnapshotData(parsedSnapshot, InternalController);
    } else {
        CurrentSnapshot->SetActuality(RequestedActuality);
        OnSnapshotRefresh();
        Schedule(Config.GetRefreshPeriod(), new TEvRefresh());
    }
}

void TDSAccessorRefresher::Handle(TEvEnrichSnapshotResult::TPtr& ev) {
    RequestedActuality = TInstant::Zero();
    Schedule(Config.GetRefreshPeriod(), new TEvRefresh());
    CurrentSnapshot = ev->Get()->GetEnrichedSnapshot();
    *CurrentSelection.mutable_result_sets() = std::move(*ProposedProto.mutable_result_sets());
    OnSnapshotModified();
    OnSnapshotRefresh();
}

void TDSAccessorRefresher::Handle(TEvEnrichSnapshotProblem::TPtr& ev) {
    RequestedActuality = TInstant::Zero();
    Schedule(Config.GetRefreshPeriod(), new TEvRefresh());
    ALS_INFO(NKikimrServices::METADATA_PROVIDER) << "enrich problem: " << ev->Get()->GetErrorText();
}

void TDSAccessorRefresher::Handle(TEvRefresh::TPtr& /*ev*/) {
    TStringBuilder sb;
    RequestedActuality = TInstant::Now();
    auto& managers = SnapshotConstructor->GetManagers();
    Y_VERIFY(managers.size());
    for (auto&& i : managers) {
        sb << "SELECT * FROM `" + EscapeC(i->GetStorageTablePath()) + "`;";
    }
    Register(new NRequest::TYQLQuerySessionedActor(sb, NACLib::TSystemUsers::Metadata(), Config.GetRequestConfig(), InternalController));
}

TDSAccessorRefresher::TDSAccessorRefresher(const TConfig& config, NFetcher::ISnapshotsFetcher::TPtr snapshotConstructor)
    : SnapshotConstructor(snapshotConstructor)
    , Config(config)
{

}

void TDSAccessorRefresher::Bootstrap() {
    RegisterState();
    InternalController = std::make_shared<TRefreshInternalController>(SelfId());
    Sender<TEvRefresh>().SendTo(SelfId());
}

}
