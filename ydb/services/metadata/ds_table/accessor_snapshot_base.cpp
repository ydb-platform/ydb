#include "accessor_snapshot_base.h"
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <library/cpp/actors/core/log.h>

#include <util/string/escape.h>

namespace NKikimr::NMetadata::NProvider {

void TDSAccessorBase::OnNewParsedSnapshot(Ydb::Table::ExecuteQueryResult&& /*qResult*/, NFetcher::ISnapshot::TPtr snapshot) {
    SnapshotConstructor->EnrichSnapshotData(snapshot, InternalController);
}

void TDSAccessorBase::OnIncorrectSnapshotFromYQL(const TString& errorMessage) {
    ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot parse current snapshot: " << errorMessage;
}

void TDSAccessorBase::Handle(TEvYQLResponse::TPtr& ev) {
    auto& currentFullReply = ev->Get()->GetResponse();
    Ydb::Table::ExecuteQueryResult qResult;
    currentFullReply.operation().result().UnpackTo(&qResult);
    Y_VERIFY((size_t)qResult.result_sets().size() == SnapshotConstructor->GetManagers().size());
    auto parsedSnapshot = SnapshotConstructor->ParseSnapshot(qResult, RequestedActuality);
    if (!parsedSnapshot) {
        OnIncorrectSnapshotFromYQL("snapshot is null after parsing");
    } else {
        OnNewParsedSnapshot(std::move(qResult), parsedSnapshot);
    }
}

void TDSAccessorBase::Handle(TEvEnrichSnapshotResult::TPtr& ev) {
    RequestedActuality = TInstant::Zero();
    OnNewEnrichedSnapshot(ev->Get()->GetEnrichedSnapshot());
}

void TDSAccessorBase::OnSnapshotEnrichingError(const TString& errorMessage) {
    ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot enrich current snapshot: " << errorMessage;
}

void TDSAccessorBase::Handle(TEvEnrichSnapshotProblem::TPtr& ev) {
    RequestedActuality = TInstant::Zero();
    OnSnapshotEnrichingError(ev->Get()->GetErrorText());
}

void TDSAccessorBase::StartSnapshotsFetching() {
    TStringBuilder sb;
    RequestedActuality = TInstant::Now();
    auto& managers = SnapshotConstructor->GetManagers();
    Y_VERIFY(managers.size());
    for (auto&& i : managers) {
        sb << "SELECT * FROM `" + EscapeC(i->GetStorageTablePath()) + "`;";
    }
    Register(new NRequest::TYQLQuerySessionedActor(sb, NACLib::TSystemUsers::Metadata(), Config, InternalController));
}

TDSAccessorBase::TDSAccessorBase(const NRequest::TConfig& config, NFetcher::ISnapshotsFetcher::TPtr snapshotConstructor)
    : Config(config)
    , SnapshotConstructor(snapshotConstructor)
{

}

void TDSAccessorBase::Bootstrap() {
    InternalController = std::make_shared<TRefreshInternalController>(SelfId());
    OnBootstrap();
}

}
