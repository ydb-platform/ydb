#include "accessor_refresh.h"
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <library/cpp/actors/core/log.h>

#include <util/string/escape.h>

namespace NKikimr::NMetadataProvider {
using namespace NInternal::NRequest;

void TDSAccessorRefresher::Handle(TEvRequestResult<TDialogSelect>::TPtr& ev) {
    const TString startString = CurrentSelection.SerializeAsString();

    auto currentFullReply = ev->Get()->GetResult();
    Ydb::Table::ExecuteQueryResult qResult;
    currentFullReply.operation().result().UnpackTo(&qResult);
    Y_VERIFY((size_t)qResult.result_sets().size() == SnapshotConstructor->GetTables().size());
    *CurrentSelection.mutable_result_sets() = std::move(*qResult.mutable_result_sets());
    auto parsedSnapshot = SnapshotConstructor->ParseSnapshot(CurrentSelection, RequestedActuality);
    if (!parsedSnapshot) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot parse current snapshot";
    }

    if (!!parsedSnapshot && CurrentSelection.SerializeAsString() != startString) {
        ALS_INFO(NKikimrServices::METADATA_PROVIDER) << "New refresher data: " << CurrentSelection.DebugString();
        SnapshotConstructor->EnrichSnapshotData(parsedSnapshot, GetController());
    } else {
        Schedule(Config.GetRefreshPeriod(), new TEvRefresh());
    }
}

void TDSAccessorRefresher::Handle(TEvEnrichSnapshotResult::TPtr& ev) {
    RequestedActuality = TInstant::Zero();
    Schedule(Config.GetRefreshPeriod(), new TEvRefresh());
    CurrentSnapshot = ev->Get()->GetEnrichedSnapshot();
    OnSnapshotModified();
}

void TDSAccessorRefresher::Handle(TEvEnrichSnapshotProblem::TPtr& ev) {
    RequestedActuality = TInstant::Zero();
    Schedule(Config.GetRefreshPeriod(), new TEvRefresh());
    ALS_INFO(NKikimrServices::METADATA_PROVIDER) << "enrich problem: " << ev->Get()->GetErrorText();
}

void TDSAccessorRefresher::Handle(TEvRequestResult<TDialogCreateSession>::TPtr& ev) {
    Ydb::Table::CreateSessionResponse currentFullReply = ev->Get()->GetResult();
    Ydb::Table::CreateSessionResult session;
    currentFullReply.operation().result().UnpackTo(&session);
    const TString sessionId = session.session_id();
    Y_VERIFY(sessionId);
    Ydb::Table::ExecuteDataQueryRequest request;
    TStringBuilder sb;
    auto& tables = SnapshotConstructor->GetTables();
    Y_VERIFY(tables.size());
    for (auto&& i : tables) {
        sb << "SELECT * FROM `" + EscapeC(i) + "`;";
    }

    request.mutable_query()->set_yql_text(sb);
    request.set_session_id(sessionId);
    request.mutable_tx_control()->mutable_begin_tx()->mutable_snapshot_read_only();

    Register(new TYDBRequest<TDialogSelect>(std::move(request), SelfId(), Config.GetRequestConfig()));
}

void TDSAccessorRefresher::Handle(TEvRefresh::TPtr& /*ev*/) {
    Register(new TYDBRequest<TDialogCreateSession>(TDialogCreateSession::TRequest(), SelfId(), Config.GetRequestConfig()));
}

void TDSAccessorRefresher::OnInitialized() {
    Sender<TEvRefresh>().SendTo(SelfId());
}

TDSAccessorRefresher::TDSAccessorRefresher(const TConfig& config, ISnapshotParser::TPtr snapshotConstructor)
    : TBase(config.GetRequestConfig())
    , SnapshotConstructor(snapshotConstructor)
    , Config(config)
{

}

ISnapshotAcceptorController::TPtr TDSAccessorRefresher::GetController() const {
    if (!ControllerImpl) {
        ControllerImpl = std::make_shared<TSnapshotAcceptorController>(SelfId());
    }
    return ControllerImpl;
}

}
