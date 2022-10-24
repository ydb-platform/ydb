#include "accessor_refresh.h"
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <library/cpp/actors/core/log.h>

#include <util/string/escape.h>

namespace NKikimr::NMetadataProvider {
using namespace NInternal::NRequest;

bool TDSAccessorRefresher::Handle(TEvRequestResult<TDialogSelect>::TPtr& ev) {
    const TString startString = CurrentSelection.SerializeAsString();

    auto currentFullReply = ev->Get()->GetResult();
    Ydb::Table::ExecuteQueryResult qResult;
    currentFullReply.operation().result().UnpackTo(&qResult);
    Y_VERIFY(qResult.result_sets().size() == 1);
    CurrentSelection = qResult.result_sets()[0];
    auto parsedSnapshot = SnapshotConstructor->ParseSnapshot(CurrentSelection, RequestedActuality);
    if (!!parsedSnapshot) {
        CurrentSnapshot = parsedSnapshot;
    } else {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot parse current snapshot";
    }

    RequestedActuality = TInstant::Zero();
    Schedule(Config.GetRefreshPeriod(), new TEvRefresh());
    if (!!parsedSnapshot && CurrentSelection.SerializeAsString() != startString) {
        ALS_INFO(NKikimrServices::METADATA_PROVIDER) << "New refresher data: " << CurrentSelection.DebugString();
        OnSnapshotModified();
        return true;
    }
    return false;
}

void TDSAccessorRefresher::Handle(TEvRequestResult<TDialogCreateSession>::TPtr& ev) {
    Ydb::Table::CreateSessionResponse currentFullReply = ev->Get()->GetResult();
    Ydb::Table::CreateSessionResult session;
    currentFullReply.operation().result().UnpackTo(&session);
    const TString sessionId = session.session_id();
    Y_VERIFY(sessionId);
    Ydb::Table::ExecuteDataQueryRequest request;
    request.mutable_query()->set_yql_text("SELECT * FROM `" + EscapeC(SnapshotConstructor->GetTablePath()) + "`");
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
    : TBase(config, snapshotConstructor->GetTableSchema())
    , SnapshotConstructor(snapshotConstructor)
{
}

}
