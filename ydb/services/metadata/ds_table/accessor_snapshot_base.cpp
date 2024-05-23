#include "accessor_snapshot_base.h"
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <ydb/library/actors/core/log.h>

#include <util/string/escape.h>

namespace NKikimr::NMetadata::NProvider {

void TDSAccessorBase::OnNewParsedSnapshot(Ydb::Table::ExecuteQueryResult&& /*qResult*/, NFetcher::ISnapshot::TPtr snapshot) {
    SnapshotConstructor->EnrichSnapshotData(snapshot, InternalController);
}

void TDSAccessorBase::OnConstructSnapshotError(const TString& errorMessage) {
    ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot construct snapshot: " << errorMessage;
}

void TDSAccessorBase::Handle(NRequest::TEvRequestFailed::TPtr& ev) {
    OnConstructSnapshotError("on request failed: " + ev->Get()->GetErrorMessage());
}

void TDSAccessorBase::Handle(NRequest::TEvRequestResult<NRequest::TDialogYQLRequest>::TPtr& ev) {
    auto& currentFullReply = ev->Get()->GetResult();
    Ydb::Table::ExecuteQueryResult qResult;
    currentFullReply.operation().result().UnpackTo(&qResult);

    auto& managers = SnapshotConstructor->GetManagers();
    Y_ABORT_UNLESS(managers.size());
    Ydb::Table::ExecuteQueryResult qResultFull;
    ui32 replyIdx = 0;
    for (auto&& i : managers) {
        auto it = CurrentExistence.find(i->GetStorageTablePath());
        Y_ABORT_UNLESS(it != CurrentExistence.end());
        Y_ABORT_UNLESS(it->second);
        if (it->second == 1) {
            *qResultFull.add_result_sets() = std::move(qResult.result_sets()[replyIdx]);
            ++replyIdx;
        } else {
            qResultFull.add_result_sets();
        }
    }
    Y_ABORT_UNLESS((int)replyIdx == qResult.result_sets().size());
    Y_ABORT_UNLESS((size_t)qResultFull.result_sets().size() == SnapshotConstructor->GetManagers().size());
    auto parsedSnapshot = SnapshotConstructor->ParseSnapshot(qResultFull, RequestedActuality);
    if (!parsedSnapshot) {
        OnConstructSnapshotError("snapshot is null after parsing");
    } else {
        OnNewParsedSnapshot(std::move(qResultFull), parsedSnapshot);
    }
}

void TDSAccessorBase::Handle(TEvEnrichSnapshotResult::TPtr& ev) {
    RequestedActuality = TInstant::Zero();
    OnNewEnrichedSnapshot(ev->Get()->GetEnrichedSnapshot());
}

void TDSAccessorBase::Handle(TEvEnrichSnapshotProblem::TPtr& ev) {
    RequestedActuality = TInstant::Zero();
    OnConstructSnapshotError("on enrich: " + ev->Get()->GetErrorText());
}

void TDSAccessorBase::Handle(TEvRecheckExistence::TPtr& ev) {
    Register(new TTableExistsActor(InternalController, ev->Get()->GetPath(), TDuration::Seconds(5)));
}

void TDSAccessorBase::Handle(TTableExistsActor::TEvController::TEvError::TPtr& ev) {
    AFL_ERROR(NKikimrServices::METADATA_PROVIDER)("action", "cannot detect path existence")("path", ev->Get()->GetPath())("error", ev->Get()->GetErrorMessage());
    Schedule(TDuration::Seconds(1), new TEvRecheckExistence(ev->Get()->GetPath()));
}

void TDSAccessorBase::Handle(TTableExistsActor::TEvController::TEvResult::TPtr& ev) {
    auto it = ExistenceChecks.find(ev->Get()->GetTablePath());
    Y_ABORT_UNLESS(it != ExistenceChecks.end());
    if (ev->Get()->IsTableExists()) {
        it->second = 1;
    } else {
        it->second = -1;
    }
    bool hasExists = false;
    for (auto&& i : ExistenceChecks) {
        if (i.second == 0) {
            return;
        }
        if (i.second == 1) {
            hasExists = true;
        }
    }
    if (!hasExists) {
        OnNewParsedSnapshot(Ydb::Table::ExecuteQueryResult(), SnapshotConstructor->CreateEmpty(RequestedActuality));
    } else {
        StartSnapshotsFetchingImpl();
    }
}

void TDSAccessorBase::StartSnapshotsFetching() {
    RequestedActuality = TInstant::Now();
    auto& managers = SnapshotConstructor->GetManagers();
    Y_ABORT_UNLESS(managers.size());
    bool hasExistsCheckers = false;
    for (auto&& i : managers) {
        auto it = ExistenceChecks.find(i->GetStorageTablePath());
        if (it == ExistenceChecks.end() || it->second == -1) {
            Register(new TTableExistsActor(InternalController, i->GetStorageTablePath(), TDuration::Seconds(5)));
            hasExistsCheckers = true;
            ExistenceChecks[i->GetStorageTablePath()] = 0;
        } else if (it->second == 0) {
            hasExistsCheckers = true;
        }
    }
    if (!hasExistsCheckers) {
        StartSnapshotsFetchingImpl();
    }
}

void TDSAccessorBase::StartSnapshotsFetchingImpl() {
    RequestedActuality = TInstant::Now();
    auto& managers = SnapshotConstructor->GetManagers();
    Y_ABORT_UNLESS(managers.size());
    CurrentExistence = ExistenceChecks;
    TStringBuilder sb;
    for (auto&& i : managers) {
        auto it = CurrentExistence.find(i->GetStorageTablePath());
        Y_ABORT_UNLESS(it != CurrentExistence.end());
        Y_ABORT_UNLESS(it->second);
        if (it->second == 1) {
            sb << "SELECT * FROM `" + EscapeC(i->GetStorageTablePath()) + "`;" << Endl;
        }
    }
    NRequest::TYQLRequestExecutor::Execute(sb, NACLib::TSystemUsers::Metadata(), InternalController, true);
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
