#include "table_creator.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/retry/retry_policy.h>

#include <util/generic/guid.h>
#include <util/generic/utility.h>
#include <util/random/random.h>

namespace NKikimr {

namespace {

class TTableCreator : public NActors::TActorBootstrapped<TTableCreator> {

using TTableCreatorRetryPolicy = IRetryPolicy<bool>;

public:
    TTableCreator(
        TVector<TString> pathComponents,
        TVector<NKikimrSchemeOp::TColumnDescription> columns,
        TVector<TString> keyColumns,
        NKikimrServices::EServiceKikimr logService,
        TMaybe<NKikimrSchemeOp::TTTLSettings> ttlSettings = Nothing())
        : PathComponents(std::move(pathComponents))
        , Columns(std::move(columns))
        , KeyColumns(std::move(keyColumns))
        , LogService(logService)
        , TtlSettings(std::move(ttlSettings))
        , LogPrefix("Table " + TableName() + " updater. ")
    {
        Y_ABORT_UNLESS(!PathComponents.empty());
        Y_ABORT_UNLESS(!Columns.empty());
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TTableCreator>::Registered(sys, owner);
        Owner = owner;
    }

    STRICT_STFUNC(StateFuncCheck,
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        sFunc(NActors::TEvents::TEvWakeup, CheckTableExistence);
        hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
    )

    STRICT_STFUNC(StateFuncUpgrade,
        hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
        hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
        sFunc(NActors::TEvents::TEvWakeup, RunTableRequest);
        hFunc(TEvTabletPipe::TEvClientConnected, Handle);
        hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
    )

    void Bootstrap() {
        Become(&TTableCreator::StateFuncCheck);
        CheckTableExistence();
    }

    void CheckTableExistence() {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        auto pathComponents = SplitPath(AppData()->TenantName);
        request->DatabaseName = CanonizePath(pathComponents);
        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        pathComponents.insert(pathComponents.end(), PathComponents.begin(), PathComponents.end());
        entry.Path = pathComponents;
        entry.ShowPrivatePath = true;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void RunTableRequest() {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        NKikimrSchemeOp::TModifyScheme& modifyScheme = *request->Record.MutableTransaction()->MutableModifyScheme();
        auto pathComponents = SplitPath(AppData()->TenantName);
        for (size_t i = 0; i < PathComponents.size() - 1; ++i) {
            pathComponents.emplace_back(PathComponents[i]);
        }
        modifyScheme.SetWorkingDir(CanonizePath(pathComponents));
        LOG_DEBUG_S(*TlsActivationContext, LogService,
            LogPrefix << "Full table path:" << modifyScheme.GetWorkingDir() << "/" << TableName());
        modifyScheme.SetOperationType(OperationType);
        modifyScheme.SetInternal(true);
        modifyScheme.SetAllowAccessToPrivatePaths(true);
        NKikimrSchemeOp::TTableDescription* tableDesc;
        if (OperationType == NKikimrSchemeOp::ESchemeOpCreateTable) {
            tableDesc = modifyScheme.MutableCreateTable();
            for (const TString& k : KeyColumns) {
                tableDesc->AddKeyColumnNames(k);
            }
        } else {
            Y_DEBUG_ABORT_UNLESS(OperationType == NKikimrSchemeOp::ESchemeOpAlterTable);
            tableDesc = modifyScheme.MutableAlterTable();
        }
        tableDesc->SetName(TableName());
        for (const NKikimrSchemeOp::TColumnDescription& col : Columns) {
            *tableDesc->AddColumns() = col;
        }
        if (TtlSettings) {
            tableDesc->MutableTTLSettings()->CopyFrom(*TtlSettings);
        }
        Send(MakeTxProxyID(), std::move(request));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;
        const NSchemeCache::TSchemeCacheNavigate& request = *ev->Get()->Request;
        Y_ABORT_UNLESS(request.ResultSet.size() == 1);
        const NSchemeCache::TSchemeCacheNavigate::TEntry& result  = request.ResultSet[0];
        if (result.Status != EStatus::Ok) {
            LOG_DEBUG_S(*TlsActivationContext, LogService,
                LogPrefix << "Describe result: " << result.Status);
        }

        switch (result.Status) {
            case EStatus::Unknown:
                [[fallthrough]];
            case EStatus::PathNotTable:
                [[fallthrough]];
            case EStatus::PathNotPath:
                [[fallthrough]];
            case EStatus::AccessDenied:
                [[fallthrough]];
            case EStatus::RedirectLookupError:
                Fail(result.Status);
                break;
            case EStatus::RootUnknown:
                [[fallthrough]];
            case EStatus::PathErrorUnknown:
                Become(&TTableCreator::StateFuncUpgrade);
                OperationType = NKikimrSchemeOp::ESchemeOpCreateTable;
                LOG_NOTICE_S(*TlsActivationContext, LogService, LogPrefix << "Creating table");
                RunTableRequest();
                break;
            case EStatus::LookupError:
                [[fallthrough]];
            case EStatus::TableCreationNotComplete:
                Retry();
                break;
            case EStatus::Ok:
                ExcludeExistingColumns(result.Columns);
                if (!Columns.empty()) {
                    OperationType = NKikimrSchemeOp::ESchemeOpAlterTable;
                    Become(&TTableCreator::StateFuncUpgrade);
                    RunTableRequest();
                } else {
                    Success();
                }
                break;
        }
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        LOG_DEBUG_S(*TlsActivationContext, LogService,
            LogPrefix << "TEvProposeTransactionStatus: " << ev->Get()->Record);
        const auto ssStatus = ev->Get()->Record.GetSchemeShardStatus();
        switch (ev->Get()->Status()) {
            case NTxProxy::TResultStatus::ExecComplete:
                [[fallthrough]];
            case NTxProxy::TResultStatus::ExecAlready:
                if (ssStatus == NKikimrScheme::EStatus::StatusSuccess || ssStatus == NKikimrScheme::EStatus::StatusAlreadyExists) {
                    Success(ev);
                } else {
                    Fail(ev);
                }
                break;
            case NTxProxy::TResultStatus::ProxyShardNotAvailable:
                Retry();
                break;
            case NTxProxy::TResultStatus::ExecError:
                if (ssStatus == NKikimrScheme::EStatus::StatusMultipleModifications) {
                    SubscribeOnTransactionOrFallback(ev);
                // In the process of creating a database, errors of the form may occur -
                // database doesn't have storage pools at all to create tablet
                // channels to storage pool binding by profile id
                // Also, this status is returned when column types mismatch - 
                // need to fallback to rebuild column diff
                } else if (ssStatus == NKikimrScheme::EStatus::StatusInvalidParameter) {
                    FallBack(true /* long delay */);
                } else {
                    Fail(ev);
                }
                break;
            case NTxProxy::TResultStatus::ExecInProgress:
                SubscribeOnTransactionOrFallback(ev);
                break;
            default:
                Fail(ev);
        }
    }

    void Retry(bool longDelay = false) {
        auto delay = GetRetryDelay(longDelay);
        if (delay) {
            Schedule(*delay, new NActors::TEvents::TEvWakeup());
        } else {
            Fail();
        }
    }

    void FallBack(bool longDelay = false) {
        if (SchemePipeActorId){
            PipeClientClosedByUs = true;
            NTabletPipe::CloseClient(SelfId(), SchemePipeActorId);
        }
        Become(&TTableCreator::StateFuncCheck);
        Retry(longDelay);
    }

    void SubscribeOnTransactionOrFallback(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const ui64 txId = ev->Get()->Status() == NTxProxy::TResultStatus::ExecInProgress ? ev->Get()->Record.GetTxId() : ev->Get()->Record.GetPathCreateTxId();
        if (txId == 0) {
            LOG_DEBUG_S(*TlsActivationContext, LogService,
                LogPrefix << "Unable to subscribe to concurrent transaction, falling back");
            FallBack();
            return;
        }
        PipeClientClosedByUs = false;
        NActors::IActor* pipeActor = NTabletPipe::CreateClient(SelfId(), ev->Get()->Record.GetSchemeShardTabletId());
        Y_ABORT_UNLESS(pipeActor);
        SchemePipeActorId = Register(pipeActor);
        auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
        request->Record.SetTxId(txId);
        NTabletPipe::SendData(SelfId(), SchemePipeActorId, std::move(request));
        LOG_DEBUG_S(*TlsActivationContext, LogService,
            LogPrefix << "Subscribe on create table tx: " << txId);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            LOG_ERROR_S(*TlsActivationContext, LogService,
                LogPrefix << "Request: " << GetOperationType() << ". Tablet to pipe not connected: " << NKikimrProto::EReplyStatus_Name(ev->Get()->Status) << ", retry");
            PipeClientClosedByUs = true;
            NTabletPipe::CloseClient(SelfId(), SchemePipeActorId);
            SchemePipeActorId = {};
            Retry();
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        SchemePipeActorId = {};
        if (!PipeClientClosedByUs) {
            LOG_ERROR_S(*TlsActivationContext, LogService,
                LogPrefix << "Request: " << GetOperationType() << ". Tablet to pipe destroyed, retry");
            Retry();
        }
        PipeClientClosedByUs = false;
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&) {
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        LOG_DEBUG_S(*TlsActivationContext, LogService,
            LogPrefix << "Request: " << GetOperationType() << ". Transaction completed: " << ev->Get()->Record.GetTxId() << ". Doublechecking...");
        FallBack();
    }

    void Fail(NSchemeCache::TSchemeCacheNavigate::EStatus status) {
        LOG_ERROR_S(*TlsActivationContext, LogService,
            LogPrefix << "Failed to upgrade table: " << status);
        Reply();
    }

    void Fail(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        LOG_ERROR_S(*TlsActivationContext, LogService,
            LogPrefix << "Failed " << GetOperationType() << " request: " << ev->Get()->Status() << ". Response: " << ev->Get()->Record);
        Reply();
    }

    void Fail() {
        LOG_ERROR_S(*TlsActivationContext, LogService, LogPrefix << "Retry limit exceeded");
        Reply();
    }

    void Success() {
        Reply();
    }

    void Success(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        LOG_INFO_S(*TlsActivationContext, LogService,
            LogPrefix << "Successful " << GetOperationType() <<  " request: " << ev->Get()->Status());
        Reply();
    }

    void Reply() {
        Send(Owner, new TEvTableCreator::TEvCreateTableResponse());
        if (SchemePipeActorId) {
            NTabletPipe::CloseClient(SelfId(), SchemePipeActorId);
        }
        PassAway();
    }

    const TString& TableName() const {
        return PathComponents.back();
    }

private:
    void ExcludeExistingColumns(const THashMap<ui32, TSysTables::TTableColumnInfo>& existingColumns) {
        THashSet<TString> existingNames;
        TStringBuilder columns;
        for (const auto& [_, colInfo] : existingColumns) {
            existingNames.insert(colInfo.Name);
            if (columns) {
                columns << ", ";
            }
            columns << colInfo.Name;
        }

        TVector<NKikimrSchemeOp::TColumnDescription> filteredColumns;
        TStringBuilder filtered;
        for (auto& col : Columns) {
            if (!existingNames.contains(col.GetName())) {
                if (filtered) {
                    filtered << ", ";
                }
                filtered << col.GetName();
                filteredColumns.emplace_back(std::move(col));
            }
        }
        if (filteredColumns.empty()) {
            LOG_DEBUG_S(*TlsActivationContext, LogService,
                LogPrefix << "Column diff is empty, finishing");
        } else {
            LOG_NOTICE_S(*TlsActivationContext, LogService,
                LogPrefix << "Adding columns. New columns: " << filtered << ". Existing columns: " << columns);
        }


        Columns = std::move(filteredColumns);
    }

    TStringBuf GetOperationType() const {
        return OperationType == NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable ? "create" : "alter";
    }

    TMaybe<TDuration> GetRetryDelay(bool longDelay = false) {
        if (!RetryState) {
            RetryState = CreateRetryState();
        }
        return RetryState->GetNextRetryDelay(longDelay);
    }

    static TTableCreatorRetryPolicy::IRetryState::TPtr CreateRetryState() {
        return TTableCreatorRetryPolicy::GetFixedIntervalPolicy(
                  [](bool longDelay){return longDelay ? ERetryErrorClass::LongRetry : ERetryErrorClass::ShortRetry;}
                , TDuration::MilliSeconds(100)
                , TDuration::MilliSeconds(300)
                , 100
            )->CreateRetryState();
    }

    const TVector<TString> PathComponents;
    TVector<NKikimrSchemeOp::TColumnDescription> Columns;
    const TVector<TString> KeyColumns;
    NKikimrServices::EServiceKikimr LogService;
    const TMaybe<NKikimrSchemeOp::TTTLSettings> TtlSettings;
    NKikimrSchemeOp::EOperationType OperationType = NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable;
    NActors::TActorId Owner;
    NActors::TActorId SchemePipeActorId;
    bool PipeClientClosedByUs = false;
    const TString LogPrefix;
    TTableCreatorRetryPolicy::IRetryState::TPtr RetryState;
};

} // namespace

NActors::IActor* CreateTableCreator(
    TVector<TString> pathComponents,
    TVector<NKikimrSchemeOp::TColumnDescription> columns,
    TVector<TString> keyColumns,
    NKikimrServices::EServiceKikimr logService,
    TMaybe<NKikimrSchemeOp::TTTLSettings> ttlSettings)
{
    return new TTableCreator(std::move(pathComponents), std::move(columns),
        std::move(keyColumns), logService, std::move(ttlSettings));
}

} // namespace NKikimr
