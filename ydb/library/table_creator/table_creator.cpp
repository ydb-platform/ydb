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

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
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
        TMaybe<NKikimrSchemeOp::TTTLSettings> ttlSettings = Nothing(),
        bool isSystemUser = false,
        TMaybe<NKikimrSchemeOp::TPartitioningPolicy> partitioningPolicy = Nothing())
        : PathComponents(std::move(pathComponents))
        , Columns(std::move(columns))
        , KeyColumns(std::move(keyColumns))
        , LogService(logService)
        , TtlSettings(std::move(ttlSettings))
        , IsSystemUser(isSystemUser)
        , PartitioningPolicy(std::move(partitioningPolicy))
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
        hFunc(TEvents::TEvUndelivered, Handle)
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
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(NTableCreator::BuildSchemeCacheNavigateRequest(
            {PathComponents}
        ).Release()), IEventHandle::FlagTrackDelivery);
    }

    void RunTableRequest() {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        NKikimrSchemeOp::TModifyScheme& modifyScheme = *request->Record.MutableTransaction()->MutableModifyScheme();
        auto pathComponents = SplitPath(AppData()->TenantName);
        for (size_t i = 0; i < PathComponents.size() - 1; ++i) {
            pathComponents.emplace_back(PathComponents[i]);
        }
        modifyScheme.SetWorkingDir(CanonizePath(pathComponents));
        TString fullPath = modifyScheme.GetWorkingDir() + "/" + TableName();
        LOG_DEBUG_S(*TlsActivationContext, LogService, LogPrefix << "Full table path:" << fullPath);
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
        if (IsSystemUser) {
            request->Record.SetUserToken(NACLib::TSystemUsers::Metadata().SerializeAsString());
        }
        if (PartitioningPolicy) {
            auto* partitioningPolicy = tableDesc->MutablePartitionConfig()->MutablePartitioningPolicy();
            partitioningPolicy->CopyFrom(*PartitioningPolicy);
        }

        Send(MakeTxProxyID(), std::move(request));
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->Reason == NActors::TEvents::TEvUndelivered::ReasonActorUnknown) {
            Retry();
            return;
        }
        Fail("Scheme cache is unavailable");
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
            Fail("Retry limit exceeded");
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
        TString message = TStringBuilder() << "Failed to upgrade table: " << status;
        LOG_ERROR_S(*TlsActivationContext, LogService, LogPrefix << message);
        Reply(false, message);
    }

    void Fail(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        TString message = TStringBuilder() << "Failed " << GetOperationType() << " request: " << ev->Get()->Status() << ". Response: " << ev->Get()->Record;
        LOG_ERROR_S(*TlsActivationContext, LogService, LogPrefix << message);
        Reply(false, message);
    }

    void Fail(const TString& message) {
        LOG_ERROR_S(*TlsActivationContext, LogService, LogPrefix << message);
        Reply(false, message);
    }

    void Success() {
        Reply(true);
    }

    void Success(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        LOG_INFO_S(*TlsActivationContext, LogService,
            LogPrefix << "Successful " << GetOperationType() <<  " request: " << ev->Get()->Status());
        Reply(true);
    }

    void Reply(bool success, const TString& message) {
        Reply(success, {NYql::TIssue(message)});
    }

    void Reply(bool success, NYql::TIssues issues = {}) {
        Send(Owner, new TEvTableCreator::TEvCreateTableResponse(success, std::move(issues)));
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
    bool IsSystemUser = false;
    const TMaybe<NKikimrSchemeOp::TPartitioningPolicy> PartitioningPolicy;
    NKikimrSchemeOp::EOperationType OperationType = NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable;
    NActors::TActorId Owner;
    NActors::TActorId SchemePipeActorId;
    bool PipeClientClosedByUs = false;
    const TString LogPrefix;
    TTableCreatorRetryPolicy::IRetryState::TPtr RetryState;
};

} // namespace

namespace NTableCreator {

THolder<NSchemeCache::TSchemeCacheNavigate> BuildSchemeCacheNavigateRequest(const TVector<TVector<TString>>& pathsComponents, const TString& database, TIntrusiveConstPtr<NACLib::TUserToken> userToken) {
    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    auto databasePath = SplitPath(database);
    request->DatabaseName = CanonizePath(databasePath);
    if (userToken && !userToken->GetSerializedToken().empty()) {
        request->UserToken = userToken;
    }

    for (const auto& pathComponents : pathsComponents) {
        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        entry.ShowPrivatePath = true;
        entry.Path = databasePath;
        entry.Path.insert(entry.Path.end(), pathComponents.begin(), pathComponents.end());
    }

    return request;
}

THolder<NSchemeCache::TSchemeCacheNavigate> BuildSchemeCacheNavigateRequest(const TVector<TVector<TString>>& pathsComponents) {
    return BuildSchemeCacheNavigateRequest(pathsComponents, AppData()->TenantName, nullptr);
}

NKikimrSchemeOp::TColumnDescription TMultiTableCreator::Col(const TString& columnName, const char* columnType) {
    NKikimrSchemeOp::TColumnDescription desc;
    desc.SetName(columnName);
    desc.SetType(columnType);
    return desc;
}

NKikimrSchemeOp::TColumnDescription TMultiTableCreator::Col(const TString& columnName, NScheme::TTypeId columnType) {
    return Col(columnName, NScheme::TypeName(columnType));
}

NKikimrSchemeOp::TTTLSettings TMultiTableCreator::TtlCol(const TString& columnName, TDuration expireAfter, TDuration runInterval) {
    NKikimrSchemeOp::TTTLSettings settings;
    settings.MutableEnabled()->SetExpireAfterSeconds(expireAfter.Seconds());
    settings.MutableEnabled()->SetColumnName(columnName);
    settings.MutableEnabled()->MutableSysSettings()->SetRunInterval(runInterval.MicroSeconds());
    return settings;
}

TMultiTableCreator::TMultiTableCreator(std::vector<NActors::IActor*> tableCreators)
    : TableCreators(std::move(tableCreators))
{}

void TMultiTableCreator::Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) {
    TBase::Registered(sys, owner);
    Owner = owner;
}

void TMultiTableCreator::Bootstrap() {
    Become(&TMultiTableCreator::StateFunc);

    TablesCreating = TableCreators.size();
    for (const auto creator : TableCreators) {
        Register(creator);
    }
}

void TMultiTableCreator::Handle(TEvTableCreator::TEvCreateTableResponse::TPtr& ev) {
    if (!ev->Get()->Success) {
        Success = false;
        Issues.AddIssues(std::move(ev->Get()->Issues));
    }

    Y_ABORT_UNLESS(TablesCreating > 0);
    if (--TablesCreating == 0) {
        OnTablesCreated(Success, std::move(Issues));
        PassAway();
    }
}

STRICT_STFUNC(TMultiTableCreator::StateFunc,
    hFunc(TEvTableCreator::TEvCreateTableResponse, Handle);
);

} // namespace NTableCreator

NActors::IActor* CreateTableCreator(
    TVector<TString> pathComponents,
    TVector<NKikimrSchemeOp::TColumnDescription> columns,
    TVector<TString> keyColumns,
    NKikimrServices::EServiceKikimr logService,
    TMaybe<NKikimrSchemeOp::TTTLSettings> ttlSettings,
    bool isSystemUser,
    TMaybe<NKikimrSchemeOp::TPartitioningPolicy> partitioningPolicy)
{
    return new TTableCreator(std::move(pathComponents), std::move(columns),
        std::move(keyColumns), logService, std::move(ttlSettings), isSystemUser,
        std::move(partitioningPolicy));
}

} // namespace NKikimr
