#include "dst_creator.h"
#include "logging.h"
#include "private_events.h"
#include "util.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/tx/scheme_board/events.h>
#include <ydb/core/tx/scheme_board/subscriber.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NReplication::NController {

using namespace NConsole;
using namespace NSchemeShard;

class TDstCreator: public TActorBootstrapped<TDstCreator> {
    void Resolve(const TPathId& pathId) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();

        auto& entry = request->ResultSet.emplace_back();
        entry.TableId = pathId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.RedirectRequired = false;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TThis::StateResolveDatabase);
    }

    STATEFN(StateResolveDatabase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* response = ev->Get()->Request.Get();

        Y_ABORT_UNLESS(response->ResultSet.size() == 1);
        const auto& entry = response->ResultSet.front();

        LOG_T("Handle " << ev->Get()->ToString()
            << ": entry# " << entry.ToString());

        switch (entry.Status) {
        case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
            break;
        default:
            LOG_W("Unexpected status"
                << ": entry# " << entry.ToString());
            return Error(NKikimrScheme::StatusSchemeError, "Cannot resolve domain info");
        }

        if (!DomainKey) {
            if (!entry.DomainInfo) {
                LOG_E("Empty domain info"
                    << ": entry# " << entry.ToString());
                return Error(NKikimrScheme::StatusSchemeError, "Empty domain info");
            }

            if (entry.SecurityObject) {
                Owner = entry.SecurityObject->GetOwnerSID();
            }

            DomainKey = entry.DomainInfo->DomainKey;
            Resolve(DomainKey);
        } else {
            Database = CanonizePath(entry.Path);
            DescribeSrcPath(true);
        }
    }

    void GetTableProfiles() {
        LOG_T("Get table profiles");

        using namespace NKikimrConsole;
        auto ev = MakeHolder<TEvConfigsDispatcher::TEvGetConfigRequest>((ui32)TConfigItem::TableProfilesConfigItem);
        Send(MakeConfigsDispatcherID(SelfId().NodeId()), std::move(ev), IEventHandle::FlagTrackDelivery);

        Become(&TThis::StateGetTableProfiles);
    }

    STATEFN(StateGetTableProfiles) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvConfigsDispatcher::TEvGetConfigResponse, Handle);
            sFunc(TEvents::TEvUndelivered, DescribeSrcPath);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvConfigsDispatcher::TEvGetConfigResponse::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());
        TableProfiles.Load(ev->Get()->Config->GetTableProfilesConfig());
        DescribeSrcPath();
    }

    void DescribeSrcPath(bool bootstrap = false) {
        Become(&TThis::StateDescribeSrcPath);

        switch (Kind) {
        case TReplication::ETargetKind::Table:
            if (bootstrap) {
                GetTableProfiles();
            } else {
                Send(YdbProxy, new TEvYdbProxy::TEvDescribeTableRequest(SrcPath, NYdb::NTable::TDescribeTableSettings()
                    .WithKeyShardBoundary(true)));
            }
            break;
        case TReplication::ETargetKind::IndexTable:
            Y_ABORT("unreachable");
        }
    }

    STATEFN(StateDescribeSrcPath) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDescribeTableResponse, Handle);
            sFunc(TEvents::TEvWakeup, DescribeSrcPath);
        default:
            return StateBase(ev);
        }
    }

    static NKikimrScheme::EStatus ConvertStatus(NYdb::EStatus status) {
        switch (status) {
        case NYdb::EStatus::SUCCESS:
            return NKikimrScheme::StatusSuccess;
        case NYdb::EStatus::BAD_REQUEST:
            return NKikimrScheme::StatusInvalidParameter;
        case NYdb::EStatus::UNAUTHORIZED:
            return NKikimrScheme::StatusAccessDenied;
        case NYdb::EStatus::SCHEME_ERROR:
            return NKikimrScheme::StatusSchemeError;
        case NYdb::EStatus::PRECONDITION_FAILED:
            return NKikimrScheme::StatusPreconditionFailed;
        case NYdb::EStatus::ALREADY_EXISTS:
            return NKikimrScheme::StatusAlreadyExists;
        default:
            return NKikimrScheme::StatusNotAvailable;
        }
    }

    void Handle(TEvYdbProxy::TEvDescribeTableResponse::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        Y_ABORT_UNLESS(Kind == TReplication::ETargetKind::Table);
        const auto& result = ev->Get()->Result;

        if (!result.IsSuccess()) {
            if (IsRetryableError(result)) {
                return Retry();
            }

            return Error(ConvertStatus(result.GetStatus()), TStringBuilder() << "Cannot describe table"
                << ": status: " << result.GetStatus()
                << ", issue: " << result.GetIssues().ToOneLineString());
        }

        Ydb::Table::CreateTableRequest scheme;
        result.GetTableDescription().SerializeTo(scheme);

        // filter out unsupported index types
        auto& indexes = *scheme.mutable_indexes();
        for (auto it = indexes.begin(); it != indexes.end();) {
            switch (it->type_case()) {
            case Ydb::Table::TableIndex::kGlobalIndex:
            case Ydb::Table::TableIndex::kGlobalUniqueIndex:
                ++it;
                continue;
            default:
                it = indexes.erase(it);
                break;
            }
        }

        Ydb::StatusIds::StatusCode status;
        TString error;
        if (!FillTableDescription(TxBody, scheme, TableProfiles, status, error)) {
            return Error(NKikimrScheme::StatusSchemeError, error);
        }

        std::pair<TString, TString> pathPair;
        if (!TrySplitPathByDb(DstPath, Database, pathPair, error)) {
            return Error(NKikimrScheme::StatusSchemeError, error);
        }

        TxBody.SetWorkingDir(pathPair.first);

        NKikimrSchemeOp::TTableDescription* desc = nullptr;
        if (scheme.indexes_size()) {
            NeedToCheck = true;
            TxBody.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateIndexedTable);
            TxBody.SetInternal(true);
            desc = TxBody.MutableCreateIndexedTable()->MutableTableDescription();
            if (!FillIndexDescription(*TxBody.MutableCreateIndexedTable(), scheme, status, error)) {
                return Error(NKikimrScheme::StatusSchemeError, error);
            }
        } else {
            TxBody.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTable);
            desc = TxBody.MutableCreateTable();
        }

        Y_ABORT_UNLESS(desc);
        desc->SetName(pathPair.second);

        FillReplicationConfig(*desc->MutableReplicationConfig());
        if (scheme.indexes_size()) {
            for (auto& index : *TxBody.MutableCreateIndexedTable()->MutableIndexDescription()) {
                FillReplicationConfig(*index.MutableIndexImplTableDescriptions(0)->MutableReplicationConfig());
            }
        }

        AllocateTxId();
    }

    static void FillReplicationConfig(NKikimrSchemeOp::TTableReplicationConfig& replicationConfig) {
        replicationConfig.SetMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
        replicationConfig.SetConsistency(NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_WEAK);
    }

    void AllocateTxId() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
        Become(&TThis::StateAllocateTxId);
    }

    STATEFN(StateAllocateTxId) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        TxId = ev->Get()->TxId;
        PipeCache = ev->Get()->Services.LeaderPipeCache;
        CreateDst();
    }

    void CreateDst() {
        auto ev = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(TxId, SchemeShardId);
        *ev->Record.AddTransaction() = TxBody;

        if (Owner) {
            ev->Record.SetOwner(Owner);
        }

        Send(PipeCache, new TEvPipeCache::TEvForward(ev.Release(), SchemeShardId, true));
        Become(&TThis::StateCreateDst);
    }

    STATEFN(StateCreateDst) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            hFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            sFunc(TEvents::TEvWakeup, AllocateTxId);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());
        const auto& record = ev->Get()->Record;

        switch (record.GetStatus()) {
        case NKikimrScheme::StatusAccepted:
            if (!NeedToCheck) {
                DstPathId = TPathId(SchemeShardId, record.GetPathId());
            }
            Y_DEBUG_ABORT_UNLESS(TxId == record.GetTxId());
            return SubscribeTx(record.GetTxId());
        case NKikimrScheme::StatusMultipleModifications:
            if (record.HasPathCreateTxId()) {
                NeedToCheck = true;
                return SubscribeTx(record.GetPathCreateTxId());
            } else {
                return Error(record.GetStatus(), record.GetReason());
            }
            break;
        case NKikimrScheme::StatusAlreadyExists:
            return DescribeDstPath();
        default:
            return Error(record.GetStatus(), record.GetReason());
        }
    }

    void SubscribeTx(ui64 txId) {
        LOG_D("Subscribe tx"
            << ": txId# " << txId);
        Send(PipeCache, new TEvPipeCache::TEvForward(new TEvSchemeShard::TEvNotifyTxCompletion(txId), SchemeShardId));
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        if (NeedToCheck) {
            DescribeDstPath();
        } else {
            Success();
        }
    }

    void DescribeDstPath() {
        Send(PipeCache, new TEvPipeCache::TEvForward(new TEvSchemeShard::TEvDescribeScheme(DstPath), SchemeShardId));
        Become(&TThis::StateDescribeDstPath);
    }

    STATEFN(StateDescribeDstPath) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            sFunc(TEvents::TEvWakeup, DescribeDstPath);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());
        const auto& record = ev->Get()->GetRecord();

        switch (record.GetStatus()) {
            case NKikimrScheme::StatusSuccess: {
                TString error;
                if (!CheckScheme(record.GetPathDescription(), error)) {
                    return Error(NKikimrScheme::StatusSchemeError, error);
                } else {
                    DstPathId = TPathId(record.GetPathOwnerId(), record.GetPathId());
                    return Success();
                }
                break;
            }
            case NKikimrScheme::StatusPathDoesNotExist:
                return AllocateTxId();
            case NKikimrScheme::StatusSchemeError:
            case NKikimrScheme::StatusAccessDenied:
            case NKikimrScheme::StatusRedirectDomain:
            case NKikimrScheme::StatusNameConflict:
            case NKikimrScheme::StatusInvalidParameter:
            case NKikimrScheme::StatusPreconditionFailed:
                return Error(record.GetStatus(), record.GetReason());
            default:
                return Retry();
        }
    }

    bool CheckScheme(const NKikimrSchemeOp::TPathDescription& desc, TString& error) const {
        switch (Kind) {
        case TReplication::ETargetKind::Table:
            return CheckTableScheme(desc.GetTable(), error);
        case TReplication::ETargetKind::IndexTable:
            Y_ABORT("unreachable");
        }
    }

    bool CheckTableScheme(const NKikimrSchemeOp::TTableDescription& got, TString& error) const {
        if (!got.HasReplicationConfig()) {
            error = "Empty replication config";
            return false;
        }

        const auto& replicationConfig = got.GetReplicationConfig();

        switch (replicationConfig.GetMode()) {
        case NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY:
            break;
        default:
            error = "Unsupported replication mode";
            return false;
        }

        switch (replicationConfig.GetConsistency()) {
        case NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_WEAK:
            break;
        default:
            error = TStringBuilder() << "Unsupported replication consistency"
                << ": " << static_cast<int>(replicationConfig.GetConsistency());
            return false;
        }

        const NKikimrSchemeOp::TIndexedTableCreationConfig* indexedDesc = nullptr;
        const NKikimrSchemeOp::TTableDescription* tableDesc = nullptr;
        if (TxBody.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateIndexedTable) {
            indexedDesc = &TxBody.GetCreateIndexedTable();
            tableDesc = &indexedDesc->GetTableDescription();
        } else {
            tableDesc = &TxBody.GetCreateTable();
        }

        Y_ABORT_UNLESS(tableDesc);

        // check key
        if (tableDesc->KeyColumnNamesSize() != got.KeyColumnNamesSize()) {
            error = TStringBuilder() << "Key columns size mismatch"
                << ": expected: " << tableDesc->KeyColumnNamesSize()
                << ", got: " << got.KeyColumnNamesSize();
            return false;
        }

        for (ui32 i = 0; i < tableDesc->KeyColumnNamesSize(); ++i) {
            if (tableDesc->GetKeyColumnNames(i) != got.GetKeyColumnNames(i)) {
                error = TStringBuilder() << "Key column name mismatch"
                    << ": position: " << i
                    << ", expected: " << tableDesc->GetKeyColumnNames(i)
                    << ", got: " << got.GetKeyColumnNames(i);
                return false;
            }
        }

        // check columns
        THashMap<TStringBuf, TStringBuf> columns;
        for (const auto& column : got.GetColumns()) {
            columns.emplace(column.GetName(), column.GetType());
        }

        if (tableDesc->ColumnsSize() != columns.size()) {
            error = TStringBuilder() << "Columns size mismatch"
                << ": expected: " << tableDesc->ColumnsSize()
                << ", got: " << columns.size();
            return false;
        }

        for (const auto& column : tableDesc->GetColumns()) {
            auto it = columns.find(column.GetName());
            if (it == columns.end()) {
                error = TStringBuilder() << "Cannot find column"
                    << ": name: " << column.GetName();
                return false;
            }

            if (column.GetType() != it->second) {
                error = TStringBuilder() << "Column type mismatch"
                    << ": name: " << column.GetName()
                    << ", expected: " << column.GetType()
                    << ", got: " << it->second;
                return false;
            }
        }

        // check indexes
        THashMap<TStringBuf, const NKikimrSchemeOp::TIndexDescription*> indexes;
        for (const auto& index : got.GetTableIndexes()) {
            indexes.emplace(index.GetName(), &index);
        }

        if (!indexedDesc) {
            if (!indexes.empty()) {
                error = TStringBuilder() << "Indexes size mismatch"
                    << ": expected: " << 0
                    << ", got: " << indexes.size();
                return false;
            }

            return true;
        }

        if (indexedDesc->IndexDescriptionSize() != indexes.size()) {
            error = TStringBuilder() << "Indexes size mismatch"
                << ": expected: " << indexedDesc->IndexDescriptionSize()
                << ", got: " << indexes.size();
            return false;
        }

        for (const auto& index : indexedDesc->GetIndexDescription()) {
            auto it = indexes.find(index.GetName());
            if (it == indexes.end()) {
                error = TStringBuilder() << "Cannot find index"
                    << ": name: " << index.GetName();
                return false;
            }

            if (index.GetType() != it->second->GetType()) {
                error = TStringBuilder() << "Index type mismatch"
                    << ": name: " << index.GetName()
                    << ", expected: " << NKikimrSchemeOp::EIndexType_Name(index.GetType())
                    << ", got: " << NKikimrSchemeOp::EIndexType_Name(it->second->GetType());
                return false;
            }

            if (index.KeyColumnNamesSize() != it->second->KeyColumnNamesSize()) {
                error = TStringBuilder() << "Index key columns size mismatch"
                    << ": name: " << index.GetName()
                    << ", expected: " << index.KeyColumnNamesSize()
                    << ", got: " << it->second->KeyColumnNamesSize();
                return false;
            }

            for (ui32 i = 0; i < index.KeyColumnNamesSize(); ++i) {
                if (index.GetKeyColumnNames(i) != it->second->GetKeyColumnNames(i)) {
                    error = TStringBuilder() << "Index key column name mismatch"
                        << ": name: " << index.GetName()
                        << ", position: " << i
                        << ", expected: " << index.GetKeyColumnNames(i)
                        << ", got: " << it->second->GetKeyColumnNames(i);
                    return false;
                }
            }

            if (index.DataColumnNamesSize() != it->second->DataColumnNamesSize()) {
                error = TStringBuilder() << "Index data columns size mismatch"
                    << ": name: " << index.GetName()
                    << ", expected: " << index.DataColumnNamesSize()
                    << ", got: " << it->second->DataColumnNamesSize();
                return false;
            }

            for (ui32 i = 0; i < index.DataColumnNamesSize(); ++i) {
                if (index.GetDataColumnNames(i) != it->second->GetDataColumnNames(i)) {
                    error = TStringBuilder() << "Index data column name mismatch"
                        << ": name: " << index.GetName()
                        << ", position: " << i
                        << ", expected: " << index.GetDataColumnNames(i)
                        << ", got: " << it->second->GetDataColumnNames(i);
                    return false;
                }
            }
        }

        return true;
    }

    void SubscribeDstPath() {
        Subscriber = Register(CreateSchemeBoardSubscriber(SelfId(), DstPath));
        Become(&TThis::StateSubscribeDstPath);
    }

    STATEFN(StateSubscribeDstPath) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardEvents::TEvNotifyUpdate, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        const auto& desc = ev->Get()->DescribeSchemeResult;
        if (desc.GetStatus() != NKikimrScheme::StatusSuccess) {
            return;
        }

        const auto& entryDesc = desc.GetPathDescription().GetSelf();
        if (!entryDesc.HasCreateFinished() || !entryDesc.GetCreateFinished()) {
            return;
        }

        DstPathId = ev->Get()->PathId;
        return Success();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        if (SchemeShardId != ev->Get()->TabletId) {
            return;
        }

        Retry();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());
        Retry();
    }

    void Success() {
        Y_ABORT_UNLESS(DstPathId);
        LOG_I("Success"
            << ": dstPathId# " << DstPathId);

        Send(Parent, new TEvPrivate::TEvCreateDstResult(ReplicationId, TargetId, DstPathId));
        PassAway();
    }

    void Error(NKikimrScheme::EStatus status, const TString& error) {
        LOG_E("Error"
            << ": status# " << status
            << ", reason# " << error);

        Send(Parent, new TEvPrivate::TEvCreateDstResult(ReplicationId, TargetId, status, error));
        PassAway();
    }

    void Retry() {
        LOG_D("Retry");
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup);
    }

    void PassAway() override {
        if (const auto& actorId = std::exchange(Subscriber, {})) {
            Send(actorId, new TEvents::TEvPoison());
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_DST_CREATOR;
    }

    explicit TDstCreator(
            const TActorId& parent,
            ui64 schemeShardId,
            const TActorId& proxy,
            const TPathId& pathId,
            ui64 rid,
            ui64 tid,
            TReplication::ETargetKind kind,
            const TString& srcPath,
            const TString& dstPath)
        : Parent(parent)
        , SchemeShardId(schemeShardId)
        , YdbProxy(proxy)
        , PathId(pathId)
        , ReplicationId(rid)
        , TargetId(tid)
        , Kind(kind)
        , SrcPath(srcPath)
        , DstPath(dstPath)
        , LogPrefix("DstCreator", ReplicationId, TargetId)
    {
    }

    void Bootstrap() {
        switch (Kind) {
        case TReplication::ETargetKind::Table:
            return Resolve(PathId);
        case TReplication::ETargetKind::IndexTable:
            // indexed table will be created along with its indexes
            return SubscribeDstPath();
        }
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 SchemeShardId;
    const TActorId YdbProxy;
    const TPathId PathId;
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TReplication::ETargetKind Kind;
    const TString SrcPath;
    const TString DstPath;
    const TActorLogPrefix LogPrefix;

    TPathId DomainKey;
    TString Database;
    TString Owner;
    TTableProfiles TableProfiles;
    ui64 TxId = 0;
    NKikimrSchemeOp::TModifyScheme TxBody;
    TActorId PipeCache;
    bool NeedToCheck = false;
    TPathId DstPathId;
    TActorId Subscriber;

}; // TDstCreator

IActor* CreateDstCreator(TReplication* replication, ui64 targetId, const TActorContext& ctx) {
    const auto* target = replication->FindTarget(targetId);
    Y_ABORT_UNLESS(target);
    return CreateDstCreator(ctx.SelfID, replication->GetSchemeShardId(), replication->GetYdbProxy(), replication->GetPathId(),
        replication->GetId(), target->GetId(), target->GetKind(), target->GetSrcPath(), target->GetDstPath());
}

IActor* CreateDstCreator(const TActorId& parent, ui64 schemeShardId, const TActorId& proxy, const TPathId& pathId,
        ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TString& srcPath, const TString& dstPath)
{
    return new TDstCreator(parent, schemeShardId, proxy, pathId, rid, tid, kind, srcPath, dstPath);
}

}
