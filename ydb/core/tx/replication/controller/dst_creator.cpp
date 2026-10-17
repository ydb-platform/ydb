#include "dst_creator.h"
#include "logging.h"
#include "private_events.h"
#include "util.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/replication/common/family_settings.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/tx/scheme_board/events.h>
#include <ydb/core/tx/scheme_board/subscriber.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

using namespace NConsole;
using namespace NSchemeShard;

namespace {

bool CheckColumnFamilySettings(
        const NKikimrSchemeOp::TFamilyDescription& expected,
        const NKikimrSchemeOp::TFamilyDescription& actual,
        TString& error)
{
    const auto name = GetFamilyName(expected);

    const auto expectedCodec = GetColumnCodec(expected);
    const auto actualCodec = GetColumnCodec(actual);
    if (expectedCodec != actualCodec) {
        error = TStringBuilder() << "Column family codec mismatch"
            << ": name: " << name
            << ", expected: " << static_cast<ui32>(expectedCodec)
            << ", got: " << static_cast<ui32>(actualCodec);
        return false;
    }

    const auto expectedCacheMode = expected.GetColumnCacheMode();
    const auto actualCacheMode = actual.GetColumnCacheMode();
    if (expectedCacheMode != actualCacheMode) {
        error = TStringBuilder() << "Column family cache mode mismatch"
            << ": name: " << name
            << ", expected: " << static_cast<ui32>(expectedCacheMode)
            << ", got: " << static_cast<ui32>(actualCacheMode);
        return false;
    }

    const auto& expectedData = expected.GetStorageConfig().GetData();
    const auto& expectedMedia = expectedData.GetPreferredPoolKind();
    if (!expectedMedia || expectedData.GetAllowOtherKinds()) {
        return true;
    }

    const auto& actualData = actual.GetStorageConfig().GetData();
    const auto& actualMedia = actualData.GetPreferredPoolKind();
    if (expectedMedia == actualMedia && !actualData.GetAllowOtherKinds()) {
        return true;
    }

    error = TStringBuilder() << "Column family media mismatch"
        << ": name: " << name
        << ", expected: " << expectedMedia
        << ", got: " << actualMedia;
    return false;
}

bool CheckReplicationMode(
        const NKikimrSchemeOp::TTableDescription& table,
        NKikimrSchemeOp::TTableReplicationConfig::EReplicationMode mode)
{
    return table.GetReplicationConfig().GetMode() == mode;
}

bool IsReplica(const NKikimrSchemeOp::TTableDescription& table) {
    return CheckReplicationMode(table, NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
}

bool NotReplica(const NKikimrSchemeOp::TTableDescription& table) {
    return CheckReplicationMode(table, NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE);
}

} // anonymous namespace

class TDstCreator: public TActorBootstrapped<TDstCreator> {
    void Resolve(const TPathId& pathId) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;

        auto& entry = request->ResultSet.emplace_back();
        entry.TableId = pathId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.RedirectRequired = false;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TThis::StateResolveDatabase);
    }

    STATEFN(StateResolveDatabase) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateResolveDatabase"});
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

        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()},
            {"entry", entry});

        switch (entry.Status) {
        case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
            break;
        default:
            YDB_LOG_WARN("Unexpected status",
                {"entry", entry});
            return Error(NKikimrScheme::StatusSchemeError, "Cannot resolve domain info");
        }

        if (!DomainKey) {
            if (!entry.DomainInfo) {
                YDB_LOG_ERROR("Empty domain info",
                    {"entry", entry});
                return Error(NKikimrScheme::StatusSchemeError, "Empty domain info");
            }

            if (entry.SecurityObject) {
                Owner = entry.SecurityObject->GetOwnerSID();
            }

            DomainKey = entry.DomainInfo->DomainKey;
            if (!Database) {
                Resolve(DomainKey);
            } else {
                DescribeSrcPath(true);
            }
        } else {
            Database = CanonizePath(entry.Path);
            DescribeSrcPath(true);
        }
    }

    void GetTableProfiles() {
        YDB_LOG_TRACE("Get table profiles");

        using namespace NKikimrConsole;
        auto ev = MakeHolder<TEvConfigsDispatcher::TEvGetConfigRequest>((ui32)TConfigItem::TableProfilesConfigItem);
        Send(MakeConfigsDispatcherID(SelfId().NodeId()), std::move(ev), IEventHandle::FlagTrackDelivery);

        Become(&TThis::StateGetTableProfiles);
    }

    STATEFN(StateGetTableProfiles) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateGetTableProfiles"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvConfigsDispatcher::TEvGetConfigResponse, Handle);
            sFunc(TEvents::TEvUndelivered, DescribeSrcPath);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvConfigsDispatcher::TEvGetConfigResponse::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
        TableProfiles.Load(ev->Get()->Config->GetTableProfilesConfig());
        DescribeSrcPath();
    }

    void DescribeSrcPath(bool bootstrap = false) {
        Become(&TThis::StateDescribeSrcPath);

        switch (Kind) {
        case TReplication::ETargetKind::Table:
        case TReplication::ETargetKind::IndexTable:
            if (bootstrap) {
                GetTableProfiles();
            } else {
                Send(YdbProxy, new TEvYdbProxy::TEvDescribeTableRequest(SrcPath, NYdb::NTable::TDescribeTableSettings()
                    .WithKeyShardBoundary(true)));
            }
            break;
        case TReplication::ETargetKind::Transfer:
            Y_ABORT("unreachable");
        }
    }

    STATEFN(StateDescribeSrcPath) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateDescribeSrcPath"});
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
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        Y_ABORT_UNLESS(Kind == TReplication::ETargetKind::Table || Kind == TReplication::ETargetKind::IndexTable);
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
            case Ydb::Table::TableIndex::kGlobalAsyncIndex:
                if (AppData()->FeatureFlags.GetEnableAsyncIndexReplication()) {
                    ++it;
                } else {
                    it = indexes.erase(it);
                }
                continue;
            default:
                it = indexes.erase(it);
                break;
            }
        }

        Ydb::StatusIds::StatusCode status;
        TString error;

        if (!FillTableDescription(TxBody, scheme, TableProfiles, status, error, scheme.indexes_size())) {
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
            if (!FillIndexDescription(*TxBody.MutableCreateIndexedTable(), scheme, AppData()->FeatureFlags.GetEnableCompactFulltextIndex(), status, error)) {
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
                // Async indexes are maintained by the destination's own change exchange.
                // Only synchronous index tables have independent replication targets.
                if (index.GetType() != NKikimrSchemeOp::EIndexTypeGlobalAsync) {
                    FillReplicationConfig(*index.MutableIndexImplTableDescriptions(0)->MutableReplicationConfig());
                }
            }
        }

        AllocateTxId();
    }

    void FillReplicationConfig(NKikimrSchemeOp::TTableReplicationConfig& replicationConfig) const {
        NController::FillReplicationConfig(replicationConfig, Mode, Consistency);
    }

    void AllocateTxId() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
        Become(&TThis::StateAllocateTxId);
    }

    STATEFN(StateAllocateTxId) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateAllocateTxId"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        TxId = ev->Get()->TxId;
        PipeCache = ev->Get()->Services.LeaderPipeCache;
        if (SkipInitialScan && !Attaching) {
            DescribeDstPath();
        } else if (Attaching) {
            AttachDst();
        } else {
            CreateDst();
        }
    }

    void CreateDst() {
        ExecuteSchemeTx(TxBody);
    }

    void AttachDst() {
        NKikimrSchemeOp::TModifyScheme tx;
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
        tx.SetInternal(true);
        DstPathId.ToProto(tx.MutableAlterTable()->MutablePathId());
        FillReplicationConfig(*tx.MutableAlterTable()->MutableReplicationConfig());
        ExecuteSchemeTx(tx);
    }

    void ExecuteSchemeTx(const NKikimrSchemeOp::TModifyScheme& tx) {
        auto ev = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(TxId, SchemeShardId);
        *ev->Record.AddTransaction() = tx;

        if (Owner) {
            ev->Record.SetOwner(Owner);
        }

        Send(PipeCache, new TEvPipeCache::TEvForward(ev.Release(), SchemeShardId, true));
        Become(&TThis::StateExecuteSchemeTx);
    }

    STATEFN(StateExecuteSchemeTx) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateExecuteSchemeTx"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            hFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            sFunc(TEvents::TEvWakeup, AllocateTxId);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
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
                Attaching = false;
                return SubscribeTx(record.GetPathCreateTxId());
            } else if (Attaching) {
                Attaching = false;
                Become(&TThis::StateDescribeDstPath);
                return Retry();
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
        YDB_LOG_DEBUG("Subscribe tx",
            {"txId", txId});
        Send(PipeCache, new TEvPipeCache::TEvForward(new TEvSchemeShard::TEvNotifyTxCompletion(txId), SchemeShardId));
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        if (NeedToCheck) {
            DescribeDstPath();
        } else {
            Success();
        }
    }

    void DescribeDstPath() {
        Send(PipeCache, new TEvPipeCache::TEvForward(
            PendingDstPathId
                ? new TEvSchemeShard::TEvDescribeScheme(PendingDstPathId)
                : new TEvSchemeShard::TEvDescribeScheme(DstPath), SchemeShardId));
        Become(&TThis::StateDescribeDstPath);
    }

    STATEFN(StateDescribeDstPath) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateDescribeDstPath"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            sFunc(TEvents::TEvWakeup, DescribeDstPath);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
        const auto& record = ev->Get()->GetRecord();

        switch (record.GetStatus()) {
        case NKikimrScheme::StatusSuccess: {
            const auto& desc = record.GetPathDescription();

            if (PendingDstPathId) {
                DstPathId = PendingDstPathId;
                if (IsReplica(desc.GetTable())) {
                    TString error;
                    if (!CheckReplicationConfig(desc.GetTable().GetReplicationConfig(), Mode, Consistency, error)) {
                        return Error(NKikimrScheme::StatusSchemeError, error);
                    }
                    return Success();
                }
                if (NotReplica(desc.GetTable())) {
                    Attaching = true;
                    NeedToCheck = true;
                    return AllocateTxId();
                }
                return Error(NKikimrScheme::StatusSchemeError, "Unexpected destination replication mode");
            }

            TString error;
            if (!CheckScheme(desc, error)) {
                return Error(NKikimrScheme::StatusSchemeError, error);
            } else {
                DstPathId = TPathId(record.GetPathOwnerId(), record.GetPathId());
                if (SkipInitialScan && !Attaching && !IsReplica(desc.GetTable())) {
                    Attaching = true;
                    NeedToCheck = true;
                    // The controller must persist this path before SchemeShard may alter it.
                    Send(Parent, new TEvPrivate::TEvPrepareAttachDst(ReplicationId, TargetId, DstPathId));
                    return Become(&TThis::StateWaitForPrepareAttachDstResult);
                }
                return Success();
            }
            break;
        }
        case NKikimrScheme::StatusPathDoesNotExist:
            if (SkipInitialScan) {
                return Error(record.GetStatus(), TStringBuilder() << "`" << DstPath << "` does not exist");
            }
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

    STATEFN(StateWaitForPrepareAttachDstResult) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateWaitForPrepareAttachDstResult"});
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvPrivate::TEvPrepareAttachDstResult, AllocateTxId);
        default:
            return StateBase(ev);
        }
    }

    bool CheckScheme(const NKikimrSchemeOp::TPathDescription& desc, TString& error) const {
        switch (Kind) {
        case TReplication::ETargetKind::Table:
        case TReplication::ETargetKind::IndexTable:
            return CheckTableScheme(desc.GetTable(), error);
        case TReplication::ETargetKind::Transfer:
            Y_ABORT("unreachable");
        }
    }

    bool CheckTableScheme(const NKikimrSchemeOp::TTableDescription& got, TString& error) const {
        if (!got.HasReplicationConfig() && (!SkipInitialScan || Attaching)) {
            error = "Empty replication config";
            return false;
        }

        if (!SkipInitialScan || Attaching || !NotReplica(got)) {
            if (!CheckReplicationConfig(got.GetReplicationConfig(), Mode, Consistency, error)) {
                return false;
            }
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
        THashMap<TStringBuf, const NKikimrSchemeOp::TColumnDescription*> columns;
        for (const auto& column : got.GetColumns()) {
            columns.emplace(column.GetName(), &column);
        }

        if (tableDesc->ColumnsSize() != columns.size()) {
            error = TStringBuilder() << "Columns size mismatch"
                << ": expected: " << tableDesc->ColumnsSize()
                << ", got: " << columns.size();
            return false;
        }

        // Compare family names instead of IDs: each cluster assigns its own IDs.
        THashMap<ui32, TStringBuf> gotFamilyNames;
        THashMap<TStringBuf, const NKikimrSchemeOp::TFamilyDescription*> families;
        gotFamilyNames.emplace(0, DefaultFamilyName);
        for (const auto& family : got.GetPartitionConfig().GetColumnFamilies()) {
            const auto name = GetFamilyName(family);
            if (name.empty()) {
                error = TStringBuilder() << "Unnamed non-default destination column family"
                    << ": id: " << family.GetId();
                return false;
            }
            gotFamilyNames[family.GetId()] = name;
            families.emplace(name, &family);
        }

        for (const auto& column : tableDesc->GetColumns()) {
            auto it = columns.find(column.GetName());
            if (it == columns.end()) {
                error = TStringBuilder() << "Cannot find column"
                    << ": name: " << column.GetName();
                return false;
            }

            if (column.GetType() != it->second->GetType()) {
                error = TStringBuilder() << "Column type mismatch"
                    << ": name: " << column.GetName()
                    << ", expected: " << column.GetType()
                    << ", got: " << it->second->GetType();
                return false;
            }

            const auto expectedFamily = GetColumnFamilyName(column);
            const auto name = gotFamilyNames.find(it->second->GetFamily());
            const TStringBuf actualFamily = name == gotFamilyNames.end() ? TStringBuf("<unknown>") : name->second;
            if (name == gotFamilyNames.end() || actualFamily != expectedFamily) {
                error = TStringBuilder() << "Column family mismatch"
                    << ": column: " << column.GetName()
                    << ", expected: " << expectedFamily
                    << ", got: " << actualFamily;
                return false;
            }
        }

        for (const auto& expected : tableDesc->GetPartitionConfig().GetColumnFamilies()) {
            const auto name = GetFamilyName(expected);
            auto it = families.find(name);
            if (it == families.end()) {
                error = TStringBuilder() << "Cannot find column family"
                    << ": name: " << name;
                return false;
            }

            if (!CheckColumnFamilySettings(expected, *it->second, error)) {
                return false;
            }

            families.erase(it);
        }

        for (const auto& item : families) {
            if (item.first != DefaultFamilyName) {
                error = TStringBuilder() << "Unexpected column family"
                    << ": name: " << item.first;
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
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateSubscribeDstPath"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardEvents::TEvNotifyDelete, Handle);
            hFunc(TSchemeBoardEvents::TEvNotifyUpdate, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TSchemeBoardEvents::TEvNotifyDelete::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        switch (Kind) {
        case TReplication::ETargetKind::Table:
        case TReplication::ETargetKind::IndexTable:
            return;
        case TReplication::ETargetKind::Transfer:
            return Error(NKikimrScheme::EStatus::StatusPathDoesNotExist,
                TStringBuilder() << "The target table `" << DstPath << "` does not exist");
        }
    }

    void Handle(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

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
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        if (SchemeShardId != ev->Get()->TabletId) {
            return;
        }

        Retry();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
        Retry();
    }

    void Success() {
        Y_ABORT_UNLESS(DstPathId);
        YDB_LOG_INFO("Success",
            {"dstPathId", DstPathId});

        Send(Parent, new TEvPrivate::TEvCreateDstResult(ReplicationId, TargetId, DstPathId));
        PassAway();
    }

    void Error(NKikimrScheme::EStatus status, const TString& error) {
        YDB_LOG_ERROR("Error",
            {"status", status},
            {"reason", error});

        Send(Parent, new TEvPrivate::TEvCreateDstResult(ReplicationId, TargetId, status, error));
        PassAway();
    }

    void Retry() {
        YDB_LOG_DEBUG("Retry");
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup);
    }

    void PassAway() override {
        if (const auto& actorId = std::exchange(Subscriber, {})) {
            Send(actorId, new TEvents::TEvPoison());
        }
        TActorBootstrapped<TDstCreator>::PassAway();
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
            const TString& dstPath,
            EReplicationMode mode,
            EConsistencyLevel consistency,
            const TString& database,
            bool skipInitialScan,
            const TPathId& pendingDstPathId)
        : Parent(parent)
        , SchemeShardId(schemeShardId)
        , YdbProxy(proxy)
        , PathId(pathId)
        , ReplicationId(rid)
        , TargetId(tid)
        , Kind(kind)
        , SrcPath(srcPath)
        , DstPath(dstPath)
        , Mode(mode)
        , Consistency(consistency)
        , SkipInitialScan(skipInitialScan)
        , LogPrefix(CreateActorLogPrefix("DstCreator", ReplicationId, TargetId))
        , Database(database)
        , PendingDstPathId(pendingDstPathId)
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
        if (PendingDstPathId) {
            return AllocateTxId();
        }
        switch (Kind) {
        case TReplication::ETargetKind::Table:
            return Resolve(PathId);
        case TReplication::ETargetKind::IndexTable:
            if (SkipInitialScan) {
                return Resolve(PathId);
            }
            [[fallthrough]];
        case TReplication::ETargetKind::Transfer:
            // indexed table will be created along with its indexes
            // transfer works with an existing table
            return SubscribeDstPath();
        }
    }

    STATEFN(StateBase) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateBase"});
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
    const EReplicationMode Mode;
    const EConsistencyLevel Consistency;
    const bool SkipInitialScan;
    const NActors::NStructuredLog::TStructuredMessage LogPrefix;

    TPathId DomainKey;
    TString Database;
    TString Owner;
    TTableProfiles TableProfiles;
    ui64 TxId = 0;
    NKikimrSchemeOp::TModifyScheme TxBody;
    TActorId PipeCache;
    bool NeedToCheck = false;
    bool Attaching = false;
    TPathId DstPathId;
    const TPathId PendingDstPathId;
    TActorId Subscriber;

}; // TDstCreator

static NKikimrSchemeOp::TTableReplicationConfig::EConsistencyLevel ConvertConsistencyLevel(EConsistencyLevel value) {
    switch (value) {
    case EConsistencyLevel::Row:
        return NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_LEVEL_ROW;
    case EConsistencyLevel::Global:
        return NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_LEVEL_GLOBAL;
    }
}

static NKikimrSchemeOp::TTableReplicationConfig::EReplicationMode ConvertMode(EReplicationMode value) {
    switch (value) {
    case EReplicationMode::ReadOnly:
        return NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY;
    }
}

void FillReplicationConfig(
        NKikimrSchemeOp::TTableReplicationConfig& out,
        EReplicationMode mode,
        EConsistencyLevel consistency)
{
    out.SetMode(ConvertMode(mode));
    out.SetConsistencyLevel(ConvertConsistencyLevel(consistency));
}

bool CheckReplicationConfig(
        const NKikimrSchemeOp::TTableReplicationConfig& in,
        EReplicationMode mode,
        EConsistencyLevel consistency,
        TString& error)
{
    if (in.GetMode() != ConvertMode(mode)) {
        error = TStringBuilder() << "Replication mode mismatch"
            << ": expected: " << ConvertMode(mode)
            << ", got: " << static_cast<int>(in.GetMode());
        return false;
    }

    if (in.GetConsistencyLevel() != ConvertConsistencyLevel(consistency)) {
        error = TStringBuilder() << "Replication consistency level mismatch"
            << ": expected: " << ConvertConsistencyLevel(consistency)
            << ", got: " << static_cast<int>(in.GetConsistencyLevel());
        return false;
    }

    return true;
}

static EConsistencyLevel ConvertConsistencyLevel(const NKikimrReplication::TConsistencySettings& settings) {
    switch (settings.GetLevelCase()) {
    case NKikimrReplication::TConsistencySettings::kRow:
        return EConsistencyLevel::Row;
    case NKikimrReplication::TConsistencySettings::kGlobal:
        return EConsistencyLevel::Global;
    default:
        Y_ABORT("Unexpected consistency level");
    }
}

IActor* CreateDstCreator(TReplication* replication, ui64 targetId, const TActorContext& ctx) {
    const auto* target = replication->FindTarget(targetId);
    Y_ABORT_UNLESS(target);

    return CreateDstCreator(ctx.SelfID, replication->GetSchemeShardId(), replication->GetYdbProxy(),
        replication->GetDatabase(), replication->GetPathId(),
        replication->GetId(), target->GetId(), target->GetKind(), target->GetSrcPath(), target->GetDstPath(),
        EReplicationMode::ReadOnly, ConvertConsistencyLevel(replication->GetConfig().GetConsistencySettings()),
        replication->GetConfig().GetSkipInitialScan(), target->GetPendingDstPathId());
}

IActor* CreateDstCreator(const TActorId& parent, ui64 schemeShardId, const TActorId& proxy,
        const TString& database, const TPathId& pathId,
        ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TString& srcPath, const TString& dstPath,
        EReplicationMode mode, EConsistencyLevel consistency, bool skipInitialScan,
        const TPathId& pendingDstPathId)
{
    return new TDstCreator(parent, schemeShardId, proxy, pathId, rid, tid, kind, srcPath, dstPath, mode, consistency, database, skipInitialScan, pendingDstPathId);
}

}
