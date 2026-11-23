#include "dst_creator.h"
#include "logging.h"
#include "private_events.h"
#include "util.h"
#include "topic_mirror.h"

#include <ydb/core/base/path.h>
#include <ydb/core/persqueue/public/schema/schema_propose.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
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
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

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

bool AreLiteralDefaultsEqual(
        const NKikimrSchemeOp::TColumnDescription& expected,
        const NKikimrSchemeOp::TColumnDescription& actual)
{
    if (expected.HasDefaultFromLiteral() != actual.HasDefaultFromLiteral()) {
        return false;
    }

    if (!expected.HasDefaultFromLiteral()) {
        return true;
    }

    const auto& expectedDefault = expected.GetDefaultFromLiteral();
    const auto& actualDefault = actual.GetDefaultFromLiteral();
    return expectedDefault.SerializeAsString() == actualDefault.SerializeAsString();
}

bool IsImported(const NKikimrSchemeOp::TTableDescription& table) {
    return table.GetReplicationConfig().GetImportState() == NKikimrSchemeOp::TTableReplicationConfig::IMPORTED;
}

} // anonymous namespace

class TDstCreator: public TActorBootstrapped<TDstCreator> {
    bool IsStandby() const {
        return DesiredState == TReplication::EState::Ready;
    }

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
                    .WithSetVal(true)
                    .WithKeyShardBoundary(true)),
                    0,
                    RetryGeneration);
            }
            break;
        case TReplication::ETargetKind::Topic:
            Send(YdbProxy, new TEvYdbProxy::TEvDescribeTopicRequest(SrcPath, {}), 0, RetryGeneration);
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
            hFunc(TEvYdbProxy::TEvDescribeTopicResponse, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
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
        if (ev->Cookie != RetryGeneration) {
            return;
        }

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
        const auto description = result.GetTableDescription();
        description.SerializeTo(scheme);

        // SDK table descriptions do not retain literal defaults when serialized.
        const auto& source = NYdb::TProtoAccessor::GetProto(description);
        THashMap<TStringBuf, const Ydb::TypedValue*> literalDefaults;
        for (const auto& column : source.columns()) {
            if (column.has_from_literal()) {
                literalDefaults.emplace(column.name(), &column.from_literal());
            }
        }
        for (auto& column : *scheme.mutable_columns()) {
            if (const auto it = literalDefaults.find(column.name()); it != literalDefaults.end()) {
                column.mutable_from_literal()->CopyFrom(*it->second);
            }
        }

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

        // Sequences (e.g. for serial columns) are only created by the indexed flavor of the create
        // operation, so it is required whenever some column defaults from a sequence.
        bool hasSequences = false;
        for (const auto& column : scheme.columns()) {
            if (column.has_from_sequence()) {
                hasSequences = true;
                break;
            }
        }
        const bool indexedTable = scheme.indexes_size() || hasSequences;

        if (!FillTableDescription(TxBody, scheme, TableProfiles, status, error, indexedTable)) {
            return Error(NKikimrScheme::StatusSchemeError, error);
        }

        std::pair<TString, TString> pathPair;
        if (!TrySplitPathByDb(DstPath, Database, pathPair, error)) {
            return Error(NKikimrScheme::StatusSchemeError, error);
        }

        TxBody.SetWorkingDir(pathPair.first);

        NKikimrSchemeOp::TTableDescription* desc = nullptr;
        if (indexedTable) {
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

    void Handle(TEvYdbProxy::TEvDescribeTopicResponse::TPtr& ev) {
        if (ev->Cookie != RetryGeneration) {
            return;
        }

        const auto& result = ev->Get()->Result;
        if (!result.IsSuccess()) {
            if (IsRetryableError(result)) {
                return Retry();
            }

            return Error(ConvertStatus(result.GetStatus()), result.GetIssues().ToOneLineString());
        }

        if (TopicMirror.Error) {
            return Error(NKikimrScheme::StatusInvalidParameter, TopicMirror.Error);
        }

        auto request = BuildTopicMirrorRequest(result.GetTopicDescription(), TopicMirror.Config->GetConsumer());

        std::pair<TString, TString> path;
        TString error;
        if (!TrySplitPathByDb(DstPath, Database, path, error)) {
            return Error(NKikimrScheme::StatusInvalidParameter, error);
        }

        TxBody.Clear();
        auto proposal = NPQ::NSchema::ProposeCreateTopic(TxBody, std::move(request), Database, path.first, path.second);
        if (!proposal) {
            return Error(NKikimrScheme::StatusInvalidParameter, proposal.GetErrorMessage());
        }

        // Start mirroring only after the controller has persisted the destination id.
        TxBody.MutableCreatePersQueueGroup()->MutablePQTabletConfig()
            ->SetReplicationSourceConsumer(TopicMirror.Config->GetConsumer());

        AllocateTxId();
    }

    void FillReplicationConfig(NKikimrSchemeOp::TTableReplicationConfig& replicationConfig) const {
        NController::FillReplicationConfig(replicationConfig, Mode, Consistency);
        if (CreatePlaceholder) {
            replicationConfig.SetImportState(NKikimrSchemeOp::TTableReplicationConfig::WAITING_IMPORT);
        }
        if (AssignOwner) {
            PathId.ToProto(replicationConfig.MutableOwnerPathId());
        }
    }

    void AllocateTxId() {
        WaitingTxId = 0;
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
        if (DropIndexTx.HasDropIndex()) {
            ExecuteSchemeTx(DropIndexTx);
        } else if (SwitchoverTx.HasAlterTable()) {
            ExecuteSchemeTx(SwitchoverTx);
        } else if (InitialScan || CreatePlaceholder || Kind == TReplication::ETargetKind::Topic) {
            CreateDst();
        } else {
            DescribeDstPath();
        }
    }

    void CreateDst() {
        ExecuteSchemeTx(TxBody);
    }

    bool ShouldDropVectorIndex(const NKikimrSchemeOp::TIndexDescription& index) const {
        return !InitialScan && !CreatePlaceholder
            && Kind == TReplication::ETargetKind::Table
            && index.GetType() == NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree;
    }

    void PrepareDropIndex(const NKikimrSchemeOp::TIndexDescription& index,
            const NKikimrScheme::TEvDescribeSchemeResult& desc)
    {
        const auto parts = SplitPath(desc.GetPath());
        Y_ABORT_UNLESS(!parts.empty());
        DropIndexTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropIndex);
        DropIndexTx.SetInternal(true);
        DropIndexTx.SetWorkingDir(TString(ExtractParent(desc.GetPath())));
        DropIndexTx.MutableDropIndex()->SetTableName(parts.back());
        DropIndexTx.MutableDropIndex()->SetIndexName(index.GetName());

        // DROP INDEX is addressed by name. Bind it to the table and index we
        // validated so a concurrent schema change cannot redirect the removal.
        auto* tableCondition = DropIndexTx.AddApplyIf();
        tableCondition->SetPathId(DstPathId.LocalPathId);
        tableCondition->SetPathVersion(desc.GetPathDescription().GetSelf().GetPathVersion());
        DropIndexTx.AddApplyIf()->SetPathId(index.GetLocalPathId());
        NeedToCheck = true;
        AllocateTxId();
    }

    void PrepareSwitchover(const NKikimrScheme::TEvDescribeSchemeResult& desc) {
        // Demote the table the replication did not create: the same internal alter the controller
        // already uses to release a destination on DONE, in the opposite direction.
        SwitchoverTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
        SwitchoverTx.SetInternal(true);
        DstPathId.ToProto(SwitchoverTx.MutableAlterTable()->MutablePathId());
        auto& config = *SwitchoverTx.MutableAlterTable()->MutableReplicationConfig();
        FillReplicationConfig(config);
        // The data is already in place, which is what the attach path means by a complete import.
        config.SetImportState(NKikimrSchemeOp::TTableReplicationConfig::IMPORTED);

        // Bind the demote to the table that was just validated, so a concurrent schema change
        // cannot redirect it.
        auto* condition = SwitchoverTx.AddApplyIf();
        condition->SetPathId(DstPathId.LocalPathId);
        SwitchoverPathVersion = desc.GetPathDescription().GetSelf().GetPathVersion();
        condition->SetPathVersion(SwitchoverPathVersion);

        // Describe again afterwards: the ordinary replica checks then apply to the demoted table,
        // so a restart in between simply re-runs the validation and finds nothing left to do.
        NeedToCheck = true;
        AllocateTxId();
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
            hFunc(TEvents::TEvWakeup, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
        const auto& record = ev->Get()->Record;
        if (record.GetTxId() != TxId) {
            return;
        }

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
            } else if (DropIndexTx.HasDropIndex()) {
                DropIndexTx.Clear();
                Become(&TThis::StateDescribeDstPath);
                return Retry();
            } else if (SwitchoverTx.HasAlterTable()) {
                SwitchoverTx.Clear();
                Become(&TThis::StateDescribeDstPath);
                return Retry();
            } else {
                return Error(record.GetStatus(), record.GetReason());
            }
            break;
        case NKikimrScheme::StatusAlreadyExists:
            return DescribeDstPath();
        case NKikimrScheme::StatusPreconditionFailed:
        case NKikimrScheme::StatusPathDoesNotExist:
            if (DropIndexTx.HasDropIndex()) {
                return DescribeDstPath();
            }
            if (SwitchoverTx.HasAlterTable()) {
                // Either the ApplyIf no longer holds -- the destination moved under us, possibly
                // because the demote itself committed and its reply was lost -- or the alter is
                // refused outright, as it is for a table in a backup collection. Describing the
                // destination again tells the two apart, so keep the refusal: it is the answer if
                // the table comes back unchanged.
                SwitchoverRefusal = TRefusal{
                    .PathVersion = SwitchoverPathVersion,
                    .Status = record.GetStatus(),
                    .Reason = record.GetReason(),
                };
                SwitchoverTx.Clear();
                return DescribeDstPath();
            }
            return Error(record.GetStatus(), record.GetReason());
        default:
            return Error(record.GetStatus(), record.GetReason());
        }
    }

    void SubscribeTx(ui64 txId) {
        YDB_LOG_DEBUG("Subscribe tx",
            {"txId", txId});
        WaitingTxId = txId;
        Send(PipeCache, new TEvPipeCache::TEvForward(new TEvSchemeShard::TEvNotifyTxCompletion(txId), SchemeShardId));
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        if (!WaitingTxId || ev->Get()->Record.GetTxId() != WaitingTxId) {
            return;
        }

        if (NeedToCheck) {
            DescribeDstPath();
        } else {
            Success();
        }
    }

    void DescribeDstPath() {
        DropIndexTx.Clear();
        Send(PipeCache, new TEvPipeCache::TEvForward(
            new TEvSchemeShard::TEvDescribeScheme(DstPath), SchemeShardId));
        Become(&TThis::StateDescribeDstPath);
    }

    STATEFN(StateDescribeDstPath) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateDescribeDstPath"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
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

            TString error;
            if (!CheckScheme(desc, error)) {
                return Error(NKikimrScheme::StatusSchemeError, error);
            }

            DstPathId = TPathId(record.GetPathOwnerId(), record.GetPathId());
            if (!IsOwnedByThisReplication(desc.GetTable())) {
                return Error(NKikimrScheme::StatusPreconditionFailed, TStringBuilder()
                    << "`" << DstPath << "` is owned by another replication");
            }
            for (const auto& index : desc.GetTable().GetTableIndexes()) {
                if (ShouldDropVectorIndex(index)) {
                    return PrepareDropIndex(index, record);
                }
            }
            if (NeedsSwitchover(desc.GetTable())) {
                const auto version = desc.GetSelf().GetPathVersion();
                if (SwitchoverRefusal && SwitchoverRefusal->PathVersion == version) {
                    // Nothing about the destination has changed, so the refused demote would only
                    // be refused again. Report the reason instead of retrying it forever.
                    return Error(SwitchoverRefusal->Status, SwitchoverRefusal->Reason);
                }
                return PrepareSwitchover(record);
            }
            return Success();
        }
        case NKikimrScheme::StatusPathDoesNotExist:
            if (!InitialScan && Kind != TReplication::ETargetKind::Topic) {
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

    bool CheckScheme(const NKikimrSchemeOp::TPathDescription& desc, TString& error) const {
        switch (Kind) {
        case TReplication::ETargetKind::Table:
        case TReplication::ETargetKind::IndexTable:
            return CheckTableScheme(desc.GetTable(), error);
        case TReplication::ETargetKind::Topic: {
            const auto& expectedConsumer = TopicMirror.Config->GetConsumer();
            const auto& actualConsumer = desc.GetPersQueueGroup().GetPQTabletConfig().GetReplicationSourceConsumer();
            if (desc.HasPersQueueGroup() && !expectedConsumer.empty() && actualConsumer == expectedConsumer) {
                return true;
            }
            error = "Destination topic was not created by this replication";
            return false;
        }
        case TReplication::ETargetKind::Transfer:
            Y_ABORT("unreachable");
        }
    }

    bool IsOwnedByThisReplication(const NKikimrSchemeOp::TTableDescription& table) const {
        const auto& config = table.GetReplicationConfig();
        // Tables without an owner keep the legacy behavior.
        return !config.HasOwnerPathId() || TPathId::FromProto(config.GetOwnerPathId()) == PathId;
    }

    // A destination bootstrapped by SWITCHOVER is an ordinary writable table until the controller
    // demotes it, so the replica configuration checks cannot be applied to it yet. Everything else
    // -- keys, columns, families -- is still verified before anything is altered.
    bool NeedsSwitchover(const NKikimrSchemeOp::TTableDescription& got) const {
        return Switchover
            && got.GetReplicationConfig().GetMode() == NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE;
    }

    bool CheckTableScheme(const NKikimrSchemeOp::TTableDescription& got, TString& error) const {
        if (!NeedsSwitchover(got)) {
            if (!got.HasReplicationConfig()) {
                error = TStringBuilder() << "Cannot attach destination table '" << DstPath
                    << "': replication configuration is missing. Recreate the replication and import data"
                    << " into its destination tables before switching to STANDBY";
                return false;
            }

            if (!CheckReplicationConfig(got.GetReplicationConfig(), Mode, Consistency, error)) {
                return false;
            }

            if (!InitialScan && IsStandby() && !IsImported(got)) {
                error = "Destination table import is not complete (expected IMPORTED)";
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

            if (!AreLiteralDefaultsEqual(column, *it->second)) {
                error = TStringBuilder() << "Column literal default mismatch"
                    << ": name: " << column.GetName();
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
            if (ShouldDropVectorIndex(index)) {
                continue;
            }
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
        case TReplication::ETargetKind::Topic:
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

        if (Kind == TReplication::ETargetKind::IndexTable && !IsOwnedByThisReplication(desc.GetPathDescription().GetTable())) {
            return Error(NKikimrScheme::StatusPreconditionFailed, "Index table is not owned by this replication");
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

        Send(Parent, new TEvPrivate::TEvCreateDstResult(ReplicationId, TargetId, DstPathId, CreatePlaceholder));
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
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup(RetryGeneration));
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag != RetryGeneration) {
            return;
        }

        const auto state = CurrentStateFunc();
        if (state == &TThis::StateDescribeSrcPath) {
            DescribeSrcPath();
        } else if (state == &TThis::StateDescribeDstPath) {
            DescribeDstPath();
        } else if (state == &TThis::StateExecuteSchemeTx) {
            if (SwitchoverTx.HasAlterTable()) {
                // The demote may already have committed: its reply or completion notification can
                // be lost when the pipe breaks. Replaying it would be refused, because its ApplyIf
                // names the path version from before the demote bumped it, and the destination
                // would be left a read-only replica with no workers. Reconcile instead -- a fresh
                // describe either finds it already demoted, or prepares the demote anew.
                SwitchoverTx.Clear();
                return DescribeDstPath();
            }
            AllocateTxId();
        }
    }

    void Handle(TEvPrivate::TEvUpdateYdbProxy::TPtr& ev) {
        if (YdbProxy == ev->Get()->YdbProxy) {
            return;
        }

        YdbProxy = ev->Get()->YdbProxy;
        if (CurrentStateFunc() == &TThis::StateDescribeSrcPath) {
            // The old proxy discards its outstanding requests on shutdown.
            // Ignore late replies/retries and validate using the new credentials.
            ++RetryGeneration;
            DescribeSrcPath();
        }
        // Other states either have not used the source yet or are already
        // reconciling local DDL. Keep their requests and retries intact.
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
            bool initialScan,
            TReplication::EState desiredState,
            bool switchover,
            TTopicMirrorSettings&& topicMirror)
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
        , InitialScan(initialScan)
        , Switchover(switchover)
        // A switchover destination already exists and holds its data: there is no placeholder to
        // create for it, paused or not.
        , CreatePlaceholder(kind != TReplication::ETargetKind::Topic
            && !initialScan
            && !switchover
            && desiredState == TReplication::EState::Paused)
        , DesiredState(desiredState)
        , TopicMirror(std::move(topicMirror))
        , LogPrefix(CreateActorLogPrefix("DstCreator", ReplicationId, TargetId))
        , Database(database)
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
        AssignOwner = AppData()->FeatureFlags.GetEnableAsyncReplicationTableOwnership();
        switch (Kind) {
        case TReplication::ETargetKind::Table:
        case TReplication::ETargetKind::Topic:
            return Resolve(PathId);
        case TReplication::ETargetKind::IndexTable:
            if (!InitialScan && !CreatePlaceholder) {
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
            hFunc(TEvPrivate::TEvUpdateYdbProxy, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 SchemeShardId;
    TActorId YdbProxy;
    const TPathId PathId;
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TReplication::ETargetKind Kind;
    const TString SrcPath;
    const TString DstPath;
    const EReplicationMode Mode;
    const EConsistencyLevel Consistency;
    const bool InitialScan;
    const bool Switchover;
    const bool CreatePlaceholder;
    const TReplication::EState DesiredState;
    const TTopicMirrorSettings TopicMirror;
    const NActors::NStructuredLog::TStructuredMessage LogPrefix;

    TPathId DomainKey;
    TString Database;
    TString Owner;
    TTableProfiles TableProfiles;
    ui64 TxId = 0;
    ui64 WaitingTxId = 0;
    NKikimrSchemeOp::TModifyScheme TxBody;
    NKikimrSchemeOp::TModifyScheme DropIndexTx;
    NKikimrSchemeOp::TModifyScheme SwitchoverTx;
    ui64 SwitchoverPathVersion = 0;
    struct TRefusal {
        ui64 PathVersion;
        NKikimrScheme::EStatus Status;
        TString Reason;
    };
    std::optional<TRefusal> SwitchoverRefusal;
    TActorId PipeCache;
    bool NeedToCheck = false;
    bool AssignOwner = false;
    ui64 RetryGeneration = 0;
    TPathId DstPathId;
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

    if (target->GetKind() == TReplication::ETargetKind::Topic) {
        TTopicMirrorSettings topicMirror(replication->GetConfig().GetSrcConnectionParams(),
            target->GetSrcPath(), target->GetStreamConsumerName());
        return CreateDstTopicCreator(ctx.SelfID, replication->GetSchemeShardId(), replication->GetYdbProxy(),
            replication->GetDatabase(), replication->GetPathId(),
            replication->GetId(), target->GetId(), target->GetSrcPath(), target->GetDstPath(), std::move(topicMirror));
    }

    return CreateDstCreator(ctx.SelfID, replication->GetSchemeShardId(), replication->GetYdbProxy(),
        replication->GetDatabase(), replication->GetPathId(),
        replication->GetId(), target->GetId(), target->GetKind(), target->GetSrcPath(), target->GetDstPath(),
        EReplicationMode::ReadOnly, ConvertConsistencyLevel(replication->GetConfig().GetConsistencySettings()),
        replication->GetConfig().GetInitialScan(), replication->GetDesiredState(),
        IsSwitchover(replication->GetConfig()));
}

IActor* CreateDstCreator(const TActorId& parent, ui64 schemeShardId, const TActorId& proxy,
        const TString& database, const TPathId& pathId,
        ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TString& srcPath, const TString& dstPath,
        EReplicationMode mode, EConsistencyLevel consistency, bool initialScan,
        TReplication::EState desiredState, bool switchover)
{
    return new TDstCreator(parent, schemeShardId, proxy, pathId, rid, tid, kind, srcPath, dstPath, mode, consistency, database, initialScan, desiredState, switchover, {});
}

IActor* CreateDstTopicCreator(const TActorId& parent, ui64 schemeShardId, const TActorId& proxy,
        const TString& database, const TPathId& pathId,
        ui64 rid, ui64 tid, const TString& srcPath, const TString& dstPath,
        TTopicMirrorSettings&& topicMirror)
{
    return new TDstCreator(parent, schemeShardId, proxy, pathId, rid, tid, TReplication::ETargetKind::Topic,
        srcPath, dstPath, EReplicationMode::ReadOnly, EConsistencyLevel::Row, database, true,
        TReplication::EState::Ready, false, std::move(topicMirror));
}

}
