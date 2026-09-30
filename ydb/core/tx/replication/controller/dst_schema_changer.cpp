#include "dst_schema_changer.h"

#include "logging.h"
#include "private_events.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/replication/common/family_settings.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash.h>
#include <util/stream/output.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

using namespace NSchemeShard;

namespace {

using TFamily = NKikimrReplication::TSchemaChange::TFamily;

bool HasSupportedFamilySettings(const NKikimrSchemeOp::TFamilyDescription& family) {
    if (family.HasColumnCodec()) {
        const auto codec = family.GetColumnCodec();
        if (codec != NKikimrSchemeOp::ColumnCodecPlain && codec != NKikimrSchemeOp::ColumnCodecLZ4) {
            return false;
        }
    } else if (family.GetCodec() > 1) {
        return false;
    }

    const auto cacheMode = family.GetColumnCacheMode();
    return cacheMode == NKikimrSchemeOp::ColumnCacheModeRegular
        || cacheMode == NKikimrSchemeOp::ColumnCacheModeTryKeepInMemory;
}

} // anonymous namespace

// Executes the data-plane schema change after the controller has parked every
// worker at the same CDC barrier. It always describes before proposing and
// after completion: a completed SchemeShard notification alone is not enough
// to distinguish a replay from a conflicting concurrent DDL.
class TSchemaChangeDstAlterer: public TActorBootstrapped<TSchemaChangeDstAlterer> {
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
        YDB_LOG_TRACE("Allocated tx id",
            {"txId", ev->Get()->TxId});

        PipeCache = ev->Get()->Services.LeaderPipeCache;
        if (TxId) {
            if (Kind != TReplication::ETargetKind::Table || !DstPathId) {
                return Error(NKikimrScheme::StatusInvalidParameter,
                    "schema changes are supported only for existing table targets");
            }
            return DescribeDst();
        }

        TxId = ev->Get()->TxId;
        Send(Parent, new TEvPrivate::TEvSchemaChangeDstAlterTxId(ReplicationId, TargetId, TxId));
        Become(&TThis::StatePersistTxId);
    }

    STATEFN(StatePersistTxId) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvSchemaChangeDstAlterTxIdSaved, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvPrivate::TEvSchemaChangeDstAlterTxIdSaved::TPtr& ev) {
        TxId = ev->Get()->TxId;
        if (Kind != TReplication::ETargetKind::Table || !DstPathId) {
            return Error(NKikimrScheme::StatusInvalidParameter,
                "schema changes are supported only for existing table targets");
        }
        DescribeDst();
    }

    void DescribeDst() {
        Send(PipeCache, new TEvPipeCache::TEvForward(
            new TEvSchemeShard::TEvDescribeScheme(DstPathId), SchemeShardId));
        Become(&TThis::StateDescribeDst);
    }

    STATEFN(StateDescribeDst) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateDescribeDst"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            sFunc(TEvents::TEvWakeup, DescribeDst);
        default:
            return StateBase(ev);
        }
    }

    bool CheckColumnFamily(
            const NKikimrSchemeOp::TColumnDescription& column,
            const NKikimrReplication::TSchemaChange::TColumn& desired,
            const THashMap<ui32, TStringBuf>& familyNames,
            TString& error)
    {
        // Family IDs are local to the destination cluster.
        const auto family = familyNames.find(column.GetFamily());
        if (family == familyNames.end() || family->second.empty()) {
            error = TStringBuilder() << "destination column '" << column.GetName()
                << "' refers to unknown family id " << column.GetFamily();
            return false;
        }

        if (family->second == desired.GetFamily()) {
            return true;
        }

        auto* changed = Alter.MutableAlterTable()->AddColumns();
        changed->SetName(column.GetName());
        changed->SetFamilyName(desired.GetFamily());
        error = TStringBuilder() << "column '" << column.GetName() << "' family"
            << ": expected " << desired.GetFamily()
            << ", actual " << family->second;

        return true;
    }

    bool CheckAndBuildFamily(
            const NKikimrReplication::TSchemaChange::TFamily& desired,
            const NKikimrSchemeOp::TFamilyDescription* actual,
            TString& error)
    {
        if (actual && !HasSupportedFamilySettings(*actual)) {
            error = TStringBuilder() << "destination column family '" << desired.GetName()
                << "' has unsupported compression or cache mode";
            return false;
        }

        const TFamilySettings expectedSettings{
            .Media = desired.GetMedia(),
            .Compression = desired.GetCompression(),
            .CacheMode = desired.GetCacheMode(),
        };
        const auto actualSettings = TFamilySettings::FromProto(actual);
        if (actual && expectedSettings == actualSettings) {
            return true;
        }

        if (actual && actualSettings.Media && !expectedSettings.Media) {
            error = TStringBuilder() << "column family '" << desired.GetName()
                << "' has explicit destination media '" << actualSettings.Media << "' but source media is unspecified";
            return false;
        }

        auto* changed = Alter.MutableAlterTable()->MutablePartitionConfig()->AddColumnFamilies();
        changed->SetName(desired.GetName());
        if (expectedSettings.Media && actualSettings.Media != expectedSettings.Media) {
            auto* data = changed->MutableStorageConfig()->MutableData();
            data->SetPreferredPoolKind(desired.GetMedia());
            data->SetAllowOtherKinds(false);
        }

        if (!actual || actualSettings.Compression != expectedSettings.Compression) {
            changed->SetColumnCodec(desired.GetCompression() == TFamily::COMPRESSION_LZ4
                ? NKikimrSchemeOp::ColumnCodecLZ4 : NKikimrSchemeOp::ColumnCodecPlain);
        }

        if (!actual || actualSettings.CacheMode != expectedSettings.CacheMode) {
            changed->SetColumnCacheMode(desired.GetCacheMode() == TFamily::CACHE_MODE_IN_MEMORY
                ? NKikimrSchemeOp::ColumnCacheModeTryKeepInMemory
                : NKikimrSchemeOp::ColumnCacheModeRegular);
        }

        if (!actual && desired.GetName() != DefaultFamilyName) {
            error = TStringBuilder() << "missing column family '" << desired.GetName() << "'";
        } else {
            error = TStringBuilder() << "column family '" << desired.GetName() << "' differs"
                << ": expected " << expectedSettings
                << ", actual " << actualSettings;
        }

        return true;
    }

    bool CheckAndBuildFamilies(const NKikimrSchemeOp::TTableDescription& current, TString& error) {
        THashSet<TString> desiredFamilies;
        for (const auto& family : DesiredSchema.GetFamilies()) {
            if (!family.GetName() || !desiredFamilies.insert(family.GetName()).second) {
                error = "schema change contains an invalid or duplicate column family";
                return false;
            }
        }

        for (const auto& column : DesiredSchema.GetColumns()) {
            if (!desiredFamilies.contains(column.GetFamily())) {
                error = TStringBuilder() << "column '" << column.GetName() << "' names unknown family '"
                    << column.GetFamily() << "'";
                return false;
            }
        }

        THashMap<TString, const NKikimrSchemeOp::TFamilyDescription*> currentFamilies;
        for (const auto& family : current.GetPartitionConfig().GetColumnFamilies()) {
            const TString name(GetFamilyName(family));
            if (name.empty() || !currentFamilies.emplace(name, &family).second) {
                error = "destination table has an invalid or duplicate column family";
                return false;
            }

            if (!desiredFamilies.contains(name)) {
                error = TStringBuilder() << "destination table has unexpected column family '" << name << "'";
                return false;
            }
        }

        for (const auto& desired : DesiredSchema.GetFamilies()) {
            const auto it = currentFamilies.find(desired.GetName());
            const auto* actual = it == currentFamilies.end() ? nullptr : it->second;
            if (!CheckAndBuildFamily(desired, actual, error)) {
                return false;
            }
        }

        return true;
    }

    bool CheckAndBuildAlter(const NKikimrSchemeOp::TTableDescription& current, TString& error) {
        if (current.KeyColumnNamesSize() != DesiredSchema.PrimaryKeyColumnNamesSize()) {
            error = "primary-key column count differs from the source schema";
            return false;
        }

        THashSet<TString> desiredKeys;
        for (ui32 i = 0; i < current.KeyColumnNamesSize(); ++i) {
            const auto& key = DesiredSchema.GetPrimaryKeyColumnNames(i);
            if (current.GetKeyColumnNames(i) != key || !desiredKeys.insert(key).second) {
                error = "primary-key columns differ from the source schema";
                return false;
            }
        }

        THashMap<TString, const NKikimrReplication::TSchemaChange::TColumn*> desiredColumns;
        for (const auto& column : DesiredSchema.GetColumns()) {
            if (!column.GetName() || !column.GetType() || !desiredColumns.emplace(column.GetName(), &column).second) {
                error = "schema change contains an invalid or duplicate column";
                return false;
            }
        }
        for (const auto& key : desiredKeys) {
            if (!desiredColumns.contains(key)) {
                error = "schema change key does not name a column";
                return false;
            }
        }

        THashSet<TString> currentColumns;
        Alter.Clear();
        Alter.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
        Alter.SetInternal(true);
        DstPathId.ToProto(Alter.MutableAlterTable()->MutablePathId());

        const bool hasFamilies = DesiredSchema.FamiliesSize();
        THashMap<ui32, TStringBuf> familyNames;
        if (hasFamilies) {
            familyNames.emplace(0, DefaultFamilyName);
            for (const auto& family : current.GetPartitionConfig().GetColumnFamilies()) {
                familyNames.emplace(family.GetId(), GetFamilyName(family));
            }
        }

        for (const auto& column : current.GetColumns()) {
            if (!currentColumns.insert(column.GetName()).second) {
                error = "destination table has duplicate column names";
                return false;
            }
            const auto desired = desiredColumns.find(column.GetName());
            if (desired == desiredColumns.end()) {
                if (desiredKeys.contains(column.GetName())) {
                    error = TStringBuilder() << "attempt to drop primary-key column: " << column.GetName();
                    return false;
                }
                Alter.MutableAlterTable()->AddDropColumns()->SetName(column.GetName());
                error = TStringBuilder() << "unexpected destination column '" << column.GetName() << "'";
                continue;
            }
            if (desired->second->GetType() != column.GetType()) {
                error = TStringBuilder() << "column type differs from source schema: " << column.GetName()
                    << ", source: " << desired->second->GetType()
                    << ", destination: " << column.GetType();
                return false;
            }
            if (hasFamilies && !CheckColumnFamily(column, *desired->second, familyNames, error)) {
                return false;
            }
        }

        for (const auto& desired : DesiredSchema.GetColumns()) {
            if (currentColumns.contains(desired.GetName())) {
                continue;
            }

            auto* added = Alter.MutableAlterTable()->AddColumns();
            added->SetName(desired.GetName());
            added->SetType(desired.GetType());
            if (hasFamilies) {
                added->SetFamilyName(desired.GetFamily());
            }

            error = TStringBuilder() << "missing destination column '" << desired.GetName() << "'";
        }

        if (hasFamilies && !CheckAndBuildFamilies(current, error)) {
            return false;
        }

        return true;
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        const auto& record = ev->Get()->GetRecord();
        switch (record.GetStatus()) {
        case NKikimrScheme::StatusSuccess:
            break;
        case NKikimrScheme::StatusPathDoesNotExist:
        case NKikimrScheme::StatusSchemeError:
        case NKikimrScheme::StatusAccessDenied:
        case NKikimrScheme::StatusInvalidParameter:
        case NKikimrScheme::StatusPreconditionFailed:
            return Error(record.GetStatus(), TStringBuilder() << "cannot describe destination table: " << record.GetReason());
        default:
            return RetryDescribe();
        }

        TString error;
        if (!CheckAndBuildAlter(record.GetPathDescription().GetTable(), error)) {
            return Error(NKikimrScheme::StatusPreconditionFailed, error);
        }

        const auto& alter = Alter.GetAlterTable();
        const bool hasChanges = alter.ColumnsSize()
            || alter.DropColumnsSize()
            || alter.GetPartitionConfig().ColumnFamiliesSize();
        if (!hasChanges) {
            // SchemeShard publishes the new description at planning time,
            // before all destination shards finish ProposedWaitParts. The
            // persisted TxId may still own an in-flight DDL after recovery.
            if (!AlterCompletionConfirmed) {
                return SubscribeTx(TxId);
            }

            return Success();
        }

        if (AlterCompletionConfirmed) {
            return Error(NKikimrScheme::StatusPreconditionFailed,
                TStringBuilder() << "destination schema differs after the DDL transaction completed: " << error);
        }

        ProposeAlter();
    }

    void ProposeAlter() {
        auto ev = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(TxId, SchemeShardId);
        *ev->Record.AddTransaction() = Alter;
        Send(PipeCache, new TEvPipeCache::TEvForward(ev.Release(), SchemeShardId, true));
        Become(&TThis::StateProposeAlter);
    }

    STATEFN(StateProposeAlter) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateProposeAlter"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            hFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            sFunc(TEvents::TEvWakeup, AllocateTxId);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        switch (record.GetStatus()) {
        case NKikimrScheme::StatusAccepted:
            if (record.GetTxId() != TxId) {
                return Error(NKikimrScheme::StatusSchemeError, "SchemeShard returned an unexpected transaction id");
            }
            return SubscribeTx(record.GetTxId());
        case NKikimrScheme::StatusMultipleModifications:
        case NKikimrScheme::StatusResourceExhausted:
            return RetryAllocate();
        case NKikimrScheme::StatusAlreadyExists:
            // A lost proposal result can race its own completed DDL. Never
            // treat this as success blindly: re-describe and release workers
            // only if the destination exactly matches DesiredSchema.
            return DescribeDst();
        default:
            if (record.GetReason().find("unable determine pool") != TString::npos) {
                TStringBuilder reason;
                reason << record.GetReason();
                for (const auto& family : DesiredSchema.GetFamilies()) {
                    if (family.HasMedia()) {
                        reason << "; family '" << family.GetName() << "' requires pool kind '"
                            << family.GetMedia() << "'";
                    }
                }
                return Error(record.GetStatus(), reason);
            }
            return Error(record.GetStatus(), record.GetReason());
        }
    }

    void SubscribeTx(ui64 txId) {
        Send(PipeCache, new TEvPipeCache::TEvForward(
            new TEvSchemeShard::TEvNotifyTxCompletion(txId), SchemeShardId));
        Become(&TThis::StateProposeAlter);
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        if (ev->Get()->Record.GetTxId() != TxId) {
            return Error(NKikimrScheme::StatusSchemeError,
                "SchemeShard completed an unexpected transaction id");
        }

        AlterCompletionConfirmed = true;
        // Re-describe before releasing workers. This makes replay after a
        // controller restart safe and catches an ambiguous completion result.
        DescribeDst();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        if (ev->Get()->TabletId != SchemeShardId) {
            return;
        }
        RetryAllocate();
    }

    void Handle(TEvents::TEvUndelivered::TPtr&) {
        RetryAllocate();
    }

    void RetryDescribe() {
        Schedule(RetryInterval, new TEvents::TEvWakeup);
    }

    void RetryAllocate() {
        Schedule(RetryInterval, new TEvents::TEvWakeup);
    }

    void Success() {
        Send(Parent, new TEvPrivate::TEvSchemaChangeDstAlterResult(ReplicationId, TargetId, TxId));
        PassAway();
    }

    void Error(NKikimrScheme::EStatus status, const TString& error) {
        const TString reason = TStringBuilder() << "destination table " << DstPathId.ToString()
            << ", source schema version " << DesiredSchema.GetSourceSchemaVersion() << ": " << error;
        YDB_LOG_ERROR("Schema change destination alter failed",
            {"status", status},
            {"reason", reason});
        Send(Parent, new TEvPrivate::TEvSchemaChangeDstAlterResult(ReplicationId, TargetId, TxId, status, reason));
        PassAway();
    }

    void PassAway() override {
        if (PipeCache) {
            Send(PipeCache, new TEvPipeCache::TEvUnlink(SchemeShardId));
        }
        TActorBootstrapped<TSchemaChangeDstAlterer>::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_DST_ALTERER;
    }

    TSchemaChangeDstAlterer(const TActorId& parent, ui64 schemeShardId, ui64 rid, ui64 tid,
            TReplication::ETargetKind kind, const TPathId& dstPathId,
            const NKikimrReplication::TSchemaChange& desiredSchema, ui64 txId)
        : Parent(parent)
        , SchemeShardId(schemeShardId)
        , ReplicationId(rid)
        , TargetId(tid)
        , Kind(kind)
        , DstPathId(dstPathId)
        , DesiredSchema(desiredSchema)
        , LogPrefix(CreateActorLogPrefix("SchemaChangeDstAlterer", ReplicationId, TargetId))
        , TxId(txId)
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
        AllocateTxId();
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
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TReplication::ETargetKind Kind;
    const TPathId DstPathId;
    const NKikimrReplication::TSchemaChange DesiredSchema;
    NActors::NStructuredLog::TStructuredMessage LogPrefix;

    ui64 TxId = 0;
    bool AlterCompletionConfirmed = false;
    TActorId PipeCache;
    NKikimrSchemeOp::TModifyScheme Alter;
    static constexpr auto RetryInterval = TDuration::Seconds(10);
};

IActor* CreateSchemaChangeDstAlterer(const TActorId& parent, ui64 schemeShardId,
    ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TPathId& dstPathId,
    const NKikimrReplication::TSchemaChange& desiredSchema, ui64 txId)
{
    return new TSchemaChangeDstAlterer(parent, schemeShardId, rid, tid, kind, dstPathId, desiredSchema, txId);
}

}
