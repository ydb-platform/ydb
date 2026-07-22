#pragma once

#include "schemeshard__backup_collection_common.h"
#include "schemeshard__root_shred_manager.h"
#include "schemeshard__tenant_shred_manager.h"
#include "schemeshard_impl.h"
#include "schemeshard_pq_helpers.h"  // for PQGroupReserve

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/tx/schemeshard/olap/operations/local_index_helpers.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/protos/table_stats.pb.h>  // for TStoragePoolsStats
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/schemeshard/index/index_build_info.h>
#include <ydb/core/util/pb.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxInit : public TTransactionBase<TSchemeShard> {
    TSideEffects OnComplete;
    TMemoryChanges MemChanges;
    TStorageChanges DbChanges;
    THashMap<TTxId, TDeque<TPathId>> Publications;
    TVector<TPathId> TablesToClean;
    TDeque<TPathId> BlockStoreVolumesToClean;
    TVector<ui64> ExportsToResume;
    TVector<ui64> ImportsToResume;
    THashMap<TPathId, TVector<TPathId>> CdcStreamScansToResume;
    TVector<TPathId> RestoreTablesToUnmark;
    TVector<ui64> IncrementalBackupsToResume;
    TVector<ui64> FullBackupsToResume;
    bool Broken = false;

    explicit TTxInit(TSelf *self)
        : TBase(self)
    {}

    bool CreateScheme(TTransactionContext &txc);


    void CollectObjectsToClean();


    typedef std::tuple<TPathId, TPathId, TString, TString,
                       TPathElement::EPathType,
                       TStepId, TTxId, TStepId, TTxId,
                       TString, TTxId,
                       ui64, ui64, ui64,
                       TString, TActorId> TPathRec;
    typedef TDeque<TPathRec> TPathRows;

    template <typename SchemaTable, typename TRowSet>
    static TPathRec MakePathRec(const TPathId& pathId, const TPathId& parentPathId, TRowSet& rowSet) {
        return std::make_tuple(pathId, parentPathId,
            rowSet.template GetValue<typename SchemaTable::Name>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::Owner>(),
            static_cast<TPathElement::EPathType>(rowSet.template GetValue<typename SchemaTable::PathType>()),
            rowSet.template GetValueOrDefault<typename SchemaTable::StepCreated>(InvalidStepId),
            rowSet.template GetValue<typename SchemaTable::CreateTxId>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::StepDropped>(InvalidStepId),
            rowSet.template GetValueOrDefault<typename SchemaTable::DropTxId>(InvalidTxId),
            rowSet.template GetValueOrDefault<typename SchemaTable::ACL>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::LastTxId>(InvalidTxId),
            rowSet.template GetValueOrDefault<typename SchemaTable::DirAlterVersion>(1),
            rowSet.template GetValueOrDefault<typename SchemaTable::UserAttrsAlterVersion>(1),
            rowSet.template GetValueOrDefault<typename SchemaTable::ACLVersion>(0),
            rowSet.template GetValueOrDefault<typename SchemaTable::TempDirOwnerActorId_Deprecated>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::TempDirOwnerActorIdRaw>()
        );
    }

    TPathElement::TPtr MakePathElement(const TPathRec& rec) const;


    bool LoadPaths(NIceDb::TNiceDb& db, TPathRows& pathRows) const;


    typedef std::tuple<TPathId, TString, TString> TUserAttrsRec;
    typedef TDeque<TUserAttrsRec> TUserAttrsRows;

    template <typename SchemaTable, typename TRowSet>
    static TUserAttrsRec MakeUserAttrsRec(const TPathId& pathId, TRowSet& rowSet) {
        return std::make_tuple(pathId,
            rowSet.template GetValue<typename SchemaTable::AttrName>(),
            rowSet.template GetValue<typename SchemaTable::AttrValue>()
        );
    }

    bool LoadUserAttrs(NIceDb::TNiceDb& db, TUserAttrsRows& userAttrsRows) const;


    bool LoadUserAttrsAlterData(NIceDb::TNiceDb& db, TUserAttrsRows& userAttrsRows) const;


    typedef std::tuple<TPathId, ui32, ui64, TString, TString, TString, ui64, TString, bool, TString, bool, TString, TString, bool, TString, TString> TTableRec;
    typedef TDeque<TTableRec> TTableRows;

    template <typename SchemaTable, typename TRowSet>
    static TTableRec MakeTableRec(const TPathId& pathId, TRowSet& rowSet) {
        return std::make_tuple(pathId,
            rowSet.template GetValue<typename SchemaTable::NextColId>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::AlterVersion>(0),
            rowSet.template GetValueOrDefault<typename SchemaTable::PartitionConfig>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::AlterTableFull>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::AlterTable>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::PartitioningVersion>(0),
            rowSet.template GetValueOrDefault<typename SchemaTable::TTLSettings>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::IsBackup>(false),
            rowSet.template GetValueOrDefault<typename SchemaTable::ReplicationConfig>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::IsTemporary>(false),
            rowSet.template GetValueOrDefault<typename SchemaTable::OwnerActorId>(""),
            rowSet.template GetValueOrDefault<typename SchemaTable::IncrementalBackupConfig>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::IsRestore>(false),
            rowSet.template GetValueOrDefault<typename SchemaTable::DetailedMetricsSettings>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::MultiColumnStatistics>()
        );
    }

    bool LoadTables(NIceDb::TNiceDb& db, TTableRows& tableRows) const;


    typedef std::tuple<TPathId, ui32, TString, NScheme::TTypeInfo, TString, ui32, ui64, ui64, ui32, ETableColumnDefaultKind, TString, bool, bool, bool> TColumnRec;
    typedef TDeque<TColumnRec> TColumnRows;

    template <typename SchemaTable, typename TRowSet>
    static TColumnRec MakeColumnRec(const TPathId& pathId, TRowSet& rowSet) {
        const auto typeId = static_cast<NScheme::TTypeId>(rowSet.template GetValue<typename SchemaTable::ColType>());
        NScheme::TTypeInfoMod typeInfoMod;

        if (const TString typeData = rowSet.template GetValueOrDefault<typename SchemaTable::ColTypeData>("")) {
            NKikimrProto::TTypeInfo protoType;
            Y_ABORT_UNLESS(ParseFromStringNoSizeLimit(protoType, typeData));
            typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(typeId, &protoType);
        } else {
            // Decimal column was created before SchemaTable::ColTypeData was filled with decimal params
            // So, it's default
            if (typeId == NScheme::NTypeIds::Decimal) {
                typeInfoMod.TypeInfo = NScheme::TTypeInfo(NScheme::TDecimalType::Default());
            } else {
                typeInfoMod.TypeInfo = NScheme::TTypeInfo(typeId);
            }
        }

        return std::make_tuple(pathId,
            rowSet.template GetValue<typename SchemaTable::ColId>(),
            rowSet.template GetValue<typename SchemaTable::ColName>(),
            typeInfoMod.TypeInfo,
            typeInfoMod.TypeMod,
            rowSet.template GetValue<typename SchemaTable::ColKeyOrder>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::CreateVersion>(0),
            rowSet.template GetValueOrDefault<typename SchemaTable::DeleteVersion>(-1),
            rowSet.template GetValueOrDefault<typename SchemaTable::Family>(0),
            rowSet.template GetValue<typename SchemaTable::DefaultKind>(),
            rowSet.template GetValue<typename SchemaTable::DefaultValue>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::NotNull>(false),
            rowSet.template GetValueOrDefault<typename SchemaTable::IsBuildInProgress>(false),
            rowSet.template GetValueOrDefault<typename SchemaTable::SetNotNullInProgress>(false)
        );
    }

    bool LoadColumns(NIceDb::TNiceDb& db, TColumnRows& columnRows) const;


    bool LoadColumnsAlters(NIceDb::TNiceDb& db, TColumnRows& columnRows) const;


    typedef std::tuple<TPathId, ui64, TString, TShardIdx, ui64, ui64> TTablePartitionRec;
    typedef TDeque<TTablePartitionRec> TTablePartitionsRows;

    template <typename SchemaTable, typename TRowSet>
    static TTablePartitionRec MakeTablePartitionRec(const TPathId& pathId, const TShardIdx& shardIdx, TRowSet& rowSet) {
        return std::make_tuple(pathId,
            rowSet.template GetValue<typename SchemaTable::Id>(),
            rowSet.template GetValue<typename SchemaTable::RangeEnd>(),
            shardIdx,
            rowSet.template GetValueOrDefault<typename SchemaTable::LastCondErase>(0),
            rowSet.template GetValueOrDefault<typename SchemaTable::NextCondErase>(0)
        );
    }

    bool LoadTablePartitions(NIceDb::TNiceDb& db, TTablePartitionsRows& partitionsRows) const;


    typedef std::tuple<TShardIdx, TString> TTableShardPartitionConfigRec;
    typedef TDeque<TTableShardPartitionConfigRec> TTableShardPartitionConfigRows;

    template <typename SchemaTable, typename TRowSet>
    static TTableShardPartitionConfigRec MakeTableShardPartitionConfigRec(const TShardIdx& shardIdx, TRowSet& rowSet) {
        return std::make_tuple(shardIdx,
            rowSet.template GetValue<typename SchemaTable::PartitionConfig>()
        );
    }

    bool LoadTableShardPartitionConfigs(NIceDb::TNiceDb& db, TTableShardPartitionConfigRows& partitionsRows) const;


    typedef std::tuple<TTxId, TPathId, ui64> TPublicationRec;
    typedef TDeque<TPublicationRec> TPublicationsRows;

    template <typename SchemaTable, typename TRowSet>
    static TPublicationRec MakePublicationRec(const TPathId& pathId, TRowSet& rowSet) {
        return std::make_tuple(
            rowSet.template GetValue<typename SchemaTable::TxId>(),
            pathId,
            rowSet.template GetValue<typename SchemaTable::Version>()
        );
    }

    bool LoadPublications(NIceDb::TNiceDb& db, TPublicationsRows& publicationsRows) const;


    typedef std::tuple<TShardIdx> TShardsToDeleteRec;
    typedef TDeque<TShardsToDeleteRec> TShardsToDeleteRows;

    bool LoadShardsToDelete(NIceDb::TNiceDb& db, TShardsToDeleteRows& shardsToDelete) const;


    bool LoadSystemShardsToDelete(NIceDb::TNiceDb& db, TShardsToDeleteRows& shardsToDelete) const;


    typedef std::tuple<TOperationId, TShardIdx, TTxState::ETxState> TTxShardRec;
    typedef TVector<TTxShardRec> TTxShardsRows;

    template <typename SchemaTable, typename TRowSet>
    TTxShardRec MakeTxShardRec(const TOperationId& opId, TRowSet& rowSet) const {
        return MakeTxShardRec<SchemaTable>(opId,
            Self->MakeLocalId(rowSet.template GetValue<typename SchemaTable::ShardIdx>()), rowSet
        );
    }

    template <typename SchemaTable, typename TRowSet>
    static TTxShardRec MakeTxShardRec(const TOperationId& opId, const TShardIdx& shardIdx, TRowSet& rowSet) {
        return std::make_tuple(opId, shardIdx,
            static_cast<TTxState::ETxState>(rowSet.template GetValue<typename SchemaTable::Operation>())
        );
    }

    bool LoadTxShards(NIceDb::TNiceDb& db, TTxShardsRows& txShards) const;


    typedef std::tuple<TShardIdx, TTabletId, TPathId, TTxId, TTabletTypes::EType> TShardsRec;
    typedef TDeque<TShardsRec> TShardsRows;

    template <typename SchemaTable, typename TRowSet>
    static TShardsRec MakeShardsRec(const TShardIdx& shardIdx, const TPathId& pathId, TRowSet& rowSet) {
        return std::make_tuple(shardIdx,
            rowSet.template GetValue<typename SchemaTable::TabletId>(),
            pathId,
            rowSet.template GetValueOrDefault<typename SchemaTable::LastTxId>(InvalidTxId),
            rowSet.template GetValue<typename SchemaTable::TabletType>()
        );
    }

    bool LoadShards(NIceDb::TNiceDb& db, TShardsRows& shards) const;


    bool LoadSharedShards(NIceDb::TNiceDb& db) const;


    typedef std::tuple<TPathId, TString, TString, TString, TString, bool, TString, ui32, bool, bool, TString, TString> TBackupSettingsRec;
    typedef TDeque<TBackupSettingsRec> TBackupSettingsRows;

    template <typename SchemaTable, typename TRowSet>
    static TBackupSettingsRec MakeBackupSettingsRec(const TPathId& pathId, TRowSet& rowSet) {
        return std::make_tuple(pathId,
            rowSet.template GetValue<typename SchemaTable::TableName>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::YTSettings>(""),
            rowSet.template GetValueOrDefault<typename SchemaTable::S3Settings>(""),
            rowSet.template GetValueOrDefault<typename SchemaTable::ScanSettings>(""),
            rowSet.template GetValueOrDefault<typename SchemaTable::NeedToBill>(true),
            rowSet.template GetValueOrDefault<typename SchemaTable::TableDescription>(""),
            rowSet.template GetValueOrDefault<typename SchemaTable::NumberOfRetries>(0),
            rowSet.template GetValueOrDefault<typename SchemaTable::EnableChecksums>(false),
            rowSet.template GetValueOrDefault<typename SchemaTable::EnablePermissions>(false),
            rowSet.template GetValueOrDefault<typename SchemaTable::ChangefeedUnderlyingTopics>(""),
            rowSet.template GetValueOrDefault<typename SchemaTable::FSSettings>("")
        );
    }

    bool LoadBackupSettings(NIceDb::TNiceDb& db, TBackupSettingsRows& settings) const;


    typedef std::tuple<TPathId, TTxId, ui64, ui32, ui32, ui64, ui64, ui8> TCompletedBackupRestoreRec;
    typedef TDeque<TCompletedBackupRestoreRec> TCompletedBackupRestoreRows;

    template <typename SchemaTable, typename TRowSet>
    static TCompletedBackupRestoreRec MakeCompletedBackupRestoreRec(const TPathId& pathId, TRowSet& rowSet) {
        return std::make_tuple(pathId,
            rowSet.template GetValue<typename SchemaTable::TxId>(),
            rowSet.template GetValue<typename SchemaTable::DateTimeOfCompletion>(),
            rowSet.template GetValue<typename SchemaTable::SuccessShardCount>(),
            rowSet.template GetValue<typename SchemaTable::TotalShardCount>(),
            rowSet.template GetValue<typename SchemaTable::StartTime>(),
            rowSet.template GetValue<typename SchemaTable::DataTotalSize>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::Kind>(0)
        );
    }

    bool LoadBackupRestoreHistory(NIceDb::TNiceDb& db, TCompletedBackupRestoreRows& history) const;


    typedef std::tuple<TTxId, TShardIdx, bool, TString, ui64, ui64> TShardBackupStatusRec;
    typedef TDeque<TShardBackupStatusRec> TShardBackupStatusRows;

    template <typename SchemaTable, typename TRowSet>
    static TShardBackupStatusRec MakeShardBackupStatusRec(const TShardIdx& shardIdx, TRowSet& rowSet) {
        return std::make_tuple(
            rowSet.template GetValue<typename SchemaTable::TxId>(),
            shardIdx, false,
            rowSet.template GetValue<typename SchemaTable::Explain>(),
            0, 0
        );
    }

    template <typename T, typename U, typename V>
    bool LoadBackupStatusesImpl(TShardBackupStatusRows& statuses, T& byShardBackupStatus, U& byMigratedShardBackupStatus, V& byTxShardStatus) const;


    bool LoadBackupStatuses(NIceDb::TNiceDb& db, TShardBackupStatusRows& statuses) const;


    typedef std::tuple<TPathId, ui64, NKikimrSchemeOp::EIndexType, NKikimrSchemeOp::EIndexState, TString> TTableIndexRec;
    typedef TDeque<TTableIndexRec> TTableIndexRows;

    bool LoadTableIndexes(NIceDb::TNiceDb& db, TTableIndexRows& tableIndexes) const;


    typedef std::tuple<TPathId, ui64, TString> TTableIndexColRec;
    typedef TDeque<TTableIndexColRec> TTableIndexKeyRows;
    typedef TDeque<TTableIndexColRec> TTableIndexDataRows;

    template <typename SchemaTable, typename TRowSet>
    static TTableIndexColRec MakeTableIndexColRec(const TPathId& pathId, TRowSet& rowSet) {
        return std::make_tuple(pathId,
            rowSet.template GetValue<typename SchemaTable::KeyId>(),
            rowSet.template GetValue<typename SchemaTable::KeyName>()
        );
    }

    bool LoadTableIndexKeys(NIceDb::TNiceDb& db, TTableIndexKeyRows& tableIndexKeys) const;


    bool LoadTableIndexDataColumns(NIceDb::TNiceDb& db, TTableIndexDataRows& tableIndexData) const;


    typedef std::tuple<TShardIdx, ui32, TString, TString> TChannelBindingRec;
    typedef TDeque<TChannelBindingRec> TChannelBindingRows;

    template <typename SchemaTable, typename TRowSet>
    static TChannelBindingRec MakeChannelBindingRec(const TShardIdx& shardIdx, TRowSet& rowSet) {
        return std::make_tuple(shardIdx,
            rowSet.template GetValue<typename SchemaTable::ChannelId>(),
            rowSet.template GetValue<typename SchemaTable::Binding>(),
            rowSet.template GetValue<typename SchemaTable::PoolName>()
        );
    }

    bool LoadChannelBindings(NIceDb::TNiceDb& db, TChannelBindingRows& channeldBindings) const;


    typedef std::tuple<TPathId, TString, ui64> TKesusInfosRec;
    typedef TDeque<TKesusInfosRec> TKesusInfosRows;

    template <typename SchemaTable, typename TRowSet>
    static TKesusInfosRec MakeKesusInfosRec(const TPathId& pathId, TRowSet& rowSet) {
        return std::make_tuple(pathId,
            rowSet.template GetValue<typename SchemaTable::Config>(),
            rowSet.template GetValueOrDefault<typename SchemaTable::Version>()
        );
    }

    bool LoadKesusInfos(NIceDb::TNiceDb& db, TKesusInfosRows& kesusInfosData) const;


    typedef std::tuple<TPathId, TString, ui64> TKesusAlterRec;
    typedef TDeque<TKesusAlterRec> TKesusAlterRows;

    bool LoadKesusAlters(NIceDb::TNiceDb& db, TKesusAlterRows& kesusAlterData) const;


    template <typename PersistentTable, typename TRowSet>
    TSchemeLimits LoadSchemeLimitsImpl(const TSchemeLimits& defaults, TRowSet& rowSet) {
        return TSchemeLimits {
            .MaxDepth = rowSet.template GetValueOrDefault<typename PersistentTable::DepthLimit>(defaults.MaxDepth),
            .MaxPaths = rowSet.template GetValueOrDefault<typename PersistentTable::PathsLimit>(defaults.MaxPaths),
            .MaxChildrenInDir = rowSet.template GetValueOrDefault<typename PersistentTable::ChildrenLimit>(defaults.MaxChildrenInDir),
            .MaxAclBytesSize = rowSet.template GetValueOrDefault<typename PersistentTable::AclByteSizeLimit>(defaults.MaxAclBytesSize),
            .MaxPathElementLength = rowSet.template GetValueOrDefault<typename PersistentTable::PathElementLength>(defaults.MaxPathElementLength),
            .ExtraPathSymbolsAllowed = rowSet.template GetValueOrDefault<typename PersistentTable::ExtraPathSymbolsAllowed>(defaults.ExtraPathSymbolsAllowed),
            .MaxTableColumns = rowSet.template GetValueOrDefault<typename PersistentTable::TableColumnsLimit>(defaults.MaxTableColumns),
            .MaxColumnTableColumns = rowSet.template GetValueOrDefault<typename PersistentTable::ColumnTableColumnsLimit>(defaults.MaxColumnTableColumns),
            .MaxTableColumnNameLength = rowSet.template GetValueOrDefault<typename PersistentTable::TableColumnNameLengthLimit>(defaults.MaxTableColumnNameLength),
            .MaxTableKeyColumns = rowSet.template GetValueOrDefault<typename PersistentTable::TableKeyColumnsLimit>(defaults.MaxTableKeyColumns),
            .MaxTableIndices = rowSet.template GetValueOrDefault<typename PersistentTable::TableIndicesLimit>(defaults.MaxTableIndices),
            .MaxTableCdcStreams = rowSet.template GetValueOrDefault<typename PersistentTable::TableCdcStreamsLimit>(defaults.MaxTableCdcStreams),
            .MaxShards = rowSet.template GetValueOrDefault<typename PersistentTable::ShardsLimit>(defaults.MaxShards),
            .MaxShardsInPath = rowSet.template GetValueOrDefault<typename PersistentTable::PathShardsLimit>(defaults.MaxShardsInPath),
            .MaxConsistentCopyTargets = rowSet.template GetValueOrDefault<typename PersistentTable::ConsistentCopyingTargetsLimit>(defaults.MaxConsistentCopyTargets),
            .MaxPQPartitions = rowSet.template GetValueOrDefault<typename PersistentTable::PQPartitionsLimit>(defaults.MaxPQPartitions),
            .MaxExports = rowSet.template GetValueOrDefault<typename PersistentTable::ExportsLimit>(defaults.MaxExports),
            .MaxImports = rowSet.template GetValueOrDefault<typename PersistentTable::ImportsLimit>(defaults.MaxImports),
        };
    }

    template <typename TRowSet>
    TSchemeLimits LoadSchemeLimits(const TSchemeLimits& defaults, TRowSet& rowSet) {
        return LoadSchemeLimitsImpl<Schema::SubDomains>(defaults, rowSet);
    }

    template <typename TRowSet>
    TSchemeLimits LoadSchemeLimitsAlter(const TSchemeLimits& defaults, TRowSet& rowSet) {
        return LoadSchemeLimitsImpl<Schema::SubDomainsAlterData>(defaults, rowSet);
    }

    bool ReadEverything(TTransactionContext& txc, const TActorContext& ctx);


    void RestoreIncrementalRestoreOpPathStates(const NKikimrSchemeOp::TLongIncrementalRestoreOp& op);


    void ScheduleOrphanedIncrementalRestoreOp(const TOperationId& opId, const NKikimrSchemeOp::TLongIncrementalRestoreOp& op, TSideEffects& onComplete, const TActorContext& ctx);


    TTxType GetTxType() const override;


    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override;


    void Complete(const TActorContext &ctx) override;

};

} // namespace NSchemeShard
} // namespace NKikimr
