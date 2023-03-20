#include "schemeshard_impl.h"

#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/schemeshard/schemeshard_utils.h>
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
    bool Broken = false;

    explicit TTxInit(TSelf *self)
        : TBase(self)
    {}

    bool CreateScheme(TTransactionContext &txc) {
        if (!txc.DB.GetScheme().IsEmpty())
            return false;

        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

        return true;
    }

    void CollectObjectsToClean() {
        THashSet<TPathId> underOperation;
        for (auto& item : Self->TxInFlight) {
            const TTxState& txState = item.second;
            underOperation.insert(txState.TargetPathId);
            if (txState.SourcePathId) {
                underOperation.insert(txState.SourcePathId);
            }
        }

        TablesToClean.clear();
        for (auto& tItem : Self->Tables) {
            TPathId pathId = tItem.first;
            TPathElement::TPtr path = Self->PathsById.at(pathId);

            if (path->IsTable() && path->Dropped() && !underOperation.contains(pathId)) {
                TablesToClean.push_back(pathId);
            }
        }

        BlockStoreVolumesToClean.clear();
        for (auto& xpair : Self->BlockStoreVolumes) {
            TPathId pathId = xpair.first;
            TPathElement::TPtr path = Self->PathsById.at(pathId);

            if (path->IsBlockStoreVolume() && path->Dropped() && !underOperation.contains(pathId)) {
                BlockStoreVolumesToClean.push_back(pathId);
            }
        }

        for (const auto& item : Self->PathsById) {
            if (item.second->DbRefCount == 0 && item.second->Dropped()) {
                Y_VERIFY_DEBUG(!underOperation.contains(item.first));
                Self->CleanDroppedPathsCandidates.insert(item.first);
            }
        }

        for (const auto& item : Self->SubDomains) {
            if (!item.second->GetInternalShards().empty()) {
                continue;
            }
            auto path = Self->PathsById.at(item.first);
            if (path->DbRefCount == 1 && path->AllChildrenCount == 0 && path->Dropped()) {
                Y_VERIFY_DEBUG(!underOperation.contains(item.first));
                Self->CleanDroppedSubDomainsCandidates.insert(item.first);
            }
        }
    }

    typedef std::tuple<TPathId, TPathId, TString, TString,
                       TPathElement::EPathType,
                       TStepId, TTxId, TStepId, TTxId,
                       TString, TTxId,
                       ui64, ui64, ui64> TPathRec;
    typedef TDeque<TPathRec> TPathRows;

    TPathElement::TPtr MakePathElement(const TPathRec& rec) const {
        TPathId pathId = std::get<0>(rec);
        TPathId parentPathId = std::get<1>(rec);

        TString name = std::get<2>(rec);
        TString owner = std::get<3>(rec);

        TPathId domainId = Self->RootPathId();
        if (pathId != Self->RootPathId()) {
            Y_VERIFY_S(Self->PathsById.contains(parentPathId), "Parent path not found"
                           << ", pathId: " << pathId
                           << ", parentPathId: " << parentPathId);
            auto parent = Self->PathsById.at(parentPathId);
            if (parent->IsDomainRoot()) {
                domainId = parentPathId;
            } else {
                domainId = parent->DomainPathId;
            }
        }

        TPathElement::TPtr path = new TPathElement(pathId, parentPathId, domainId, name, owner);

        std::tie(
            std::ignore, //pathId
            std::ignore, //parentPathId
            std::ignore, //name
            std::ignore, //owner
            path->PathType,
            path->StepCreated,
            path->CreateTxId,
            path->StepDropped,
            path->DropTxId,
            path->ACL,
            path->LastTxId,
            path->DirAlterVersion,
            path->UserAttrs->AlterVersion,
            path->ACLVersion
            ) = rec;

        path->PathState = TPathElement::EPathState::EPathStateNoChanges;
        if (path->StepDropped) {
            path->PathState = TPathElement::EPathState::EPathStateNotExist;
        }

        return path;
    }

    bool LoadPaths(NIceDb::TNiceDb& db, TPathRows& pathRows) const {
        {
            {
                auto rows = db.Table<Schema::MigratedPaths>().Range().Select();
                if (!rows.IsReady()) {
                    return false;
                }
                while (!rows.EndOfSet()) {
                    TPathId pathId = TPathId(rows.GetValue<Schema::MigratedPaths::OwnerPathId>(), rows.GetValue<Schema::MigratedPaths::LocalPathId>());
                    TPathId parentPathId = TPathId(rows.GetValue<Schema::MigratedPaths::ParentOwnerId>(), rows.GetValue<Schema::MigratedPaths::ParentLocalId>());

                    TString name = rows.GetValue<Schema::MigratedPaths::Name>();

                    TPathElement::EPathType pathType = (TPathElement::EPathType)rows.GetValue<Schema::MigratedPaths::PathType>();

                    TStepId stepCreated = rows.GetValueOrDefault<Schema::MigratedPaths::StepCreated>(InvalidStepId);
                    TTxId txIdCreated = rows.GetValue<Schema::MigratedPaths::CreateTxId>();

                    TStepId stepDropped = rows.GetValueOrDefault<Schema::MigratedPaths::StepDropped>(InvalidStepId);
                    TTxId txIdDropped = rows.GetValueOrDefault<Schema::MigratedPaths::DropTxId>(InvalidTxId);

                    TString owner = rows.GetValueOrDefault<Schema::MigratedPaths::Owner>();
                    TString acl = rows.GetValueOrDefault<Schema::MigratedPaths::ACL>();

                    TTxId lastTxId =  rows.GetValueOrDefault<Schema::MigratedPaths::LastTxId>(InvalidTxId);

                    ui64 dirAlterVer = rows.GetValueOrDefault<Schema::MigratedPaths::DirAlterVersion>(1);
                    ui64 userAttrsAlterVer = rows.GetValueOrDefault<Schema::MigratedPaths::UserAttrsAlterVersion>(1);
                    ui64 aclAlterVer = rows.GetValueOrDefault<Schema::MigratedPaths::ACLVersion>(0);

                    pathRows.emplace_back(pathId, parentPathId, name, owner, pathType,
                                            stepCreated, txIdCreated, stepDropped, txIdDropped,
                                            acl, lastTxId, dirAlterVer, userAttrsAlterVer, aclAlterVer);

                    if (!rows.Next()) {
                        return false;
                    }
                }

            }


            auto rows = db.Table<Schema::Paths>().Range().Select();
            if (!rows.IsReady()) {
                return false;
            }
            while (!rows.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rows.GetValue<Schema::Paths::Id>());
                TPathId parentPathId = TPathId(rows.GetValueOrDefault<Schema::Paths::ParentOwnerId>(Self->TabletID()),
                                               rows.GetValue<Schema::Paths::ParentId>());

                TString name = rows.GetValue<Schema::Paths::Name>();

                if (pathId.LocalPathId == 0) {
                    // Skip special incompatibility marker
                    Y_VERIFY_S(parentPathId.LocalPathId == 0 && name == "/incompatible/",
                        "Unexpected row PathId# " << pathId << " ParentPathId# " << parentPathId << " Name# " << name);
                    if (!rows.Next()) {
                        return false;
                    }
                    continue;
                }

                TPathElement::EPathType pathType = (TPathElement::EPathType)rows.GetValue<Schema::Paths::PathType>();

                TStepId stepCreated = rows.GetValueOrDefault<Schema::Paths::StepCreated>(InvalidStepId);
                TTxId txIdCreated = rows.GetValue<Schema::Paths::CreateTxId>();

                TStepId stepDropped = rows.GetValueOrDefault<Schema::Paths::StepDropped>(InvalidStepId);
                TTxId txIdDropped = rows.GetValueOrDefault<Schema::Paths::DropTxId>(InvalidTxId);

                TString owner = rows.GetValueOrDefault<Schema::Paths::Owner>();
                TString acl = rows.GetValueOrDefault<Schema::Paths::ACL>();

                TTxId lastTxId =  rows.GetValueOrDefault<Schema::Paths::LastTxId>(InvalidTxId);

                ui64 dirAlterVer = rows.GetValueOrDefault<Schema::Paths::DirAlterVersion>(1);
                ui64 userAttrsAlterVer = rows.GetValueOrDefault<Schema::Paths::UserAttrsAlterVersion>(1);
                ui64 aclAlterVer = rows.GetValueOrDefault<Schema::Paths::ACLVersion>(0);

                if (pathId == parentPathId) {
                    pathRows.emplace_front(pathId, parentPathId, name, owner, pathType,
                                            stepCreated, txIdCreated, stepDropped, txIdDropped,
                                            acl, lastTxId, dirAlterVer, userAttrsAlterVer, aclAlterVer);
                } else {
                    pathRows.emplace_back(pathId, parentPathId, name, owner, pathType,
                                            stepCreated, txIdCreated, stepDropped, txIdDropped,
                                            acl, lastTxId, dirAlterVer, userAttrsAlterVer, aclAlterVer);
                }

                if (!rows.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    typedef std::tuple<TPathId, TString, TString> TUserAttrsRec;
    typedef TDeque<TUserAttrsRec> TUserAttrsRows;

    bool LoadUserAttrs(NIceDb::TNiceDb& db, TUserAttrsRows& userAttrsRows) const {
        {
            auto rowSet = db.Table<Schema::UserAttributes>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rowSet.GetValue<Schema::UserAttributes::PathId>());

                TString name = rowSet.GetValue<Schema::UserAttributes::AttrName>();
                TString value = rowSet.GetValue<Schema::UserAttributes::AttrValue>();

                userAttrsRows.emplace_back(pathId, name, value);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedUserAttributes>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                TPathId pathId = TPathId(rowSet.GetValue<Schema::MigratedUserAttributes::OwnerPathId>(),
                                         rowSet.GetValue<Schema::MigratedUserAttributes::LocalPathId>());

                TString name = rowSet.GetValue<Schema::MigratedUserAttributes::AttrName>();
                TString value = rowSet.GetValue<Schema::MigratedUserAttributes::AttrValue>();

                userAttrsRows.emplace_back(pathId, name, value);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    bool LoadUserAttrsAlterData(NIceDb::TNiceDb& db, TUserAttrsRows& userAttrsRows) const {
        {
            auto rowSet = db.Table<Schema::UserAttributesAlterData>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rowSet.GetValue<Schema::UserAttributesAlterData::PathId>());

                TString name = rowSet.GetValue<Schema::UserAttributesAlterData::AttrName>();
                TString value = rowSet.GetValue<Schema::UserAttributesAlterData::AttrValue>();

                userAttrsRows.emplace_back(pathId, name, value);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedUserAttributesAlterData>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                TPathId pathId = TPathId(rowSet.GetValue<Schema::MigratedUserAttributesAlterData::OwnerPathId>(),
                                         rowSet.GetValue<Schema::MigratedUserAttributesAlterData::LocalPathId>());

                TString name = rowSet.GetValue<Schema::MigratedUserAttributesAlterData::AttrName>();
                TString value = rowSet.GetValue<Schema::MigratedUserAttributesAlterData::AttrValue>();

                userAttrsRows.emplace_back(pathId, name, value);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    typedef std::tuple<TPathId, ui32, ui64, TString, TString, TString, ui64, TString, bool> TTableRec;
    typedef TDeque<TTableRec> TTableRows;

    bool LoadTables(NIceDb::TNiceDb& db, TTableRows& tableRows) const {
        {
            auto rowSet = db.Table<Schema::Tables>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rowSet.GetValue<Schema::Tables::TabId>());

                ui32 nextCollId = rowSet.GetValue<Schema::Tables::NextColId>();
                ui64 alterVersion = rowSet.GetValueOrDefault<Schema::Tables::AlterVersion>(0);
                TString partitionConfig = rowSet.GetValueOrDefault<Schema::Tables::PartitionConfig>();
                TString alterTabletFull = rowSet.GetValueOrDefault<Schema::Tables::AlterTableFull>();
                TString alterTabletDiff = rowSet.GetValueOrDefault<Schema::Tables::AlterTable>();
                ui64 partitionVersion = rowSet.GetValueOrDefault<Schema::Tables::PartitioningVersion>(0);
                TString ttlSettings = rowSet.GetValueOrDefault<Schema::Tables::TTLSettings>();
                bool isBackup = rowSet.GetValueOrDefault<Schema::Tables::IsBackup>(false);

                tableRows.emplace_back(pathId,
                                       nextCollId, alterVersion, partitionConfig, alterTabletFull, alterTabletDiff, partitionVersion, ttlSettings, isBackup);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedTables>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                TPathId pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedTables::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedTables::LocalPathId>());

                ui32 nextCollId = rowSet.GetValue<Schema::MigratedTables::NextColId>();
                ui64 alterVersion = rowSet.GetValueOrDefault<Schema::MigratedTables::AlterVersion>(0);
                TString partitionConfig = rowSet.GetValueOrDefault<Schema::MigratedTables::PartitionConfig>();
                TString alterTabletFull = rowSet.GetValueOrDefault<Schema::MigratedTables::AlterTableFull>();
                TString alterTabletDiff = rowSet.GetValueOrDefault<Schema::MigratedTables::AlterTable>();
                ui64 partitionVersion = rowSet.GetValueOrDefault<Schema::MigratedTables::PartitioningVersion>(0);
                TString ttlSettings = rowSet.GetValueOrDefault<Schema::MigratedTables::TTLSettings>();
                bool isBackup = rowSet.GetValueOrDefault<Schema::MigratedTables::IsBackup>(false);

                tableRows.emplace_back(pathId,
                                       nextCollId, alterVersion, partitionConfig, alterTabletFull, alterTabletDiff, partitionVersion, ttlSettings, isBackup);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    typedef std::tuple<TPathId, ui32, TString, NScheme::TTypeInfo, TString, ui32, ui64, ui64, ui32, ETableColumnDefaultKind, TString, bool> TColumnRec;
    typedef TDeque<TColumnRec> TColumnRows;

    bool LoadColumns(NIceDb::TNiceDb& db, TColumnRows& columnRows) const {
        {
            auto rowSet = db.Table<Schema::Columns>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rowSet.GetValue<Schema::Columns::TabId>());

                ui32 colId = rowSet.GetValue<Schema::Columns::ColId>();
                TString colName = rowSet.GetValue<Schema::Columns::ColName>();
                NScheme::TTypeId typeId = (NScheme::TTypeId)rowSet.GetValue<Schema::Columns::ColType>();
                TString typeData = rowSet.GetValueOrDefault<Schema::Columns::ColTypeData>("");
                ui32 keyOrder = rowSet.GetValue<Schema::Columns::ColKeyOrder>();
                ui64 createVersion = rowSet.GetValueOrDefault<Schema::Columns::CreateVersion>(0);
                ui64 deleteVersion = rowSet.GetValueOrDefault<Schema::Columns::DeleteVersion>(-1);
                ui32 family = rowSet.GetValueOrDefault<Schema::Columns::Family>(0);
                auto defaultKind = rowSet.GetValue<Schema::Columns::DefaultKind>();
                auto defaultValue = rowSet.GetValue<Schema::Columns::DefaultValue>();
                auto notNull = rowSet.GetValueOrDefault<Schema::Columns::NotNull>(false);

                NScheme::TTypeInfoMod typeInfoMod;
                if (typeData) {
                    NKikimrProto::TTypeInfo protoType;
                    Y_VERIFY(ParseFromStringNoSizeLimit(protoType, typeData));
                    typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(typeId, &protoType);
                } else {
                    typeInfoMod.TypeInfo = NScheme::TTypeInfo(typeId);
                }

                columnRows.emplace_back(pathId, colId,
                                        colName, typeInfoMod.TypeInfo, typeInfoMod.TypeMod,
                                        keyOrder, createVersion, deleteVersion,
                                        family, defaultKind, defaultValue, notNull);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedColumns>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                TPathId pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedColumns::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedColumns::LocalPathId>());

                ui32 colId = rowSet.GetValue<Schema::MigratedColumns::ColId>();
                TString colName = rowSet.GetValue<Schema::MigratedColumns::ColName>();
                NScheme::TTypeId typeId = (NScheme::TTypeId)rowSet.GetValue<Schema::MigratedColumns::ColType>();
                TString typeData = rowSet.GetValueOrDefault<Schema::MigratedColumns::ColTypeData>("");
                ui32 keyOrder = rowSet.GetValue<Schema::MigratedColumns::ColKeyOrder>();
                ui64 createVersion = rowSet.GetValueOrDefault<Schema::MigratedColumns::CreateVersion>(0);
                ui64 deleteVersion = rowSet.GetValueOrDefault<Schema::MigratedColumns::DeleteVersion>(-1);
                ui32 family = rowSet.GetValueOrDefault<Schema::MigratedColumns::Family>(0);
                auto defaultKind = rowSet.GetValue<Schema::MigratedColumns::DefaultKind>();
                auto defaultValue = rowSet.GetValue<Schema::MigratedColumns::DefaultValue>();
                auto notNull = rowSet.GetValueOrDefault<Schema::MigratedColumns::NotNull>(false);

                NScheme::TTypeInfoMod typeInfoMod;
                if (typeData) {
                    NKikimrProto::TTypeInfo protoType;
                    Y_VERIFY(ParseFromStringNoSizeLimit(protoType, typeData));
                    typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(typeId, &protoType);
                } else {
                    typeInfoMod.TypeInfo = NScheme::TTypeInfo(typeId);
                }

                columnRows.emplace_back(pathId, colId,
                                        colName, typeInfoMod.TypeInfo, typeInfoMod.TypeMod,
                                        keyOrder, createVersion, deleteVersion,
                                        family, defaultKind, defaultValue, notNull);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    bool LoadColumnsAlters(NIceDb::TNiceDb& db, TColumnRows& columnRows) const {
        {
            auto rowSet = db.Table<Schema::ColumnAlters>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rowSet.GetValue<Schema::ColumnAlters::TabId>());

                ui32 colId = rowSet.GetValue<Schema::ColumnAlters::ColId>();
                TString colName = rowSet.GetValue<Schema::ColumnAlters::ColName>();
                NScheme::TTypeId typeId = (NScheme::TTypeId)rowSet.GetValue<Schema::ColumnAlters::ColType>();
                TString typeData = rowSet.GetValue<Schema::ColumnAlters::ColTypeData>();
                ui32 keyOrder = rowSet.GetValue<Schema::ColumnAlters::ColKeyOrder>();
                ui64 createVersion = rowSet.GetValueOrDefault<Schema::ColumnAlters::CreateVersion>(0);
                ui64 deleteVersion = rowSet.GetValueOrDefault<Schema::ColumnAlters::DeleteVersion>(-1);
                ui32 family = rowSet.GetValueOrDefault<Schema::ColumnAlters::Family>(0);
                auto defaultKind = rowSet.GetValue<Schema::ColumnAlters::DefaultKind>();
                auto defaultValue = rowSet.GetValue<Schema::ColumnAlters::DefaultValue>();
                auto notNull = rowSet.GetValueOrDefault<Schema::ColumnAlters::NotNull>(false);

                NScheme::TTypeInfoMod typeInfoMod;
                if (typeData) {
                    NKikimrProto::TTypeInfo protoType;
                    Y_VERIFY(ParseFromStringNoSizeLimit(protoType, typeData));
                    typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(typeId, &protoType);
                } else {
                    typeInfoMod.TypeInfo = NScheme::TTypeInfo(typeId);
                }

                columnRows.emplace_back(pathId, colId,
                                        colName, typeInfoMod.TypeInfo, typeInfoMod.TypeMod,
                                        keyOrder, createVersion, deleteVersion,
                                        family, defaultKind, defaultValue, notNull);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedColumnAlters>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                TPathId pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedColumnAlters::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedColumnAlters::LocalPathId>());

                ui32 colId = rowSet.GetValue<Schema::MigratedColumnAlters::ColId>();
                TString colName = rowSet.GetValue<Schema::MigratedColumnAlters::ColName>();
                NScheme::TTypeId typeId = (NScheme::TTypeId)rowSet.GetValue<Schema::MigratedColumnAlters::ColType>();
                TString typeData = rowSet.GetValueOrDefault<Schema::MigratedColumnAlters::ColTypeData>("");
                ui32 keyOrder = rowSet.GetValue<Schema::MigratedColumnAlters::ColKeyOrder>();
                ui64 createVersion = rowSet.GetValueOrDefault<Schema::MigratedColumnAlters::CreateVersion>(0);
                ui64 deleteVersion = rowSet.GetValueOrDefault<Schema::MigratedColumnAlters::DeleteVersion>(-1);
                ui32 family = rowSet.GetValueOrDefault<Schema::MigratedColumnAlters::Family>(0);
                auto defaultKind = rowSet.GetValue<Schema::MigratedColumnAlters::DefaultKind>();
                auto defaultValue = rowSet.GetValue<Schema::MigratedColumnAlters::DefaultValue>();
                auto notNull = rowSet.GetValueOrDefault<Schema::MigratedColumnAlters::NotNull>(false);

                NScheme::TTypeInfoMod typeInfoMod;
                if (typeData) {
                    NKikimrProto::TTypeInfo protoType;
                    Y_VERIFY(ParseFromStringNoSizeLimit(protoType, typeData));
                    typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(typeId, &protoType);
                } else {
                    typeInfoMod.TypeInfo = NScheme::TTypeInfo(typeId);
                }

                columnRows.emplace_back(pathId, colId,
                                        colName, typeInfoMod.TypeInfo, typeInfoMod.TypeMod,
                                        keyOrder, createVersion, deleteVersion,
                                        family, defaultKind, defaultValue, notNull);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    typedef std::tuple<TPathId, ui64, TString, TShardIdx, ui64, ui64> TTablePartitionRec;
    typedef TDeque<TTablePartitionRec> TTablePartitionsRows;

    bool LoadTablePartitions(NIceDb::TNiceDb& db, TTablePartitionsRows& partitionsRows) const {
        {
            auto rowSet = db.Table<Schema::TablePartitions>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rowSet.GetValue<Schema::TablePartitions::TabId>());
                ui64 id = rowSet.GetValue<Schema::TablePartitions::Id>();
                TString rangeEnd = rowSet.GetValue<Schema::TablePartitions::RangeEnd>();
                TShardIdx datashardIdx = Self->MakeLocalId(rowSet.GetValue<Schema::TablePartitions::DatashardIdx>());
                ui64 lastCondErase = rowSet.GetValueOrDefault<Schema::TablePartitions::LastCondErase>(0);
                ui64 nextCondErase = rowSet.GetValueOrDefault<Schema::TablePartitions::NextCondErase>(0);

                partitionsRows.emplace_back(pathId, id, rangeEnd, datashardIdx, lastCondErase, nextCondErase);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedTablePartitions>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                TPathId pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedTablePartitions::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedTablePartitions::LocalPathId>());
                ui64 id = rowSet.GetValue<Schema::MigratedTablePartitions::Id>();
                TString rangeEnd = rowSet.GetValue<Schema::MigratedTablePartitions::RangeEnd>();
                TShardIdx datashardIdx = TShardIdx(rowSet.GetValue<Schema::MigratedTablePartitions::OwnerShardIdx>(),
                                                   rowSet.GetValue<Schema::MigratedTablePartitions::LocalShardIdx>());
                ui64 lastCondErase = rowSet.GetValueOrDefault<Schema::MigratedTablePartitions::LastCondErase>(0);
                ui64 nextCondErase = rowSet.GetValueOrDefault<Schema::MigratedTablePartitions::NextCondErase>(0);

                partitionsRows.emplace_back(pathId, id, rangeEnd, datashardIdx, lastCondErase, nextCondErase);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        // We need to sort partitions by PathId/PartitionId due to incompatible change 1
        std::sort(partitionsRows.begin(), partitionsRows.end());

        return true;
    }

    typedef std::tuple<TShardIdx, TString> TTableShardPartitionConfigRec;
    typedef TDeque<TTableShardPartitionConfigRec> TTableShardPartitionConfigRows;

    bool LoadTableShardPartitionConfigs(NIceDb::TNiceDb& db, TTableShardPartitionConfigRows& partitionsRows) const {
        {
            auto rowSet = db.Table<Schema::TableShardPartitionConfigs>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                TShardIdx shardIdx = Self->MakeLocalId(rowSet.GetValue<Schema::TableShardPartitionConfigs::ShardIdx>());
                TString data = rowSet.GetValue<Schema::TableShardPartitionConfigs::PartitionConfig>();

                partitionsRows.emplace_back(shardIdx, data);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedTableShardPartitionConfigs>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                TShardIdx shardIdx = TShardIdx(
                    rowSet.GetValue<Schema::MigratedTableShardPartitionConfigs::OwnerShardIdx>(),
                    rowSet.GetValue<Schema::MigratedTableShardPartitionConfigs::LocalShardIdx>());
                TString data = rowSet.GetValue<Schema::MigratedTableShardPartitionConfigs::PartitionConfig>();

                partitionsRows.emplace_back(shardIdx, data);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    typedef std::tuple<TTxId, TPathId, ui64> TPublicationRec;
    typedef TDeque<TPublicationRec> TPublicationsRows;

    bool LoadPublications(NIceDb::TNiceDb& db, TPublicationsRows& publicationsRows) const {
        {
            auto rowSet = db.Table<Schema::PublishingPaths>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                TTxId txId = rowSet.GetValue<Schema::PublishingPaths::TxId>();
                TPathId pathId = Self->MakeLocalId(rowSet.GetValue<Schema::PublishingPaths::PathId>());
                ui64 version = rowSet.GetValue<Schema::PublishingPaths::Version>();

                publicationsRows.emplace_back(txId, pathId, version);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedPublishingPaths>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                TTxId txId = rowSet.GetValue<Schema::MigratedPublishingPaths::TxId>();
                TPathId pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedPublishingPaths::PathOwnerId>(),
                    rowSet.GetValue<Schema::MigratedPublishingPaths::LocalPathId>());
                ui64 version = rowSet.GetValue<Schema::MigratedPublishingPaths::Version>();

                publicationsRows.emplace_back(txId, pathId, version);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    typedef std::tuple<TShardIdx> TShardsToDeleteRec;
    typedef TDeque<TShardsToDeleteRec> TShardsToDeleteRows;

    bool LoadShardsToDelete(NIceDb::TNiceDb& db, TShardsToDeleteRows& shardsToDelete) const {
        {
            auto rowSet = db.Table<Schema::ShardsToDelete>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                TShardIdx shardIdx = Self->MakeLocalId(rowSet.GetValue<Schema::ShardsToDelete::ShardIdx>());

                shardsToDelete.emplace_back(shardIdx);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedShardsToDelete>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                TShardIdx shardIdx = TShardIdx(
                    rowSet.GetValue<Schema::MigratedShardsToDelete::ShardOwnerId>(),
                    rowSet.GetValue<Schema::MigratedShardsToDelete::ShardLocalIdx>());

                shardsToDelete.emplace_back(shardIdx);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    typedef std::tuple<TOperationId, TShardIdx, TTxState::ETxState> TTxShardRec;
    typedef TVector<TTxShardRec> TTxShardsRows;

    bool LoadTxShards(NIceDb::TNiceDb& db, TTxShardsRows& txShards) const {
        {
            auto rowset = db.Table<Schema::TxShards>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                auto operationId = TOperationId(rowset.GetValue<Schema::TxShards::TxId>(), 0);
                TShardIdx shardIdx = Self->MakeLocalId(rowset.GetValue<Schema::TxShards::ShardIdx>());
                TTxState::ETxState operation = (TTxState::ETxState)rowset.GetValue<Schema::TxShards::Operation>();

                txShards.emplace_back(operationId, shardIdx, operation);

                if (!rowset.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowset = db.Table<Schema::TxShardsV2>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                auto operationId = TOperationId(rowset.GetValue<Schema::TxShardsV2::TxId>(),
                                                rowset.GetValue<Schema::TxShardsV2::TxPartId>());
                TShardIdx shardIdx = Self->MakeLocalId(rowset.GetValue<Schema::TxShardsV2::ShardIdx>());
                TTxState::ETxState operation = (TTxState::ETxState)rowset.GetValue<Schema::TxShardsV2::Operation>();

                txShards.emplace_back(operationId, shardIdx, operation);

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowset = db.Table<Schema::MigratedTxShards>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                auto operationId = TOperationId(rowset.GetValue<Schema::MigratedTxShards::TxId>(),
                                                rowset.GetValue<Schema::MigratedTxShards::TxPartId>());
                TShardIdx shardIdx = TShardIdx(rowset.GetValue<Schema::MigratedTxShards::ShardOwnerId>(),
                                               rowset.GetValue<Schema::MigratedTxShards::ShardLocalIdx>());
                TTxState::ETxState operation = (TTxState::ETxState)rowset.GetValue<Schema::MigratedTxShards::Operation>();

                txShards.emplace_back(operationId, shardIdx, operation);

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        Sort(txShards);
        auto last = Unique(txShards.begin(), txShards.end());
        txShards.erase(last, txShards.end());

        return true;
    }

    typedef std::tuple<TShardIdx, TTabletId, TPathId, TTxId, TTabletTypes::EType> TShardsRec;
    typedef TDeque<TShardsRec> TShardsRows;

    bool LoadShards(NIceDb::TNiceDb& db, TShardsRows& shards) const {
        {
            auto rowSet = db.Table<Schema::Shards>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto shardIdx = Self->MakeLocalId(rowSet.GetValue<Schema::Shards::ShardIdx>());
                auto tabletID = rowSet.GetValue<Schema::Shards::TabletId>();
                auto pathId = TPathId(rowSet.GetValueOrDefault<Schema::Shards::OwnerPathId>(Self->TabletID()),
                                       rowSet.GetValue<Schema::Shards::PathId>());
                auto currentTxId = rowSet.GetValueOrDefault<Schema::Shards::LastTxId>(InvalidTxId);
                auto tabletType = rowSet.GetValue<Schema::Shards::TabletType>();

                shards.emplace_back(shardIdx, tabletID, pathId, currentTxId, tabletType);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedShards>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto shardIdx = TShardIdx(rowSet.GetValue<Schema::MigratedShards::OwnerShardId>(),
                                          rowSet.GetValue<Schema::MigratedShards::LocalShardId>());
                auto tabletID = rowSet.GetValue<Schema::MigratedShards::TabletId>();
                auto pathId = TPathId(rowSet.GetValue<Schema::MigratedShards::OwnerPathId>(),
                                      rowSet.GetValue<Schema::MigratedShards::LocalPathId>());
                auto currentTxId = rowSet.GetValueOrDefault<Schema::MigratedShards::LastTxId>(InvalidTxId);
                auto tabletType = rowSet.GetValue<Schema::MigratedShards::TabletType>();

                shards.emplace_back(shardIdx, tabletID, pathId, currentTxId, tabletType);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    typedef std::tuple<TPathId, TString, TString, TString, TString, bool, TString, ui32> TBackupSettingsRec;
    typedef TDeque<TBackupSettingsRec> TBackupSettingsRows;

    bool LoadBackupSettings(NIceDb::TNiceDb& db, TBackupSettingsRows& settings) const {
        {
            auto rowSet = db.Table<Schema::BackupSettings>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::BackupSettings::PathId>());
                auto tableName = rowSet.GetValue<Schema::BackupSettings::TableName>();
                auto ytSettings = rowSet.GetValueOrDefault<Schema::BackupSettings::YTSettings>("");
                auto s3Settings = rowSet.GetValueOrDefault<Schema::BackupSettings::S3Settings>("");
                auto scanSettings = rowSet.GetValueOrDefault<Schema::BackupSettings::ScanSettings>("");
                auto needToBill = rowSet.GetValueOrDefault<Schema::BackupSettings::NeedToBill>(true);
                auto tableDesc = rowSet.GetValueOrDefault<Schema::BackupSettings::TableDescription>("");
                auto nRetries = rowSet.GetValueOrDefault<Schema::BackupSettings::NumberOfRetries>(0);

                settings.emplace_back(pathId, tableName, ytSettings, s3Settings, scanSettings, needToBill, tableDesc, nRetries);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedBackupSettings>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto pathId = TPathId(rowSet.GetValue<Schema::MigratedBackupSettings::OwnerPathId>(),
                                      rowSet.GetValue<Schema::MigratedBackupSettings::LocalPathId>());
                auto tableName = rowSet.GetValue<Schema::MigratedBackupSettings::TableName>();
                auto ytSettings = rowSet.GetValueOrDefault<Schema::MigratedBackupSettings::YTSettings>("");
                auto s3Settings = rowSet.GetValueOrDefault<Schema::MigratedBackupSettings::S3Settings>("");
                auto scanSettings = rowSet.GetValueOrDefault<Schema::MigratedBackupSettings::ScanSettings>("");
                auto needToBill = rowSet.GetValueOrDefault<Schema::MigratedBackupSettings::NeedToBill>(true);
                auto tableDesc = rowSet.GetValueOrDefault<Schema::MigratedBackupSettings::TableDescription>("");
                auto nRetries = rowSet.GetValueOrDefault<Schema::MigratedBackupSettings::NumberOfRetries>(0);

                settings.emplace_back(pathId, tableName, ytSettings, s3Settings, scanSettings, needToBill, tableDesc, nRetries);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    typedef std::tuple<TPathId, TTxId, ui64, ui32, ui32, ui64, ui64, ui8> TCompletedBackupRestoreRec;
    typedef TDeque<TCompletedBackupRestoreRec> TCompletedBackupRestoreRows;

    bool LoadBackupRestoreHistory(NIceDb::TNiceDb& db, TCompletedBackupRestoreRows& history) const {
        {
            auto rowSet = db.Table<Schema::CompletedBackups>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::CompletedBackups::PathId>());
                auto txId = rowSet.GetValue<Schema::CompletedBackups::TxId>();
                auto completeTime = rowSet.GetValue<Schema::CompletedBackups::DateTimeOfCompletion>();

                auto successShardsCount = rowSet.GetValue<Schema::CompletedBackups::SuccessShardCount>();
                auto totalShardCount = rowSet.GetValue<Schema::CompletedBackups::TotalShardCount>();
                auto startTime = rowSet.GetValue<Schema::CompletedBackups::StartTime>();
                auto dataSize = rowSet.GetValue<Schema::CompletedBackups::DataTotalSize>();
                auto kind = rowSet.GetValueOrDefault<Schema::CompletedBackups::Kind>(0);

                history.emplace_back(pathId, txId, completeTime, successShardsCount, totalShardCount, startTime, dataSize, kind);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedCompletedBackups>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto pathId = TPathId(rowSet.GetValue<Schema::MigratedCompletedBackups::OwnerPathId>(),
                                      rowSet.GetValue<Schema::MigratedCompletedBackups::LocalPathId>());
                auto txId = rowSet.GetValue<Schema::MigratedCompletedBackups::TxId>();
                auto completeTime = rowSet.GetValue<Schema::MigratedCompletedBackups::DateTimeOfCompletion>();

                auto successShardsCount = rowSet.GetValue<Schema::MigratedCompletedBackups::SuccessShardCount>();
                auto totalShardCount = rowSet.GetValue<Schema::MigratedCompletedBackups::TotalShardCount>();
                auto startTime = rowSet.GetValue<Schema::MigratedCompletedBackups::StartTime>();
                auto dataSize = rowSet.GetValue<Schema::MigratedCompletedBackups::DataTotalSize>();
                auto kind = rowSet.GetValueOrDefault<Schema::MigratedCompletedBackups::Kind>(0);

                history.emplace_back(pathId, txId, completeTime, successShardsCount, totalShardCount, startTime, dataSize, kind);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    typedef std::tuple<TTxId, TShardIdx, bool, TString, ui64, ui64> TShardBackupStatusRec;
    typedef TDeque<TShardBackupStatusRec> TShardBackupStatusRows;

    template <typename T, typename U, typename V>
    bool LoadBackupStatusesImpl(TShardBackupStatusRows& statuses,
            T& byShardBackupStatus, U& byMigratedShardBackupStatus, V& byTxShardStatus) const {
        {
            T& rowSet = byShardBackupStatus;
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto txId = rowSet.template GetValue<Schema::ShardBackupStatus::TxId>();
                auto shardIdx = Self->MakeLocalId(rowSet.template GetValue<Schema::ShardBackupStatus::ShardIdx>());
                auto explain = rowSet.template GetValue<Schema::ShardBackupStatus::Explain>();

                statuses.emplace_back(txId, shardIdx, false, explain, 0, 0);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            U& rowSet = byMigratedShardBackupStatus;
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto txId = rowSet.template GetValue<Schema::MigratedShardBackupStatus::TxId>();
                auto shardIdx = TShardIdx(rowSet.template GetValue<Schema::MigratedShardBackupStatus::OwnerShardId>(),
                                          rowSet.template GetValue<Schema::MigratedShardBackupStatus::LocalShardId>());
                auto explain = rowSet.template GetValue<Schema::MigratedShardBackupStatus::Explain>();

                statuses.emplace_back(txId, shardIdx, false, explain, 0, 0);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            V& rowSet = byTxShardStatus;
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto txId = rowSet.template GetValue<Schema::TxShardStatus::TxId>();
                auto shardIdx = TShardIdx(
                    rowSet.template GetValue<Schema::TxShardStatus::OwnerShardId>(),
                    rowSet.template GetValue<Schema::TxShardStatus::LocalShardId>()
                );
                auto success = rowSet.template GetValue<Schema::TxShardStatus::Success>();
                auto error = rowSet.template GetValue<Schema::TxShardStatus::Error>();
                auto bytes = rowSet.template GetValue<Schema::TxShardStatus::BytesProcessed>();
                auto rows = rowSet.template GetValue<Schema::TxShardStatus::RowsProcessed>();

                statuses.emplace_back(txId, shardIdx, success, error, bytes, rows);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    bool LoadBackupStatuses(NIceDb::TNiceDb& db, TShardBackupStatusRows& statuses) const {
        auto byShardBackupStatus = db.Table<Schema::ShardBackupStatus>().Range().Select();
        auto byMigratedShardBackupStatus = db.Table<Schema::MigratedShardBackupStatus>().Range().Select();
        auto byTxShardStatus = db.Table<Schema::TxShardStatus>().Range().Select();

        return LoadBackupStatusesImpl(statuses, byShardBackupStatus, byMigratedShardBackupStatus, byTxShardStatus);
    }

    typedef std::tuple<TPathId, ui64, NKikimrSchemeOp::EIndexType, NKikimrSchemeOp::EIndexState> TTableIndexRec;
    typedef TDeque<TTableIndexRec> TTableIndexRows;

    bool LoadTableIndexes(NIceDb::TNiceDb& db, TTableIndexRows& tableIndexes) const {
        {
            auto rowSet = db.Table<Schema::TableIndex>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto pathId = Self->MakeLocalId(TLocalPathId(rowSet.GetValue<Schema::TableIndex::PathId>()));
                auto alterVersion = rowSet.GetValue<Schema::TableIndex::AlterVersion>();
                auto type = rowSet.GetValue<Schema::TableIndex::IndexType>();
                auto state = rowSet.GetValue<Schema::TableIndex::State>();

                tableIndexes.emplace_back(pathId, alterVersion, type, state);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedTableIndex>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto pathId = TPathId(TOwnerId(rowSet.GetValue<Schema::MigratedTableIndex::OwnerPathId>()),
                                      TLocalPathId(rowSet.GetValue<Schema::MigratedTableIndex::LocalPathId>()));
                auto alterVersion = rowSet.GetValue<Schema::MigratedTableIndex::AlterVersion>();
                auto type = rowSet.GetValue<Schema::MigratedTableIndex::IndexType>();
                auto state = rowSet.GetValue<Schema::MigratedTableIndex::State>();

                tableIndexes.emplace_back(pathId, alterVersion, type, state);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    typedef std::tuple<TPathId, ui64, TString> TTableIndexColRec;
    typedef TDeque<TTableIndexColRec> TTableIndexKeyRows;
    typedef TDeque<TTableIndexColRec> TTableIndexDataRows;

    bool LoadTableIndexKeys(NIceDb::TNiceDb& db, TTableIndexKeyRows& tableIndexKeys) const {
        {
            auto rowSet = db.Table<Schema::TableIndexKeys>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto pathId = Self->MakeLocalId(TLocalPathId(rowSet.GetValue<Schema::TableIndexKeys::PathId>()));
                auto id = rowSet.GetValue<Schema::TableIndexKeys::KeyId>();
                auto name = rowSet.GetValue<Schema::TableIndexKeys::KeyName>();

                tableIndexKeys.emplace_back(pathId, id, name);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedTableIndexKeys>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto pathId = TPathId(rowSet.GetValue<Schema::MigratedTableIndexKeys::OwnerPathId>(),
                                      rowSet.GetValue<Schema::MigratedTableIndexKeys::LocalPathId>());
                auto id = rowSet.GetValue<Schema::MigratedTableIndexKeys::KeyId>();
                auto name = rowSet.GetValue<Schema::MigratedTableIndexKeys::KeyName>();

                tableIndexKeys.emplace_back(pathId, id, name);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    bool LoadTableIndexDataColumns(NIceDb::TNiceDb& db, TTableIndexDataRows& tableIndexData) const {
        auto rowSet = db.Table<Schema::TableIndexDataColumns>().Range().Select();
        if (!rowSet.IsReady()) {
            return false;
        }

        while (!rowSet.EndOfSet()) {
            auto pathId = TPathId(rowSet.GetValue<Schema::TableIndexDataColumns::PathOwnerId>(),
                                  rowSet.GetValue<Schema::TableIndexDataColumns::PathLocalId>());
            auto id = rowSet.GetValue<Schema::TableIndexDataColumns::DataColumnId>();
            auto name = rowSet.GetValue<Schema::TableIndexDataColumns::DataColumnName>();

            tableIndexData.emplace_back(pathId, id, name);
            if (!rowSet.Next()) {
                return false;
            }
        }
        return true;
    }

    typedef std::tuple<TShardIdx, ui32, TString, TString> TChannelBindingRec;
    typedef TDeque<TChannelBindingRec> TChannelBindingRows;

    bool LoadChannelBindings(NIceDb::TNiceDb& db, TChannelBindingRows& tableIndexKeys) const {
        {
            auto rowSet = db.Table<Schema::ChannelsBinding>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto shardIdx = Self->MakeLocalId(rowSet.GetValue<Schema::ChannelsBinding::ShardId>());
                ui32 channelId = rowSet.GetValue<Schema::ChannelsBinding::ChannelId>();
                TString binding = rowSet.GetValue<Schema::ChannelsBinding::Binding>();
                TString poolName = rowSet.GetValue<Schema::ChannelsBinding::PoolName>();

                tableIndexKeys.emplace_back(shardIdx, channelId, binding, poolName);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedChannelsBinding>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto shardIdx = TShardIdx(rowSet.GetValue<Schema::MigratedChannelsBinding::OwnerShardId>(),
                                          rowSet.GetValue<Schema::MigratedChannelsBinding::LocalShardId>());
                ui32 channelId = rowSet.GetValue<Schema::MigratedChannelsBinding::ChannelId>();
                TString binding = rowSet.GetValue<Schema::MigratedChannelsBinding::Binding>();
                TString poolName = rowSet.GetValue<Schema::MigratedChannelsBinding::PoolName>();

                tableIndexKeys.emplace_back(shardIdx, channelId, binding, poolName);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    typedef std::tuple<TPathId, TString, ui64> TKesusInfosRec;
    typedef TDeque<TKesusInfosRec> TKesusInfosRows;

    bool LoadKesusInfos(NIceDb::TNiceDb& db, TKesusInfosRows& kesusInfosData) const {
        {
            auto rowSet = db.Table<Schema::KesusInfos>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::KesusInfos::PathId>());
                auto config = rowSet.GetValue<Schema::KesusInfos::Config>();
                auto version = rowSet.GetValueOrDefault<Schema::KesusInfos::Version>();

                kesusInfosData.emplace_back(pathId, config, version);
                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedKesusInfos>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto pathId = TPathId(rowSet.GetValue<Schema::MigratedKesusInfos::OwnerPathId>(),
                                      rowSet.GetValue<Schema::MigratedKesusInfos::LocalPathId>());
                auto config = rowSet.GetValue<Schema::MigratedKesusInfos::Config>();
                auto version = rowSet.GetValueOrDefault<Schema::MigratedKesusInfos::Version>();

                kesusInfosData.emplace_back(pathId, config, version);
                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    typedef std::tuple<TPathId, TString, ui64> TKesusAlterRec;
    typedef TDeque<TKesusAlterRec> TKesusAlterRows;

    bool LoadKesusAlters(NIceDb::TNiceDb& db, TKesusAlterRows& kesusAlterData) const {
        {
            auto rowSet = db.Table<Schema::KesusAlters>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::KesusAlters::PathId>());
                auto config = rowSet.GetValue<Schema::KesusAlters::Config>();
                auto version = rowSet.GetValueOrDefault<Schema::KesusAlters::Version>();

                kesusAlterData.emplace_back(pathId, config, version);
                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        {
            auto rowSet = db.Table<Schema::MigratedKesusAlters>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto pathId = TPathId(rowSet.GetValue<Schema::MigratedKesusAlters::OwnerPathId>(),
                                      rowSet.GetValue<Schema::MigratedKesusAlters::LocalPathId>());
                auto config = rowSet.GetValue<Schema::MigratedKesusAlters::Config>();
                auto version = rowSet.GetValueOrDefault<Schema::MigratedKesusAlters::Version>();

                kesusAlterData.emplace_back(pathId, config, version);
                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    bool ReadEverything(TTransactionContext& txc, const TActorContext& ctx) {
        const TOwnerId selfId = Self->TabletID();

        Self->Clear();

        NIceDb::TNiceDb db(txc.DB);
        if (!db.Precharge<Schema>()) {
            return false;
        }

#define RETURN_IF_NO_PRECHARGED(readIsOk) \
        if (!readIsOk) { \
            return false;\
        }

        RETURN_IF_NO_PRECHARGED(Self->ReadSysValue(db, Schema::SysParam_MaxIncompatibleChange, Self->MaxIncompatibleChange));
        if (Self->MaxIncompatibleChange > Schema::MaxIncompatibleChangeSupported) {
            LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TTxInit, unsupported changes detected: MaxIncompatibleChange = " << Self->MaxIncompatibleChange <<
                        ", MaxIncompatibleChangeSupported = " << Schema::MaxIncompatibleChangeSupported <<
                        ", restarting!");
            Self->BreakTabletAndRestart(ctx);
            Broken = true;
            return true;
        }

        {
            ui64 initStateVal = (ui64)TTenantInitState::InvalidState;
            RETURN_IF_NO_PRECHARGED(Self->ReadSysValue(db, Schema::SysParam_TenantInitState, initStateVal, (ui64)TTenantInitState::InvalidState));
            Self->InitState = TTenantInitState::EInitState(initStateVal);
        }

        RETURN_IF_NO_PRECHARGED(Self->ReadSysValue(db, Schema::SysParam_NextPathId, Self->NextLocalPathId));
        RETURN_IF_NO_PRECHARGED(Self->ReadSysValue(db, Schema::SysParam_NextShardIdx, Self->NextLocalShardIdx));

        {
            ui64 isReadOnlyModeVal = 0;
            RETURN_IF_NO_PRECHARGED(Self->ReadSysValue(db, Schema::SysParam_IsReadOnlyMode, isReadOnlyModeVal));
            Self->IsReadOnlyMode = isReadOnlyModeVal;
        }

        if (!Self->IsDomainSchemeShard) {
            ui64 parentDomainSchemeShard = 0;
            RETURN_IF_NO_PRECHARGED(Self->ReadSysValue(db, Schema::SysParam_ParentDomainSchemeShard, parentDomainSchemeShard));

            ui64 parentDomainPathId = 0;
            RETURN_IF_NO_PRECHARGED(Self->ReadSysValue(db, Schema::SysParam_ParentDomainPathId, parentDomainPathId));

            Self->ParentDomainId = TPathId(parentDomainSchemeShard, parentDomainPathId);

            RETURN_IF_NO_PRECHARGED(Self->ReadSysValue(db, Schema::SysParam_ParentDomainOwner, Self->ParentDomainOwner));
            RETURN_IF_NO_PRECHARGED(Self->ReadSysValue(db, Schema::SysParam_ParentDomainEffectiveACL, Self->ParentDomainEffectiveACL));
            RETURN_IF_NO_PRECHARGED(Self->ReadSysValue(db, Schema::SysParam_ParentDomainEffectiveACLVersion, Self->ParentDomainEffectiveACLVersion));

            Self->ParentDomainCachedEffectiveACL.Init(Self->ParentDomainEffectiveACL);
        } else {
            Self->ParentDomainId = Self->RootPathId();
        }

        {
            ui64 secondsSinceEpoch = 0;
            RETURN_IF_NO_PRECHARGED(Self->ReadSysValue(db, Schema::SysParam_ServerlessStorageLastBillTime, secondsSinceEpoch));
            Self->ServerlessStorageLastBillTime = TInstant::Seconds(secondsSinceEpoch);
        }

#undef RETURN_IF_NO_PRECHARGED

        if (!Self->IsSchemeShardConfigured()) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit, SS hasn't been configured yet"
                             << ", state: " << (ui64)Self->InitState
                             << ", at schemeshard: " << Self->TabletID());
            return true;
        }

        // Read reversed migrations
        {
            auto attrsRowset = db.Table<Schema::RevertedMigrations>().Range().Select();
            if (!attrsRowset.IsReady()) {
                return false;
            }

            while (!attrsRowset.EndOfSet()) {
                TLocalPathId localPathId = attrsRowset.GetValue<Schema::RevertedMigrations::LocalPathId>();
                TPathId pathId = Self->MakeLocalId(localPathId);
                TTabletId abandonedSchemeShardID = attrsRowset.GetValue<Schema::RevertedMigrations::SchemeShardId>();

                Self->RevertedMigrations[pathId].push_back(abandonedSchemeShardID);

                if (!attrsRowset.Next()) {
                    return false;
                }
            }
        }

        // Read paths
        {

            TPathRows pathRows;
            if (!LoadPaths(db, pathRows)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TTxInit for Paths"
                             << ", read records: " << pathRows.size()
                             << ", at schemeshard: " << Self->TabletID());

            if (pathRows) {
                // read Root
                TPathElement::TPtr rootPath = MakePathElement(pathRows.front());
                Y_VERIFY(rootPath->PathId == Self->RootPathId());
                Y_VERIFY(rootPath->ParentPathId == Self->RootPathId());
                Y_VERIFY(rootPath->DomainPathId == Self->RootPathId());

                Y_VERIFY(!IsStartWithSlash(rootPath->Name));
                Self->RootPathElements = SplitPath(rootPath->Name);

                Y_VERIFY(!rootPath->StepDropped);
                Self->PathsById[rootPath->PathId] = rootPath;

                pathRows.pop_front();
            }

            Y_VERIFY_DEBUG(IsSorted(pathRows.begin(), pathRows.end()));

            for (auto& rec: pathRows) {
                TPathElement::TPtr path = MakePathElement(rec);

                Y_VERIFY(path->PathId != Self->RootPathId());

                Y_VERIFY_S(!Self->PathsById.contains(path->PathId), "Path already exists"
                               << ", pathId: " << path->PathId);

                TPathElement::TPtr parent = Self->PathsById.at(path->ParentPathId);
                parent->DbRefCount++;
                parent->AllChildrenCount++;

                Self->AttachChild(path);
                Self->PathsById[path->PathId] = path;
            }
        }

        // Read user attrs
        {
            TUserAttrsRows userAttrsRows;
            if (!LoadUserAttrs(db, userAttrsRows)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for UserAttributes"
                             << ", read records: " << userAttrsRows.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (auto& rec: userAttrsRows) {
                TPathId pathId = std::get<0>(rec);
                TString name = std::get<1>(rec);
                TString value = std::get<2>(rec);

                Y_VERIFY_S(Self->PathsById.contains(pathId), "Unknown pathId: " << pathId);
                auto pathElem = Self->PathsById.at(pathId);

                Y_VERIFY(pathElem->UserAttrs);
                Y_VERIFY(pathElem->UserAttrs->AlterVersion > 0);
                pathElem->UserAttrs->Set(name, value);
            }
        }

        // Read user attrs alter data
        {
            TUserAttrsRows userAttrsRows;
            if (!LoadUserAttrsAlterData(db, userAttrsRows)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for UserAttributesAlterData"
                             << ", read records: " << userAttrsRows.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (auto& rec: userAttrsRows) {
                TPathId pathId = std::get<0>(rec);
                TString name = std::get<1>(rec);
                TString value = std::get<2>(rec);

                Y_VERIFY_S(Self->PathsById.contains(pathId), "Unknown pathId: " << pathId);
                auto pathElem = Self->PathsById.at(pathId);

                Y_VERIFY(pathElem->UserAttrs);
                if (pathElem->UserAttrs->AlterData == nullptr) {
                    pathElem->UserAttrs->AlterData = new TUserAttributes(pathElem->UserAttrs->AlterVersion + 1);
                }

                pathElem->UserAttrs->AlterData->Set(name, value);
            }
        }

        // Read SubDomains
        {
            TSchemeLimits rootLimits = TSchemeShard::DefaultLimits;

            if (Self->PathsById.contains(Self->RootPathId()) && Self->IsDomainSchemeShard) {
                auto row = db.Table<Schema::SubDomains>().Key(Self->RootPathId().LocalPathId).Select();

                if (!row.IsReady())
                    return false;

                ui64 version = 0;

                if (row.IsValid()) {
                    version = row.GetValue<Schema::SubDomains::AlterVersion>();
                    rootLimits.MaxDepth = row.GetValueOrDefault<Schema::SubDomains::DepthLimit>(rootLimits.MaxDepth);
                    rootLimits.MaxPaths = row.GetValueOrDefault<Schema::SubDomains::PathsLimit>(rootLimits.MaxPathsCompat);
                    rootLimits.MaxChildrenInDir = row.GetValueOrDefault<Schema::SubDomains::ChildrenLimit>(rootLimits.MaxChildrenInDir);
                    rootLimits.MaxAclBytesSize = row.GetValueOrDefault<Schema::SubDomains::AclByteSizeLimit>(rootLimits.MaxAclBytesSize);
                    rootLimits.MaxTableColumns = row.GetValueOrDefault<Schema::SubDomains::TableColumnsLimit>(rootLimits.MaxTableColumns);
                    rootLimits.MaxTableColumnNameLength = row.GetValueOrDefault<Schema::SubDomains::TableColumnNameLengthLimit>(rootLimits.MaxTableColumnNameLength);
                    rootLimits.MaxTableKeyColumns = row.GetValueOrDefault<Schema::SubDomains::TableKeyColumnsLimit>(rootLimits.MaxTableKeyColumns);
                    rootLimits.MaxTableIndices = row.GetValueOrDefault<Schema::SubDomains::TableIndicesLimit>(rootLimits.MaxTableIndices);
                    rootLimits.MaxTableCdcStreams = row.GetValueOrDefault<Schema::SubDomains::TableCdcStreamsLimit>(rootLimits.MaxTableCdcStreams);
                    rootLimits.MaxShards = row.GetValueOrDefault<Schema::SubDomains::ShardsLimit>(rootLimits.MaxShards);
                    rootLimits.MaxShardsInPath = row.GetValueOrDefault<Schema::SubDomains::PathShardsLimit>(rootLimits.MaxShardsInPath);
                    rootLimits.MaxConsistentCopyTargets = row.GetValueOrDefault<Schema::SubDomains::ConsistentCopyingTargetsLimit>(rootLimits.MaxConsistentCopyTargets);
                    rootLimits.MaxPathElementLength = row.GetValueOrDefault<Schema::SubDomains::PathElementLength>(rootLimits.MaxPathElementLength);
                    rootLimits.ExtraPathSymbolsAllowed = row.GetValueOrDefault<Schema::SubDomains::ExtraPathSymbolsAllowed>(rootLimits.ExtraPathSymbolsAllowed);
                    rootLimits.MaxPQPartitions = row.GetValueOrDefault<Schema::SubDomains::PQPartitionsLimit>(rootLimits.MaxPQPartitions);
                }

                TSubDomainInfo::TPtr rootDomainInfo = new TSubDomainInfo(version, Self->RootPathId());
                rootDomainInfo->SetSchemeLimits(rootLimits);
                rootDomainInfo->SetSecurityStateVersion(row.GetValueOrDefault<Schema::SubDomains::SecurityStateVersion>());

                rootDomainInfo->InitializeAsGlobal(Self->CreateRootProcessingParams(ctx));

                Self->SubDomains[Self->RootPathId()] = rootDomainInfo;
            }

            auto rowset = db.Table<Schema::SubDomains>().Range().Select();
            if (!rowset.IsReady())
                return false;
            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::SubDomains::PathId>();
                TPathId pathId(selfId, localPathId);

                Y_VERIFY_S(Self->PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);

                TPathElement::TPtr path = Self->PathsById.at(pathId);
                Y_VERIFY_S(path->IsDomainRoot(), "Path is not a domain, pathId: " << pathId);

                if (!path->IsRoot() || !Self->IsDomainSchemeShard) {
                    Y_VERIFY(!Self->SubDomains.contains(pathId));

                    ui64 alterVersion = rowset.GetValue<Schema::SubDomains::AlterVersion>();
                    ui64 planResolution = rowset.GetValue<Schema::SubDomains::PlanResolution>();
                    ui32 timeCastBuckets = rowset.GetValue<Schema::SubDomains::TimeCastBuckets>();
                    TPathId resourcesDomainId = TPathId(
                        rowset.GetValue<Schema::SubDomains::ResourcesDomainOwnerPathId>(),
                        rowset.GetValue<Schema::SubDomains::ResourcesDomainLocalPathId>());
                    TSubDomainInfo::TPtr domainInfo = new TSubDomainInfo(
                        alterVersion,
                        planResolution,
                        timeCastBuckets,
                        resourcesDomainId);
                    Self->SubDomains[pathId] = domainInfo;
                    Self->IncrementPathDbRefCount(pathId);

                    TTabletId sharedHiveId = rowset.GetValue<Schema::SubDomains::SharedHiveId>();
                    domainInfo->SetSharedHive(sharedHiveId);

                    TSchemeLimits limits = rootLimits;
                    limits.MaxDepth = rowset.GetValueOrDefault<Schema::SubDomains::DepthLimit>(limits.MaxDepth);
                    limits.MaxPaths = rowset.GetValueOrDefault<Schema::SubDomains::PathsLimit>(limits.MaxPaths);
                    limits.MaxChildrenInDir = rowset.GetValueOrDefault<Schema::SubDomains::ChildrenLimit>(limits.MaxChildrenInDir);
                    limits.MaxAclBytesSize = rowset.GetValueOrDefault<Schema::SubDomains::AclByteSizeLimit>(limits.MaxAclBytesSize);
                    limits.MaxTableColumns = rowset.GetValueOrDefault<Schema::SubDomains::TableColumnsLimit>(limits.MaxTableColumns);
                    limits.MaxTableColumnNameLength = rowset.GetValueOrDefault<Schema::SubDomains::TableColumnNameLengthLimit>(limits.MaxTableColumnNameLength);
                    limits.MaxTableKeyColumns = rowset.GetValueOrDefault<Schema::SubDomains::TableKeyColumnsLimit>(limits.MaxTableKeyColumns);
                    limits.MaxTableIndices = rowset.GetValueOrDefault<Schema::SubDomains::TableIndicesLimit>(limits.MaxTableIndices);
                    limits.MaxTableCdcStreams = rowset.GetValueOrDefault<Schema::SubDomains::TableCdcStreamsLimit>(limits.MaxTableCdcStreams);
                    limits.MaxShards = rowset.GetValueOrDefault<Schema::SubDomains::ShardsLimit>(limits.MaxShards);
                    limits.MaxShardsInPath = rowset.GetValueOrDefault<Schema::SubDomains::PathShardsLimit>(limits.MaxShardsInPath);
                    limits.MaxConsistentCopyTargets = rowset.GetValueOrDefault<Schema::SubDomains::ConsistentCopyingTargetsLimit>(limits.MaxConsistentCopyTargets);
                    limits.MaxPathElementLength = rowset.GetValueOrDefault<Schema::SubDomains::PathElementLength>(limits.MaxPathElementLength);
                    limits.ExtraPathSymbolsAllowed = rowset.GetValueOrDefault<Schema::SubDomains::ExtraPathSymbolsAllowed>(limits.ExtraPathSymbolsAllowed);
                    limits.MaxPQPartitions = rowset.GetValueOrDefault<Schema::SubDomains::PQPartitionsLimit>(limits.MaxPQPartitions);

                    domainInfo->SetSchemeLimits(limits);

                    if (rowset.HaveValue<Schema::SubDomains::DeclaredSchemeQuotas>()) {
                        NKikimrSubDomains::TSchemeQuotas declaredSchemeQuotas;
                        Y_VERIFY(ParseFromStringNoSizeLimit(declaredSchemeQuotas, rowset.GetValue<Schema::SubDomains::DeclaredSchemeQuotas>()));
                        domainInfo->SetDeclaredSchemeQuotas(declaredSchemeQuotas);
                    }

                    if (rowset.HaveValue<Schema::SubDomains::DatabaseQuotas>()) {
                        Ydb::Cms::DatabaseQuotas databaseQuotas;
                        Y_VERIFY(ParseFromStringNoSizeLimit(databaseQuotas, rowset.GetValue<Schema::SubDomains::DatabaseQuotas>()));
                        domainInfo->SetDatabaseQuotas(databaseQuotas, Self);
                    }

                    domainInfo->SetDomainStateVersion(rowset.GetValueOrDefault<Schema::SubDomains::StateVersion>(0));
                    domainInfo->SetSecurityStateVersion(rowset.GetValueOrDefault<Schema::SubDomains::SecurityStateVersion>());
                    domainInfo->SetDiskQuotaExceeded(rowset.GetValueOrDefault<Schema::SubDomains::DiskQuotaExceeded>(false));
                    if (domainInfo->GetDiskQuotaExceeded()) {
                        Self->ChangeDiskSpaceQuotaExceeded(+1);
                    }
                }

                if (!rowset.Next())
                    return false;
            }

            // Read SubDomainsAlterData
            {
                auto rowset = db.Table<Schema::SubDomainsAlterData>().Range().Select();
                if (!rowset.IsReady())
                    return false;
                while (!rowset.EndOfSet()) {
                    TLocalPathId localPathId = rowset.GetValue<Schema::SubDomainsAlterData::PathId>();
                    TPathId pathId(selfId, localPathId);

                    ui64 alterVersion = rowset.GetValue<Schema::SubDomainsAlterData::AlterVersion>();
                    ui64 planResolution = rowset.GetValue<Schema::SubDomainsAlterData::PlanResolution>();
                    ui32 timeCastBuckets = rowset.GetValue<Schema::SubDomainsAlterData::TimeCastBuckets>();

                    TPathId resourcesDomainId = TPathId(
                        rowset.GetValue<Schema::SubDomainsAlterData::ResourcesDomainOwnerPathId>(),
                        rowset.GetValue<Schema::SubDomainsAlterData::ResourcesDomainLocalPathId>());
                    if (resourcesDomainId && Self->IsDomainSchemeShard) {
                        // we cannot check that on TSS
                        Y_VERIFY_S(Self->SubDomains.contains(resourcesDomainId), "Unknown ResourcesDomainId: " << resourcesDomainId);
                    }

                    Y_VERIFY_S(Self->PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);
                    TPathElement::TPtr path = Self->PathsById.at(pathId);
                    Y_VERIFY_S(path->IsDomainRoot(), "Path is not a subdomain, pathId: " << pathId);

                    Y_VERIFY(Self->SubDomains.contains(pathId));
                    auto subdomainInfo = Self->SubDomains[pathId];
                    Y_VERIFY(!subdomainInfo->GetAlter());

                    TSubDomainInfo::TPtr alter;
                    alter = new TSubDomainInfo(
                        alterVersion,
                        planResolution,
                        timeCastBuckets,
                        resourcesDomainId);

                    TTabletId sharedHiveId = rowset.GetValue<Schema::SubDomainsAlterData::SharedHiveId>();
                    alter->SetSharedHive(sharedHiveId);

                    alter->SetSchemeLimits(subdomainInfo->GetSchemeLimits()); // do not change SchemeLimits

                    if (Self->IsDomainSchemeShard && path->IsRoot()) {
                        alter->InitializeAsGlobal(Self->CreateRootProcessingParams(ctx));
                    }

                    if (rowset.HaveValue<Schema::SubDomainsAlterData::DeclaredSchemeQuotas>()) {
                        NKikimrSubDomains::TSchemeQuotas declaredSchemeQuotas;
                        Y_VERIFY(ParseFromStringNoSizeLimit(declaredSchemeQuotas, rowset.GetValue<Schema::SubDomainsAlterData::DeclaredSchemeQuotas>()));
                        alter->SetDeclaredSchemeQuotas(declaredSchemeQuotas);
                    }

                    if (rowset.HaveValue<Schema::SubDomainsAlterData::DatabaseQuotas>()) {
                        Ydb::Cms::DatabaseQuotas databaseQuotas;
                        Y_VERIFY(ParseFromStringNoSizeLimit(databaseQuotas, rowset.GetValue<Schema::SubDomainsAlterData::DatabaseQuotas>()));
                        alter->SetDatabaseQuotas(databaseQuotas);
                    }

                    subdomainInfo->SetAlter(alter);

                    if (!rowset.Next())
                        return false;
                }
            }

            // Set ResourcesDomainId for older subdomains & check validity
            for (auto [id, subDomain] : Self->SubDomains) {
                auto alter = subDomain->GetAlter();

                if (!subDomain->GetResourcesDomainId() && (!alter || !alter->GetResourcesDomainId())) {
                    if (Self->IsDomainSchemeShard) {
                        subDomain->SetResourcesDomainId(id);
                    } else {
                        subDomain->SetResourcesDomainId(Self->ParentDomainId);
                    }

                    continue;
                }

                if (!Self->IsDomainSchemeShard) {
                    // we cannot check validity on TSS
                    continue;
                }

                if (auto resourcesDomainId = subDomain->GetResourcesDomainId()) {
                    Y_VERIFY_S(Self->SubDomains.contains(resourcesDomainId), "Unknown ResourcesDomainId: " << resourcesDomainId);
                }

                if (!alter) {
                    continue;
                }

                if (auto resourcesDomainId = alter->GetResourcesDomainId()) {
                    Y_VERIFY_S(Self->SubDomains.contains(resourcesDomainId), "Unknown ResourcesDomainId: " << resourcesDomainId);
                }
            }
        }

        // Read SubDomainShards
        {
            auto rowset = db.Table<Schema::SubDomainShards>().Range().Select();
            if (!rowset.IsReady())
                return false;
            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::SubDomainShards::PathId>();
                TPathId pathId(selfId, localPathId);

                TLocalShardIdx localShardIdx = rowset.GetValue<Schema::SubDomainShards::ShardIdx>();
                TShardIdx shardIdx = Self->MakeLocalId(localShardIdx);

                Y_VERIFY(Self->SubDomains.contains(pathId));
                Self->SubDomains[pathId]->AddPrivateShard(shardIdx);

                if (!rowset.Next())
                    return false;
            }

            // Read SubDomainShardsAlterData
            {
                auto rowset = db.Table<Schema::SubDomainShardsAlterData>().Range().Select();
                if (!rowset.IsReady())
                    return false;
                while (!rowset.EndOfSet()) {
                    TLocalPathId localPathId = rowset.GetValue<Schema::SubDomainShardsAlterData::PathId>();
                    TPathId pathId(selfId, localPathId);

                    TLocalShardIdx localShardIdx = rowset.GetValue<Schema::SubDomainShardsAlterData::ShardIdx>();
                    TShardIdx shardIdx = Self->MakeLocalId(localShardIdx);

                    Y_VERIFY(Self->SubDomains.contains(pathId));
                    auto subdomainInfo = Self->SubDomains[pathId];
                    Y_VERIFY(subdomainInfo->GetAlter());
                    subdomainInfo->GetAlter()->AddPrivateShard(shardIdx);

                    if (!rowset.Next())
                        return false;
                }
            }
        }

        // Read SubDomainSchemeQuotas
        {
            auto rowset = db.Table<Schema::SubDomainSchemeQuotas>().Range().Select();
            if (!rowset.IsReady())
                return false;
            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::SubDomainSchemeQuotas::PathId>();
                TPathId pathId(selfId, localPathId);

                if (Self->SubDomains.contains(pathId)) {
                    TSchemeQuota quota;
                    quota.BucketSize = rowset.GetValue<Schema::SubDomainSchemeQuotas::BucketSize>();
                    quota.BucketDuration = TDuration::MicroSeconds(rowset.GetValue<Schema::SubDomainSchemeQuotas::BucketDurationUs>());
                    quota.Available = rowset.GetValue<Schema::SubDomainSchemeQuotas::Available>();
                    quota.LastUpdate = TInstant::MicroSeconds(rowset.GetValue<Schema::SubDomainSchemeQuotas::LastUpdateUs>());
                    quota.Dirty = false;
                    Self->SubDomains[pathId]->AddSchemeQuota(quota);
                }

                if (!rowset.Next())
                    return false;
            }
        }

        {
            TTableRows tableRows;
            if (!LoadTables(db, tableRows)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for Tables"
                             << ", read records: " << tableRows.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (const auto& rec: tableRows) {
                TPathId pathId = std::get<0>(rec);

                Y_VERIFY_S(Self->PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);
                Y_VERIFY_S(Self->PathsById.at(pathId)->IsTable(), "Path is not a table, pathId: " << pathId);
                Y_VERIFY_S(Self->Tables.FindPtr(pathId) == nullptr, "Table duplicated in DB, pathId: " << pathId);

                TTableInfo::TPtr tableInfo = new TTableInfo();
                tableInfo->NextColumnId = std::get<1>(rec);
                tableInfo->AlterVersion = std::get<2>(rec);

                TString partitionConfig = std::get<3>(rec);
                if (partitionConfig) {
                    auto& config = tableInfo->MutablePartitionConfig();
                    bool parseOk = ParseFromStringNoSizeLimit(config, partitionConfig);
                    Y_VERIFY(parseOk);

                    if (config.ColumnFamiliesSize() > 1) {
                        // Fix any incorrect legacy config at load time
                        TPartitionConfigMerger::DeduplicateColumnFamiliesById(config);
                    }

                    if (config.HasCrossDataCenterFollowerCount()) {
                        config.ClearFollowerCount();
                    }
                }

                TString alterTabletFull = std::get<4>(rec);
                TString alterTabletDiff = std::get<5>(rec);
                if (alterTabletFull) {
                    tableInfo->InitAlterData();
                    tableInfo->AlterData->TableDescriptionFull = NKikimrSchemeOp::TTableDescription();
                    auto& tableDesc = tableInfo->AlterData->TableDescriptionFull.GetRef();
                    bool parseOk = ParseFromStringNoSizeLimit(tableDesc, alterTabletFull);
                    Y_VERIFY(parseOk);

                    if (tableDesc.HasPartitionConfig() &&
                        tableDesc.GetPartitionConfig().ColumnFamiliesSize() > 1)
                    {
                        // Fix any incorrect legacy config at load time
                        TPartitionConfigMerger::DeduplicateColumnFamiliesById(*tableDesc.MutablePartitionConfig());
                    }
                } else if (alterTabletDiff) {
                    tableInfo->InitAlterData();
                    bool parseOk = ParseFromStringNoSizeLimit(tableInfo->AlterData->TableDescriptionDiff, alterTabletDiff);
                    Y_VERIFY(parseOk);
                }

                tableInfo->PartitioningVersion = std::get<6>(rec);

                TString ttlSettings = std::get<7>(rec);
                if (ttlSettings) {
                    bool parseOk = ParseFromStringNoSizeLimit(tableInfo->MutableTTLSettings(), ttlSettings);
                    Y_VERIFY(parseOk);
                }

                tableInfo->IsBackup = std::get<8>(rec);

                Self->Tables[pathId] = tableInfo;
                Self->IncrementPathDbRefCount(pathId);
                if (tableInfo->IsTTLEnabled()) {
                    Self->TTLEnabledTables[pathId] = tableInfo;
                    Self->TabletCounters->Simple()[COUNTER_TTL_ENABLED_TABLE_COUNT].Add(1);
                }
            }

        }

        // Read External Tables
        {
            auto rowset = db.Table<Schema::ExternalTable>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                TOwnerId ownerPathId = rowset.GetValue<Schema::ExternalTable::OwnerPathId>();
                TLocalPathId localPathId = rowset.GetValue<Schema::ExternalTable::LocalPathId>();
                TPathId pathId(ownerPathId, localPathId);

                auto& externalTable = Self->ExternalTables[pathId] = new TExternalTableInfo();
                externalTable->SourceType = rowset.GetValue<Schema::ExternalTable::SourceType>();
                externalTable->DataSourcePath = rowset.GetValue<Schema::ExternalTable::DataSourcePath>();
                externalTable->Location = rowset.GetValue<Schema::ExternalTable::Location>();
                externalTable->AlterVersion = rowset.GetValue<Schema::ExternalTable::AlterVersion>();
                externalTable->Content = rowset.GetValue<Schema::ExternalTable::Content>();
                Self->IncrementPathDbRefCount(pathId);

                if (!rowset.Next())
                    return false;
            }
        }

        // Externel Data Source
        {
            auto rowset = db.Table<Schema::ExternalDataSource>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                TOwnerId ownerPathId = rowset.GetValue<Schema::ExternalDataSource::OwnerPathId>();
                TLocalPathId localPathId = rowset.GetValue<Schema::ExternalDataSource::LocalPathId>();
                TPathId pathId(ownerPathId, localPathId);

                auto& externalDataSource = Self->ExternalDataSources[pathId] = new TExternalDataSourceInfo();
                externalDataSource->AlterVersion = rowset.GetValue<Schema::ExternalDataSource::AlterVersion>();
                externalDataSource->SourceType = rowset.GetValue<Schema::ExternalDataSource::SourceType>();
                externalDataSource->Location = rowset.GetValue<Schema::ExternalDataSource::Location>();
                externalDataSource->Installation = rowset.GetValue<Schema::ExternalDataSource::Installation>();
                Y_PROTOBUF_SUPPRESS_NODISCARD externalDataSource->Auth.ParseFromString(rowset.GetValue<Schema::ExternalDataSource::Auth>());
                Y_PROTOBUF_SUPPRESS_NODISCARD externalDataSource->ExternalTableReferences.ParseFromString(rowset.GetValue<Schema::ExternalDataSource::ExternalTableReferences>());

                Self->IncrementPathDbRefCount(pathId);

                if (!rowset.Next())
                    return false;
            }
        }

        // Read table columns
        {
            TColumnRows columnRows;
            if (!LoadColumns(db, columnRows)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for Columns"
                             << ", read records: " << columnRows.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (const auto& rec: columnRows) {
                TPathId pathId = std::get<0>(rec);
                ui32 colId = std::get<1>(rec);
                TString colName = std::get<2>(rec);
                NScheme::TTypeInfo typeInfo = std::get<3>(rec);
                TString typeMod = std::get<4>(rec);
                ui32 keyOrder = std::get<5>(rec);
                ui64 createVersion = std::get<6>(rec);
                ui64 deleteVersion = std::get<7>(rec);
                ui32 family = std::get<8>(rec);
                auto defaultKind = std::get<9>(rec);
                auto defaultValue = std::get<10>(rec);
                auto notNull = std::get<11>(rec);

                Y_VERIFY_S(Self->PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);
                Y_VERIFY_S(Self->PathsById.at(pathId)->IsTable() || Self->PathsById.at(pathId)->IsExternalTable(), "Path is not a table or external table, pathId: " << pathId);
                Y_VERIFY_S(Self->Tables.FindPtr(pathId) || Self->ExternalTables.FindPtr(pathId), "Table or external table don't exist, pathId: " << pathId);

                TTableInfo::TColumn colInfo(colName, colId, typeInfo, typeMod);
                colInfo.KeyOrder = keyOrder;
                colInfo.CreateVersion = createVersion;
                colInfo.DeleteVersion = deleteVersion;
                colInfo.Family = family;
                colInfo.DefaultKind = defaultKind;
                colInfo.DefaultValue = defaultValue;
                colInfo.NotNull = notNull;

                if (auto it = Self->Tables.find(pathId); it != Self->Tables.end()) {
                    TTableInfo::TPtr tableInfo = it->second;
                    Y_VERIFY_S(colId < tableInfo->NextColumnId, "Column id should be less than NextColId"
                                << ", columnId: " << colId
                                << ", NextColId: " << tableInfo->NextColumnId);

                    tableInfo->Columns[colId] = colInfo;

                    if (colInfo.KeyOrder != (ui32)-1) {
                        tableInfo->KeyColumnIds.resize(Max<ui32>(tableInfo->KeyColumnIds.size(), colInfo.KeyOrder + 1));
                        tableInfo->KeyColumnIds[colInfo.KeyOrder] = colId;
                    }
                } else if (auto it = Self->ExternalTables.find(pathId); it != Self->ExternalTables.end()) {
                    TExternalTableInfo::TPtr externalTableInfo = it->second;
                    externalTableInfo->Columns[colId] = colInfo;
                }
            }
        }

        // Read table columns' alters
        {
            TColumnRows columnRows;
            if (!LoadColumnsAlters(db, columnRows)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for ColumnsAlters"
                             << ", read records: " << columnRows.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (const auto& rec: columnRows) {
                TPathId pathId = std::get<0>(rec);
                ui32 colId = std::get<1>(rec);
                TString colName = std::get<2>(rec);
                NScheme::TTypeInfo typeInfo = std::get<3>(rec);
                TString typeMod = std::get<4>(rec);
                ui32 keyOrder = std::get<5>(rec);
                ui64 createVersion = std::get<6>(rec);
                ui64 deleteVersion = std::get<7>(rec);
                ui32 family = std::get<8>(rec);
                auto defaultKind = std::get<9>(rec);
                auto defaultValue = std::get<10>(rec);
                auto notNull = std::get<11>(rec);

                Y_VERIFY_S(Self->PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);
                Y_VERIFY_S(Self->PathsById.at(pathId)->IsTable(), "Path is not a table, pathId: " << pathId);

                Y_VERIFY_S(Self->Tables.FindPtr(pathId), "Table doesn't exist, pathId: " << pathId);

                TTableInfo::TPtr tableInfo = Self->Tables[pathId];
                tableInfo->InitAlterData();
                if (colId >= tableInfo->AlterData->NextColumnId) {
                    tableInfo->AlterData->NextColumnId = colId + 1; // calc next NextColumnId
                }

                TTableInfo::TColumn colInfo(colName, colId, typeInfo, typeMod);
                colInfo.KeyOrder = keyOrder;
                colInfo.CreateVersion = createVersion;
                colInfo.DeleteVersion = deleteVersion;
                colInfo.Family = family;
                colInfo.DefaultKind = defaultKind;
                colInfo.DefaultValue = defaultValue;
                colInfo.NotNull = notNull;
                tableInfo->AlterData->Columns[colId] = colInfo;
            }
        }

        // Read shards (any type of tablets)
        THashMap<TPathId, TShardIdx> pqBalancers; // pathId -> shardIdx
        THashMap<TPathId, TShardIdx> nbsVolumeShards; // pathId -> shardIdx
        THashMap<TPathId, TShardIdx> fileStoreShards; // pathId -> shardIdx
        THashMap<TPathId, TShardIdx> kesusShards; // pathId -> shardIdx
        THashMap<TPathId, TShardIdx> replicationControllers;
        THashMap<TPathId, TShardIdx> blobDepotShards;
        THashMap<TPathId, TVector<TShardIdx>> olapColumnShards;
        {
            TShardsRows shards;
            if (!LoadShards(db, shards)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for Shards"
                             << ", read records: " << shards.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (auto& rec: shards) {
                TShardIdx idx = std::get<0>(rec);

                LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                             "TTxInit for Shards"
                                << ", read: " << idx
                                << ", tabletId: " << std::get<1>(rec)
                                << ", PathId: " << std::get<2>(rec)
                                << ", TabletType: " << TTabletTypes::TypeToStr(std::get<4>(rec))
                                << ", at schemeshard: " << Self->TabletID());

                Y_VERIFY(!Self->ShardInfos.contains(idx));
                TShardInfo& shard = Self->ShardInfos[idx];

                shard.TabletID = std::get<1>(rec);
                shard.PathId = std::get<2>(rec);
                shard.CurrentTxId = std::get<3>(rec);
                shard.TabletType = std::get<4>(rec);

                Self->IncrementPathDbRefCount(shard.PathId);

                Y_VERIFY(shard.TabletType != ETabletType::TypeInvalid, "upgrade schema was wrong");

                switch (shard.TabletType) {
                    case ETabletType::PersQueueReadBalancer:
                        pqBalancers[shard.PathId] = idx;
                        break;
                    case ETabletType::BlockStoreVolume:
                        nbsVolumeShards[shard.PathId] = idx;
                        break;
                    case ETabletType::FileStore:
                        fileStoreShards[shard.PathId] = idx;
                        break;
                    case ETabletType::Kesus:
                        kesusShards[shard.PathId] = idx;
                        break;
                    case ETabletType::ColumnShard:
                        olapColumnShards[shard.PathId].push_back(idx);
                        break;
                    case ETabletType::ReplicationController:
                        replicationControllers.emplace(shard.PathId, idx);
                        break;
                    case ETabletType::BlobDepot:
                        blobDepotShards.emplace(shard.PathId, idx);
                        break;
                    default:
                        break;
                }
            }
        }

        {
            auto adoptedRowset = db.Table<Schema::AdoptedShards>().Range().Select();
            if (!adoptedRowset.IsReady())
                return false;
            while (!adoptedRowset.EndOfSet()) {
                TLocalShardIdx localShardIdx = adoptedRowset.GetValue<Schema::AdoptedShards::ShardIdx>();
                TShardIdx shardIdx = Self->MakeLocalId(localShardIdx);
                TAdoptedShard& adoptedShard = Self->AdoptedShards[shardIdx];
                adoptedShard.PrevOwner = adoptedRowset.GetValue<Schema::AdoptedShards::PrevOwner>();
                adoptedShard.PrevShardIdx = adoptedRowset.GetValue<Schema::AdoptedShards::PrevShardIdx>();

                TShardInfo* shard = Self->ShardInfos.FindPtr(shardIdx);
                Y_VERIFY(shard);

                TTabletId tabletId = adoptedRowset.GetValue<Schema::AdoptedShards::TabletId>();
                Y_VERIFY(shard->TabletID == InvalidTabletId || shard->TabletID == tabletId);
                shard->TabletID = tabletId;

                if (!adoptedRowset.Next())
                    return false;
            }
        }

        // Read partitions
        {
            TTablePartitionsRows tablePartitions;
            if (!LoadTablePartitions(db, tablePartitions)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for TablePartitions"
                             << ", read records: " << tablePartitions.size()
                             << ", at schemeshard: " << Self->TabletID());

            TPathId prevTableId;
            TVector<TTableShardInfo> partitions;

            const auto now = ctx.Now();
            for (auto& rec: tablePartitions) {
                TPathId tableId = std::get<0>(rec);
                ui64 id = std::get<1>(rec);
                TString rangeEnd = std::get<2>(rec);
                TShardIdx datashardIdx = std::get<3>(rec);
                ui64 lastCondErase = std::get<4>(rec);
                ui64 nextCondErase = std::get<5>(rec);

                if (tableId != prevTableId) {
                    if (prevTableId) {
                        Y_VERIFY(!partitions.empty());
                        Y_VERIFY(Self->Tables.contains(prevTableId));
                        TTableInfo::TPtr tableInfo = Self->Tables.at(prevTableId);
                        Self->SetPartitioning(prevTableId, tableInfo, std::move(partitions));
                        partitions.clear();
                    }

                    prevTableId = tableId;
                }

                // TODO: check that table exists
                if (partitions.size() <= id) {
                    partitions.resize(id+1);
                }

                partitions[id] = TTableShardInfo(datashardIdx, rangeEnd, lastCondErase, nextCondErase);

                if (Self->TTLEnabledTables.contains(tableId)) {
                    auto& lag = partitions[id].LastCondEraseLag;
                    if (now >= partitions[id].LastCondErase) {
                        lag = now - partitions[id].LastCondErase;
                    } else {
                        lag = TDuration::Zero();
                    }

                    Self->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].IncrementFor(lag->Seconds());
                }

                // TODO: check rangeEnd validity
                // TODO: check datashard idx existence
            }

            if (prevTableId) {
                Y_VERIFY(!partitions.empty());
                Y_VERIFY(Self->Tables.contains(prevTableId));
                TTableInfo::TPtr tableInfo = Self->Tables.at(prevTableId);
                Self->SetPartitioning(prevTableId, tableInfo, std::move(partitions));
            }
        }

        // Read partition config patches
        {
            TTableShardPartitionConfigRows tablePartitions;
            if (!LoadTableShardPartitionConfigs(db, tablePartitions)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for TableShardPartitionConfigs"
                             << ", read records: " << tablePartitions.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (auto& rec: tablePartitions) {
                TShardIdx shardIdx = std::get<0>(rec);
                TString data = std::get<1>(rec);

                // NOTE: we ignore rows for shards we don't know
                if (auto* shardInfo = Self->ShardInfos.FindPtr(shardIdx)) {
                    // NOTE: we ignore rows for shards that don't belong to known tables
                    auto it = Self->Tables.find(shardInfo->PathId);
                    if (it != Self->Tables.end()) {
                        bool parseOk = ParseFromStringNoSizeLimit(it->second->PerShardPartitionConfig[shardIdx], data);
                        Y_VERIFY(parseOk);
                    }
                }
            }
        }

        // Read partition stats
        {
            auto rowSet = db.Table<Schema::TablePartitionStats>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            TPathId prevTableId;

            while (!rowSet.EndOfSet()) {
                const TPathId tableId = TPathId(
                    rowSet.GetValue<Schema::TablePartitionStats::TableOwnerId>(),
                    rowSet.GetValue<Schema::TablePartitionStats::TableLocalId>());

                if (tableId != prevTableId) {
                    if (prevTableId) {
                        Y_VERIFY(Self->Tables.contains(prevTableId));
                        TTableInfo::TPtr tableInfo = Self->Tables.at(prevTableId);
                        if (!tableInfo->IsBackup && !tableInfo->IsShardsStatsDetached()) {
                            Self->ResolveDomainInfo(prevTableId)->AggrDiskSpaceUsage(Self, tableInfo->GetStats().Aggregated);
                        }
                    }

                    prevTableId = tableId;
                }

                Y_VERIFY(Self->Tables.contains(tableId));
                TTableInfo::TPtr tableInfo = Self->Tables.at(tableId);

                const ui64 partitionId = rowSet.GetValue<Schema::TablePartitionStats::PartitionId>();
                Y_VERIFY(partitionId < tableInfo->GetPartitions().size());

                const TShardIdx shardIdx = tableInfo->GetPartitions()[partitionId].ShardIdx;
                Y_VERIFY(shardIdx != InvalidShardIdx);

                if (Self->ShardInfos.contains(shardIdx)) {
                    const TShardInfo& shardInfo = Self->ShardInfos.at(shardIdx);
                    if (shardInfo.PathId != tableId) {
                         tableInfo->DetachShardsStats();
                    }
                }

                TPartitionStats stats;

                stats.SeqNo = TMessageSeqNo(
                    rowSet.GetValue<Schema::TablePartitionStats::SeqNoGeneration>(),
                    rowSet.GetValue<Schema::TablePartitionStats::SeqNoRound>());

                stats.RowCount = rowSet.GetValue<Schema::TablePartitionStats::RowCount>();
                stats.DataSize = rowSet.GetValue<Schema::TablePartitionStats::DataSize>();
                stats.IndexSize = rowSet.GetValue<Schema::TablePartitionStats::IndexSize>();

                stats.LastAccessTime = TInstant::FromValue(rowSet.GetValue<Schema::TablePartitionStats::LastAccessTime>());
                stats.LastUpdateTime = TInstant::FromValue(rowSet.GetValue<Schema::TablePartitionStats::LastUpdateTime>());

                stats.ImmediateTxCompleted = rowSet.GetValue<Schema::TablePartitionStats::ImmediateTxCompleted>();
                stats.PlannedTxCompleted = rowSet.GetValue<Schema::TablePartitionStats::PlannedTxCompleted>();
                stats.TxRejectedByOverload = rowSet.GetValue<Schema::TablePartitionStats::TxRejectedByOverload>();
                stats.TxRejectedBySpace = rowSet.GetValue<Schema::TablePartitionStats::TxRejectedBySpace>();
                stats.TxCompleteLag = TDuration::FromValue(rowSet.GetValue<Schema::TablePartitionStats::TxCompleteLag>());
                stats.InFlightTxCount = rowSet.GetValue<Schema::TablePartitionStats::InFlightTxCount>();

                stats.RowUpdates = rowSet.GetValue<Schema::TablePartitionStats::RowUpdates>();
                stats.RowDeletes = rowSet.GetValue<Schema::TablePartitionStats::RowDeletes>();
                stats.RowReads = rowSet.GetValue<Schema::TablePartitionStats::RowReads>();
                stats.RangeReads = rowSet.GetValue<Schema::TablePartitionStats::RangeReads>();
                stats.RangeReadRows = rowSet.GetValue<Schema::TablePartitionStats::RangeReadRows>();

                TInstant now = AppData(ctx)->TimeProvider->Now();
                stats.SetCurrentRawCpuUsage(rowSet.GetValue<Schema::TablePartitionStats::CPU>(), now);
                stats.Memory = rowSet.GetValue<Schema::TablePartitionStats::Memory>();
                stats.Network = rowSet.GetValue<Schema::TablePartitionStats::Network>();
                stats.Storage = rowSet.GetValue<Schema::TablePartitionStats::Storage>();
                stats.ReadThroughput = rowSet.GetValue<Schema::TablePartitionStats::ReadThroughput>();
                stats.WriteThroughput = rowSet.GetValue<Schema::TablePartitionStats::WriteThroughput>();
                stats.ReadIops = rowSet.GetValue<Schema::TablePartitionStats::ReadIops>();
                stats.WriteIops = rowSet.GetValue<Schema::TablePartitionStats::WriteIops>();

                stats.SearchHeight = rowSet.GetValueOrDefault<Schema::TablePartitionStats::SearchHeight>();
                stats.FullCompactionTs = rowSet.GetValueOrDefault<Schema::TablePartitionStats::FullCompactionTs>();
                stats.MemDataSize = rowSet.GetValueOrDefault<Schema::TablePartitionStats::MemDataSize>();

                tableInfo->UpdateShardStats(shardIdx, stats);

                // note that we don't update shard metrics here, because we will always update
                // the shard metrics in TSchemeShard::SetPartitioning

                if (!rowSet.Next()) {
                    return false;
                }
            }

            if (prevTableId) {
                Y_VERIFY(Self->Tables.contains(prevTableId));
                TTableInfo::TPtr tableInfo = Self->Tables.at(prevTableId);
                if (!tableInfo->IsBackup && !tableInfo->IsShardsStatsDetached()) {
                    Self->ResolveDomainInfo(prevTableId)->AggrDiskSpaceUsage(Self, tableInfo->GetStats().Aggregated);
                }
            }
        }

        // Read channels binding
        {
            TChannelBindingRows channelBindingRows;
            if (!LoadChannelBindings(db, channelBindingRows)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for ChannelsBinding"
                             << ", read records: " << channelBindingRows.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (auto& rec: channelBindingRows) {
                TShardIdx shardIdx = std::get<0>(rec);
                ui32 channelId = std::get<1>(rec);
                TString bindingData = std::get<2>(rec);
                TString poolName = std::get<3>(rec);

                Y_VERIFY(Self->ShardInfos.contains(shardIdx));
                TShardInfo& shardInfo = Self->ShardInfos[shardIdx];
                if (shardInfo.BindedChannels.size() <= channelId) {
                    shardInfo.BindedChannels.resize(channelId + 1);
                }
                TChannelBind& channelBind = shardInfo.BindedChannels[channelId];

                if (bindingData) {
                    bool parseOk = ParseFromStringNoSizeLimit(channelBind, bindingData);
                    Y_VERIFY(parseOk);
                }
                if (poolName) {
                    channelBind.SetStoragePoolName(poolName);
                }
            }

        }

        // Read PersQueue groups
        {
            auto rowset = db.Table<Schema::PersQueueGroups>().Range().Select();
            if (!rowset.IsReady())
                return false;
            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::PersQueueGroups::PathId>();
                TPathId pathId(selfId, localPathId);

                TTopicInfo::TPtr pqGroup = new TTopicInfo();
                pqGroup->TabletConfig = rowset.GetValue<Schema::PersQueueGroups::TabletConfig>();
                pqGroup->MaxPartsPerTablet = rowset.GetValue<Schema::PersQueueGroups::MaxPQPerShard>();
                pqGroup->AlterVersion = rowset.GetValue<Schema::PersQueueGroups::AlterVersion>();
                pqGroup->NextPartitionId = rowset.GetValueOrDefault<Schema::PersQueueGroups::NextPartitionId>(0);
                pqGroup->TotalGroupCount = rowset.GetValueOrDefault<Schema::PersQueueGroups::TotalGroupCount>(0);

                const bool ok = pqGroup->FillKeySchema(pqGroup->TabletConfig);
                Y_VERIFY(ok);

                Self->Topics[pathId] = pqGroup;
                Self->IncrementPathDbRefCount(pathId);

                auto it = pqBalancers.find(pathId);
                if (it != pqBalancers.end()) {
                    auto idx = it->second;
                    Y_VERIFY(Self->ShardInfos.contains(idx));
                    const TShardInfo& shard = Self->ShardInfos.at(idx);
                    pqGroup->BalancerTabletID = shard.TabletID;
                    pqGroup->BalancerShardIdx = idx;
                }

                if (!rowset.Next())
                    return false;
            }
        }

        // Read PersQueues
        {
            auto rowset = db.Table<Schema::PersQueues>().Range().Select();
            if (!rowset.IsReady())
                return false;
            while (!rowset.EndOfSet()) {
                TTopicTabletInfo::TTopicPartitionInfo pqInfo;
                TLocalPathId localPathId = rowset.GetValue<Schema::PersQueues::PathId>();
                TPathId pathId(selfId, localPathId);
                pqInfo.PqId = rowset.GetValue<Schema::PersQueues::PqId>();
                pqInfo.GroupId = rowset.GetValueOrDefault<Schema::PersQueues::GroupId>(pqInfo.PqId + 1);
                TLocalShardIdx localShardIdx = rowset.GetValue<Schema::PersQueues::ShardIdx>();
                TShardIdx shardIdx = Self->MakeLocalId(localShardIdx);
                pqInfo.AlterVersion = rowset.GetValue<Schema::PersQueues::AlterVersion>();

                if (rowset.HaveValue<Schema::PersQueues::RangeBegin>()) {
                    if (!pqInfo.KeyRange) {
                        pqInfo.KeyRange.ConstructInPlace();
                    }

                    pqInfo.KeyRange->FromBound = rowset.GetValue<Schema::PersQueues::RangeBegin>();
                }

                if (rowset.HaveValue<Schema::PersQueues::RangeEnd>()) {
                    if (!pqInfo.KeyRange) {
                        pqInfo.KeyRange.ConstructInPlace();
                    }

                    pqInfo.KeyRange->ToBound = rowset.GetValue<Schema::PersQueues::RangeEnd>();
                }

                auto it = Self->Topics.find(pathId);
                Y_VERIFY(it != Self->Topics.end());
                Y_VERIFY(it->second);
                TTopicInfo::TPtr pqGroup = it->second;
                if (pqInfo.AlterVersion <= pqGroup->AlterVersion)
                    ++pqGroup->TotalPartitionCount;
                if (pqInfo.PqId >= pqGroup->NextPartitionId) {
                    pqGroup->NextPartitionId = pqInfo.PqId + 1;
                    pqGroup->TotalGroupCount = pqInfo.PqId + 1;
                }

                TTopicTabletInfo::TPtr& pqShard = pqGroup->Shards[shardIdx];
                if (!pqShard) {
                    pqShard.Reset(new TTopicTabletInfo());
                }
                pqShard->Partitions.push_back(pqInfo);

                if (!rowset.Next())
                    return false;
            }
        }

        // Read PersQueue groups' alters
        {
            auto rowset = db.Table<Schema::PersQueueGroupAlters>().Range().Select();
            if (!rowset.IsReady())
                return false;
            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::PersQueueGroupAlters::PathId>();
                TPathId pathId(selfId, localPathId);

                TTopicInfo::TPtr alterData = new TTopicInfo();
                alterData->TabletConfig = rowset.GetValue<Schema::PersQueueGroupAlters::TabletConfig>();
                alterData->MaxPartsPerTablet = rowset.GetValue<Schema::PersQueueGroupAlters::MaxPQPerShard>();
                alterData->AlterVersion = rowset.GetValue<Schema::PersQueueGroupAlters::AlterVersion>();
                alterData->TotalGroupCount = rowset.GetValue<Schema::PersQueueGroupAlters::TotalGroupCount>();
                alterData->NextPartitionId = rowset.GetValueOrDefault<Schema::PersQueueGroupAlters::NextPartitionId>(alterData->TotalGroupCount);
                alterData->BootstrapConfig = rowset.GetValue<Schema::PersQueueGroupAlters::BootstrapConfig>();

                const bool ok = alterData->FillKeySchema(alterData->TabletConfig);
                Y_VERIFY(ok);

                auto it = Self->Topics.find(pathId);
                Y_VERIFY(it != Self->Topics.end());

                alterData->TotalPartitionCount = it->second->GetTotalPartitionCountWithAlter();
                alterData->BalancerTabletID = it->second->BalancerTabletID;
                alterData->BalancerShardIdx = it->second->BalancerShardIdx;
                it->second->AlterData = alterData;

                if (!rowset.Next())
                    return false;
            }
        }

        // Read PersQueue groups stats
        {
            auto rowset = db.Table<Schema::PersQueueGroupStats>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }
            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::PersQueueGroupStats::PathId>();
                TPathId pathId(selfId, localPathId);

                auto it = Self->Topics.find(pathId);
                if (it != Self->Topics.end()) {
                    auto& topic = it->second;

                    auto dataSize = rowset.GetValue<Schema::PersQueueGroupStats::DataSize>();
                    auto usedReserveSize = rowset.GetValue<Schema::PersQueueGroupStats::UsedReserveSize>();
                    if (dataSize >= usedReserveSize) {
                        topic->Stats.SeqNo = TMessageSeqNo(rowset.GetValue<Schema::PersQueueGroupStats::SeqNoGeneration>(), rowset.GetValue<Schema::PersQueueGroupStats::SeqNoRound>());
                        topic->Stats.DataSize = dataSize;
                        topic->Stats.UsedReserveSize = usedReserveSize;

                        Self->ResolveDomainInfo(pathId)->AggrDiskSpaceUsage(topic->Stats, {});
                    }
                }

                if (!rowset.Next()) {
                    return false;
                }
            }
        }


        // Read RTMR volumes
        {
            auto rowset = db.Table<Schema::RtmrVolumes>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::RtmrVolumes::PathId>();
                TPathId pathId(selfId, localPathId);

                Self->RtmrVolumes[pathId] = new TRtmrVolumeInfo();
                Self->IncrementPathDbRefCount(pathId);

                if (!rowset.Next())
                    return false;
            }
        }

        // Read RTMR partitions
        {
            auto rowset = db.Table<Schema::RTMRPartitions>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::RTMRPartitions::PathId>();
                TPathId pathId(selfId, localPathId);

                auto it = Self->RtmrVolumes.find(pathId);
                Y_VERIFY(it != Self->RtmrVolumes.end());
                Y_VERIFY(it->second);

                auto partitionId = rowset.GetValue<Schema::RTMRPartitions::PartitionId>();
                Y_VERIFY(partitionId.size() == sizeof(TGUID));

                TGUID guidId;
                Copy(partitionId.cbegin(), partitionId.cend(), (char*)guidId.dw);

                ui64 busKey = rowset.GetValue<Schema::RTMRPartitions::BusKey>();
                TLocalShardIdx localShardIdx = rowset.GetValue<Schema::RTMRPartitions::ShardIdx>();
                TShardIdx shardIdx = Self->MakeLocalId(localShardIdx);
                auto tabletId = Self->ShardInfos.at(shardIdx).TabletID;

                TRtmrPartitionInfo::TPtr partitionInfo = new TRtmrPartitionInfo(guidId, busKey, shardIdx, tabletId);
                it->second->Partitions[shardIdx] = partitionInfo;

                if (!rowset.Next())
                    return false;
            }
        }

        // Read Solomon volumes
        {
            auto rowset = db.Table<Schema::SolomonVolumes>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::SolomonVolumes::PathId>();
                TPathId pathId = Self->MakeLocalId(localPathId);
                ui64 version = rowset.GetValueOrDefault<Schema::SolomonVolumes::Version>(1);

                TSolomonVolumeInfo::TPtr solomon = new TSolomonVolumeInfo(version);
                solomon->Version = version;

                Self->SolomonVolumes[pathId] = solomon;
                Self->IncrementPathDbRefCount(pathId);

                if (!rowset.Next())
                    return false;
            }
        }

        // Read Solomon partitions
        {
            auto rowset = db.Table<Schema::SolomonPartitions>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::SolomonPartitions::PathId>();
                TPathId pathId = Self->MakeLocalId(localPathId);

                auto it = Self->SolomonVolumes.find(pathId);
                Y_VERIFY(it != Self->SolomonVolumes.end());
                Y_VERIFY(it->second);

                ui64 partitionId = rowset.GetValue<Schema::SolomonPartitions::PartitionId>();
                TLocalShardIdx localShardIdx = rowset.GetValue<Schema::SolomonPartitions::ShardId>();
                TShardIdx shardIdx = Self->MakeLocalId(localShardIdx);

                auto shardInfo = Self->ShardInfos.FindPtr(shardIdx);
                if (shardInfo) {
                    it->second->Partitions[shardIdx] = new TSolomonPartitionInfo(partitionId, shardInfo->TabletID);
                } else {
                    it->second->Partitions[shardIdx] = new TSolomonPartitionInfo(partitionId);
                }


                if (!rowset.Next())
                    return false;
            }
        }

        // Read Alter Solomon volumes
        {
            auto rowset = db.Table<Schema::AlterSolomonVolumes>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                auto pathId = TPathId(rowset.GetValue<Schema::AlterSolomonVolumes::OwnerPathId>(),
                                      rowset.GetValue<Schema::AlterSolomonVolumes::LocalPathId>());
                ui64 version = rowset.GetValue<Schema::AlterSolomonVolumes::Version>();

                Y_VERIFY(Self->SolomonVolumes.contains(pathId));
                TSolomonVolumeInfo::TPtr solomon = Self->SolomonVolumes.at(pathId);

                Y_VERIFY(solomon->AlterData == nullptr);
                solomon->AlterData = solomon->CreateAlter(version);

                if (!rowset.Next())
                    return false;
            }
        }

        // Read Alter Solomon partitions
        {
            auto rowset = db.Table<Schema::AlterSolomonPartitions>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                const auto pathId = TPathId(rowset.GetValue<Schema::AlterSolomonPartitions::OwnerPathId>(),
                                            rowset.GetValue<Schema::AlterSolomonPartitions::LocalPathId>());

                const auto shardIdx = TShardIdx(rowset.GetValue<Schema::AlterSolomonPartitions::ShardOwnerId>(),
                                                rowset.GetValue<Schema::AlterSolomonPartitions::ShardLocalIdx>());

                const ui64 partitionId = rowset.GetValue<Schema::AlterSolomonPartitions::PartitionId>();

                Y_VERIFY(Self->SolomonVolumes.contains(pathId));
                TSolomonVolumeInfo::TPtr solomon = Self->SolomonVolumes.at(pathId);
                Y_VERIFY(solomon->AlterData);

                if (solomon->Partitions.size() <= partitionId) {
                    Y_VERIFY(!solomon->AlterData->Partitions.contains(shardIdx));

                    auto shardInfo = Self->ShardInfos.FindPtr(shardIdx);
                    if (shardInfo) {
                        solomon->AlterData->Partitions[shardIdx] = new TSolomonPartitionInfo(partitionId, shardInfo->TabletID);
                    } else {
                        solomon->AlterData->Partitions[shardIdx] = new TSolomonPartitionInfo(partitionId);
                    }

                } else {
                    //old partition
                    Y_VERIFY(solomon->AlterData->Partitions.contains(shardIdx));
                    Y_VERIFY(solomon->AlterData->Partitions.at(shardIdx)->PartitionId == partitionId);
                    Y_VERIFY(Self->ShardInfos.contains(shardIdx));
                }

                if (!rowset.Next())
                    return false;
            }
        }

        // Read Table Indexes
        {
            TTableIndexRows indexes;
            if (!LoadTableIndexes(db, indexes)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for TableIndexes"
                             << ", read records: " << indexes.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (auto& rec: indexes) {
                TPathId pathId = std::get<0>(rec);
                ui64 alterVersion = std::get<1>(rec);
                TTableIndexInfo::EType indexType = std::get<2>(rec);
                TTableIndexInfo::EState state = std::get<3>(rec);

                Y_VERIFY_S(Self->PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);
                TPathElement::TPtr path = Self->PathsById.at(pathId);
                Y_VERIFY_S(path->IsTableIndex(), "Path is not a table index"
                               << ", pathId: " << pathId
                               << ", path type: " << NKikimrSchemeOp::EPathType_Name(path->PathType));

                Y_VERIFY(!Self->Indexes.contains(pathId));
                Self->Indexes[pathId] = new TTableIndexInfo(alterVersion, indexType, state);
                Self->IncrementPathDbRefCount(pathId);
            }

            // Read IndexesAlterData
            {
                auto rowset = db.Table<Schema::TableIndexAlterData>().Range().Select();
                if (!rowset.IsReady())
                    return false;

                while (!rowset.EndOfSet()) {
                    TLocalPathId localPathId = rowset.GetValue<Schema::TableIndexAlterData::PathId>();
                    TPathId pathId(selfId, localPathId);

                    ui64 alterVersion = rowset.GetValue<Schema::TableIndexAlterData::AlterVersion>();
                    TTableIndexInfo::EType indexType = rowset.GetValue<Schema::TableIndexAlterData::IndexType>();
                    TTableIndexInfo::EState state = rowset.GetValue<Schema::TableIndexAlterData::State>();

                    Y_VERIFY_S(Self->PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);
                    TPathElement::TPtr path = Self->PathsById.at(pathId);
                    Y_VERIFY_S(path->IsTableIndex(), "Path is not a table index, pathId: " << pathId);

                    if (!Self->Indexes.contains(pathId)) {
                        Self->Indexes[pathId] = TTableIndexInfo::NotExistedYet(indexType);
                        Self->IncrementPathDbRefCount(pathId);
                    }
                    auto tableIndex = Self->Indexes.at(pathId);
                    Y_VERIFY(tableIndex->AlterData == nullptr);
                    Y_VERIFY(tableIndex->AlterVersion < alterVersion);
                    tableIndex->AlterData = new TTableIndexInfo(alterVersion, indexType, state);

                    Y_VERIFY_S(Self->PathsById.contains(path->ParentPathId), "Parent path is not found"
                                   << ", index pathId: " << pathId
                                   << ", parent pathId: " << path->ParentPathId);
                    TPathElement::TPtr parent = Self->PathsById.at(path->ParentPathId);
                    Y_VERIFY_S(parent->IsTable(), "Parent path is not a table"
                                   << ", index pathId: " << pathId
                                   << ", parent pathId: " << path->ParentPathId);

                    if (!rowset.Next()) {
                        return false;
                    }
                }
            }
        }

        // Read Table TableIndexKeys
        {

            TTableIndexKeyRows indexKeys;
            TTableIndexDataRows indexData;
            if (!LoadTableIndexKeys(db, indexKeys)) {
                return false;
            }

            if (!LoadTableIndexDataColumns(db, indexData)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for TableIndexKeys"
                             << ", read records: " << indexKeys.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (const auto& rec: indexKeys) {
                TPathId pathId = std::get<0>(rec);
                ui32 keyId = std::get<1>(rec);
                TString keyName = std::get<2>(rec);

                Y_VERIFY_S(Self->PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);
                TPathElement::TPtr path = Self->PathsById.at(pathId);
                Y_VERIFY_S(path->IsTableIndex(), "Path is not a table index, pathId: " << pathId);

                Y_VERIFY(Self->Indexes.contains(pathId));
                auto tableIndex = Self->Indexes.at(pathId);

                Y_VERIFY(keyId == tableIndex->IndexKeys.size());
                tableIndex->IndexKeys.emplace_back(keyName);
            }

            // See KIKIMR-13300 and restore VERIFY after at least one restart
            TVector<std::pair<TPathId, ui32>> leakedDataColumns;
            for (const auto& rec: indexData) {
                TPathId pathId = std::get<0>(rec);
                ui32 dataId = std::get<1>(rec);
                TString dataName = std::get<2>(rec);

                if (Self->PathsById.contains(pathId)) {
                    TPathElement::TPtr path = Self->PathsById.at(pathId);
                    Y_VERIFY_S(path->IsTableIndex(), "Path is not a table index, pathId: " << pathId);

                    if (Self->Indexes.contains(pathId)) {
                        auto tableIndex = Self->Indexes.at(pathId);

                        Y_VERIFY(dataId == tableIndex->IndexDataColumns.size());
                        tableIndex->IndexDataColumns.emplace_back(dataName);
                    } else {
                        leakedDataColumns.emplace_back(pathId, dataId);
                    }
                } else {
                    leakedDataColumns.emplace_back(pathId, dataId);
                }
            }

            for (const auto& pair : leakedDataColumns) {
                db.Table<Schema::TableIndexDataColumns>().Key(pair.first.OwnerId, pair.first.LocalPathId, pair.second).Delete();
            }
            leakedDataColumns.clear();

            // Read TableIndexKeysAlterData
            {
                auto rowset = db.Table<Schema::TableIndexKeysAlterData>().Range().Select();
                if (!rowset.IsReady())
                    return false;

                while (!rowset.EndOfSet()) {
                    TLocalPathId localPathId = rowset.GetValue<Schema::TableIndexKeysAlterData::PathId>();
                    TPathId pathId(selfId, localPathId);

                    ui32 keyId = rowset.GetValue<Schema::TableIndexKeysAlterData::KeyId>();
                    TString keyName = rowset.GetValue<Schema::TableIndexKeysAlterData::KeyName>();

                    Y_VERIFY_S(Self->PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);
                    TPathElement::TPtr path = Self->PathsById.at(pathId);
                    Y_VERIFY_S(path->IsTableIndex(), "Path is not a table index, pathId: " << pathId);

                    Y_VERIFY(Self->Indexes.contains(pathId));
                    auto tableIndex = Self->Indexes.at(pathId);

                    Y_VERIFY(tableIndex->AlterData != nullptr);
                    auto alterData = tableIndex->AlterData;
                    Y_VERIFY(tableIndex->AlterVersion < alterData->AlterVersion);

                    Y_VERIFY(keyId == alterData->IndexKeys.size());
                    alterData->IndexKeys.emplace_back(keyName);

                    if (!rowset.Next())
                        return false;
                }
            }

            // Read TableIndexDataColumnsAlterData
            {
                TVector<std::pair<TPathId, ui32>> leakedDataColumnsAlterData;
                auto rowset = db.Table<Schema::TableIndexDataColumnsAlterData>().Range().Select();
                if (!rowset.IsReady())
                    return false;

                while (!rowset.EndOfSet()) {
                    TOwnerId ownerId = rowset.GetValue<Schema::TableIndexDataColumnsAlterData::PathOwnerId>();
                    TLocalPathId localPathId = rowset.GetValue<Schema::TableIndexDataColumnsAlterData::PathLocalId>();
                    TPathId pathId(ownerId, localPathId);

                    ui32 dataColId = rowset.GetValue<Schema::TableIndexDataColumnsAlterData::DataColumnId>();
                    TString dataColName = rowset.GetValue<Schema::TableIndexDataColumnsAlterData::DataColumnName>();

                    if (Self->PathsById.contains(pathId)) {
                        TPathElement::TPtr path = Self->PathsById.at(pathId);
                        Y_VERIFY_S(path->IsTableIndex(), "Path is not a table index, pathId: " << pathId);

                        if (Self->Indexes.contains(pathId)) {
                            auto tableIndex = Self->Indexes.at(pathId);

                            Y_VERIFY(tableIndex->AlterData != nullptr);
                            auto alterData = tableIndex->AlterData;
                            Y_VERIFY(tableIndex->AlterVersion < alterData->AlterVersion);

                            Y_VERIFY(dataColId == alterData->IndexDataColumns.size());
                            alterData->IndexDataColumns.emplace_back(dataColName);
                        } else {
                           leakedDataColumnsAlterData.emplace_back(pathId, dataColId);
                        }
                    } else {
                       leakedDataColumnsAlterData.emplace_back(pathId, dataColId);
                    }

                    if (!rowset.Next())
                        return false;
                }
                for (const auto& pair : leakedDataColumnsAlterData) {
                    db.Table<Schema::TableIndexDataColumnsAlterData>().Key(pair.first.OwnerId, pair.first.LocalPathId, pair.second).Delete();
                }
            }
        }

        // Read CdcStream
        {
            auto rowset = db.Table<Schema::CdcStream>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                auto pathId = TPathId(
                    TOwnerId(rowset.GetValue<Schema::CdcStream::OwnerPathId>()),
                    TLocalPathId(rowset.GetValue<Schema::CdcStream::LocalPathId>())
                );
                auto alterVersion = rowset.GetValue<Schema::CdcStream::AlterVersion>();
                auto mode = rowset.GetValue<Schema::CdcStream::Mode>();
                auto format = rowset.GetValue<Schema::CdcStream::Format>();
                auto vt = rowset.GetValueOrDefault<Schema::CdcStream::VirtualTimestamps>(false);
                auto state = rowset.GetValue<Schema::CdcStream::State>();

                Y_VERIFY_S(Self->PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);
                auto path = Self->PathsById.at(pathId);

                Y_VERIFY_S(path->IsCdcStream(), "Path is not a cdc stream"
                    << ", pathId: " << pathId
                    << ", path type: " << NKikimrSchemeOp::EPathType_Name(path->PathType));

                Y_VERIFY(!Self->CdcStreams.contains(pathId));
                Self->CdcStreams[pathId] = new TCdcStreamInfo(alterVersion, mode, format, vt, state);
                Self->IncrementPathDbRefCount(pathId);

                if (state == NKikimrSchemeOp::ECdcStreamStateScan) {
                    Y_VERIFY_S(Self->PathsById.contains(path->ParentPathId), "Parent path is not found"
                        << ", cdc stream pathId: " << pathId
                        << ", parent pathId: " << path->ParentPathId);
                    CdcStreamScansToResume[path->ParentPathId].push_back(pathId);
                }

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // Read CdcStreamAlterData
        {
            auto rowset = db.Table<Schema::CdcStreamAlterData>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                auto pathId = TPathId(
                    TOwnerId(rowset.GetValue<Schema::CdcStreamAlterData::OwnerPathId>()),
                    TLocalPathId(rowset.GetValue<Schema::CdcStreamAlterData::LocalPathId>())
                );

                auto alterVersion = rowset.GetValue<Schema::CdcStreamAlterData::AlterVersion>();
                auto mode = rowset.GetValue<Schema::CdcStreamAlterData::Mode>();
                auto format = rowset.GetValue<Schema::CdcStreamAlterData::Format>();
                auto vt = rowset.GetValueOrDefault<Schema::CdcStreamAlterData::VirtualTimestamps>(false);
                auto state = rowset.GetValue<Schema::CdcStreamAlterData::State>();

                Y_VERIFY_S(Self->PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);
                auto path = Self->PathsById.at(pathId);

                Y_VERIFY_S(path->IsCdcStream(), "Path is not a cdc stream"
                    << ", pathId: " << pathId
                    << ", path type: " << NKikimrSchemeOp::EPathType_Name(path->PathType));

                if (!Self->CdcStreams.contains(pathId)) {
                    Y_VERIFY(alterVersion == 1);
                    Self->CdcStreams[pathId] = TCdcStreamInfo::New(mode, format, vt);
                    Self->IncrementPathDbRefCount(pathId);
                }

                auto stream = Self->CdcStreams.at(pathId);
                Y_VERIFY(stream->AlterData == nullptr);
                Y_VERIFY(stream->AlterVersion < alterVersion);
                stream->AlterData = new TCdcStreamInfo(alterVersion, mode, format, vt, state);

                Y_VERIFY_S(Self->PathsById.contains(path->ParentPathId), "Parent path is not found"
                    << ", cdc stream pathId: " << pathId
                    << ", parent pathId: " << path->ParentPathId);
                auto parent = Self->PathsById.at(path->ParentPathId);

                Y_VERIFY_S(parent->IsTable(), "Parent path is not a table"
                    << ", cdc stream pathId: " << pathId
                    << ", parent pathId: " << path->ParentPathId);
                Y_VERIFY_S(Self->Tables.contains(path->ParentPathId), "Parent path is not found in Tables map"
                    << ", cdc stream pathId: " << pathId
                    << ", parent pathId: " << path->ParentPathId);

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // Read CdcStreamScanShardStatus
        {
            auto rowset = db.Table<Schema::CdcStreamScanShardStatus>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                auto pathId = TPathId(
                    rowset.GetValue<Schema::CdcStreamScanShardStatus::OwnerPathId>(),
                    rowset.GetValue<Schema::CdcStreamScanShardStatus::LocalPathId>()
                );
                auto shardIdx = TShardIdx(
                    rowset.GetValue<Schema::CdcStreamScanShardStatus::OwnerShardIdx>(),
                    rowset.GetValue<Schema::CdcStreamScanShardStatus::LocalShardIdx>()
                );
                auto status = rowset.GetValue<Schema::CdcStreamScanShardStatus::Status>();

                Y_VERIFY_S(Self->CdcStreams.contains(pathId), "Cdc stream not found"
                    << ": pathId# " << pathId);

                auto stream = Self->CdcStreams.at(pathId);
                stream->ScanShards.emplace(shardIdx, status);

                if (status != NKikimrTxDataShard::TEvCdcStreamScanResponse::DONE) {
                    stream->PendingShards.insert(shardIdx);
                } else {
                    stream->DoneShards.insert(shardIdx);
                }
            }
        }

        // Read DomainsPools
        {
            auto rowset = db.Table<Schema::StoragePools>().Range().Select();
            if (!rowset.IsReady())
                return false;
            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::StoragePools::PathId>();
                TPathId pathId(selfId, localPathId);

                TString name = rowset.GetValue<Schema::StoragePools::PoolName>();
                TString kind = rowset.GetValue<Schema::StoragePools::PoolKind>();

                Y_VERIFY(Self->SubDomains.contains(pathId));
                Self->SubDomains[pathId]->AddStoragePool(TStoragePool(name, kind));

                if (!rowset.Next())
                    return false;
            }

            // Read DomainsPoolsAlterData
            {
                auto rowset = db.Table<Schema::StoragePoolsAlterData>().Range().Select();
                if (!rowset.IsReady())
                    return false;

                while (!rowset.EndOfSet()) {
                    TLocalPathId localPathId = rowset.GetValue<Schema::StoragePoolsAlterData::PathId>();
                    TPathId pathId(selfId, localPathId);

                    TString name = rowset.GetValue<Schema::StoragePoolsAlterData::PoolName>();
                    TString kind = rowset.GetValue<Schema::StoragePoolsAlterData::PoolKind>();

                    Y_VERIFY(Self->SubDomains.contains(pathId));
                    auto subdomainInfo = Self->SubDomains[pathId];
                    Y_VERIFY(subdomainInfo->GetAlter());
                    subdomainInfo->GetAlter()->AddStoragePool(TStoragePool(name, kind));

                    if (!rowset.Next())
                        return false;
                }
            }
        }

        // Initialize SubDomains
        {
            for(auto item: Self->SubDomains) {
                auto pathId = item.first;
                Y_VERIFY_S(Self->PathsById.contains(pathId), "Unknown pathId: " << pathId);
                auto pathItem = Self->PathsById.at(pathId);
                if (pathItem->Dropped()) {
                    continue;
                }
                if (pathItem->IsRoot() && Self->IsDomainSchemeShard) {
                    continue;
                }
                auto subDomainInfo = item.second;
                subDomainInfo->Initialize(Self->ShardInfos);
                if (subDomainInfo->GetAlter()) {
                    subDomainInfo->GetAlter()->Initialize(Self->ShardInfos);
                }
            }

            if (!Self->IsDomainSchemeShard && Self->SubDomains.contains(Self->RootPathId())) {
                Self->SubDomains.at(Self->RootPathId())->Initialize(Self->ShardInfos);
            }
        }

        // Read BlockStoreVolumes
        {
            auto rowset = db.Table<Schema::BlockStoreVolumes>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::BlockStoreVolumes::PathId>();
                TPathId pathId(selfId, localPathId);

                TBlockStoreVolumeInfo::TPtr volume = new TBlockStoreVolumeInfo();
                {
                    auto cfg = rowset.GetValue<Schema::BlockStoreVolumes::VolumeConfig>();
                    bool parseOk = ParseFromStringNoSizeLimit(volume->VolumeConfig, cfg);
                    Y_VERIFY(parseOk);
                }
                volume->AlterVersion = rowset.GetValue<Schema::BlockStoreVolumes::AlterVersion>();
                volume->MountToken = rowset.GetValue<Schema::BlockStoreVolumes::MountToken>();
                volume->TokenVersion = rowset.GetValue<Schema::BlockStoreVolumes::TokenVersion>();
                Self->BlockStoreVolumes[pathId] = volume;
                Self->IncrementPathDbRefCount(pathId);

                auto it = nbsVolumeShards.find(pathId);
                if (it != nbsVolumeShards.end()) {
                    auto shardIdx = it->second;
                    const auto& shard = Self->ShardInfos[shardIdx];
                    volume->VolumeTabletId = shard.TabletID;
                    volume->VolumeShardIdx = shardIdx;
                }

                if (!rowset.Next())
                    return false;
            }
        }

        // Read BlockStorePartitions
        {
            auto rowset = db.Table<Schema::BlockStorePartitions>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::BlockStorePartitions::PathId>();
                TPathId pathId(selfId, localPathId);

                TLocalShardIdx localShardIdx = rowset.GetValue<Schema::BlockStorePartitions::ShardIdx>();
                TShardIdx shardIdx = Self->MakeLocalId(localShardIdx);

                auto it = Self->BlockStoreVolumes.find(pathId);
                Y_VERIFY(it != Self->BlockStoreVolumes.end());
                TBlockStoreVolumeInfo::TPtr volume = it->second;
                Y_VERIFY(volume);
                TBlockStorePartitionInfo::TPtr& part = volume->Shards[shardIdx];
                Y_VERIFY(!part);
                part.Reset(new TBlockStorePartitionInfo());
                part->PartitionId = rowset.GetValue<Schema::BlockStorePartitions::PartitionId>();
                part->AlterVersion = rowset.GetValue<Schema::BlockStorePartitions::AlterVersion>();
                if (part->AlterVersion <= volume->AlterVersion)
                    ++volume->DefaultPartitionCount; // visible partition

                if (!rowset.Next())
                    return false;
            }
        }

        // Read BlockStoreVolumeAlters
        {
            auto rowset = db.Table<Schema::BlockStoreVolumeAlters>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::BlockStoreVolumeAlters::PathId>();
                TPathId pathId(selfId, localPathId);

                TBlockStoreVolumeInfo::TPtr alterData = new TBlockStoreVolumeInfo();
                {
                    auto cfg = rowset.GetValue<Schema::BlockStoreVolumeAlters::VolumeConfig>();
                    bool parseOk = ParseFromStringNoSizeLimit(alterData->VolumeConfig, cfg);
                    Y_VERIFY(parseOk);
                }
                alterData->AlterVersion = rowset.GetValue<Schema::BlockStoreVolumeAlters::AlterVersion>();
                alterData->DefaultPartitionCount = rowset.GetValue<Schema::BlockStoreVolumeAlters::PartitionCount>();

                auto it = Self->BlockStoreVolumes.find(pathId);
                Y_VERIFY(it != Self->BlockStoreVolumes.end());
                TBlockStoreVolumeInfo::TPtr volume = it->second;
                Y_VERIFY(volume);
                alterData->VolumeTabletId = volume->VolumeTabletId;
                alterData->VolumeShardIdx = volume->VolumeShardIdx;

                Y_VERIFY(!volume->AlterData);
                volume->AlterData = std::move(alterData);

                if (!rowset.Next())
                    return false;
            }
        }

        // Read FileStoreInfos
        {
            auto rowset = db.Table<Schema::FileStoreInfos>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                TLocalPathId localPathId = rowset.GetValue<Schema::FileStoreInfos::PathId>();
                TPathId pathId(selfId, localPathId);

                TFileStoreInfo::TPtr fs = new TFileStoreInfo();
                {
                    auto cfg = rowset.GetValue<Schema::FileStoreInfos::Config>();
                    bool parseOk = ParseFromStringNoSizeLimit(fs->Config, cfg);
                    Y_VERIFY(parseOk);
                    fs->Version = rowset.GetValueOrDefault<Schema::FileStoreInfos::Version>();
                }
                Self->FileStoreInfos[pathId] = fs;
                Self->IncrementPathDbRefCount(pathId);

                auto it = fileStoreShards.find(pathId);
                if (it != fileStoreShards.end()) {
                    TShardIdx shardIdx = it->second;
                    const auto& shard = Self->ShardInfos[shardIdx];
                    fs->IndexShardIdx = shardIdx;
                    fs->IndexTabletId = shard.TabletID;
                }

                if (!rowset.Next())
                    return false;
            }

            // Read FileStoreAlters
            {
                auto rowset = db.Table<Schema::FileStoreAlters>().Range().Select();
                if (!rowset.IsReady())
                    return false;

                while (!rowset.EndOfSet()) {
                    TLocalPathId localPathId = rowset.GetValue<Schema::FileStoreAlters::PathId>();
                    TPathId pathId(selfId, localPathId);

                    auto it = Self->FileStoreInfos.find(pathId);
                    Y_VERIFY(it != Self->FileStoreInfos.end());

                    TFileStoreInfo::TPtr fs = it->second;
                    Y_VERIFY(fs);

                    {
                        fs->AlterConfig = MakeHolder<NKikimrFileStore::TConfig>();
                        auto cfg = rowset.GetValue<Schema::FileStoreAlters::Config>();
                        bool parseOk = ParseFromStringNoSizeLimit(*fs->AlterConfig, cfg);
                        Y_VERIFY(parseOk);
                        fs->AlterVersion = rowset.GetValue<Schema::FileStoreAlters::Version>();
                    }

                    if (!rowset.Next())
                        return false;
                }
            }
        }

        // Read KesusInfos
        {
            TKesusInfosRows kesusRows;
            if (!LoadKesusInfos(db, kesusRows)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for KesusInfos"
                             << ", read records: " << kesusRows.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (const auto& rec: kesusRows) {
                const TPathId& pathId = std::get<0>(rec);
                const TString& config = std::get<1>(rec);
                const ui64& version = std::get<2>(rec);

                TKesusInfo::TPtr kesus = new TKesusInfo();
                {
                    bool parseOk = ParseFromStringNoSizeLimit(kesus->Config, config);
                    Y_VERIFY(parseOk);
                    kesus->Version = version;
                }
                Self->KesusInfos[pathId] = kesus;
                Self->IncrementPathDbRefCount(pathId);

                auto it = kesusShards.find(pathId);
                if (it != kesusShards.end()) {
                    const auto& shardIdx = it->second;
                    const auto& shard = Self->ShardInfos[shardIdx];
                    kesus->KesusShardIdx = shardIdx;
                    kesus->KesusTabletId = shard.TabletID;
                }
            }
        }

           // Read KesusAlters
        {
            TKesusAlterRows kesusAlterRows;
            if (!LoadKesusAlters(db, kesusAlterRows)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for KesusAlters"
                             << ", read records: " << kesusAlterRows.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (const auto& rec: kesusAlterRows) {
                const TPathId& pathId = std::get<0>(rec);
                const TString& config = std::get<1>(rec);
                const ui64& version = std::get<2>(rec);

                auto it = Self->KesusInfos.find(pathId);
                Y_VERIFY(it != Self->KesusInfos.end());
                TKesusInfo::TPtr kesus = it->second;
                Y_VERIFY(kesus);
                {
                    kesus->AlterConfig.Reset(new Ydb::Coordination::Config);
                    bool parseOk = ParseFromStringNoSizeLimit(*kesus->AlterConfig, config);
                    Y_VERIFY(parseOk);
                    kesus->AlterVersion = version;
                }
            }
        }

        TVector<TOperationId> splitOpIds;
        TVector<TOperationId> forceDropOpIds;
        THashSet<TPathId> pathsUnderOperation;
        // Read in-flight txid
        {
            auto txInFlightRowset = db.Table<Schema::TxInFlightV2>().Range().Select();
            if (!txInFlightRowset.IsReady())
                return false;
            while (!txInFlightRowset.EndOfSet()) {
                auto operationId = TOperationId(txInFlightRowset.GetValue<Schema::TxInFlightV2::TxId>(),
                                                txInFlightRowset.GetValue<Schema::TxInFlightV2::TxPartId>());

                TTxState& txState = Self->TxInFlight[operationId];

                txState.TxType =        (TTxState::ETxType)txInFlightRowset.GetValue<Schema::TxInFlightV2::TxType>();

                txState.State =         (TTxState::ETxState)txInFlightRowset.GetValue<Schema::TxInFlightV2::State>();

                TLocalPathId ownerTarget =  txInFlightRowset.GetValue<Schema::TxInFlightV2::TargetOwnerPathId>();
                TLocalPathId localTarget =  txInFlightRowset.GetValue<Schema::TxInFlightV2::TargetPathId>();
                txState.TargetPathId = ownerTarget == InvalidOwnerId
                    ? TPathId(selfId, localTarget)
                    : TPathId(ownerTarget, localTarget);

                txState.MinStep =       txInFlightRowset.GetValueOrDefault<Schema::TxInFlightV2::MinStep>(InvalidStepId);
                txState.PlanStep =      txInFlightRowset.GetValueOrDefault<Schema::TxInFlightV2::PlanStep>(InvalidStepId);

                TString extraData =     txInFlightRowset.GetValueOrDefault<Schema::TxInFlightV2::ExtraBytes>("");
                txState.StartTime =     TInstant::MicroSeconds(txInFlightRowset.GetValueOrDefault<Schema::TxInFlightV2::StartTime>());
                txState.DataTotalSize = txInFlightRowset.GetValueOrDefault<Schema::TxInFlightV2::DataTotalSize>(0);
                txState.Cancel = txInFlightRowset.GetValueOrDefault<Schema::TxInFlightV2::CancelBackup>(false);
                txState.BuildIndexId =  txInFlightRowset.GetValueOrDefault<Schema::TxInFlightV2::BuildIndexId>();

                txState.SourcePathId =  TPathId(txInFlightRowset.GetValueOrDefault<Schema::TxInFlightV2::SourceOwnerId>(),
                                                txInFlightRowset.GetValueOrDefault<Schema::TxInFlightV2::SourceLocalPathId>());

                if (txState.TxType == TTxState::TxCopyTable && txState.SourcePathId) {
                    Y_VERIFY(txState.SourcePathId);
                    TPathElement::TPtr srcPath = Self->PathsById.at(txState.SourcePathId);
                    Y_VERIFY_S(srcPath, "Null path element, pathId: " << txState.SourcePathId);

                    // CopyTable source must not be altered or dropped while the Tx is in progress
                    srcPath->PathState = TPathElement::EPathState::EPathStateCopying;
                    srcPath->DbRefCount++;
                }

                if (txState.TxType == TTxState::TxMoveTable || txState.TxType == TTxState::TxMoveTableIndex) {
                    Y_VERIFY(txState.SourcePathId);
                    TPathElement::TPtr srcPath = Self->PathsById.at(txState.SourcePathId);
                    Y_VERIFY_S(srcPath, "Null path element, pathId: " << txState.SourcePathId);

                    // Moving source must not be altered or dropped while the Tx is in progress
                    if (!srcPath->Dropped()) {
                        srcPath->PathState = TPathElement::EPathState::EPathStateMoving;
                    }
                    srcPath->DbRefCount++;
                }

                if (txState.TxType == TTxState::TxCreateSubDomain) {
                    Y_VERIFY(Self->SubDomains.contains(txState.TargetPathId));
                    auto subDomainInfo = Self->SubDomains.at(txState.TargetPathId);
                    if (txState.State <= TTxState::Propose) {
                        Y_VERIFY(subDomainInfo->GetAlter());
                    }
                } else if (txState.TxType == TTxState::TxSplitTablePartition || txState.TxType == TTxState::TxMergeTablePartition) {
                    Y_VERIFY(!extraData.empty(), "Split Tx must have non-empty split description");
                    txState.SplitDescription = std::make_shared<NKikimrTxDataShard::TSplitMergeDescription>();
                    bool deserializeRes = ParseFromStringNoSizeLimit(*txState.SplitDescription, extraData);
                    Y_VERIFY(deserializeRes);
                    splitOpIds.push_back(operationId);
                } else if (txState.TxType == TTxState::TxFinalizeBuildIndex) {
                    if (!extraData.empty()) {
                        txState.BuildIndexOutcome = std::make_shared<NKikimrSchemeOp::TBuildIndexOutcome>();
                        bool deserializeRes = ParseFromStringNoSizeLimit(*txState.BuildIndexOutcome, extraData);
                        Y_VERIFY(deserializeRes);
                    }
                } else if (txState.TxType == TTxState::TxAlterTable) {
                    if (txState.State <= TTxState::Propose) {
                        // If state is >=Propose then alter has already been applied to the table
                        // and AlterData should be cleared

                        TPathId tablePathId = txState.TargetPathId;

                        Y_VERIFY_S(Self->PathsById.contains(tablePathId), "Path doesn't exist, pathId: " << tablePathId);
                        Y_VERIFY_S(Self->PathsById.at(tablePathId)->IsTable(), "Path is not a table, pathId: " << tablePathId);
                        Y_VERIFY_S(Self->Tables.FindPtr(tablePathId), "Table doesn't exist, pathId: " << tablePathId);

                        // legacy, ???
                        Y_VERIFY(Self->Tables.contains(tablePathId));
                        TTableInfo::TPtr tableInfo = Self->Tables.at(tablePathId);
                        tableInfo->InitAlterData();
                        tableInfo->DeserializeAlterExtraData(extraData);
                    }
                } else if (txState.TxType == TTxState::TxBackup || txState.TxType == TTxState::TxRestore) {
                    auto byShardBackupStatus = db.Table<Schema::ShardBackupStatus>().Range(operationId.GetTxId()).Select();
                    auto byMigratedShardBackupStatus = db.Table<Schema::MigratedShardBackupStatus>().Range(operationId.GetTxId()).Select();
                    auto byTxShardStatus = db.Table<Schema::TxShardStatus>().Range(operationId.GetTxId()).Select();

                    TShardBackupStatusRows statuses;
                    if (!LoadBackupStatusesImpl(statuses, byShardBackupStatus, byMigratedShardBackupStatus, byTxShardStatus)) {
                        return false;
                    }

                    for (auto& rec : statuses) {
                        auto shardIdx = std::get<1>(rec);
                        auto success = std::get<2>(rec);
                        auto error = std::get<3>(rec);
                        auto bytes = std::get<4>(rec);
                        auto rows = std::get<5>(rec);

                        txState.ShardStatuses[shardIdx] = TTxState::TShardStatus(success, error, bytes, rows);
                    }
                } else if (txState.TxType == TTxState::TxForceDropSubDomain || txState.TxType == TTxState::TxForceDropExtSubDomain) {
                    forceDropOpIds.push_back(operationId);
                } else if (txState.TxType == TTxState::TxAlterUserAttributes) {
                    Y_VERIFY_S(Self->PathsById.contains(txState.TargetPathId), "Unknown pathId: " << txState.TargetPathId);
                    TPathElement::TPtr path = Self->PathsById.at(txState.TargetPathId);
                    if (!path->UserAttrs->AlterData) {
                        path->UserAttrs->AlterData = new TUserAttributes(path->UserAttrs->AlterVersion + 1);
                    }
                }


                Y_VERIFY(txState.TxType != TTxState::TxInvalid);
                Y_VERIFY(txState.State != TTxState::Invalid);

                // Change path state (cause there's a transaction on it)
                // It's possible to restore Create or Alter TX on dropped PathId. Preserve 'NotExists' state.
                Y_VERIFY_S(Self->PathsById.contains(txState.TargetPathId), "No path element"
                               << ", txId: " << operationId.GetTxId()
                               << ", pathId: " << txState.TargetPathId);
                TPathElement::TPtr path = Self->PathsById.at(txState.TargetPathId);
                Y_VERIFY_S(path, "No path element for Tx: " <<  operationId.GetTxId() << ", pathId: " << txState.TargetPathId);
                path->PathState = CalcPathState(txState.TxType, path->PathState);

                if (path->PathState == TPathElement::EPathState::EPathStateDrop) {
                    path->DropTxId = operationId.GetTxId();
                }

                if (txState.TxType != TTxState::TxSplitTablePartition && txState.TxType != TTxState::TxMergeTablePartition) {
                    path->LastTxId = operationId.GetTxId();
                }
                path->DbRefCount++;

                // Remember which paths are still under operation
                pathsUnderOperation.insert(txState.TargetPathId);

                if (CdcStreamScansToResume.contains(txState.TargetPathId)) {
                    CdcStreamScansToResume.erase(txState.TargetPathId);
                }

                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Adjusted PathState"
                                << ", pathId: " << txState.TargetPathId
                                << ", name: " << path->Name.data()
                                << ", state: " <<  NKikimrSchemeOp::EPathState_Name(path->PathState)
                                << ", txId: " << operationId.GetTxId()
                                << ", TxType: " << TTxState::TypeName(txState.TxType)
                                << ", LastTxId: " << path->LastTxId);

                if (!Self->Operations.contains(operationId.GetTxId())) {
                    Self->Operations[operationId.GetTxId()] = new TOperation(operationId.GetTxId());
                }

                TOperation::TPtr operation = Self->Operations.at(operationId.GetTxId());
                Y_VERIFY(operationId.GetSubTxId() == operation->Parts.size());
                ISubOperation::TPtr part = operation->RestorePart(txState.TxType, txState.State);
                operation->AddPart(part);

                if (!txInFlightRowset.Next())
                    return false;
            }
        }

        // Read tx's shards
        {
            TTxShardsRows txShardsRows;
            if (!LoadTxShards(db, txShardsRows)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for TxShards"
                             << ", read records: " << txShardsRows.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (auto& rec: txShardsRows) {
                TOperationId operationId = std::get<0>(rec);
                TShardIdx shardIdx = std::get<1>(rec);;
                TTxState::ETxState operation = std::get<2>(rec);

                TTxState* txState = Self->FindTx(operationId);
                Y_VERIFY_S(txState, "There's shard for unknown Operation"
                               << ", shardIdx: " << shardIdx
                               << ", txId: " << operationId.GetTxId());

                // shard no present in ShardInfos if it is deleted, type unknown
                auto type = Self->ShardInfos.contains(shardIdx) ? Self->ShardInfos.at(shardIdx).TabletType : ETabletType::TypeInvalid;
                txState->Shards.emplace_back(TTxState::TShardOperation(shardIdx, type, operation));

                if (!Self->ShardInfos.contains(shardIdx)) {
                    if (txState->CanDeleteParts()
                        || ((txState->TxType == TTxState::TxAlterTable || txState->TxType == TTxState::TxCopyTable) //KIKIMR-7723
                            && (txState->State == TTxState::Waiting || txState->State == TTxState::CreateParts)))
                    {
                        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                                   "Already deleted shard in operation"
                                       << ", shardIdx: " << shardIdx
                                       << ", txId: " << operationId.GetTxId()
                                       << ", TxType: " << TTxState::TypeName(txState->TxType)
                                       << ", TxState: " << TTxState::StateName(txState->State)
                                   );
                    } else {
                        Y_VERIFY_S(Self->ShardInfos.contains(shardIdx), "Unknown shard"
                                       << ", shardIdx: " << shardIdx
                                       << ", txId: " << operationId.GetTxId()
                                       << ", TxType: " << TTxState::TypeName(txState->TxType)
                                       << ", TxState: " << TTxState::StateName(txState->State));
                    }
                } else { // remove that branch since we sure in persisting SourcePathId
                    // Figure out source path id for the Tx (for CopyTable)
                    if (Self->ShardInfos.at(shardIdx).PathId != txState->TargetPathId &&
                        !txState->SourcePathId &&
                        txState->TxType != TTxState::TxForceDropSubDomain &&
                        txState->TxType != TTxState::TxForceDropExtSubDomain &&
                        txState->TxType != TTxState::TxCreateColumnTable &&
                        txState->TxType != TTxState::TxAlterColumnTable &&
                        txState->TxType != TTxState::TxDropColumnTable &&
                        txState->TxType != TTxState::TxCreateSequence &&
                        txState->TxType != TTxState::TxAlterSequence &&
                        txState->TxType != TTxState::TxDropSequence &&
                        txState->TxType != TTxState::TxCreateReplication &&
                        txState->TxType != TTxState::TxAlterReplication &&
                        txState->TxType != TTxState::TxDropReplication)
                    {
                        Y_VERIFY_S(txState->TxType == TTxState::TxCopyTable, "Only CopyTable Tx can have participating shards from a different table"
                                       << ", txId: " << operationId.GetTxId()
                                       << ", targetPathId: " << txState->TargetPathId
                                       << ", shardIdx: " << shardIdx
                                       << ", shardPathId: " << Self->ShardInfos.at(shardIdx).PathId);

                        txState->SourcePathId = Self->ShardInfos.at(shardIdx).PathId;
                        Y_VERIFY(txState->SourcePathId != InvalidPathId);
                        Y_VERIFY_S(Self->PathsById.contains(txState->SourcePathId), "No source path element for Operation"
                                     << ", txId: " << operationId.GetTxId()
                                     << ", pathId: " << txState->SourcePathId);

                        TPathElement::TPtr srcPath = Self->PathsById.at(txState->SourcePathId);
                        Y_VERIFY_S(srcPath, "Null path element, pathId: " << txState->SourcePathId);

                        // CopyTable source must not be altered or dropped while the Tx is in progress
                        srcPath->PathState = TPathElement::EPathState::EPathStateCopying;
                        srcPath->DbRefCount++;
                    }
                }
            }
        }

        // After all shard operations are loaded we can fill range ends for shards participating in split operations
        // and register shards as busy
        for (TOperationId opId : splitOpIds) {
            THashMap<TShardIdx, TString> shardIdxToRangeEnd;
            TTxState* txState = Self->FindTx(opId);
            Y_VERIFY_S(txState, "No txState for split/merge opId, txId: " << opId.GetTxId());
            Y_VERIFY(txState->SplitDescription);

            Y_VERIFY(Self->Tables.contains(txState->TargetPathId));
            TTableInfo::TPtr tableInfo = Self->Tables.at(txState->TargetPathId);
            tableInfo->RegisterSplitMergeOp(opId, *txState);

            for (ui32 i = 0; i < txState->SplitDescription->DestinationRangesSize(); ++i) {
                const auto& dst = txState->SplitDescription->GetDestinationRanges(i);
                auto localShardIdx = TLocalShardIdx(dst.GetShardIdx());
                auto shardIdx = Self->MakeLocalId(localShardIdx);
                shardIdxToRangeEnd[shardIdx] = dst.GetKeyRangeEnd();
            }
            for (TTxState::TShardOperation& shardOp : txState->Shards) {
                if (shardOp.Operation == TTxState::CreateParts) {
                    Y_VERIFY(shardIdxToRangeEnd.contains(shardOp.Idx));
                    shardOp.RangeEnd = shardIdxToRangeEnd.at(shardOp.Idx);
                }
            }
        }

        //after all txs and splitTxs was loaded and processed, it is valid to initiate force drops
        for (TOperationId opId: forceDropOpIds) {
            TTxState* txState = Self->FindTx(opId);
            Y_VERIFY(txState);
            auto paths = Self->ListSubTree(txState->TargetPathId, ctx);
            Self->MarkAsDropping(paths, opId.GetTxId(), ctx);
        }

        // Read txid dependencies
        {
            auto txDependenciesRowset = db.Table<Schema::TxDependencies>().Range().Select();
            if (!txDependenciesRowset.IsReady())
                return false;

            while (!txDependenciesRowset.EndOfSet()) {
                auto txId = txDependenciesRowset.GetValue<Schema::TxDependencies::TxId>();
                auto dependentTxId = txDependenciesRowset.GetValue<Schema::TxDependencies::DependentTxId>();

                Y_VERIFY_S(Self->Operations.contains(txId), "Parent operation is not found"
                                                            << ", parent txId " << txId
                                                            << ", dependentTxId " << dependentTxId);

                Y_VERIFY_S(Self->Operations.contains(dependentTxId), "Dependent operation is not found"
                                                                     << ", dependent txId:" << dependentTxId
                                                                     << ", parent txId " << txId);

                Self->Operations.at(txId)->DependentOperations.insert(dependentTxId);
                Self->Operations.at(dependentTxId)->WaitOperations.insert(txId);

                if (!txDependenciesRowset.Next())
                    return false;
            }
        }

        // Read shards to delete
        {
            TShardsToDeleteRows shardsToDelete;
            if (!LoadShardsToDelete(db, shardsToDelete)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for ShardToDelete"
                             << ", read records: " << shardsToDelete.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (auto& rec: shardsToDelete) {
                OnComplete.DeleteShard(std::get<0>(rec));
            }
        }

        // Read backup settings
        {
            TBackupSettingsRows backupSettings;
            if (!LoadBackupSettings(db, backupSettings)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for BackupSettings"
                             << ", read records: " << backupSettings.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (auto& rec: backupSettings) {
                TPathId pathId = std::get<0>(rec);
                TString tableName = std::get<1>(rec);
                TString ytSerializedSettings = std::get<2>(rec);
                TString s3SerializedSettings = std::get<3>(rec);
                TString scanSettings = std::get<4>(rec);
                bool needToBill = std::get<5>(rec);
                TString tableDesc = std::get<6>(rec);
                ui32 nRetries = std::get<7>(rec);

                Y_VERIFY(tableName.size() > 0);

                TTableInfo::TPtr tableInfo = Self->Tables.at(pathId);
                Y_VERIFY(tableInfo.Get() != nullptr);

                tableInfo->BackupSettings.SetTableName(tableName);
                tableInfo->BackupSettings.SetNeedToBill(needToBill);
                tableInfo->BackupSettings.SetNumberOfRetries(nRetries);

                if (ytSerializedSettings) {
                    auto settings = tableInfo->BackupSettings.MutableYTSettings();
                    Y_VERIFY(ParseFromStringNoSizeLimit(*settings, ytSerializedSettings));
                } else if (s3SerializedSettings) {
                    auto settings = tableInfo->BackupSettings.MutableS3Settings();
                    Y_VERIFY(ParseFromStringNoSizeLimit(*settings, s3SerializedSettings));
                } else {
                    Y_FAIL("Unknown settings");
                }

                if (scanSettings) {
                    auto settings = tableInfo->BackupSettings.MutableScanSettings();
                    Y_VERIFY(ParseFromStringNoSizeLimit(*settings, scanSettings));
                }

                if (tableDesc) {
                    auto desc = tableInfo->BackupSettings.MutableTable();
                    Y_VERIFY(ParseFromStringNoSizeLimit(*desc, tableDesc));
                }

                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Loaded backup settings"
                                << ", pathId: " << pathId
                                << ", tablename: " << tableName.data());
            }
        }

        // Read restore tasks
        {
            auto rowSet = db.Table<Schema::RestoreTasks>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }

            while (!rowSet.EndOfSet()) {
                auto pathId = TPathId(
                    rowSet.GetValue<Schema::RestoreTasks::OwnerPathId>(),
                    rowSet.GetValue<Schema::RestoreTasks::LocalPathId>());
                auto task = rowSet.GetValue<Schema::RestoreTasks::Task>();

                TTableInfo::TPtr tableInfo = Self->Tables.at(pathId);
                Y_VERIFY(tableInfo.Get() != nullptr);
                Y_VERIFY(ParseFromStringNoSizeLimit(tableInfo->RestoreSettings, task));

                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Loaded restore task"
                                << ", pathId: " << pathId);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        // Read security state
        NLoginProto::TSecurityState securityState;
        {
            auto rowset = db.Table<Schema::LoginKeys>().Select();

            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                auto& key = *securityState.AddPublicKeys();
                key.SetKeyId(rowset.GetValue<Schema::LoginKeys::KeyId>());
                key.SetKeyDataPEM(rowset.GetValue<Schema::LoginKeys::KeyDataPEM>());
                key.SetExpiresAt(rowset.GetValueOrDefault<Schema::LoginKeys::ExpiresAt>());
                if (!rowset.Next()) {
                    return false;
                }
            }
        }
        std::unordered_map<TString, int> sidIndex;
        {
            auto rowset = db.Table<Schema::LoginSids>().Select();

            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                auto& sid = *securityState.AddSids();
                sid.SetName(rowset.GetValue<Schema::LoginSids::SidName>());
                sid.SetType(rowset.GetValue<Schema::LoginSids::SidType>());
                sid.SetHash(rowset.GetValue<Schema::LoginSids::SidHash>());
                sidIndex[sid.name()] = securityState.SidsSize() - 1;
                if (!rowset.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowset = db.Table<Schema::LoginSidMembers>().Select();

            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                TString sidName = rowset.GetValue<Schema::LoginSidMembers::SidName>();
                auto itSidIndex = sidIndex.find(sidName);
                if (itSidIndex != sidIndex.end()) {
                    NLoginProto::TSid& sid = (*securityState.MutableSids())[itSidIndex->second];
                    sid.AddMembers(rowset.GetValue<Schema::LoginSidMembers::SidMember>());
                }
                if (!rowset.Next()) {
                    return false;
                }
            }
        }
        Self->LoginProvider.UpdateSecurityState(std::move(securityState));

        {
            TShardBackupStatusRows backupStatuses;
            if (!LoadBackupStatuses(db, backupStatuses)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for ShardBackupStatus"
                             << ", read records: " << backupStatuses.size()
                             << ", at schemeshard: " << Self->TabletID());

            THashMap<TTxId, TShardBackupStatusRows> statusesByTxId;
            for (auto& rec: backupStatuses) {
                TTxId txId = std::get<0>(rec);
                statusesByTxId[txId].push_back(rec);
            }

            TCompletedBackupRestoreRows history;
            if (!LoadBackupRestoreHistory(db, history)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for CompletedBackup"
                             << ", read records: " << history.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (auto& rec: history) {
                auto pathId = std::get<0>(rec);
                auto txId = std::get<1>(rec);
                auto completeTime = std::get<2>(rec);

                auto successShardsCount = std::get<3>(rec);
                auto totalShardCount = std::get<4>(rec);
                auto startTime = std::get<5>(rec);
                auto dataSize = std::get<6>(rec);
                auto kind = static_cast<TTableInfo::TBackupRestoreResult::EKind>(std::get<7>(rec));

                TTableInfo::TBackupRestoreResult info;
                info.CompletionDateTime = completeTime;
                info.TotalShardCount = totalShardCount;
                info.SuccessShardCount = successShardsCount;
                info.StartDateTime = startTime;
                info.DataTotalSize = dataSize;

                if (!Self->Tables.FindPtr(pathId)) {
                    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                                "Skip record in CompletedBackups"
                                    << ", pathId: " << pathId
                                    << ", txid: " << txId);
                    continue;
                }

                TTableInfo::TPtr tableInfo = Self->Tables.at(pathId);

                if (statusesByTxId.contains(txId)) {
                    for (auto& recByTxId: statusesByTxId.at(txId)) {
                        auto shardIdx = std::get<1>(recByTxId);
                        auto success = std::get<2>(recByTxId);
                        auto error = std::get<3>(recByTxId);
                        auto bytes = std::get<4>(recByTxId);
                        auto rows = std::get<5>(recByTxId);

                        info.ShardStatuses[shardIdx] = TTxState::TShardStatus(success, error, bytes, rows);
                    }
                }

                switch (kind) {
                case TTableInfo::TBackupRestoreResult::EKind::Backup:
                    tableInfo->BackupHistory[txId] = std::move(info);
                    break;
                case TTableInfo::TBackupRestoreResult::EKind::Restore:
                    tableInfo->RestoreHistory[txId] = std::move(info);
                    break;
                }

                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Loaded completed backup status"
                                << ", pathId: " << pathId
                                << ", txid: " << txId);
            }
        }

        // Other persistent params
        for (const auto& si : Self->ShardInfos) {
            auto shardIdx = si.first;
            auto tabletId = si.second.TabletID;
            auto pathId = si.second.PathId;
            Self->TabletIdToShardIdx[tabletId] = shardIdx;

            Y_VERIFY(Self->PathsById.contains(pathId));
            auto path = Self->PathsById.at(pathId); //path should't be dropped?
            path->IncShardsInside();

            auto domainInfo = Self->ResolveDomainInfo(pathId); //domain should't be dropped?
            domainInfo->AddInternalShard(shardIdx, Self->IsBackupTable(pathId));

            switch (si.second.TabletType) {
            case ETabletType::DataShard:
                {
                    const auto table = Self->Tables.FindPtr(pathId);
                    if (tabletId != InvalidTabletId) {
                        bool active = !path->Dropped() &&
                                      !path->PlannedToDrop() &&
                                      table && (*table)->GetStats().PartitionStats.contains(shardIdx);
                        Self->TabletCounters->Simple()[active ? COUNTER_TABLE_SHARD_ACTIVE_COUNT : COUNTER_TABLE_SHARD_INACTIVE_COUNT].Add(1);
                    }
                    break;
                }
            case ETabletType::PersQueue:
                Self->TabletCounters->Simple()[COUNTER_PQ_SHARD_COUNT].Add(1);
                break;
            case ETabletType::PersQueueReadBalancer:
                Self->TabletCounters->Simple()[COUNTER_PQ_RB_SHARD_COUNT].Add(1);
                break;
            case ETabletType::BlockStoreVolume:
                Self->TabletCounters->Simple()[COUNTER_BLOCKSTORE_VOLUME_SHARD_COUNT].Add(1);
                break;
            case ETabletType::BlockStorePartition:
                Self->TabletCounters->Simple()[COUNTER_BLOCKSTORE_PARTITION_SHARD_COUNT].Add(1);
                break;
            case ETabletType::BlockStorePartition2:
                Self->TabletCounters->Simple()[COUNTER_BLOCKSTORE_PARTITION2_SHARD_COUNT].Add(1);
                break;
            case ETabletType::FileStore:
                Self->TabletCounters->Simple()[COUNTER_FILESTORE_COUNT].Add(1);
                break;
            case ETabletType::Kesus:
                Self->TabletCounters->Simple()[COUNTER_KESUS_SHARD_COUNT].Add(1);
                break;
            case ETabletType::Coordinator:
                Self->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_COORDINATOR_COUNT].Add(1);
                break;
            case ETabletType::Mediator:
                Self->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_MEDIATOR_COUNT].Add(1);
                break;
            case ETabletType::RTMRPartition:
                Self->TabletCounters->Simple()[COUNTER_RTMR_PARTITIONS_COUNT].Add(1);
                break;
            case ETabletType::KeyValue:
                Self->TabletCounters->Simple()[COUNTER_SOLOMON_PARTITIONS_COUNT].Add(1);
                break;
            case ETabletType::SchemeShard:
                Self->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_SCHEME_SHARD_COUNT].Add(1);
                break;
            case ETabletType::Hive:
                Self->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_HIVE_COUNT].Add(1);
                break;
            case ETabletType::SysViewProcessor:
                Self->TabletCounters->Simple()[COUNTER_SYS_VIEW_PROCESSOR_COUNT].Add(1);
                break;
            case ETabletType::ColumnShard:
                Self->TabletCounters->Simple()[COUNTER_COLUMN_SHARDS].Add(1);
                break;
            case ETabletType::SequenceShard:
                Self->TabletCounters->Simple()[COUNTER_SEQUENCESHARD_COUNT].Add(1);
                domainInfo->AddSequenceShard(shardIdx);
                break;
            case ETabletType::ReplicationController:
                Self->TabletCounters->Simple()[COUNTER_REPLICATION_CONTROLLER_COUNT].Add(1);
                break;
            case ETabletType::BlobDepot:
                Self->TabletCounters->Simple()[COUNTER_BLOB_DEPOT_COUNT].Add(1);
                break;
            default:
                Y_FAIL_S("dont know how to interpret tablet type"
                         << ", type id: " << (ui32)si.second.TabletType
                         << ", pathId: " << pathId
                         << ", shardId: " << shardIdx
                         << ", tabletId: " << tabletId);
            }
        }

        for (const auto& item : Self->PathsById) {
            auto& path = item.second;

            if (path->Dropped()) {
                continue;
            }

            TPathElement::TPtr parent = Self->PathsById.at(path->ParentPathId);
            TPathElement::TPtr inclusiveDomainPath = Self->PathsById.at(Self->ResolvePathIdForDomain(parent)); // take upper domain id info even when the path is domain by itself
            TSubDomainInfo::TPtr inclusiveDomainInfo = Self->ResolveDomainInfo(parent);

            if (inclusiveDomainPath->IsExternalSubDomainRoot()) {
                path->PathState = TPathElement::EPathState::EPathStateMigrated;
                continue;
            }

            if (!path->IsRoot()) {
                const bool isBackupTable = Self->IsBackupTable(item.first);
                parent->IncAliveChildren(1, isBackupTable);
                inclusiveDomainInfo->IncPathsInside(1, isBackupTable);
            }

            Self->TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Add(path->UserAttrs->Size());

            if (path->IsPQGroup()) {
                auto pqGroup = Self->Topics.at(path->PathId);
                auto delta = pqGroup->AlterData ? pqGroup->AlterData->TotalPartitionCount : pqGroup->TotalPartitionCount;
                auto tabletConfig = pqGroup->AlterData ? (pqGroup->AlterData->TabletConfig.empty() ? pqGroup->TabletConfig : pqGroup->AlterData->TabletConfig)
                                                       : pqGroup->TabletConfig;
                NKikimrPQ::TPQTabletConfig config;
                Y_VERIFY(!tabletConfig.empty());
                bool parseOk = ParseFromStringNoSizeLimit(config, tabletConfig);
                Y_VERIFY(parseOk);

                const PQGroupReserve reserve(config, delta);

                inclusiveDomainInfo->IncPQPartitionsInside(delta);
                inclusiveDomainInfo->IncPQReservedStorage(reserve.Storage);

                Self->TabletCounters->Simple()[COUNTER_STREAM_SHARDS_COUNT].Add(delta);
                Self->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_THROUGHPUT].Add(reserve.Throughput);
                Self->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_STORAGE].Add(reserve.Storage);
            }

            if (path->PlannedToDrop()) {
                continue;
            }

            if (path->IsDirectory()) {
                Self->TabletCounters->Simple()[COUNTER_DIR_COUNT].Add(1);
            } else if (path->IsTable()) {
                Self->TabletCounters->Simple()[COUNTER_TABLE_COUNT].Add(1);
            } else if (path->IsPQGroup()) {
                Self->TabletCounters->Simple()[COUNTER_PQ_GROUP_COUNT].Add(1);
            } if (path->IsSubDomainRoot()) {
                Self->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_COUNT].Add(1);
            } if (path->IsExternalSubDomainRoot()) {
                Self->TabletCounters->Simple()[COUNTER_EXTSUB_DOMAIN_COUNT].Add(1);
            } else if (path->IsBlockStoreVolume()) {
                Self->TabletCounters->Simple()[COUNTER_BLOCKSTORE_VOLUME_COUNT].Add(1);
            } else if (path->IsKesus()) {
                Self->TabletCounters->Simple()[COUNTER_KESUS_COUNT].Add(1);
            } else if (path->IsSolomon()) {
                Self->TabletCounters->Simple()[COUNTER_SOLOMON_VOLUME_COUNT].Add(1);
            } else if (path->IsOlapStore()) {
                Self->TabletCounters->Simple()[COUNTER_OLAP_STORE_COUNT].Add(1);
            } else if (path->IsColumnTable()) {
                Self->TabletCounters->Simple()[COUNTER_COLUMN_TABLE_COUNT].Add(1);
            } else if (path->IsSequence()) {
                Self->TabletCounters->Simple()[COUNTER_SEQUENCE_COUNT].Add(1);
            } else if (path->IsReplication()) {
                Self->TabletCounters->Simple()[COUNTER_REPLICATION_COUNT].Add(1);
            } else if (path->IsExternalTable()) {
                Self->TabletCounters->Simple()[COUNTER_EXTERNAL_TABLE_COUNT].Add(1);
            }

            path->ApplySpecialAttributes();
        }

        for (const auto& kv : Self->BlockStoreVolumes) {
            auto itPath = Self->PathsById.find(kv.first);
            if (itPath == Self->PathsById.end() || itPath->second->Dropped()) {
                continue;
            }
            auto volumeSpace = kv.second->GetVolumeSpace();
            auto domainDir = Self->PathsById.at(Self->ResolvePathIdForDomain(itPath->second));
            domainDir->ChangeVolumeSpaceBegin(volumeSpace, { });
        }

        // Find all operations that were in the process of execution
        for (auto& item : Self->TxInFlight) {
            const TTxState& txState = item.second;

            ui32 inFlightCounter = TTxState::TxTypeInFlightCounter(txState.TxType);
            Self->TabletCounters->Simple()[inFlightCounter].Add(1);
        }

        // Publications
        {
            TPublicationsRows publicationRows;
            if (!LoadPublications(db, publicationRows)) {
                return false;
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for Publications"
                             << ", read records: " << publicationRows.size()
                             << ", at schemeshard: " << Self->TabletID());

            for (auto& rec: publicationRows) {
                TTxId txId = std::get<0>(rec);
                TPathId pathId = std::get<1>(rec);
                ui64 version = std::get<2>(rec);

                LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                             "Resume publishing for paths"
                                 << ", tx: " << txId
                                 << ", path id: " << pathId
                                 << ", version: " << version
                                 << ", at schemeshard: " << Self->TabletID());

                if (Self->Operations.contains(txId)) {
                    TOperation::TPtr operation = Self->Operations.at(txId);
                    operation->AddPublishingPath(pathId, version);
                } else {
                    Self->Publications[txId].Paths.emplace(pathId, version);
                }

                Publications[txId].push_back(pathId);
                Self->IncrementPathDbRefCount(pathId);
            }
        }

        // Read exports
        {
            // read main info
            {
                auto rowset = db.Table<Schema::Exports>().Range().Select();
                if (!rowset.IsReady()) {
                    return false;
                }

                while (!rowset.EndOfSet()) {
                    ui64 id = rowset.GetValue<Schema::Exports::Id>();
                    TString uid = rowset.GetValue<Schema::Exports::Uid>();
                    TExportInfo::EKind kind = static_cast<TExportInfo::EKind>(rowset.GetValueOrDefault<Schema::Exports::Kind>(0));
                    TString settings = rowset.GetValue<Schema::Exports::Settings>();
                    auto domainPathId = TPathId(rowset.GetValueOrDefault<Schema::Exports::DomainPathOwnerId>(selfId),
                                                rowset.GetValue<Schema::Exports::DomainPathId>());

                    TExportInfo::TPtr exportInfo = new TExportInfo(id, uid, kind, settings, domainPathId);

                    if (rowset.HaveValue<Schema::Exports::UserSID>()) {
                        exportInfo->UserSID = rowset.GetValue<Schema::Exports::UserSID>();
                    }

                    ui32 items = rowset.GetValue<Schema::Exports::Items>();
                    exportInfo->Items.resize(items);

                    exportInfo->ExportPathId = TPathId(rowset.GetValueOrDefault<Schema::Exports::ExportOwnerPathId>(selfId),
                                                       rowset.GetValueOrDefault<Schema::Exports::ExportPathId>(InvalidLocalPathId));
                    if (exportInfo->ExportPathId.LocalPathId == InvalidLocalPathId) {
                        exportInfo->ExportPathId = InvalidPathId;
                    }
                    exportInfo->State = static_cast<TExportInfo::EState>(rowset.GetValue<Schema::Exports::State>());
                    exportInfo->WaitTxId = rowset.GetValueOrDefault<Schema::Exports::WaitTxId>(InvalidTxId);
                    exportInfo->Issue = rowset.GetValueOrDefault<Schema::Exports::Issue>(TString());

                    Self->Exports[id] = exportInfo;
                    if (uid) {
                        Self->ExportsByUid[uid] = exportInfo;
                    }

                    if (exportInfo->WaitTxId != InvalidTxId) {
                        Self->TxIdToExport[exportInfo->WaitTxId] = {id, Max<ui32>()};
                    }

                    if (exportInfo->IsInProgress()) {
                        ExportsToResume.push_back(exportInfo->Id);
                    }

                    if (!rowset.Next()) {
                        return false;
                    }
                }
            }

            // read items info
            {
                auto rowset = db.Table<Schema::ExportItems>().Range().Select();
                if (!rowset.IsReady()) {
                    return false;
                }

                while (!rowset.EndOfSet()) {
                    ui64 exportId = rowset.GetValue<Schema::ExportItems::ExportId>();
                    Y_VERIFY_S(Self->Exports.contains(exportId), "Export not found"
                               << ": exportId# " << exportId);

                    TExportInfo::TPtr exportInfo = Self->Exports.at(exportId);

                    ui32 itemIdx = rowset.GetValue<Schema::ExportItems::Index>();
                    Y_VERIFY_S(itemIdx < exportInfo->Items.size(), "Invalid item's index"
                               << ": exportId# " << exportId
                               << ", itemIdx# " << itemIdx);

                    TExportInfo::TItem& item = exportInfo->Items[itemIdx];
                    item.SourcePathName = rowset.GetValue<Schema::ExportItems::SourcePathName>();

                    item.SourcePathId.OwnerId = rowset.GetValueOrDefault<Schema::ExportItems::SourceOwnerPathId>(selfId);
                    item.SourcePathId.LocalPathId = rowset.GetValue<Schema::ExportItems::SourcePathId>();

                    item.State = static_cast<TExportInfo::EState>(rowset.GetValue<Schema::ExportItems::State>());
                    item.WaitTxId = rowset.GetValueOrDefault<Schema::ExportItems::BackupTxId>(InvalidTxId);
                    item.Issue = rowset.GetValueOrDefault<Schema::ExportItems::Issue>(TString());

                    if (item.State <= TExportInfo::EState::Transferring && item.WaitTxId == InvalidTxId) {
                        exportInfo->PendingItems.push_back(itemIdx);
                    } else if (item.WaitTxId != InvalidTxId) {
                        Self->TxIdToExport[item.WaitTxId] = {exportId, itemIdx};
                    }

                    if (!rowset.Next()) {
                        return false;
                    }
                }
            }
        }

        // Read imports
        {
            // read main info
            {
                auto rowset = db.Table<Schema::Imports>().Range().Select();
                if (!rowset.IsReady()) {
                    return false;
                }

                while (!rowset.EndOfSet()) {
                    ui64 id = rowset.GetValue<Schema::Imports::Id>();
                    TString uid = rowset.GetValue<Schema::Imports::Uid>();
                    TImportInfo::EKind kind = static_cast<TImportInfo::EKind>(rowset.GetValue<Schema::Imports::Kind>());
                    auto domainPathId = TPathId(rowset.GetValue<Schema::Imports::DomainPathOwnerId>(),
                                                rowset.GetValue<Schema::Imports::DomainPathLocalId>());

                    Ydb::Import::ImportFromS3Settings settings;
                    Y_VERIFY(ParseFromStringNoSizeLimit(settings, rowset.GetValue<Schema::Imports::Settings>()));

                    TImportInfo::TPtr importInfo = new TImportInfo(id, uid, kind, settings, domainPathId);

                    if (rowset.HaveValue<Schema::Imports::UserSID>()) {
                        importInfo->UserSID = rowset.GetValue<Schema::Imports::UserSID>();
                    }

                    ui32 items = rowset.GetValue<Schema::Imports::Items>();
                    importInfo->Items.resize(items);

                    importInfo->State = static_cast<TImportInfo::EState>(rowset.GetValue<Schema::Imports::State>());
                    importInfo->Issue = rowset.GetValueOrDefault<Schema::Imports::Issue>(TString());

                    Self->Imports[id] = importInfo;
                    if (uid) {
                        Self->ImportsByUid[uid] = importInfo;
                    }

                    switch (importInfo->State) {
                    case TImportInfo::EState::Waiting:
                    case TImportInfo::EState::Cancellation:
                        ImportsToResume.push_back(importInfo->Id);
                        break;
                    default:
                        break;
                    }

                    if (!rowset.Next()) {
                        return false;
                    }
                }
            }

            // read items info
            {
                auto rowset = db.Table<Schema::ImportItems>().Range().Select();
                if (!rowset.IsReady()) {
                    return false;
                }

                while (!rowset.EndOfSet()) {
                    ui64 importId = rowset.GetValue<Schema::ImportItems::ImportId>();
                    Y_VERIFY_S(Self->Imports.contains(importId), "Import not found"
                               << ": importId# " << importId);

                    TImportInfo::TPtr importInfo = Self->Imports.at(importId);

                    ui32 itemIdx = rowset.GetValue<Schema::ImportItems::Index>();
                    Y_VERIFY_S(itemIdx < importInfo->Items.size(), "Invalid item's index"
                               << ": importId# " << importId
                               << ", itemIdx# " << itemIdx);

                    TImportInfo::TItem& item = importInfo->Items[itemIdx];
                    item.DstPathName = rowset.GetValue<Schema::ImportItems::DstPathName>();
                    item.DstPathId = TPathId(rowset.GetValueOrDefault<Schema::ImportItems::DstPathOwnerId>(InvalidOwnerId),
                                             rowset.GetValueOrDefault<Schema::ImportItems::DstPathLocalId>(InvalidLocalPathId));

                    if (rowset.HaveValue<Schema::ImportItems::Scheme>()) {
                        Ydb::Table::CreateTableRequest scheme;
                        Y_VERIFY(ParseFromStringNoSizeLimit(scheme, rowset.GetValue<Schema::ImportItems::Scheme>()));
                        item.Scheme = scheme;
                    }

                    item.State = static_cast<TImportInfo::EState>(rowset.GetValue<Schema::ImportItems::State>());
                    item.WaitTxId = rowset.GetValueOrDefault<Schema::ImportItems::WaitTxId>(InvalidTxId);
                    item.NextIndexIdx = rowset.GetValueOrDefault<Schema::ImportItems::NextIndexIdx>(0);
                    item.Issue = rowset.GetValueOrDefault<Schema::ImportItems::Issue>(TString());

                    if (item.WaitTxId != InvalidTxId) {
                        Self->TxIdToImport[item.WaitTxId] = {importId, itemIdx};
                    }

                    if (!rowset.Next()) {
                        return false;
                    }
                }
            }
        }

        // Read index build
        {
            // read main info
            {
                auto rowset = db.Table<Schema::IndexBuild>().Range().Select();
                if (!rowset.IsReady()) {
                    return false;
                }

                while (!rowset.EndOfSet()) {
                    TIndexBuildId id = rowset.GetValue<Schema::IndexBuild::Id>();
                    TString uid = rowset.GetValue<Schema::IndexBuild::Uid>();

                    TIndexBuildInfo::TPtr indexInfo = new TIndexBuildInfo(id, uid);

                    indexInfo->DomainPathId = TPathId(
                        rowset.GetValue<Schema::IndexBuild::DomainOwnerId>(),
                        rowset.GetValue<Schema::IndexBuild::DomainLocalId>());

                    indexInfo->TablePathId = TPathId(
                        rowset.GetValue<Schema::IndexBuild::TableOwnerId>(),
                        rowset.GetValue<Schema::IndexBuild::TableLocalId>());

                    indexInfo->IndexName = rowset.GetValue<Schema::IndexBuild::IndexName>();
                    indexInfo->IndexType = rowset.GetValue<Schema::IndexBuild::IndexType>();

                    indexInfo->State = TIndexBuildInfo::EState(rowset.GetValue<Schema::IndexBuild::State>());
                    indexInfo->Issue = rowset.GetValueOrDefault<Schema::IndexBuild::Issue>();
                    indexInfo->CancelRequested = rowset.GetValueOrDefault<Schema::IndexBuild::CancelRequest>(false);

                    indexInfo->LockTxId = rowset.GetValueOrDefault<Schema::IndexBuild::LockTxId>(indexInfo->LockTxId);
                    indexInfo->LockTxStatus = rowset.GetValueOrDefault<Schema::IndexBuild::LockTxStatus>(indexInfo->LockTxStatus);
                    indexInfo->LockTxDone = rowset.GetValueOrDefault<Schema::IndexBuild::LockTxDone>(indexInfo->LockTxDone);

                    indexInfo->InitiateTxId = rowset.GetValueOrDefault<Schema::IndexBuild::InitiateTxId>(indexInfo->InitiateTxId);
                    indexInfo->InitiateTxStatus = rowset.GetValueOrDefault<Schema::IndexBuild::InitiateTxStatus>(indexInfo->InitiateTxStatus);
                    indexInfo->InitiateTxDone = rowset.GetValueOrDefault<Schema::IndexBuild::InitiateTxDone>(indexInfo->InitiateTxDone);

                    indexInfo->Limits.MaxBatchRows = rowset.GetValue<Schema::IndexBuild::MaxBatchRows>();
                    indexInfo->Limits.MaxBatchBytes = rowset.GetValue<Schema::IndexBuild::MaxBatchBytes>();
                    indexInfo->Limits.MaxShards = rowset.GetValue<Schema::IndexBuild::MaxShards>();
                    indexInfo->Limits.MaxRetries = rowset.GetValueOrDefault<Schema::IndexBuild::MaxRetries>(indexInfo->Limits.MaxRetries);

                    indexInfo->ApplyTxId = rowset.GetValueOrDefault<Schema::IndexBuild::ApplyTxId>(indexInfo->ApplyTxId);
                    indexInfo->ApplyTxStatus = rowset.GetValueOrDefault<Schema::IndexBuild::ApplyTxStatus>(indexInfo->ApplyTxStatus);
                    indexInfo->ApplyTxDone = rowset.GetValueOrDefault<Schema::IndexBuild::ApplyTxDone>(indexInfo->ApplyTxDone);

                    indexInfo->UnlockTxId = rowset.GetValueOrDefault<Schema::IndexBuild::UnlockTxId>(indexInfo->UnlockTxId);
                    indexInfo->UnlockTxStatus = rowset.GetValueOrDefault<Schema::IndexBuild::UnlockTxStatus>(indexInfo->UnlockTxStatus);
                    indexInfo->UnlockTxDone = rowset.GetValueOrDefault<Schema::IndexBuild::UnlockTxDone>(indexInfo->UnlockTxDone);

                    indexInfo->Billed = TBillingStats(
                        rowset.GetValueOrDefault<Schema::IndexBuild::RowsBilled>(0),
                        rowset.GetValueOrDefault<Schema::IndexBuild::BytesBilled>(0));

                    Y_VERIFY(!Self->IndexBuilds.contains(id));
                    Self->IndexBuilds[id] = indexInfo;
                    if (uid) {
                        Self->IndexBuildsByUid[uid] = indexInfo;
                    }

                    OnComplete.ToProgress(id);

                    if (!rowset.Next()) {
                        return false;
                    }
                }
            }

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "IndexBuild "
                             << ", records: " << Self->IndexBuilds.size()
                             << ", at schemeshard: " << Self->TabletID());

            // read index build columns
            {
                auto rowset = db.Table<Schema::IndexBuildColumns>().Range().Select();
                if (!rowset.IsReady()) {
                    return false;
                }

                while (!rowset.EndOfSet()) {
                    TIndexBuildId id = rowset.GetValue<Schema::IndexBuildColumns::Id>();
                    Y_VERIFY_S(Self->IndexBuilds.contains(id), "BuildIndex not found"
                                   << ": id# " << id);

                    TIndexBuildInfo::TPtr buildInfo = Self->IndexBuilds.at(id);

                    TString columnName = rowset.GetValue<Schema::IndexBuildColumns::ColumnName>();
                    EIndexColumnKind columnKind = rowset.GetValueOrDefault<Schema::IndexBuildColumns::ColumnKind>(EIndexColumnKind::KeyColumn);
                    ui32 columnNo = rowset.GetValue<Schema::IndexBuildColumns::ColumnNo>();

                    Y_VERIFY_S(columnNo == (buildInfo->IndexColumns.size() + buildInfo->DataColumns.size()),
                               "Unexpected non contiguous column number# " << columnNo <<
                               " indexColumns# " << buildInfo->IndexColumns.size() <<
                               " dataColumns# " << buildInfo->DataColumns.size());

                    switch (columnKind) {
                        case EIndexColumnKind::KeyColumn:
                            buildInfo->IndexColumns.push_back(columnName);
                        break;
                        case EIndexColumnKind::DataColumn:
                            buildInfo->DataColumns.push_back(columnName);
                        break;
                        default:
                            Y_FAIL_S("Unknown column kind# " << (int)columnKind);
                        break;
                    }

                    if (!rowset.Next()) {
                        return false;
                    }
                }
            }

            // read index build upload progress
            {
                auto rowset = db.Table<Schema::IndexBuildShardStatus>().Range().Select();
                if (!rowset.IsReady()) {
                    return false;
                }

                while (!rowset.EndOfSet()) {
                    TIndexBuildId id = rowset.GetValue<Schema::IndexBuildShardStatus::Id>();
                    Y_VERIFY_S(Self->IndexBuilds.contains(id), "BuildIndex not found"
                                   << ": id# " << id);

                    TIndexBuildInfo::TPtr buildInfo = Self->IndexBuilds.at(id);

                    TShardIdx shardIdx = TShardIdx(rowset.GetValue<Schema::IndexBuildShardStatus::OwnerShardIdx>(),
                                                   rowset.GetValue<Schema::IndexBuildShardStatus::LocalShardIdx>());

                    NKikimrTx::TKeyRange range = rowset.GetValue<Schema::IndexBuildShardStatus::Range>();
                    TString lastKeyAck = rowset.GetValue<Schema::IndexBuildShardStatus::LastKeyAck>();

                    buildInfo->Shards.emplace(shardIdx, TIndexBuildInfo::TShardStatus(TSerializedTableRange(range), std::move(lastKeyAck)));
                    TIndexBuildInfo::TShardStatus& shardStatus = buildInfo->Shards.at(shardIdx);

                    shardStatus.Status = rowset.GetValue<Schema::IndexBuildShardStatus::Status>();

                    shardStatus.DebugMessage = rowset.GetValueOrDefault<Schema::IndexBuildShardStatus::Message>();
                    shardStatus.UploadStatus = rowset.GetValueOrDefault<Schema::IndexBuildShardStatus::UploadStatus>(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED);

                    shardStatus.Processed = TBillingStats(
                        rowset.GetValueOrDefault<Schema::IndexBuildShardStatus::RowsProcessed>(0),
                        rowset.GetValueOrDefault<Schema::IndexBuildShardStatus::BytesProcessed>(0));

                    buildInfo->Processed += shardStatus.Processed;

                    if (!rowset.Next()) {
                        return false;
                    }
                }
            }
        }

        // Read snapshot tables
        {
            ui64 records = 0;
            {
                auto rowset = db.Table<Schema::SnapshotTables>().Range().Select();
                if (!rowset.IsReady()) {
                    return false;
                }

                while (!rowset.EndOfSet()) {
                    TTxId id = rowset.GetValue<Schema::SnapshotTables::Id>();

                    TPathId tableId = TPathId(
                        rowset.GetValue<Schema::SnapshotTables::TableOwnerId>(),
                        rowset.GetValue<Schema::SnapshotTables::TableLocalId>());

                    Self->TablesWithSnapshots.emplace(tableId, id);
                    Self->SnapshotTables[id].insert(tableId);
                    ++records;

                    if (!rowset.Next()) {
                        return false;
                    }
                }
            }
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "SnapshotTables: "
                             << " snapshots: " << Self->SnapshotTables.size()
                             << " tables: " << records
                             << ", at schemeshard: " << Self->TabletID());


            // read snapshot steps
            {
                auto rowset = db.Table<Schema::SnapshotSteps>().Range().Select();
                if (!rowset.IsReady()) {
                    return false;
                }

                while (!rowset.EndOfSet()) {
                    TTxId id = rowset.GetValue<Schema::SnapshotSteps::Id>();
                    Y_VERIFY_S(Self->SnapshotTables.contains(id), "Snapshot not found"
                                   << ": id# " << id);

                    TStepId stepId = rowset.GetValue<Schema::SnapshotSteps::StepId>();

                    Self->SnapshotsStepIds[id] = stepId;

                    if (!rowset.Next()) {
                        return false;
                    }
                }
            }
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "SnapshotSteps: "
                             << " snapshots: " << Self->SnapshotsStepIds.size()
                             << ", at schemeshard: " << Self->TabletID());
        }

        // Read long locks
        {
            auto rowset = db.Table<Schema::LongLocks>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                auto pathId = TPathId(
                    rowset.GetValue<Schema::LongLocks::PathOwnerId>(),
                    rowset.GetValue<Schema::LongLocks::PathLocalId>());

                TTxId txId = rowset.GetValue<Schema::LongLocks::LockId>();

                Self->LockedPaths[pathId] = txId;

                if (!rowset.Next()) {
                    return false;
                }
            }
        }
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "LongLocks: "
                         << " records: " << Self->LockedPaths.size()
                         << ", at schemeshard: " << Self->TabletID());

        // Read olap stores
        {
            auto rowset = db.Table<Schema::OlapStores>().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rowset.GetValue<Schema::OlapStores::PathId>());
                ui64 alterVersion = rowset.GetValue<Schema::OlapStores::AlterVersion>();
                NKikimrSchemeOp::TColumnStoreDescription description;
                Y_VERIFY(description.ParseFromString(rowset.GetValue<Schema::OlapStores::Description>()));
                NKikimrSchemeOp::TColumnStoreSharding sharding;
                Y_VERIFY(sharding.ParseFromString(rowset.GetValue<Schema::OlapStores::Sharding>()));

                Self->OlapStores[pathId] = new TOlapStoreInfo(alterVersion, std::move(description), std::move(sharding));
                Self->IncrementPathDbRefCount(pathId);
                Self->SetPartitioning(pathId, Self->OlapStores[pathId]);

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // Read olap stores (alters)
        {
            auto rowset = db.Table<Schema::OlapStoresAlters>().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rowset.GetValue<Schema::OlapStoresAlters::PathId>());
                ui64 alterVersion = rowset.GetValue<Schema::OlapStoresAlters::AlterVersion>();
                NKikimrSchemeOp::TColumnStoreDescription description;
                Y_VERIFY(description.ParseFromString(rowset.GetValue<Schema::OlapStoresAlters::Description>()));
                NKikimrSchemeOp::TColumnStoreSharding sharding;
                Y_VERIFY(sharding.ParseFromString(rowset.GetValue<Schema::OlapStoresAlters::Sharding>()));
                TMaybe<NKikimrSchemeOp::TAlterColumnStore> alterBody;
                if (rowset.HaveValue<Schema::OlapStoresAlters::AlterBody>()) {
                    Y_VERIFY(alterBody.ConstructInPlace().ParseFromString(rowset.GetValue<Schema::OlapStoresAlters::AlterBody>()));
                }

                Y_VERIFY_S(Self->OlapStores.contains(pathId),
                    "Cannot load alter for olap store " << pathId);

                Self->OlapStores[pathId]->AlterData = new TOlapStoreInfo(alterVersion, std::move(description), std::move(sharding), std::move(alterBody));

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // Read olap tables
        {
            auto rowset = db.Table<Schema::ColumnTables>().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rowset.GetValue<Schema::ColumnTables::PathId>());
                ui64 alterVersion = rowset.GetValue<Schema::ColumnTables::AlterVersion>();
                NKikimrSchemeOp::TColumnTableDescription description;
                Y_VERIFY(description.ParseFromString(rowset.GetValue<Schema::ColumnTables::Description>()));
                NKikimrSchemeOp::TColumnTableSharding sharding;
                Y_VERIFY(sharding.ParseFromString(rowset.GetValue<Schema::ColumnTables::Sharding>()));
                TMaybe<NKikimrSchemeOp::TColumnStoreSharding> storeSharding;
                if (rowset.HaveValue<Schema::ColumnTables::StandaloneSharding>()) {
                    Y_VERIFY(storeSharding.ConstructInPlace().ParseFromString(
                        rowset.GetValue<Schema::ColumnTables::StandaloneSharding>()));
                }

                auto tableInfo = Self->ColumnTables.BuildNew(pathId, new TColumnTableInfo(alterVersion,
                    std::move(description), std::move(sharding), std::move(storeSharding)));
                Self->IncrementPathDbRefCount(pathId);

                if (tableInfo->OlapStorePathId) {
                    auto itStore = Self->OlapStores.find(*tableInfo->OlapStorePathId);
                    if (itStore != Self->OlapStores.end()) {
                        itStore->second->ColumnTables.insert(pathId);
                        if (pathsUnderOperation.contains(pathId)) {
                            itStore->second->ColumnTablesUnderOperation.insert(pathId);
                        }
                    }
                }

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // Read olap tables (alters)
        {
            auto rowset = db.Table<Schema::ColumnTablesAlters>().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rowset.GetValue<Schema::ColumnTablesAlters::PathId>());
                ui64 alterVersion = rowset.GetValue<Schema::ColumnTablesAlters::AlterVersion>();
                NKikimrSchemeOp::TColumnTableDescription description;
                Y_VERIFY(description.ParseFromString(rowset.GetValue<Schema::ColumnTablesAlters::Description>()));
                NKikimrSchemeOp::TColumnTableSharding sharding;
                Y_VERIFY(sharding.ParseFromString(rowset.GetValue<Schema::ColumnTablesAlters::Sharding>()));
                TMaybe<NKikimrSchemeOp::TAlterColumnTable> alterBody;
                if (rowset.HaveValue<Schema::ColumnTablesAlters::AlterBody>()) {
                    Y_VERIFY(alterBody.ConstructInPlace().ParseFromString(rowset.GetValue<Schema::ColumnTablesAlters::AlterBody>()));
                }
                TMaybe<NKikimrSchemeOp::TColumnStoreSharding> storeSharding;
                if (rowset.HaveValue<Schema::ColumnTablesAlters::StandaloneSharding>()) {
                    Y_VERIFY(storeSharding.ConstructInPlace().ParseFromString(
                        rowset.GetValue<Schema::ColumnTablesAlters::StandaloneSharding>()));
                }

                Y_VERIFY_S(Self->ColumnTables.contains(pathId),
                    "Cannot load alter for olap table " << pathId);

                TColumnTableInfo::TPtr alterData = new TColumnTableInfo(alterVersion,
                    std::move(description), std::move(sharding), std::move(storeSharding), std::move(alterBody));
                auto ctInfo = Self->ColumnTables.TakeVerified(pathId);
                ctInfo->AlterData = alterData;

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // Read sequences
        {
            auto rowset = db.Table<Schema::Sequences>().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rowset.GetValue<Schema::Sequences::PathId>());
                ui64 alterVersion = rowset.GetValue<Schema::Sequences::AlterVersion>();
                NKikimrSchemeOp::TSequenceDescription description;
                Y_VERIFY(description.ParseFromString(rowset.GetValue<Schema::Sequences::Description>()));
                NKikimrSchemeOp::TSequenceSharding sharding;
                Y_VERIFY(sharding.ParseFromString(rowset.GetValue<Schema::Sequences::Sharding>()));

                TSequenceInfo::TPtr sequenceInfo = new TSequenceInfo(alterVersion, std::move(description), std::move(sharding));
                Self->Sequences[pathId] = sequenceInfo;
                Self->IncrementPathDbRefCount(pathId);

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // Read sequences (alters)
        {
            auto rowset = db.Table<Schema::SequencesAlters>().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rowset.GetValue<Schema::SequencesAlters::PathId>());
                ui64 alterVersion = rowset.GetValue<Schema::SequencesAlters::AlterVersion>();
                NKikimrSchemeOp::TSequenceDescription description;
                Y_VERIFY(description.ParseFromString(rowset.GetValue<Schema::SequencesAlters::Description>()));
                NKikimrSchemeOp::TSequenceSharding sharding;
                Y_VERIFY(sharding.ParseFromString(rowset.GetValue<Schema::SequencesAlters::Sharding>()));

                TSequenceInfo::TPtr alterData = new TSequenceInfo(alterVersion, std::move(description), std::move(sharding));
                Y_VERIFY_S(Self->Sequences.contains(pathId),
                    "Cannot load alter for sequence " << pathId);
                Self->Sequences[pathId]->AlterData = alterData;

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // Read replications
        {
            auto rowset = db.Table<Schema::Replications>().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rowset.GetValue<Schema::Replications::PathId>());
                ui64 alterVersion = rowset.GetValue<Schema::Replications::AlterVersion>();
                NKikimrSchemeOp::TReplicationDescription description;
                Y_VERIFY(description.ParseFromString(rowset.GetValue<Schema::Replications::Description>()));

                TReplicationInfo::TPtr replicationInfo = new TReplicationInfo(alterVersion, std::move(description));
                Self->Replications[pathId] = replicationInfo;
                Self->IncrementPathDbRefCount(pathId);

                if (replicationControllers.contains(pathId)) {
                    replicationInfo->ControllerShardIdx = replicationControllers.at(pathId);
                }

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // Read replication alters
        {
            auto rowset = db.Table<Schema::ReplicationsAlterData>().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                TPathId pathId = Self->MakeLocalId(rowset.GetValue<Schema::ReplicationsAlterData::PathId>());
                ui64 alterVersion = rowset.GetValue<Schema::ReplicationsAlterData::AlterVersion>();
                NKikimrSchemeOp::TReplicationDescription description;
                Y_VERIFY(description.ParseFromString(rowset.GetValue<Schema::ReplicationsAlterData::Description>()));

                TReplicationInfo::TPtr alterData = new TReplicationInfo(alterVersion, std::move(description));
                Y_VERIFY_S(Self->Replications.contains(pathId),
                    "Cannot load alter for replication " << pathId);
                auto replicationInfo = Self->Replications.at(pathId);

                alterData->ControllerShardIdx = replicationInfo->ControllerShardIdx;
                replicationInfo->AlterData = alterData;

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // Read blob depots
        {
            using T = Schema::BlobDepots;

            auto rowset = db.Table<T>().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                const TPathId pathId = Self->MakeLocalId(rowset.GetValue<T::PathId>());
                const ui64 alterVersion = rowset.GetValue<T::AlterVersion>();
                NKikimrSchemeOp::TBlobDepotDescription description;
                const bool success = description.ParseFromString(rowset.GetValue<T::Description>());
                Y_VERIFY(success);

                auto blobDepot = MakeIntrusive<TBlobDepotInfo>(alterVersion, description);
                Self->BlobDepots[pathId] = blobDepot;
                Self->IncrementPathDbRefCount(pathId);

                if (const auto it = blobDepotShards.find(pathId); it != blobDepotShards.end()) {
                    blobDepot->BlobDepotShardIdx = it->second;
                    if (const auto jt = Self->ShardInfos.find(it->second); jt != Self->ShardInfos.end()) {
                        blobDepot->BlobDepotTabletId = jt->second.TabletID;
                    }
                }

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        for (auto& item : Self->Operations) {
            auto& operation = item.second;
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxInit for TxInFlight"
                             << " execute ProgressState for all parts "
                             << ", txId: " << operation->TxId
                             << ", parts: " <<  operation->Parts.size()
                             << ", await num: " << operation->WaitOperations.size()
                             << ", dependent num: " << operation->DependentOperations.size()
                             << ", at schemeshard: " << Self->TabletID());

            if (operation->WaitOperations.size()) {
                continue;
            }

            for (auto& part: operation->Parts) {
                TOperationContext context{Self, txc, ctx, OnComplete, MemChanges, DbChanges};
                part->ProgressState(context);
            }
        }

        CollectObjectsToClean();

        OnComplete.ApplyOnExecute(Self, txc, ctx);
        DbChanges.Apply(Self, txc, ctx);
        return true;
    }

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        try {
            bool newScheme = CreateScheme(txc);
            if (newScheme)
                return true;
            return ReadEverything(txc, ctx);
        } catch (const TNotReadyTabletException &) {
            return false;
        } catch (const TSchemeErrorTabletException &ex) {
            Y_FAIL("there must be no leaked scheme error exceptions: %s", ex.what());
        } catch (const std::exception& ex) {
            Y_FAIL("there must be no leaked exceptions: %s", ex.what());
        } catch (...) {
            Y_FAIL("there must be no leaked exceptions");
        }
    }

    void Complete(const TActorContext &ctx) override {
        if (Broken) {
            return;
        }

        auto delayPublications = OnComplete.ExtractPublicationsToSchemeBoard(); //there no Populator exist jet
        for (auto& [txId, pathIds] : Publications) {
            std::move(pathIds.begin(), pathIds.end(), std::back_inserter(delayPublications[txId]));
        }

        OnComplete.ApplyOnComplete(Self, ctx);

        if (!Self->IsSchemeShardConfigured()) {
            if (Self->IsDomainSchemeShard) { // self initiation
                Self->Execute(Self->CreateTxInitRoot(), ctx);
            } else { // wait initiation msg
                Self->SignalTabletActive(ctx);
                Self->Become(&TSelf::StateConfigure);
            }
            return;
        }

        // flatten
        TVector<TPathId> cdcStreamScansToResume;
        for (auto& [_, v] : CdcStreamScansToResume) {
            std::move(v.begin(), v.end(), std::back_inserter(cdcStreamScansToResume));
        }

        Self->ActivateAfterInitialization(ctx, {
            .DelayPublications = std::move(delayPublications),
            .ExportIds = ExportsToResume,
            .ImportsIds = ImportsToResume,
            .CdcStreamScans = std::move(cdcStreamScansToResume),
            .TablesToClean = std::move(TablesToClean),
            .BlockStoreVolumesToClean = std::move(BlockStoreVolumesToClean),
        });
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxInit() {
    return new TTxInit(this);
}

}}
