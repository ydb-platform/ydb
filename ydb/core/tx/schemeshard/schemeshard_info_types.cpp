#include "schemeshard_info_types.h"
#include "schemeshard_utils.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/compile_time_flags.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/util/pb.h>

#include <ydb/library/yql/minikql/mkql_type_ops.h>

#include <util/generic/algorithm.h>

namespace NKikimr {
namespace NSchemeShard {

TTableInfo::TAlterDataPtr TTableInfo::CreateAlterData(
    TPtr source,
    NKikimrSchemeOp::TTableDescription& op,
    const NScheme::TTypeRegistry& typeRegistry,
    const TSchemeLimits& limits, const TSubDomainInfo& subDomain,
    TString& errStr, const THashSet<TString>& localSequences)
{
    TAlterDataPtr alterData = new TTableInfo::TAlterTableInfo();
    alterData->TableDescriptionFull = NKikimrSchemeOp::TTableDescription();

    alterData->PartitionConfigFull().CopyFrom(op.GetPartitionConfig());

    TColumnFamiliesMerger columnFamilyMerger(alterData->PartitionConfigFull());

    if (source) {
        alterData->AlterVersion = source->AlterVersion + 1;
        alterData->NextColumnId = source->NextColumnId;
    }
    THashMap<TString, ui32> colName2Id;
    THashSet<ui32> keys;

    if (source) {
        for (const auto& col : source->Columns) {
            Y_VERIFY(col.first == col.second.Id);
            // There could be columns with same name. Only one of them can be active.
            if (col.second.IsDropped())
                continue;

            colName2Id[col.second.Name] = col.first;
            if (col.second.KeyOrder != Max<ui32>())
                keys.insert(col.first);
        }
    }

    for (auto& col : *op.MutableColumns()) {
        TString colName = col.GetName();

        if (colName.size() > limits.MaxTableColumnNameLength) {
            errStr = TStringBuilder()
                << "Column name too long '" << colName << "'. "
                << "Limit: " << limits.MaxTableColumnNameLength;
            return nullptr;
        }

        if (!IsValidColumnName(colName)) {
            errStr = Sprintf("Invalid name for column '%s'", colName.data());
            return nullptr;
        }

        auto typeName = NMiniKQL::AdaptLegacyYqlType(col.GetType());
        const NScheme::IType* type = typeRegistry.GetType(typeName);

        NKikimrSchemeOp::TFamilyDescription* columnFamily = nullptr;
        if (col.HasFamily() && col.HasFamilyName()) {
            columnFamily = columnFamilyMerger.Get(col.GetFamily(), col.GetFamilyName(), errStr);
        } else if (col.HasFamily()) {
            columnFamily = columnFamilyMerger.Get(col.GetFamily(), errStr);
        } else if (col.HasFamilyName()) {
            columnFamily = columnFamilyMerger.AddOrGet(col.GetFamilyName(), errStr);
        }

        if ((col.HasFamily() || col.HasFamilyName()) && !columnFamily) {
            return nullptr;
        }

        bool isAlterColumn = (source && colName2Id.contains(colName));
        if (isAlterColumn) {
            if (keys.contains(colName2Id.at(colName))) {
                errStr = TStringBuilder()
                    << "Cannot alter key column ' " << colName << "' with id " << colName2Id.at(colName);
                return nullptr;
            }

            if (col.HasType()) {
                errStr = Sprintf("Cannot alter type for column '%s'", colName.data());
                return nullptr;
            }

            if (!columnFamily) {
                errStr = Sprintf("Nothing to alter for column '%s'", colName.data());
                return nullptr;
            }

            if (col.DefaultValue_case() != NKikimrSchemeOp::TColumnDescription::DEFAULTVALUE_NOT_SET) {
                errStr = Sprintf("Cannot alter default for column '%s'", colName.c_str());
                return nullptr;
            }

            ui32 colId = colName2Id[colName];
            TTableInfo::TColumn& column = alterData->Columns[colId];
            column = source->Columns[colId];
            column.Family = columnFamily->GetId();
        } else {
            if (colName2Id.contains(colName)) {
                errStr = Sprintf("Column '%s' specified more than once", colName.data());
                return nullptr;
            }

            if (source && !col.HasType()) {
                errStr = Sprintf("Column '%s' cannot be altered (does not exist)", colName.c_str());
                return nullptr;
            }

            if (!type) {
                errStr = Sprintf("Type '%s' specified for column '%s' is not supported by storage", col.GetType().data(), colName.data());
                return nullptr;
            }

            // Only allow YQL types
            if (!NScheme::NTypeIds::IsYqlType(type->GetTypeId())) {
                errStr = Sprintf("Type '%s' specified for column '%s' is no longer supported", col.GetType().data(), colName.data());
                return nullptr;
            }

            ui32 colId = col.HasId() ? col.GetId() : alterData->NextColumnId;
            if (alterData->Columns.contains(colId)) {
                errStr = Sprintf("Duplicate column id: %" PRIu32, colId);
                return nullptr;
            }

            if (col.HasDefaultFromSequence() && !localSequences.contains(col.GetDefaultFromSequence())) {
                errStr = Sprintf("Column '%s' cannot use an unknown sequence '%s'", colName.c_str(), col.GetDefaultFromSequence().c_str());
                return nullptr;
            }

            alterData->NextColumnId = Max(colId + 1, alterData->NextColumnId);

            colName2Id[colName] = colId;
            TTableInfo::TColumn& column = alterData->Columns[colId];
            column = TTableInfo::TColumn(colName, colId, type->GetTypeId());
            column.Family = columnFamily ? columnFamily->GetId() : 0;
            column.NotNull = col.GetNotNull();
            if (source)
                column.CreateVersion = alterData->AlterVersion;
            if (col.HasDefaultFromSequence()) {
                column.DefaultKind = ETableColumnDefaultKind::FromSequence;
                column.DefaultValue = col.GetDefaultFromSequence();
            }
        }
    }

    for (const auto& c : alterData->Columns) {
        if (c.second.Family != 0 && keys.contains(c.second.Id)) {
            errStr = Sprintf("Key column '%s' must belong to the default family", c.second.Name.data());
            return nullptr;
        }
    }

    if (source) {
        for (const auto& col : op.GetDropColumns()) {
            TString colName = col.GetName();
            auto it = colName2Id.find(colName);
            if (it == colName2Id.end()) {
                errStr = Sprintf("Can't drop unknown column: '%s'", colName.data());
                return nullptr;
            }

            ui32 colId = it->second;
            if (source->Columns.find(colId) == source->Columns.end()) {
                errStr = Sprintf("Add + drop same column: '%s'", colName.data());
                return nullptr;
            }
            if (keys.find(colId) != keys.end()) {
                errStr = Sprintf("Can't drop key column: '%s'", colName.data());
                return nullptr;
            }
            if (alterData->Columns[colId].DeleteVersion == alterData->AlterVersion) {
                errStr = Sprintf("Duplicate drop column: %s", colName.data());
                return nullptr;
            }
            if (source->TTLSettings().HasEnabled() && source->TTLSettings().GetEnabled().GetColumnName() == colName) {
                errStr = Sprintf("Can't drop TTL column: '%s', disable TTL first ", colName.data());
                return nullptr;
            }

            alterData->Columns[colId] = source->Columns[colId];
            alterData->Columns[colId].DeleteVersion = alterData->AlterVersion;
        }
    }

    if ((colName2Id.size() - op.DropColumnsSize()) > limits.MaxTableColumns) {
        errStr = TStringBuilder()
            << "Too many columns"
            << ": current: " << (source ? source->Columns.size() : 0)
            << ", new: " << (source ? colName2Id.size() - source->Columns.size() : op.ColumnsSize())
            << ", dropping: " << op.DropColumnsSize()
            << ". Limit: " << limits.MaxTableColumns;
        return nullptr;
    }

    if (op.HasTTLSettings()) {
        const auto& ttl = op.GetTTLSettings();

        if (!ValidateTtlSettings(ttl, source ? source->Columns : THashMap<ui32, TColumn>(), alterData->Columns, colName2Id, subDomain, errStr)) {
            return nullptr;
        }

        alterData->TableDescriptionFull->MutableTTLSettings()->CopyFrom(ttl);
    }

    alterData->IsBackup = op.GetIsBackup();

    if (source && op.KeyColumnNamesSize() == 0)
        return alterData;

    TVector<ui32>& keyColIds = alterData->KeyColumnIds;
    ui32 keyOrder = 0;
    for (auto& keyName : op.GetKeyColumnNames()) {
        if (!colName2Id.contains(keyName)) {
            errStr = Sprintf("Unknown column '%s' specified in key column list", keyName.data());
            return nullptr;
        }

        ui32 colId = colName2Id[keyName];
        TTableInfo::TColumn& column = alterData->Columns[colId];
        if (column.KeyOrder != (ui32)-1) {
            errStr = Sprintf("Column '%s' specified more than once in key column list", keyName.data());
            return nullptr;
        }
        if (source && colId < source->NextColumnId && !keys.contains(colId)) {
            errStr = Sprintf("Cannot add existing column '%s' to key", keyName.data());
            return nullptr;
        }
        if (column.Family != 0) {
            errStr = Sprintf("Key column '%s' must belong to the default family", keyName.data());
            return nullptr;
        }
        column.KeyOrder = keyOrder;
        keyColIds.push_back(colId);
        ++keyOrder;
    }

    if (keyColIds.size() > limits.MaxTableKeyColumns) {
        errStr = TStringBuilder()
            << "Too many key columns"
            << ": current: " << (source ? source->KeyColumnIds.size() : 0)
            << ", new: " << keyColIds.size()
            << ". Limit: " << limits.MaxTableKeyColumns;
        return nullptr;
    }

    if (source) {
        // key columns reorder or deletion is not supported
        const TVector<ui32>& oldColIds = source->KeyColumnIds;
        if (keyColIds.size() < oldColIds.size()) {
            errStr = Sprintf("Can't remove key column");
            return nullptr;
        }
        for (ui32 i = 0; i < oldColIds.size(); ++i) {
            if (oldColIds[i] != keyColIds[i]) {
                errStr = Sprintf("Can't remove/reorder key columns");
                return nullptr;
            }
        }
    }

    return alterData;
}

void TTableInfo::ResetDescriptionCache() {
    TableDescription.ClearId_Deprecated();
    TableDescription.ClearPathId();
    TableDescription.ClearName();
    TableDescription.ClearColumns();
    TableDescription.ClearKeyColumnIds();
}

TVector<ui32> TTableInfo::FillDescriptionCache(TPathElement::TPtr pathInfo) {
    Y_VERIFY(pathInfo && pathInfo->IsTable());

    TVector<ui32> keyColumnIds;
    for (auto& col : Columns) {
        ui32 colId = col.second.Id;
        ui32 keyOrder = col.second.KeyOrder;
        if (keyOrder != (ui32)-1) {
            keyColumnIds.resize(std::max<ui32>(keyColumnIds.size(), keyOrder+1));
            keyColumnIds[keyOrder] = colId;
        }
    }

    if (!TableDescription.HasPathId()) {
        TableDescription.SetName(pathInfo->Name);
        TableDescription.SetId_Deprecated(pathInfo->PathId.LocalPathId);
        PathIdFromPathId(pathInfo->PathId, TableDescription.MutablePathId());

        for (auto& c : Columns) {
            const TColumn& column = c.second;
            if (column.IsDropped()) {
                continue;
            }
            auto colDescr = TableDescription.AddColumns();
            colDescr->SetName(column.Name);
            colDescr->SetId(column.Id);
            colDescr->SetTypeId(column.PType);
            colDescr->SetFamily(column.Family);
        }
        for (auto ci : keyColumnIds) {
            TableDescription.AddKeyColumnIds(ci);
        }
    }

    return keyColumnIds;
}

namespace {
template<class TProto, class TGetId, class TPreferred>
inline THashMap<ui32, size_t> DeduplicateRepeatedById(
    google::protobuf::RepeatedPtrField<TProto>* items,
    const TGetId& getId,
    const TPreferred& preferred)
{
    Y_VERIFY(items, "Unexpected nullptr items");

    int size = items->size();
    THashMap<ui32, size_t> posById;

    if (size > 0) {
        posById[getId(items->Get(0))] = 0;
    }

    if (size > 1) {
        // For each item find the correct position
        // We want items sorted in their first-seen order
        // But each position must be filled with a preferred item
        int dst = 1;
        for (int src = 1; src < size; ++src) {
            ui32 id = getId(items->Get(src));
            auto it = posById.find(id);
            if (it != posById.end()) {
                int existing = it->second;
                if (preferred(items->Get(existing), items->Get(src))) {
                    items->SwapElements(existing, src);
                }
            } else {
                if (dst != src) {
                    items->SwapElements(dst, src);
                }
                posById[id] = dst++;
            }
        }

        if (dst < size) {
            items->Truncate(dst);
        }
    }

    return posById;
}
}

NKikimrSchemeOp::TPartitionConfig TPartitionConfigMerger::DefaultConfig(const TAppData* appData) {
    NKikimrSchemeOp::TPartitionConfig cfg;

    TIntrusiveConstPtr<NLocalDb::TCompactionPolicy> compactionPolicy = appData->DomainsInfo->GetDefaultUserTablePolicy();
    compactionPolicy->Serialize(*cfg.MutableCompactionPolicy());

    return cfg;
}

bool TPartitionConfigMerger::ApplyChanges(
    NKikimrSchemeOp::TPartitionConfig &result,
    const NKikimrSchemeOp::TPartitionConfig &src, const NKikimrSchemeOp::TPartitionConfig &changes,
    const TAppData *appData, TString &errDesr)
{
    result.CopyFrom(src); // inherit all data from src

    if (!ApplyChangesInColumnFamilies(result, src, changes, errDesr)) {
        return false;
    }

    if (changes.StorageRoomsSize()) {
        errDesr = TStringBuilder()
            << "StorageRooms should not be present in request.";
        return false;
    }

    if (changes.HasFreezeState()) {
        if (changes.GetFreezeState() == NKikimrSchemeOp::EFreezeState::Unspecified) {
            errDesr = TStringBuilder() << "Unexpected freeze state";
            return false;
        }
        TVector<const NProtoBuf::FieldDescriptor*> fields;
        auto reflection = changes.GetReflection();
        reflection->ListFields(changes, &fields);
        if (fields.size() > 1) {
            errDesr = TStringBuilder()
                << "Mix freeze cmd with other options is forbiden";
            return false;
        }
    }

    if (changes.HasCompactionPolicy()) {
        result.MutableCompactionPolicy()->Clear();
        result.MutableCompactionPolicy()->CopyFrom(changes.GetCompactionPolicy());
    }

    if (changes.HasNamedCompactionPolicy()) {
        auto policyName = changes.GetNamedCompactionPolicy();
        if (policyName.empty()) {
            errDesr = "Empty compaction policy name, either set name or don't fill the section NamedCompactionPolicy";
            return false;
        }

        if (!appData->DomainsInfo->NamedCompactionPolicies.contains(policyName)) {
            errDesr = TStringBuilder() << "Invalid compaction policy name: " <<  policyName;
            return false;
        }

        auto policyPtr = appData->DomainsInfo->NamedCompactionPolicies.at(policyName);
        result.MutableCompactionPolicy()->Clear();
        policyPtr->Serialize(*result.MutableCompactionPolicy());
    }


    if (changes.FollowerGroupsSize()) {
        // use FollowerGroups
        if (result.HasFollowerCount()) {
            // migration into FollowerCount -> HasFollowerGroup
            // just abandon FollowerCount
            result.ClearFollowerCount();
        }

        if (result.HasCrossDataCenterFollowerCount()) {
            // migration into CrossDataCenterFollowerCount -> HasFollowerGroup
            // just abandon CrossDataCenterFollowerCount
            result.ClearCrossDataCenterFollowerCount();
        }

        if (changes.HasAllowFollowerPromotion()) {
            // migration into AllowFollowerPromotion -> HasFollowerGroup
            // just abandon AllowFollowerPromotion
            result.ClearAllowFollowerPromotion();
        }

        // here is the right place to compare and check src->FollowerGroups and changes->FollowerGroup for wise update
        result.MutableFollowerGroups()->CopyFrom(changes.GetFollowerGroups());
    }

    if (changes.HasCrossDataCenterFollowerCount()) {
        if (result.FollowerGroupsSize()) {
            errDesr = TStringBuilder() << "Forbidded downgrade from FollowerGroup option to the HasCrossDataCenterFollowerCount option";
            return false;
        }

        if (result.HasFollowerCount()) {
            // migration into FollowerCount -> CrossDataCenterFollowerCount
            // just abandon FollowerCount
            result.ClearFollowerCount();
        }

        result.SetCrossDataCenterFollowerCount(changes.GetCrossDataCenterFollowerCount());
    }

    if (changes.HasFollowerCount()) {
        if (result.HasCrossDataCenterFollowerCount()) {
            errDesr = TStringBuilder() << "Forbidded downgrade from CrossDataCenterFollowerCount option to the FollowerGroup option";
            return false;
        }

        if (result.FollowerGroupsSize()) {
            errDesr = TStringBuilder() << "Forbidded downgrade from FollowerGroup option to the FollowerGroup option";
            return false;
        }

        result.SetFollowerCount(changes.GetFollowerCount());
    }

    if (changes.HasAllowFollowerPromotion()) {
        if (result.FollowerGroupsSize()) {
            errDesr = TStringBuilder() << "Forbidded downgrade from FollowerGroup option to the AllowFollowerPromotion option";
            return false;
        }

        result.SetAllowFollowerPromotion(changes.GetAllowFollowerPromotion());
    }

    if (changes.HasExecutorCacheSize()) {
        result.SetExecutorCacheSize(changes.GetExecutorCacheSize());
    }

    if (changes.HasResourceProfile()) {
        result.SetResourceProfile(changes.GetResourceProfile());
    }

    if (changes.HasChannelProfileId()) {
        result.SetChannelProfileId(changes.GetChannelProfileId());
    }

    if (changes.HasTxReadSizeLimit()) {
        result.SetTxReadSizeLimit(changes.GetTxReadSizeLimit());
    }

    if (changes.HasDisableStatisticsCalculation()) {
        result.SetDisableStatisticsCalculation(changes.GetDisableStatisticsCalculation());
    }

    if (changes.HasPartitioningPolicy()) {
        auto& cfgPolicy = *result.MutablePartitioningPolicy();
        auto& changesPolicy = changes.GetPartitioningPolicy();

        if (changesPolicy.HasSizeToSplit()) {
            cfgPolicy.SetSizeToSplit(changesPolicy.GetSizeToSplit());
        }

        if (changesPolicy.HasMinPartitionsCount()) {
            cfgPolicy.SetMinPartitionsCount(changesPolicy.GetMinPartitionsCount());
        }

        if (changesPolicy.HasMaxPartitionsCount()) {
            cfgPolicy.SetMaxPartitionsCount(changesPolicy.GetMaxPartitionsCount());
        }

        if (changesPolicy.HasFastSplitSettings()) {
            cfgPolicy.MutableFastSplitSettings()->CopyFrom(changesPolicy.GetFastSplitSettings());
        }

        if (changesPolicy.HasSplitByLoadSettings()) {
            cfgPolicy.MutableSplitByLoadSettings()->CopyFrom(changesPolicy.GetSplitByLoadSettings());
        }
    }

    if (changes.HasPipelineConfig()) {
        result.MutablePipelineConfig()->CopyFrom(changes.GetPipelineConfig());
    }

    if (changes.HasEnableFilterByKey()) {
        result.SetEnableFilterByKey(changes.GetEnableFilterByKey());
    }

    if (changes.HasExecutorFastLogPolicy()) {
        result.SetExecutorFastLogPolicy(changes.GetExecutorFastLogPolicy());
    }

    if (changes.HasEnableEraseCache()) {
        result.SetEnableEraseCache(changes.GetEnableEraseCache());
    }

    if (changes.HasEraseCacheMinRows()) {
        result.SetEraseCacheMinRows(changes.GetEraseCacheMinRows());
    }

    if (changes.HasEraseCacheMaxBytes()) {
        result.SetEraseCacheMaxBytes(changes.GetEraseCacheMaxBytes());
    }

    if (changes.HasFreezeState()) {
        result.SetFreezeState(changes.GetFreezeState());
    } else {
        //Do not send freeze state to DS
        result.ClearFreezeState();
    }

    if (changes.HasShadowData()) {
        result.SetShadowData(changes.GetShadowData());
    }

    if (changes.HasKeepSnapshotTimeout()) {
        result.SetKeepSnapshotTimeout(changes.GetKeepSnapshotTimeout());
    }

    return true;
}

bool TPartitionConfigMerger::ApplyChangesInColumnFamilies(
    NKikimrSchemeOp::TPartitionConfig &result,
    const NKikimrSchemeOp::TPartitionConfig &src, const NKikimrSchemeOp::TPartitionConfig &changes,
    TString &errDesr)
{
    result.MutableColumnFamilies()->CopyFrom(src.GetColumnFamilies());
    TColumnFamiliesMerger merger(result);

    THashSet<ui32> changedCFamilies;

    for (const auto& changesFamily : changes.GetColumnFamilies()) {
        NKikimrSchemeOp::TFamilyDescription* cFamilyPtr = nullptr;
        if (changesFamily.HasId() && changesFamily.HasName()) {
            cFamilyPtr = merger.AddOrGet(changesFamily.GetId(), changesFamily.GetName(), errDesr);
        } else if (changesFamily.HasId()) {
            cFamilyPtr = merger.AddOrGet(changesFamily.GetId(), errDesr);
        } else if (changesFamily.HasName()) {
            cFamilyPtr = merger.AddOrGet(changesFamily.GetName(), errDesr);
        } else {
            cFamilyPtr = merger.AddOrGet(0, errDesr);
        }

        if (!cFamilyPtr) {
            return false;
        }

        auto& dstFamily = *cFamilyPtr;
        const auto& familyId = dstFamily.GetId();
        const auto& familyName = dstFamily.GetName();

        if (!changedCFamilies.insert(familyId).second) {
            errDesr = TStringBuilder()
                << "Multiple changes for the same column family are not allowed. ColumnFamily id: " << familyId << " name: " << familyName;
            return false;
        }

        if (changesFamily.HasRoom() || changesFamily.HasCodec() || changesFamily.HasInMemory()) {
            errDesr = TStringBuilder()
                << "Deprecated parameters in column family. ColumnFamily id: " << familyId << " name: " << familyName;
            return false;
        }

        if (familyId != 0) {
            const bool allowColumnFamilies = (
                KIKIMR_SCHEMESHARD_ALLOW_COLUMN_FAMILIES ||
                AppData()->AllowColumnFamiliesForTest);
            if (!allowColumnFamilies) {
                errDesr = TStringBuilder()
                    << "Server support for column families is not yet available";
                return false;
            }
            if (changesFamily.HasStorageConfig()) {
                if (changesFamily.GetStorageConfig().HasDataThreshold() ||
                    changesFamily.GetStorageConfig().HasExternalThreshold() ||
                    changesFamily.GetStorageConfig().HasSysLog() ||
                    changesFamily.GetStorageConfig().HasLog() ||
                    changesFamily.GetStorageConfig().HasExternal())
                {
                    errDesr = TStringBuilder()
                        << "Unsupported StorageConfig settings found. Column Family id: " << familyId << " name: " << familyName;
                    return false;
                }
            }
            if (changesFamily.HasStorage()) {
                errDesr = TStringBuilder()
                    << "Deprecated Storage parameter in column family. ColumnFamily id: " << familyId << " name: " << familyName;
                return false;
            }
        }

        if (changesFamily.HasColumnCodec()) {
            if (changesFamily.GetColumnCodec() == NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD) {
                errDesr = TStringBuilder()
                    << "Unsupported ColumnCodec. ColumnFamily id: " << familyId << " name: " << familyName;
                return false;
            }
            dstFamily.SetColumnCodec(changesFamily.GetColumnCodec());
        }

        if (changesFamily.HasColumnCache()) {
            dstFamily.SetColumnCache(changesFamily.GetColumnCache());
        }

        if (changesFamily.HasStorage()) {
            dstFamily.SetStorage(changesFamily.GetStorage());
        }

        if (changesFamily.HasStorageConfig()) {
            const auto& srcStorage = changesFamily.GetStorageConfig();
            auto& dstStorage = *dstFamily.MutableStorageConfig();

            if (srcStorage.HasSysLog()) {
                dstStorage.MutableSysLog()->CopyFrom(srcStorage.GetSysLog());
            }

            if (srcStorage.HasLog()) {
                dstStorage.MutableLog()->CopyFrom(srcStorage.GetLog());
            }

            if (srcStorage.HasData()) {
                dstStorage.MutableData()->CopyFrom(srcStorage.GetData());
            }

            if (srcStorage.HasExternal()) {
                dstStorage.MutableExternal()->CopyFrom(srcStorage.GetExternal());
            }

            if (srcStorage.HasDataThreshold()) {
                dstStorage.SetDataThreshold(srcStorage.GetDataThreshold());
            }

            if (srcStorage.HasExternalThreshold()) {
                dstStorage.SetExternalThreshold(srcStorage.GetExternalThreshold());
            }
        }
    }

    return true;
}

THashMap<ui32, size_t> TPartitionConfigMerger::DeduplicateColumnFamiliesById(NKikimrSchemeOp::TPartitionConfig &config)
{
    return DeduplicateRepeatedById(
                config.MutableColumnFamilies(),
                [](const auto& item) { return item.GetId(); },
    [](const auto& left, const auto& right) -> bool {
        if (left.HasStorageConfig() && !right.HasStorageConfig()) {
            // Dropping storage config is not allowed
            return false;
        }
        if (left.HasId() && !right.HasId()) {
            // New item without id is suspicious
            return false;
        }
        // Prefer the right element
        return true;
    });
}

THashMap<ui32, size_t> TPartitionConfigMerger::DeduplicateStorageRoomsById(NKikimrSchemeOp::TPartitionConfig &config)
{
    return DeduplicateRepeatedById(
                config.MutableStorageRooms(),
                [](const auto& item) { return item.GetRoomId(); },
    [](const auto& left, const auto& right) -> bool {
        Y_UNUSED(left);
        Y_UNUSED(right);
        // Always prefer the right element
        return true;
    });
}

NKikimrSchemeOp::TFamilyDescription &TPartitionConfigMerger::MutableColumnFamilyById(
    NKikimrSchemeOp::TPartitionConfig &partitionConfig,
    THashMap<ui32, size_t> &posById, ui32 familyId)
{
    auto it = posById.find(familyId);
    if (it != posById.end()) {
        return *partitionConfig.MutableColumnFamilies(it->second);
    }

    auto& family = *partitionConfig.AddColumnFamilies();
    posById[familyId] = partitionConfig.ColumnFamiliesSize() - 1;
    family.SetId(familyId);
    return family;
}

bool TPartitionConfigMerger::VerifyCreateParams(
    const NKikimrSchemeOp::TPartitionConfig &config,
    const TAppData *appData, const bool shadowDataAllowed, TString &errDescr)
{
    if (config.HasShadowData()) {
        if (!shadowDataAllowed) {
            errDescr = TStringBuilder() << "Setting ShadowData is prohibited";
            return false;
        }
    }

    if (!config.HasCompactionPolicy()) {
        errDescr = TStringBuilder() << "CompactionPolicy should be set";
        return false;
    }

    if (config.HasChannelProfileId()) {
        ui32 channelProfile = config.GetChannelProfileId();
        if (channelProfile >= appData->ChannelProfiles->Profiles.size()) {
            errDescr = TStringBuilder()
                    << "Unknown profileId " << channelProfile
                    << ", should be in [0 .. " << appData->ChannelProfiles->Profiles.size() - 1 << "]";
            return false;
        }
    }

    if (config.HasFollowerCount()) {
        ui32 count = config.GetFollowerCount();
        if (count > MaxFollowersCount) {
            errDescr = TStringBuilder()
                    << "Too much followers: " << count;
            return false;
        }
    }

    if (config.HasCrossDataCenterFollowerCount()) {
        ui32 count = config.GetCrossDataCenterFollowerCount();
        if (count > MaxFollowersCount) {
            errDescr = TStringBuilder()
                    << "Too much followers: " << count;
            return false;
        }
    }

    for (const auto& followerGroup: config.GetFollowerGroups()) {
        if (followerGroup.HasFollowerCount()) {
            if (followerGroup.GetFollowerCount() > MaxFollowersCount) {
                errDescr = TStringBuilder()
                        << "FollowerGroup: Too much followers: " << followerGroup.GetFollowerCount();
                return false;
            }
        }

        if (followerGroup.HasAllowClientRead()) {
            errDescr = TStringBuilder()
                    << "FollowerGroup: AllowClientRead is enabled, but hasn't been tested";
            return false;
        }

        if (followerGroup.AllowedNodeIDsSize()) {
            errDescr = TStringBuilder()
                    << "FollowerGroup: AllowedNodeIDs is enabled, but hasn't been tested";
            return false;
        }

        if (followerGroup.AllowedDataCenterNumIDsSize() || followerGroup.AllowedDataCentersSize()) {
            errDescr = TStringBuilder()
                    << "FollowerGroup: AllowedDataCenterIDs is enabled, hasn't been tested";
            return false;
        }

        if (followerGroup.HasLocalNodeOnly()) {
            errDescr = TStringBuilder()
                    << "FollowerGroup: LocalNodeOnly is enabled, but hasn't been tested";
            return false;
        }

        if (followerGroup.HasRequireDifferentNodes()) {
            errDescr = TStringBuilder()
                    << "FollowerGroup: LocalNodeOnly is enabled, but hasn't been tested";
            return false;
        }
    }

    if (config.HasFollowerCount() + config.HasCrossDataCenterFollowerCount() + (config.FollowerGroupsSize() > 0) > 1) {
        errDescr = TStringBuilder()
                << "PartitionConfig: FollowerCount, CrossDataCenterFollowerCount and FollowerGroup are mutually exclusive.";
        return false;
    }

    if (config.FollowerGroupsSize() > 1) {
        errDescr = TStringBuilder()
                << "FollowerGroup: only one follower group is allowed for now";
        return false;
    }

    bool hasStorageConfig = false;
    bool hasAuxilaryFamilies = false;

    for (const auto& family : config.GetColumnFamilies()) {
        ui32 fId = family.GetId();
        if (fId == 0) {
            hasStorageConfig = family.HasStorageConfig();
        } else {
            hasAuxilaryFamilies = true;
        }
    }

    if (hasAuxilaryFamilies && !hasStorageConfig) {
        errDescr = TStringBuilder()
                << "Column families require StorageConfig specification";
        return false;
    }

    for (const auto& family : config.GetColumnFamilies()) {
        ui32 fId = family.GetId();
        auto fName = family.GetName();
        if (fId == 0) {
            if (fName != "" && fName != "default") {
                errDescr = TStringBuilder()
                    << "Column family with id " << fId << " has to be named as default or be empty";
            }
        } else {
            if (fName == "default") {
                errDescr = TStringBuilder()
                    << "Column family with id " << fId << " has name default"
                    << ", name default is reseved for family with id 0";
            }
        }
    }

    if (!VerifyCompactionPolicy(config.GetCompactionPolicy(), errDescr)) {
        return false;
    }

    return true;
}

bool IsEquivalent(
    const NKikimrSchemeOp::TStorageSettings& left,
    const NKikimrSchemeOp::TStorageSettings& right)
{
    return left.HasPreferredPoolKind() == right.HasPreferredPoolKind()
        && left.GetPreferredPoolKind() == right.GetPreferredPoolKind()
        && left.HasAllowOtherKinds() == right.HasAllowOtherKinds()
        && left.GetAllowOtherKinds() == right.GetAllowOtherKinds();
}

bool TPartitionConfigMerger::VerifyAlterParams(
    const NKikimrSchemeOp::TPartitionConfig &srcConfig,
    const NKikimrSchemeOp::TPartitionConfig &dstConfig,
    const TAppData *appData, const bool shadowDataAllowed, TString &errDescr)
{
    if (!VerifyCreateParams(dstConfig, appData, shadowDataAllowed, errDescr)) {
        return false;
    }


    if (dstConfig.GetShadowData() && !srcConfig.GetShadowData()) {
        errDescr = TStringBuilder() << "Cannot enable ShadowData after table is created";
        return false;
    }

    if (dstConfig.HasChannelProfileId()) {
        for (const auto& family : dstConfig.GetColumnFamilies()) {
            if (family.HasStorageConfig()) {
                errDescr = TStringBuilder()
                        << "Migration from profile id by storage config is not allowed, was "
                        << srcConfig.GetChannelProfileId() << ", asks storage config";
                return false;
            }
        }

        if (srcConfig.GetChannelProfileId() != dstConfig.GetChannelProfileId()) {
            errDescr = TStringBuilder()
                    << "Profile modification is not allowed, was "
                    << srcConfig.GetChannelProfileId()
                    << ", asks "
                    <<  dstConfig.GetChannelProfileId();
            return false;
        }
    }

    const NKikimrSchemeOp::TStorageConfig* wasStorageConfig = nullptr;
    for (const auto& family : srcConfig.GetColumnFamilies()) {
        if (family.GetId() == 0 && family.HasStorageConfig()) {
            wasStorageConfig = &family.GetStorageConfig();
            break;
        }
    }

    const NKikimrSchemeOp::TStorageConfig* isStorageConfig = nullptr;
    for (const auto& family : dstConfig.GetColumnFamilies()) {
        if (family.GetId() == 0 && family.HasStorageConfig()) {
            isStorageConfig = &family.GetStorageConfig();
            break;
        }
    }

    if (wasStorageConfig) {
        Y_VERIFY(isStorageConfig); // by inherit logic

        auto& srcStorage = *wasStorageConfig;
        auto& cfgStorage = *isStorageConfig;

        // SysLog and Log cannot be reassigned
        if (srcStorage.HasSysLog() != cfgStorage.HasSysLog() ||
                srcStorage.HasLog() != cfgStorage.HasLog() ||
                !IsEquivalent(srcStorage.GetSysLog(), cfgStorage.GetSysLog()) ||
                !IsEquivalent(srcStorage.GetLog(), cfgStorage.GetLog()))
        {
            errDescr = TStringBuilder()
                    << "Incompatible alter of storage config in default column family denied."
                    << " Data either missing or different in request."
                    << " Was '" << srcStorage.ShortDebugString()
                    << "', in request '" << cfgStorage.ShortDebugString() << "'";
            return false;
        }

        if (!KIKIMR_SCHEMESHARD_ALLOW_COLUMN_FAMILIES && !AppData()->AllowColumnFamiliesForTest) {
            // When feature flag is not enabled we don't allow changes to data channels
            // This is so we stay compatible without surprises during migration periods
            if (srcStorage.HasData() != cfgStorage.HasData() ||
                    srcStorage.HasExternal() != cfgStorage.HasExternal() ||
                    !IsEquivalent(srcStorage.GetData(), cfgStorage.GetData()) ||
                    !IsEquivalent(srcStorage.GetExternal(), cfgStorage.GetExternal()))
            {
                errDescr = TStringBuilder()
                        << "Changing column family storage is currently disabled on the server."
                        << " Was '" << srcStorage.ShortDebugString()
                        << "', in request '" << cfgStorage.ShortDebugString() << "'";
                return false;
            }
        }
    }

    if (isStorageConfig) {
        if (!wasStorageConfig) {
            errDescr = TStringBuilder()
                    << "Couldn't add storage configuration if it hasn't been set before";
            return false;
        }
    }

    // Verify auxilary families
    bool srcFamiliesBuilt = false;
    THashMap<ui32, const NKikimrSchemeOp::TFamilyDescription*> srcFamilies;
    for (const auto& family : dstConfig.GetColumnFamilies()) {
        if (family.GetId() == 0) {
            // Primary family has already been checked above
            continue;
        }

        if (!srcFamiliesBuilt) {
            for (const auto& srcFamily : srcConfig.GetColumnFamilies()) {
                if (srcFamily.GetId() != 0) {
                    srcFamilies[srcFamily.GetId()] = &srcFamily;
                }
            }
            srcFamiliesBuilt = true;
        }

        const auto* srcFamily = srcFamilies.Value(family.GetId(), nullptr);

        if (srcFamily && srcFamily->HasStorageConfig()) {
            Y_VERIFY(family.HasStorageConfig()); // by inherit logic

            const auto& srcStorage = srcFamily->GetStorageConfig();
            const auto& dstStorage = family.GetStorageConfig();

            // Data may be freely changed, however unsupported settings cannot be handled
            if (srcStorage.HasSysLog() != dstStorage.HasSysLog() ||
                    srcStorage.HasLog() != dstStorage.HasLog() ||
                    srcStorage.HasExternal() != dstStorage.HasExternal())
            {
                errDescr = TStringBuilder()
                        << "Incompatible alter of storage config in column family " << family.GetId() << " denied."
                        << " Was '" << srcStorage.ShortDebugString()
                        << "', requested '" << dstStorage.ShortDebugString() << "'";
                return false;
            }

            if (!KIKIMR_SCHEMESHARD_ALLOW_COLUMN_FAMILIES && !AppData()->AllowColumnFamiliesForTest) {
                // When feature flag is not enabled we don't allow changes to data channels
                // This is so we stay compatible without surprises during migration periods
                if (srcStorage.HasData() != dstStorage.HasData() ||
                        !IsEquivalent(srcStorage.GetData(), dstStorage.GetData()))
                {
                    errDescr = TStringBuilder()
                            << "Changing column family storage is currently disabled on the server."
                            << " Was '" << srcStorage.ShortDebugString()
                            << "', in request '" << dstStorage.ShortDebugString() << "'";
                    return false;
                }
            }

            continue;
        }

        if (!KIKIMR_SCHEMESHARD_ALLOW_COLUMN_FAMILIES && !AppData()->AllowColumnFamiliesForTest) {
            // When feature flag is not enabled we don't allow adding StorageConfig
            if (family.HasStorageConfig() && (!srcFamily || !srcFamily->HasStorageConfig())) {
                errDescr = TStringBuilder()
                        << "Adding column family storage is currently disabled on the server.";
                return false;
            }
        }
    }

    if (dstConfig.HasCompactionPolicy()) {
        if (!VerifyCompactionPolicy(dstConfig.GetCompactionPolicy(), errDescr)) {
            return false;
        }
        NLocalDb::TCompactionPolicy oldPolicy(srcConfig.GetCompactionPolicy());
        NLocalDb::TCompactionPolicy newPolicy(dstConfig.GetCompactionPolicy());
        if (!NLocalDb::ValidateCompactionPolicyChange(oldPolicy, newPolicy, errDescr)) {
            return false;
        }
    }

    if (!VerifyCommandOnFrozenTable(srcConfig, dstConfig)) {
        errDescr = TStringBuilder() <<
                                       "Table is frozen. Only unfreeze alter is allowed";
        return false;
    }

    return true;
}

bool TPartitionConfigMerger::VerifyCompactionPolicy(const NKikimrSchemeOp::TCompactionPolicy &policy, TString &err)
{
    if (policy.HasCompactionStrategy()) {
        switch (policy.GetCompactionStrategy()) {
        case NKikimrSchemeOp::CompactionStrategyUnset:
        case NKikimrSchemeOp::CompactionStrategyGenerational:
            break;
        case NKikimrSchemeOp::CompactionStrategySharded:
            if (!KIKIMR_ALLOW_SHARDED_COMPACTION) {
                err = TStringBuilder()
                        << "Unsupported compaction strategy.";
                return false;
            }
            break;
        default:
            err = TStringBuilder()
                    << "Unsupported compaction strategy.";
            return false;
        }
    }

    return true;
}

bool TPartitionConfigMerger::VerifyCommandOnFrozenTable(const NKikimrSchemeOp::TPartitionConfig &srcConfig, const NKikimrSchemeOp::TPartitionConfig &dstConfig)
{
    if (srcConfig.HasFreezeState() &&
            srcConfig.GetFreezeState() == NKikimrSchemeOp::EFreezeState::Freeze) {
        if (dstConfig.HasFreezeState() &&
                dstConfig.GetFreezeState() == NKikimrSchemeOp::EFreezeState::Unfreeze) {
            // Only unfreeze cmd is allowd
            return true;
        }
        return false;
    }
    return true;
}

void TTableInfo::FinishAlter() {
    Y_VERIFY(AlterData, "No alter data at Alter complete");
    AlterVersion = AlterData->AlterVersion;
    NextColumnId = AlterData->NextColumnId;
    for (const auto& col : AlterData->Columns) {
        TColumn * oldCol = Columns.FindPtr(col.first);
        if (oldCol) {
            //oldCol->CreateVersion = col.second.CreateVersion;
            oldCol->DeleteVersion = col.second.DeleteVersion;
            oldCol->Family = col.second.Family;
        } else {
            Columns[col.first] = col.second;
            if (col.second.KeyOrder != (ui32)-1) {
                KeyColumnIds.resize(Max<ui32>(KeyColumnIds.size(), col.second.KeyOrder + 1));
                KeyColumnIds[col.second.KeyOrder] = col.first;
            }
        }
    }

    // Apply partition config changes
    auto& partitionConfig = MutablePartitionConfig();
    if (AlterData->IsFullPartitionConfig()) {
        auto& newConfig = AlterData->PartitionConfigFull();
        partitionConfig.Swap(&newConfig);
    } else {
        // Copy pase from 18-6
        // Apply partition config changes
        const auto& newConfig = AlterData->PartitionConfigDiff();
        if (newConfig.HasExecutorCacheSize()) {
            partitionConfig.SetExecutorCacheSize(newConfig.GetExecutorCacheSize());
        }
        if (newConfig.HasTxReadSizeLimit()) {
            partitionConfig.SetTxReadSizeLimit(newConfig.GetTxReadSizeLimit());
        }
        if (newConfig.HasDisableStatisticsCalculation()) {
            partitionConfig.SetDisableStatisticsCalculation(newConfig.GetDisableStatisticsCalculation());
        }
        if (newConfig.HasCompactionPolicy()) {
            partitionConfig.MutableCompactionPolicy()->CopyFrom(newConfig.GetCompactionPolicy());
        }
        if (newConfig.HasPartitioningPolicy()) {
            partitionConfig.MutablePartitioningPolicy()->CopyFrom(newConfig.GetPartitioningPolicy());
        }
        if (newConfig.HasPipelineConfig()) {
            partitionConfig.MutablePipelineConfig()->CopyFrom(newConfig.GetPipelineConfig());
        }
        if (newConfig.HasFollowerCount()) {
            partitionConfig.SetFollowerCount(newConfig.GetFollowerCount());
        }
        if (newConfig.HasAllowFollowerPromotion()) {
            partitionConfig.SetAllowFollowerPromotion(newConfig.GetAllowFollowerPromotion());
        }
        if (newConfig.HasCrossDataCenterFollowerCount()) {
            partitionConfig.SetCrossDataCenterFollowerCount(newConfig.GetCrossDataCenterFollowerCount());
        }
        if (newConfig.HasEnableFilterByKey()) {
            partitionConfig.SetEnableFilterByKey(newConfig.GetEnableFilterByKey());
        }
        if (newConfig.HasExecutorFastLogPolicy()) {
            partitionConfig.SetExecutorFastLogPolicy(newConfig.GetExecutorFastLogPolicy());
        }
        if (newConfig.HasEnableEraseCache()) {
            partitionConfig.SetEnableEraseCache(newConfig.GetEnableEraseCache());
        }
        if (newConfig.HasEraseCacheMinRows()) {
            partitionConfig.SetEraseCacheMinRows(newConfig.GetEraseCacheMinRows());
        }
        if (newConfig.HasEraseCacheMaxBytes()) {
            partitionConfig.SetEraseCacheMaxBytes(newConfig.GetEraseCacheMaxBytes());
        }
        if (newConfig.ColumnFamiliesSize()) {
            // N.B. there is no deduplication, assumes legacy data
            partitionConfig.ClearColumnFamilies();
            partitionConfig.AddColumnFamilies()->CopyFrom(*newConfig.GetColumnFamilies().rbegin());
        }
        if (newConfig.HasKeepSnapshotTimeout()) {
            partitionConfig.SetKeepSnapshotTimeout(newConfig.GetKeepSnapshotTimeout());
        }
    }

    // Avoid ShadowData==false in the resulting config
    if (partitionConfig.HasShadowData() && !partitionConfig.GetShadowData()) {
        partitionConfig.ClearShadowData();
    }

    // Apply TTL params
    if (AlterData->TableDescriptionFull.Defined() && AlterData->TableDescriptionFull->HasTTLSettings()) {
        MutableTTLSettings().Swap(AlterData->TableDescriptionFull->MutableTTLSettings());
    }

    // Force FillDescription to regenerate TableDescription
    ResetDescriptionCache();

    AlterData.Reset();
}

#if 1 // legacy
TString TTableInfo::SerializeAlterExtraData() const {
    Y_VERIFY(AlterData);
    NKikimrSchemeOp::TAlterExtraData alterExtraData;
    alterExtraData.MutablePartitionConfig()->CopyFrom(AlterData->PartitionConfigDiff());
    TString str;
    bool serializeRes = alterExtraData.SerializeToString(&str);
    Y_VERIFY(serializeRes);
    return str;
}

void TTableInfo::DeserializeAlterExtraData(const TString& str) {
    Y_VERIFY(AlterData);
    NKikimrSchemeOp::TAlterExtraData alterExtraData;
    bool deserializeRes = ParseFromStringNoSizeLimit(alterExtraData, str);
    Y_VERIFY(deserializeRes);
    AlterData->PartitionConfigDiff().Swap(alterExtraData.MutablePartitionConfig());
}
#endif

void TTableInfo::SetPartitioning(TVector<TTableShardInfo>&& newPartitioning) {
    THashMap<TShardIdx, TPartitionStats> newPartitionStats;
    TPartitionStats newAggregatedStats;
    newAggregatedStats.PartCount = newPartitioning.size();
    ui64 cpuTotal = 0;
    for (const auto& np : newPartitioning) {
        auto idx = np.ShardIdx;
        auto& newStats(newPartitionStats[idx]);
        newStats = Stats.PartitionStats.contains(idx) ? Stats.PartitionStats[idx] : TPartitionStats();
        newAggregatedStats.RowCount += newStats.RowCount;
        newAggregatedStats.DataSize += newStats.DataSize;
        newAggregatedStats.IndexSize += newStats.IndexSize;
        cpuTotal += newStats.GetCurrentRawCpuUsage();
        newAggregatedStats.Memory += newStats.Memory;
        newAggregatedStats.Network += newStats.Network;
        newAggregatedStats.Storage += newStats.Storage;
        newAggregatedStats.ReadThroughput += newStats.ReadThroughput;
        newAggregatedStats.WriteThroughput += newStats.WriteThroughput;
        newAggregatedStats.ReadIops += newStats.ReadIops;
        newAggregatedStats.WriteIops += newStats.WriteIops;
    }
    newAggregatedStats.SetCurrentRawCpuUsage(cpuTotal, AppData()->TimeProvider->Now());
    newAggregatedStats.LastAccessTime = Stats.Aggregated.LastAccessTime;
    newAggregatedStats.LastUpdateTime = Stats.Aggregated.LastUpdateTime;

    newAggregatedStats.ImmediateTxCompleted = Stats.Aggregated.ImmediateTxCompleted;
    newAggregatedStats.PlannedTxCompleted = Stats.Aggregated.PlannedTxCompleted;
    newAggregatedStats.TxRejectedByOverload = Stats.Aggregated.TxRejectedByOverload;
    newAggregatedStats.TxRejectedBySpace = Stats.Aggregated.TxRejectedBySpace;

    newAggregatedStats.RowUpdates = Stats.Aggregated.RowUpdates;
    newAggregatedStats.RowDeletes = Stats.Aggregated.RowDeletes;
    newAggregatedStats.RowReads = Stats.Aggregated.RowReads;
    newAggregatedStats.RangeReads = Stats.Aggregated.RangeReads;
    newAggregatedStats.RangeReadRows = Stats.Aggregated.RangeReadRows;

    if (SplitOpsInFlight.empty()) {
        ExpectedPartitionCount = newPartitioning.size();
    }

    if (Partitions.empty()) {
        Y_VERIFY(SplitOpsInFlight.empty());
    }

    Stats.PartitionStats.swap(newPartitionStats);
    Stats.Aggregated = newAggregatedStats;
    Partitions.swap(newPartitioning);
    PreSerializedPathDescription.clear();

    CondEraseSchedule.clear();
    InFlightCondErase.clear();
    Shard2PartitionIdx.clear();
    for (ui32 i = 0; i < Partitions.size(); ++i) {
        Shard2PartitionIdx[Partitions[i].ShardIdx] = i;
        CondEraseSchedule.push(Partitions.begin() + i);
    }
}

void TTableInfo::UpdateShardStats(TShardIdx datashardIdx, TPartitionStats& newStats) {
    // Ignore stats from unknown datashard (it could have been split)
    if (!Stats.PartitionStats.contains(datashardIdx))
        return;

    TPartitionStats& oldStats = Stats.PartitionStats[datashardIdx];

    if (newStats.SeqNo <= oldStats.SeqNo) {
        // Ignore outdated message
        return;
    }

    if (newStats.SeqNo.Generation > oldStats.SeqNo.Generation) {
        // Reset incremental counter baselines if tablet has restarted
        oldStats.ImmediateTxCompleted = 0;
        oldStats.PlannedTxCompleted = 0;
        oldStats.TxRejectedByOverload = 0;
        oldStats.TxRejectedBySpace = 0;
        oldStats.RowUpdates = 0;
        oldStats.RowDeletes = 0;
        oldStats.RowReads = 0;
        oldStats.RangeReads = 0;
        oldStats.RangeReadRows = 0;
    }

    Stats.Aggregated.RowCount += (newStats.RowCount - oldStats.RowCount);
    Stats.Aggregated.DataSize += (newStats.DataSize - oldStats.DataSize);
    Stats.Aggregated.IndexSize += (newStats.IndexSize - oldStats.IndexSize);
    Stats.Aggregated.LastAccessTime = Max(Stats.Aggregated.LastAccessTime, newStats.LastAccessTime);
    Stats.Aggregated.LastUpdateTime = Max(Stats.Aggregated.LastUpdateTime, newStats.LastUpdateTime);
    Stats.Aggregated.ImmediateTxCompleted += (newStats.ImmediateTxCompleted - oldStats.ImmediateTxCompleted);
    Stats.Aggregated.PlannedTxCompleted += (newStats.PlannedTxCompleted - oldStats.PlannedTxCompleted);
    Stats.Aggregated.TxRejectedByOverload += (newStats.TxRejectedByOverload - oldStats.TxRejectedByOverload);
    Stats.Aggregated.TxRejectedBySpace += (newStats.TxRejectedBySpace - oldStats.TxRejectedBySpace);
    Stats.Aggregated.InFlightTxCount += (newStats.InFlightTxCount - oldStats.InFlightTxCount);

    Stats.Aggregated.RowUpdates += (newStats.RowUpdates - oldStats.RowUpdates);
    Stats.Aggregated.RowDeletes += (newStats.RowDeletes - oldStats.RowDeletes);
    Stats.Aggregated.RowReads += (newStats.RowReads - oldStats.RowReads);
    Stats.Aggregated.RangeReads += (newStats.RangeReads - oldStats.RangeReads);
    Stats.Aggregated.RangeReadRows += (newStats.RangeReadRows - oldStats.RangeReadRows);

    i64 cpuUsageDelta = newStats.GetCurrentRawCpuUsage() - oldStats.GetCurrentRawCpuUsage();
    i64 prevCpuUsage = Stats.Aggregated.GetCurrentRawCpuUsage();
    ui64 newAggregatedCpuUsage = std::max<i64>(0, prevCpuUsage + cpuUsageDelta);
    TInstant now = AppData()->TimeProvider->Now();
    Stats.Aggregated.SetCurrentRawCpuUsage(newAggregatedCpuUsage, now);
    Stats.Aggregated.Memory += (newStats.Memory - oldStats.Memory);
    Stats.Aggregated.Network += (newStats.Network - oldStats.Network);
    Stats.Aggregated.Storage += (newStats.Storage - oldStats.Storage);
    Stats.Aggregated.ReadThroughput += (newStats.ReadThroughput - oldStats.ReadThroughput);
    Stats.Aggregated.WriteThroughput += (newStats.WriteThroughput - oldStats.WriteThroughput);
    Stats.Aggregated.ReadIops += (newStats.ReadIops - oldStats.ReadIops);
    Stats.Aggregated.WriteIops += (newStats.WriteIops - oldStats.WriteIops);

    newStats.SaveCpuUsageHistory(oldStats);
    oldStats = newStats;
    Stats.PartitionStatsUpdated++;

    // Rescan stats for aggregations only once in a while
    if (Stats.PartitionStatsUpdated >= Stats.PartitionStats.size()) {
        Stats.PartitionStatsUpdated = 0;
        Stats.Aggregated.TxCompleteLag = TDuration();
        for (const auto& ps : Stats.PartitionStats) {
            Stats.Aggregated.TxCompleteLag = Max(Stats.Aggregated.TxCompleteLag, ps.second.TxCompleteLag);
        }
    }
}

void TTableInfo::RegisterSplitMegreOp(TOperationId opId, const TTxState& txState) {
    Y_VERIFY(txState.TxType == TTxState::TxSplitTablePartition || txState.TxType == TTxState::TxMergeTablePartition);
    Y_VERIFY(txState.SplitDescription);

    if (SplitOpsInFlight.empty()) {
        Y_VERIFY_S(Partitions.size() == ExpectedPartitionCount,
                   "info "
                       << "ExpectedPartitionCount: " << ExpectedPartitionCount
                       << " Partitions.size(): " << Partitions.size());
    }

    if (txState.State < TTxState::NotifyPartitioningChanged) {
        ui64 srcCount = txState.SplitDescription->SourceRangesSize();
        ui64 dstCount = txState.SplitDescription->DestinationRangesSize();
        ExpectedPartitionCount += dstCount;
        ExpectedPartitionCount -= srcCount;
    }

    Y_VERIFY(!SplitOpsInFlight.contains(opId));
    SplitOpsInFlight.emplace(opId);
    ShardsInSplitMergeByOpId.emplace(opId, TVector<TShardIdx>());

    for (const auto& shardInfo: txState.Shards) {
        ShardsInSplitMergeByShards.emplace(shardInfo.Idx, opId);
        ShardsInSplitMergeByOpId.at(opId).push_back(shardInfo.Idx);
    }
}

bool TTableInfo::IsShardInSplitMergeOp(TShardIdx idx) const {
    if (ShardsInSplitMergeByShards.contains(idx)) {
        TOperationId opId = ShardsInSplitMergeByShards.at(idx);
        Y_VERIFY(ShardsInSplitMergeByOpId.contains(opId));
        Y_VERIFY(SplitOpsInFlight.contains(opId));
    }

    return ShardsInSplitMergeByShards.contains(idx);
}


void TTableInfo::AbortSplitMergeOp(TOperationId opId) {
    Y_VERIFY(SplitOpsInFlight.contains(opId));
    for (const auto& shardIdx: ShardsInSplitMergeByOpId.at(opId)) {
        ShardsInSplitMergeByShards.erase(shardIdx);
    }
    ShardsInSplitMergeByOpId.erase(opId);
    SplitOpsInFlight.erase(opId);
}

void TTableInfo::FinishSplitMergeOp(TOperationId opId) {
    AbortSplitMergeOp(opId);

    if (SplitOpsInFlight.empty()) {
        Y_VERIFY_S(Partitions.size() == ExpectedPartitionCount,
                   "info "
                       << "ExpectedPartitionCount: " << ExpectedPartitionCount
                       << " Partitions.size(): " << Partitions.size());
    }
}



bool TTableInfo::TryAddShardToMerge(const TSplitSettings& splitSettings,
                                    const TForceShardSplitSettings& forceShardSplitSettings,
                                    TShardIdx shardIdx, TVector<TShardIdx>& shardsToMerge,
                                    THashSet<TTabletId>& partOwners, ui64& totalSize, float& totalLoad) const
{
    if (ExpectedPartitionCount + 1 - shardsToMerge.size() <= GetMinPartitionsCount()) {
        return false;
    }

    if (IsShardInSplitMergeOp(shardIdx)) {
        return false;
    }

    const TPartitionStats* stats = Stats.PartitionStats.FindPtr(shardIdx);

    if (!stats) {
        return false;
    }

    if (stats->ShardState != NKikimrTxDataShard::Ready) {
        return false;
    }

    if (stats->PartOwners.empty()) {
        return false;
    }

    // We don't want to merge shards that have borrowed non-compacted data
    if (stats->HasBorrowedData)
        return false;

    bool canMerge = false;

    // Check if we can try merging by size
    if (IsMergeBySizeEnabled(forceShardSplitSettings) && stats->DataSize + totalSize <= GetSizeToMerge(forceShardSplitSettings)) {
        canMerge = true;
    }

    // Check if we can try merging by load
    TInstant now = AppData()->TimeProvider->Now();
    TDuration minUptime = TDuration::Seconds(splitSettings.MergeByLoadMinUptimeSec);
    if (!canMerge && IsMergeByLoadEnabled() && stats->StartTime && stats->StartTime + minUptime < now) {
        canMerge = true;
    }

    if (!canMerge)
        return false;

    // Check that total size doesn't exceed the limits
    if (IsSplitBySizeEnabled(forceShardSplitSettings) && stats->DataSize + totalSize >= GetShardSizeToSplit(forceShardSplitSettings)*0.9) {
        return false;
    }

    // Check that total load doesn't exceed the limits
    float shardLoad = stats->GetCurrentRawCpuUsage() * 0.000001;
    if (IsMergeByLoadEnabled()) {
        const auto& settings = PartitionConfig().GetPartitioningPolicy().GetSplitByLoadSettings();
        i64 cpuPercentage = settings.GetCpuPercentageThreshold();
        float cpuUsageThreshold = 0.01 * (cpuPercentage ? cpuPercentage : (i64)splitSettings.FastSplitCpuPercentageThreshold);

        // Calculate shard load based on historical data
        TDuration loadDuration = TDuration::Seconds(splitSettings.MergeByLoadMinLowLoadDurationSec);
        shardLoad = 0.01 * stats->GetLatestMaxCpuUsagePercent(now - loadDuration);

        if (shardLoad + totalLoad > cpuUsageThreshold *0.7)
            return false;
    }

    // Merged shards must not have borrowed parts from the same original tablet
    // because this will break part ref-counting
    for (auto tabletId : stats->PartOwners) {
        if (partOwners.contains(tabletId))
            return false;
    }

    shardsToMerge.push_back(shardIdx);
    totalSize += stats->DataSize;
    totalLoad += shardLoad;
    partOwners.insert(stats->PartOwners.begin(), stats->PartOwners.end());

    return true;
}

bool TTableInfo::CheckCanMergePartitions(const TSplitSettings& splitSettings,
                                         const TForceShardSplitSettings& forceShardSplitSettings,
                                         TShardIdx shardIdx, TVector<TShardIdx>& shardsToMerge) const
{
    // Don't split/merge backup tables
    if (IsBackup) {
        return false;
    }

    // Ignore stats from unknown datashard (it could have been split)
    if (!Stats.PartitionStats.contains(shardIdx)) {
        return false;
    }

    if (Partitions.size() <= GetMinPartitionsCount()) {
        return false;
    }

    if (!Shard2PartitionIdx.contains(shardIdx)) {
        return false;
    }

    i64 partitionIdx = *Shard2PartitionIdx.FindPtr(shardIdx);

    shardsToMerge.clear();
    ui64 totalSize = 0;
    float totalLoad = 0;
    THashSet<TTabletId> partOwners;

    // Make sure we can actually merge current shard first
    if (!TryAddShardToMerge(splitSettings, forceShardSplitSettings, shardIdx, shardsToMerge, partOwners, totalSize, totalLoad)) {
        return false;
    }

    for (i64 pi = partitionIdx - 1; pi >= 0; --pi) {
        if (!TryAddShardToMerge(splitSettings, forceShardSplitSettings, GetPartitions()[pi].ShardIdx, shardsToMerge, partOwners, totalSize, totalLoad)) {
            break;
        }
    }
    // make shardsToMerge ordered by partition index
    Reverse(shardsToMerge.begin(), shardsToMerge.end());

    for (ui64 pi = partitionIdx + 1; pi < GetPartitions().size(); ++pi) {
        if (!TryAddShardToMerge(splitSettings, forceShardSplitSettings, GetPartitions()[pi].ShardIdx, shardsToMerge, partOwners, totalSize, totalLoad)) {
            break;
        }
    }

    return shardsToMerge.size() > 1;
}

bool TTableInfo::CheckFastSplitForPartition(const TSplitSettings& splitSettings, TShardIdx shardIdx, ui64 dataSize, ui64 rowCount) const {
    // Don't split/merge backup tables
    if (IsBackup)
        return false;

    // Ignore stats from unknown datashard (it could have been split)
    if (!Stats.PartitionStats.contains(shardIdx))
        return false;

    if (!Shard2PartitionIdx.contains(shardIdx))
        return false;

    const ui64 MIN_ROWS_FOR_FAST_SPLIT = 1000;
    ui64 sizeThreshold = splitSettings.FastSplitSizeThreshold;
    ui64 rowCountThreshold = splitSettings.FastSplitRowCountThreshold;
    float cpuUsageThreshold = 0.01 * splitSettings.FastSplitCpuPercentageThreshold;

    const auto& partitionConfig = PartitionConfig();

    if (partitionConfig.GetPartitioningPolicy().HasFastSplitSettings()) {
        const auto& settings = partitionConfig.GetPartitioningPolicy().GetFastSplitSettings();
        sizeThreshold = settings.GetSizeThreshold();
        rowCountThreshold = settings.GetRowCountThreshold();
        cpuUsageThreshold = 0.01 * settings.GetCpuPercentageThreshold();
    }

    const auto& stats = *Stats.PartitionStats.FindPtr(shardIdx);
    if ((dataSize < sizeThreshold && rowCount < rowCountThreshold) ||
         rowCount < MIN_ROWS_FOR_FAST_SPLIT ||
         stats.InFlightTxCount == 0 ||
         stats.GetCurrentRawCpuUsage() < cpuUsageThreshold * 1000000)
    {
        return false;
    }

    return true;
}

bool TTableInfo::CheckSplitByLoad(const TSplitSettings& splitSettings, TShardIdx shardIdx, ui64 dataSize, ui64 rowCount) const {
    // Don't split/merge backup tables
    if (IsBackup)
        return false;

    if (!splitSettings.SplitByLoadEnabled)
        return false;

    // Ignore stats from unknown datashard (it could have been split)
    if (!Stats.PartitionStats.contains(shardIdx))
        return false;
    if (!Shard2PartitionIdx.contains(shardIdx))
        return false;

    // A shard can be overloaded by heavy reads of non-existing keys.
    // So we want to be able to split it even if it has no data.
    const ui64 MIN_ROWS_FOR_SPLIT_BY_LOAD = 0;
    const ui64 MIN_SIZE_FOR_SPLIT_BY_LOAD = 0;

    const auto& partitionConfig = PartitionConfig();
    const auto& policy = partitionConfig.GetPartitioningPolicy();

    ui64 maxShards = policy.GetMaxPartitionsCount();
    if (maxShards == 0) {
        // Don't want to trigger "too many shards" or "too many readsets" errors
        maxShards = splitSettings.SplitByLoadMaxShardsDefault;
    }

    if (!policy.HasSplitByLoadSettings() || !policy.GetSplitByLoadSettings().GetEnabled()) {
        return false;
    }

    const auto& settings = policy.GetSplitByLoadSettings();
    i64 cpuPercentage = settings.GetCpuPercentageThreshold();

    float cpuUsageThreshold = 0.01 * (cpuPercentage ? cpuPercentage : (i64)splitSettings.FastSplitCpuPercentageThreshold);

    const auto& stats = *Stats.PartitionStats.FindPtr(shardIdx);
    if (rowCount < MIN_ROWS_FOR_SPLIT_BY_LOAD ||
        dataSize < MIN_SIZE_FOR_SPLIT_BY_LOAD ||
        Stats.PartitionStats.size() >= maxShards ||
        stats.GetCurrentRawCpuUsage() < cpuUsageThreshold * 1000000)
    {
        return false;
    }

    return true;
}

TChannelsMapping GetPoolsMapping(const TChannelsBindings& bindings) {
    TChannelsMapping mapping;
    for (const auto& bind : bindings) {
        mapping.emplace_back(bind.GetStoragePoolName());
    }
    return mapping;
}

TString TExportInfo::ToString() const {
    return TStringBuilder() << "{"
        << " Id: " << Id
        << " Uid: '" << Uid << "'"
        << " Kind: " << Kind
        << " DomainPathId: " << DomainPathId
        << " ExportPathId: " << ExportPathId
        << " UserSID: '" << UserSID << "'"
        << " State: " << State
        << " WaitTxId: " << WaitTxId
        << " Issue: '" << Issue << "'"
        << " Items: " << Items.size()
        << " PendingItems: " << PendingItems.size()
        << " PendingDropItems: " << PendingDropItems.size()
    << " }";
}

TString TExportInfo::TItem::ToString(ui32 idx) const {
    return TStringBuilder() << "{"
        << " Idx: " << idx
        << " SourcePathName: '" << SourcePathName << "'"
        << " SourcePathId: " << SourcePathId
        << " State: " << State
        << " SubState: " << SubState
        << " WaitTxId: " << WaitTxId
        << " Issue: '" << Issue << "'"
    << " }";
}

bool TExportInfo::TItem::IsDone(const TExportInfo::TItem& item) {
    return item.State == EState::Done;
}

bool TExportInfo::TItem::IsDropped(const TExportInfo::TItem& item) {
    return item.State == EState::Dropped;
}

bool TExportInfo::AllItemsAreDropped() const {
    return AllOf(Items, &TExportInfo::TItem::IsDropped);
}

void TExportInfo::AddNotifySubscriber(const TActorId &actorId) {
    Y_VERIFY(!IsFinished());
    Subscribers.insert(actorId);
}

TString TImportInfo::ToString() const {
    return TStringBuilder() << "{"
        << " Id: " << Id
        << " Uid: '" << Uid << "'"
        << " Kind: " << Kind
        << " DomainPathId: " << DomainPathId
        << " UserSID: '" << UserSID << "'"
        << " State: " << State
        << " Issue: '" << Issue << "'"
        << " Items: " << Items.size()
    << " }";
}

TString TImportInfo::TItem::ToString(ui32 idx) const {
    return TStringBuilder() << "{"
        << " Idx: " << idx
        << " DstPathName: '" << DstPathName << "'"
        << " DstPathId: " << DstPathId
        << " State: " << State
        << " SubState: " << SubState
        << " WaitTxId: " << WaitTxId
        << " Issue: '" << Issue << "'"
    << " }";
}

bool TImportInfo::TItem::IsDone(const TImportInfo::TItem& item) {
    return item.State == EState::Done;
}

bool TImportInfo::IsFinished() const {
    return State == EState::Done || State == EState::Cancelled;
}

void TImportInfo::AddNotifySubscriber(const TActorId &actorId) {
    Y_VERIFY(!IsFinished());
    Subscribers.insert(actorId);
}

TIndexBuildInfo::TShardStatus::TShardStatus(TSerializedTableRange range, TString lastKeyAck)
    : Range(std::move(range))
    , LastKeyAck(std::move(lastKeyAck))
{}

TColumnFamiliesMerger::TColumnFamiliesMerger(NKikimrSchemeOp::TPartitionConfig &container)
    : Container(container)
    , DeduplicationById(TPartitionConfigMerger::DeduplicateColumnFamiliesById(Container))
{
    IdByName["default"] = 0;

    // we trust in Container data
    for (const auto& family : Container.GetColumnFamilies()) {
        if (family.GetName()) { // ignore name "", some families don't have a name
            NameByIds.emplace(family.GetId(), family.GetName());
            IdByName.emplace(family.GetName(), family.GetId());
        }
        NextAutogenId = Max(NextAutogenId, family.GetId());
    }
}

bool TColumnFamiliesMerger::Has(ui32 familyId) const {
    return DeduplicationById.contains(familyId);
}

NKikimrSchemeOp::TFamilyDescription *TColumnFamiliesMerger::Get(ui32 familyId, TString &errDescr) {
    if (!Has(familyId)) {
        errDescr = TStringBuilder()
            << "Column family with id: " << familyId << " doesn't present"
            << ", auto generation new column family is allowed only by name in column description";
        return nullptr;
    }

    auto& dstFamily = TPartitionConfigMerger::MutableColumnFamilyById(Container, DeduplicationById, familyId);

    return &dstFamily;
}

NKikimrSchemeOp::TFamilyDescription *TColumnFamiliesMerger::AddOrGet(ui32 familyId, TString &errDescr) {
    if (NameByIds.contains(familyId)) {
        return AddOrGet(familyId, NameByIds.at(familyId), errDescr);
    }

    auto& dstFamily = TPartitionConfigMerger::MutableColumnFamilyById(Container, DeduplicationById, familyId);
    return &dstFamily;
}

NKikimrSchemeOp::TFamilyDescription *TColumnFamiliesMerger::AddOrGet(const TString &familyName, TString &errDescr) {
    const auto& canonicFamilyName = CanonizeName(familyName);

    if (IdByName.contains(canonicFamilyName)) {
        return AddOrGet(IdByName.at(canonicFamilyName), canonicFamilyName, errDescr);
    }

    // generate id
    if (NextAutogenId >= MAX_AUTOGENERATED_FAMILY_ID) {
        errDescr = TStringBuilder()
                << "Column family id overflow at adding column family with name " << familyName;
        return nullptr;
    }

    return AddOrGet(++NextAutogenId, canonicFamilyName, errDescr);
}

NKikimrSchemeOp::TFamilyDescription *TColumnFamiliesMerger::Get(ui32 familyId, const TString &familyName, TString &errDescr) {
    const auto& canonicFamilyName = CanonizeName(familyName);

    if (IdByName.contains(canonicFamilyName) && IdByName.at(canonicFamilyName) != familyId) {
        errDescr = TStringBuilder()
            << "at the request column family has Id " << familyId << " and name " << familyName
            << ", but table already has the column family with name " << canonicFamilyName << " and different Id " << IdByName.at(canonicFamilyName);
        return nullptr;
    }

    if (NameByIds.contains(familyId) && NameByIds.at(familyId) != canonicFamilyName) {
        errDescr = TStringBuilder()
            << "at the request column family has Id " << familyId << " and name " << familyName
            << ", but table already has the column family with Id " << familyId << " and different name " << NameByIds.at(familyId);
        return nullptr;
    }

    return Get(familyId, errDescr);
}

NKikimrSchemeOp::TFamilyDescription *TColumnFamiliesMerger::AddOrGet(ui32 familyId, const TString &familyName, TString &errDescr) {
    const auto& canonicFamilyName = CanonizeName(familyName);

    if (IdByName.contains(canonicFamilyName) && IdByName.at(canonicFamilyName) != familyId) {
        errDescr = TStringBuilder()
            << "at the request column family has Id " << familyId << " and name " << familyName
            << ", but table already has the column family with name " << canonicFamilyName << " and different Id " << IdByName.at(canonicFamilyName);
        return nullptr;
    }

    if (NameByIds.contains(familyId) && NameByIds.at(familyId) != canonicFamilyName) {
        errDescr = TStringBuilder()
            << "at the request column family has Id " << familyId << " and name " << familyName
            << ", but table already has the column family with Id " << familyId << " and different name " << NameByIds.at(familyId);
        return nullptr;
    }

    auto& dstFamily = TPartitionConfigMerger::MutableColumnFamilyById(Container, DeduplicationById, familyId);

    if (!dstFamily.HasName()) {
        dstFamily.SetName(canonicFamilyName);
    }

    NameByIds[familyId] = canonicFamilyName;
    IdByName[canonicFamilyName] = familyId;

    return &dstFamily;
}

const TString &TColumnFamiliesMerger::CanonizeName(const TString &familyName) {
    static const TString defName = "default";

    if (!familyName) {
        return defName;
    }

    return familyName;
}

void TPQShardInfo::TKeyRange::SerializeToProto(NKikimrPQ::TPartitionKeyRange& proto) const {
    if (FromBound) {
        proto.SetFromBound(*FromBound);
    }

    if (ToBound) {
        proto.SetToBound(*ToBound);
    }
}

bool TPersQueueGroupInfo::FillKeySchema(const NKikimrPQ::TPQTabletConfig& tabletConfig, TString& error) {
    KeySchema.clear();
    KeySchema.reserve(tabletConfig.PartitionKeySchemaSize());

    for (const auto& component : tabletConfig.GetPartitionKeySchema()) {
        if (!NScheme::NTypeIds::IsYqlType(component.GetTypeId())) {
            error = TStringBuilder() << "TypeId is not supported"
                << ": typeId# " << component.GetTypeId()
                << ", component# " << component.GetName();
            return false;
        }

        KeySchema.push_back(component.GetTypeId());
    }

    return true;
}

bool TPersQueueGroupInfo::FillKeySchema(const TString& tabletConfig) {
    NKikimrPQ::TPQTabletConfig proto;
    if (!proto.ParseFromString(tabletConfig)) {
        return false;
    }

    TString unused;
    return FillKeySchema(proto, unused);
}

TBillingStats::TBillingStats(ui64 rows, ui64 bytes)
    : Rows(rows)
    , Bytes(bytes)
{
}

TBillingStats::TBillingStats(const TBillingStats &other)
    : Rows(other.Rows)
    , Bytes(other.Bytes)
{
}

TBillingStats &TBillingStats::operator =(const TBillingStats &other) {
    if (this == &other) {
        return *this;
    }

    Rows = other.Rows;
    Bytes = other.Bytes;
    return *this;
}

TBillingStats TBillingStats::operator -(const TBillingStats &other) const {
    Y_VERIFY(Rows >= other.Rows);
    Y_VERIFY(Bytes >= other.Bytes);

    return TBillingStats(Rows - other.Rows, Bytes - other.Bytes);
}

TBillingStats &TBillingStats::operator -=(const TBillingStats &other) {
    if (this == &other) {
        Rows = 0;
        Bytes = 0;
        return *this;
    }

    Y_VERIFY(Rows >= other.Rows);
    Y_VERIFY(Bytes >= other.Bytes);

    Rows -= other.Rows;
    Bytes -= other.Bytes;
    return *this;
}

TBillingStats TBillingStats::operator +(const TBillingStats &other) const {
    return TBillingStats(Rows + other.Rows, Bytes + other.Bytes);
}

TBillingStats &TBillingStats::operator +=(const TBillingStats &other) {
    if (this == &other) {
        Rows += Rows;
        Bytes += Bytes;
        return *this;
    }

    Rows += other.Rows;
    Bytes += other.Bytes;
    return *this;
}

bool TBillingStats::operator < (const TBillingStats &other) const {
    return Rows < other.Rows && Bytes < other.Bytes;
}

bool TBillingStats::operator <= (const TBillingStats &other) const {
    return Rows <= other.Rows && Bytes <= other.Bytes;
}

bool TBillingStats::operator ==(const TBillingStats &other) const {
    return Rows == other.Rows && Bytes == other.Bytes;
}

TString TBillingStats::ToString() const {
    return TStringBuilder()
            << "{"
            << " rows: " << GetRows()
            << " bytes: " << GetBytes()
            << " }";
}

ui64 TBillingStats::GetRows() const {
    return Rows;
}

ui64 TBillingStats::GetBytes() const {
    return Bytes;
}

NKikimr::NSchemeShard::TBillingStats::operator bool() const {
    return Rows || Bytes;
}

TOlapStoreInfo::TOlapStoreInfo(
        ui64 alterVersion,
        NKikimrSchemeOp::TColumnStoreDescription&& description,
        NKikimrSchemeOp::TColumnStoreSharding&& sharding,
        TMaybe<NKikimrSchemeOp::TAlterColumnStore>&& alterBody)
    : AlterVersion(alterVersion)
    , Description(std::move(description))
    , Sharding(std::move(sharding))
    , AlterBody(std::move(alterBody))
{
    size_t schemaPresetIndex = 0;
    for (const auto& presetProto : Description.GetSchemaPresets()) {
        Y_VERIFY(presetProto.HasId());
        Y_VERIFY(presetProto.HasName());
        Y_VERIFY(presetProto.HasSchema());
        Y_VERIFY(!SchemaPresets.contains(presetProto.GetId()));
        auto& preset = SchemaPresets[presetProto.GetId()];
        preset.Id = presetProto.GetId();
        preset.Name = presetProto.GetName();
        preset.ProtoIndex = schemaPresetIndex++;
        SchemaPresetByName[preset.Name] = preset.Id;
        for (const auto& colProto : presetProto.GetSchema().GetColumns()) {
            Y_VERIFY(colProto.HasId());
            Y_VERIFY(colProto.HasName());
            Y_VERIFY(!preset.Columns.contains(colProto.GetId()));
            Y_VERIFY(!preset.ColumnsByName.contains(colProto.GetName()));
            auto& col = preset.Columns[colProto.GetId()];
            col.Id = colProto.GetId();
            col.Name = colProto.GetName();
            col.TypeId = colProto.GetTypeId();
            preset.ColumnsByName[col.Name] = col.Id;
        }
        for (const auto& keyName : presetProto.GetSchema().GetKeyColumnNames()) {
            Y_VERIFY(preset.ColumnsByName.contains(keyName));
            auto& col = *preset.FindColumnByName(keyName);
            Y_VERIFY(col.KeyOrder == Max<ui32>());
            col.KeyOrder = preset.KeyColumnIds.size();
            preset.KeyColumnIds.push_back(col.Id);
        }
        for (const auto& tierConfig : presetProto.GetSchema().GetStorageTiers()) {
            preset.Tiers.insert(tierConfig.GetName());
        }
        preset.Engine = presetProto.GetSchema().GetEngine();
        preset.NextColumnId = presetProto.GetSchema().GetNextColumnId();
        preset.Version = presetProto.GetSchema().GetVersion();
    }

    for (const auto& shardIdx : Sharding.GetColumnShards()) {
        ColumnShards.push_back(TShardIdx(
            TOwnerId(shardIdx.GetOwnerId()),
            TLocalShardIdx(shardIdx.GetLocalId())));
    }
}

TColumnTableInfo::TColumnTableInfo(
        ui64 alterVersion,
        NKikimrSchemeOp::TColumnTableDescription&& description,
        NKikimrSchemeOp::TColumnTableSharding&& sharding,
        TMaybe<NKikimrSchemeOp::TAlterColumnTable>&& alterBody)
    : AlterVersion(alterVersion)
    , Description(std::move(description))
    , Sharding(std::move(sharding))
    , AlterBody(std::move(alterBody))
{
    OlapStorePathId = TPathId(
        TOwnerId(Description.GetColumnStorePathId().GetOwnerId()),
        TLocalPathId(Description.GetColumnStorePathId().GetLocalId()));

    ColumnShards.reserve(Sharding.GetColumnShards().size());
    for (ui64 columnShard : Sharding.GetColumnShards()) {
        ColumnShards.push_back(columnShard);
    }
}

TSequenceInfo::TSequenceInfo(
        ui64 alterVersion,
        NKikimrSchemeOp::TSequenceDescription&& description,
        NKikimrSchemeOp::TSequenceSharding&& sharding)
    : AlterVersion(alterVersion)
    , Description(std::move(description))
    , Sharding(std::move(sharding))
{
    // TODO: extract necessary info
}

bool TSequenceInfo::ValidateCreate(const NKikimrSchemeOp::TSequenceDescription& p, TString& err) {
    if (p.HasPathId() || p.HasVersion() || p.HasSequenceShard()) {
        err = "CreateSequence does not allow internal fields to be specified";
        return false;
    }

    i64 increment = p.HasIncrement() ? p.GetIncrement() : 1;
    if (increment == 0) {
        err = "CreateSequence requires Increment != 0";
        return false;
    }

    i64 minValue, maxValue, startValue;
    if (increment > 0) {
        minValue = p.HasMinValue() ? p.GetMinValue() : 1;
        maxValue = p.HasMaxValue() ? p.GetMaxValue() : Max<i64>();
        startValue = p.HasStartValue() ? p.GetStartValue() : minValue;
    } else {
        minValue = p.HasMinValue() ? p.GetMinValue() : Min<i64>();
        maxValue = p.HasMaxValue() ? p.GetMaxValue() : -1;
        startValue = p.HasStartValue() ? p.GetStartValue() : maxValue;
    }

    if (!(minValue <= maxValue)) {
        err = TStringBuilder()
            << "CreateSequence requires MinValue (" << minValue << ") <= MaxValue (" << maxValue << ")";
        return false;
    }

    if (!(minValue <= startValue && startValue <= maxValue)) {
        err = TStringBuilder()
            << "CreateSequence requires StartValue (" << startValue << ") between"
            << " MinValue (" << minValue << ") and MaxValue (" << maxValue << ")";
        return false;
    }

    return true;
}

} // namespace NSchemeShard
} // namespace NKikimr

