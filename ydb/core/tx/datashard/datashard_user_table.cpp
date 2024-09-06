#include "datashard_user_table.h"

#include <ydb/core/base/path.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr {

using NTabletFlatExecutor::TTransactionContext;

namespace NDataShard {

TUserTable::TUserTable(ui32 localTid, const NKikimrSchemeOp::TTableDescription& descr, ui32 shadowTid)
    : LocalTid(localTid)
    , ShadowTid(shadowTid)
{
    Y_PROTOBUF_SUPPRESS_NODISCARD descr.SerializeToString(&Schema);
    Name = ExtractBase(descr.GetPath());
    Path = descr.GetPath();
    ParseProto(descr);
}

TUserTable::TUserTable(const TUserTable& table, const NKikimrSchemeOp::TTableDescription& descr)
    : TUserTable(table)
{
    Y_VERIFY_S(Name == descr.GetName(), "Name: " << Name << " descr.Name: " << descr.GetName());
    ParseProto(descr);
    AlterSchema();
}

void TUserTable::SetPath(const TString &path)
{
    Name = ExtractBase(path);
    Path = path;
    AlterSchema();
}

void TUserTable::SetTableSchemaVersion(ui64 schemaVersion)
{
    NKikimrSchemeOp::TTableDescription schema;
    GetSchema(schema);
    schema.SetTableSchemaVersion(schemaVersion);
    SetSchema(schema);

    TableSchemaVersion = schemaVersion;
}

bool TUserTable::ResetTableSchemaVersion()
{
    if (TableSchemaVersion) {
        NKikimrSchemeOp::TTableDescription schema;
        GetSchema(schema);

        schema.ClearTableSchemaVersion();
        TableSchemaVersion = 0;

        SetSchema(schema);
        return true;
    }

    return false;
}

void TUserTable::AddIndex(const NKikimrSchemeOp::TIndexDescription& indexDesc) {
    Y_ABORT_UNLESS(indexDesc.HasPathOwnerId() && indexDesc.HasLocalPathId());
    const auto addIndexPathId = TPathId(indexDesc.GetPathOwnerId(), indexDesc.GetLocalPathId());

    auto it = Indexes.lower_bound(addIndexPathId);
    if (it != Indexes.end() && it->first == addIndexPathId) {
        return;
    }

    Indexes.emplace_hint(it, addIndexPathId, TTableIndex(indexDesc, Columns));
    AsyncIndexCount += ui32(indexDesc.GetType() == TTableIndex::EType::EIndexTypeGlobalAsync);

    NKikimrSchemeOp::TTableDescription schema;
    GetSchema(schema);

    schema.MutableTableIndexes()->Add()->CopyFrom(indexDesc);
    SetSchema(schema);
}

void TUserTable::SwitchIndexState(const TPathId& indexPathId, TTableIndex::EState state) {
    auto it = Indexes.find(indexPathId);
    if (it == Indexes.end()) {
        return;
    }

    it->second.State = state;

    // This isn't really necessary now, because no one rely on index state
    NKikimrSchemeOp::TTableDescription schema;
    GetSchema(schema);

    auto& indexes = *schema.MutableTableIndexes();
    for (auto it = indexes.begin(); it != indexes.end(); ++it) {
        if (indexPathId == TPathId(it->GetPathOwnerId(), it->GetLocalPathId())) {
            it->SetState(state);
            SetSchema(schema);
            return;
        }
    }

    Y_ABORT("unreachable");
}

void TUserTable::DropIndex(const TPathId& indexPathId) {
    auto it = Indexes.find(indexPathId);
    if (it == Indexes.end()) {
        return;
    }

    AsyncIndexCount -= ui32(it->second.Type == TTableIndex::EType::EIndexTypeGlobalAsync);
    Indexes.erase(it);

    NKikimrSchemeOp::TTableDescription schema;
    GetSchema(schema);

    auto& indexes = *schema.MutableTableIndexes();
    for (auto it = indexes.begin(); it != indexes.end(); ++it) {
        if (indexPathId == TPathId(it->GetPathOwnerId(), it->GetLocalPathId())) {
            indexes.erase(it);
            SetSchema(schema);
            return;
        }
    }

    Y_ABORT("unreachable");
}

bool TUserTable::HasAsyncIndexes() const {
    return AsyncIndexCount > 0;
}

static bool IsJsonCdcStream(TUserTable::TCdcStream::EFormat format) {
    switch (format) {
    case TUserTable::TCdcStream::EFormat::ECdcStreamFormatJson:
    case TUserTable::TCdcStream::EFormat::ECdcStreamFormatDynamoDBStreamsJson:
    case TUserTable::TCdcStream::EFormat::ECdcStreamFormatDebeziumJson:
        return true;
    default:
        return false;
    }
}

void TUserTable::AddCdcStream(const NKikimrSchemeOp::TCdcStreamDescription& streamDesc) {
    Y_ABORT_UNLESS(streamDesc.HasPathId());
    const auto streamPathId = PathIdFromPathId(streamDesc.GetPathId());

    if (CdcStreams.contains(streamPathId)) {
        return;
    }

    CdcStreams.emplace(streamPathId, TCdcStream(streamDesc));
    JsonCdcStreamCount += ui32(IsJsonCdcStream(streamDesc.GetFormat()));

    NKikimrSchemeOp::TTableDescription schema;
    GetSchema(schema);

    schema.MutableCdcStreams()->Add()->CopyFrom(streamDesc);
    SetSchema(schema);
}

void TUserTable::SwitchCdcStreamState(const TPathId& streamPathId, TCdcStream::EState state) {
    auto it = CdcStreams.find(streamPathId);
    if (it == CdcStreams.end()) {
        return;
    }

    it->second.State = state;

    NKikimrSchemeOp::TTableDescription schema;
    GetSchema(schema);

    for (auto it = schema.MutableCdcStreams()->begin(); it != schema.MutableCdcStreams()->end(); ++it) {
        if (streamPathId != PathIdFromPathId(it->GetPathId())) {
            continue;
        }

        it->SetState(state);
        SetSchema(schema);

        return;
    }

    Y_ABORT("unreachable");
}

void TUserTable::DropCdcStream(const TPathId& streamPathId) {
    auto it = CdcStreams.find(streamPathId);
    if (it == CdcStreams.end()) {
        return;
    }

    JsonCdcStreamCount -= ui32(IsJsonCdcStream(it->second.Format));
    CdcStreams.erase(it);

    NKikimrSchemeOp::TTableDescription schema;
    GetSchema(schema);

    for (auto it = schema.GetCdcStreams().begin(); it != schema.GetCdcStreams().end(); ++it) {
        if (streamPathId != PathIdFromPathId(it->GetPathId())) {
            continue;
        }

        schema.MutableCdcStreams()->erase(it);
        SetSchema(schema);

        return;
    }

    Y_ABORT("unreachable");
}

bool TUserTable::HasCdcStreams() const {
    return !CdcStreams.empty();
}

bool TUserTable::NeedSchemaSnapshots() const {
    return JsonCdcStreamCount > 0;
}

bool TUserTable::IsReplicated() const {
    switch (ReplicationConfig.Mode) {
        case NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE:
            return false;
        default:
            return true;
    }
}

bool TUserTable::IsIncrementalRestore() const {
    return IncrementalBackupConfig.Mode == NKikimrSchemeOp::TTableIncrementalBackupConfig::RESTORE_MODE_INCREMENTAL_BACKUP;
}

void TUserTable::ParseProto(const NKikimrSchemeOp::TTableDescription& descr)
{
    // We expect schemeshard to send us full list of storage rooms
    if (descr.GetPartitionConfig().StorageRoomsSize()) {
        Rooms.clear();
    }

    for (const auto& roomDescr : descr.GetPartitionConfig().GetStorageRooms()) {
        TStorageRoom::TPtr room = new TStorageRoom(roomDescr);
        Rooms[room->GetId()] = room;
    }

    for (const auto& family : descr.GetPartitionConfig().GetColumnFamilies()) {
        auto it = Families.find(family.GetId());
        if (it == Families.end()) {
            it = Families.emplace(std::make_pair(family.GetId(), TUserFamily(family))).first;
        } else {
            it->second.Update(family);
        }
    }

    for (auto& kv : Families) {
        auto roomIt = Rooms.find(kv.second.GetRoomId());
        if (roomIt != Rooms.end()) {
            kv.second.Update(roomIt->second);
        }
    }

    for (const auto& col : descr.GetColumns()) {
        TUserColumn& column = Columns[col.GetId()];
        if (column.Name.empty()) {
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(col.GetTypeId(),
                col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr);
            column = TUserColumn(typeInfoMod.TypeInfo, typeInfoMod.TypeMod, col.GetName());
        }
        column.Family = col.GetFamily();
        column.NotNull = col.GetNotNull();
    }

    for (const auto& col : descr.GetDropColumns()) {
        ui32 colId = col.GetId();
        auto it = Columns.find(colId);
        Y_ABORT_UNLESS(it != Columns.end());
        Y_ABORT_UNLESS(!it->second.IsKey);
        Columns.erase(it);
    }

    if (descr.KeyColumnIdsSize()) {
        Y_ABORT_UNLESS(descr.KeyColumnIdsSize() >= KeyColumnIds.size());
        for (ui32 i = 0; i < KeyColumnIds.size(); ++i) {
            Y_ABORT_UNLESS(KeyColumnIds[i] == descr.GetKeyColumnIds(i));
        }

        KeyColumnIds.clear();
        KeyColumnIds.reserve(descr.KeyColumnIdsSize());
        KeyColumnTypes.resize(descr.KeyColumnIdsSize());
        for (size_t i = 0; i < descr.KeyColumnIdsSize(); ++i) {
            ui32 keyColId = descr.GetKeyColumnIds(i);
            KeyColumnIds.push_back(keyColId);

            TUserColumn * col = Columns.FindPtr(keyColId);
            Y_ABORT_UNLESS(col);
            col->IsKey = true;
            KeyColumnTypes[i] = col->Type;
        }

        Y_ABORT_UNLESS(KeyColumnIds.size() == KeyColumnTypes.size());
    }

    if (descr.HasPartitionRangeBegin()) {
        Y_ABORT_UNLESS(descr.HasPartitionRangeEnd());
        Range = TSerializedTableRange(descr.GetPartitionRangeBegin(),
                                      descr.GetPartitionRangeEnd(),
                                      descr.GetPartitionRangeBeginIsInclusive(),
                                      descr.GetPartitionRangeEndIsInclusive());
    }

    TableSchemaVersion = descr.GetTableSchemaVersion();
    IsBackup = descr.GetIsBackup();
    ReplicationConfig = TReplicationConfig(descr.GetReplicationConfig());
    IncrementalBackupConfig = TIncrementalBackupConfig(descr.GetIncrementalBackupConfig());

    CheckSpecialColumns();

    for (const auto& indexDesc : descr.GetTableIndexes()) {
        Y_ABORT_UNLESS(indexDesc.HasPathOwnerId() && indexDesc.HasLocalPathId());
        Indexes.emplace(TPathId(indexDesc.GetPathOwnerId(), indexDesc.GetLocalPathId()), TTableIndex(indexDesc, Columns));
        AsyncIndexCount += ui32(indexDesc.GetType() == TTableIndex::EType::EIndexTypeGlobalAsync);
    }

    for (const auto& streamDesc : descr.GetCdcStreams()) {
        Y_ABORT_UNLESS(streamDesc.HasPathId());
        CdcStreams.emplace(PathIdFromPathId(streamDesc.GetPathId()), TCdcStream(streamDesc));
        JsonCdcStreamCount += ui32(IsJsonCdcStream(streamDesc.GetFormat()));
    }
}

void TUserTable::CheckSpecialColumns() {
    SpecialColTablet = Max<ui32>();
    SpecialColEpoch = Max<ui32>();
    SpecialColUpdateNo = Max<ui32>();

    for (const auto &xpair : Columns) {
        const ui32 colId = xpair.first;
        const auto &column = xpair.second;

        if (column.IsKey || column.Type.GetTypeId() != NScheme::NTypeIds::Uint64)
            continue;

        if (column.Name == "__tablet")
            SpecialColTablet = colId;
        else if (column.Name == "__updateEpoch")
            SpecialColEpoch = colId;
        else if (column.Name == "__updateNo")
            SpecialColUpdateNo = colId;
    }
}

void TUserTable::AlterSchema() {
    NKikimrSchemeOp::TTableDescription schema;
    GetSchema(schema);

    auto& partConfig = *schema.MutablePartitionConfig();
    partConfig.ClearStorageRooms();
    for (const auto& room : Rooms) {
        partConfig.AddStorageRooms()->CopyFrom(*room.second);
    }

    // FIXME: these generated column families are incorrect!
    partConfig.ClearColumnFamilies();
    for (const auto& f : Families) {
        const TUserFamily& family = f.second;
        auto columnFamily = partConfig.AddColumnFamilies();
        columnFamily->SetId(f.first);
        columnFamily->SetName(family.GetName());
        columnFamily->SetStorage(family.Storage);
        columnFamily->SetColumnCodec(family.ColumnCodec);
        columnFamily->SetColumnCache(family.ColumnCache);
        columnFamily->SetRoom(family.GetRoomId());
    }

    schema.ClearColumns();
    for (const auto& col : Columns) {
        const TUserColumn& column = col.second;

        auto descr = schema.AddColumns();
        descr->SetName(column.Name);
        descr->SetId(col.first);
        auto protoType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.Type, column.TypeMod);
        descr->SetTypeId(protoType.TypeId);
        if (protoType.TypeInfo) {
            *descr->MutableTypeInfo() = *protoType.TypeInfo;
        }
        descr->SetFamily(column.Family);
        descr->SetNotNull(column.NotNull);
    }

    schema.SetPartitionRangeBegin(Range.From.GetBuffer());
    schema.SetPartitionRangeBeginIsInclusive(Range.FromInclusive);
    schema.SetPartitionRangeEnd(Range.To.GetBuffer());
    schema.SetPartitionRangeEndIsInclusive(Range.ToInclusive);

    ReplicationConfig.Serialize(*schema.MutableReplicationConfig());
    IncrementalBackupConfig.Serialize(*schema.MutableIncrementalBackupConfig());

    schema.SetName(Name);
    schema.SetPath(Path);

    SetSchema(schema);
}

void TUserTable::ApplyCreate(
        TTransactionContext& txc, const TString& tableName,
        const NKikimrSchemeOp::TPartitionConfig& partConfig) const
{
    DoApplyCreate(txc, tableName, false, partConfig);
}

void TUserTable::ApplyCreateShadow(
        TTransactionContext& txc, const TString& tableName,
        const NKikimrSchemeOp::TPartitionConfig& partConfig) const
{
    DoApplyCreate(txc, tableName, true, partConfig);
}

void TUserTable::DoApplyCreate(
        TTransactionContext& txc, const TString& tableName, bool shadow,
        const NKikimrSchemeOp::TPartitionConfig& partConfig) const
{
    const ui32 tid = shadow ? ShadowTid : LocalTid;

    Y_ABORT_UNLESS(tid != 0 && tid != Max<ui32>(), "Creating table %s with bad id %" PRIu32, tableName.c_str(), tid);

    auto &alter = txc.DB.Alter();
    alter.AddTable(tableName, tid);

    THashSet<ui32> appliedRooms;
    for (const auto& fam : Families) {
        ui32 familyId = fam.first;
        const TUserFamily& family = fam.second;

        alter.AddFamily(tid, familyId, family.GetRoomId());
        alter.SetFamily(tid, familyId, family.Cache, family.Codec);
        alter.SetFamilyBlobs(tid, familyId, family.GetOuterThreshold(), family.GetExternalThreshold());
        if (appliedRooms.insert(family.GetRoomId()).second) {
            // Call SetRoom once per room
            alter.SetRoom(tid, family.GetRoomId(), family.MainChannel(), family.ExternalChannel(), family.OuterChannel());
        }
    }

    for (const auto& col : Columns) {
        ui32 columnId = col.first;
        const TUserColumn& column = col.second;

        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.Type, column.TypeMod);
        ui32 pgTypeId = columnType.TypeInfo ? columnType.TypeInfo->GetPgTypeId() : 0;
        alter.AddPgColumn(tid, column.Name, columnId, columnType.TypeId, pgTypeId, column.TypeMod, column.NotNull);
        alter.AddColumnToFamily(tid, columnId, column.Family);
    }

    for (size_t i = 0; i < KeyColumnIds.size(); ++i) {
        alter.AddColumnToKey(tid, KeyColumnIds[i]);
    }

    if (partConfig.HasCompactionPolicy()) {
        NLocalDb::TCompactionPolicyPtr policy = new NLocalDb::TCompactionPolicy(partConfig.GetCompactionPolicy());
        alter.SetCompactionPolicy(tid, *policy);
    } else {
        alter.SetCompactionPolicy(tid, *NLocalDb::CreateDefaultUserTablePolicy());
    }

    if (partConfig.HasEnableFilterByKey()) {
        alter.SetByKeyFilter(tid, partConfig.GetEnableFilterByKey());
    }

    // N.B. some settings only apply to the main table

    if (!shadow) {
        if (partConfig.HasExecutorCacheSize()) {
            alter.SetExecutorCacheSize(partConfig.GetExecutorCacheSize());
        }

        if (partConfig.HasResourceProfile() && partConfig.GetResourceProfile()) {
            alter.SetExecutorResourceProfile(partConfig.GetResourceProfile());
        }

        if (partConfig.HasExecutorFastLogPolicy()) {
            alter.SetExecutorFastLogPolicy(partConfig.GetExecutorFastLogPolicy());
        }

        alter.SetEraseCache(tid, partConfig.GetEnableEraseCache(), partConfig.GetEraseCacheMinRows(), partConfig.GetEraseCacheMaxBytes());

        if (IsBackup) {
            alter.SetColdBorrow(tid, true);
        }
    }
}

void TUserTable::ApplyAlter(
        TTransactionContext& txc, const TUserTable& oldTable,
        const NKikimrSchemeOp::TTableDescription& delta, TString& strError)
{
    const auto& configDelta = delta.GetPartitionConfig();
    NKikimrSchemeOp::TTableDescription schema;
    GetSchema(schema);
    auto& config = *schema.MutablePartitionConfig();

    auto &alter = txc.DB.Alter();

    // Check if we need to drop shadow table first
    if (configDelta.HasShadowData()) {
        if (configDelta.GetShadowData()) {
            if (!ShadowTid) {
                // Alter is creating shadow data
                strError = "Alter cannot create new shadow data";
            }
        } else {
            if (ShadowTid) {
                // Alter is removing shadow data
                alter.DropTable(ShadowTid);
                ShadowTid = 0;
            }
            config.ClearShadowData();
        }
    }

    // Most settings are applied to both main and shadow table
    TStackVec<ui32> tids;
    tids.push_back(LocalTid);
    if (ShadowTid) {
        tids.push_back(ShadowTid);
    }

    THashSet<ui32> appliedRooms;
    for (const auto& f : Families) {
        ui32 familyId = f.first;
        const TUserFamily& family = f.second;

        for (ui32 tid : tids) {
            alter.AddFamily(tid, familyId, family.GetRoomId());
            alter.SetFamily(tid, familyId, family.Cache, family.Codec);
            alter.SetFamilyBlobs(tid, familyId, family.GetOuterThreshold(), family.GetExternalThreshold());
        }

        if (appliedRooms.insert(family.GetRoomId()).second) {
            // Call SetRoom once per room
            for (ui32 tid : tids) {
                alter.SetRoom(tid, family.GetRoomId(), family.MainChannel(), family.ExternalChannel(), family.OuterChannel());
            }
        }
    }

    for (const auto& col : Columns) {
        ui32 colId = col.first;
        const TUserColumn& column = col.second;

        if (!oldTable.Columns.contains(colId)) {
            for (ui32 tid : tids) {
                auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.Type, column.TypeMod);
                ui32 pgTypeId = columnType.TypeInfo ? columnType.TypeInfo->GetPgTypeId() : 0;
                alter.AddPgColumn(tid, column.Name, colId, columnType.TypeId, pgTypeId, column.TypeMod, column.NotNull);
            }
        }

        for (ui32 tid : tids) {
            alter.AddColumnToFamily(tid, colId, column.Family);
        }
    }

    for (const auto& col : delta.GetDropColumns()) {
        ui32 colId = col.GetId();
        const TUserTable::TUserColumn * oldCol = oldTable.Columns.FindPtr(colId);
        Y_ABORT_UNLESS(oldCol);
        Y_ABORT_UNLESS(oldCol->Name == col.GetName());
        Y_ABORT_UNLESS(!Columns.contains(colId));

        for (ui32 tid : tids) {
            alter.DropColumn(tid, colId);
        }
    }

    for (size_t i = 0; i < KeyColumnIds.size(); ++i) {
        for (ui32 tid : tids) {
            alter.AddColumnToKey(tid, KeyColumnIds[i]);
        }
    }

    if (configDelta.HasCompactionPolicy()) {
        TIntrusiveConstPtr<NLocalDb::TCompactionPolicy> oldPolicy =
                txc.DB.GetScheme().Tables.find(LocalTid)->second.CompactionPolicy;
        NLocalDb::TCompactionPolicy newPolicy(configDelta.GetCompactionPolicy());

        if (NLocalDb::ValidateCompactionPolicyChange(*oldPolicy, newPolicy, strError)) {
            for (ui32 tid : tids) {
                alter.SetCompactionPolicy(tid, newPolicy);
            }
            config.ClearCompactionPolicy();
            newPolicy.Serialize(*config.MutableCompactionPolicy());
        } else {
            strError = TString("cannot change compaction policy: ") + strError;
        }
    }

    if (configDelta.HasEnableFilterByKey()) {
        config.SetEnableFilterByKey(configDelta.GetEnableFilterByKey());
        for (ui32 tid : tids) {
            alter.SetByKeyFilter(tid, configDelta.GetEnableFilterByKey());
        }
    }

    // N.B. some settings only apply to the main table

    if (configDelta.HasExecutorCacheSize()) {
        config.SetExecutorCacheSize(configDelta.GetExecutorCacheSize());
        alter.SetExecutorCacheSize(configDelta.GetExecutorCacheSize());
    }

    if (configDelta.HasResourceProfile() && configDelta.GetResourceProfile()) {
        config.SetResourceProfile(configDelta.GetResourceProfile());
        alter.SetExecutorResourceProfile(configDelta.GetResourceProfile());
    }

    if (configDelta.HasExecutorFastLogPolicy()) {
        config.SetExecutorFastLogPolicy(configDelta.GetExecutorFastLogPolicy());
        alter.SetExecutorFastLogPolicy(configDelta.GetExecutorFastLogPolicy());
    }

    if (configDelta.HasEnableEraseCache() || configDelta.HasEraseCacheMinRows() || configDelta.HasEraseCacheMaxBytes()) {
        if (configDelta.HasEnableEraseCache()) {
            config.SetEnableEraseCache(configDelta.GetEnableEraseCache());
        }
        if (configDelta.HasEraseCacheMinRows()) {
            config.SetEraseCacheMinRows(configDelta.GetEraseCacheMinRows());
        }
        if (configDelta.HasEraseCacheMaxBytes()) {
            config.SetEraseCacheMaxBytes(configDelta.GetEraseCacheMaxBytes());
        }
        alter.SetEraseCache(LocalTid, config.GetEnableEraseCache(), config.GetEraseCacheMinRows(), config.GetEraseCacheMaxBytes());
    }

    schema.SetTableSchemaVersion(delta.GetTableSchemaVersion());

    SetSchema(schema);
}

void TUserTable::ApplyDefaults(TTransactionContext& txc) const
{
    const auto* tableInfo = txc.DB.GetScheme().GetTableInfo(LocalTid);
    if (!tableInfo) {
        // Local table does not exist, no need to apply any defaults
        return;
    }

    NKikimrSchemeOp::TTableDescription schema;
    GetSchema(schema);
    const auto& config = schema.GetPartitionConfig();

    if ((!config.HasEnableEraseCache() && config.GetEnableEraseCache() != tableInfo->EraseCacheEnabled) ||
        (config.GetEnableEraseCache() && !config.HasEraseCacheMinRows() && config.GetEraseCacheMinRows() != tableInfo->EraseCacheMinRows) ||
        (config.GetEnableEraseCache() && !config.HasEraseCacheMaxBytes() && config.GetEraseCacheMaxBytes() != tableInfo->EraseCacheMaxBytes))
    {
        // Protobuf defaults for erase cache changed, apply to local database
        txc.DB.Alter().SetEraseCache(LocalTid, config.GetEnableEraseCache(), config.GetEraseCacheMinRows(), config.GetEraseCacheMaxBytes());
    }
}

void TUserTable::Fix_KIKIMR_17222(NTable::TDatabase& db) const
{
    Fix_KIKIMR_17222(db, LocalTid);
    if (ShadowTid) {
        Fix_KIKIMR_17222(db, ShadowTid);
    }
}

void TUserTable::Fix_KIKIMR_17222(NTable::TDatabase& db, ui32 tid) const
{
    const auto* tableInfo = db.GetScheme().GetTableInfo(tid);
    if (!tableInfo) {
        // Local table does not exist, nothing to fix
        return;
    }

    for (const auto& fam : Families) {
        ui32 familyId = fam.first;
        if (tableInfo->Families.contains(familyId)) {
            // Family exists, nothing to fix
            continue;
        }

        const TUserFamily& family = fam.second;

        db.Alter().AddFamily(tid, familyId, family.GetRoomId());
        db.Alter().SetFamily(tid, familyId, family.Cache, family.Codec);
        db.Alter().SetFamilyBlobs(tid, familyId, family.GetOuterThreshold(), family.GetExternalThreshold());
    }
}

}}
