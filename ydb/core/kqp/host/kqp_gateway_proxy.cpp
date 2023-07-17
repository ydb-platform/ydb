#include "kqp_host_impl.h"

#include <ydb/core/grpc_services/table_settings.h>
#include <ydb/core/ydb_convert/column_families.h>

namespace NKikimr::NKqp {

using namespace NThreading;
using namespace NYql;
using namespace NYql::NCommon;

namespace {

bool ConvertDataSlotToYdbTypedValue(NYql::EDataSlot fromType, const TString& fromValue, Ydb::Type* toType,
    Ydb::Value* toValue)
{
    switch (fromType) {
    case NYql::EDataSlot::Bool:
        toType->set_type_id(Ydb::Type::BOOL);
        toValue->set_bool_value(FromString<bool>(fromValue));
        break;
    case NYql::EDataSlot::Int8:
        toType->set_type_id(Ydb::Type::INT8);
        toValue->set_int32_value(FromString<i32>(fromValue));
        break;
    case NYql::EDataSlot::Uint8:
        toType->set_type_id(Ydb::Type::UINT8);
        toValue->set_uint32_value(FromString<ui32>(fromValue));
        break;
    case NYql::EDataSlot::Int16:
        toType->set_type_id(Ydb::Type::INT16);
        toValue->set_int32_value(FromString<i32>(fromValue));
        break;
    case NYql::EDataSlot::Uint16:
        toType->set_type_id(Ydb::Type::UINT16);
        toValue->set_uint32_value(FromString<ui32>(fromValue));
        break;
    case NYql::EDataSlot::Int32:
        toType->set_type_id(Ydb::Type::INT32);
        toValue->set_int32_value(FromString<i32>(fromValue));
        break;
    case NYql::EDataSlot::Uint32:
        toType->set_type_id(Ydb::Type::UINT32);
        toValue->set_uint32_value(FromString<ui32>(fromValue));
        break;
    case NYql::EDataSlot::Int64:
        toType->set_type_id(Ydb::Type::INT64);
        toValue->set_int64_value(FromString<i64>(fromValue));
        break;
    case NYql::EDataSlot::Uint64:
        toType->set_type_id(Ydb::Type::UINT64);
        toValue->set_uint64_value(FromString<ui64>(fromValue));
        break;
    case NYql::EDataSlot::Float:
        toType->set_type_id(Ydb::Type::FLOAT);
        toValue->set_float_value(FromString<float>(fromValue));
        break;
    case NYql::EDataSlot::Double:
        toType->set_type_id(Ydb::Type::DOUBLE);
        toValue->set_double_value(FromString<double>(fromValue));
        break;
    case NYql::EDataSlot::Json:
        toType->set_type_id(Ydb::Type::JSON);
        toValue->set_text_value(fromValue);
        break;
    case NYql::EDataSlot::String:
        toType->set_type_id(Ydb::Type::STRING);
        toValue->set_bytes_value(fromValue);
        break;
    case NYql::EDataSlot::Utf8:
        toType->set_type_id(Ydb::Type::UTF8);
        toValue->set_text_value(fromValue);
        break;
    case NYql::EDataSlot::Date:
        toType->set_type_id(Ydb::Type::DATE);
        toValue->set_uint32_value(FromString<ui32>(fromValue));
        break;
    case NYql::EDataSlot::Datetime:
        toType->set_type_id(Ydb::Type::DATETIME);
        toValue->set_uint32_value(FromString<ui32>(fromValue));
        break;
    case NYql::EDataSlot::Timestamp:
        toType->set_type_id(Ydb::Type::TIMESTAMP);
        toValue->set_uint64_value(FromString<ui64>(fromValue));
        break;
    case NYql::EDataSlot::Interval:
        toType->set_type_id(Ydb::Type::INTERVAL);
        toValue->set_int64_value(FromString<i64>(fromValue));
        break;
    default:
        return false;
    }
    return true;
}

bool ConvertCreateTableSettingsToProto(NYql::TKikimrTableMetadataPtr metadata, Ydb::Table::CreateTableRequest& proto,
    Ydb::StatusIds::StatusCode& code, TString& error)
{
    for (const auto& family : metadata->ColumnFamilies) {
        auto* familyProto = proto.add_column_families();
        familyProto->set_name(family.Name);
        if (family.Data) {
            familyProto->mutable_data()->set_media(family.Data.GetRef());
        }
        if (family.Compression) {
            if (to_lower(family.Compression.GetRef()) == "off") {
                familyProto->set_compression(Ydb::Table::ColumnFamily::COMPRESSION_NONE);
            } else if (to_lower(family.Compression.GetRef()) == "lz4") {
                familyProto->set_compression(Ydb::Table::ColumnFamily::COMPRESSION_LZ4);
            } else {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Unknown compression '" << family.Compression.GetRef() << "' for a column family";
                return false;
            }
        }
    }

    if (metadata->TableSettings.CompactionPolicy) {
        proto.set_compaction_policy(metadata->TableSettings.CompactionPolicy.GetRef());
    }

    if (metadata->TableSettings.PartitionBy) {
        if (metadata->TableSettings.PartitionBy.size() > metadata->KeyColumnNames.size()) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = "\"Partition by\" contains more columns than primary key does";
            return false;
        } else if (metadata->TableSettings.PartitionBy.size() == metadata->KeyColumnNames.size()) {
            for (size_t i = 0; i < metadata->TableSettings.PartitionBy.size(); ++i) {
                if (metadata->TableSettings.PartitionBy[i] != metadata->KeyColumnNames[i]) {
                    code = Ydb::StatusIds::BAD_REQUEST;
                    error = "\"Partition by\" doesn't match primary key";
                    return false;
                }
            }
        } else {
            code = Ydb::StatusIds::UNSUPPORTED;
            error = "\"Partition by\" is not supported yet";
            return false;
        }
    }

    if (metadata->TableSettings.AutoPartitioningBySize) {
        auto& partitioningSettings = *proto.mutable_partitioning_settings();
        TString value = to_lower(metadata->TableSettings.AutoPartitioningBySize.GetRef());
        if (value == "enabled") {
            partitioningSettings.set_partitioning_by_size(Ydb::FeatureFlag::ENABLED);
        } else if (value == "disabled") {
            partitioningSettings.set_partitioning_by_size(Ydb::FeatureFlag::DISABLED);
        } else {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown feature flag '"
                << metadata->TableSettings.AutoPartitioningBySize.GetRef()
                << "' for auto partitioning by size";
            return false;
        }
    }

    if (metadata->TableSettings.PartitionSizeMb) {
        auto& partitioningSettings = *proto.mutable_partitioning_settings();
        partitioningSettings.set_partition_size_mb(metadata->TableSettings.PartitionSizeMb.GetRef());
    }

    if (metadata->TableSettings.AutoPartitioningByLoad) {
        auto& partitioningSettings = *proto.mutable_partitioning_settings();
        TString value = to_lower(metadata->TableSettings.AutoPartitioningByLoad.GetRef());
        if (value == "enabled") {
            partitioningSettings.set_partitioning_by_load(Ydb::FeatureFlag::ENABLED);
        } else if (value == "disabled") {
            partitioningSettings.set_partitioning_by_load(Ydb::FeatureFlag::DISABLED);
        } else {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown feature flag '"
                << metadata->TableSettings.AutoPartitioningByLoad.GetRef()
                << "' for auto partitioning by load";
            return false;
        }
    }

    if (metadata->TableSettings.MinPartitions) {
        auto& partitioningSettings = *proto.mutable_partitioning_settings();
        partitioningSettings.set_min_partitions_count(metadata->TableSettings.MinPartitions.GetRef());
    }

    if (metadata->TableSettings.MaxPartitions) {
        auto& partitioningSettings = *proto.mutable_partitioning_settings();
        partitioningSettings.set_max_partitions_count(metadata->TableSettings.MaxPartitions.GetRef());
    }

    if (metadata->TableSettings.UniformPartitions) {
        if (metadata->TableSettings.PartitionAtKeys) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Uniform partitions and partitions at keys settings are mutually exclusive."
                << " Use either one of them.";
            return false;
        }
        proto.set_uniform_partitions(metadata->TableSettings.UniformPartitions.GetRef());
    }

    if (metadata->TableSettings.PartitionAtKeys) {
        auto* borders = proto.mutable_partition_at_keys();
        for (const auto& splitPoint : metadata->TableSettings.PartitionAtKeys) {
            auto* border = borders->Addsplit_points();
            auto &keyType = *border->mutable_type()->mutable_tuple_type();
            for (const auto& key : splitPoint) {
                auto* type = keyType.add_elements()->mutable_optional_type()->mutable_item();
                auto* value = border->mutable_value()->add_items();
                if (!ConvertDataSlotToYdbTypedValue(key.first, key.second, type, value)) {
                    code = Ydb::StatusIds::BAD_REQUEST;
                    error = TStringBuilder() << "Unsupported type for PartitionAtKeys: '"
                        << key.first << "'";
                    return false;
                }
            }
        }
    }

    if (metadata->TableSettings.KeyBloomFilter) {
        TString value = to_lower(metadata->TableSettings.KeyBloomFilter.GetRef());
        if (value == "enabled") {
            proto.set_key_bloom_filter(Ydb::FeatureFlag::ENABLED);
        } else if (value == "disabled") {
            proto.set_key_bloom_filter(Ydb::FeatureFlag::DISABLED);
        } else {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown feature flag '"
                << metadata->TableSettings.KeyBloomFilter.GetRef()
                << "' for key bloom filter";
            return false;
        }
    }

    if (metadata->TableSettings.ReadReplicasSettings) {
        if (!NYql::ConvertReadReplicasSettingsToProto(metadata->TableSettings.ReadReplicasSettings.GetRef(),
                *proto.mutable_read_replicas_settings(), code, error)) {
            return false;
        }
    }

    if (const auto& ttl = metadata->TableSettings.TtlSettings) {
        if (ttl.IsSet()) {
            ConvertTtlSettingsToProto(ttl.GetValueSet(), *proto.mutable_ttl_settings());
        } else {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = "Can't reset TTL settings";
            return false;
        }
    }

    if (const auto& tiering = metadata->TableSettings.Tiering) {
        if (tiering.IsSet()) {
            proto.set_tiering(tiering.GetValueSet());
        } else {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = "Can't reset TIERING";
            return false;
        }
    }

    return true;
}

THashMap<TString, TString> GetDefaultFromSequences(NYql::TKikimrTableMetadataPtr metadata) {
    THashMap<TString, TString> sequences;
    for(const auto& [name, column]: metadata->Columns) {
        const auto& seq = column.DefaultFromSequence;
        if (!seq.empty()) {
            sequences.emplace(seq, column.Type);
        }
    }
    return sequences;
}

void FillCreateTableColumnDesc(NKikimrSchemeOp::TTableDescription& tableDesc, const TString& name,
    NYql::TKikimrTableMetadataPtr metadata)
{
    tableDesc.SetName(name);

    Y_ENSURE(metadata->ColumnOrder.size() == metadata->Columns.size());
    for (const auto& name : metadata->ColumnOrder) {
        auto columnIt = metadata->Columns.find(name);
        Y_ENSURE(columnIt != metadata->Columns.end());

        auto& columnDesc = *tableDesc.AddColumns();
        columnDesc.SetName(columnIt->second.Name);
        columnDesc.SetType(columnIt->second.Type);
        columnDesc.SetNotNull(columnIt->second.NotNull);
        if (columnIt->second.Families) {
            columnDesc.SetFamilyName(*columnIt->second.Families.begin());
        }

        const auto& seq = columnIt->second.DefaultFromSequence;
        if (!seq.empty()) {
            columnDesc.SetDefaultFromSequence(seq);
        }
    }

    for (TString& keyColumn : metadata->KeyColumnNames) {
        tableDesc.AddKeyColumnNames(keyColumn);
    }
}

bool FillCreateTableDesc(NYql::TKikimrTableMetadataPtr metadata, NKikimrSchemeOp::TTableDescription& tableDesc,
    const TTableProfiles& profiles, Ydb::StatusIds::StatusCode& code, TString& error, TList<TString>& warnings)
{
    Ydb::Table::CreateTableRequest createTableProto;
    if (!profiles.ApplyTableProfile(*createTableProto.mutable_profile(), tableDesc, code, error)
        || !ConvertCreateTableSettingsToProto(metadata, createTableProto, code, error)) {
        return false;
    }

    TColumnFamilyManager families(tableDesc.MutablePartitionConfig());

    for (const auto& familySettings : createTableProto.column_families()) {
        if (!families.ApplyFamilySettings(familySettings, &code, &error)) {
            return false;
        }
    }

    if (families.Modified && !families.ValidateColumnFamilies(&code, &error)) {
        return false;
    }

    if (!NGRpcService::FillCreateTableSettingsDesc(tableDesc, createTableProto, profiles, code, error, warnings)) {
        return false;
    }
    return true;
}

template <class TResult>
static TFuture<TResult> PrepareUnsupported(const char* name) {
    TResult result;
    result.AddIssue(TIssue({}, TStringBuilder()
        <<"Operation is not supported in current execution mode, check query type. Operation: " << name));
    return MakeFuture(result);
}

template <class TResult>
static TFuture<TResult> PrepareSuccess() {
    TResult result;
    result.SetSuccess();
    return MakeFuture(result);
}

bool IsDdlPrepareAllowed(TKikimrSessionContext& sessionCtx) {
    if (!sessionCtx.Config().EnablePreparedDdl) {
        return false;
    }

    auto queryType = sessionCtx.Query().Type;
    if (queryType != EKikimrQueryType::Query && queryType != EKikimrQueryType::Script) {
        return false;
    }

    return true;
}

#define FORWARD_ENSURE_NO_PREPARE(name, ...) \
    if (IsPrepare()) { \
        return PrepareUnsupported<TGenericResult>(#name); \
    } \
    return Gateway->name(__VA_ARGS__);

#define CHECK_PREPARED_DDL(name) \
    if (SessionCtx && SessionCtx->Query().SuppressDdlChecks) { \
        YQL_ENSURE(SessionCtx->Query().Type == EKikimrQueryType::YqlScript); \
        return PrepareSuccess<TGenericResult>(); \
    } \
    if (IsPrepare() && !IsDdlPrepareAllowed(*SessionCtx)) { \
        return PrepareUnsupported<TGenericResult>(#name); \
    }

class TKqpGatewayProxy : public IKikimrGateway {
public:
    TKqpGatewayProxy(const TIntrusivePtr<IKqpGateway>& gateway,
        const TIntrusivePtr<TKikimrSessionContext>& sessionCtx)
        : Gateway(gateway)
        , SessionCtx(sessionCtx)
    {
        YQL_ENSURE(Gateway);
    }

public:
    bool HasCluster(const TString& cluster) override {
        return Gateway->HasCluster(cluster);
    }

    TVector<TString> GetClusters() override {
        return Gateway->GetClusters();
    }

    TString GetDefaultCluster() override {
        return Gateway->GetDefaultCluster();
    }

    TMaybe<TString> GetSetting(const TString& cluster, const TString& name) override {
        return Gateway->GetSetting(cluster, name);
    }

    void SetToken(const TString& cluster, const TIntrusiveConstPtr<NACLib::TUserToken>& token) override {
        Gateway->SetToken(cluster, token);
    }

    TFuture<TListPathResult> ListPath(const TString& cluster, const TString& path) override {
        return Gateway->ListPath(cluster, path);
    }

    TFuture<TTableMetadataResult> LoadTableMetadata(const TString& cluster, const TString& table,
        TLoadTableMetadataSettings settings) override
    {
        return Gateway->LoadTableMetadata(cluster, table, settings);
    }

    TFuture<TGenericResult> CreateTable(TKikimrTableMetadataPtr metadata, bool createDir) override {
        CHECK_PREPARED_DDL(CreateTable);

        std::pair<TString, TString> pathPair;
        TString error;
        if (!SplitTablePath(metadata->Name, GetDatabase(), pathPair, error, createDir)) {
            return MakeFuture(ResultFromError<TGenericResult>(error));
        }

        bool isPrepare = IsPrepare();
        auto gateway = Gateway;
        auto sessionCtx = SessionCtx;
        auto profilesFuture = Gateway->GetTableProfiles();
        auto tablePromise = NewPromise<TGenericResult>();
        profilesFuture.Subscribe([gateway, sessionCtx, metadata, tablePromise, pathPair, isPrepare]
            (const TFuture<IKqpGateway::TKqpTableProfilesResult>& future) mutable {
                auto profilesResult = future.GetValue();
                if (!profilesResult.Success()) {
                    tablePromise.SetValue(ResultFromIssues<TGenericResult>(profilesResult.Status(),
                        profilesResult.Issues()));
                    return;
                }

                NKikimrSchemeOp::TModifyScheme schemeTx;
                schemeTx.SetWorkingDir(pathPair.first);
                const auto sequences = GetDefaultFromSequences(metadata);

                NKikimrSchemeOp::TTableDescription* tableDesc = nullptr;
                if (!metadata->Indexes.empty() || !sequences.empty()) {
                    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateIndexedTable);
                    tableDesc = schemeTx.MutableCreateIndexedTable()->MutableTableDescription();
                    for (const auto& index : metadata->Indexes) {
                        auto indexDesc = schemeTx.MutableCreateIndexedTable()->AddIndexDescription();
                        indexDesc->SetName(index.Name);
                        switch (index.Type) {
                            case NYql::TIndexDescription::EType::GlobalSync:
                                indexDesc->SetType(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
                                break;
                            case NYql::TIndexDescription::EType::GlobalAsync:
                                indexDesc->SetType(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalAsync);
                                break;
                        }

                        indexDesc->SetState(static_cast<::NKikimrSchemeOp::EIndexState>(index.State));
                        for (const auto& col : index.KeyColumns) {
                            indexDesc->AddKeyColumnNames(col);
                        }
                        for (const auto& col : index.DataColumns) {
                            indexDesc->AddDataColumnNames(col);
                        }
                    }
                    FillCreateTableColumnDesc(*tableDesc, pathPair.second, metadata);
                    for(const auto& [seq, seqType]: sequences) {
                        auto seqDesc = schemeTx.MutableCreateIndexedTable()->MutableSequenceDescription()->Add();
                        seqDesc->SetName(seq);
                        const auto type = to_lower(seqType);
                        seqDesc->SetMinValue(1);
                        if (type == "int64") {
                            seqDesc->SetMaxValue(9223372036854775807);
                        } else if (type == "int32") {
                            seqDesc->SetMaxValue(2147483647);
                        } else if (type == "int16") {
                            seqDesc->SetMaxValue(32767);
                        }
                        seqDesc->SetCycle(false);
                    }

                } else {
                    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTable);
                    tableDesc = schemeTx.MutableCreateTable();
                    FillCreateTableColumnDesc(*tableDesc, pathPair.second, metadata);
                }

                Ydb::StatusIds::StatusCode code;
                TList<TString> warnings;
                TString error;
                if (!FillCreateTableDesc(metadata, *tableDesc, profilesResult.Profiles, code, error, warnings)) {
                    IKqpGateway::TGenericResult errResult;
                    errResult.AddIssue(NYql::TIssue(error));
                    errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                    tablePromise.SetValue(errResult);
                    return;
                }

                if (isPrepare) {
                    auto& phyQuery = *sessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                    auto& phyTx = *phyQuery.AddTransactions();
                    phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                    phyTx.MutableSchemeOperation()->MutableCreateTable()->Swap(&schemeTx);

                    TGenericResult result;
                    result.SetSuccess();
                    tablePromise.SetValue(result);
                } else {
                    gateway->ModifyScheme(std::move(schemeTx)).Subscribe([tablePromise, warnings]
                        (const TFuture<TGenericResult>& future) mutable {
                            auto result = future.GetValue();
                            for (const auto& warning : warnings) {
                                result.AddIssue(
                                    NYql::TIssue(warning).SetCode(NKikimrIssues::TIssuesIds::WARNING,
                                        NYql::TSeverityIds::S_WARNING)
                                );
                            }

                            tablePromise.SetValue(result);
                        });
                }
            });

        return tablePromise.GetFuture();
    }

    TFuture<TGenericResult> AlterTable(const TString& cluster, Ydb::Table::AlterTableRequest&& req,
        const TMaybe<TString>& requestType) override
    {
        FORWARD_ENSURE_NO_PREPARE(AlterTable, cluster, std::move(req), requestType);
    }

    TFuture<TGenericResult> RenameTable(const TString& src, const TString& dst, const TString& cluster) override {
        FORWARD_ENSURE_NO_PREPARE(RenameTable, src, dst, cluster);
    }

    TFuture<TGenericResult> DropTable(const TString& cluster, const TString& table) override {
        FORWARD_ENSURE_NO_PREPARE(DropTable, cluster, table);
    }

    TFuture<TGenericResult> CreateTopic(const TString& cluster, Ydb::Topic::CreateTopicRequest&& request) override {
        FORWARD_ENSURE_NO_PREPARE(CreateTopic, cluster, std::move(request));
    }

    TFuture<TGenericResult> AlterTopic(const TString& cluster, Ydb::Topic::AlterTopicRequest&& request) override {
        FORWARD_ENSURE_NO_PREPARE(AlterTopic, cluster, std::move(request));
    }

    TFuture<TGenericResult> DropTopic(const TString& cluster, const TString& topic) override {
        FORWARD_ENSURE_NO_PREPARE(DropTopic, cluster, topic);
    }

    TFuture<TGenericResult> ModifyPermissions(const TString& cluster,
        const TModifyPermissionsSettings& settings) override
    {
        FORWARD_ENSURE_NO_PREPARE(ModifyPermissions, cluster, settings);
    }

    TFuture<TGenericResult> CreateUser(const TString& cluster, const TCreateUserSettings& settings) override {
        FORWARD_ENSURE_NO_PREPARE(CreateUser, cluster, settings);
    }

    TFuture<TGenericResult> AlterUser(const TString& cluster, const TAlterUserSettings& settings) override {
        FORWARD_ENSURE_NO_PREPARE(AlterUser, cluster, settings);
    }

    TFuture<TGenericResult> DropUser(const TString& cluster, const TDropUserSettings& settings) override {
        FORWARD_ENSURE_NO_PREPARE(DropUser, cluster, settings);
    }

    TFuture<TGenericResult> UpsertObject(const TString& cluster, const TUpsertObjectSettings& settings) override {
        FORWARD_ENSURE_NO_PREPARE(UpsertObject, cluster, settings);
    }

    TFuture<TGenericResult> CreateObject(const TString& cluster, const TCreateObjectSettings& settings) override {
        FORWARD_ENSURE_NO_PREPARE(CreateObject, cluster, settings);
    }

    TFuture<TGenericResult> AlterObject(const TString& cluster, const TAlterObjectSettings& settings) override {
        FORWARD_ENSURE_NO_PREPARE(AlterObject, cluster, settings);
    }

    TFuture<TGenericResult> DropObject(const TString& cluster, const TDropObjectSettings& settings) override {
        FORWARD_ENSURE_NO_PREPARE(DropObject, cluster, settings);
    }

    TFuture<TGenericResult> CreateGroup(const TString& cluster, const TCreateGroupSettings& settings) override {
        FORWARD_ENSURE_NO_PREPARE(CreateGroup, cluster, settings);
    }

    TFuture<TGenericResult> AlterGroup(const TString& cluster, TAlterGroupSettings& settings) override {
        FORWARD_ENSURE_NO_PREPARE(AlterGroup, cluster, settings);
    }

    TFuture<TGenericResult> DropGroup(const TString& cluster, const TDropGroupSettings& settings) override {
        FORWARD_ENSURE_NO_PREPARE(DropGroup, cluster, settings);
    }

    TFuture<TGenericResult> CreateColumnTable(TKikimrTableMetadataPtr metadata, bool createDir) override {
        FORWARD_ENSURE_NO_PREPARE(CreateColumnTable, metadata, createDir);
    }

    TFuture<TGenericResult> AlterColumnTable(const TString& cluster,
        const TAlterColumnTableSettings& settings) override
    {
        FORWARD_ENSURE_NO_PREPARE(AlterColumnTable, cluster, settings);
    }

    TFuture<TGenericResult> CreateTableStore(const TString& cluster,
        const TCreateTableStoreSettings& settings) override
    {
        FORWARD_ENSURE_NO_PREPARE(CreateTableStore, cluster, settings);
    }

    TFuture<TGenericResult> AlterTableStore(const TString& cluster,
        const TAlterTableStoreSettings& settings) override
    {
        FORWARD_ENSURE_NO_PREPARE(AlterTableStore, cluster, settings);
    }

    TFuture<TGenericResult> DropTableStore(const TString& cluster,
        const TDropTableStoreSettings& settings) override
    {
        FORWARD_ENSURE_NO_PREPARE(DropTableStore, cluster, settings);
    }

    TFuture<TGenericResult> CreateExternalTable(const TString& cluster, const TCreateExternalTableSettings& settings,
        bool createDir) override
    {
        FORWARD_ENSURE_NO_PREPARE(CreateExternalTable, cluster, settings, createDir);
    }

    TFuture<TGenericResult> AlterExternalTable(const TString& cluster,
        const TAlterExternalTableSettings& settings) override
    {
        FORWARD_ENSURE_NO_PREPARE(AlterExternalTable, cluster, settings);
    }

    TFuture<TGenericResult> DropExternalTable(const TString& cluster,
        const TDropExternalTableSettings& settings) override
    {
        FORWARD_ENSURE_NO_PREPARE(DropExternalTable, cluster, settings);
    }

    TFuture<TGenericResult> CreateExternalDataSource(const TString& cluster,
        const TCreateExternalDataSourceSettings& settings, bool createDir) override
    {
        FORWARD_ENSURE_NO_PREPARE(CreateExternalDataSource, cluster, settings, createDir);
    }

    TFuture<TGenericResult> AlterExternalDataSource(const TString& cluster,
        const TAlterExternalDataSourceSettings& settings) override
    {
        FORWARD_ENSURE_NO_PREPARE(AlterExternalDataSource, cluster, settings);
    }

    TFuture<TGenericResult> DropExternalDataSource(const TString& cluster,
        const TDropExternalDataSourceSettings& settings) override
    {
        FORWARD_ENSURE_NO_PREPARE(DropExternalDataSource, cluster, settings);
    }

    TVector<TString> GetCollectedSchemeData() override {
        return Gateway->GetCollectedSchemeData();
    }

    TFuture<TExecuteLiteralResult> ExecuteLiteral(const TString& program, 
        const NKikimrMiniKQL::TType& resultType, NKikimr::NKqp::TTxAllocatorState::TPtr txAlloc) override 
    {
        return Gateway->ExecuteLiteral(program, resultType, txAlloc);
    }


private:
    bool IsPrepare() const {
        if (!SessionCtx) {
            return false;
        }

        return SessionCtx->Query().PrepareOnly;
    }

    TString GetDatabase() const {
        if (SessionCtx) {
            return SessionCtx->GetDatabase();
        }

        return Gateway->GetDatabase();
    }

private:
    TIntrusivePtr<IKqpGateway> Gateway;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
};

#undef FORWARD_ENSURE_NO_PREPARE
#undef CHECK_PREPARED_DDL

} // namespace

TIntrusivePtr<IKikimrGateway> CreateKqpGatewayProxy(const TIntrusivePtr<IKqpGateway>& gateway,
    const TIntrusivePtr<TKikimrSessionContext>& sessionCtx)
{
    return MakeIntrusive<TKqpGatewayProxy>(gateway, sessionCtx);
}

} // namespace NKikimr::NKqp
