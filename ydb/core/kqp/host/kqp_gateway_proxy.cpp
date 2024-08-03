#include "kqp_host_impl.h"

#include <ydb/core/grpc_services/table_settings.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/column_families.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/parser.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/lib/actors/pq_schema_actor.h>

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
    case NYql::EDataSlot::Date32:
        toType->set_type_id(Ydb::Type::DATE32);
        toValue->set_int32_value(FromString<i32>(fromValue));
        break;
    case NYql::EDataSlot::Datetime64:
        toType->set_type_id(Ydb::Type::DATETIME64);
        toValue->set_int64_value(FromString<i64>(fromValue));
        break;
    case NYql::EDataSlot::Timestamp64:
        toType->set_type_id(Ydb::Type::TIMESTAMP64);
        toValue->set_int64_value(FromString<i64>(fromValue));
        break;
    case NYql::EDataSlot::Interval64:
        toType->set_type_id(Ydb::Type::INTERVAL64);
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

    if (metadata->TableSettings.StoreExternalBlobs) {
        auto& storageSettings = *proto.mutable_storage_settings();
        TString value = to_lower(metadata->TableSettings.StoreExternalBlobs.GetRef());
        if (value == "enabled") {
            storageSettings.set_store_external_blobs(Ydb::FeatureFlag::ENABLED);
        } else if (value == "disabled") {
            storageSettings.set_store_external_blobs(Ydb::FeatureFlag::DISABLED);
        } else {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown feature flag '"
                << metadata->TableSettings.StoreExternalBlobs.GetRef()
                << "' for store external blobs";
            return false;
        }
    }

    proto.set_temporary(metadata->Temporary);

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
        const auto& cMeta = columnIt->second;

        auto& columnDesc = *tableDesc.AddColumns();
        columnDesc.SetName(columnIt->second.Name);
        columnDesc.SetType(columnIt->second.Type);
        columnDesc.SetNotNull(columnIt->second.NotNull);
        if (columnIt->second.Families) {
            columnDesc.SetFamilyName(*columnIt->second.Families.begin());
        }

        if (cMeta.IsDefaultFromSequence()) {
            columnDesc.SetDefaultFromSequence(
                cMeta.DefaultFromSequence);
        }

        if (cMeta.IsDefaultFromLiteral()) {
            columnDesc.MutableDefaultFromLiteral()->CopyFrom(
                cMeta.DefaultFromLiteral);
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

template <typename T>
void FillColumnTableSchema(NKikimrSchemeOp::TColumnTableSchema& schema, const T& metadata)
{
    Y_ENSURE(metadata.ColumnOrder.size() == metadata.Columns.size());
    for (const auto& name : metadata.ColumnOrder) {
        auto columnIt = metadata.Columns.find(name);
        Y_ENSURE(columnIt != metadata.Columns.end());

        NKikimrSchemeOp::TOlapColumnDescription& columnDesc = *schema.AddColumns();
        columnDesc.SetName(columnIt->second.Name);
        columnDesc.SetType(columnIt->second.Type);
        columnDesc.SetNotNull(columnIt->second.NotNull);
    }

    for (const auto& keyColumn : metadata.KeyColumnNames) {
        schema.AddKeyColumnNames(keyColumn);
    }

    schema.SetEngine(NKikimrSchemeOp::EColumnTableEngine::COLUMN_ENGINE_REPLACING_TIMESERIES);
}

bool FillCreateColumnTableDesc(NYql::TKikimrTableMetadataPtr metadata,
        NKikimrSchemeOp::TColumnTableDescription& tableDesc, Ydb::StatusIds::StatusCode& code, TString& error)
{
    if (metadata->Columns.empty()) {
        tableDesc.SetSchemaPresetName("default");
    }

    auto& hashSharding = *tableDesc.MutableSharding()->MutableHashSharding();

    for (const TString& column : metadata->TableSettings.PartitionBy) {
        if (!metadata->Columns.count(column)) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown column '" << column << "' in partition by key";
            return false;
        }

        hashSharding.AddColumns(column);
    }

    if (metadata->TableSettings.PartitionByHashFunction) {
        if (to_lower(metadata->TableSettings.PartitionByHashFunction.GetRef()) == "cloud_logs") {
            hashSharding.SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CLOUD_LOGS);
        } else if (to_lower(metadata->TableSettings.PartitionByHashFunction.GetRef()) == "consistency_hash_64") {
            hashSharding.SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64);
        } else if (to_lower(metadata->TableSettings.PartitionByHashFunction.GetRef()) == "modulo_n") {
            hashSharding.SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_MODULO_N);
        } else {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown hash function '"
                << metadata->TableSettings.PartitionByHashFunction.GetRef() << "' to partition by";
            return false;
        }
    } else {
        hashSharding.SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64);
    }

    if (metadata->TableSettings.MinPartitions) {
        tableDesc.SetColumnShardCount(*metadata->TableSettings.MinPartitions);
    }

    if (metadata->TableSettings.TtlSettings.Defined() && metadata->TableSettings.TtlSettings.IsSet()) {
        const auto& inputSettings = metadata->TableSettings.TtlSettings.GetValueSet();
        auto& resultSettings = *tableDesc.MutableTtlSettings();
        resultSettings.MutableEnabled()->SetColumnName(inputSettings.ColumnName);
        resultSettings.MutableEnabled()->SetExpireAfterSeconds(inputSettings.ExpireAfter.Seconds());
        if (inputSettings.ColumnUnit) {
            resultSettings.MutableEnabled()->SetColumnUnit(static_cast<NKikimrSchemeOp::TTTLSettings::EUnit>(*inputSettings.ColumnUnit));
        }
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
        const TIntrusivePtr<TKikimrSessionContext>& sessionCtx,
        TActorSystem* actorSystem)
        : Gateway(gateway)
        , SessionCtx(sessionCtx)
        , ActorSystem(actorSystem)
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

    bool GetDatabaseForLoginOperation(TString& database) {
        return NSchemeHelpers::SetDatabaseForLoginOperation(database, GetDomainLoginOnly(), GetDomainName(), GetDatabase());
    }

    TFuture<TListPathResult> ListPath(const TString& cluster, const TString& path) override {
        return Gateway->ListPath(cluster, path);
    }

    TFuture<TTableMetadataResult> LoadTableMetadata(const TString& cluster, const TString& table,
        TLoadTableMetadataSettings settings) override
    {
        return Gateway->LoadTableMetadata(cluster, table, settings);
    }

    TFuture<TGenericResult> CreateTable(TKikimrTableMetadataPtr metadata, bool createDir, bool existingOk, bool replaceIfExists) override {
        Y_UNUSED(replaceIfExists);
        CHECK_PREPARED_DDL(CreateTable);

        std::pair<TString, TString> pathPair;
        TString error;
        if (!NSchemeHelpers::SplitTablePath(metadata->Name, GetDatabase(), pathPair, error, createDir)) {
            return MakeFuture(ResultFromError<TGenericResult>(error));
        }

        bool isPrepare = IsPrepare();
        auto gateway = Gateway;
        auto sessionCtx = SessionCtx;
        auto profilesFuture = Gateway->GetTableProfiles();
        auto tablePromise = NewPromise<TGenericResult>();
        auto temporary = metadata->Temporary;
        profilesFuture.Subscribe([gateway, sessionCtx, metadata, tablePromise, pathPair, isPrepare, temporary, existingOk]
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
                            case NYql::TIndexDescription::EType::GlobalSyncUnique:
                                indexDesc->SetType(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
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
                    if (sequences.size() > 0 && !sessionCtx->Config().EnableSequences) {
                        IKqpGateway::TGenericResult errResult;
                        errResult.AddIssue(NYql::TIssue("Sequences are not supported yet."));
                        errResult.SetStatus(NYql::YqlStatusFromYdbStatus(Ydb::StatusIds::UNSUPPORTED));
                        tablePromise.SetValue(errResult);
                        return;
                    }

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
                    schemeTx.SetFailedOnAlreadyExists(!existingOk);
                    phyTx.MutableSchemeOperation()->MutableCreateTable()->Swap(&schemeTx);

                    TGenericResult result;
                    result.SetSuccess();
                    tablePromise.SetValue(result);
                } else {
                    if (temporary) {
                        auto code = Ydb::StatusIds::BAD_REQUEST;
                        auto error = TStringBuilder() << "Not allowed to create temp table";
                        IKqpGateway::TGenericResult errResult;
                        errResult.AddIssue(NYql::TIssue(error));
                        errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                        tablePromise.SetValue(errResult);
                    }
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

    TFuture<TGenericResult> PrepareAlterTable(const TString&, Ydb::Table::AlterTableRequest&& req,
        const TMaybe<TString>&, ui64 flags, NKikimrIndexBuilder::TIndexBuildSettings&& buildSettings)
    {
        YQL_ENSURE(SessionCtx->Query().PreparingQuery);
        auto promise = NewPromise<TGenericResult>();
        const auto ops = GetAlterOperationKinds(&req);
        if (ops.size() != 1) {
            auto code = Ydb::StatusIds::BAD_REQUEST;
            auto error = TStringBuilder() << "Unqualified alter table request.";
            IKqpGateway::TGenericResult errResult;
            errResult.AddIssue(NYql::TIssue(error));
            errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
            promise.SetValue(errResult);
            return promise.GetFuture();
        }

        const auto opType = *ops.begin();
        auto tablePromise = NewPromise<TGenericResult>();
        if (opType == EAlterOperationKind::AddIndex) {
            auto &phyQuery =
                *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto &phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
            auto buildOp = phyTx.MutableSchemeOperation()->MutableBuildOperation();
            Ydb::StatusIds::StatusCode code;
            TString error;
            if (!BuildAlterTableAddIndexRequest(&req, buildOp, flags, code, error)) {
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue(error));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                tablePromise.SetValue(errResult);
                return tablePromise.GetFuture();
            }
            TGenericResult result;
            result.SetSuccess();
            tablePromise.SetValue(result);
            return tablePromise.GetFuture();
        }

        auto profilesFuture = Gateway->GetTableProfiles();
        auto sessionCtx = SessionCtx;
        profilesFuture.Subscribe(
            [tablePromise, sessionCtx, alterReq = std::move(req), buildSettings = std::move(buildSettings)](
                const TFuture<IKqpGateway::TKqpTableProfilesResult> &future) mutable {
                auto profilesResult = future.GetValue();
                if (!profilesResult.Success()) {
                    tablePromise.SetValue(ResultFromIssues<TGenericResult>(
                        profilesResult.Status(), profilesResult.Issues()));
                    return;
                }

                auto &phyQuery =
                    *sessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto &phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

                NKikimrSchemeOp::TModifyScheme modifyScheme;


                const TPathId invalidPathId;
                Ydb::StatusIds::StatusCode code;
                TString error;
                if (!BuildAlterTableModifyScheme(&alterReq, &modifyScheme, profilesResult.Profiles, invalidPathId, code, error)) {
                    IKqpGateway::TGenericResult errResult;
                    errResult.AddIssue(NYql::TIssue(error));
                    errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                    tablePromise.SetValue(errResult);
                    return;
                }

                if (buildSettings.has_column_build_operation()) {
                    buildSettings.MutableAlterMainTablePayload()->PackFrom(modifyScheme);
                    phyTx.MutableSchemeOperation()->MutableBuildOperation()->CopyFrom(buildSettings);
                } else {
                    phyTx.MutableSchemeOperation()->MutableAlterTable()->CopyFrom(modifyScheme);
                }

                TGenericResult result;
                result.SetSuccess();
                tablePromise.SetValue(result);
            });

        return tablePromise.GetFuture();
    }

    TFuture<TGenericResult> SendSchemeExecuterRequest(const TString& cluster, const TMaybe<TString>& requestType,
        const std::shared_ptr<const NKikimr::NKqp::TKqpPhyTxHolder> &phyTx) override
    {
        return Gateway->SendSchemeExecuterRequest(cluster, requestType, phyTx);
    }

    TFuture<TGenericResult> AlterTable(const TString& cluster, Ydb::Table::AlterTableRequest&& req,
        const TMaybe<TString>& requestType, ui64 flags, NKikimrIndexBuilder::TIndexBuildSettings&& buildSettings) override
    {
        CHECK_PREPARED_DDL(AlterTable);

        auto tablePromise = NewPromise<TGenericResult>();

        if (!IsPrepare()) {
            SessionCtx->Query().PrepareOnly = false;
            if (!SessionCtx->Query().PreparingQuery) {
                SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
            }

            if (SessionCtx->Query().PreparingQuery->MutablePhysicalQuery()->GetTransactions().size() > 0) {
                auto code = Ydb::StatusIds::BAD_REQUEST;
                auto error = TStringBuilder() << "multiple transactions are not supported for alter table operation.";
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue(error));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                tablePromise.SetValue(errResult);
                return tablePromise.GetFuture();
            }
        }

        auto prepareFuture = PrepareAlterTable(cluster, std::move(req), requestType, flags, std::move(buildSettings));
        if (IsPrepare())
            return prepareFuture;

        auto sessionCtx = SessionCtx;
        auto gateway = Gateway;
        prepareFuture.Subscribe([cluster, requestType, tablePromise, sessionCtx, gateway](const TFuture<IKqpGateway::TGenericResult> &future) mutable {
            auto result = future.GetValue();
            TPreparedQueryHolder::TConstPtr preparedQuery = std::make_shared<TPreparedQueryHolder>(sessionCtx->Query().PreparingQuery.release(), nullptr);
            if (result.Success()) {
                auto executeFuture = gateway->SendSchemeExecuterRequest(cluster, requestType, preparedQuery->GetPhyTx(0));
                executeFuture.Subscribe([tablePromise](const TFuture<IKqpGateway::TGenericResult> &future) mutable {
                    auto fresult = future.GetValue();
                    if (fresult.Success()) {
                        TGenericResult result;
                        result.SetSuccess();
                        tablePromise.SetValue(result);
                    } else {
                        tablePromise.SetValue(
                            ResultFromIssues<TGenericResult>(fresult.Status(), fresult.Issues())
                        );
                    }
                });
                return;
            } else {
                tablePromise.SetValue(ResultFromIssues<TGenericResult>(
                    result.Status(), result.Issues()));

                return;
            }
        });

        return tablePromise.GetFuture();
    }

    TFuture<TGenericResult> RenameTable(const TString& src, const TString& dst, const TString& cluster) override {
        CHECK_PREPARED_DDL(RenameTable);

        auto metadata = SessionCtx->Tables().GetTable(cluster, src).Metadata;

        std::pair<TString, TString> pathPair;
        TString error;
        if (!NSchemeHelpers::SplitTablePath(metadata->Name, GetDatabase(), pathPair, error, false)) {
            return MakeFuture(ResultFromError<TGenericResult>(error));
        }

        auto temporary = metadata->Temporary;
        auto renameTablePromise = NewPromise<TGenericResult>();

        NKikimrSchemeOp::TModifyScheme schemeTx;
        schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpMoveTable);
        schemeTx.SetWorkingDir(pathPair.first);

        auto* renameTable = schemeTx.MutableMoveTable();
        renameTable->SetSrcPath(src);
        renameTable->SetDstPath(dst);

        if (IsPrepare()) {
            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);


            phyTx.MutableSchemeOperation()->MutableAlterTable()->Swap(&schemeTx);
            TGenericResult result;
            result.SetSuccess();
            renameTablePromise.SetValue(result);
        } else {
            if (temporary) {
                auto code = Ydb::StatusIds::BAD_REQUEST;
                auto error = TStringBuilder() << "Not allowed to rename temp table";
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue(error));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                renameTablePromise.SetValue(errResult);
            }
            return Gateway->RenameTable(src, dst, cluster);
        }

        return renameTablePromise.GetFuture();
    }

    TFuture<TGenericResult> DropTable(const TString& cluster, const TDropTableSettings& settings) override {
        CHECK_PREPARED_DDL(DropTable);

        auto metadata = SessionCtx->Tables().GetTable(cluster, settings.Table).Metadata;

        std::pair<TString, TString> pathPair;
        TString error;
        if (!NSchemeHelpers::SplitTablePath(metadata->Name, GetDatabase(), pathPair, error, false)) {
            return MakeFuture(ResultFromError<TGenericResult>(error));
        }

        auto temporary = metadata->Temporary;
        auto dropPromise = NewPromise<TGenericResult>();

        NKikimrSchemeOp::TModifyScheme schemeTx;
        schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTable);
        schemeTx.SetWorkingDir(pathPair.first);

        auto* drop = schemeTx.MutableDrop();
        drop->SetName(pathPair.second);

        if (IsPrepare()) {
            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);


            phyTx.MutableSchemeOperation()->MutableDropTable()->Swap(&schemeTx);
            phyTx.MutableSchemeOperation()->MutableDropTable()->SetSuccessOnNotExist(settings.SuccessOnNotExist);
            TGenericResult result;
            result.SetSuccess();
            dropPromise.SetValue(result);
        } else {
            if (temporary) {
                auto code = Ydb::StatusIds::BAD_REQUEST;
                auto error = TStringBuilder() << "Not allowed to drop temp table";
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue(error));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                dropPromise.SetValue(errResult);
            }
            return Gateway->DropTable(cluster, settings);
        }

        return dropPromise.GetFuture();
    }

    TFuture<TGenericResult> CreateTopic(const TString& cluster, Ydb::Topic::CreateTopicRequest&& request, bool existingOk) override {
        CHECK_PREPARED_DDL(CreateTopic);
        Y_UNUSED(cluster);

        std::pair<TString, TString> pathPair;
        TString error;
        auto createPromise = NewPromise<TGenericResult>();
        if (!NSchemeHelpers::SplitTablePath(request.path(), GetDatabase(), pathPair, error, false)) {
            return MakeFuture(ResultFromError<TGenericResult>(error));
        }
        NKikimrSchemeOp::TModifyScheme schemeTx;
        schemeTx.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup);

        schemeTx.SetWorkingDir(pathPair.first);

        auto pqDescr = schemeTx.MutableCreatePersQueueGroup();
        pqDescr->SetName(pathPair.second);
        NKikimr::NGRpcProxy::V1::FillProposeRequestImpl(pathPair.second, request, schemeTx, AppData(ActorSystem), error, pathPair.first);
        TList<TString> warnings;

        if (IsPrepare()) {
            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);


            phyTx.MutableSchemeOperation()->MutableCreateTopic()->Swap(&schemeTx);
            phyTx.MutableSchemeOperation()->MutableCreateTopic()->SetFailedOnAlreadyExists(!existingOk);
            TGenericResult result;
            result.SetSuccess();
            createPromise.SetValue(result);
        } else {
            Gateway->ModifyScheme(std::move(schemeTx)).Subscribe([createPromise, warnings]
                        (const TFuture<TGenericResult>& future) mutable {
                            auto result = future.GetValue();
                            for (const auto& warning : warnings) {
                                result.AddIssue(
                                    NYql::TIssue(warning).SetCode(NKikimrIssues::TIssuesIds::WARNING,
                                        NYql::TSeverityIds::S_WARNING)
                                );
                            }
                            createPromise.SetValue(result);
                        });
        }
        return createPromise.GetFuture();
    }

    TFuture<TGenericResult> AlterTopic(const TString& cluster, Ydb::Topic::AlterTopicRequest&& request, bool missingOk) override {
        CHECK_PREPARED_DDL(AlterTopic);
        Y_UNUSED(cluster);
        std::pair<TString, TString> pathPair;
        TString error;
        if (!NSchemeHelpers::SplitTablePath(request.path(), GetDatabase(), pathPair, error, false)) {
            return MakeFuture(ResultFromError<TGenericResult>(error));
        }
        //pqDescr->SetName(pathPair.second);
        TList<TString> warnings;
        auto alterPromise = NewPromise<TGenericResult>();

        if (IsPrepare()) {
            TAlterTopicSettings settings{std::move(request), pathPair.second, pathPair.first, missingOk};
            auto getModifySchemeFuture = Gateway->AlterTopicPrepared(std::move(settings));


            auto* phyQuery = SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();

            getModifySchemeFuture.Subscribe([=] (const auto future) mutable {
                TGenericResult result;
                auto modifySchemeResult = future.GetValue();
                if (modifySchemeResult.Status == Ydb::StatusIds::SUCCESS) {
                    if (modifySchemeResult.ModifyScheme.HasAlterPersQueueGroup()) {
                        auto* phyTx = phyQuery->AddTransactions();
                        phyTx->SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                        phyTx->MutableSchemeOperation()->MutableAlterTopic()->Swap(&modifySchemeResult.ModifyScheme);
                        phyTx->MutableSchemeOperation()->MutableAlterTopic()->SetSuccessOnNotExist(missingOk);
                    }
                    result.SetSuccess();

                } else {
                    result.SetStatus(NYql::YqlStatusFromYdbStatus(modifySchemeResult.Status));
                    result.AddIssues(modifySchemeResult.Issues);
                }
                alterPromise.SetValue(result);
            });

        } else {
            return Gateway->AlterTopic(cluster, std::move(request), missingOk);
        }
        return alterPromise.GetFuture();

    }

    NThreading::TFuture<NKikimr::NGRpcProxy::V1::TAlterTopicResponse> AlterTopicPrepared(TAlterTopicSettings&& settings) override {
        return Gateway->AlterTopicPrepared(std::move(settings));
    }

    TFuture<TGenericResult> DropTopic(const TString& cluster, const TString& topic, bool missingOk) override {
        CHECK_PREPARED_DDL(DropTopic);
        Y_UNUSED(cluster);

        std::pair<TString, TString> pathPair;
        TString error;
        auto dropPromise = NewPromise<TGenericResult>();
        if (!NSchemeHelpers::SplitTablePath(topic, GetDatabase(), pathPair, error, false)) {
            return MakeFuture(ResultFromError<TGenericResult>(error));
        }

        if (IsPrepare()) {
            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup);

            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.MutableDrop()->SetName(pathPair.second);

            phyTx.MutableSchemeOperation()->MutableDropTopic()->Swap(&schemeTx);
            phyTx.MutableSchemeOperation()->MutableDropTopic()->SetSuccessOnNotExist(missingOk);
            TGenericResult result;
            result.SetSuccess();
            dropPromise.SetValue(result);
        } else {
            return Gateway->DropTopic(cluster, topic, missingOk);

        }
        return dropPromise.GetFuture();
    }

    TFuture<TGenericResult> ModifyPermissions(const TString& cluster,
        const TModifyPermissionsSettings& settings) override
    {
        CHECK_PREPARED_DDL(ModifyPermissions);

        if (IsPrepare()) {
            auto modifyPermissionsPromise = NewPromise<TGenericResult>();

            if (settings.Permissions.empty() && !settings.IsPermissionsClear) {
                return MakeFuture(ResultFromError<TGenericResult>("No permissions names for modify permissions"));
            }

            if (settings.Paths.empty()) {
                return MakeFuture(ResultFromError<TGenericResult>("No paths for modify permissions"));
            }

            if (settings.Roles.empty()) {
                return MakeFuture(ResultFromError<TGenericResult>("No roles for modify permissions"));
            }

            NACLib::TDiffACL acl;
            switch (settings.Action) {
                case NYql::TModifyPermissionsSettings::EAction::Grant: {
                    for (const auto& sid : settings.Roles) {
                        for (const auto& permission : settings.Permissions) {
                            TACLAttrs aclAttrs = ConvertYdbPermissionNameToACLAttrs(permission);
                            acl.AddAccess(NACLib::EAccessType::Allow, aclAttrs.AccessMask, sid, aclAttrs.InheritanceType);
                        }
                    }
                    break;
                }
                case NYql::TModifyPermissionsSettings::EAction::Revoke: {
                    if (settings.IsPermissionsClear) {
                        for (const auto& sid : settings.Roles) {
                            acl.ClearAccessForSid(sid);
                        }
                    } else {
                        for (const auto& sid : settings.Roles) {
                            for (const auto& permission : settings.Permissions) {
                                TACLAttrs aclAttrs = ConvertYdbPermissionNameToACLAttrs(permission);
                                acl.RemoveAccess(NACLib::EAccessType::Allow, aclAttrs.AccessMask, sid, aclAttrs.InheritanceType);
                            }
                        }
                    }
                    break;
                }
                default: {
                    return MakeFuture(ResultFromError<TGenericResult>("Unknown permission action"));
                }
            }

            const auto serializedDiffAcl = acl.SerializeAsString();

            TVector<std::pair<const TString*, std::pair<TString, TString>>> pathPairs;
            pathPairs.reserve(settings.Paths.size());
            for (const auto& path : settings.Paths) {
                pathPairs.push_back(std::make_pair(&path, NSchemeHelpers::SplitPathByDirAndBaseNames(path)));
            }

            for (const auto& path : pathPairs) {
                const auto& [dirname, basename] = path.second;

                NKikimrSchemeOp::TModifyScheme schemeTx;
                schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpModifyACL);
                schemeTx.SetWorkingDir(dirname);
                schemeTx.MutableModifyACL()->SetName(basename);
                schemeTx.MutableModifyACL()->SetDiffACL(serializedDiffAcl);

                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableModifyPermissions()->Swap(&schemeTx);
            }

            TGenericResult result;
            result.SetSuccess();
            modifyPermissionsPromise.SetValue(result);
            return modifyPermissionsPromise;
        } else {
            return Gateway->ModifyPermissions(cluster, settings);
        }
    }

    TFuture<TGenericResult> CreateUser(const TString& cluster, const TCreateUserSettings& settings) override {
        CHECK_PREPARED_DDL(CreateUser);

        if (IsPrepare()) {
            auto createUserPromise = NewPromise<TGenericResult>();

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& createUser = *schemeTx.MutableAlterLogin()->MutableCreateUser();
            createUser.SetUser(settings.UserName);
            if (settings.Password) {
                createUser.SetPassword(settings.Password);
            }

            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            phyTx.MutableSchemeOperation()->MutableCreateUser()->Swap(&schemeTx);

            TGenericResult result;
            result.SetSuccess();
            createUserPromise.SetValue(result);
            return createUserPromise.GetFuture();
        } else {
            return Gateway->CreateUser(cluster, settings);
        }
    }

    TFuture<TGenericResult> AlterUser(const TString& cluster, const TAlterUserSettings& settings) override {
        CHECK_PREPARED_DDL(AlterUser);

        if (IsPrepare()) {
            auto alterUserPromise = NewPromise<TGenericResult>();

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& alterUser = *schemeTx.MutableAlterLogin()->MutableModifyUser();
            alterUser.SetUser(settings.UserName);
            if (settings.Password) {
                alterUser.SetPassword(settings.Password);
            }

            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            phyTx.MutableSchemeOperation()->MutableAlterUser()->Swap(&schemeTx);
            TGenericResult result;
            result.SetSuccess();
            alterUserPromise.SetValue(result);
            return alterUserPromise.GetFuture();
        } else {
            return Gateway->AlterUser(cluster, settings);
        }
    }

    TFuture<TGenericResult> DropUser(const TString& cluster, const TDropUserSettings& settings) override {
        CHECK_PREPARED_DDL(DropUser);

        if (IsPrepare()) {
            auto dropUserPromise = NewPromise<TGenericResult>();

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& dropUser = *schemeTx.MutableAlterLogin()->MutableRemoveUser();
            dropUser.SetUser(settings.UserName);
            dropUser.SetMissingOk(settings.MissingOk);

            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            phyTx.MutableSchemeOperation()->MutableAlterUser()->Swap(&schemeTx);
            TGenericResult result;
            result.SetSuccess();
            dropUserPromise.SetValue(result);
            return dropUserPromise.GetFuture();
        } else {
            return Gateway->DropUser(cluster, settings);
        }
    }

    struct TRemoveLastPhyTxHelper {
        TRemoveLastPhyTxHelper() = default;

        ~TRemoveLastPhyTxHelper() {
            if (Query) {
                Query->MutableTransactions()->RemoveLast();
            }
        }

        NKqpProto::TKqpPhyTx& Capture(NKqpProto::TKqpPhyQuery* query) {
            Query = query;
            return *Query->AddTransactions();
        }

        void Forget() {
            Query = nullptr;
        }
    private:
        NKqpProto::TKqpPhyQuery* Query = nullptr;
    };

    template <class TSettings>
    TGenericResult PrepareObjectOperation(const TString& cluster, const TSettings& settings,
        NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus (NMetadata::NModifications::IOperationsManager::* prepareMethod)(NKqpProto::TKqpSchemeOperation&, const TSettings&, const NMetadata::IClassBehaviour::TPtr&, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext&) const)
    {
        TRemoveLastPhyTxHelper phyTxRemover;
        try {
            if (cluster != SessionCtx->GetCluster()) {
                return ResultFromError<TGenericResult>("Invalid cluster: " + cluster);
            }

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return ResultFromError<TGenericResult>("Couldn't get domain name");
            }

            NMetadata::IClassBehaviour::TPtr cBehaviour(NMetadata::IClassBehaviour::TPtr(NMetadata::IClassBehaviour::TFactory::Construct(settings.GetTypeId())));
            if (!cBehaviour) {
                return ResultFromError<TGenericResult>(TStringBuilder() << "Incorrect object type: \"" << settings.GetTypeId() << "\"");
            }

            if (!cBehaviour->GetOperationsManager()) {
                return ResultFromError<TGenericResult>(TStringBuilder() << "Object type \"" << settings.GetTypeId() << "\" does not have manager for operations");
            }

            NMetadata::NModifications::IOperationsManager::TExternalModificationContext context;
            context.SetDatabase(SessionCtx->GetDatabase());
            context.SetActorSystem(ActorSystem);
            if (SessionCtx->GetUserToken()) {
                context.SetUserToken(*SessionCtx->GetUserToken());
            }

            auto& phyTx = phyTxRemover.Capture(SessionCtx->Query().PreparingQuery->MutablePhysicalQuery());
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
            phyTx.MutableSchemeOperation()->SetObjectType(settings.GetTypeId());

            NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus prepareStatus =
                (cBehaviour->GetOperationsManager().get()->*prepareMethod)(
                    *phyTx.MutableSchemeOperation(), settings, cBehaviour, context);

            TGenericResult result;
            if (prepareStatus.Ok()) {
                result.SetSuccess();
                phyTxRemover.Forget();
            } else {
                result.AddIssue(NYql::TIssue(prepareStatus.GetErrorMessage()));
                result.SetStatus(prepareStatus.GetStatus());
            }
            return result;
        } catch (const std::exception& e) {
            return ResultFromException<TGenericResult>(e);
        }
    }

    TFuture<TGenericResult> UpsertObject(const TString& cluster, const TUpsertObjectSettings& settings) override {
        CHECK_PREPARED_DDL(UpsertObject);

        if (IsPrepare()) {
            return MakeFuture(PrepareObjectOperation(cluster, settings, &NMetadata::NModifications::IOperationsManager::PrepareUpsertObjectSchemeOperation));
        } else {
            return Gateway->UpsertObject(cluster, settings);
        }
    }

    TFuture<TGenericResult> CreateObject(const TString& cluster, const TCreateObjectSettings& settings) override {
        CHECK_PREPARED_DDL(CreateObject);

        if (IsPrepare()) {
            return MakeFuture(PrepareObjectOperation(cluster, settings, &NMetadata::NModifications::IOperationsManager::PrepareCreateObjectSchemeOperation));
        } else {
            return Gateway->CreateObject(cluster, settings);
        }
    }

    TFuture<TGenericResult> AlterObject(const TString& cluster, const TAlterObjectSettings& settings) override {
        CHECK_PREPARED_DDL(AlterObject);

        if (IsPrepare()) {
            return MakeFuture(PrepareObjectOperation(cluster, settings, &NMetadata::NModifications::IOperationsManager::PrepareAlterObjectSchemeOperation));
        } else {
            return Gateway->AlterObject(cluster, settings);
        }
    }

    TFuture<TGenericResult> DropObject(const TString& cluster, const TDropObjectSettings& settings) override {
        CHECK_PREPARED_DDL(DropObject);

        if (IsPrepare()) {
            return MakeFuture(PrepareObjectOperation(cluster, settings, &NMetadata::NModifications::IOperationsManager::PrepareDropObjectSchemeOperation));
        } else {
            return Gateway->DropObject(cluster, settings);
        }
    }

    TFuture<TGenericResult> CreateGroup(const TString& cluster, const TCreateGroupSettings& settings) override {
        CHECK_PREPARED_DDL(CreateGroup);

        if (IsPrepare()) {
            auto createGroupPromise = NewPromise<TGenericResult>();

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& createGroup = *schemeTx.MutableAlterLogin()->MutableCreateGroup();
            createGroup.SetGroup(settings.GroupName);

            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            phyTx.MutableSchemeOperation()->MutableCreateGroup()->Swap(&schemeTx);

            if (settings.Roles.size()) {
                AddUsersToGroup(database, settings.GroupName, settings.Roles, NYql::TAlterGroupSettings::EAction::AddRoles);
            }

            TGenericResult result;
            result.SetSuccess();
            createGroupPromise.SetValue(result);
            return createGroupPromise.GetFuture();
        } else {
            return Gateway->CreateGroup(cluster, settings);
        }
    }

    TFuture<TGenericResult> AlterGroup(const TString& cluster, TAlterGroupSettings& settings) override {
        CHECK_PREPARED_DDL(UpdateGroup);

        if (IsPrepare()) {
            auto alterGroupPromise = NewPromise<TGenericResult>();

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            if (!settings.Roles.size()) {
                return MakeFuture(ResultFromError<TGenericResult>("No roles given for AlterGroup request"));
            }

            AddUsersToGroup(database, settings.GroupName, settings.Roles, settings.Action);

            TGenericResult result;
            result.SetSuccess();
            alterGroupPromise.SetValue(result);
            return alterGroupPromise.GetFuture();
        } else {
            return Gateway->AlterGroup(cluster, settings);
        }
    }

    TFuture<TGenericResult> RenameGroup(const TString& cluster, TRenameGroupSettings& settings) override {
        CHECK_PREPARED_DDL(RenameGroup);

        if (IsPrepare()) {
            auto renameGroupPromise = NewPromise<TGenericResult>();

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& renameGroup = *schemeTx.MutableAlterLogin()->MutableRenameGroup();
            renameGroup.SetGroup(settings.GroupName);
            renameGroup.SetNewName(settings.NewName);

            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            phyTx.MutableSchemeOperation()->MutableRenameGroup()->Swap(&schemeTx);
            TGenericResult result;
            result.SetSuccess();
            renameGroupPromise.SetValue(result);
            return renameGroupPromise.GetFuture();
        } else {
            return Gateway->RenameGroup(cluster, settings);
        }
    }

    TFuture<TGenericResult> DropGroup(const TString& cluster, const TDropGroupSettings& settings) override {
        CHECK_PREPARED_DDL(DropGroup);

        if (IsPrepare()) {
            auto dropGroupPromise = NewPromise<TGenericResult>();

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& dropGroup = *schemeTx.MutableAlterLogin()->MutableRemoveGroup();
            dropGroup.SetGroup(settings.GroupName);
            dropGroup.SetMissingOk(settings.MissingOk);

            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            phyTx.MutableSchemeOperation()->MutableAlterUser()->Swap(&schemeTx);
            TGenericResult result;
            result.SetSuccess();
            dropGroupPromise.SetValue(result);
            return dropGroupPromise.GetFuture();
        } else {
            return Gateway->DropGroup(cluster, settings);
        }
    }

    TFuture<TGenericResult> CreateColumnTable(TKikimrTableMetadataPtr metadata,
            bool createDir, bool existingOk) override {
        CHECK_PREPARED_DDL(CreateColumnTable);

        try {
            const auto& cluster = metadata->Cluster;

            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(metadata->Name, GetDatabase(), pathPair, error, createDir)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);

            Ydb::StatusIds::StatusCode code;
            TString error;

            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateColumnTable);
            schemeTx.SetFailedOnAlreadyExists(!existingOk);

            NKikimrSchemeOp::TColumnTableDescription* tableDesc = schemeTx.MutableCreateColumnTable();

            tableDesc->SetName(pathPair.second);
            FillColumnTableSchema(*tableDesc->MutableSchema(), *metadata);

            if (!FillCreateColumnTableDesc(metadata, *tableDesc, code, error)) {
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue(error));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                return MakeFuture(std::move(errResult));
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableCreateTable()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterColumnTable(const TString& cluster,
        const TAlterColumnTableSettings& settings) override
    {
        CHECK_PREPARED_DDL(AlterColumnTable);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Table, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);

            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnTable);
            NKikimrSchemeOp::TAlterColumnTable* alter = schemeTx.MutableAlterColumnTable();
            alter->SetName(settings.Table);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableAlterColumnTable()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateSequence(const TString& cluster,
            const TCreateSequenceSettings& settings, bool existingOk) override {
        CHECK_PREPARED_DDL(CreateSequence);

        if (!SessionCtx->Config().EnableSequences) {
            IKqpGateway::TGenericResult errResult;
            errResult.AddIssue(NYql::TIssue("Sequences are not supported yet."));
            errResult.SetStatus(NYql::YqlStatusFromYdbStatus(Ydb::StatusIds::UNSUPPORTED));
            return MakeFuture(std::move(errResult));
        }

        try {

            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);

            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSequence);
            schemeTx.SetFailedOnAlreadyExists(!existingOk);

            NKikimrSchemeOp::TSequenceDescription* seqDesc = schemeTx.MutableSequence();

            seqDesc->SetName(pathPair.second);

            if (settings.SequenceSettings.MinValue) {
                seqDesc->SetMinValue(*settings.SequenceSettings.MinValue);
            }
            if (settings.SequenceSettings.MaxValue) {
                seqDesc->SetMaxValue(*settings.SequenceSettings.MaxValue);
            }
            if (settings.SequenceSettings.Increment) {
                seqDesc->SetIncrement(*settings.SequenceSettings.Increment);
            }
            if (settings.SequenceSettings.StartValue) {
                seqDesc->SetStartValue(*settings.SequenceSettings.StartValue);
            }
            if (settings.SequenceSettings.Cache) {
                seqDesc->SetCache(*settings.SequenceSettings.Cache);
            }
            if (settings.SequenceSettings.Cycle) {
                seqDesc->SetCycle(*settings.SequenceSettings.Cycle);
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableCreateSequence()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> DropSequence(const TString& cluster,
            const NYql::TDropSequenceSettings& settings, bool missingOk) override {
        CHECK_PREPARED_DDL(DropSequence);

        if (!SessionCtx->Config().EnableSequences) {
            IKqpGateway::TGenericResult errResult;
            errResult.AddIssue(NYql::TIssue("Sequences are not supported yet."));
            errResult.SetStatus(NYql::YqlStatusFromYdbStatus(Ydb::StatusIds::UNSUPPORTED));
            return MakeFuture(std::move(errResult));
        }

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetSuccessOnNotExist(missingOk);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropSequence);

            NKikimrSchemeOp::TDrop* drop = schemeTx.MutableDrop();
            drop->SetName(pathPair.second);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableDropSequence()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterSequence(const TString& cluster,
            const TAlterSequenceSettings& settings, bool missingOk) override {
        CHECK_PREPARED_DDL(AlterSequence);

        if (!SessionCtx->Config().EnableSequences) {
            IKqpGateway::TGenericResult errResult;
            errResult.AddIssue(NYql::TIssue("Sequences are not supported yet."));
            errResult.SetStatus(NYql::YqlStatusFromYdbStatus(Ydb::StatusIds::UNSUPPORTED));
            return MakeFuture(std::move(errResult));
        }

        try {

            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterSequence);
            schemeTx.SetSuccessOnNotExist(missingOk);

            NKikimrSchemeOp::TSequenceDescription* seqDesc = schemeTx.MutableSequence();
            seqDesc->SetName(pathPair.second);

            if (settings.SequenceSettings.MinValue) {
                seqDesc->SetMinValue(*settings.SequenceSettings.MinValue);
            }
            if (settings.SequenceSettings.MaxValue) {
                seqDesc->SetMaxValue(*settings.SequenceSettings.MaxValue);
            }
            if (settings.SequenceSettings.Increment) {
                seqDesc->SetIncrement(*settings.SequenceSettings.Increment);
            }
            if (settings.SequenceSettings.StartValue) {
                seqDesc->SetStartValue(*settings.SequenceSettings.StartValue);
            }
            if (settings.SequenceSettings.Cache) {
                seqDesc->SetCache(*settings.SequenceSettings.Cache);
            }
            if (settings.SequenceSettings.Cycle) {
                seqDesc->SetCycle(*settings.SequenceSettings.Cycle);
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableAlterSequence()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateTableStore(const TString& cluster,
        const TCreateTableStoreSettings& settings, bool existingOk) override
    {
        CHECK_PREPARED_DDL(CreateTableStore);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.TableStore, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);

            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateColumnStore);
            schemeTx.SetFailedOnAlreadyExists(!existingOk);

            NKikimrSchemeOp::TColumnStoreDescription* storeDesc = schemeTx.MutableCreateColumnStore();
            storeDesc->SetName(pathPair.second);
            storeDesc->SetColumnShardCount(settings.ShardsCount);

            NKikimrSchemeOp::TColumnTableSchemaPreset* schemaPreset = storeDesc->AddSchemaPresets();
            schemaPreset->SetName("default");
            FillColumnTableSchema(*schemaPreset->MutableSchema(), settings);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableCreateTableStore()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterTableStore(const TString& cluster,
        const TAlterTableStoreSettings& settings) override
    {
        CHECK_PREPARED_DDL(AlterTableStore);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.TableStore, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);

            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnStore);
            NKikimrSchemeOp::TAlterColumnStore* alter = schemeTx.MutableAlterColumnStore();
            alter->SetName(pathPair.second);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableAlterTableStore()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> DropTableStore(const TString& cluster,
        const TDropTableStoreSettings& settings, bool missingOk) override
    {
        CHECK_PREPARED_DDL(DropTableStore);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.TableStore, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetSuccessOnNotExist(missingOk);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropColumnStore);
            NKikimrSchemeOp::TDrop* drop = schemeTx.MutableDrop();
            drop->SetName(pathPair.second);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableDropTableStore()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateExternalTable(const TString& cluster, const TCreateExternalTableSettings& settings,
        bool createDir, bool existingOk, bool replaceIfExists) override
    {
        CHECK_PREPARED_DDL(CreateExternalTable);

        if (IsPrepare()) {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.ExternalTable, GetDatabase(), pathPair, error, createDir)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            TRemoveLastPhyTxHelper phyTxRemover;
            auto& phyTx = phyTxRemover.Capture(SessionCtx->Query().PreparingQuery->MutablePhysicalQuery());
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            auto& schemeTx = *phyTx.MutableSchemeOperation()->MutableCreateExternalTable();
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateExternalTable);
            schemeTx.SetFailedOnAlreadyExists(!existingOk);

            NKikimrSchemeOp::TExternalTableDescription& externalTableDesc = *schemeTx.MutableCreateExternalTable();
            NSchemeHelpers::FillCreateExternalTableColumnDesc(externalTableDesc, pathPair.second, replaceIfExists, settings);
            TGenericResult result;
            result.SetSuccess();
            phyTxRemover.Forget();
            return MakeFuture(result);
        } else {
            return Gateway->CreateExternalTable(cluster, settings, createDir, existingOk, replaceIfExists);
        }
    }

    TFuture<TGenericResult> AlterExternalTable(const TString& cluster,
        const TAlterExternalTableSettings& settings) override
    {
        FORWARD_ENSURE_NO_PREPARE(AlterExternalTable, cluster, settings);
    }

    TFuture<TGenericResult> DropExternalTable(const TString& cluster,
        const TDropExternalTableSettings& settings,
        bool missingOk) override
    {
        CHECK_PREPARED_DDL(DropExternalTable);

        if (IsPrepare()) {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.ExternalTable, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            TRemoveLastPhyTxHelper phyTxRemover;
            auto& phyTx = phyTxRemover.Capture(SessionCtx->Query().PreparingQuery->MutablePhysicalQuery());
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            auto& schemeTx = *phyTx.MutableSchemeOperation()->MutableDropExternalTable();
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropExternalTable);
            schemeTx.SetSuccessOnNotExist(missingOk);

            NKikimrSchemeOp::TDrop& drop = *schemeTx.MutableDrop();
            drop.SetName(pathPair.second);

            TGenericResult result;
            result.SetSuccess();
            phyTxRemover.Forget();
            return MakeFuture(result);
        } else {
            return Gateway->DropExternalTable(cluster, settings, missingOk);
        }
    }

    static TString AdjustPath(const TString& path, const TString& database) {
        if (path.StartsWith('/')) {
            if (!path.StartsWith(database)) {
                throw yexception() << "Path '" << path << "' not in database '" << database << "'";
            }
            return path;
        } else {
            return database + '/' + path;
        }
    }

    TFuture<TGenericResult> CreateReplication(const TString& cluster, const NYql::TCreateReplicationSettings& settings) override {
        CHECK_PREPARED_DDL(CreateReplication);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            TString error;
            std::pair<TString, TString> pathPair;
            if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, true)) {
                return MakeFuture(ResultFromError<TGenericResult>(error));
            }

            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetWorkingDir(pathPair.first);
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateReplication);

            auto& op = *tx.MutableReplication();
            op.SetName(pathPair.second);

            auto& config = *op.MutableConfig();
            auto& params = *config.MutableSrcConnectionParams();
            if (const auto& connectionString = settings.Settings.ConnectionString) {
                const auto parseResult = NYdb::ParseConnectionString(*connectionString);
                params.SetEndpoint(parseResult.Endpoint);
                params.SetDatabase(parseResult.Database);
            }
            if (const auto& endpoint = settings.Settings.Endpoint) {
                params.SetEndpoint(*endpoint);
            }
            if (const auto& database = settings.Settings.Database) {
                params.SetDatabase(*database);
            }
            if (const auto& oauth = settings.Settings.OAuthToken) {
                oauth->Serialize(*params.MutableOAuthToken());
            }
            if (const auto& staticCreds = settings.Settings.StaticCredentials) {
                staticCreds->Serialize(*params.MutableStaticCredentials());
            }

            auto& targets = *config.MutableSpecific();
            for (const auto& [src, dst] : settings.Targets) {
                auto& target = *targets.AddTargets();
                target.SetSrcPath(AdjustPath(src, params.GetDatabase()));
                target.SetDstPath(AdjustPath(dst, GetDatabase()));
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableCreateReplication()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterReplication(const TString& cluster, const NYql::TAlterReplicationSettings& settings) override {
        CHECK_PREPARED_DDL(AlterReplication);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetWorkingDir(pathPair.first);
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterReplication);

            auto& op = *tx.MutableAlterReplication();
            op.SetName(pathPair.second);

            if (const auto& done = settings.Settings.StateDone) {
                auto& state = *op.MutableState();
                state.MutableDone()->SetFailoverMode(
                    static_cast<NKikimrReplication::TReplicationState::TDone::EFailoverMode>(done->FailoverMode));
            }

            if (settings.Settings.ConnectionString || settings.Settings.Endpoint || settings.Settings.Database ||
                    settings.Settings.OAuthToken || settings.Settings.StaticCredentials) {
                auto& config = *op.MutableConfig();
                auto& params = *config.MutableSrcConnectionParams();
                if (const auto& connectionString = settings.Settings.ConnectionString) {
                    const auto parseResult = NYdb::ParseConnectionString(*connectionString);
                    params.SetEndpoint(parseResult.Endpoint);
                    params.SetDatabase(parseResult.Database);
                }
                if (const auto& endpoint = settings.Settings.Endpoint) {
                    params.SetEndpoint(*endpoint);
                }
                if (const auto& database = settings.Settings.Database) {
                    params.SetDatabase(*database);
                }
                if (const auto& oauth = settings.Settings.OAuthToken) {
                    oauth->Serialize(*params.MutableOAuthToken());
                }
                if (const auto& staticCreds = settings.Settings.StaticCredentials) {
                    staticCreds->Serialize(*params.MutableStaticCredentials());
                }
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableAlterReplication()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> DropReplication(const TString& cluster, const NYql::TDropReplicationSettings& settings) override {
        CHECK_PREPARED_DDL(DropReplication);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetWorkingDir(pathPair.first);
            if (settings.Cascade) {
                tx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropReplicationCascade);
            } else {
                tx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropReplication);
            }

            auto& op = *tx.MutableDrop();
            op.SetName(pathPair.second);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableDropReplication()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TVector<NKikimrKqp::TKqpTableMetadataProto> GetCollectedSchemeData() override {
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

    bool GetDomainLoginOnly() {
        return Gateway->GetDomainLoginOnly();
    }

    TMaybe<TString> GetDomainName() {
        return Gateway->GetDomainName();
    }

    void AddUsersToGroup(const TString& database, const TString& group, const std::vector<TString>& roles, const NYql::TAlterGroupSettings::EAction& action) {
        for (const auto& role : roles) {
            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);

            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            switch (action) {
                case NYql::TAlterGroupSettings::EAction::AddRoles: {
                    auto& alterGroup = *schemeTx.MutableAlterLogin()->MutableAddGroupMembership();
                    alterGroup.SetGroup(group);
                    alterGroup.SetMember(role);
                    phyTx.MutableSchemeOperation()->MutableAddGroupMembership()->Swap(&schemeTx);
                    break;
                }
                case NYql::TAlterGroupSettings::EAction::RemoveRoles: {
                    auto& alterGroup = *schemeTx.MutableAlterLogin()->MutableRemoveGroupMembership();
                    alterGroup.SetGroup(group);
                    alterGroup.SetMember(role);
                    phyTx.MutableSchemeOperation()->MutableRemoveGroupMembership()->Swap(&schemeTx);
                    break;
                }
            }
        }
    }

private:
    TIntrusivePtr<IKqpGateway> Gateway;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TActorSystem* ActorSystem = nullptr;
};

#undef FORWARD_ENSURE_NO_PREPARE
#undef CHECK_PREPARED_DDL

} // namespace

TIntrusivePtr<IKikimrGateway> CreateKqpGatewayProxy(const TIntrusivePtr<IKqpGateway>& gateway,
    const TIntrusivePtr<TKikimrSessionContext>& sessionCtx, TActorSystem* actorSystem)
{
    return MakeIntrusive<TKqpGatewayProxy>(gateway, sessionCtx, actorSystem);
}

} // namespace NKikimr::NKqp
