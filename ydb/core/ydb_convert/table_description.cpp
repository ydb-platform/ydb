#include "column_families.h"
#include "table_description.h"
#include "table_settings.h"
#include "ydb_convert.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/protos/follower_group.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/scheme/protos/type_info.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/hash.h>

namespace NKikimr {

static NProtoBuf::Timestamp MillisecToProtoTimeStamp(ui64 ms) {
    NProtoBuf::Timestamp timestamp;
    timestamp.set_seconds((i64)(ms / 1000));
    timestamp.set_nanos((i32)((ms % 1000) * 1000000));
    return timestamp;
}

template <typename TStoragePoolHolder>
using TAddStoragePoolFunc = Ydb::Table::StoragePool* (TStoragePoolHolder::*)();

template <typename TStoragePoolHolder>
static void FillStoragePool(TStoragePoolHolder* out, TAddStoragePoolFunc<TStoragePoolHolder> func,
        const NKikimrSchemeOp::TStorageSettings& in)
{
    if (in.GetAllowOtherKinds()) {
        return;
    }

    std::invoke(func, out)->set_media(in.GetPreferredPoolKind());
}

THashSet<EAlterOperationKind> GetAlterOperationKinds(const Ydb::Table::AlterTableRequest* req) {
    THashSet<EAlterOperationKind> ops;

    if (req->add_columns_size() || req->drop_columns_size() ||
        req->alter_columns_size() ||
        req->ttl_action_case() !=
            Ydb::Table::AlterTableRequest::TTL_ACTION_NOT_SET ||
        req->has_alter_storage_settings() || req->add_column_families_size() ||
        req->alter_column_families_size() || req->set_compaction_policy() ||
        req->has_alter_partitioning_settings() ||
        req->set_key_bloom_filter() != Ydb::FeatureFlag::STATUS_UNSPECIFIED ||
        req->has_set_read_replicas_settings())
    {
        ops.emplace(EAlterOperationKind::Common);
    }

    if (req->add_indexes_size()) {
        ops.emplace(EAlterOperationKind::AddIndex);
    }

    if (req->drop_indexes_size()) {
        ops.emplace(EAlterOperationKind::DropIndex);
    }

    if (req->add_changefeeds_size()) {
        ops.emplace(EAlterOperationKind::AddChangefeed);
    }

    if (req->drop_changefeeds_size()) {
        ops.emplace(EAlterOperationKind::DropChangefeed);
    }

    if (req->alter_attributes_size()) {
        ops.emplace(EAlterOperationKind::Attribute);
    }

    if (req->rename_indexes_size()) {
        ops.emplace(EAlterOperationKind::RenameIndex);
    }

    return ops;
}

namespace {

std::pair<TString, TString> SplitPathIntoWorkingDirAndName(const TString& path) {
    auto splitPos = path.find_last_of('/');
    if (splitPos == path.npos || splitPos + 1 == path.size()) {
        ythrow yexception() << "wrong path format '" << path << "'" ;
    }
    return {path.substr(0, splitPos), path.substr(splitPos + 1)};
}

}


bool FillAlterTableSettingsDesc(NKikimrSchemeOp::TTableDescription& out,
    const Ydb::Table::AlterTableRequest& in, const TTableProfiles& profiles,
    Ydb::StatusIds::StatusCode& code, TString& error, const TAppData* appData) {

    bool changed = false;
    auto &partitionConfig = *out.MutablePartitionConfig();

    if (in.set_compaction_policy()) {
        if (!profiles.ApplyCompactionPolicy(in.set_compaction_policy(), partitionConfig, code, error, appData)) {
            return false;
        }

        changed = true;
    }

    return NKikimr::FillAlterTableSettingsDesc(out, in, code, error, changed);
}

bool BuildAlterTableAddIndexRequest(const Ydb::Table::AlterTableRequest* req, NKikimrIndexBuilder::TIndexBuildSettings* settings,
    ui64 flags,
    Ydb::StatusIds::StatusCode& code, TString& error)
{
    const auto ops = GetAlterOperationKinds(req);
    if (ops.size() != 1 || *ops.begin() != EAlterOperationKind::AddIndex) {
        code = Ydb::StatusIds::INTERNAL_ERROR;
        error = "Unexpected build alter table add index call.";
        return false;
    }

    if (req->add_indexes_size() != 1) {
        code = Ydb::StatusIds::UNSUPPORTED;
        error = "Only one index can be added by one operation";
        return false;
    }

    const auto desc = req->add_indexes(0);

    if (!desc.name()) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = "Index must have a name";
        return false;
    }

    if (!desc.index_columns_size()) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = "At least one column must be specified";
        return false;
    }

    if (!desc.data_columns().empty() && !AppData()->FeatureFlags.GetEnableDataColumnForIndexTable()) {
        code = Ydb::StatusIds::UNSUPPORTED;
        error = "Data column feature is not supported yet";
        return false;
    }

    if (flags & NKqpProto::TKqpSchemeOperation::FLAG_PG_MODE) {
        settings->set_pg_mode(true);
    }

    if (flags & NKqpProto::TKqpSchemeOperation::FLAG_IF_NOT_EXISTS) {
        settings->set_if_not_exist(true);
    }

    settings->set_source_path(req->path());
    auto tableIndex = settings->mutable_index();
    tableIndex->CopyFrom(req->add_indexes(0));

    return true;
}

bool BuildAlterTableModifyScheme(const TString& path, const Ydb::Table::AlterTableRequest* req,
    NKikimrSchemeOp::TModifyScheme* modifyScheme, const TTableProfiles& profiles,
    const TPathId& resolvedPathId,
    Ydb::StatusIds::StatusCode& code, TString& error)
{
    std::pair<TString, TString> pathPair;
    const auto ops = GetAlterOperationKinds(req);
    if (ops.empty()) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = "Empty alter";
        return false;
    }

    if (ops.size() > 1) {
        code = Ydb::StatusIds::UNSUPPORTED;
        error = "Mixed alter is unsupported";
        return false;
    }

    const auto OpType = *ops.begin();

    try {
        pathPair = SplitPathIntoWorkingDirAndName(path);
    } catch (const std::exception&) {
        code = Ydb::StatusIds::BAD_REQUEST;
        return false;
    }

    if (!AppData()->FeatureFlags.GetEnableChangefeeds() && OpType == EAlterOperationKind::AddChangefeed) {
        code = Ydb::StatusIds::UNSUPPORTED;
        error =  "Changefeeds are not supported yet";
        return false;
    }

    if (req->rename_indexes_size() != 1 && OpType == EAlterOperationKind::RenameIndex) {
        code = Ydb::StatusIds::UNSUPPORTED;
        error = "Only one index can be renamed by one operation";
        return false;
    }

    if (req->drop_changefeeds_size() != 1 && OpType == EAlterOperationKind::DropChangefeed) {
        code = Ydb::StatusIds::UNSUPPORTED;
        error = "Only one changefeed can be removed by one operation";
        return false;
    }

    if (req->add_changefeeds_size() != 1 && OpType == EAlterOperationKind::AddChangefeed) {
        code = Ydb::StatusIds::UNSUPPORTED;
        error = "Only one changefeed can be added by one operation";
        return false;
    }

    if (req->drop_indexes_size() != 1 && OpType == EAlterOperationKind::DropIndex) {
        code = Ydb::StatusIds::UNSUPPORTED;
        error = "Only one index can be removed by one operation";
        return false;
    }

    const auto& workingDir = pathPair.first;
    const auto& name = pathPair.second;
    modifyScheme->SetWorkingDir(workingDir);

    for(const auto& rename: req->rename_indexes()) {
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMoveIndex);
        auto& alter = *modifyScheme->MutableMoveIndex();
        alter.SetTablePath(path);
        alter.SetSrcPath(rename.source_name());
        alter.SetDstPath(rename.destination_name());
        alter.SetAllowOverwrite(rename.replace_destination());
    }

    for (const auto& drop : req->drop_changefeeds()) {
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStream);
        auto op = modifyScheme->MutableDropCdcStream();
        op->AddStreamName(drop);
        op->SetTableName(name);
    }

    for (const auto &add : req->add_changefeeds()) {
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStream);
        auto op = modifyScheme->MutableCreateCdcStream();
        op->SetTableName(name);

        if (add.has_retention_period()) {
            op->SetRetentionPeriodSeconds(add.retention_period().seconds());
        }

        if (add.has_topic_partitioning_settings()) {
            i64 minActivePartitions =
                add.topic_partitioning_settings().min_active_partitions();
            if (minActivePartitions < 0) {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = "Topic partitions count must be positive";
                return false;
            } else if (minActivePartitions == 0) {
                minActivePartitions = 1;
            }
            op->SetTopicPartitions(minActivePartitions);

            if (add.topic_partitioning_settings().has_auto_partitioning_settings()) {
                auto& partitioningSettings = add.topic_partitioning_settings().auto_partitioning_settings();
                op->SetTopicAutoPartitioning(partitioningSettings.strategy() != ::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_DISABLED);

                i64 maxActivePartitions =
                    add.topic_partitioning_settings().max_active_partitions();
                if (maxActivePartitions < 0) {
                    code = Ydb::StatusIds::BAD_REQUEST;
                    error = "Topic max active partitions count must be positive";
                    return false;
                } else if (maxActivePartitions == 0) {
                    maxActivePartitions = 50;
                }
                op->SetMaxPartitionCount(maxActivePartitions);
            }
        }

        if (!FillChangefeedDescription(*op->MutableStreamDescription(), add, code, error)) {
            return false;
        }
    }

    for (const auto& drop : req->drop_indexes()) {
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropIndex);
        auto desc = modifyScheme->MutableDropIndex();
        desc->SetIndexName(drop);
        desc->SetTableName(name);
    }

    if (OpType == EAlterOperationKind::Common) {
        modifyScheme->SetOperationType(
            NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable);

        auto desc = modifyScheme->MutableAlterTable();
        desc->SetName(name);

        for (const auto &drop : req->drop_columns()) {
            desc->AddDropColumns()->SetName(drop);
        }

        if (!FillColumnDescription(*desc, req->add_columns(), code, error)) {
            return false;
        }

        for (const auto &alter : req->alter_columns()) {
            auto column = desc->AddColumns();
            column->SetName(alter.name());

            if (alter.Hasnot_null()) {
                column->SetNotNull(alter.Getnot_null());
            }

            if (!alter.family().empty()) {
                column->SetFamilyName(alter.family());
            }
            switch (alter.default_value_case()) {
                case Ydb::Table::ColumnMeta::kFromSequence: {
                    auto fromSequence = column->MutableDefaultFromSequence();
                    TString sequenceName = alter.from_sequence().name();
                    if (!IsStartWithSlash(sequenceName)) {
                        *fromSequence = JoinPath({workingDir, sequenceName});
                    }
                    break;
                }
                case Ydb::Table::ColumnMeta::kEmptyDefault: {
                    column->SetEmptyDefault(google::protobuf::NullValue());
                    break;
                }
                default: break;
            }
        }

        bool hadPartitionConfig = desc->HasPartitionConfig();
        TColumnFamilyManager families(desc->MutablePartitionConfig());

        // Apply storage settings to the default column family
        if (req->has_alter_storage_settings()) {
            if (!families.ApplyStorageSettings(req->alter_storage_settings(), &code,
                                            &error)) {
                return false;
            }
        }

        for (const auto &familySettings : req->add_column_families()) {
            if (!families.ApplyFamilySettings(familySettings, &code, &error)) {
                return false;
            }
        }

        for (const auto &familySettings : req->alter_column_families()) {
            if (!families.ApplyFamilySettings(familySettings, &code, &error)) {
                return false;
            }
        }

        // Avoid altering partition config unless we changed something
        if (!families.Modified && !hadPartitionConfig) {
            desc->ClearPartitionConfig();
        }

        if (!FillAlterTableSettingsDesc(*desc, *req, profiles, code, error,
                                        AppData())) {
            return false;
        }
    }

    if (OpType == EAlterOperationKind::Attribute) {
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterUserAttributes);
        if (resolvedPathId) {
            modifyScheme->AddApplyIf()->SetPathId(resolvedPathId.LocalPathId);
        }

        auto& alter = *modifyScheme->MutableAlterUserAttributes();
        alter.SetPathName(name);

        for (auto [key, value] : req->alter_attributes()) {
            auto& attr = *alter.AddUserAttributes();
            attr.SetKey(key);
            if (value) {
                attr.SetValue(value);
            }
        }
    }

    return true;
}

bool BuildAlterTableModifyScheme(const Ydb::Table::AlterTableRequest* req, NKikimrSchemeOp::TModifyScheme* modifyScheme,
    const TTableProfiles& profiles, const TPathId& resolvedPathId, Ydb::StatusIds::StatusCode& code, TString& error)
{
    return BuildAlterTableModifyScheme(req->path(), req, modifyScheme, profiles, resolvedPathId, code, error);
}

template <typename TColumn>
static Ydb::Type* AddColumn(Ydb::Table::ColumnMeta* newColumn, const TColumn& column) {
    newColumn->set_name(column.GetName());

    Ydb::Type* columnType = nullptr;
    auto typeDesc = NPg::TypeDescFromPgTypeName(column.GetType());
    if (typeDesc) {
        columnType = newColumn->mutable_type();
        auto* pg = columnType->mutable_pg_type();
        pg->set_type_name(NPg::PgTypeNameFromTypeDesc(typeDesc));
        pg->set_type_modifier(NPg::TypeModFromPgTypeName(column.GetType()));
        pg->set_oid(NPg::PgTypeIdFromTypeDesc(typeDesc));
        pg->set_typlen(0);
        pg->set_typmod(0);
        if (column.GetNotNull()) {
            newColumn->set_not_null(column.GetNotNull());
        }
    } else if (column.HasTypeInfo() && column.GetTypeInfo().HasDecimalPrecision() && column.GetTypeInfo().HasDecimalScale()) {
        if (column.GetNotNull()) {
            columnType = newColumn->mutable_type();
        } else {
            columnType = newColumn->mutable_type()->mutable_optional_type()->mutable_item();
        }
        Y_ENSURE(columnType);
        auto typeParams = columnType->mutable_decimal_type();
        typeParams->set_precision(column.GetTypeInfo().GetDecimalPrecision());
        typeParams->set_scale(column.GetTypeInfo().GetDecimalScale());
    } else {
        NYql::NProto::TypeIds protoType;
        if (!NYql::NProto::TypeIds_Parse(column.GetType(), &protoType)) {
            throw NYql::TErrorException(NKikimrIssues::TIssuesIds::DEFAULT_ERROR)
                << "Got invalid type: " << column.GetType() << " for column: " << column.GetName();
        }
        if (column.GetNotNull()) {
            columnType = newColumn->mutable_type();
        } else {
            columnType = newColumn->mutable_type()->mutable_optional_type()->mutable_item();
        }
        Y_ENSURE(columnType);
        NMiniKQL::ExportPrimitiveTypeToProto(protoType, *columnType);
    }
    return columnType;
}

template <>
Ydb::Type* AddColumn<NKikimrSchemeOp::TColumnDescription>(Ydb::Table::ColumnMeta* newColumn, const NKikimrSchemeOp::TColumnDescription& column) {
    newColumn->set_name(column.GetName());

    Ydb::Type* columnType = nullptr;
    auto typeDesc = NPg::TypeDescFromPgTypeName(column.GetType());
    if (typeDesc) {
        columnType = newColumn->mutable_type();
        auto* pg = columnType->mutable_pg_type();
        pg->set_type_name(NPg::PgTypeNameFromTypeDesc(typeDesc));
        pg->set_type_modifier(NPg::TypeModFromPgTypeName(column.GetType()));
        pg->set_oid(NPg::PgTypeIdFromTypeDesc(typeDesc));
        pg->set_typlen(0);
        pg->set_typmod(0);
        if (column.GetNotNull()) {
            newColumn->set_not_null(column.GetNotNull());
        }
    } else if (column.HasTypeInfo() && column.GetTypeInfo().HasDecimalPrecision()) {
        if (column.GetNotNull()) {
            columnType = newColumn->mutable_type();
        } else {
            columnType = newColumn->mutable_type()->mutable_optional_type()->mutable_item();
        }
        Y_ENSURE(columnType);
        auto typeParams = columnType->mutable_decimal_type();
        typeParams->set_precision(column.GetTypeInfo().GetDecimalPrecision());
        typeParams->set_scale(column.GetTypeInfo().GetDecimalScale());
    } else {
        NYql::NProto::TypeIds protoType;
        if (!NYql::NProto::TypeIds_Parse(column.GetType(), &protoType)) {
            throw NYql::TErrorException(NKikimrIssues::TIssuesIds::DEFAULT_ERROR)
                << "Got invalid type: " << column.GetType() << " for column: " << column.GetName();
        }
        if (column.GetNotNull()) {
            columnType = newColumn->mutable_type();
        } else {
            columnType = newColumn->mutable_type()->mutable_optional_type()->mutable_item();
        }
        Y_ENSURE(columnType);
        NMiniKQL::ExportPrimitiveTypeToProto(protoType, *columnType);
    }
    switch (column.GetDefaultValueCase()) {
        case NKikimrSchemeOp::TColumnDescription::kDefaultFromLiteral: {
            auto fromLiteral = newColumn->mutable_from_literal();
            *fromLiteral = column.GetDefaultFromLiteral();
            break;
        }
        case NKikimrSchemeOp::TColumnDescription::kDefaultFromSequence: {
            auto* fromSequence = newColumn->mutable_from_sequence();
            fromSequence->set_name(column.GetDefaultFromSequence());
            break;
        }
        case NKikimrSchemeOp::TColumnDescription::kEmptyDefault: {
            newColumn->set_empty_default(google::protobuf::NullValue());
            break;
        }
        default: break;
    }

    return columnType;
}

void FillColumnDescription(Ydb::Table::ColumnMeta& out, const NKikimrSchemeOp::TColumnDescription& in) {
    AddColumn(&out, in);
}

template <typename TYdbProto>
void FillColumnDescriptionImpl(TYdbProto& out,
        NKikimrMiniKQL::TType& splitKeyType, const NKikimrSchemeOp::TTableDescription& in) {

    splitKeyType.SetKind(NKikimrMiniKQL::ETypeKind::Tuple);
    splitKeyType.MutableTuple()->MutableElement()->Reserve(in.KeyColumnIdsSize());
    THashMap<ui32, size_t> columnIdToKeyPos;
    for (size_t keyPos = 0; keyPos < in.KeyColumnIdsSize(); ++keyPos) {
        ui32 colId = in.GetKeyColumnIds(keyPos);
        columnIdToKeyPos[colId] = keyPos;
        splitKeyType.MutableTuple()->AddElement();
    }

    for (const auto& column : in.GetColumns()) {
        auto newColumn = out.add_columns();
        Ydb::Type* columnType = AddColumn(newColumn, column);

        if (columnIdToKeyPos.count(column.GetId())) {
            size_t keyPos = columnIdToKeyPos[column.GetId()];
            auto tupleElement = splitKeyType.MutableTuple()->MutableElement(keyPos);
            tupleElement->SetKind(NKikimrMiniKQL::ETypeKind::Optional);
            ConvertYdbTypeToMiniKQLType(*columnType, *tupleElement->MutableOptional()->MutableItem());
        }

        if (column.HasFamilyName()) {
            newColumn->set_family(column.GetFamilyName());
        }
    }

    if (in.HasTTLSettings()) {
        if (in.GetTTLSettings().HasEnabled()) {
            Ydb::StatusIds::StatusCode code;
            TString error;
            if (!FillTtlSettings(*out.mutable_ttl_settings(), in.GetTTLSettings().GetEnabled(), code, error)) {
                ythrow yexception() << "invalid TTL settings: " << error;
            }
        }
    }
}

void FillColumnDescription(Ydb::Table::DescribeTableResult& out,
        NKikimrMiniKQL::TType& splitKeyType, const NKikimrSchemeOp::TTableDescription& in) {
    FillColumnDescriptionImpl(out, splitKeyType, in);
}

void FillColumnDescription(Ydb::Table::CreateTableRequest& out,
        NKikimrMiniKQL::TType& splitKeyType, const NKikimrSchemeOp::TTableDescription& in) {
    FillColumnDescriptionImpl(out, splitKeyType, in);
}

template <typename TYdbProto>
void FillColumnDescriptionImpl(TYdbProto& out, const NKikimrSchemeOp::TColumnTableDescription& in) {
    auto& schema = in.GetSchema();

    for (const auto& column : schema.GetColumns()) {
        auto newColumn = out.add_columns();
        AddColumn(newColumn, column);

        if (column.HasColumnFamilyName()) {
            newColumn->set_family(column.GetColumnFamilyName());
        }
    }

    for (auto& name : schema.GetKeyColumnNames()) {
        out.add_primary_key(name);
    }

    if (in.HasSharding() && in.GetSharding().HasHashSharding()) {
        auto * partitioning = out.mutable_partitioning_settings();
        for (auto& column : in.GetSharding().GetHashSharding().GetColumns()) {
            partitioning->add_partition_by(column);
        }
    }

    if (in.HasTtlSettings()) {
        if (in.GetTtlSettings().HasEnabled()) {
            Ydb::StatusIds::StatusCode status;
            TString error;
            if (!FillTtlSettings(*out.mutable_ttl_settings(), in.GetTtlSettings().GetEnabled(), status, error)) {
                ythrow yexception() << "invalid TTL settings: " << error;
            }
        }
    }

    out.set_store_type(Ydb::Table::StoreType::STORE_TYPE_COLUMN);
}

void FillColumnDescription(Ydb::Table::DescribeTableResult& out, const NKikimrSchemeOp::TColumnTableDescription& in) {
    FillColumnDescriptionImpl(out, in);
}

void FillColumnDescription(Ydb::Table::CreateTableRequest& out, const NKikimrSchemeOp::TColumnTableDescription& in) {
    FillColumnDescriptionImpl(out, in);
}

bool ExtractColumnTypeInfo(NScheme::TTypeInfo& outTypeInfo, TString& outTypeMod,
    const Ydb::Type& inType, Ydb::StatusIds::StatusCode& status, TString& error)
{
    ui32 typeId = 0;
    auto itemType = inType.has_optional_type() ? inType.optional_type().item() : inType;
    switch (itemType.type_case()) {
        case Ydb::Type::kTypeId:
            typeId = (ui32)itemType.type_id();
            break;
        case Ydb::Type::kDecimalType: {
            ui32 precision = itemType.decimal_type().precision();
            ui32 scale = itemType.decimal_type().scale();
            if (!NKikimr::NScheme::TDecimalType::Validate(precision, scale, error)) {
                status = Ydb::StatusIds::BAD_REQUEST;
                return false;
            }
            outTypeInfo = NScheme::TTypeInfo(NScheme::TDecimalType(precision, scale));
            return true;
        }
        case Ydb::Type::kPgType: {
            const auto& pgType = itemType.pg_type();
            const auto& typeName = pgType.type_name();
            auto desc = NPg::TypeDescFromPgTypeName(typeName);
            if (!desc) {
                status = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Invalid PG type name: " << typeName;
                return false;
            }
            outTypeInfo = NScheme::TTypeInfo(desc);
            outTypeMod = pgType.type_modifier();
            return true;
        }

        default: {
            status = Ydb::StatusIds::BAD_REQUEST;
            error = "Only optional of data types are supported for table columns";
            return false;
        }
    }

    if (!NYql::NProto::TypeIds_IsValid((int)typeId)) {
        status = Ydb::StatusIds::BAD_REQUEST;
        error = TStringBuilder() << "Got invalid typeId: " << (int)typeId;
        return false;
    }

    outTypeInfo = NScheme::TTypeInfo(typeId);
    return true;
}

bool FillColumnDescription(NKikimrSchemeOp::TTableDescription& out,
    const google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta>& in, Ydb::StatusIds::StatusCode& status, TString& error) {

    for (const auto& column : in) {
        NKikimrSchemeOp:: TColumnDescription* cd = out.AddColumns();
        cd->SetName(column.name());
        bool notOptional = !column.type().has_optional_type();
        if (!column.has_not_null()) {
            if (!column.type().has_pg_type()) {
                cd->SetNotNull(notOptional);
            }
        } else {
            if (!column.type().has_pg_type() && notOptional != column.not_null()) {
                status = Ydb::StatusIds::BAD_REQUEST;
                error = "Not consistent column type and not_null option for column: " + column.name();
                return false;
            }
            cd->SetNotNull(column.not_null());
        }
        if (cd->GetNotNull() && !AppData()->FeatureFlags.GetEnableNotNullColumns()) {
            status = Ydb::StatusIds::UNSUPPORTED;
            error = "Not null columns feature is not supported yet";
            return false;
        }

        NScheme::TTypeInfo typeInfo;
        TString typeMod;
        if (!ExtractColumnTypeInfo(typeInfo, typeMod, column.type(), status, error)) {
            return false;
        }
        cd->SetType(NScheme::TypeName(typeInfo, typeMod));

        if (NScheme::NTypeIds::IsParametrizedType(typeInfo.GetTypeId())) {
            NScheme::ProtoFromTypeInfo(typeInfo, typeMod, *cd->MutableTypeInfo());
        }

        if (!column.family().empty()) {
            cd->SetFamilyName(column.family());
        }

        switch (column.default_value_case()) {
            case Ydb::Table::ColumnMeta::kFromLiteral: {
                auto fromLiteral = cd->MutableDefaultFromLiteral();
                *fromLiteral = column.from_literal();
                break;
            }
            case Ydb::Table::ColumnMeta::kFromSequence: {
                auto fromSequence = cd->MutableDefaultFromSequence();
                *fromSequence = column.from_sequence().name();
                break;
            }
            case Ydb::Table::ColumnMeta::kEmptyDefault: {
                cd->SetEmptyDefault(google::protobuf::NullValue());
                break;
            }
            default: break;
        }
    }

    return true;
}

NKikimrSchemeOp::TOlapColumnDescription* GetAddColumn(NKikimrSchemeOp::TColumnTableDescription& out) {
    return out.MutableSchema()->AddColumns();
}

NKikimrSchemeOp::TOlapColumnDescription* GetAddColumn(NKikimrSchemeOp::TAlterColumnTable& out) {
    return out.MutableAlterSchema()->AddAddColumns();
}

template <typename TColumnTable>
bool FillColumnDescriptionImpl(TColumnTable& out, const google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta>& in,
    Ydb::StatusIds::StatusCode& status, TString& error) {
    for (const auto& column : in) {
        if (column.type().has_pg_type()) {
            status = Ydb::StatusIds::BAD_REQUEST;
            error = "Unsupported column type for column: " + column.name();
            return false;
        }

        auto* columnDesc = GetAddColumn(out);
        columnDesc->SetName(column.name());

        NScheme::TTypeInfo typeInfo;
        TString typeMod;
        if (!ExtractColumnTypeInfo(typeInfo, typeMod, column.type(), status, error)) {
            return false;
        }
        columnDesc->SetType(NScheme::TypeName(typeInfo, typeMod));
        columnDesc->SetNotNull(column.not_null());

        if (NScheme::NTypeIds::IsParametrizedType(typeInfo.GetTypeId())) {
            NScheme::ProtoFromTypeInfo(typeInfo, typeMod, *columnDesc->MutableTypeInfo());
        }

        if (!column.Getfamily().empty()) {
            columnDesc->SetColumnFamilyName(column.Getfamily());
        }

        if (column.has_from_literal()) {
            status = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Default values are not supported in column tables";
            return false;
        }

        if (column.has_from_sequence()) {
            status = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Default sequences are not supported in column tables";
            return false;
        }
    }

    return true;
}

bool FillColumnDescription(NKikimrSchemeOp::TColumnTableDescription& out, const google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta>& in,
    Ydb::StatusIds::StatusCode& status, TString& error) {
    return FillColumnDescriptionImpl(out, in, status, error);
}

bool FillColumnDescription(NKikimrSchemeOp::TAlterColumnTable& out, const google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta>& in,
    Ydb::StatusIds::StatusCode& status, TString& error) {
    return FillColumnDescriptionImpl(out, in, status, error);
}

bool FillColumnFamily(
    const Ydb::Table::ColumnFamily& from, NKikimrSchemeOp::TFamilyDescription* to, Ydb::StatusIds::StatusCode& status, TString& error) {
    to->SetName(from.name());
    if (from.has_data()) {
        status = Ydb::StatusIds::BAD_REQUEST;
        error = TStringBuilder() << "Field `DATA` is not supported for OLAP tables in column family '" << from.name() << "'";
        return false;
    }
    switch (from.compression()) {
        case Ydb::Table::ColumnFamily::COMPRESSION_UNSPECIFIED:
            break;
        case Ydb::Table::ColumnFamily::COMPRESSION_NONE:
            to->SetColumnCodec(NKikimrSchemeOp::ColumnCodecPlain);
            break;
        case Ydb::Table::ColumnFamily::COMPRESSION_LZ4:
            to->SetColumnCodec(NKikimrSchemeOp::ColumnCodecLZ4);
            break;
        case Ydb::Table::ColumnFamily::COMPRESSION_ZSTD:
            to->SetColumnCodec(NKikimrSchemeOp::ColumnCodecZSTD);
            break;
        default:
            status = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unsupported compression value " << (ui32)from.compression() << " in column family '" << from.name()
                                     << "'";
            return false;
    }
    if (from.has_compression_level()) {
        to->SetColumnCodecLevel(from.compression_level());
    }
    return true;
}

bool BuildAlterColumnTableModifyScheme(const TString& path, const Ydb::Table::AlterTableRequest* req,
    NKikimrSchemeOp::TModifyScheme* modifyScheme, Ydb::StatusIds::StatusCode& status, TString& error) {
    const auto ops = GetAlterOperationKinds(req);
    if (ops.empty()) {
        status = Ydb::StatusIds::BAD_REQUEST;
        error = "Empty alter";
        return false;
    }

    if (ops.size() > 1) {
        status = Ydb::StatusIds::UNSUPPORTED;
        error = "Mixed alter is unsupported";
        return false;
    }

    const auto OpType = *ops.begin();

    std::pair<TString, TString> pathPair;
    try {
        pathPair = SplitPathIntoWorkingDirAndName(path);
    } catch (const std::exception&) {
        status = Ydb::StatusIds::BAD_REQUEST;
        return false;
    }

    const auto& workingDir = pathPair.first;
    const auto& name = pathPair.second;
    modifyScheme->SetWorkingDir(workingDir);

    if (OpType == EAlterOperationKind::Common) {
        auto alterColumnTable = modifyScheme->MutableAlterColumnTable();
        alterColumnTable->SetName(name);
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnTable);

        for (const auto& drop : req->drop_columns()) {
            alterColumnTable->MutableAlterSchema()->AddDropColumns()->SetName(drop);
        }

        if (!FillColumnDescription(*alterColumnTable, req->add_columns(), status, error)) {
            return false;
        }

        for (const auto& alter : req->alter_columns()) {
            auto alterColumn = alterColumnTable->MutableAlterSchema()->AddAlterColumns();
            alterColumn->SetName(alter.Getname());

            if (!alter.family().empty()) {
                alterColumn->SetColumnFamilyName(alter.family());
            }
        }

        for (const auto& add : req->add_column_families()) {
            if (add.compression() == Ydb::Table::ColumnFamily::COMPRESSION_UNSPECIFIED) {
                status = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Compression value is not set for column family '" << add.name() << "'";
            }
            if (!FillColumnFamily(add, alterColumnTable->MutableAlterSchema()->AddAddColumnFamily(), status, error)) {
                return false;
            }
        }

        for (const auto& alter : req->alter_column_families()) {
            if (!FillColumnFamily(alter, alterColumnTable->MutableAlterSchema()->AddAlterColumnFamily(), status, error)) {
                return false;
            }
        }

        if (req->has_set_ttl_settings()) {
            if (!FillTtlSettings(*alterColumnTable->MutableAlterTtlSettings()->MutableEnabled(), req->Getset_ttl_settings(), status, error)) {
                return false;
            }
        } else if (req->has_drop_ttl_settings()) {
            alterColumnTable->MutableAlterTtlSettings()->MutableDisabled();
        }
    }

    return true;
}

bool BuildAlterColumnTableModifyScheme(
    const Ydb::Table::AlterTableRequest* req, NKikimrSchemeOp::TModifyScheme* modifyScheme, Ydb::StatusIds::StatusCode& code, TString& error) {
    return BuildAlterColumnTableModifyScheme(req->path(), req, modifyScheme, code, error);
}

template <typename TYdbProto>
void FillTableBoundaryImpl(TYdbProto& out,
        const NKikimrSchemeOp::TTableDescription& in, const NKikimrMiniKQL::TType& splitKeyType) {

    for (const auto& boundary : in.GetSplitBoundary()) {
        if (boundary.HasSerializedKeyPrefix()) {
            throw NYql::TErrorException(NKikimrIssues::TIssuesIds::DEFAULT_ERROR)
                << "Unexpected serialized response from txProxy";
        } else if (boundary.HasKeyPrefix()) {
            Ydb::TypedValue* ydbValue = nullptr;

            if constexpr (std::is_same<TYdbProto, Ydb::Table::DescribeTableResult>::value) {
                ydbValue = out.add_shard_key_bounds();
            } else if constexpr (std::is_same<TYdbProto, Ydb::Table::CreateTableRequest>::value
                || std::is_same<TYdbProto, Ydb::Table::GlobalIndexSettings>::value
            ) {
                ydbValue = out.mutable_partition_at_keys()->add_split_points();
            } else {
                Y_ABORT("Unknown proto type");
            }

            ConvertMiniKQLTypeToYdbType(
                splitKeyType,
                *ydbValue->mutable_type());

            ConvertMiniKQLValueToYdbValue(
                splitKeyType,
                boundary.GetKeyPrefix(),
                *ydbValue->mutable_value());
        } else {
            throw NYql::TErrorException(NKikimrIssues::TIssuesIds::DEFAULT_ERROR)
                << "Got invalid boundary";
        }
    }
}

void FillTableBoundary(Ydb::Table::DescribeTableResult& out,
        const NKikimrSchemeOp::TTableDescription& in, const NKikimrMiniKQL::TType& splitKeyType) {
    FillTableBoundaryImpl<Ydb::Table::DescribeTableResult>(out, in, splitKeyType);
}

void FillTableBoundary(Ydb::Table::CreateTableRequest& out,
        const NKikimrSchemeOp::TTableDescription& in, const NKikimrMiniKQL::TType& splitKeyType) {
    FillTableBoundaryImpl<Ydb::Table::CreateTableRequest>(out, in, splitKeyType);
}

template <typename TYdbProto>
static void FillDefaultPartitioningSettings(TYdbProto& out) {
    // (!) We assume that all partitioning methods are disabled by default. But we don't know it for sure.
    out.set_partitioning_by_size(Ydb::FeatureFlag::DISABLED);
    out.set_partitioning_by_load(Ydb::FeatureFlag::DISABLED);
}

template <typename TYdbProto>
void FillPartitioningSettings(TYdbProto& out, const NKikimrSchemeOp::TPartitioningPolicy& policy) {
    if (policy.HasSizeToSplit()) {
        if (policy.GetSizeToSplit()) {
            out.set_partitioning_by_size(Ydb::FeatureFlag::ENABLED);
            out.set_partition_size_mb(policy.GetSizeToSplit() / (1 << 20));
        } else {
            out.set_partitioning_by_size(Ydb::FeatureFlag::DISABLED);
        }
    } else {
        // (!) We assume that partitioning by size is disabled by default. But we don't know it for sure.
        out.set_partitioning_by_size(Ydb::FeatureFlag::DISABLED);
    }

    if (policy.HasSplitByLoadSettings()) {
        bool enabled = policy.GetSplitByLoadSettings().GetEnabled();
        out.set_partitioning_by_load(enabled ? Ydb::FeatureFlag::ENABLED : Ydb::FeatureFlag::DISABLED);
    } else {
        // (!) We assume that partitioning by load is disabled by default. But we don't know it for sure.
        out.set_partitioning_by_load(Ydb::FeatureFlag::DISABLED);
    }

    if (policy.HasMinPartitionsCount() && policy.GetMinPartitionsCount()) {
        out.set_min_partitions_count(policy.GetMinPartitionsCount());
    }

    if (policy.HasMaxPartitionsCount() && policy.GetMaxPartitionsCount()) {
        out.set_max_partitions_count(policy.GetMaxPartitionsCount());
    }
}

template <typename TYdbProto>
void FillPartitioningSettingsImpl(TYdbProto& out,
        const NKikimrSchemeOp::TTableDescription& in) {

    auto& outPartSettings = *out.mutable_partitioning_settings();

    if (!in.HasPartitionConfig()) {
        FillDefaultPartitioningSettings(outPartSettings);
        return;
    }

    const auto& partConfig = in.GetPartitionConfig();
    if (!partConfig.HasPartitioningPolicy()) {
        FillDefaultPartitioningSettings(outPartSettings);
        return;
    }

    FillPartitioningSettings(outPartSettings, partConfig.GetPartitioningPolicy());
}

void FillGlobalIndexSettings(Ydb::Table::GlobalIndexSettings& settings,
    const NKikimrSchemeOp::TTableDescription& indexImplTableDescription
) {
    if (indexImplTableDescription.SplitBoundarySize()) {
        NKikimrMiniKQL::TType splitKeyType;
        Ydb::Table::DescribeTableResult unused;
        FillColumnDescription(unused, splitKeyType, indexImplTableDescription);
        FillTableBoundaryImpl<Ydb::Table::GlobalIndexSettings>(
            settings,
            indexImplTableDescription,
            splitKeyType
        );
    }

    FillPartitioningSettingsImpl(settings, indexImplTableDescription);
    FillReadReplicasSettings(settings, indexImplTableDescription);
}

template <typename TYdbProto>
void FillIndexDescriptionImpl(TYdbProto& out, const NKikimrSchemeOp::TTableDescription& in) {

    for (const auto& tableIndex : in.GetTableIndexes()) {
        auto index = out.add_indexes();

        index->set_name(tableIndex.GetName());

        *index->mutable_index_columns() = {
            tableIndex.GetKeyColumnNames().begin(),
            tableIndex.GetKeyColumnNames().end()
        };

        *index->mutable_data_columns() = {
            tableIndex.GetDataColumnNames().begin(),
            tableIndex.GetDataColumnNames().end()
        };

        switch (tableIndex.GetType()) {
        case NKikimrSchemeOp::EIndexType::EIndexTypeGlobal:
            FillGlobalIndexSettings(
                *index->mutable_global_index()->mutable_settings(),
                tableIndex.GetIndexImplTableDescriptions(0)
            );
            break;
        case NKikimrSchemeOp::EIndexType::EIndexTypeGlobalAsync:
            FillGlobalIndexSettings(
                *index->mutable_global_async_index()->mutable_settings(),
                tableIndex.GetIndexImplTableDescriptions(0)
            );
            break;
        case NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique:
            FillGlobalIndexSettings(
                *index->mutable_global_unique_index()->mutable_settings(),
                tableIndex.GetIndexImplTableDescriptions(0)
            );
            break;
        case NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree: {
            FillGlobalIndexSettings(
                *index->mutable_global_vector_kmeans_tree_index()->mutable_level_table_settings(),
                tableIndex.GetIndexImplTableDescriptions(0)
            );
            FillGlobalIndexSettings(
                *index->mutable_global_vector_kmeans_tree_index()->mutable_posting_table_settings(),
                tableIndex.GetIndexImplTableDescriptions(1)
            );
            const bool prefixVectorIndex = tableIndex.GetKeyColumnNames().size() > 1;
            if (prefixVectorIndex) {
                FillGlobalIndexSettings(
                    *index->mutable_global_vector_kmeans_tree_index()->mutable_prefix_table_settings(),
                    tableIndex.GetIndexImplTableDescriptions(2)
                );                    
            }

            *index->mutable_global_vector_kmeans_tree_index()->mutable_vector_settings() = tableIndex.GetVectorIndexKmeansTreeDescription().GetSettings();

            break;
        }
        default:
            break;
        };

        if constexpr (std::is_same<TYdbProto, Ydb::Table::DescribeTableResult>::value) {
            if (tableIndex.GetState() == NKikimrSchemeOp::EIndexState::EIndexStateReady) {
                index->set_status(Ydb::Table::TableIndexDescription::STATUS_READY);
            } else {
                index->set_status(Ydb::Table::TableIndexDescription::STATUS_BUILDING);
            }
            index->set_size_bytes(tableIndex.GetDataSize());
        }
    }
}

void FillIndexDescription(Ydb::Table::DescribeTableResult& out,
        const NKikimrSchemeOp::TTableDescription& in) {
    FillIndexDescriptionImpl(out, in);
}

void FillIndexDescription(Ydb::Table::CreateTableRequest& out,
        const NKikimrSchemeOp::TTableDescription& in) {
    FillIndexDescriptionImpl(out, in);
}

bool FillIndexDescription(NKikimrSchemeOp::TIndexedTableCreationConfig& out,
    const Ydb::Table::CreateTableRequest& in, Ydb::StatusIds::StatusCode& status, TString& error) {

    auto returnError = [&status, &error](Ydb::StatusIds::StatusCode code, const TString& msg) -> bool {
        status = code;
        error = msg;
        return false;
    };

    for (const auto& index : in.indexes()) {
        auto indexDesc = out.MutableIndexDescription()->Add();

        if (!index.data_columns().empty() && !AppData()->FeatureFlags.GetEnableDataColumnForIndexTable()) {
            return returnError(Ydb::StatusIds::UNSUPPORTED, "Data column feature is not supported yet");
        }

        // common fields
        indexDesc->SetName(index.name());

        for (const auto& col : index.index_columns()) {
            indexDesc->AddKeyColumnNames(col);
        }

        for (const auto& col : index.data_columns()) {
            indexDesc->AddDataColumnNames(col);
        }

        // specific fields
        std::vector<NKikimrSchemeOp::TTableDescription> indexImplTableDescriptionsVector;
        switch (index.type_case()) {
        case Ydb::Table::TableIndex::kGlobalIndex:
            indexDesc->SetType(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
            break;

        case Ydb::Table::TableIndex::kGlobalAsyncIndex:
            indexDesc->SetType(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalAsync);
            break;

        case Ydb::Table::TableIndex::kGlobalUniqueIndex:
            indexDesc->SetType(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
            break;

        case Ydb::Table::TableIndex::kGlobalVectorKmeansTreeIndex:
            indexDesc->SetType(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree);
            *indexDesc->MutableVectorIndexKmeansTreeDescription()->MutableSettings() = index.global_vector_kmeans_tree_index().vector_settings();
            break;

        default:
            // pass through
            // TODO: maybe return BAD_REQUEST?
            break;
        }

        if (!FillIndexTablePartitioning(indexImplTableDescriptionsVector, index, status, error)) {
            return false;
        }
        *indexDesc->MutableIndexImplTableDescriptions() = {indexImplTableDescriptionsVector.begin(), indexImplTableDescriptionsVector.end()};
    }

    return true;
}

template <typename TOutProto, typename TInProto>
void FillAttributesImpl(TOutProto& out, const TInProto& in) {
    if (!in.UserAttributesSize()) {
        return;
    }

    auto& outAttrs = *out.mutable_attributes();
    for (const auto& inAttr : in.GetUserAttributes()) {
        outAttrs[inAttr.GetKey()] = inAttr.GetValue();
    }
}
void FillChangefeedDescription(Ydb::Table::ChangefeedDescription& out,
    const NKikimrSchemeOp::TCdcStreamDescription& in) {

    out.set_name(in.GetName());
    out.set_virtual_timestamps(in.GetVirtualTimestamps());
    out.set_schema_changes(in.GetSchemaChanges());
    out.set_aws_region(in.GetAwsRegion());

    if (const auto value = in.GetResolvedTimestampsIntervalMs()) {
        out.mutable_resolved_timestamps_interval()->set_seconds(TDuration::MilliSeconds(value).Seconds());
    }

    switch (in.GetMode()) {
    case NKikimrSchemeOp::ECdcStreamMode::ECdcStreamModeKeysOnly:
    case NKikimrSchemeOp::ECdcStreamMode::ECdcStreamModeUpdate:
    case NKikimrSchemeOp::ECdcStreamMode::ECdcStreamModeNewImage:
    case NKikimrSchemeOp::ECdcStreamMode::ECdcStreamModeOldImage:
    case NKikimrSchemeOp::ECdcStreamMode::ECdcStreamModeNewAndOldImages:
        out.set_mode(static_cast<Ydb::Table::ChangefeedMode::Mode>(in.GetMode()));
        break;
    default:
        break;
    }

    switch (in.GetFormat()) {
    case NKikimrSchemeOp::ECdcStreamFormat::ECdcStreamFormatJson:
        out.set_format(Ydb::Table::ChangefeedFormat::FORMAT_JSON);
        break;
    case NKikimrSchemeOp::ECdcStreamFormat::ECdcStreamFormatDynamoDBStreamsJson:
        out.set_format(Ydb::Table::ChangefeedFormat::FORMAT_DYNAMODB_STREAMS_JSON);
        break;
    case NKikimrSchemeOp::ECdcStreamFormat::ECdcStreamFormatDebeziumJson:
        out.set_format(Ydb::Table::ChangefeedFormat::FORMAT_DEBEZIUM_JSON);
        break;
    default:
        break;
    }

    switch (in.GetState()) {
    case NKikimrSchemeOp::ECdcStreamState::ECdcStreamStateReady:
    case NKikimrSchemeOp::ECdcStreamState::ECdcStreamStateDisabled:
    case NKikimrSchemeOp::ECdcStreamState::ECdcStreamStateScan:
        out.set_state(static_cast<Ydb::Table::ChangefeedDescription::State>(in.GetState()));
        break;
    default:
        break;
    }

    if (in.HasScanProgress()) {
        auto& scanProgress = *out.mutable_initial_scan_progress();
        scanProgress.set_parts_total(in.GetScanProgress().GetShardsTotal());
        scanProgress.set_parts_completed(in.GetScanProgress().GetShardsCompleted());
    }

    FillAttributesImpl(out, in);

}
void FillChangefeedDescription(Ydb::Table::DescribeTableResult& out,
        const NKikimrSchemeOp::TTableDescription& in) {

    for (const auto& stream : in.GetCdcStreams()) {
        FillChangefeedDescription(*out.add_changefeeds(), stream);
    }
}

template <typename T>
bool FillChangefeedDescriptionCommon(NKikimrSchemeOp::TCdcStreamDescription& out,
        const T& in, Ydb::StatusIds::StatusCode& status, TString& error) {

    out.SetName(in.name());
    out.SetVirtualTimestamps(in.virtual_timestamps());
    out.SetSchemaChanges(in.schema_changes());
    out.SetAwsRegion(in.aws_region());

    if (in.has_resolved_timestamps_interval()) {
        out.SetResolvedTimestampsIntervalMs(TDuration::Seconds(in.resolved_timestamps_interval().seconds()).MilliSeconds());
    }

    switch (in.mode()) {
    case Ydb::Table::ChangefeedMode::MODE_KEYS_ONLY:
    case Ydb::Table::ChangefeedMode::MODE_UPDATES:
    case Ydb::Table::ChangefeedMode::MODE_NEW_IMAGE:
    case Ydb::Table::ChangefeedMode::MODE_OLD_IMAGE:
    case Ydb::Table::ChangefeedMode::MODE_NEW_AND_OLD_IMAGES:
        out.SetMode(static_cast<NKikimrSchemeOp::ECdcStreamMode>(in.mode()));
        break;
    default:
        status = Ydb::StatusIds::BAD_REQUEST;
        error = "Invalid changefeed mode";
        return false;
    }

    switch (in.format()) {
    case Ydb::Table::ChangefeedFormat::FORMAT_JSON:
        out.SetFormat(NKikimrSchemeOp::ECdcStreamFormat::ECdcStreamFormatJson);
        break;
    case Ydb::Table::ChangefeedFormat::FORMAT_DYNAMODB_STREAMS_JSON:
        out.SetFormat(NKikimrSchemeOp::ECdcStreamFormat::ECdcStreamFormatDynamoDBStreamsJson);
        break;
    case Ydb::Table::ChangefeedFormat::FORMAT_DEBEZIUM_JSON:
        out.SetFormat(NKikimrSchemeOp::ECdcStreamFormat::ECdcStreamFormatDebeziumJson);
        break;
    default:
        status = Ydb::StatusIds::BAD_REQUEST;
        error = "Invalid changefeed format";
        return false;
    }

    for (const auto& [key, value] : in.attributes()) {
        auto& attr = *out.AddUserAttributes();
        attr.SetKey(key);
        attr.SetValue(value);
    }

    return true;
}

bool FillChangefeedDescription(NKikimrSchemeOp::TCdcStreamDescription& out,
        const Ydb::Table::Changefeed& in, Ydb::StatusIds::StatusCode& status, TString& error) {
    if (in.initial_scan()) {
        if (!AppData()->FeatureFlags.GetEnableChangefeedInitialScan()) {
            status = Ydb::StatusIds::UNSUPPORTED;
            error = "Changefeed initial scan is not supported yet";
            return false;
        }
        out.SetState(NKikimrSchemeOp::ECdcStreamState::ECdcStreamStateScan);
    }
    return FillChangefeedDescriptionCommon(out, in, status, error);
}

bool FillChangefeedDescription(NKikimrSchemeOp::TCdcStreamDescription& out,
        const Ydb::Table::ChangefeedDescription& in, Ydb::StatusIds::StatusCode& status, TString& error) {
    return FillChangefeedDescriptionCommon(out, in, status, error);
}

void FillTableStats(Ydb::Table::DescribeTableResult& out,
        const NKikimrSchemeOp::TPathDescription& in, bool withPartitionStatistic, const TMap<ui64, ui64>& nodeMap) {

    auto stats = out.mutable_table_stats();

    if (withPartitionStatistic) {
        for (const auto& tablePartitionStat : in.GetTablePartitionStats()) {
            auto partition = stats->add_partition_stats();
            partition->set_rows_estimate(tablePartitionStat.GetRowCount());
            partition->set_store_size(tablePartitionStat.GetDataSize() + tablePartitionStat.GetIndexSize());
        }
    }

    if (!nodeMap.empty()) {
        size_t id = 0;
        if ((size_t)stats->partition_stats_size() != in.TablePartitionsSize()) {
            ythrow yexception() << "malformed TPathDescription.";
        }

        for (const auto& part : in.GetTablePartitions()) {
            auto it = nodeMap.find(part.GetDatashardId());
            if (it == nodeMap.end()) {
                ythrow yexception() << "unknown datashardId to fill DescribeTableResult";
            }
            stats->mutable_partition_stats(id++)->set_leader_node_id(it->second);
        }
    }

    stats->set_rows_estimate(in.GetTableStats().GetRowCount());
    stats->set_partitions(in.GetTableStats().GetPartCount());

    stats->set_store_size(in.GetTableStats().GetDataSize() + in.GetTableStats().GetIndexSize());
    for (const auto& index : in.GetTable().GetTableIndexes()) {
        stats->set_store_size(stats->store_size() + index.GetDataSize());
    }

    ui64 modificationTimeMs = in.GetTableStats().GetLastUpdateTime();
    if (modificationTimeMs) {
        auto modificationTime = MillisecToProtoTimeStamp(modificationTimeMs);
        stats->mutable_modification_time()->CopyFrom(modificationTime);
    }

    ui64 creationTimeMs = in.GetSelf().GetCreateStep();
    if (creationTimeMs) {
        auto creationTime = MillisecToProtoTimeStamp(creationTimeMs);
        stats->mutable_creation_time()->CopyFrom(creationTime);
    }
}

static bool IsDefaultFamily(const NKikimrSchemeOp::TFamilyDescription& family) {
    if (family.HasId() && family.GetId() == 0) {
        return true; // explicit id 0
    }
    if (!family.HasId() && !family.HasName()) {
        return true; // neither id nor name specified
    }
    return false;
}

template <typename TYdbProto>
void FillStorageSettingsImpl(TYdbProto& out,
        const NKikimrSchemeOp::TTableDescription& in) {

    if (!in.HasPartitionConfig()) {
        return;
    }

    const auto& partConfig = in.GetPartitionConfig();
    if (partConfig.ColumnFamiliesSize() == 0) {
        return;
    }

    for (size_t i = 0; i < partConfig.ColumnFamiliesSize(); ++i) {
        const auto& family = partConfig.GetColumnFamilies(i);
        if (IsDefaultFamily(family)) {
            // Default family also specifies some per-table storage settings
            auto* settings = out.mutable_storage_settings();
            settings->set_store_external_blobs(Ydb::FeatureFlag::DISABLED);

            if (family.HasStorageConfig()) {
                using StorageSettings = Ydb::Table::StorageSettings;

                if (family.GetStorageConfig().HasSysLog()) {
                    FillStoragePool(settings, &StorageSettings::mutable_tablet_commit_log0, family.GetStorageConfig().GetSysLog());
                }
                if (family.GetStorageConfig().HasLog()) {
                    FillStoragePool(settings, &StorageSettings::mutable_tablet_commit_log1, family.GetStorageConfig().GetLog());
                }
                if (family.GetStorageConfig().HasExternal()) {
                    FillStoragePool(settings, &StorageSettings::mutable_external, family.GetStorageConfig().GetExternal());
                }

                const ui32 externalThreshold = family.GetStorageConfig().GetExternalThreshold();
                if (externalThreshold != 0 && externalThreshold != Max<ui32>()) {
                    settings->set_store_external_blobs(Ydb::FeatureFlag::ENABLED);
                }
            }

            // Check legacy settings for enabled external blobs
            switch (family.GetStorage()) {
                case NKikimrSchemeOp::ColumnStorage1:
                    // default or unset, no legacy external blobs
                    break;
                case NKikimrSchemeOp::ColumnStorage2:
                case NKikimrSchemeOp::ColumnStorage1Ext1:
                case NKikimrSchemeOp::ColumnStorage1Ext2:
                case NKikimrSchemeOp::ColumnStorage2Ext1:
                case NKikimrSchemeOp::ColumnStorage2Ext2:
                case NKikimrSchemeOp::ColumnStorage1Med2Ext2:
                case NKikimrSchemeOp::ColumnStorage2Med2Ext2:
                case NKikimrSchemeOp::ColumnStorageTest_1_2_1k:
                    settings->set_store_external_blobs(Ydb::FeatureFlag::ENABLED);
                    break;
            }

            break;
        }
    }
}

void FillStorageSettings(Ydb::Table::DescribeTableResult& out,
        const NKikimrSchemeOp::TTableDescription& in) {
    FillStorageSettingsImpl(out, in);
}

void FillStorageSettings(Ydb::Table::CreateTableRequest& out,
        const NKikimrSchemeOp::TTableDescription& in) {
    FillStorageSettingsImpl(out, in);
}

void FillColumnFamily(Ydb::Table::ColumnFamily& out, const NKikimrSchemeOp::TFamilyDescription& in, bool isColumnTable) {
    if (in.HasName() && !in.GetName().empty()) {
        out.set_name(in.GetName());
    } else if (IsDefaultFamily(in)) {
        out.set_name("default");
    } else if (in.HasId()) {
        out.set_name(TStringBuilder() << "<id: " << in.GetId() << ">");
    } else {
        out.set_name(in.GetName());
    }

    if (!isColumnTable && in.HasStorageConfig() && in.GetStorageConfig().HasData()) {
        FillStoragePool(&out, &Ydb::Table::ColumnFamily::mutable_data, in.GetStorageConfig().GetData());
    }

    if (in.HasColumnCodec()) {
        switch (in.GetColumnCodec()) {
            case NKikimrSchemeOp::ColumnCodecPlain:
                out.set_compression(Ydb::Table::ColumnFamily::COMPRESSION_NONE);
                break;
            case NKikimrSchemeOp::ColumnCodecLZ4:
                out.set_compression(Ydb::Table::ColumnFamily::COMPRESSION_LZ4);
                break;
            case NKikimrSchemeOp::ColumnCodecZSTD: {
                if (!isColumnTable) {
                    break; // FIXME: not supported
                }
                out.set_compression(Ydb::Table::ColumnFamily::COMPRESSION_ZSTD);
                break;
            }
        }
    } else if (in.GetCodec() == 1) {
        // Legacy setting, see datashard
        out.set_compression(Ydb::Table::ColumnFamily::COMPRESSION_LZ4);
    } else {
        out.set_compression(Ydb::Table::ColumnFamily::COMPRESSION_NONE);
    }

    // Check legacy settings for permanent in-memory cache
    if (in.GetInMemory() || in.GetColumnCache() == NKikimrSchemeOp::ColumnCacheEver) {
        out.set_keep_in_memory(Ydb::FeatureFlag::ENABLED);
    }
}

template <typename TYdbProto>
void FillColumnFamiliesImpl(TYdbProto& out,
        const NKikimrSchemeOp::TTableDescription& in) {

    if (!in.HasPartitionConfig()) {
        return;
    }

    const auto& partConfig = in.GetPartitionConfig();
    if (partConfig.ColumnFamiliesSize() == 0) {
        return;
    }

    for (size_t i = 0; i < partConfig.ColumnFamiliesSize(); ++i) {
        const auto& family = partConfig.GetColumnFamilies(i);
        auto* r = out.add_column_families();

        FillColumnFamily(*r, family, false);
    }
}

void FillColumnFamilies(Ydb::Table::DescribeTableResult& out,
        const NKikimrSchemeOp::TTableDescription& in) {
    FillColumnFamiliesImpl(out, in);
}

void FillColumnFamilies(Ydb::Table::CreateTableRequest& out,
        const NKikimrSchemeOp::TTableDescription& in) {
    FillColumnFamiliesImpl(out, in);
}

template <typename TYdbProto>
void FillColumnFamiliesImpl(TYdbProto& out,
        const NKikimrSchemeOp::TColumnTableDescription& in) {
    const auto& schema = in.GetSchema();
    for (size_t i = 0; i < schema.ColumnFamiliesSize(); ++i) {
        const auto& family = schema.GetColumnFamilies(i);
        auto* r = out.add_column_families();

        FillColumnFamily(*r, family, true);
    }
}

void FillColumnFamilies(Ydb::Table::DescribeTableResult& out,
        const NKikimrSchemeOp::TColumnTableDescription& in) {
    FillColumnFamiliesImpl(out, in);
}

void FillColumnFamilies(Ydb::Table::CreateTableRequest& out,
        const NKikimrSchemeOp::TColumnTableDescription& in) {
    FillColumnFamiliesImpl(out, in);
}

void FillAttributes(Ydb::Table::DescribeTableResult& out,
        const NKikimrSchemeOp::TPathDescription& in) {
    FillAttributesImpl(out, in);
}

void FillAttributes(Ydb::Table::CreateTableRequest& out,
        const NKikimrSchemeOp::TPathDescription& in) {
    FillAttributesImpl(out, in);
}

void FillPartitioningSettings(Ydb::Table::DescribeTableResult& out,
        const NKikimrSchemeOp::TTableDescription& in) {
    FillPartitioningSettingsImpl(out, in);
}

void FillPartitioningSettings(Ydb::Table::CreateTableRequest& out,
        const NKikimrSchemeOp::TTableDescription& in) {
    FillPartitioningSettingsImpl(out, in);
}

bool CopyExplicitPartitions(NKikimrSchemeOp::TTableDescription& out,
    const Ydb::Table::ExplicitPartitions& in, Ydb::StatusIds::StatusCode& status, TString& error) {

    try {
        for (auto &point : in.split_points()) {
            auto &dst = *out.AddSplitBoundary()->MutableKeyPrefix();
            ConvertYdbValueToMiniKQLValue(point.type(), point.value(), dst);
        }
    } catch (const std::exception &e) {
        status = Ydb::StatusIds::BAD_REQUEST;
        error = TString("cannot convert split points: ") + e.what();
        return false;
    }

    return true;
}

template <typename TYdbProto>
void FillKeyBloomFilterImpl(TYdbProto& out,
        const NKikimrSchemeOp::TTableDescription& in) {

    if (!in.HasPartitionConfig()) {
        return;
    }

    const auto& partConfig = in.GetPartitionConfig();
    if (!partConfig.HasEnableFilterByKey()) {
        return;
    }

    if (partConfig.GetEnableFilterByKey()) {
        out.set_key_bloom_filter(Ydb::FeatureFlag::ENABLED);
    } else {
        out.set_key_bloom_filter(Ydb::FeatureFlag::DISABLED);
    }
}

void FillKeyBloomFilter(Ydb::Table::DescribeTableResult& out,
        const NKikimrSchemeOp::TTableDescription& in) {
    FillKeyBloomFilterImpl(out, in);
}

void FillKeyBloomFilter(Ydb::Table::CreateTableRequest& out,
        const NKikimrSchemeOp::TTableDescription& in) {
    FillKeyBloomFilterImpl(out, in);
}

template <typename TYdbProto>
void FillReadReplicasSettingsImpl(TYdbProto& out,
        const NKikimrSchemeOp::TTableDescription& in) {

    if (!in.HasPartitionConfig()) {
        return;
    }

    const auto& partConfig = in.GetPartitionConfig();
    if (!partConfig.FollowerGroupsSize() && !partConfig.HasCrossDataCenterFollowerCount() && !partConfig.HasFollowerCount()) {
        return;
    }

    if (partConfig.FollowerGroupsSize()) {
        if (partConfig.FollowerGroupsSize() > 1) {
            // Not supported yet
            return;
        }
        const auto& followerGroup = partConfig.GetFollowerGroups(0);
        if (followerGroup.GetFollowerCountPerDataCenter()) {
            out.mutable_read_replicas_settings()->set_per_az_read_replicas_count(followerGroup.GetFollowerCount());
        } else {
            out.mutable_read_replicas_settings()->set_any_az_read_replicas_count(followerGroup.GetFollowerCount());
        }
    } else if (partConfig.HasCrossDataCenterFollowerCount()) {
        out.mutable_read_replicas_settings()->set_per_az_read_replicas_count(partConfig.GetCrossDataCenterFollowerCount());
    } else if (partConfig.HasFollowerCount()) {
        out.mutable_read_replicas_settings()->set_any_az_read_replicas_count(partConfig.GetFollowerCount());
    }
}

void FillReadReplicasSettings(Ydb::Table::DescribeTableResult& out,
        const NKikimrSchemeOp::TTableDescription& in) {
    FillReadReplicasSettingsImpl(out, in);
}

void FillReadReplicasSettings(Ydb::Table::CreateTableRequest& out,
        const NKikimrSchemeOp::TTableDescription& in) {
    FillReadReplicasSettingsImpl(out, in);
}

void FillReadReplicasSettings(Ydb::Table::GlobalIndexSettings& out,
    const NKikimrSchemeOp::TTableDescription& in) {
    FillReadReplicasSettingsImpl(out, in);
}

bool FillTableDescription(NKikimrSchemeOp::TModifyScheme& out,
        const Ydb::Table::CreateTableRequest& in, const TTableProfiles& profiles,
        Ydb::StatusIds::StatusCode& status, TString& error, bool indexedTable)
{
    NKikimrSchemeOp::TTableDescription* tableDesc = nullptr;
    if (indexedTable) {
        tableDesc = out.MutableCreateIndexedTable()->MutableTableDescription();
    } else {
        tableDesc = out.MutableCreateTable();
    }

    if (!FillColumnDescription(*tableDesc, in.columns(), status, error)) {
        return false;
    }

    tableDesc->MutableKeyColumnNames()->CopyFrom(in.primary_key());

    if (!profiles.ApplyTableProfile(in.profile(), *tableDesc, status, error)) {
        return false;
    }

    TColumnFamilyManager families(tableDesc->MutablePartitionConfig());
    if (in.has_storage_settings() && !families.ApplyStorageSettings(in.storage_settings(), &status, &error)) {
        return false;
    }
    for (const auto& familySettings : in.column_families()) {
        if (!families.ApplyFamilySettings(familySettings, &status, &error)) {
            return false;
        }
    }

    for (auto [key, value] : in.attributes()) {
        auto& attr = *out.MutableAlterUserAttributes()->AddUserAttributes();
        attr.SetKey(key);
        attr.SetValue(value);
    }

    TList<TString> warnings;
    if (!FillCreateTableSettingsDesc(*tableDesc, in, status, error, warnings, false)) {
        return false;
    }

    return true;
}

template <typename TYdbProto>
bool FillSequenceDescriptionImpl(TYdbProto& out, const NKikimrSchemeOp::TTableDescription& in, Ydb::StatusIds::StatusCode& status, TString& error) {
    THashMap<TString, NKikimrSchemeOp::TSequenceDescription> sequences;

    for (const auto& sequenceDescription : in.GetSequences()) {
        sequences[sequenceDescription.GetName()] = sequenceDescription;
    }

    for (auto& column : *out.mutable_columns()) {

        switch (column.default_value_case()) {
            case Ydb::Table::ColumnMeta::kFromSequence: {
                auto* fromSequence = column.mutable_from_sequence();

                const auto& sequenceDescription = sequences.at(fromSequence->name());

                if (sequenceDescription.HasMinValue()) {
                    fromSequence->set_min_value(sequenceDescription.GetMinValue());
                }
                if (sequenceDescription.HasMaxValue()) {
                    fromSequence->set_max_value(sequenceDescription.GetMaxValue());
                }
                if (sequenceDescription.HasStartValue()) {
                    fromSequence->set_start_value(sequenceDescription.GetStartValue());
                }
                if (sequenceDescription.HasCache()) {
                    fromSequence->set_cache(sequenceDescription.GetCache());
                }
                if (sequenceDescription.HasIncrement()) {
                    fromSequence->set_increment(sequenceDescription.GetIncrement());
                }
                if (sequenceDescription.HasCycle()) {
                    fromSequence->set_cycle(sequenceDescription.GetCycle());
                }
                if (sequenceDescription.HasSetVal()) {
                    auto* setVal = fromSequence->mutable_set_val();
                    setVal->set_next_used(sequenceDescription.GetSetVal().GetNextUsed());
                    setVal->set_next_value(sequenceDescription.GetSetVal().GetNextValue());
                }
                if (sequenceDescription.HasDataType()) {
                    auto* dataType = fromSequence->mutable_data_type();
                    auto typeDesc = NPg::TypeDescFromPgTypeName(sequenceDescription.GetDataType());
                    if (typeDesc) {
                        auto* pg = dataType->mutable_pg_type();
                        auto typeId = NPg::PgTypeIdFromTypeDesc(typeDesc);
                        switch (typeId) {
                            case INT2OID:
                            case INT4OID:
                            case INT8OID:
                                break;
                            default: {
                                TString sequenceType = NPg::PgTypeNameFromTypeDesc(typeDesc);
                                status = Ydb::StatusIds::BAD_REQUEST;
                                error = Sprintf(
                                    "Invalid type name %s for sequence: %s", sequenceType.c_str(), sequenceDescription.GetName().data()
                                );
                                return false;
                                break;
                            }
                        }
                        pg->set_type_name(NPg::PgTypeNameFromTypeDesc(typeDesc));
                        pg->set_type_modifier(NPg::TypeModFromPgTypeName(sequenceDescription.GetDataType()));
                        pg->set_oid(NPg::PgTypeIdFromTypeDesc(typeDesc));
                        pg->set_typlen(0);
                        pg->set_typmod(0);
                    } else {
                        NYql::NProto::TypeIds protoType;
                        if (!NYql::NProto::TypeIds_Parse(sequenceDescription.GetDataType(), &protoType)) {
                            status = Ydb::StatusIds::BAD_REQUEST;
                            error = Sprintf(
                                "Invalid type name %s for sequence: %s", sequenceDescription.GetDataType().data(), sequenceDescription.GetName().data()
                            );
                            return false;
                        }
                        switch (protoType) {
                            case NYql::NProto::TypeIds::Int16:
                            case NYql::NProto::TypeIds::Int32:
                            case NYql::NProto::TypeIds::Int64: {
                                NMiniKQL::ExportPrimitiveTypeToProto(protoType, *dataType);
                                break;
                            }
                            default: {
                                status = Ydb::StatusIds::BAD_REQUEST;
                                error = Sprintf(
                                    "Invalid type name %s for sequence: %s", sequenceDescription.GetDataType().data(), sequenceDescription.GetName().data()
                                );
                                return false;
                            }
                        }
                    }
                }
                break;
            }
            case Ydb::Table::ColumnMeta::kFromLiteral: {
                break;
            }
            default: break;
        }
    }
    return true;
}

bool FillSequenceDescription(Ydb::Table::DescribeTableResult& out, const NKikimrSchemeOp::TTableDescription& in, Ydb::StatusIds::StatusCode& status, TString& error) {
    return FillSequenceDescriptionImpl(out, in, status, error);
}

bool FillSequenceDescription(Ydb::Table::CreateTableRequest& out, const NKikimrSchemeOp::TTableDescription& in, Ydb::StatusIds::StatusCode& status, TString& error) {
    return FillSequenceDescriptionImpl(out, in, status, error);
}

bool FillSequenceDescription(NKikimrSchemeOp::TSequenceDescription& out, const Ydb::Table::SequenceDescription& in, Ydb::StatusIds::StatusCode& status, TString& error) {
    out.SetName(in.name());
    if (in.has_min_value()) {
        out.SetMinValue(in.min_value());
    }
    if (in.has_max_value()) {
        out.SetMaxValue(in.max_value());
    }
    if (in.has_start_value()) {
        out.SetStartValue(in.start_value());
    }
    if (in.has_cache()) {
        out.SetCache(in.cache());
    }
    if (in.has_increment()) {
        out.SetIncrement(in.increment());
    }
    if (in.has_cycle()) {
        out.SetCycle(in.cycle());
    }
    if (in.has_set_val()) {
        auto* setVal = out.MutableSetVal();
        setVal->SetNextUsed(in.set_val().next_used());
        setVal->SetNextValue(in.set_val().next_value());
    }
    if (in.has_data_type()) {
        NScheme::TTypeInfo typeInfo;
        TString typeMod;
        if (!ExtractColumnTypeInfo(typeInfo, typeMod, in.data_type(), status, error)) {
            return false;
        }

        switch (typeInfo.GetTypeId()) {
            case NScheme::NTypeIds::Int16:
            case NScheme::NTypeIds::Int32:
            case NScheme::NTypeIds::Int64: {
                out.SetDataType(NScheme::TypeName(typeInfo, typeMod));
                break;
            }
            case NScheme::NTypeIds::Pg: {
                switch (NPg::PgTypeIdFromTypeDesc(typeInfo.GetPgTypeDesc())) {
                    case INT2OID:
                    case INT4OID:
                    case INT8OID: {
                        out.SetDataType(NScheme::TypeName(typeInfo, typeMod));
                        break;
                    }
                    default: {
                        TString sequenceType = NPg::PgTypeNameFromTypeDesc(typeInfo.GetPgTypeDesc());
                        status = Ydb::StatusIds::BAD_REQUEST;
                        error = Sprintf(
                            "Invalid type name %s for sequence: %s", sequenceType.c_str(), out.GetName().data()
                        );
                        return false;
                    }
                }
                break;
            }
            default: {
                TString sequenceType = NScheme::TypeName(typeInfo.GetTypeId());
                status = Ydb::StatusIds::BAD_REQUEST;
                error = Sprintf(
                    "Invalid type name %s for sequence: %s", sequenceType.c_str(), out.GetName().data()
                );
                return false;
            }
        }
    }
    return true;
}

}  // namespace NKikimr
