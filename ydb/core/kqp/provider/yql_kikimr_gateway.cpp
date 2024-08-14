#include "yql_kikimr_gateway.h"

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/protos/replication.pb.h>

#include <util/string/split.h>
#include <util/string/strip.h>

namespace NYql {

using namespace NThreading;
using namespace NKikimr::NMiniKQL;
using namespace NUdf;

static void CreateDirs(std::shared_ptr<TVector<TString>> partsHolder, size_t index,
    TPromise<IKikimrGateway::TGenericResult>& promise, IKikimrGateway::TCreateDirFunc createDir)
{
    auto partPromise = NewPromise<IKikimrGateway::TGenericResult>();
    auto& parts = *partsHolder;

    partPromise.GetFuture().Apply(
        [partsHolder, index, promise, createDir](const TFuture<IKikimrGateway::TGenericResult>& future) mutable {
            auto& result = future.GetValue();
            if (index == partsHolder->size() - 1) {
                promise.SetValue(result);
                return;
            }

            if (!result.Success() && result.Status() != TIssuesIds::KIKIMR_ACCESS_DENIED) {
                promise.SetValue(result);
                return;
            }

            CreateDirs(partsHolder, index + 1, promise, createDir);
        });

    TString basePath = NKikimr::NKqp::NSchemeHelpers::CombinePath(parts.begin(), parts.begin() + index);

    createDir(basePath, parts[index], partPromise);
}

TKikimrPathId TKikimrPathId::Parse(const TStringBuf& str) {
    TStringBuf ownerStr;
    TStringBuf idStr;
    YQL_ENSURE(str.TrySplit(':', ownerStr, idStr));

    return TKikimrPathId(FromString<ui64>(ownerStr), FromString<ui64>(idStr));
}

void TReplicationSettings::TOAuthToken::Serialize(NKikimrReplication::TOAuthToken& proto) const {
    if (Token) {
        proto.SetToken(Token);
    }
    if (TokenSecretName) {
        proto.SetTokenSecretName(TokenSecretName);
    }
}

void TReplicationSettings::TStaticCredentials::Serialize(NKikimrReplication::TStaticCredentials& proto) const {
    if (UserName) {
        proto.SetUser(UserName);
    }
    if (Password) {
        proto.SetPassword(Password);
    }
    if (PasswordSecretName) {
        proto.SetPasswordSecretName(PasswordSecretName);
    }
}

TFuture<IKikimrGateway::TGenericResult> IKikimrGateway::CreatePath(const TString& path, TCreateDirFunc createDir) {
    auto partsHolder = std::make_shared<TVector<TString>>(NKikimr::SplitPath(path));
    auto& parts = *partsHolder;

    if (parts.size() < 2) {
        TGenericResult result;
        result.SetSuccess();
        return MakeFuture<TGenericResult>(result);
    }

    auto pathPromise = NewPromise<TGenericResult>();
    CreateDirs(partsHolder, 1, pathPromise, createDir);

    return pathPromise.GetFuture();
}

void IKikimrGateway::BuildIndexMetadata(TTableMetadataResult& loadTableMetadataResult) {
    auto tableMetadata = loadTableMetadataResult.Metadata;
    YQL_ENSURE(tableMetadata);

    if (tableMetadata->Indexes.empty()) {
        return;
    }

    const auto& cluster = tableMetadata->Cluster;
    const auto& tableName = tableMetadata->Name;
    const size_t indexesCount = tableMetadata->Indexes.size();

    NKikimr::NTableIndex::TTableColumns tableColumns;
    tableColumns.Columns.reserve(tableMetadata->Columns.size());
    for (auto& column: tableMetadata->Columns) {
        tableColumns.Columns.insert_noresize(column.first);
    }
    tableColumns.Keys = tableMetadata->KeyColumnNames;

    tableMetadata->SecondaryGlobalIndexMetadata.resize(indexesCount);
    for (size_t i = 0; i < indexesCount; i++) {
        const auto& index = tableMetadata->Indexes[i];
        auto indexTablePath = NKikimr::NKqp::NSchemeHelpers::CreateIndexTablePath(tableName, index.Name);
        NKikimr::NTableIndex::TTableColumns indexTableColumns = NKikimr::NTableIndex::CalcTableImplDescription(
                    tableColumns,
                    NKikimr::NTableIndex::TIndexColumns{index.KeyColumns, {}});

        TKikimrTableMetadataPtr indexTableMetadata = new TKikimrTableMetadata(cluster, indexTablePath);
        indexTableMetadata->DoesExist = true;
        indexTableMetadata->KeyColumnNames = indexTableColumns.Keys;
        for (auto& column: indexTableColumns.Columns) {
            indexTableMetadata->Columns[column] = tableMetadata->Columns.at(column);
        }

        tableMetadata->SecondaryGlobalIndexMetadata[i] = indexTableMetadata;
    }
}

bool TTtlSettings::TryParse(const NNodes::TCoNameValueTupleList& node, TTtlSettings& settings, TString& error) {
    using namespace NNodes;

    for (const auto& field : node) {
        auto name = field.Name().Value();
        if (name == "columnName") {
            YQL_ENSURE(field.Value().Maybe<TCoAtom>());
            settings.ColumnName = field.Value().Cast<TCoAtom>().StringValue();
        } else if (name == "expireAfter") {
            YQL_ENSURE(field.Value().Maybe<TCoInterval>());
            auto value = FromString<i64>(field.Value().Cast<TCoInterval>().Literal().Value());
            if (value < 0) {
                error = "Interval value cannot be negative";
                return false;
            }

            settings.ExpireAfter = TDuration::FromValue(value);
        } else if (name == "columnUnit") {
            YQL_ENSURE(field.Value().Maybe<TCoAtom>());
            auto value = field.Value().Cast<TCoAtom>().StringValue();
            if (value == "seconds") {
                settings.ColumnUnit = EUnit::Seconds;
            } else if (value == "milliseconds") {
                settings.ColumnUnit = EUnit::Milliseconds;
            } else if (value == "microseconds") {
                settings.ColumnUnit = EUnit::Microseconds;
            } else if (value == "nanoseconds") {
                settings.ColumnUnit = EUnit::Nanoseconds;
            } else {
                error = TStringBuilder() << "Invalid unit: " << value;
                return false;
            }
        } else {
            error = TStringBuilder() << "Unknown field: " << name;
            return false;
        }
    }

    return true;
}

bool TTableSettings::IsSet() const {
    return CompactionPolicy || PartitionBy || AutoPartitioningBySize || UniformPartitions || PartitionAtKeys
        || PartitionSizeMb || AutoPartitioningByLoad || MinPartitions || MaxPartitions || KeyBloomFilter
        || ReadReplicasSettings || TtlSettings || DataSourcePath || Location || ExternalSourceParameters
        || StoreExternalBlobs;
}

EYqlIssueCode YqlStatusFromYdbStatus(ui32 ydbStatus) {
    switch (ydbStatus) {
        case Ydb::StatusIds::SUCCESS:
            return TIssuesIds::SUCCESS;
        case Ydb::StatusIds::BAD_REQUEST:
            return TIssuesIds::KIKIMR_BAD_REQUEST;
        case Ydb::StatusIds::UNAUTHORIZED:
            return TIssuesIds::KIKIMR_ACCESS_DENIED;
        case Ydb::StatusIds::INTERNAL_ERROR:
            return TIssuesIds::DEFAULT_ERROR;
        case Ydb::StatusIds::ABORTED:
            return TIssuesIds::KIKIMR_OPERATION_ABORTED;
        case Ydb::StatusIds::UNAVAILABLE:
            return TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE;
        case Ydb::StatusIds::OVERLOADED:
            return TIssuesIds::KIKIMR_OVERLOADED;
        case Ydb::StatusIds::SCHEME_ERROR:
            return TIssuesIds::KIKIMR_SCHEME_ERROR;
        case Ydb::StatusIds::GENERIC_ERROR:
            return TIssuesIds::DEFAULT_ERROR;
        case Ydb::StatusIds::TIMEOUT:
            return TIssuesIds::KIKIMR_TIMEOUT;
        case Ydb::StatusIds::BAD_SESSION:
            return TIssuesIds::KIKIMR_TOO_MANY_TRANSACTIONS;
        case Ydb::StatusIds::PRECONDITION_FAILED:
            return TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION;
        case Ydb::StatusIds::CANCELLED:
            return TIssuesIds::KIKIMR_OPERATION_CANCELLED;
        case Ydb::StatusIds::UNSUPPORTED:
            return TIssuesIds::KIKIMR_UNSUPPORTED;
        default:
            return TIssuesIds::DEFAULT_ERROR;
    }
}

void SetColumnType(Ydb::Type& protoType, const TString& typeName, bool notNull) {
    auto* typeDesc = NKikimr::NPg::TypeDescFromPgTypeName(typeName);
    if (typeDesc) {
        Y_ABORT_UNLESS(!notNull, "It is not allowed to create NOT NULL pg columns");
        auto* pg = protoType.mutable_pg_type();
        pg->set_type_name(NKikimr::NPg::PgTypeNameFromTypeDesc(typeDesc));
        pg->set_type_modifier(NKikimr::NPg::TypeModFromPgTypeName(typeName));
        pg->set_oid(NKikimr::NPg::PgTypeIdFromTypeDesc(typeDesc));
        pg->set_typlen(0);
        pg->set_typmod(0);
        return;
    }

    NUdf::EDataSlot dataSlot = NUdf::GetDataSlot(typeName);
    if (dataSlot == NUdf::EDataSlot::Decimal) {
        auto decimal = notNull ? protoType.mutable_decimal_type() :
            protoType.mutable_optional_type()->mutable_item()->mutable_decimal_type();
        // We have no params right now
        // TODO: Fix decimal params support for kikimr
        decimal->set_precision(22);
        decimal->set_scale(9);
    } else {
        auto& primitive = notNull ? protoType : *protoType.mutable_optional_type()->mutable_item();
        auto id = NUdf::GetDataTypeInfo(dataSlot).TypeId;
        primitive.set_type_id(static_cast<Ydb::Type::PrimitiveTypeId>(id));
    }
}

bool ConvertReadReplicasSettingsToProto(const TString settings, Ydb::Table::ReadReplicasSettings& proto,
    Ydb::StatusIds::StatusCode& code, TString& error)
{
    TVector<TString> azs = StringSplitter(to_lower(settings)).Split(',').SkipEmpty();
    bool wrongFormat = false;
    if (azs) {
        for (const auto& az : azs) {
            TVector<TString> azSettings = StringSplitter(az).Split(':').SkipEmpty();
            if (azSettings.size() != 2) {
                wrongFormat = true;
                break;
            }
            TVector<TString> valueString = StringSplitter(azSettings[1]).Split(' ').SkipEmpty();
            TVector<TString> nameString = StringSplitter(azSettings[0]).Split(' ').SkipEmpty();
            ui64 value;
            if (valueString.size() != 1 || !TryFromString<ui64>(valueString[0], value) || nameString.size() != 1) {
                wrongFormat = true;
                break;
            }
            if ("per_az" == nameString[0]) {
                if (azs.size() != 1) {
                    wrongFormat = true;
                    break;
                }
                proto.set_per_az_read_replicas_count(value);
            } else if ("any_az" == nameString[0]) {
                if (azs.size() != 1) {
                    wrongFormat = true;
                    break;
                }
                proto.set_any_az_read_replicas_count(value);
            } else {
                code = Ydb::StatusIds::UNSUPPORTED;
                error = "Specifying read replicas count for each AZ in cluster is not supported yet";
                return false;
                //auto& clusterReplicasSettings = *proto.mutable_cluster_replicas_settings();
                //auto& azDesc = *clusterReplicasSettings.add_az_read_replicas_settings();
                //azDesc.set_name(nameString[0]);
                //azDesc.set_read_replicas_count(value);
            }
        }
    } else {
        wrongFormat = true;
    }
    if (wrongFormat) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = TStringBuilder() << "Wrong format for read replicas settings '" << settings
            << "'. It should be one of: "
            << "1) 'PER_AZ:<read_replicas_count>' to set equal read replicas count for every AZ; "
            << "2) 'ANY_AZ:<read_replicas_count>' to set total read replicas count between all AZs; "
            << "3) '<az1_name>:<read_replicas_count1>, <az2_name>:<read_replicas_count2>, ...' "
            << "to specify read replicas count for each AZ in cluster.";
        return false;
    }
    return true;
}

void ConvertTtlSettingsToProto(const NYql::TTtlSettings& settings, Ydb::Table::TtlSettings& proto) {
    if (!settings.ColumnUnit) {
        auto& opts = *proto.mutable_date_type_column();
        opts.set_column_name(settings.ColumnName);
        opts.set_expire_after_seconds(settings.ExpireAfter.Seconds());
    } else {
        auto& opts = *proto.mutable_value_since_unix_epoch();
        opts.set_column_name(settings.ColumnName);
        opts.set_column_unit(static_cast<Ydb::Table::ValueSinceUnixEpochModeSettings::Unit>(*settings.ColumnUnit));
        opts.set_expire_after_seconds(settings.ExpireAfter.Seconds());
    }
}

Ydb::FeatureFlag::Status GetFlagValue(const TMaybe<bool>& value) {
    if (!value) {
        return Ydb::FeatureFlag::STATUS_UNSPECIFIED;
    }

    return *value
        ? Ydb::FeatureFlag::ENABLED
        : Ydb::FeatureFlag::DISABLED;
}

ETableType GetTableTypeFromString(const TStringBuf& tableType) {
    if (tableType == "table") {
        return ETableType::Table;
    }
    if (tableType == "tableStore") {
        return ETableType::TableStore;
    }
    if (tableType == "externalTable") {
        return ETableType::ExternalTable;
    }
    return ETableType::Unknown;
}


template<typename TEnumType>
static std::shared_ptr<THashMap<TString, TEnumType>> MakeEnumMapping(
        const google::protobuf::EnumDescriptor* descriptor, const TString& prefix
) {
    auto result = std::make_shared<THashMap<TString, TEnumType>>();
    for (auto i = 0; i < descriptor->value_count(); i++) {
        TString name = to_lower(descriptor->value(i)->name());
        TStringBuf nameBuf(name);
        if (!prefix.empty()) {
            nameBuf.SkipPrefix(prefix);
            result->insert(std::make_pair(
                    TString(nameBuf),
                    static_cast<TEnumType>(descriptor->value(i)->number())
            ));
        }
        result->insert(std::make_pair(
                name, static_cast<TEnumType>(descriptor->value(i)->number())
        ));
    }
    return result;
}

static std::shared_ptr<THashMap<TString, Ydb::Topic::Codec>> GetCodecsMapping() {
    static std::shared_ptr<THashMap<TString, Ydb::Topic::Codec>> codecsMapping;
    if (codecsMapping == nullptr) {
        codecsMapping = MakeEnumMapping<Ydb::Topic::Codec>(Ydb::Topic::Codec_descriptor(), "codec_");
    }
    return codecsMapping;
}

static std::shared_ptr<THashMap<TString, Ydb::Topic::AutoPartitioningStrategy>> GetAutoPartitioningStrategiesMapping() {
    static std::shared_ptr<THashMap<TString, Ydb::Topic::AutoPartitioningStrategy>> strategiesMapping;
    if (strategiesMapping == nullptr) {
        strategiesMapping = MakeEnumMapping<Ydb::Topic::AutoPartitioningStrategy>(Ydb::Topic::AutoPartitioningStrategy_descriptor(), "auto_partitioning_strategy_");
    }
    return strategiesMapping;
}

static std::shared_ptr<THashMap<TString, Ydb::Topic::MeteringMode>> GetMeteringModesMapping() {
    static std::shared_ptr<THashMap<TString, Ydb::Topic::MeteringMode>> metModesMapping;
    if (metModesMapping == nullptr) {
        metModesMapping = MakeEnumMapping<Ydb::Topic::MeteringMode>(
                Ydb::Topic::MeteringMode_descriptor(), "metering_mode_"
        );
    }
    return metModesMapping;
}

bool GetTopicMeteringModeFromString(const TString& meteringMode, Ydb::Topic::MeteringMode& result) {
    auto mapping = GetMeteringModesMapping();
    auto normMode = to_lower(meteringMode);
    auto iter = mapping->find(normMode);
    if (iter.IsEnd()) {
        return false;
    } else {
        result = iter->second;
        return true;
    }
}

bool GetTopicAutoPartitioningStrategyFromString(const TString& strategy, Ydb::Topic::AutoPartitioningStrategy& result) {
    auto mapping = GetAutoPartitioningStrategiesMapping();
    auto normStrategy = to_lower(strategy);
    auto iter = mapping->find(normStrategy);
    if (iter.IsEnd()) {
        return false;
    } else {
        result = iter->second;
        return true;
    }
}

TVector<Ydb::Topic::Codec> GetTopicCodecsFromString(const TStringBuf& codecsStr) {
    const TVector<TString> codecsList = StringSplitter(codecsStr).Split(',').SkipEmpty();
    TVector<Ydb::Topic::Codec> result;
    auto mapping = GetCodecsMapping();
    for (const auto& codec : codecsList) {
        auto normCodec = to_lower(Strip(codec));
        auto iter = mapping->find(normCodec);
        if (iter.IsEnd()) {
            return {};
        } else {
            result.push_back(iter->second);
        }
    }
    return result;
}

} // namespace NYql
