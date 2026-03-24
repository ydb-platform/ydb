#include "yql_kikimr_gateway.h"

#include <util/string/cast.h>

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_string_util.h>
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

void TReplicationSettings::TIamCredentials::Serialize(NKikimrReplication::TIamCredentials& proto) const {
    InitialToken.Serialize(*proto.MutableInitialToken());
    if (ServiceAccountId) {
        proto.SetServiceAccountId(ServiceAccountId);
    }
    if (ResourceId) {
        proto.SetResourceId(ResourceId);
    }
}

void TReplicationSettings::TGlobalConsistency::Serialize(NKikimrReplication::TConsistencySettings_TGlobalConsistency& proto) const {
    if (CommitInterval) {
        proto.SetCommitIntervalMilliSeconds(CommitInterval.MilliSeconds());
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


bool TTtlSettings::TryParse(const NNodes::TCoNameValueTupleList& node, TTtlSettings& settings, TString& error) {
    using namespace NNodes;

    for (const auto& field : node) {
        auto name = field.Name().Value();
        if (name == "columnName") {
            YQL_ENSURE(field.Value().Maybe<TCoAtom>());
            settings.ColumnName = field.Value().Cast<TCoAtom>().StringValue();
        } else if (name == "tiers") {
            YQL_ENSURE(field.Value().Maybe<TExprList>());
            auto listNode = field.Value().Cast<TExprList>();

            for (size_t i = 0; i < listNode.Size(); ++i) {
                auto tierNode = listNode.Item(i);

                std::optional<TString> storageName;
                TDuration evictionDelay;
                YQL_ENSURE(tierNode.Maybe<TCoNameValueTupleList>());
                for (const auto& tierField : tierNode.Cast<TCoNameValueTupleList>()) {
                    auto tierFieldName = tierField.Name().Value();
                    if (tierFieldName == "storageName") {
                        YQL_ENSURE(tierField.Value().Maybe<TCoAtom>());
                        storageName = tierField.Value().Cast<TCoAtom>().StringValue();
                    } else if (tierFieldName == "evictionDelay") {
                        YQL_ENSURE(tierField.Value().Maybe<TCoInterval>());
                        auto value = FromString<i64>(tierField.Value().Cast<TCoInterval>().Literal().Value());
                        if (value < 0) {
                            error = "Interval value cannot be negative";
                            return false;
                        }
                        evictionDelay = TDuration::FromValue(value);
                    } else {
                        error = TStringBuilder() << "Unknown field: " << tierFieldName;
                        return false;
                    }
                }

                settings.Tiers.emplace_back(evictionDelay, storageName);
            }
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
        || StoreExternalBlobs || ExternalDataChannelsCount;
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

bool SetColumnType(const TTypeAnnotationNode* typeNode, bool notNull, Ydb::Type& protoType, TString& error) {
    switch (typeNode->GetKind()) {
    case ETypeAnnotationKind::Pg: {
        const auto* pgTypeNode = typeNode->Cast<TPgExprType>();
        const auto& typeId = pgTypeNode->GetId();
        const auto typeDesc = NKikimr::NPg::TypeDescFromPgTypeId(typeId);
        if (typeDesc) {
            Y_ABORT_UNLESS(!notNull, "It is not allowed to create NOT NULL pg columns");
            auto* pg = protoType.mutable_pg_type();
            pg->set_type_name(NKikimr::NPg::PgTypeNameFromTypeDesc(typeDesc));
            pg->set_oid(NKikimr::NPg::PgTypeIdFromTypeDesc(typeDesc));
            pg->set_typlen(0);
            pg->set_typmod(0);
            return true;
        } else {
            error = TStringBuilder() << "Unknown pg type: " << FormatType(pgTypeNode);
            return false;
        }
    }
    case ETypeAnnotationKind::Data: {
        const auto dataTypeNode = typeNode->Cast<TDataExprType>();
        const TStringBuf typeName = dataTypeNode->GetName();
        NUdf::EDataSlot dataSlot = NUdf::GetDataSlot(typeName);
        if (dataSlot == NUdf::EDataSlot::Decimal) {
            auto dataExprTypeNode = typeNode->Cast<TDataExprParamsType>();
            ui32 precision = FromString(dataExprTypeNode->GetParamOne());
            ui32 scale = FromString(dataExprTypeNode->GetParamTwo());
            if (!NKikimr::NScheme::TDecimalType::Validate(precision, scale, error)) {
                return false;
            }
            auto decimal = notNull ? protoType.mutable_decimal_type() :
                protoType.mutable_optional_type()->mutable_item()->mutable_decimal_type();
            decimal->set_precision(precision);
            decimal->set_scale(scale);
        } else {
            auto& primitive = notNull ? protoType : *protoType.mutable_optional_type()->mutable_item();
            auto id = NUdf::GetDataTypeInfo(dataSlot).TypeId;
            primitive.set_type_id(static_cast<Ydb::Type::PrimitiveTypeId>(id));
        }
        return true;
    }
    default: {
        error = TStringBuilder() << "Unexpected node kind: " << typeNode->GetKind();
        return false;
    }
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
//          << "3) '<az1_name>:<read_replicas_count1>, <az2_name>:<read_replicas_count2>, ...' "
            << "to specify read replicas count for each AZ in cluster.";
        return false;
    }
    return true;
}

void ConvertTtlSettingsToProto(const NYql::TTtlSettings& settings, Ydb::Table::TtlSettings& proto) {
    for (const auto& tier : settings.Tiers) {
        auto* outTier = proto.mutable_tiered_ttl()->add_tiers();
        if (!settings.ColumnUnit) {
            auto& expr = *outTier->mutable_date_type_column();
            expr.set_column_name(settings.ColumnName);
            expr.set_expire_after_seconds(tier.ApplyAfter.Seconds());
        } else {
            auto& expr = *outTier->mutable_value_since_unix_epoch();
            expr.set_column_name(settings.ColumnName);
            expr.set_column_unit(static_cast<Ydb::Table::ValueSinceUnixEpochModeSettings::Unit>(*settings.ColumnUnit));
            expr.set_expire_after_seconds(tier.ApplyAfter.Seconds());
        }
        if (tier.StorageName) {
            outTier->mutable_evict_to_external_storage()->set_storage(*tier.StorageName);
        } else {
            outTier->mutable_delete_();
        }
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
static THashMap<TString, TEnumType> MakeEnumMapping(
        const google::protobuf::EnumDescriptor* descriptor, const TString& prefix
) {
    auto result = THashMap<TString, TEnumType>();
    for (auto i = 0; i < descriptor->value_count(); i++) {
        TString name = to_lower(descriptor->value(i)->name());
        TStringBuf nameBuf(name);
        if (!prefix.empty()) {
            nameBuf.SkipPrefix(prefix);
            result.insert(std::make_pair(
                    TString(nameBuf),
                    static_cast<TEnumType>(descriptor->value(i)->number())
            ));
        }
        result.insert(std::make_pair(
                name, static_cast<TEnumType>(descriptor->value(i)->number())
        ));
    }
    return result;
}

static const THashMap<TString, Ydb::Topic::Codec>& GetCodecsMapping() {
    static const THashMap<TString, Ydb::Topic::Codec> codecsMapping = MakeEnumMapping<Ydb::Topic::Codec>(Ydb::Topic::Codec_descriptor(), "codec_");
    return codecsMapping;
}

static const THashMap<TString, Ydb::Topic::AutoPartitioningStrategy>& GetAutoPartitioningStrategiesMapping() {
    static const THashMap<TString, Ydb::Topic::AutoPartitioningStrategy> strategiesMapping = []() {
        auto strategiesMapping = MakeEnumMapping<Ydb::Topic::AutoPartitioningStrategy>(
            Ydb::Topic::AutoPartitioningStrategy_descriptor(), "auto_partitioning_strategy_");

        const TString prefix = "scale_";
        for (const auto& [key, value] : strategiesMapping) {
            if (key.StartsWith(prefix)) {
                TString newKey = key;
                newKey.erase(0, prefix.length());

                Y_ABORT_UNLESS(!strategiesMapping.contains(newKey));
                strategiesMapping[newKey] = value;
            }
        }
        return strategiesMapping;
    }();
    return strategiesMapping;
}

static const THashMap<TString, Ydb::Topic::MeteringMode>& GetMeteringModesMapping() {
    static const THashMap<TString, Ydb::Topic::MeteringMode> metModesMapping = MakeEnumMapping<Ydb::Topic::MeteringMode>(
        Ydb::Topic::MeteringMode_descriptor(), "metering_mode_");

    return metModesMapping;
}

bool GetTopicMeteringModeFromString(const TString& meteringMode, Ydb::Topic::MeteringMode& result) {
    const auto& mapping = GetMeteringModesMapping();
    auto normMode = to_lower(meteringMode);
    auto iter = mapping.find(normMode);
    if (iter.IsEnd()) {
        return false;
    } else {
        result = iter->second;
        return true;
    }
}

bool GetTopicAutoPartitioningStrategyFromString(const TString& strategy, Ydb::Topic::AutoPartitioningStrategy& result) {
    const auto& mapping = GetAutoPartitioningStrategiesMapping();
    auto normStrategy = to_lower(strategy);
    auto iter = mapping.find(normStrategy);
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
    const auto& mapping = GetCodecsMapping();
    for (const auto& codec : codecsList) {
        auto normCodec = to_lower(Strip(codec));
        auto iter = mapping.find(normCodec);
        if (iter.IsEnd()) {
            return {};
        } else {
            result.push_back(iter->second);
        }
    }
    return result;
}

void FillLocalBloomFilterSetting(TIndexDescription::TLocalBloomFilterDescription& desc,
    const TString& name, const TString& value, TString& error) {
    if (name == "false_positive_probability") {
        double fpp = 0.0;
        if (!TryFromString<double>(value, fpp)) {
            error = TStringBuilder() << "Invalid false_positive_probability value: " << value;
            return;
        }

        desc.FalsePositiveProbability = fpp;
        return;
    }

    error = TStringBuilder() << "Unknown index setting: " << name;
    return;
}

void FillLocalBloomNgramFilterSetting(TIndexDescription::TLocalBloomNgramFilterDescription& desc,
    const TString& name, const TString& value, TString& error) {
    if (name == "ngram_size") {
        ui32 uiValue = 0;
        if (!TryFromString<ui32>(value, uiValue)) {
            error = TStringBuilder() << "Invalid ngram_size value: " << value;
            return;
        }

        desc.NgramSize = uiValue;
        return;
    }

    if (name == "hashes_count") {
        ui32 uiValue = 0;
        if (!TryFromString<ui32>(value, uiValue)) {
            error = TStringBuilder() << "Invalid hashes_count value: " << value;
            return;
        }

        desc.HashesCount = uiValue;
        return;
    }

    if (name == "filter_size_bytes") {
        ui32 uiValue = 0;
        if (!TryFromString<ui32>(value, uiValue)) {
            error = TStringBuilder() << "Invalid filter_size_bytes value: " << value;
            return;
        }

        desc.FilterSizeBytes = uiValue;
        return;
    }

    if (name == "records_count") {
        ui32 uiValue = 0;
        if (!TryFromString<ui32>(value, uiValue)) {
            error = TStringBuilder() << "Invalid records_count value: " << value;
            return;
        }

        desc.RecordsCount = uiValue;
        return;
    }

    if (name == "case_sensitive") {
        bool boolValue = true;
        if (!TryFromString<bool>(value, boolValue)) {
            error = TStringBuilder() << "Invalid case_sensitive value: " << value;
            return;
        }

        desc.CaseSensitive = boolValue;
        return;
    }

    error = TStringBuilder() << "Unknown index setting: " << name;
    return;
}

} // namespace NYql
