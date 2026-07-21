#include <ydb/core/external_sources/external_data_source.h>
#include <ydb/core/external_sources/external_source_builder.h>
#include <ydb/core/external_sources/external_source_factory.h>
#include <ydb/core/external_sources/object_storage.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/yql/providers/s3/path_generator/yql_s3_path_generator.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <library/cpp/scheme/scheme.h>

#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <ydb/public/api/protos/draft/fq.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <array>
#include <string_view>

namespace {

constexpr size_t MaxInputSize = 64 * 1024;
constexpr size_t MaxString = 96;
constexpr size_t MaxColumns = 6;
constexpr size_t MaxProperties = 8;
constexpr size_t MaxPathRules = 24;

template <size_t N>
TString Pick(FuzzedDataProvider& fdp, const std::array<const char*, N>& values) {
    return values[fdp.ConsumeIntegralInRange<size_t>(0, N - 1)];
}

TString ConsumeToken(FuzzedDataProvider& fdp, size_t maxLen = MaxString) {
    static constexpr std::string_view Alphabet =
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789"
        "_-.${}/:*?[]{} ,";

    const size_t len = fdp.ConsumeIntegralInRange<size_t>(0, maxLen);
    TString result;
    result.reserve(len);
    for (size_t i = 0; i < len; ++i) {
        result.push_back(Alphabet[fdp.ConsumeIntegralInRange<size_t>(0, Alphabet.size() - 1)]);
    }
    return result;
}

TString ConsumeChoiceOrToken(FuzzedDataProvider& fdp, const std::array<const char*, 8>& values, size_t maxLen = MaxString) {
    if (fdp.ConsumeBool()) {
        return Pick(fdp, values);
    }
    return ConsumeToken(fdp, maxLen);
}

TString ConsumeLocation(FuzzedDataProvider& fdp) {
    static constexpr std::array<const char*, 8> Locations = {
        "https://allowed.local:443/path",
        "https://localhost/second/uri",
        "localhost/second/uri",
        "regex:1234",
        "scheme://anotherhost:1234/uri",
        "anotherhost:1234/uri",
        "s3://bucket/prefix/",
        "*"
    };
    return ConsumeChoiceOrToken(fdp, Locations, MaxString);
}

Ydb::Type MakeType(Ydb::Type::PrimitiveTypeId typeId, bool optional = false, bool doubleOptional = false) {
    Ydb::Type type;
    if (doubleOptional) {
        type.mutable_optional_type()->mutable_item()->mutable_optional_type()->mutable_item()->set_type_id(typeId);
    } else if (optional) {
        type.mutable_optional_type()->mutable_item()->set_type_id(typeId);
    } else {
        type.set_type_id(typeId);
    }
    return type;
}

Ydb::Type::PrimitiveTypeId ConsumeTypeId(FuzzedDataProvider& fdp) {
    static constexpr std::array<Ydb::Type::PrimitiveTypeId, 15> TypeIds = {
        Ydb::Type::STRING,
        Ydb::Type::UTF8,
        Ydb::Type::JSON,
        Ydb::Type::YSON,
        Ydb::Type::INT32,
        Ydb::Type::UINT32,
        Ydb::Type::INT64,
        Ydb::Type::UINT64,
        Ydb::Type::DATE,
        Ydb::Type::DATETIME,
        Ydb::Type::TIMESTAMP,
        Ydb::Type::INTERVAL,
        Ydb::Type::DATE32,
        Ydb::Type::TIMESTAMP64,
        Ydb::Type::BOOL
    };
    return TypeIds[fdp.ConsumeIntegralInRange<size_t>(0, TypeIds.size() - 1)];
}

template <typename TSchema>
void AddColumn(TSchema& schema, const TString& name, const Ydb::Type& type) {
    auto* column = schema.add_column();
    column->set_name(name);
    *column->mutable_type() = type;
}

template <typename TSchema>
void BuildSchema(TSchema& schema, FuzzedDataProvider& fdp) {
    AddColumn(schema, "city", MakeType(Ydb::Type::STRING));
    AddColumn(schema, "code", MakeType(fdp.ConsumeBool() ? Ydb::Type::INT64 : Ydb::Type::UINT32));
    AddColumn(schema, "dt", MakeType(fdp.ConsumeBool() ? Ydb::Type::DATE : Ydb::Type::DATETIME));
    AddColumn(schema, "payload", MakeType(fdp.ConsumeBool() ? Ydb::Type::STRING : Ydb::Type::UTF8, fdp.ConsumeBool()));

    for (size_t i = 0, count = fdp.ConsumeIntegralInRange<size_t>(0, MaxColumns - 4); i < count; ++i) {
        const TString name = fdp.ConsumeBool() ? TStringBuilder{} << "c" << i : ConsumeToken(fdp, 16);
        AddColumn(schema, name, MakeType(ConsumeTypeId(fdp), fdp.ConsumeBool(), fdp.ConsumeIntegralInRange<int>(0, 31) == 0));
    }
}

void AddPartitionedBy(FederatedQuery::ObjectStorageBinding::Subset& subset, FuzzedDataProvider& fdp) {
    static constexpr std::array<const char*, 5> Columns = {"city", "code", "dt", "payload", "missing"};
    for (size_t i = 0, count = fdp.ConsumeIntegralInRange<size_t>(0, 3); i < count; ++i) {
        *subset.add_partitioned_by() = Pick(fdp, Columns);
    }
    if (subset.partitioned_by().empty() && fdp.ConsumeBool()) {
        *subset.add_partitioned_by() = "city";
    }
}

void AddProjection(google::protobuf::Map<TProtoStringType, TProtoStringType>& projection, FuzzedDataProvider& fdp) {
    static constexpr std::array<const char*, 8> Templates = {
        "/${city}/",
        "/${city}/${code}/",
        "/${city}/${dt}/",
        "/${city}/${code}/${dt}/",
        "city=${city}/code=${code}/",
        "/${missing}/",
        "/${city}/${payload}/",
        ""
    };

    projection["projection.enabled"] = fdp.ConsumeBool() ? "true" : "false";
    projection["storage.location.template"] = Pick(fdp, Templates);

    if (fdp.ConsumeBool()) {
        projection["projection.city.type"] = "enum";
        projection["projection.city.values"] = fdp.ConsumeBool() ? "MSK,SPB" : ConsumeToken(fdp, 32);
    }

    if (fdp.ConsumeBool()) {
        projection["projection.code.type"] = "integer";
        projection["projection.code.min"] = ToString(fdp.ConsumeIntegralInRange<int>(-2, 2));
        projection["projection.code.max"] = ToString(fdp.ConsumeIntegralInRange<int>(0, 4));
        projection["projection.code.interval"] = ToString(fdp.ConsumeIntegralInRange<int>(0, 3));
        if (fdp.ConsumeBool()) {
            projection["projection.code.digits"] = ToString(fdp.ConsumeIntegralInRange<int>(0, 8));
        }
    }

    if (fdp.ConsumeBool()) {
        projection["projection.dt.type"] = "date";
        projection["projection.dt.min"] = fdp.ConsumeBool() ? "2010-01-01" : "NOW - 1 DAYS";
        projection["projection.dt.max"] = fdp.ConsumeBool() ? "2010-01-02" : "NOW";
        projection["projection.dt.format"] = fdp.ConsumeBool() ? "%F" : "%Y/%m/%d";
        projection["projection.dt.interval"] = ToString(fdp.ConsumeIntegralInRange<int>(0, 2));
        projection["projection.dt.unit"] = fdp.ConsumeBool() ? "DAYS" : "MONTHS";
    }

    if (fdp.ConsumeIntegralInRange<int>(0, 7) == 0) {
        projection[ConsumeToken(fdp, 24)] = ConsumeToken(fdp, 24);
    }
}

TString BuildProjectionJson(const FederatedQuery::ObjectStorageBinding::Subset& subset) {
    NSc::TValue projection;
    for (const auto& [key, value] : subset.projection()) {
        projection[key] = value;
    }
    return projection.DictEmpty() ? TString() : projection.ToJsonPretty();
}

void ExercisePathGenerator(const TString& projectionJson, const TVector<TString>& partitionedBy, FuzzedDataProvider& fdp) {
    TMap<TString, NYql::NUdf::EDataSlot> columns;
    columns["city"] = NYql::NUdf::EDataSlot::String;
    columns["code"] = fdp.ConsumeBool() ? NYql::NUdf::EDataSlot::Uint32 : NYql::NUdf::EDataSlot::Int64;
    columns["dt"] = fdp.ConsumeBool() ? NYql::NUdf::EDataSlot::Date : NYql::NUdf::EDataSlot::Datetime;

    const std::vector<TString> partitionColumns(partitionedBy.begin(), partitionedBy.end());
    auto generator = NYql::NPathGenerator::CreatePathGenerator(
        projectionJson,
        partitionColumns,
        columns,
        fdp.ConsumeIntegralInRange<size_t>(1, MaxPathRules));

    const auto& config = generator->GetConfig();
    (void)generator->GetRules().size();
    for (const auto& rule : config.Rules) {
        switch (rule.Type) {
        case NYql::NPathGenerator::IPathGenerator::EType::ENUM:
            if (!rule.Values.empty()) {
                const TString formatted = generator->Format(rule.Name, rule.Values.front());
                (void)generator->Parse(rule.Name, formatted);
            }
            break;
        case NYql::NPathGenerator::IPathGenerator::EType::INTEGER: {
            const TString value = ToString(rule.Min);
            const TString formatted = generator->Format(rule.Name, value);
            (void)generator->Parse(rule.Name, formatted);
            break;
        }
        case NYql::NPathGenerator::IPathGenerator::EType::DATE:
            if (rule.From.StartsWith("201")) {
                const TString formatted = generator->Format(rule.Name, rule.From);
                (void)generator->Parse(rule.Name, formatted);
            }
            break;
        case NYql::NPathGenerator::IPathGenerator::EType::UNDEFINED:
            break;
        }
    }
}

void ExerciseObjectStorageProtoValidation(FuzzedDataProvider& fdp) {
    FederatedQuery::Schema schema;
    FederatedQuery::ObjectStorageBinding::Subset subset;
    BuildSchema(schema, fdp);
    subset.set_path_pattern(ConsumeLocation(fdp));

    static constexpr std::array<const char*, 8> Formats = {
        "", "raw", "json_list", "csv", "csv_with_names", "parquet", "json_each_row", "unknown"
    };
    subset.set_format(Pick(fdp, Formats));
    subset.set_compression(fdp.ConsumeBool() ? "gzip" : ConsumeToken(fdp, 12));

    auto& settings = *subset.mutable_format_setting();
    if (fdp.ConsumeBool()) {
        settings["file_pattern"] = fdp.ConsumeBool() ? "*" : ConsumeToken(fdp, 24);
    }
    if (fdp.ConsumeBool()) {
        settings["data.interval.unit"] = fdp.ConsumeBool() ? "DAYS" : ConsumeToken(fdp, 16);
    }
    if (fdp.ConsumeBool()) {
        settings["csv_delimiter"] = fdp.ConsumeBool() ? "," : ConsumeToken(fdp, 4);
    }
    if (fdp.ConsumeBool()) {
        settings["data.datetime.format_name"] = fdp.ConsumeBool() ? "ISO" : ConsumeToken(fdp, 16);
        if (fdp.ConsumeBool()) {
            settings["data.datetime.format"] = "%Y-%m-%d %H:%M:%S";
        }
    }
    if (fdp.ConsumeIntegralInRange<int>(0, 7) == 0) {
        settings[ConsumeToken(fdp, 24)] = ConsumeToken(fdp, 24);
    }

    AddPartitionedBy(subset, fdp);
    if (fdp.ConsumeBool()) {
        AddProjection(*subset.mutable_projection(), fdp);
    }

    const TString location = subset.path_pattern().empty() ? TString("s3://bucket/prefix/") : subset.path_pattern();
    (void)NKikimr::NExternalSource::Validate(schema, subset, fdp.ConsumeIntegralInRange<size_t>(1, MaxPathRules), location);
    (void)NKikimr::NExternalSource::ValidateDateFormatSetting(subset.format_setting(), fdp.ConsumeBool());
    (void)NKikimr::NExternalSource::ValidateRawFormat(subset.format(), schema, subset.partitioned_by());

    const TString projectionJson = BuildProjectionJson(subset);
    TVector<TString> partitionedBy(subset.partitioned_by().begin(), subset.partitioned_by().end());
    if (!partitionedBy.empty()) {
        ExercisePathGenerator(projectionJson, partitionedBy, fdp);
    }
}

void ExerciseObjectStorageSource(FuzzedDataProvider& fdp) {
    const std::vector<TRegExMatch> hostPatterns = {"allowed.*", "localhost", "(test|regex)"};
    const auto source = NKikimr::NExternalSource::CreateObjectStorageExternalSource(
        hostPatterns,
        nullptr,
        fdp.ConsumeIntegralInRange<size_t>(1, MaxPathRules),
        nullptr,
        fdp.ConsumeBool(),
        false);

    NKikimrExternalSources::TSchema schema;
    NKikimrExternalSources::TGeneral general;
    BuildSchema(schema, fdp);
    general.set_location(ConsumeLocation(fdp));

    static constexpr std::array<const char*, 8> Formats = {
        "", "raw", "json_list", "csv", "csv_with_names", "parquet", "json_each_row", "unknown"
    };
    auto& attrs = *general.mutable_attributes();
    attrs["format"] = Pick(fdp, Formats);
    if (fdp.ConsumeBool()) {
        attrs["compression"] = fdp.ConsumeBool() ? "gzip" : ConsumeToken(fdp, 12);
    }
    if (fdp.ConsumeBool()) {
        attrs["file_pattern"] = fdp.ConsumeBool() ? "*" : ConsumeToken(fdp, 24);
    }
    if (fdp.ConsumeBool()) {
        attrs["csv_delimiter"] = fdp.ConsumeBool() ? "," : ConsumeToken(fdp, 4);
    }
    if (fdp.ConsumeBool()) {
        attrs["data.interval.unit"] = fdp.ConsumeBool() ? "DAYS" : ConsumeToken(fdp, 16);
    }

    if (fdp.ConsumeBool()) {
        attrs["partitioned_by"] = fdp.ConsumeBool() ? R"(["city","code"])" : ConsumeToken(fdp, 48);
        google::protobuf::Map<TProtoStringType, TProtoStringType> projection;
        AddProjection(projection, fdp);
        for (const auto& [key, value] : projection) {
            attrs[key] = value;
        }
    }
    if (fdp.ConsumeIntegralInRange<int>(0, 11) == 0) {
        attrs[ConsumeToken(fdp, 16)] = ConsumeToken(fdp, 16);
    }

    const TString content = source->Pack(schema, general);
    (void)source->GetParameters(content);
    (void)source->GetName();
    (void)source->GetAuthMethods();
    (void)source->HasExternalTable();
    (void)source->CanLoadDynamicMetadata();

    NKikimrSchemeOp::TExternalDataSourceDescription proto;
    proto.SetLocation(ConsumeLocation(fdp));
    if (fdp.ConsumeBool()) {
        proto.MutableProperties()->MutableProperties()->insert({"unsupported", "value"});
    }
    source->ValidateExternalDataSource(proto.SerializeAsString());
}

void ExerciseMetadataEarlyReturns(FuzzedDataProvider& fdp) {
    const auto objectStorage = NKikimr::NExternalSource::CreateObjectStorageExternalSource(
        {},
        nullptr,
        MaxPathRules,
        nullptr,
        true,
        false);

    auto meta = std::make_shared<NKikimr::NExternalSource::TMetadata>();
    meta->TableLocation = ConsumeLocation(fdp);
    meta->DataSourceLocation = "s3://bucket";
    meta->DataSourcePath = "/prefix";
    meta->Type = "ObjectStorage";
    BuildSchema(meta->Schema, fdp);

    switch (fdp.ConsumeIntegralInRange<int>(0, 2)) {
    case 0:
        break;
    case 1:
        meta->Attributes["format"] = "csv_with_names";
        break;
    case 2:
        meta->Attributes["format"] = "unsupported";
        meta->Attributes["withinfer"] = "1";
        break;
    }

    (void)objectStorage->LoadDynamicMetadata(meta).GetValueSync();
}

void ExerciseExternalDataSource(FuzzedDataProvider& fdp) {
    const std::vector<TRegExMatch> hostPatterns = {"allowed.*", "localhost", "(test|regex)"};
    const auto source = NKikimr::NExternalSource::CreateExternalDataSource(
        "Test",
        {"auth1", "auth2"},
        {"property", "database_name", "protocol", "use_tls", "service_name", "shared_reading"},
        hostPatterns);

    NKikimrSchemeOp::TExternalDataSourceDescription proto;
    static constexpr std::array<const char*, 8> SourceTypes = {
        "PostgreSQL", "MySQL", "ClickHouse", "MongoDB", "Oracle", "YdbTopics", "Other", ""
    };
    proto.SetSourceType(Pick(fdp, SourceTypes));
    proto.SetLocation(ConsumeLocation(fdp));

    auto& props = *proto.MutableProperties()->MutableProperties();
    for (size_t i = 0, count = fdp.ConsumeIntegralInRange<size_t>(0, MaxProperties); i < count; ++i) {
        static constexpr std::array<const char*, 8> Keys = {
            "property", "database_name", "protocol", "use_tls", "service_name", "shared_reading", "schema", "unsupported"
        };
        props[Pick(fdp, Keys)] = ConsumeToken(fdp, 32);
    }

    (void)source->GetName();
    (void)source->GetAuthMethods();
    (void)source->HasExternalTable();
    source->ValidateExternalDataSource(proto.SerializeAsString());
}

void ExerciseBuilderSource(FuzzedDataProvider& fdp) {
    const std::vector<TRegExMatch> hostPatterns = {"allowed.*", "localhost", "(test|regex)"};
    auto source = NKikimr::NExternalSource::TExternalSourceBuilder("FuzzBuilder")
        .Auth({"NONE"})
        .Auth({"BASIC", "TOKEN"}, NKikimr::NExternalSource::GetHasSettingCondition("mode", "s3"))
        .Property("mode", NKikimr::NExternalSource::GetIsInListValidator({"s3", "hadoop", "none"}, false))
        .Property("required", NKikimr::NExternalSource::GetRequiredValidator(), NKikimr::NExternalSource::GetHasSettingCondition("mode", "s3"))
        .Property("optional")
        .HostnamePatterns(hostPatterns)
        .Build();

    NKikimrSchemeOp::TExternalDataSourceDescription proto;
    proto.SetLocation(ConsumeLocation(fdp));
    auto& props = *proto.MutableProperties()->MutableProperties();
    static constexpr std::array<const char*, 8> Modes = {"s3", "hadoop", "none", "bad", "", "S3", "s3 ", "local"};
    if (fdp.ConsumeBool()) {
        props["mode"] = Pick(fdp, Modes);
    }
    if (fdp.ConsumeBool()) {
        props["required"] = ConsumeToken(fdp, 32);
    }
    if (fdp.ConsumeBool()) {
        props["optional"] = ConsumeToken(fdp, 32);
    }
    if (fdp.ConsumeIntegralInRange<int>(0, 5) == 0) {
        props[ConsumeToken(fdp, 16)] = ConsumeToken(fdp, 16);
    }

    (void)source->GetName();
    (void)source->GetAuthMethods();
    (void)source->HasExternalTable();
    source->ValidateExternalDataSource(proto.SerializeAsString());
    (void)source->LoadDynamicMetadata(std::make_shared<NKikimr::NExternalSource::TMetadata>()).GetValueSync();
}

void ExerciseIcebergFactorySource(FuzzedDataProvider& fdp) {
    const TString type = ToString(NYql::EDatabaseType::Iceberg);
    const auto factory = NKikimr::NExternalSource::CreateExternalSourceFactory(
        {"allowed.*", "localhost", "(test|regex)"},
        nullptr,
        MaxPathRules,
        nullptr,
        false,
        false,
        false,
        {type});

    (void)factory->IsAvailableProvider(TString(NYql::GenericProviderName));
    const auto source = factory->GetOrCreate(type);

    NKikimrSchemeOp::TExternalDataSourceDescription proto;
    proto.SetLocation(ConsumeLocation(fdp));
    auto& props = *proto.MutableProperties()->MutableProperties();
    props["warehouse_type"] = fdp.ConsumeBool() ? "s3" : ConsumeToken(fdp, 16);
    props["database_name"] = fdp.ConsumeBool() ? "db" : ConsumeToken(fdp, 16);
    props["warehouse_s3_endpoint"] = fdp.ConsumeBool() ? "endpoint" : ConsumeToken(fdp, 24);
    props["warehouse_s3_uri"] = fdp.ConsumeBool() ? "s3://bucket/warehouse" : ConsumeToken(fdp, 32);
    props["warehouse_s3_region"] = fdp.ConsumeBool() ? "region" : ConsumeToken(fdp, 16);
    props["catalog_type"] = fdp.ConsumeBool() ? "hive_metastore" : "hadoop";
    if (fdp.ConsumeBool()) {
        props["catalog_hive_metastore_uri"] = fdp.ConsumeBool() ? "thrift://localhost:9083" : ConsumeToken(fdp, 32);
    }
    if (fdp.ConsumeIntegralInRange<int>(0, 9) == 0) {
        props[ConsumeToken(fdp, 20)] = ConsumeToken(fdp, 20);
    }

    (void)source->GetAuthMethods();
    source->ValidateExternalDataSource(proto.SerializeAsString());
}

void ExerciseParsedProtos(FuzzedDataProvider& fdp, size_t originalSize) {
    FederatedQuery::Schema schema;
    FederatedQuery::ObjectStorageBinding::Subset subset;

    const bool schemaParsed = schema.ParseFromString(fdp.ConsumeRandomLengthString(Min<size_t>(originalSize, 2048)));
    const bool subsetParsed = subset.ParseFromString(fdp.ConsumeRandomLengthString(Min<size_t>(originalSize, 2048)));
    (void)schemaParsed;
    (void)subsetParsed;

    TString location = fdp.ConsumeRandomLengthString(256);
    if (location.empty()) {
        location = "s3://bucket/prefix/";
    }
    if (subset.path_pattern().empty()) {
        subset.set_path_pattern(location);
    }

    (void)NKikimr::NExternalSource::Validate(
        schema,
        subset,
        fdp.ConsumeIntegralInRange<size_t>(1, MaxPathRules),
        location);

    const TString projectionJson = BuildProjectionJson(subset);
    TVector<TString> partitionedBy(subset.partitioned_by().begin(), subset.partitioned_by().end());
    if (!partitionedBy.empty()) {
        ExercisePathGenerator(projectionJson, partitionedBy, fdp);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > MaxInputSize) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);

    try {
        ExerciseObjectStorageProtoValidation(fdp);
    } catch (...) {
    }

    try {
        ExerciseObjectStorageSource(fdp);
    } catch (...) {
    }

    try {
        ExerciseMetadataEarlyReturns(fdp);
    } catch (...) {
    }

    try {
        ExerciseExternalDataSource(fdp);
    } catch (...) {
    }

    try {
        ExerciseBuilderSource(fdp);
    } catch (...) {
    }

    try {
        ExerciseIcebergFactorySource(fdp);
    } catch (...) {
    }

    try {
        ExerciseParsedProtos(fdp, size);
    } catch (...) {
    }

    return 0;
}
