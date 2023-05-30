#include "table_bindings_from_bindings.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>

#include <library/cpp/scheme/scheme.h>

#include <util/generic/vector.h>

namespace NFq {

using namespace NYql;

namespace {

void FillBinding(NSQLTranslation::TTranslationSettings& sqlSettings, const FederatedQuery::Binding& binding, const THashMap<TString, FederatedQuery::Connection>& connections) {
    TString clusterType;
    TString path;
    TString format;
    TString compression;
    TString schema;
    THashMap<TString, TString> formatSettings;
    NSc::TValue projection;
    NSc::TValue partitionedBy;
    switch (binding.content().setting().binding_case()) {
    case FederatedQuery::BindingSetting::kDataStreams: {
        clusterType = PqProviderName;
        auto yds = binding.content().setting().data_streams();
        path = yds.stream_name();
        format = yds.format();
        compression = yds.compression();
        schema = FormatSchema(yds.schema());
        formatSettings = {yds.format_setting().begin(), yds.format_setting().end()};
        break;
    }
    case FederatedQuery::BindingSetting::kObjectStorage: {
        clusterType = S3ProviderName;
        const auto s3 = binding.content().setting().object_storage();
        if (s3.subset().empty()) {
            throw yexception() << "No subsets in Object Storage binding " << binding.meta().id();
        }

        const auto& s = s3.subset(0);
        path = s.path_pattern();
        format = s.format();
        compression = s.compression();
        schema = FormatSchema(s.schema());
        formatSettings = {s.format_setting().begin(), s.format_setting().end()};
        for (const auto& [key, value]: s.projection()) {
            projection[key] = value;
        }
        partitionedBy.AppendAll(s.partitioned_by());
        break;
    }

    case FederatedQuery::BindingSetting::BINDING_NOT_SET: {
        throw yexception() << "BINDING_NOT_SET case for binding " << binding.meta().id() << ", name " << binding.content().name();
    }
    // Do not add default. Adding a new binding should cause a compilation error
    }

    auto connectionPtr = connections.FindPtr(binding.content().connection_id());
    if (!connectionPtr) {
        throw yexception() << "Unable to resolve connection for binding " << binding.meta().id() << ", name " << binding.content().name() << ", connection id " << binding.content().connection_id();
    }

    NSQLTranslation::TTableBindingSettings bindSettings;
    bindSettings.ClusterType = clusterType;
    bindSettings.Settings = formatSettings;
    bindSettings.Settings["cluster"] = connectionPtr->content().name();
    bindSettings.Settings["path"] = path;
    bindSettings.Settings["format"] = format;
    // todo: fill format parameters
    if (compression) {
        bindSettings.Settings["compression"] = compression;
    }
    bindSettings.Settings["schema"] = schema;

    if (!projection.DictEmpty()) {
        bindSettings.Settings["projection"] = projection.ToJsonPretty();
    }

    if (!partitionedBy.ArrayEmpty()) {
        bindSettings.Settings["partitioned_by"] = partitionedBy.ToJsonPretty();
    }

    sqlSettings.Bindings[binding.content().name()] = std::move(bindSettings);
}

} //namespace

void AddTableBindingsFromBindings(const TVector<FederatedQuery::Binding>& bindings, const THashMap<TString, FederatedQuery::Connection>& connections, NSQLTranslation::TTranslationSettings& sqlSettings) {
    for (const auto& binding : bindings) {
        FillBinding(sqlSettings, binding, connections);
    }
}

} //NFq
