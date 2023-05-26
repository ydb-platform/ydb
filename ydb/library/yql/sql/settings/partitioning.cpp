#include "partitioning.h"
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <library/cpp/json/json_reader.h>
#include <util/generic/is_in.h>
#include <util/string/builder.h>

namespace NSQLTranslation {
namespace {

TString ParsePartitionedByBinding(const TString& name, const TString& value, TVector<TString>& columns) {
    using namespace NJson;
    TJsonValue json;
    bool throwOnError = false;
    if (!ReadJsonTree(value, &json, throwOnError)) {
        return TStringBuilder() << "Binding setting " << name << " is not a valid JSON";
    }

    const TJsonValue::TArray* arr = nullptr;
    if (!json.GetArrayPointer(&arr)) {
        return TStringBuilder() << "Binding setting " << name << ": expecting array";
    }

    if (arr->empty()) {
        return TStringBuilder() << "Binding setting " << name << ": expecting non-empty array";
    }

    for (auto& item : *arr) {
        TString str;
        if (!item.GetString(&str)) {
            return TStringBuilder() << "Binding setting " << name << ": expecting non-empty array of strings";
        }
        columns.push_back(std::move(str));
    }

    return {};
}

}

TString ExtractBindingInfo(const TTranslationSettings& settings, const TString& binding, TBindingInfo& result) {
    auto pit = settings.Bindings.find(binding);

    if (pit == settings.Bindings.end()) {
        return TStringBuilder() << "Table binding `" << binding << "` is not defined";
    }

    const auto& bindSettings = pit->second;

    if (!IsIn({NYql::S3ProviderName, NYql::PqProviderName}, bindSettings.ClusterType)) {
        return TStringBuilder() << "Cluster type " << bindSettings.ClusterType << " is not supported for table bindings";
    }
    result.ClusterType = bindSettings.ClusterType;

    // ordered map ensures AST stability
    TMap<TString, TString> kvs(bindSettings.Settings.begin(), bindSettings.Settings.end());
    auto pullSettingOrFail = [&](const TString& name, TString& value) -> TString {
        auto it = kvs.find(name);
        if (it == kvs.end()) {
            return TStringBuilder() << name << " is not found for " << binding;
        }
        value = it->second;
        kvs.erase(it);
        return {};
    };

    if (const auto& error = pullSettingOrFail("cluster", result.Cluster)) {
        return error;
    }
    if (const auto& error = pullSettingOrFail("path", result.Path)) {
        return error;
    }

    if (auto it = kvs.find("schema"); it != kvs.end()) {
        result.Schema = it->second;
        kvs.erase(it);
    }

    if (auto it = kvs.find("partitioned_by"); it != kvs.end()) {
        TVector<TString> columns;
        if (const auto& error = ParsePartitionedByBinding(it->first, it->second, columns)) {
            return error;
        }
        result.Attributes.emplace("partitionedby", std::move(columns));
        kvs.erase(it);
    }

    for (auto& [key, value] : kvs) {
        if (key.empty()) {
            return "Attribute should not be empty";
        }
        result.Attributes.emplace(key, TVector<TString>{value});
    }

    return {};
}

}  // namespace NSQLTranslation
