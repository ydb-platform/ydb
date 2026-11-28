#include "config.h"

#include <yaml-cpp/yaml.h>

#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>

#include <algorithm>
#include <limits>
#include <utility>

namespace NYdbWorkload::NScheme {
namespace {

void CheckKeys(const YAML::Node& node, std::initializer_list<TString> allowed, const TString& context) {
    Y_ENSURE(node.IsMap(), context << " must be a mapping");
    THashSet<TString> keys;
    for (const auto& entry : node) {
        Y_ENSURE(entry.first.IsScalar(), context << ": keys must be strings");
        const TString key(entry.first.Scalar());
        Y_ENSURE(keys.insert(key).second, context << ": duplicate key '" << key << "'");
        Y_ENSURE(std::find(allowed.begin(), allowed.end(), key) != allowed.end(),
            context << ": unknown key '" << key << "'");
    }
}

ui32 Integer(const YAML::Node& node, const TString& name, ui32 minimum = 1) {
    Y_ENSURE(node.IsScalar(), name << " must be an integer");
    const TString value(node.Scalar());
    Y_ENSURE(!value.empty() && std::all_of(value.begin(), value.end(), [](char c) { return c >= '0' && c <= '9'; }),
        name << " must be an integer");
    ui32 result = 0;
    Y_ENSURE(TryFromString(value, result) && result >= minimum,
        name << " must be in [" << minimum << ", " << std::numeric_limits<ui32>::max() << "]");
    return result;
}

TGroupConfig ParseGroup(const YAML::Node& node, const TString& name) {
    CheckKeys(node, {"count", "initial_count", "partitions", "alter_batch_size"}, name);
    TGroupConfig result;
    result.Count = Integer(node["count"], name + ".count");
    result.InitialCount = node["initial_count"] ? Integer(node["initial_count"], name + ".initial_count", 0) : result.Count;
    Y_ENSURE(result.InitialCount <= result.Count, name << ".initial_count must not exceed count");
    result.Partitions = Integer(node["partitions"], name + ".partitions");
    if (node["alter_batch_size"]) {
        result.AlterBatchSize = Integer(node["alter_batch_size"], name + ".alter_batch_size");
    }
    return result;
}

} // namespace

TConfig ParseConfig(const TString& yaml, const TConfigOverrides& overrides) {
    const auto documents = YAML::LoadAll(std::string(yaml));
    Y_ENSURE(documents.size() == 1, "Expected exactly one YAML document");
    auto root = documents.front();
    CheckKeys(root, {"path", "create_concurrency", "alter_concurrency", "operations", "tables", "topics"}, "config");
    Y_ENSURE(!root["operations"] || (!root["create_concurrency"] && !root["alter_concurrency"]),
        "operations cannot be combined with create_concurrency or alter_concurrency");
    if (root["operations"]) {
        CheckKeys(root["operations"], {"create", "alter", "describe", "list"}, "operations");
    }
    for (const auto& [name, count] : {std::pair{"tables", overrides.Tables}, std::pair{"topics", overrides.Topics}}) {
        if (root[name]) {
            CheckKeys(root[name], {"count", "initial_count", "partitions", "alter_batch_size"}, name);
        }
        if (count) {
            if (!root[name]) {
                root[name]["partitions"] = 1;
            }
            root[name]["count"] = std::string(*count);
        }
        if (overrides.Partitions && root[name]) {
            root[name]["partitions"] = std::string(*overrides.Partitions);
        }
    }
    if (overrides.Path) {
        root["path"] = std::string(*overrides.Path);
    }
    if (overrides.Operations) {
        YAML::Node operations(YAML::NodeType::Map);
        for (const auto& part : StringSplitter(*overrides.Operations).Split(',')) {
            const TStringBuf item = part.Token();
            const auto separator = item.find('=');
            Y_ENSURE(separator != TStringBuf::npos && separator > 0 && separator + 1 < item.size(),
                "--operations must be a comma-separated list such as create=16,describe=32");
            const std::string name(item.substr(0, separator));
            Y_ENSURE(!operations[name], "Duplicate operation: " << name);
            operations[name] = std::string(item.substr(separator + 1));
        }
        root.remove("create_concurrency");
        root.remove("alter_concurrency");
        root["operations"] = operations;
    }
    TConfig result;
    Y_ENSURE(root["path"].IsScalar(), "path must be a relative database path");
    result.Path = root["path"].Scalar();
    Y_ENSURE(!result.Path.empty(), "path must not be empty");
    for (const auto& part : StringSplitter(result.Path).Split('/')) {
        const TStringBuf component = part.Token();
        Y_ENSURE(!component.empty() && component != "." && component != "..", "path must contain non-empty relative components");
        for (char c : component) {
            Y_ENSURE((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                || c == '_' || c == '-' || c == '.', "Unsupported character in path");
        }
    }
    if (root["operations"]) {
        const auto operations = root["operations"];
        CheckKeys(operations, {"create", "alter", "describe", "list"}, "operations");
        result.CreateConcurrency = operations["create"] ? Integer(operations["create"], "operations.create") : 0;
        result.AlterConcurrency = operations["alter"] ? Integer(operations["alter"], "operations.alter") : 0;
        result.DescribeConcurrency = operations["describe"] ? Integer(operations["describe"], "operations.describe") : 0;
        result.ListConcurrency = operations["list"] ? Integer(operations["list"], "operations.list") : 0;
        Y_ENSURE(result.TotalConcurrency() > 0, "operations must not be empty");
        Y_ENSURE(result.TotalConcurrency() <= MAX_IN_FLIGHT_OPERATIONS,
            "Sum of operation concurrency limits must not exceed " << MAX_IN_FLIGHT_OPERATIONS);
    } else {
        if (root["create_concurrency"]) {
            result.CreateConcurrency = Integer(root["create_concurrency"], "create_concurrency");
        }
        if (root["alter_concurrency"]) {
            result.AlterConcurrency = Integer(root["alter_concurrency"], "alter_concurrency");
        }
        Y_ENSURE(result.TotalConcurrency() <= MAX_IN_FLIGHT_OPERATIONS,
            "create_concurrency + alter_concurrency must not exceed " << MAX_IN_FLIGHT_OPERATIONS);
    }
    if (root["tables"]) {
        result.Tables = ParseGroup(root["tables"], "tables");
    }
    if (root["topics"]) {
        result.Topics = ParseGroup(root["topics"], "topics");
    }
    Y_ENSURE(result.Tables || result.Topics ||
        (result.ListConcurrency && !result.CreateConcurrency && !result.AlterConcurrency && !result.DescribeConcurrency),
        "At least one of tables or topics is required unless only list is enabled");
    Y_ENSURE(!overrides.Partitions || result.Tables || result.Topics, "--partitions requires tables or topics");
    return result;
}

ui64 TConfig::TotalConcurrency() const {
    return ui64(CreateConcurrency) + AlterConcurrency + DescribeConcurrency + ListConcurrency;
}

ui64 TConfig::ObjectCount() const {
    return ui64(Tables ? Tables->Count : 0) + (Topics ? Topics->Count : 0);
}

ui64 TConfig::InitialObjectCount() const {
    return ui64(Tables ? Tables->InitialCount : 0) + (Topics ? Topics->InitialCount : 0);
}

TObject TConfig::InitialObject(ui64 index) const {
    Y_ENSURE(index < InitialObjectCount(), "Initial object index out of range");
    if (Tables && index >= Tables->InitialCount) {
        index += Tables->Count - Tables->InitialCount;
    }
    return Object(index);
}

TObject TConfig::Object(ui64 index) const {
    Y_ENSURE(index < ObjectCount(), "Object index out of range");
    if (Tables && index < Tables->Count) {
        return {EObjectType::Table, TStringBuilder() << Path << "/table_" << index, Tables->Partitions, Tables->AlterBatchSize};
    }
    index -= Tables ? Tables->Count : 0;
    return {EObjectType::Topic, TStringBuilder() << Path << "/topic_" << index, Topics->Partitions, Topics->AlterBatchSize};
}

} // namespace NYdbWorkload::NScheme
