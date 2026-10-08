#include "yql_ydb_provider_impl.h"
#include <ydb/library/yql/providers/ydb/query/common/settings.h>

#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

namespace NYql::NYdbQuery {

void AddCluster(TState& state, const TString& name, const THashMap<TString, TString>& properties) {
    if (properties.contains("database_id") || properties.contains("mdb_cluster_id")) {
        throw yexception() << "Ydb currently requires explicit LOCATION and DATABASE_NAME; database ID resolution is not supported";
    }
    // A Ydb EDS may be shared by table reads and topic operations. Topic-only
    // properties do not affect the table reader; PQ consumes them on its route.
    TCluster cluster;
    cluster.Endpoint = properties.Value("location", "");
    cluster.Database = properties.Value("database_name", "");
    // The existing Ydb DDL also accepts relative database names for topics.
    // Use the same absolute SDK database for table reads.
    if (!cluster.Database.empty() && !cluster.Database.StartsWith('/')) {
        cluster.Database = "/" + cluster.Database;
    }
    TString tls = properties.Value("use_tls", "false");
    if (const auto error = ValidateConnectionSettings(cluster.Endpoint, cluster.Database, tls)) {
        throw yexception() << error;
    }
    while (cluster.Database.size() > 1 && cluster.Database.EndsWith('/')) {
        cluster.Database.pop_back();
    }

    tls.to_lower();
    cluster.UseTls = tls == "true";

    TString token;
    const auto auth = properties.Value("authMethod", "");
    if (auth == "TOKEN") {
        if (properties.Value("token", "").empty()) {
            throw yexception() << "Ydb TOKEN credentials are missing";
        }
        const auto reference = properties.Value("tokenReference", "");
        token = reference.empty()
            ? TStructuredTokenBuilder().SetIAMToken(properties.Value("token", "")).ToJson()
            : ComposeStructuredTokenJsonForTokenAuthWithSecret(reference, properties.Value("token", ""));
    } else if (auth == "NONE") {
        token = TStructuredTokenBuilder().SetNoAuth().ToJson();
    } else {
        throw yexception() << "Ydb currently supports only TOKEN and NONE authentication";
    }

    state.Clusters[name] = std::move(cluster);
    state.Tokens[name] = std::move(token);
    state.ValidClusters.insert(name);
}

const TTypeAnnotationNode* ParseColumnType(const Ydb::Type& type, TExprContext& ctx) {
    const bool optional = type.has_optional_type();
    const auto& primitive = optional ? type.optional_type().item() : type;
    if (!primitive.has_type_id()) {
        return nullptr;
    }
    EDataSlot slot;
    switch (primitive.type_id()) {
        case Ydb::Type::BOOL: slot = EDataSlot::Bool; break;
        case Ydb::Type::INT8: slot = EDataSlot::Int8; break;
        case Ydb::Type::INT16: slot = EDataSlot::Int16; break;
        case Ydb::Type::INT32: slot = EDataSlot::Int32; break;
        case Ydb::Type::INT64: slot = EDataSlot::Int64; break;
        case Ydb::Type::UINT8: slot = EDataSlot::Uint8; break;
        case Ydb::Type::UINT16: slot = EDataSlot::Uint16; break;
        case Ydb::Type::UINT32: slot = EDataSlot::Uint32; break;
        case Ydb::Type::UINT64: slot = EDataSlot::Uint64; break;
        case Ydb::Type::FLOAT: slot = EDataSlot::Float; break;
        case Ydb::Type::DOUBLE: slot = EDataSlot::Double; break;
        case Ydb::Type::STRING: slot = EDataSlot::String; break;
        case Ydb::Type::UTF8: slot = EDataSlot::Utf8; break;
        default: return nullptr;
    }
    const TTypeAnnotationNode* result = ctx.MakeType<TDataExprType>(slot);
    if (optional) {
        result = ctx.MakeType<TOptionalExprType>(result);
    }
    return result;
}

} // namespace NYql::NYdbQuery
