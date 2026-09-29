#include "yql_ydb_remote_provider_impl.h"

#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

#include <util/string/cast.h>

namespace NYql::NYdbRemote {

void AddCluster(TState& state, const TString& name, const THashMap<TString, TString>& properties) {
    Y_ENSURE(properties.Value("database_id", "").empty() && properties.Value("mdb_cluster_id", "").empty(),
        "Native YDB currently requires explicit LOCATION and DATABASE_NAME; database ID resolution is not supported");
    TCluster cluster;
    cluster.Endpoint = properties.Value("location", "");
    cluster.Database = properties.Value("database_name", "");
    Y_ENSURE(cluster.Database.StartsWith('/') && !cluster.Database.Contains('\0'),
        "Native YDB requires an absolute DATABASE_NAME");
    while (cluster.Database.size() > 1 && cluster.Database.EndsWith('/')) {
        cluster.Database.pop_back();
    }

    // EDS LOCATION is a host:port pair. Reject URLs/userinfo rather than allowing
    // credentials or an alternate endpoint to enter the serialized source.
    const auto portSeparator = cluster.Endpoint.rfind(':');
    ui32 port = 0;
    Y_ENSURE(portSeparator != TString::npos && portSeparator != 0 &&
        !cluster.Endpoint.Contains('/') && !cluster.Endpoint.Contains('@') &&
        !cluster.Endpoint.Contains('?') && !cluster.Endpoint.Contains('#') &&
        !cluster.Endpoint.Contains('\0') &&
        TryFromString(TStringBuf(cluster.Endpoint).SubStr(portSeparator + 1), port) && port > 0 && port <= 65535,
        "Native YDB requires LOCATION in host:port format");

    TString tls = properties.Value("use_tls", "false");
    tls.to_lower();
    Y_ENSURE(tls == "true" || tls == "false", "Native YDB USE_TLS must be true or false");
    cluster.UseTls = tls == "true";

    TString token;
    const auto auth = properties.Value("authMethod", "");
    if (auth == "TOKEN") {
        Y_ENSURE(!properties.Value("token", "").empty(), "Native YDB TOKEN credentials are missing");
        const auto reference = properties.Value("tokenReference", "");
        token = reference.empty()
            ? TStructuredTokenBuilder().SetIAMToken(properties.Value("token", "")).ToJson()
            : ComposeStructuredTokenJsonForTokenAuthWithSecret(reference, properties.Value("token", ""));
    } else if (auth == "NONE") {
        token = TStructuredTokenBuilder().SetNoAuth().ToJson();
    } else {
        ythrow yexception() << "Native YDB currently supports only TOKEN and NONE authentication";
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

} // namespace NYql::NYdbRemote
