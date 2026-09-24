#pragma once

#include <util/generic/strbuf.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/api/protos/ydb_import.pb.h>

#include <type_traits>

namespace NKikimr::NGRpcService {

// Inspect database and schema-object paths in the incoming protobuf. In particular,
// SQL, rate-limiter resource names and external storage paths are not schema paths.
template <typename TContext, typename TRequest>
void CountSchemaRequestPaths(const TContext& context, const TRequest& request) {
    const auto count = [&](TStringBuf path) {
        context.CountResourcePath(path);
    };
    if constexpr (requires { TStringBuf(request); }) {
        count(request);
    }
    if constexpr (std::is_same_v<TRequest, Ydb::Cms::CreateDatabaseRequest>
        || std::is_same_v<TRequest, Ydb::Cms::AlterDatabaseRequest>
        || std::is_same_v<TRequest, Ydb::Cms::GetDatabaseStatusRequest>
        || std::is_same_v<TRequest, Ydb::Cms::GetScaleRecommendationRequest>
        || std::is_same_v<TRequest, Ydb::Cms::RemoveDatabaseRequest>) {
        context.CountDatabasePath(request.path());
    } else if constexpr (requires { TStringBuf(request.path()); }) {
        count(request.path());
    } else if constexpr (requires { TStringBuf(*request.path().begin()); }) {
        for (const auto& path : request.path()) {
            count(path);
        }
    }
    if constexpr (requires { TStringBuf(*request.paths().begin()); }) {
        for (const auto& path : request.paths()) {
            count(path);
        }
    }
    if constexpr (requires { TStringBuf(request.table()); }) {
        count(request.table());
    }
    if constexpr (requires { TStringBuf(request.stream_name()); }) {
        count(request.stream_name());
    }
    if constexpr (requires { TStringBuf(request.stream_arn()); }) {
        count(request.stream_arn());
    }
    if constexpr (requires { TStringBuf(request.source_path()); }) {
        count(request.source_path());
    }
    if constexpr (requires { TStringBuf(request.destination_path()); }) {
        count(request.destination_path());
    }
    if constexpr (requires { TStringBuf(request.coordination_node_path()); }) {
        count(request.coordination_node_path());
    }
    if constexpr (requires { request.tables().begin(); }) {
        for (const auto& table : request.tables()) {
            CountSchemaRequestPaths(context, table);
        }
    }
    if constexpr (requires { request.topics().begin(); }) {
        for (const auto& topic : request.topics()) {
            CountSchemaRequestPaths(context, topic);
        }
    }
    if constexpr (requires { request.write_sessions().begin(); }) {
        for (const auto& session : request.write_sessions()) {
            count(session.topic());
        }
    }
    if constexpr (requires { request.read_sessions().begin(); }) {
        for (const auto& session : request.read_sessions()) {
            count(session.topic());
        }
    }
    if constexpr (requires { request.settings(); }) {
        using TSettings = std::decay_t<decltype(request.settings())>;
        const auto& settings = request.settings();
        if constexpr (std::is_same_v<TSettings, Ydb::Export::ExportToYtSettings>
            || std::is_same_v<TSettings, Ydb::Export::ExportToS3Settings>
            || std::is_same_v<TSettings, Ydb::Export::ExportToFsSettings>) {
            if constexpr (requires { settings.source_path(); }) {
                count(settings.source_path());
            }
            for (const auto& item : settings.items()) {
                count(item.source_path());
            }
        } else if constexpr (std::is_same_v<TSettings, Ydb::Import::ImportFromS3Settings>
            || std::is_same_v<TSettings, Ydb::Import::ImportFromFsSettings>) {
            count(settings.destination_path());
            for (const auto& item : settings.items()) {
                count(item.destination_path());
            }
        }
    }
    if constexpr (requires { request.ttl_settings(); }) {
        CountSchemaRequestPaths(context, request.ttl_settings());
    }
    if constexpr (requires { request.set_ttl_settings(); }) {
        CountSchemaRequestPaths(context, request.set_ttl_settings());
    }
    if constexpr (requires { request.tiered_ttl().tiers(); }) {
        for (const auto& tier : request.tiered_ttl().tiers()) {
            if (tier.has_evict_to_external_storage()) {
                count(tier.evict_to_external_storage().storage());
            }
        }
    }
}

} // namespace NKikimr::NGRpcService
