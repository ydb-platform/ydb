#pragma once

#include "column_shard_log_writer.h"

#include <ydb/library/services/services.pb.h>

namespace NKikimr::NKqp::NEventLog::NAudit {

class TBaseAuditEventLogWriter : public TColumnShardLogWriter {
public:
    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto dict = TEventLogColumn::TDatabaseSettings::Dictionary();

        return TVector<std::shared_ptr<TEventLogColumn>>{
            std::make_shared<TDBLogMessageIdColumn>(1),
            std::make_shared<TDBLogMessageTimeColumn>(),
            std::make_shared<TDBLogMessagePrioColumn>(),
            std::make_shared<TDBLogMessageNodeIdColumn>(),
            std::make_shared<TDBLogMessageErrorColumn>(),
            std::make_shared<TDBLogMessageStringValueColumn>("subject"),
            std::make_shared<TDBLogMessageStringValueColumn>("sanitized_token", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("operation", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("component", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("status", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("reason"),
            std::make_shared<TDBLogMessageStringValueColumn>("request_id"),
            std::make_shared<TDBLogMessageStringValueColumn>("remote_address", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("detailed_status"),
            std::make_shared<TDBLogMessageStringValueColumn>("database", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("cloud_id", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("folder_id", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("resource_id", dict),
        };
    }

    bool Filter(const NActors::NStructuredLog::TLogMessage& message) override {
        if (message.Component != NKikimrServices::AUDIT_LOG_WRITER ||
            message.TextMessage != "Audit event") {
            return false;
        }

        if (ComponentName.empty()) {
            return true;
        }

        TStringValueExtractor extractor;
        const auto& component = extractor.ExtractValue(
            message.StructuredMessage, std::vector<TKeyName>{"component"});
        return component.has_value() && component.value() == ComponentName;
    }

protected:
    TBaseAuditEventLogWriter(
        const TDatabaseSettings& settings,
        TString componentName,
        TVector<std::shared_ptr<TEventLogColumn>> columns)
        :TColumnShardLogWriter(settings, std::move(columns))
        ,ComponentName(std::move(componentName))
    {
    }

    const TString ComponentName;
};

class TSchemeShardEventLogWriter : public TBaseAuditEventLogWriter {
public:
    TSchemeShardEventLogWriter(const TDatabaseSettings& settings)
        :TBaseAuditEventLogWriter(settings, "schemeshard", GetColumns())
    {
    }

    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto result = TBaseAuditEventLogWriter::GetColumns();

        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("tx_id"),
            std::make_shared<TDBLogMessageStringValueColumn>("paths"),
            std::make_shared<TDBLogMessageStringValueColumn>("new_owner"),
            std::make_shared<TDBLogMessageStringValueColumn>("acl_add"),
            std::make_shared<TDBLogMessageStringValueColumn>("user_attrs_add"),
            std::make_shared<TDBLogMessageStringValueColumn>("user_attrs_remove"),
            std::make_shared<TDBLogMessageStringValueColumn>("login_user"),
            std::make_shared<TDBLogMessageStringValueColumn>("login_group"),
            std::make_shared<TDBLogMessageStringValueColumn>("login_member"),
            std::make_shared<TDBLogMessageStringValueColumn>("login_user_change"),
            std::make_shared<TDBLogMessageStringValueColumn>("login_user_level"),
            std::make_shared<TDBLogMessageStringValueColumn>("id"),
            std::make_shared<TDBLogMessageStringValueColumn>("uid"),
            std::make_shared<TDBLogMessageStringValueColumn>("start_time"),
            std::make_shared<TDBLogMessageStringValueColumn>("end_time"),
            std::make_shared<TDBLogMessageStringValueColumn>("last_login"),
            std::make_shared<TDBLogMessageStringValueColumn>("export_type"),
            std::make_shared<TDBLogMessageStringValueColumn>("export_item_count"),
            std::make_shared<TDBLogMessageStringValueColumn>("export_yt_prefix"),
            std::make_shared<TDBLogMessageStringValueColumn>("export_s3_bucket"),
            std::make_shared<TDBLogMessageStringValueColumn>("export_s3_prefix"),
            std::make_shared<TDBLogMessageStringValueColumn>("import_type"),
            std::make_shared<TDBLogMessageStringValueColumn>("import_item_count"),
            std::make_shared<TDBLogMessageStringValueColumn>("import_s3_bucket"),
            std::make_shared<TDBLogMessageStringValueColumn>("import_s3_prefix"),
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }
};

class TGrpcProxyEventLogWriter : public TBaseAuditEventLogWriter {
public:
    TGrpcProxyEventLogWriter(const TDatabaseSettings& settings)
        : TBaseAuditEventLogWriter(settings, "grpc-proxy", GetColumns())
    {
    }

    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto result = TBaseAuditEventLogWriter::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("grpc_method"),
            std::make_shared<TDBLogMessageStringValueColumn>("request"),
            std::make_shared<TDBLogMessageStringValueColumn>("start_time"),
            std::make_shared<TDBLogMessageStringValueColumn>("end_time"),
            std::make_shared<TDBLogMessageStringValueColumn>("tx_id"),
            std::make_shared<TDBLogMessageStringValueColumn>("begin_tx"),
            std::make_shared<TDBLogMessageStringValueColumn>("commit_tx"),
            std::make_shared<TDBLogMessageStringValueColumn>("query_text"),
            std::make_shared<TDBLogMessageStringValueColumn>("prepared_query_id"),
            std::make_shared<TDBLogMessageStringValueColumn>("program_text"),
            std::make_shared<TDBLogMessageStringValueColumn>("schema_changes"),
            std::make_shared<TDBLogMessageStringValueColumn>("table"),
            std::make_shared<TDBLogMessageStringValueColumn>("row_count"),
            std::make_shared<TDBLogMessageStringValueColumn>("tablet_id"),
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }
};

class TGrpcConnEventLogWriter : public TBaseAuditEventLogWriter {
public:
    TGrpcConnEventLogWriter(const TDatabaseSettings& settings)
        : TBaseAuditEventLogWriter(settings, "grpc-conn", GetColumns())
    {
    }

    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto result = TBaseAuditEventLogWriter::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns;
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }
};

class TGrpcLoginEventLogWriter : public TBaseAuditEventLogWriter {
public:
    TGrpcLoginEventLogWriter(const TDatabaseSettings& settings)
        : TBaseAuditEventLogWriter(settings, "grpc-login", GetColumns())
    {
    }

    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto result = TBaseAuditEventLogWriter::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("login_user"),
            std::make_shared<TDBLogMessageStringValueColumn>("login_user_level")};
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }
};

class TMonitoringEventLogWriter : public TBaseAuditEventLogWriter {
public:
    TMonitoringEventLogWriter(const TDatabaseSettings& settings)
        : TBaseAuditEventLogWriter(settings, "monitoring", GetColumns())
    {
    }

    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto result = TBaseAuditEventLogWriter::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("method"),
            std::make_shared<TDBLogMessageStringValueColumn>("url"),
            std::make_shared<TDBLogMessageStringValueColumn>("params"),
            std::make_shared<TDBLogMessageStringValueColumn>("body"),
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }
};

class TAuditServiceEventLogWriter : public TBaseAuditEventLogWriter {
public:
    TAuditServiceEventLogWriter(const TDatabaseSettings& settings)
        : TBaseAuditEventLogWriter(settings, "audit-service", GetColumns())
    {
    }

    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto result = TBaseAuditEventLogWriter::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("node_id"),
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }
};

class TBscEventLogWriter : public TBaseAuditEventLogWriter {
public:
    TBscEventLogWriter(const TDatabaseSettings& settings)
        : TBaseAuditEventLogWriter(settings, "bsc", GetColumns())
    {
    }

    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto result = TBaseAuditEventLogWriter::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("old_config"),
            std::make_shared<TDBLogMessageStringValueColumn>("new_config"),
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }
};

class TDistconfEventLogWriter : public TBaseAuditEventLogWriter {
public:
    TDistconfEventLogWriter(const TDatabaseSettings& settings)
        : TBaseAuditEventLogWriter(settings, "distconf", GetColumns())
    {
    }

    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto result = TBaseAuditEventLogWriter::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("old_config"),
            std::make_shared<TDBLogMessageStringValueColumn>("new_config"),
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }
};

class TWebLoginEventLogWriter : public TBaseAuditEventLogWriter {
public:
    TWebLoginEventLogWriter(const TDatabaseSettings& settings)
        : TBaseAuditEventLogWriter(settings, "web-login", GetColumns())
    {
    }

    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto result = TBaseAuditEventLogWriter::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns;
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }
};

class TConsoleEventLogWriter : public TBaseAuditEventLogWriter {
public:
    TConsoleEventLogWriter(const TDatabaseSettings& settings)
        : TBaseAuditEventLogWriter(settings, "console", GetColumns())
    {
    }

    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto result = TBaseAuditEventLogWriter::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("old_config"),
            std::make_shared<TDBLogMessageStringValueColumn>("new_config"),
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }
};

} // namespace NKikimr::NKqp::NEventLog::NAudit
