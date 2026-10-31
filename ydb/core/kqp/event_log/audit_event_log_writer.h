#pragma once

#include "column_shard_log_writer.h"

#include <ydb/library/services/services.pb.h>

namespace NKikimr::NKqp::NEventLog::NAudit {

class TAuditEventLogFilter {
public:
    explicit TAuditEventLogFilter(TString componentName)
        : ComponentName(std::move(componentName))
    {
    }

    bool operator()(const NActors::NStructuredLog::TLogMessage& message) const {
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

private:
    TString ComponentName;
};

class TAuditEventLogCommon {
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
            std::make_shared<TDBLogMessageStringValueColumn>("sanitized_token"),
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
};

class TSchemeShardEventLog {
public:
    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto result = TAuditEventLogCommon::GetColumns();
        auto dict = TEventLogColumn::TDatabaseSettings::Dictionary();

        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("tx_id"),
            std::make_shared<TDBLogMessageStringValueColumn>("paths", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("new_owner", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("acl_add", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("user_attrs_add", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("user_attrs_remove", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("login_user", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("login_group", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("login_member", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("login_user_change", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("login_user_level", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("audit_event_id"),
            std::make_shared<TDBLogMessageStringValueColumn>("uid"),
            std::make_shared<TDBLogMessageStringValueColumn>("start_time"),
            std::make_shared<TDBLogMessageStringValueColumn>("end_time"),
            std::make_shared<TDBLogMessageStringValueColumn>("last_login", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("export_type", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("export_item_count"),
            std::make_shared<TDBLogMessageStringValueColumn>("export_yt_prefix", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("export_s3_bucket", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("export_s3_prefix", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("import_type", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("import_item_count"),
            std::make_shared<TDBLogMessageStringValueColumn>("import_s3_bucket", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("import_s3_prefix", dict),
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }

    static TBaseEventLogWriter::TFilterFunction CreateFilter() {
        return TAuditEventLogFilter("schemeshard");
    }
};

class TGrpcProxyEventLog {
public:
    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto dict = TEventLogColumn::TDatabaseSettings::Dictionary();

        auto result = TAuditEventLogCommon::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("grpc_method", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("request"),
            std::make_shared<TDBLogMessageStringValueColumn>("start_time"),
            std::make_shared<TDBLogMessageStringValueColumn>("end_time"),
            std::make_shared<TDBLogMessageStringValueColumn>("tx_id"),
            std::make_shared<TDBLogMessageStringValueColumn>("begin_tx"),
            std::make_shared<TDBLogMessageStringValueColumn>("commit_tx"),
            std::make_shared<TDBLogMessageStringValueColumn>("query_text"),
            std::make_shared<TDBLogMessageStringValueColumn>("prepared_query_id"),
            std::make_shared<TDBLogMessageStringValueColumn>("program_text"),
            std::make_shared<TDBLogMessageStringValueColumn>("schema_changes", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("table", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("row_count"),
            std::make_shared<TDBLogMessageStringValueColumn>("tablet_id"),
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }

    static TBaseEventLogWriter::TFilterFunction CreateFilter() {
        return TAuditEventLogFilter("grpc-proxy");
    }
};

class TGrpcConnEventLog {
public:
    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        return TAuditEventLogCommon::GetColumns();
    }

    static TBaseEventLogWriter::TFilterFunction CreateFilter() {
        return TAuditEventLogFilter("grpc-conn");
    }
};

class TGrpcLoginEventLog {
public:
    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto dict = TEventLogColumn::TDatabaseSettings::Dictionary();
        auto result = TAuditEventLogCommon::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("login_user", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("login_user_level", dict)};
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }

    static TBaseEventLogWriter::TFilterFunction CreateFilter() {
        return TAuditEventLogFilter("grpc-login");
    }
};

class TMonitoringEventLog {
public:
    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto dict = TEventLogColumn::TDatabaseSettings::Dictionary();
        auto result = TAuditEventLogCommon::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("method", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("url", dict),
            std::make_shared<TDBLogMessageStringValueColumn>("params"),
            std::make_shared<TDBLogMessageStringValueColumn>("body"),
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }

    static TBaseEventLogWriter::TFilterFunction CreateFilter() {
        return TAuditEventLogFilter("monitoring");
    }
};

class TAuditServiceEventLog {
public:
    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        return TAuditEventLogCommon::GetColumns();
    }

    static TBaseEventLogWriter::TFilterFunction CreateFilter() {
        return TAuditEventLogFilter("audit-service");
    }
};

class TBscEventLog {
public:
    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto result = TAuditEventLogCommon::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("old_config"),
            std::make_shared<TDBLogMessageStringValueColumn>("new_config"),
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }

    static TBaseEventLogWriter::TFilterFunction CreateFilter() {
        return TAuditEventLogFilter("bsc");
    }
};

class TDistconfEventLog {
public:
    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto result = TAuditEventLogCommon::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("old_config"),
            std::make_shared<TDBLogMessageStringValueColumn>("new_config"),
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }

    static TBaseEventLogWriter::TFilterFunction CreateFilter() {
        return TAuditEventLogFilter("distconf");
    }
};

class TWebLoginEventLog {
public:
    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        return TAuditEventLogCommon::GetColumns();
    }

    static TBaseEventLogWriter::TFilterFunction CreateFilter() {
        return TAuditEventLogFilter("web-login");
    }
};

class TConsoleEventLog {
public:
    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto result = TAuditEventLogCommon::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("old_config"),
            std::make_shared<TDBLogMessageStringValueColumn>("new_config"),
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }

    static TBaseEventLogWriter::TFilterFunction CreateFilter() {
        return TAuditEventLogFilter("console");
    }
};

namespace NToColumnShard {

using TSchemeShardEventLogWriter = TColumnShardLogWriter<TSchemeShardEventLog>;
using TGrpcProxyEventLogWriter = TColumnShardLogWriter<TGrpcProxyEventLog>;
using TGrpcConnEventLogWriter = TColumnShardLogWriter<TGrpcConnEventLog>;
using TGrpcLoginEventLogWriter = TColumnShardLogWriter<TGrpcLoginEventLog>;
using TMonitoringEventLogWriter = TColumnShardLogWriter<TMonitoringEventLog>;
using TAuditServiceEventLogWriter = TColumnShardLogWriter<TAuditServiceEventLog>;
using TBscEventLogWriter = TColumnShardLogWriter<TBscEventLog>;
using TDistconfEventLogWriter = TColumnShardLogWriter<TDistconfEventLog>;
using TWebLoginEventLogWriter = TColumnShardLogWriter<TWebLoginEventLog>;
using TConsoleEventLogWriter = TColumnShardLogWriter<TConsoleEventLog>;

} // namespace NToColumnShard

} // namespace NKikimr::NKqp::NEventLog::NAudit
