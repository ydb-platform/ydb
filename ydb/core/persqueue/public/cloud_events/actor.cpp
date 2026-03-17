#include "actor.h"

#include <google/protobuf/json/json.h>
#include <util/generic/guid.h>
#include <ydb/core/audit/audit_log.h>
#include <google/protobuf/util/time_util.h>
#include <ydb/core/audit/audit_log_impl.h>
#include <ydb/core/audit/audit_log_service.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NPQ::NCloudEvents {

namespace {

std::string GetOperationType(const NKikimrSchemeOp::TModifyScheme& operation) {
    if (operation.HasCreatePersQueueGroup()) {
        return "CreateTopic";
    } else if (operation.HasAlterPersQueueGroup()) {
        return "AlterTopic";
    } else if (operation.HasDrop()) {
        return "DeleteTopic";
    }
    return "";
}

using namespace yandex::cloud::events::ydb::topics;

AutoPartitioningStrategy ConvertPartitionStrategyType(NKikimrPQ::TPQTabletConfig_TPartitionStrategyType type) {
    using TType = NKikimrPQ::TPQTabletConfig_TPartitionStrategyType;
    switch (type) {
        case TType::TPQTabletConfig_TPartitionStrategyType_DISABLED:
            return AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_DISABLED;
        case TType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT:
            return AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP;
        case TType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE:
            return AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN;
        case TType::TPQTabletConfig_TPartitionStrategyType_PAUSED:
            return AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_PAUSED;
        default:
            return AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_UNSPECIFIED;
    }
}

MeteringMode ConvertMeteringMode(NKikimrPQ::TPQTabletConfig_EMeteringMode mode) {
    using TMode = NKikimrPQ::TPQTabletConfig_EMeteringMode;
    switch (mode) {
        case TMode::TPQTabletConfig_EMeteringMode_METERING_MODE_RESERVED_CAPACITY:
            return MeteringMode::METERING_MODE_RESERVED_CAPACITY;
        case TMode::TPQTabletConfig_EMeteringMode_METERING_MODE_REQUEST_UNITS:
            return MeteringMode::METERING_MODE_REQUEST_UNITS;
        default:
            return MeteringMode::METERING_MODE_UNSPECIFIED;
    }
}

void FillTopicDetails(TopicDetails* details, const NKikimrSchemeOp::TPersQueueGroupDescription& desc) {
    if (!details) {
        return;
    }

    if (desc.HasPQTabletConfig()) {
        const auto& cfg = desc.GetPQTabletConfig();

        if (cfg.HasPartitionStrategy()) {
            const auto& ps = cfg.GetPartitionStrategy();
            auto* part = details->mutable_partitioning_settings();

            if (ps.HasMinPartitionCount()) {
                part->set_min_active_partitions(ps.GetMinPartitionCount());
            }
            if (ps.HasMaxPartitionCount()) {
                part->set_max_active_partitions(ps.GetMaxPartitionCount());
            }

            auto* autoPart = part->mutable_auto_partitioning_settings();
            autoPart->set_strategy(ConvertPartitionStrategyType(ps.GetPartitionStrategyType()));
        }

        const auto& partCfg = cfg.GetPartitionConfig();

        if (partCfg.HasWriteSpeedInBytesPerSecond()) {
            details->set_partition_write_speed_bytes_per_second(partCfg.GetWriteSpeedInBytesPerSecond());
        }

        if (partCfg.HasLifetimeSeconds()) {
            auto* retention = details->mutable_retention_period();
            *retention = google::protobuf::util::TimeUtil::SecondsToDuration(partCfg.GetLifetimeSeconds());
        }

        if (partCfg.HasStorageLimitBytes()) {
            constexpr ui64 BYTES_IN_MB = 1024ull * 1024ull;
            details->set_retention_storage_mb(partCfg.GetStorageLimitBytes() / BYTES_IN_MB);
        }

        if (cfg.HasMeteringMode()) {
            details->set_metering_mode(ConvertMeteringMode(cfg.GetMeteringMode()));
        }

        if (cfg.HasMetricsLevel()) {
            details->set_metrics_level(cfg.GetMetricsLevel());
        }

        for (const auto& c : cfg.GetConsumers()) {
            if (c.HasName()) {
                auto* consumer = details->add_consumers();
                consumer->set_name(c.GetName());
            }
        }
    }
}

template <typename TRequestParameters>
void FillTopicRequestParameters(TRequestParameters* params, const TString& path, const NKikimrSchemeOp::TPersQueueGroupDescription& desc) {
    if (!params) {
        return;
    }

    params->set_path(path);
    if (desc.HasPQTabletConfig()) {
        const auto& cfg = desc.GetPQTabletConfig();

        if (cfg.HasPartitionStrategy()) {
            const auto& ps = cfg.GetPartitionStrategy();
            auto* part = params->mutable_partitioning_settings();

            if (ps.HasMinPartitionCount()) {
                part->set_min_active_partitions(ps.GetMinPartitionCount());
            }
            if (ps.HasMaxPartitionCount()) {
                part->set_max_active_partitions(ps.GetMaxPartitionCount());
            }

            auto* autoPart = part->mutable_auto_partitioning_settings();
            autoPart->set_strategy(ConvertPartitionStrategyType(ps.GetPartitionStrategyType()));
        }

        const auto& partCfg = cfg.GetPartitionConfig();
        if (partCfg.HasLifetimeSeconds()) {
            auto* retention = params->mutable_retention_period();
            *retention = google::protobuf::util::TimeUtil::SecondsToDuration(partCfg.GetLifetimeSeconds());
        }

        if (partCfg.HasStorageLimitBytes()) {
            constexpr ui64 BYTES_IN_MB = 1024ull * 1024ull;
            params->set_retention_storage_mb(partCfg.GetStorageLimitBytes() / BYTES_IN_MB);
        }
        if (partCfg.HasWriteSpeedInBytesPerSecond()) {
            params->set_partition_write_speed_bytes_per_second(partCfg.GetWriteSpeedInBytesPerSecond());
        }
        if (cfg.HasMeteringMode()) {
            params->set_metering_mode(ConvertMeteringMode(cfg.GetMeteringMode()));
        }
        if (cfg.HasMetricsLevel()) {
            params->set_metrics_level(cfg.GetMetricsLevel());
        }
        for (const auto& c : cfg.GetConsumers()) {
            if (c.HasName()) {
                auto* consumer = params->add_consumers();
                consumer->set_name(c.GetName());
            }
        }
    }
}

static void FillRequestedPermission(
    google::protobuf::RepeatedPtrField<yandex::cloud::events::RequestedPermissions>* permissions,
    const TCloudEventInfo& info)
{
    auto* permission = permissions->Add();
    permission->set_permission("ydb.databases.alter");
    permission->set_resource_type("ydb.databases");
    TString resourceId = info.DatabaseId.empty() ? info.TopicPath : info.DatabaseId + "/" + info.TopicPath;
    permission->set_resource_id(resourceId);
    permission->set_authorized(true);
}

static void Fill(TCreateTopicEvent& ev, const TCloudEventInfo& info) {
    // Authentication
    ev.mutable_authentication()->set_authenticated(true);
    ev.mutable_authentication()->set_subject_id(info.UserSID);
    ev.mutable_authentication()->set_subject_type(
        yandex::cloud::events::Authentication::SERVICE_ACCOUNT);

    // Authorization
    ev.mutable_authorization()->set_authorized(true);
    FillRequestedPermission(ev.mutable_authorization()->mutable_permissions(), info);

    // EventMetadata
    ev.mutable_event_metadata()->set_event_id(CreateGuidAsString());
    ev.mutable_event_metadata()->set_event_type("yandex.cloud.events.ydb.topics." + GetOperationType(info.ModifyScheme));
    auto ts = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(info.CreatedAt.MilliSeconds());
    *ev.mutable_event_metadata()->mutable_created_at() = ts;
    ev.mutable_event_metadata()->set_cloud_id(info.CloudId);
    ev.mutable_event_metadata()->set_folder_id(info.FolderId);

    // RequestMetadata
    ev.mutable_request_metadata()->set_remote_address(info.RemoteAddress);
    ev.mutable_request_metadata()->set_user_agent(info.UserAgent);

    if (info.OperationStatus == NKikimrScheme::StatusSuccess) {
        ev.set_event_status(EStatus::DONE);
    } else if (info.OperationStatus == NKikimrScheme::StatusAccepted) {
        ev.set_event_status(EStatus::STARTED);
    } else {
        ev.set_event_status(EStatus::ERROR);
        ev.mutable_error()->set_message(info.Issue);
    }

    Y_VERIFY(info.ModifyScheme.HasCreatePersQueueGroup());

    auto* details = ev.mutable_details();
    details->set_path(info.TopicPath);
    FillTopicDetails(details, info.ModifyScheme.GetCreatePersQueueGroup());

    auto* requestParams = ev.mutable_request_parameters();
    FillTopicRequestParameters(requestParams, info.TopicPath, info.ModifyScheme.GetCreatePersQueueGroup());
}

static void Fill(TAlterTopicEvent& ev, const TCloudEventInfo& info) {
    ev.mutable_authentication()->set_authenticated(true);
    ev.mutable_authentication()->set_subject_id(info.UserSID);
    ev.mutable_authentication()->set_subject_type(
        yandex::cloud::events::Authentication::SERVICE_ACCOUNT);
    ev.mutable_authorization()->set_authorized(true);
    FillRequestedPermission(ev.mutable_authorization()->mutable_permissions(), info);

    ev.mutable_event_metadata()->set_event_id(CreateGuidAsString());
    ev.mutable_event_metadata()->set_event_type("yandex.cloud.events.ydb.topics." + GetOperationType(info.ModifyScheme));
    auto ts = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(info.CreatedAt.MilliSeconds());
    *ev.mutable_event_metadata()->mutable_created_at() = ts;
    ev.mutable_event_metadata()->set_cloud_id(info.CloudId);
    ev.mutable_event_metadata()->set_folder_id(info.FolderId);

    ev.mutable_request_metadata()->set_remote_address(info.RemoteAddress);
    ev.mutable_request_metadata()->set_user_agent(info.UserAgent);

    if (info.OperationStatus == NKikimrScheme::StatusSuccess) {
        ev.set_event_status(EStatus::DONE);
    } else if (info.OperationStatus == NKikimrScheme::StatusAccepted) {
        ev.set_event_status(EStatus::STARTED);
    } else {
        ev.set_event_status(EStatus::ERROR);
        ev.mutable_error()->set_message(info.Issue);
    }

    Y_VERIFY(info.ModifyScheme.HasAlterPersQueueGroup());

    auto* details = ev.mutable_details();
    details->set_path(info.TopicPath);
    FillTopicDetails(details, info.ModifyScheme.GetAlterPersQueueGroup());

    auto* requestParams = ev.mutable_request_parameters();
    FillTopicRequestParameters(requestParams, info.TopicPath, info.ModifyScheme.GetAlterPersQueueGroup());
}

static void Fill(TDeleteTopicEvent& ev, const TCloudEventInfo& info) {
    ev.mutable_authentication()->set_authenticated(true);
    ev.mutable_authentication()->set_subject_id(info.UserSID);
    ev.mutable_authentication()->set_subject_type(
        yandex::cloud::events::Authentication::SERVICE_ACCOUNT);
    ev.mutable_authorization()->set_authorized(true);
    FillRequestedPermission(ev.mutable_authorization()->mutable_permissions(), info);

    ev.mutable_event_metadata()->set_event_id(CreateGuidAsString());
    ev.mutable_event_metadata()->set_event_type("yandex.cloud.events.ydb.topics." + GetOperationType(info.ModifyScheme));
    auto ts = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(info.CreatedAt.MilliSeconds());
    *ev.mutable_event_metadata()->mutable_created_at() = ts;
    ev.mutable_event_metadata()->set_cloud_id(info.CloudId);
    ev.mutable_event_metadata()->set_folder_id(info.FolderId);

    ev.mutable_request_metadata()->set_remote_address(info.RemoteAddress);
    ev.mutable_request_metadata()->set_user_agent(info.UserAgent);

    if (info.Issue.empty()) {
        ev.set_event_status(EStatus::DONE);
    } else {
        ev.set_event_status(EStatus::ERROR);
        ev.mutable_error()->set_message(info.Issue);
    }

    ev.mutable_details()->set_path(info.TopicPath);

    auto* requestParams = ev.mutable_request_parameters();
    requestParams->set_path(info.TopicPath);
}

template<typename TEvent>
TString SerializeEvent(const TEvent& ev) {
    TString json;
    google::protobuf::json::PrintOptions printOpts;
    printOpts.preserve_proto_field_names = true;
    printOpts.always_print_primitive_fields = true;
    google::protobuf::json::MessageToJsonString(ev, &json, printOpts);
    return json;
}

}

void TCloudEventsActor::Bootstrap() {
    Become(&TCloudEventsActor::StateWork);
}

void TCloudEventsActor::Handle(TCloudEvent::TPtr& ev) {
    TString json;

    auto type = ev.Get()->Get()->Info.ModifyScheme.GetOperationType();
    if (type == NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup) {
        TCreateTopicEvent proto;
        Fill(proto, ev.Get()->Get()->Info);
        json = SerializeEvent(proto);
    } else if (type == NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup) {
        TAlterTopicEvent proto;
        Fill(proto, ev.Get()->Get()->Info);
        json = SerializeEvent(proto);
    } else if (type == NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup) {
        TDeleteTopicEvent proto;
        Fill(proto, ev.Get()->Get()->Info);
        json = SerializeEvent(proto);
    }

    NKikimr::NAudit::TAuditLogParts parts;
    parts.emplace_back("cloud_event_json", json);
    auto request = MakeHolder<NAudit::TEvAuditLog::TEvWriteAuditLog>(Now(), std::move(parts));
    NActors::TActivationContext::ActorSystem()->Send(NAudit::MakeTopicCloudEventsAuditServiceID(), request.Release());
}

} // namespace NKikimr::NPQ::NCloudEvents