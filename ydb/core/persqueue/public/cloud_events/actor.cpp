#include "actor.h"

#include <ydb/core/persqueue/public/cloud_events/proto/topics.pb.h>

#include <util/generic/guid.h>
#include <ydb/core/audit/audit_log.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/util/time_util.h>
#include <ydb/core/audit/audit_log_impl.h>
#include <ydb/core/audit/audit_log_service.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/core/security/util/net.h>
#include <util/network/address.h>



namespace NKikimr::NPQ::NCloudEvents {

using TCreateTopicEvent = yandex::cloud::events::ydb::topics::CreateTopic;
using TAlterTopicEvent = yandex::cloud::events::ydb::topics::AlterTopic;
using TDeleteTopicEvent = yandex::cloud::events::ydb::topics::DeleteTopic;
using EStatus = yandex::cloud::events::EventStatus;

namespace {

TString NormalizeRemoteAddress(const TString& peerName) {
    auto addr = NKikimr::NSecurity::ParsePeername(peerName);
    return addr ? NAddr::PrintHost(*addr) : peerName;
}

TString NormalizeSubjectId(const TString& subjectId) {
    if (subjectId.Contains('@')) {
        size_t pos = subjectId.find('@');
        if (pos != TString::npos) {
            return subjectId.substr(0, pos);
        }
    }
    return subjectId;
}

std::string GetOperationType(const NKikimrSchemeOp::TModifyScheme& operation) {
    switch (operation.GetOperationType()) {
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup:
            return "CreateTopic";
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup:
            return "AlterTopic";
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup:
            return "DeleteTopic";
        default:
            break;
    }

    return "";
}

enum EGoogleRpcCode {
    GoogleRpcOk = 0,
    GoogleRpcUnknown = 2,
    GoogleRpcInvalidArgument = 3,
    GoogleRpcNotFound = 5,
    GoogleRpcAlreadyExists = 6,
    GoogleRpcPermissionDenied = 7,
    GoogleRpcResourceExhausted = 8,
    GoogleRpcFailedPrecondition = 9,
    GoogleRpcAborted = 10,
    GoogleRpcUnavailable = 14,
};

i32 MapSchemeStatusToGoogleRpcCode(NKikimrScheme::EStatus status) {
    switch (status) {
        case NKikimrScheme::StatusPathDoesNotExist:
        case NKikimrScheme::StatusTxIdNotExists:
            return GoogleRpcNotFound;
        case NKikimrScheme::StatusAlreadyExists:
        case NKikimrScheme::StatusNameConflict:
            return GoogleRpcAlreadyExists;
        case NKikimrScheme::StatusSchemeError:
        case NKikimrScheme::StatusInvalidParameter:
            return GoogleRpcInvalidArgument;
        case NKikimrScheme::StatusAccessDenied:
            return GoogleRpcPermissionDenied;
        case NKikimrScheme::StatusQuotaExceeded:
        case NKikimrScheme::StatusResourceExhausted:
            return GoogleRpcResourceExhausted;
        case NKikimrScheme::StatusPathIsNotDirectory:
        case NKikimrScheme::StatusReadOnly:
        case NKikimrScheme::StatusTxIsNotCancellable:
        case NKikimrScheme::StatusPreconditionFailed:
            return GoogleRpcFailedPrecondition;
        case NKikimrScheme::StatusMultipleModifications:
            return GoogleRpcAborted;
        case NKikimrScheme::StatusNotAvailable:
        case NKikimrScheme::StatusRedirectDomain:
            return GoogleRpcUnavailable;
        case NKikimrScheme::StatusSuccess:
        case NKikimrScheme::StatusAccepted:
            return GoogleRpcOk;
        case NKikimrScheme::StatusReserved18:
        case NKikimrScheme::StatusReserved19:
            return GoogleRpcUnknown;
    }
    return GoogleRpcUnknown;
}

} // anonymous namespace

TString GetCloudEventType(const TCloudEventInfo& info) {
    return TString("yandex.cloud.events.ydb.topics.") + GetOperationType(info.ModifyScheme);
}

namespace {

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
            details->set_retention_storage_mb(partCfg.GetStorageLimitBytes() / 1_MB);
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

void FillRequestedPermission(
    google::protobuf::RepeatedPtrField<yandex::cloud::events::RequestedPermissions>* permissions,
    const TCloudEventInfo& info)
{
    auto* permission = permissions->Add();
    permission->set_permission("ydb.databases.alter");
    permission->set_resource_type("ydb.databases");
    TString resourceId = info.DatabaseId.empty() || info.TopicPath.StartsWith("/")
        ? info.DatabaseId + info.TopicPath
        : info.DatabaseId + "/" + info.TopicPath;
    permission->set_resource_id(resourceId);
    permission->set_authorized(true);
}

template <typename TEvent>
void FillCommonEventFields(TEvent& ev, const TCloudEventInfo& info) {
    // Authentication
    ev.mutable_authentication()->set_authenticated(true);
    ev.mutable_authentication()->set_subject_id(NormalizeSubjectId(info.UserSID));
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
    ev.mutable_request_metadata()->set_remote_address(NormalizeRemoteAddress(info.RemoteAddress));
}

template <typename TEvent, typename TFillBody>
void FillTopicEvent(TEvent& ev, const TCloudEventInfo& info, TFillBody&& fillBody) {
    FillCommonEventFields(ev, info);
    fillBody(ev);
}

template <typename TEvent>
void Fill(TEvent& ev, const TCloudEventInfo& info) {
    FillTopicEvent(ev, info, [&info](TEvent& event) {
        if (info.OperationStatus == NKikimrScheme::StatusSuccess) {
            event.set_event_status(EStatus::DONE);
        } else {
            event.set_event_status(EStatus::ERROR);
            auto* error = event.mutable_error();
            error->set_code(MapSchemeStatusToGoogleRpcCode(info.OperationStatus));
            error->set_message(info.Issue);
        }

        auto* details = event.mutable_details();
        details->set_path(info.TopicPath);

        if constexpr (std::is_same_v<TEvent, TAlterTopicEvent>) {
            const auto& desc = info.ModifyScheme.GetAlterPersQueueGroup();
            FillTopicDetails(details, desc);
            FillTopicRequestParameters(event.mutable_request_parameters(), info.TopicPath, desc);
        } else if constexpr (std::is_same_v<TEvent, TCreateTopicEvent>) {
            const auto& desc = info.ModifyScheme.GetCreatePersQueueGroup();
            FillTopicDetails(details, desc);
            FillTopicRequestParameters(event.mutable_request_parameters(), info.TopicPath, desc);
        } else if constexpr (std::is_same_v<TEvent, TDeleteTopicEvent>) {
            event.mutable_request_parameters()->set_path(info.TopicPath);
        }
    });
}

template<typename TEvent>
TString SerializeEventToProtobuf(const TEvent& ev) {
    TString data;
    if (!ev.SerializeToString(&data)) {
        LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE,
            "SerializeToString failed");
        return TString();
    }
    return data;
}

template<typename TEvent>
TString SerializeEventToJson(const TEvent& ev) {
    TString data;
    google::protobuf::util::JsonPrintOptions opts;
    opts.preserve_proto_field_names = true;
    opts.always_print_primitive_fields = true;
    if (!google::protobuf::util::MessageToJsonString(ev, &data, opts).ok()) {
        LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE,
            "MessageToJsonString failed");
        return TString();
    }
    return data;
}

template<typename TEvent>
TString SerializeEvent(const TEvent& ev, NKikimrPQ::TPQConfig_TCloudEventsConfig_EFormat format) {
    switch (format) {
        case NKikimrPQ::TPQConfig_TCloudEventsConfig_EFormat_PROTOBUF:
            return SerializeEventToProtobuf(ev);
        case NKikimrPQ::TPQConfig_TCloudEventsConfig_EFormat_JSON:
            return SerializeEventToJson(ev);
    }
    return SerializeEventToJson(ev);
}

NKikimrPQ::TPQConfig_TCloudEventsConfig_EFormat GetCloudEventsFormat() {
    return AppData()->PQConfig.GetCloudEventsConfig().GetFormat();
}

TString BuildTopicCloudEvent(const TCloudEventInfo& info) {
    TString data;
    const auto format = GetCloudEventsFormat();

    auto type = info.ModifyScheme.GetOperationType();
    if (type == NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup) {
        TCreateTopicEvent proto;
        Fill(proto, info);
        data = SerializeEvent(proto, format);
    } else if (type == NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup) {
        TAlterTopicEvent proto;
        Fill(proto, info);
        data = SerializeEvent(proto, format);
    } else if (type == NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup) {
        TDeleteTopicEvent proto;
        Fill(proto, info);
        data = SerializeEvent(proto, format);
    }

    return data;
}

} // anonymous namespace

TCloudEventsActor::TCloudEventsActor()
{
    const auto& pqConfig = AppData()->PQConfig;
    if (!pqConfig.HasCloudEventsConfig() || !pqConfig.GetCloudEventsConfig().GetEnabled()) {
        return;
    }

    const auto& cfg = pqConfig.GetCloudEventsConfig();

    if (cfg.HasFilePath()) {
        EventsWriter = MakeHolder<TFileEventsWriter>(cfg.GetFilePath());
    } else if (cfg.HasUaURI()) {
        auto counters = GetServiceCounters(AppData()->Counters.Get(), "pq.cloud_events");
        EventsWriter = MakeHolder<TUaEventsWriter>(cfg.GetUaURI(), counters);
    }
}

TCloudEventsActor::TCloudEventsActor(IEventsWriter::TPtr eventsWriter)
    : EventsWriter(std::move(eventsWriter))
{}

void TCloudEventsActor::Bootstrap() {
    Become(&TCloudEventsActor::StateWork);
}

void TCloudEventsActor::Handle(TCloudEvent::TPtr& ev) {
    if (!EventsWriter) {
        LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE,
            "No events writer configured");
        PassAway();
        return;
    }

    try {
        TString data = BuildTopicCloudEvent(ev.Get()->Get()->Info);
        if (data.empty()) {
            LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE,
                "Failed to build cloud event");
            PassAway();
            return;
        }
        EventsWriter->Write(data);
    } catch (const std::exception& e) {
        LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE,
            "Failed to write cloud event: " << e.what());
    }

    PassAway();
}

NActors::IActor* CreateCloudEventActor() {
    return new TCloudEventsActor();
}

} // namespace NKikimr::NPQ::NCloudEvents
