#include "heartbeat_actor.h"

#include <ydb/core/audit/audit_config/audit_config.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <ydb/core/audit/audit_log.h>

#include <util/network/socket.h>

namespace NKikimr::NAudit {

namespace {

class THeartbeatActor : public NActors::TActorBootstrapped<THeartbeatActor> {
public:
    THeartbeatActor(const TAuditConfig& auditConfig, TString nodeAddress)
        : HeartbeatInterval(GetInterval(auditConfig))
        , EnableLogging(auditConfig.EnableLogging(NKikimrConfig::TAuditConfig::TLogClassConfig::AuditHeartbeat, NKikimrConfig::TAuditConfig::TLogClassConfig::Completed, NACLibProto::SUBJECT_TYPE_ANONYMOUS))
        , NodeAddress(std::move(nodeAddress))
    {
    }

    static TDuration GetInterval(const TAuditConfig& auditConfig) {
        return TDuration::Seconds(auditConfig.GetHeartbeat().GetIntervalSeconds());
    }

    void Bootstrap() {
        Become(&THeartbeatActor::StateFunc);
        Y_VERIFY(HeartbeatInterval);
        if (EnableLogging) {
            PerformHeartbeat();
        } else {
            PassAway();
        }
    }

    void HeartbeatLog() {
        static const TString EmptyValue = "{none}";
        AUDIT_LOG(
            AUDIT_PART("component", "audit-service")
            AUDIT_PART("remote_address", NodeAddress.empty() ? EmptyValue : NodeAddress)
            AUDIT_PART("subject", "metadata@system")
            AUDIT_PART("sanitized_token", "{none}")
            AUDIT_PART("operation", "HEARTBEAT")
            AUDIT_PART("status", "SUCCESS")
            AUDIT_PART("node_id", ToString(SelfId().NodeId()))
        );
    }

    void ScheduleNextEvent() {
        Schedule(HeartbeatInterval, new NActors::TEvents::TEvWakeup());
    }

    void PerformHeartbeat() {
        HeartbeatLog();
        ScheduleNextEvent();
    }

    STRICT_STFUNC(StateFunc,
        sFunc(NActors::TEvents::TEvWakeup, PerformHeartbeat)
    )

private:
    const TDuration HeartbeatInterval;
    const bool EnableLogging;
    const TString NodeAddress;
};

}

TString GetHeartbeatNodeAddress(const NKikimrConfig::TAppConfig& config, ui32 nodeId) {
    TString address;
    if (const auto& dynamicConfig = config.GetDynamicNodeConfig(); dynamicConfig.HasNodeInfo()) {
        address = dynamicConfig.GetNodeInfo().GetAddress();
    } else {
        for (const auto& node : config.GetNameserviceConfig().GetNode()) {
            if (node.GetNodeId() == nodeId) {
                address = node.GetAddress();
                break;
            }
        }
    }

    // A hostname or a wildcard listener address does not identify the source IP.
    in_addr ipv4;
    if (inet_pton(AF_INET, address.c_str(), &ipv4) == 1 && ipv4.s_addr != INADDR_ANY) {
        return address;
    }
    in6_addr ipv6;
    if (inet_pton(AF_INET6, address.c_str(), &ipv6) == 1 && !IN6_IS_ADDR_UNSPECIFIED(&ipv6)) {
        return address;
    }
    return {};
}

std::unique_ptr<NActors::IActor> CreateHeartbeatActor(const TAuditConfig& auditConfig, TString nodeAddress) {
    return std::make_unique<THeartbeatActor>(auditConfig, std::move(nodeAddress));
}

} // namespace NKikimr::NAudit
