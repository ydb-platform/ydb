#include "heartbeat_actor.h"

#include <ydb/core/audit/audit_config/audit_config.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/address/interconnect_address.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <ydb/core/audit/audit_log.h>

namespace NKikimr::NAudit {

namespace {

class THeartbeatActor : public NActors::TActorBootstrapped<THeartbeatActor> {
public:
    explicit THeartbeatActor(const TAuditConfig& auditConfig, TNetworkInterfacesProvider networkInterfacesProvider)
        : HeartbeatInterval(GetInterval(auditConfig))
        , EnableLogging(auditConfig.EnableLogging(NKikimrConfig::TAuditConfig::TLogClassConfig::AuditHeartbeat, NKikimrConfig::TAuditConfig::TLogClassConfig::Completed, NACLibProto::SUBJECT_TYPE_ANONYMOUS))
        , NetworkInterfacesProvider(std::move(networkInterfacesProvider))
    {
    }

    static TDuration GetInterval(const TAuditConfig& auditConfig) {
        return TDuration::Seconds(auditConfig.GetHeartbeat().GetIntervalSeconds());
    }

    void Bootstrap() {
        Y_VERIFY(HeartbeatInterval);
        if (!EnableLogging) {
            PassAway();
            return;
        }

        Become(&THeartbeatActor::StateFunc);
        PerformHeartbeat();
    }

    TString GetNodeAddress() const {
        TString ipv6;
        for (const auto& interface : NetworkInterfacesProvider()) {
            if (!interface.Address || NAddr::IsLoopback(*interface.Address)) {
                continue;
            }
            const NInterconnect::TAddress address(*interface.Address);
            if (address.IsLinkLocal()) {
                continue;
            }
            const TString ip = address.GetAddress();
            if (ip.empty() || ip == "0.0.0.0" || ip == "::") {
                continue;
            }
            if (address.GetFamily() == AF_INET) {
                return ip;
            }
            if (ipv6.empty()) {
                ipv6 = ip;
            }
        }
        return ipv6.empty() ? TString("{none}") : ipv6;
    }

    void HeartbeatLog() {
        const TString nodeAddress = GetNodeAddress();
        AUDIT_LOG(
            AUDIT_PART("component", "audit-service")
            AUDIT_PART("remote_address", nodeAddress)
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
    const TNetworkInterfacesProvider NetworkInterfacesProvider;
};

}

std::unique_ptr<NActors::IActor> CreateHeartbeatActor(const TAuditConfig& auditConfig,
        TNetworkInterfacesProvider networkInterfacesProvider) {
    return std::make_unique<THeartbeatActor>(auditConfig, std::move(networkInterfacesProvider));
}

} // namespace NKikimr::NAudit
