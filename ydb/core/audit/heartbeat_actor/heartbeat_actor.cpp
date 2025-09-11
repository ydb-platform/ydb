#include "heartbeat_actor.h"

#include <ydb/core/audit/audit_config/audit_config.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <ydb/core/audit/audit_log.h>

namespace NKikimr::NAudit {

namespace {

class THeartbeatActor : public NActors::TActorBootstrapped<THeartbeatActor> {
public:
    explicit THeartbeatActor(const TAuditConfig& auditConfig)
        : HeartbeatInterval(GetInterval(auditConfig))
        , EnableLogging(auditConfig.EnableLogging(NKikimrConfig::TAuditConfig::TLogClassConfig::AuditHeartbeat, NKikimrConfig::TAuditConfig::TLogClassConfig::Completed, NACLibProto::SUBJECT_TYPE_ANONYMOUS))
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
        AUDIT_LOG(
            AUDIT_PART("component", "audit")
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
};

}

std::unique_ptr<NActors::IActor> CreateHeartbeatActor(const TAuditConfig& auditConfig) {
    return std::make_unique<THeartbeatActor>(auditConfig);
}

} // namespace NKikimr::NAudit
