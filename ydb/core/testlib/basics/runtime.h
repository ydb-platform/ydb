#pragma once

#include <ydb/core/audit/audit_log_service.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NActors {

    class TTestBasicRuntime : public TTestActorRuntime {
    public:
        using TTestActorRuntime::TTestActorRuntime;

        using TNodeLocationCallback = std::function<TNodeLocation(ui32)>;
        TNodeLocationCallback LocationCallback;

        NKikimr::NAudit::TAuditLogBackends AuditLogBackends;

        ~TTestBasicRuntime();

        void Initialize(TEgg) override;

        void AddICStuff();
        void AddAuditLogStuff();
    };
}
