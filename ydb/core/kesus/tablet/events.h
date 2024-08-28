#pragma once

#include "defs.h"

#include <ydb/core/base/events.h>
#include <ydb/core/protos/kesus.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_coordination.pb.h>

namespace NKikimr {
namespace NKesus {

TString CanonizeQuoterResourcePath(const TVector<TString>& path);
TString CanonizeQuoterResourcePath(const TString& path);

struct TEvKesus {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_KESUS),

        // Requests and replies
        EvDummyRequest = EvBegin + 0,
        EvDummyResponse,
        EvSetConfig,
        EvSetConfigResult,
        EvGetConfig,
        EvGetConfigResult,
        EvDescribeProxies,
        EvDescribeProxiesResult,
        EvDescribeSessions,
        EvDescribeSessionsResult,
        EvDescribeSemaphore,
        EvDescribeSemaphoreResult,
        EvCreateSemaphore,
        EvCreateSemaphoreResult,
        EvUpdateSemaphore,
        EvUpdateSemaphoreResult,
        EvDeleteSemaphore,
        EvDeleteSemaphoreResult,
        Deprecated_EvCreateTask,
        Deprecated_EvCreateTaskResult,
        Deprecated_EvUpdateTask,
        Deprecated_EvUpdateTaskResult,
        Deprecated_EvDeleteTask,
        Deprecated_EvDeleteTaskResult,
        EvRegisterProxy,
        EvRegisterProxyResult,
        EvUnregisterProxy,
        EvUnregisterProxyResult,
        EvAttachSession,
        EvAttachSessionResult,
        EvDetachSession,
        EvDetachSessionResult,
        EvDestroySession,
        EvDestroySessionResult,
        EvAcquireSemaphore,
        EvAcquireSemaphorePending,
        EvAcquireSemaphoreResult,
        EvReleaseSemaphore,
        EvReleaseSemaphoreResult,

        // Notifications
        EvProxyExpired = EvBegin + 512,
        EvSessionExpired,
        EvSessionStolen,
        Deprecated_EvClientReady,
        Deprecated_EvJobStatus,
        Deprecated_EvJobStart,
        Deprecated_EvJobStop,
        EvDescribeSemaphoreChanged,

        // Quoter API
        // Control API
        EvDescribeQuoterResources = EvBegin + 1024,
        EvDescribeQuoterResourcesResult,
        EvAddQuoterResource,
        EvAddQuoterResourceResult,
        EvUpdateQuoterResource,
        EvUpdateQuoterResourceResult,
        EvDeleteQuoterResource,
        EvDeleteQuoterResourceResult,
        // Runtime API
        EvSubscribeOnResources = EvBegin + 1024 + 512,
        EvSubscribeOnResourcesResult,
        EvUpdateConsumptionState,
        EvUpdateConsumptionStateAck,
        EvResourcesAllocated,
        EvResourcesAllocatedAck,
        EvProxyResourceConsumptionStatistics,
        EvAggregatedResourceConsumptionStatistics,
        EvGetQuoterResourceCounters,
        EvGetQuoterResourceCountersResult,
        EvAccountResources,
        EvAccountResourcesAck,
        EvReportResources,
        EvReportResourcesAck,
        EvSyncResources,
        EvSyncResourcesAck,

        EvEnd
    };

    static_assert(EvEnd <= EventSpaceEnd(TKikimrEvents::ES_KESUS),
        "expected EvEnd <= EventSpaceEnd(TKikimrEvents::ES_KESUS)");

    inline static void FillError(NKikimrKesus::TKesusError* error, Ydb::StatusIds::StatusCode status, const TString& reason) {
        Y_ABORT_UNLESS(status != Ydb::StatusIds::SUCCESS, "Attempting to set error to SUCCESS");
        error->SetStatus(status);
        error->AddIssues()->set_message(reason);
    }

    template<class TDerived, class TRecord, ui32 Event>
    struct TResultBase : public TEventPB<TDerived, TRecord, Event> {
        TResultBase() = default;

        explicit TResultBase(ui64 generation) {
            this->Record.SetProxyGeneration(generation);
            this->Record.MutableError()->SetStatus(Ydb::StatusIds::SUCCESS);
        }

        TResultBase(ui64 generation, Ydb::StatusIds::StatusCode status, const TString& reason)
            : TResultBase(generation)
        {
            FillError(this->Record.MutableError(), status, reason);
        }
    };

    struct TEvDummyRequest : public TEventPB<TEvDummyRequest, NKikimrKesus::TEvDummyRequest, EvDummyRequest> {
        TEvDummyRequest() = default;

        explicit TEvDummyRequest(bool useTransactions) {
            Record.SetUseTransactions(useTransactions);
        }
    };

    struct TEvDummyResponse : public TEventPB<TEvDummyResponse, NKikimrKesus::TEvDummyResponse, EvDummyResponse> {
        TEvDummyResponse() = default;
    };

    struct TEvSetConfig : public TEventPB<TEvSetConfig, NKikimrKesus::TEvSetConfig, EvSetConfig> {
        TEvSetConfig() = default;

        TEvSetConfig(ui64 txId, const Ydb::Coordination::Config& config, ui64 version) {
            Record.SetTxId(txId);
            Record.MutableConfig()->CopyFrom(config);
            Record.SetVersion(version);
        }

    };

    struct TEvSetConfigResult : public TEventPB<TEvSetConfigResult, NKikimrKesus::TEvSetConfigResult, EvSetConfigResult> {
        TEvSetConfigResult() = default;

        TEvSetConfigResult(ui64 txId, ui64 tabletId) {
            Record.SetTxId(txId);
            Record.SetTabletId(tabletId);
            Record.MutableError()->SetStatus(Ydb::StatusIds::SUCCESS);
        }

        void SetError(Ydb::StatusIds::StatusCode status, const TString& reason) {
            FillError(Record.MutableError(), status, reason);
        }
    };

    struct TEvGetConfig : public TEventPB<TEvGetConfig, NKikimrKesus::TEvGetConfig, EvGetConfig> {
        TEvGetConfig() = default;
    };

    struct TEvGetConfigResult : public TEventPB<TEvGetConfigResult, NKikimrKesus::TEvGetConfigResult, EvGetConfigResult> {
        TEvGetConfigResult() = default;
    };

    struct TEvDescribeProxies : public TEventPB<TEvDescribeProxies, NKikimrKesus::TEvDescribeProxies, EvDescribeProxies> {
        TEvDescribeProxies() = default;

        explicit TEvDescribeProxies(const TString& kesusPath) {
            Record.SetKesusPath(kesusPath);
        }
    };

    struct TEvDescribeProxiesResult : public TEventPB<TEvDescribeProxiesResult, NKikimrKesus::TEvDescribeProxiesResult, EvDescribeProxiesResult> {
        TEvDescribeProxiesResult() = default;
    };

    struct TEvDescribeSessions : public TEventPB<TEvDescribeSessions, NKikimrKesus::TEvDescribeSessions, EvDescribeSessions> {
        TEvDescribeSessions() = default;

        explicit TEvDescribeSessions(const TString& kesusPath) {
            Record.SetKesusPath(kesusPath);
        }
    };

    struct TEvDescribeSessionsResult : public TEventPB<TEvDescribeSessionsResult, NKikimrKesus::TEvDescribeSessionsResult, EvDescribeSessionsResult> {
        TEvDescribeSessionsResult() = default;
    };

    struct TEvDescribeSemaphore : public TEventPB<TEvDescribeSemaphore, NKikimrKesus::TEvDescribeSemaphore, EvDescribeSemaphore> {
        TEvDescribeSemaphore() = default;

        explicit TEvDescribeSemaphore(const TString& kesusPath, const TString& name) {
            Record.SetKesusPath(kesusPath);
            Record.SetName(name);
        }
    };

    struct TEvDescribeSemaphoreResult : public TResultBase<TEvDescribeSemaphoreResult, NKikimrKesus::TEvDescribeSemaphoreResult, EvDescribeSemaphoreResult> {
        using TResultBase::TResultBase;
    };

    struct TEvCreateSemaphore : public TEventPB<TEvCreateSemaphore, NKikimrKesus::TEvCreateSemaphore, EvCreateSemaphore> {
        TEvCreateSemaphore() = default;

        TEvCreateSemaphore(const TString& kesusPath, const TString& name, ui64 limit, const TString& data = TString()) {
            Record.SetKesusPath(kesusPath);
            Record.SetName(name);
            Record.SetLimit(limit);
            Record.SetData(data);
        }
    };

    struct TEvCreateSemaphoreResult : public TResultBase<TEvCreateSemaphoreResult, NKikimrKesus::TEvCreateSemaphoreResult, EvCreateSemaphoreResult> {
        using TResultBase::TResultBase;
    };

    struct TEvUpdateSemaphore : public TEventPB<TEvUpdateSemaphore, NKikimrKesus::TEvUpdateSemaphore, EvUpdateSemaphore> {
        TEvUpdateSemaphore() = default;

        TEvUpdateSemaphore(const TString& kesusPath, const TString& name, const TString& data) {
            Record.SetKesusPath(kesusPath);
            Record.SetName(name);
            Record.SetData(data);
        }
    };

    struct TEvUpdateSemaphoreResult : public TResultBase<TEvUpdateSemaphoreResult, NKikimrKesus::TEvUpdateSemaphoreResult, EvUpdateSemaphoreResult> {
        using TResultBase::TResultBase;
    };

    struct TEvDeleteSemaphore : public TEventPB<TEvDeleteSemaphore, NKikimrKesus::TEvDeleteSemaphore, EvDeleteSemaphore> {
        TEvDeleteSemaphore() = default;

        explicit TEvDeleteSemaphore(const TString& kesusPath, const TString& name, bool force = false) {
            Record.SetKesusPath(kesusPath);
            Record.SetName(name);
            Record.SetForce(force);
        }
    };

    struct TEvDeleteSemaphoreResult : public TResultBase<TEvDeleteSemaphoreResult, NKikimrKesus::TEvDeleteSemaphoreResult, EvDeleteSemaphoreResult> {
        using TResultBase::TResultBase;
    };

    struct TEvRegisterProxy : public TEventPB<TEvRegisterProxy, NKikimrKesus::TEvRegisterProxy, EvRegisterProxy> {
        TEvRegisterProxy() = default;

        TEvRegisterProxy(const TString& kesusPath, ui64 generation) {
            Record.SetKesusPath(kesusPath);
            Record.SetProxyGeneration(generation);
        }
    };

    struct TEvRegisterProxyResult : public TResultBase<TEvRegisterProxyResult, NKikimrKesus::TEvRegisterProxyResult, EvRegisterProxyResult> {
        using TResultBase::TResultBase;
    };

    struct TEvUnregisterProxy : public TEventPB<TEvUnregisterProxy, NKikimrKesus::TEvUnregisterProxy, EvUnregisterProxy> {
        TEvUnregisterProxy() = default;

        TEvUnregisterProxy(const TString& kesusPath, ui64 generation) {
            Record.SetKesusPath(kesusPath);
            Record.SetProxyGeneration(generation);
        }
    };

    struct TEvUnregisterProxyResult : public TResultBase<TEvUnregisterProxyResult, NKikimrKesus::TEvUnregisterProxyResult, EvUnregisterProxyResult> {
        using TResultBase::TResultBase;
    };

    struct TEvAttachSession : public TEventPB<TEvAttachSession, NKikimrKesus::TEvAttachSession, EvAttachSession> {
        TEvAttachSession() = default;

        TEvAttachSession(
                const TString& kesusPath,
                ui64 generation,
                ui64 sessionId,
                ui64 timeoutMillis,
                const TString& description,
                ui64 seqNo = 0,
                const TString& protectionKey = TString())
        {
            Record.SetKesusPath(kesusPath);
            Record.SetProxyGeneration(generation);
            Record.SetSessionId(sessionId);
            Record.SetTimeoutMillis(timeoutMillis);
            Record.SetDescription(description);
            Record.SetSeqNo(seqNo);
            Record.SetProtectionKey(protectionKey);
        }
    };

    struct TEvAttachSessionResult : public TResultBase<TEvAttachSessionResult, NKikimrKesus::TEvAttachSessionResult, EvAttachSessionResult> {
        TEvAttachSessionResult() = default;

        TEvAttachSessionResult(ui64 generation, ui64 sessionId)
            : TResultBase(generation)
        {
            Record.SetSessionId(sessionId);
        }

        TEvAttachSessionResult(ui64 generation, ui64 sessionId, Ydb::StatusIds::StatusCode status, const TString& reason)
            : TResultBase(generation, status, reason)
        {
            Record.SetSessionId(sessionId);
        }
    };

    struct TEvDetachSession : public TEventPB<TEvDetachSession, NKikimrKesus::TEvDetachSession, EvDetachSession> {
        TEvDetachSession() = default;

        TEvDetachSession(const TString& kesusPath, ui64 generation, ui64 sessionId) {
            Record.SetKesusPath(kesusPath);
            Record.SetProxyGeneration(generation);
            Record.SetSessionId(sessionId);
        }
    };

    struct TEvDetachSessionResult : public TResultBase<TEvDetachSessionResult, NKikimrKesus::TEvDetachSessionResult, EvDetachSessionResult> {
        using TResultBase::TResultBase;
    };

    struct TEvDestroySession : public TEventPB<TEvDestroySession, NKikimrKesus::TEvDestroySession, EvDestroySession> {
        TEvDestroySession() = default;

        TEvDestroySession(const TString& kesusPath, ui64 generation, ui64 sessionId) {
            Record.SetKesusPath(kesusPath);
            Record.SetProxyGeneration(generation);
            Record.SetSessionId(sessionId);
        }
    };

    struct TEvDestroySessionResult : public TResultBase<TEvDestroySessionResult, NKikimrKesus::TEvDestroySessionResult, EvDestroySessionResult> {
        using TResultBase::TResultBase;
    };

    struct TEvAcquireSemaphore : public TEventPB<TEvAcquireSemaphore, NKikimrKesus::TEvAcquireSemaphore, EvAcquireSemaphore> {
        TEvAcquireSemaphore() = default;

        TEvAcquireSemaphore(
                const TString& kesusPath, ui64 generation, ui64 sessionId, const TString& name, ui64 count,
                ui64 timeoutMillis = 0, const TString& data = TString(), bool ephemeral = false)
        {
            Record.SetKesusPath(kesusPath);
            Record.SetProxyGeneration(generation);
            Record.SetSessionId(sessionId);
            Record.SetName(name);
            Record.SetCount(count);
            Record.SetTimeoutMillis(timeoutMillis);
            Record.SetData(data);
            Record.SetEphemeral(ephemeral);
        }
    };

    struct TEvAcquireSemaphorePending : public TEventPB<TEvAcquireSemaphorePending, NKikimrKesus::TEvAcquireSemaphorePending, EvAcquireSemaphorePending> {
        TEvAcquireSemaphorePending() = default;

        explicit TEvAcquireSemaphorePending(ui64 generation) {
            Record.SetProxyGeneration(generation);
        }
    };

    struct TEvAcquireSemaphoreResult : public TResultBase<TEvAcquireSemaphoreResult, NKikimrKesus::TEvAcquireSemaphoreResult, EvAcquireSemaphoreResult> {
        TEvAcquireSemaphoreResult() = default;

        explicit TEvAcquireSemaphoreResult(ui64 generation, bool acquired = true)
            : TResultBase(generation)
        {
            Record.SetAcquired(acquired);
        }

        TEvAcquireSemaphoreResult(ui64 generation, Ydb::StatusIds::StatusCode status, const TString& reason)
            : TResultBase(generation, status, reason)
        {
        }
    };

    struct TEvReleaseSemaphore : public TEventPB<TEvReleaseSemaphore, NKikimrKesus::TEvReleaseSemaphore, EvReleaseSemaphore> {
        TEvReleaseSemaphore() = default;

        TEvReleaseSemaphore(const TString& kesusPath, ui64 generation, ui64 sessionId, const TString& name) {
            Record.SetKesusPath(kesusPath);
            Record.SetProxyGeneration(generation);
            Record.SetSessionId(sessionId);
            Record.SetName(name);
        }
    };

    struct TEvReleaseSemaphoreResult : public TResultBase<TEvReleaseSemaphoreResult, NKikimrKesus::TEvReleaseSemaphoreResult, EvReleaseSemaphoreResult> {
        TEvReleaseSemaphoreResult() = default;

        explicit TEvReleaseSemaphoreResult(ui64 generation, bool released = true)
            : TResultBase(generation)
        {
            Record.SetReleased(released);
        }

        TEvReleaseSemaphoreResult(ui64 generation, Ydb::StatusIds::StatusCode status, const TString& reason)
            : TResultBase(generation, status, reason)
        {
        }
    };

    struct TEvProxyExpired : public TEventPB<TEvProxyExpired, NKikimrKesus::TEvProxyExpired, EvProxyExpired> {
        TEvProxyExpired() = default;
    };

    struct TEvSessionExpired : public TEventPB<TEvSessionExpired, NKikimrKesus::TEvSessionExpired, EvSessionExpired> {
        TEvSessionExpired() = default;

        explicit TEvSessionExpired(ui64 sessionId) {
            Record.SetSessionId(sessionId);
        }
    };

    struct TEvSessionStolen : public TEventPB<TEvSessionStolen, NKikimrKesus::TEvSessionStolen, EvSessionStolen> {
        TEvSessionStolen() = default;

        TEvSessionStolen(ui64 generation, ui64 sessionId) {
            Record.SetProxyGeneration(generation);
            Record.SetSessionId(sessionId);
        }
    };

    struct TEvDescribeSemaphoreChanged : public TEventPB<TEvDescribeSemaphoreChanged, NKikimrKesus::TEvDescribeSemaphoreChanged, EvDescribeSemaphoreChanged> {
        TEvDescribeSemaphoreChanged() = default;

        explicit TEvDescribeSemaphoreChanged(ui64 generation, bool dataChanged, bool ownersChanged) {
            Record.SetProxyGeneration(generation);
            Record.SetDataChanged(dataChanged);
            Record.SetOwnersChanged(ownersChanged);
        }
    };

    // Quoter API

    struct TEvDescribeQuoterResources : public TEventPB<TEvDescribeQuoterResources, NKikimrKesus::TEvDescribeQuoterResources, EvDescribeQuoterResources> {
        using TBaseEvent = TEventPB<TEvDescribeQuoterResources, NKikimrKesus::TEvDescribeQuoterResources, EvDescribeQuoterResources>;
        using TBaseEvent::TBaseEvent;

    };

    struct TEvDescribeQuoterResourcesResult : public TEventPB<TEvDescribeQuoterResourcesResult, NKikimrKesus::TEvDescribeQuoterResourcesResult, EvDescribeQuoterResourcesResult> {
        using TBaseEvent = TEventPB<TEvDescribeQuoterResourcesResult, NKikimrKesus::TEvDescribeQuoterResourcesResult, EvDescribeQuoterResourcesResult>;
        using TBaseEvent::TBaseEvent;

        TEvDescribeQuoterResourcesResult() = default;

        TEvDescribeQuoterResourcesResult(Ydb::StatusIds::StatusCode status, const TString& reason) {
            FillError(Record.MutableError(), status, reason);
        }
    };

    struct TEvAddQuoterResource : public TEventPB<TEvAddQuoterResource, NKikimrKesus::TEvAddQuoterResource, EvAddQuoterResource> {
        using TBaseEvent = TEventPB<TEvAddQuoterResource, NKikimrKesus::TEvAddQuoterResource, EvAddQuoterResource>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvAddQuoterResourceResult : public TEventPB<TEvAddQuoterResourceResult, NKikimrKesus::TEvAddQuoterResourceResult, EvAddQuoterResourceResult> {
        using TBaseEvent = TEventPB<TEvAddQuoterResourceResult, NKikimrKesus::TEvAddQuoterResourceResult, EvAddQuoterResourceResult>;
        using TBaseEvent::TBaseEvent;

        TEvAddQuoterResourceResult(Ydb::StatusIds::StatusCode status, const TString& reason) {
            FillError(Record.MutableError(), status, reason);
        }
    };

    struct TEvUpdateQuoterResource : public TEventPB<TEvUpdateQuoterResource, NKikimrKesus::TEvUpdateQuoterResource, EvUpdateQuoterResource> {
        using TBaseEvent = TEventPB<TEvUpdateQuoterResource, NKikimrKesus::TEvUpdateQuoterResource, EvUpdateQuoterResource>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvUpdateQuoterResourceResult : public TEventPB<TEvUpdateQuoterResourceResult, NKikimrKesus::TEvUpdateQuoterResourceResult, EvUpdateQuoterResourceResult> {
        using TBaseEvent = TEventPB<TEvUpdateQuoterResourceResult, NKikimrKesus::TEvUpdateQuoterResourceResult, EvUpdateQuoterResourceResult>;
        using TBaseEvent::TBaseEvent;

        TEvUpdateQuoterResourceResult() = default;

        TEvUpdateQuoterResourceResult(Ydb::StatusIds::StatusCode status, const TString& reason) {
            FillError(Record.MutableError(), status, reason);
        }
    };

    struct TEvDeleteQuoterResource : public TEventPB<TEvDeleteQuoterResource, NKikimrKesus::TEvDeleteQuoterResource, EvDeleteQuoterResource> {       
        using TBaseEvent = TEventPB<TEvDeleteQuoterResource, NKikimrKesus::TEvDeleteQuoterResource, EvDeleteQuoterResource>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvDeleteQuoterResourceResult : public TEventPB<TEvDeleteQuoterResourceResult, NKikimrKesus::TEvDeleteQuoterResourceResult, EvDeleteQuoterResourceResult> {
        using TBaseEvent = TEventPB<TEvDeleteQuoterResourceResult, NKikimrKesus::TEvDeleteQuoterResourceResult, EvDeleteQuoterResourceResult>;
        using TBaseEvent::TBaseEvent;        
        
        TEvDeleteQuoterResourceResult() = default;

        TEvDeleteQuoterResourceResult(Ydb::StatusIds::StatusCode status, const TString& reason) {
            FillError(Record.MutableError(), status, reason);
        }
    };

    struct TEvSubscribeOnResources : public TEventPBWithArena<TEvSubscribeOnResources, NKikimrKesus::TEvSubscribeOnResources, EvSubscribeOnResources> {
        using TBaseEvent = TEventPBWithArena<TEvSubscribeOnResources, NKikimrKesus::TEvSubscribeOnResources, EvSubscribeOnResources>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvSubscribeOnResourcesResult : public TEventPBWithArena<TEvSubscribeOnResourcesResult, NKikimrKesus::TEvSubscribeOnResourcesResult, EvSubscribeOnResourcesResult> {
        using TBaseEvent = TEventPBWithArena<TEvSubscribeOnResourcesResult, NKikimrKesus::TEvSubscribeOnResourcesResult, EvSubscribeOnResourcesResult>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvUpdateConsumptionState : public TEventPBWithArena<TEvUpdateConsumptionState, NKikimrKesus::TEvUpdateConsumptionState, EvUpdateConsumptionState> {
        using TBaseEvent = TEventPBWithArena<TEvUpdateConsumptionState, NKikimrKesus::TEvUpdateConsumptionState, EvUpdateConsumptionState>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvUpdateConsumptionStateAck : public TEventPBWithArena<TEvUpdateConsumptionStateAck, NKikimrKesus::TEvUpdateConsumptionStateAck, EvUpdateConsumptionStateAck> {
        using TBaseEvent = TEventPBWithArena<TEvUpdateConsumptionStateAck, NKikimrKesus::TEvUpdateConsumptionStateAck, EvUpdateConsumptionStateAck>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvResourcesAllocated : public TEventPBWithArena<TEvResourcesAllocated, NKikimrKesus::TEvResourcesAllocated, EvResourcesAllocated> {
        using TBaseEvent = TEventPBWithArena<TEvResourcesAllocated, NKikimrKesus::TEvResourcesAllocated, EvResourcesAllocated>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvResourcesAllocatedAck : public TEventPBWithArena<TEvResourcesAllocatedAck, NKikimrKesus::TEvResourcesAllocatedAck, EvResourcesAllocatedAck> {
        using TBaseEvent = TEventPBWithArena<TEvResourcesAllocatedAck, NKikimrKesus::TEvResourcesAllocatedAck, EvResourcesAllocatedAck>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvGetQuoterResourceCounters : public TEventPBWithArena<TEvGetQuoterResourceCounters, NKikimrKesus::TEvGetQuoterResourceCounters, EvGetQuoterResourceCounters> {
        using TBaseEvent = TEventPBWithArena<TEvGetQuoterResourceCounters, NKikimrKesus::TEvGetQuoterResourceCounters, EvGetQuoterResourceCounters>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvGetQuoterResourceCountersResult : public TEventPBWithArena<TEvGetQuoterResourceCountersResult, NKikimrKesus::TEvGetQuoterResourceCountersResult, EvGetQuoterResourceCountersResult> {
        using TBaseEvent = TEventPBWithArena<TEvGetQuoterResourceCountersResult, NKikimrKesus::TEvGetQuoterResourceCountersResult, EvGetQuoterResourceCountersResult>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvAccountResources : public TEventPBWithArena<TEvAccountResources, NKikimrKesus::TEvAccountResources, EvAccountResources> {
        using TBaseEvent = TEventPBWithArena<TEvAccountResources, NKikimrKesus::TEvAccountResources, EvAccountResources>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvAccountResourcesAck : public TEventPBWithArena<TEvAccountResourcesAck, NKikimrKesus::TEvAccountResourcesAck, EvAccountResourcesAck> {
        using TBaseEvent = TEventPBWithArena<TEvAccountResourcesAck, NKikimrKesus::TEvAccountResourcesAck, EvAccountResourcesAck>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvReportResources : public TEventPBWithArena<TEvReportResources, NKikimrKesus::TEvReportResources, EvReportResources> {
        using TBaseEvent = TEventPBWithArena<TEvReportResources, NKikimrKesus::TEvReportResources, EvReportResources>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvReportResourcesAck : public TEventPBWithArena<TEvReportResourcesAck, NKikimrKesus::TEvReportResourcesAck, EvReportResourcesAck> {
        using TBaseEvent = TEventPBWithArena<TEvReportResourcesAck, NKikimrKesus::TEvReportResourcesAck, EvReportResourcesAck>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvSyncResources : public TEventPBWithArena<TEvSyncResources, NKikimrKesus::TEvSyncResources, EvSyncResources> {
        using TBaseEvent = TEventPBWithArena<TEvSyncResources, NKikimrKesus::TEvSyncResources, EvSyncResources>;
        using TBaseEvent::TBaseEvent;
    };

    struct TEvSyncResourcesAck : public TEventPBWithArena<TEvSyncResourcesAck, NKikimrKesus::TEvSyncResourcesAck, EvSyncResourcesAck> {
        using TBaseEvent = TEventPBWithArena<TEvSyncResourcesAck, NKikimrKesus::TEvSyncResourcesAck, EvSyncResourcesAck>;
        using TBaseEvent::TBaseEvent;
    };
};

}
}