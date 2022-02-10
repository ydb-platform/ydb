#pragma once

#include "events.h"
#include "proxy.h"
#include "proxy_actor.h"

#include <ydb/core/kesus/tablet/tablet.h>
#include <ydb/core/kesus/tablet/events.h>

#include <ydb/core/erasure/erasure.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NKesus {

struct TTestContext {
    TTabletTypes::EType TabletType;
    ui64 TabletId;
    THolder<TTestActorRuntime> Runtime;
    THashMap<ui64, TActorId> ProxyActors;

    TTestContext();

    void Setup(ui32 nodeCount = 2);
    void Finalize();

    virtual void SetupLogging();
    virtual void SetupTabletServices();

    // Sleeps for millis milliseconds using fake time
    void Sleep(ui64 millis);

    // Doesn't sleep unlike NKikikmr::RebootTablet
    void RebootTablet();

    // Extremely pedantic version of GrabEdgeEvent
    template<class TEvent>
    THolder<TEvent> ExpectEdgeEvent(const TActorId& actor) {
        return Runtime->GrabEdgeEvent<TEvent>(actor)->Release();
    }

    // Extremely pedantic version of GrabEdgeEvent
    template<class TEvent>
    THolder<TEvent> ExpectEdgeEvent(const TActorId& actor, ui64 cookie) {
        return Runtime->GrabEdgeEventIf<TEvent>(actor, [=](const auto& ev) {
            return ev->Cookie == cookie;
        })->Release();
    }

    // Dynamically allocates a proxy
    TActorId GetProxy(ui64 proxyId, ui32 nodeIndex = 1);

    // Sends payload to proxy from edge
    void SendFromEdge(ui64 proxyId, const TActorId& edge, IEventBase* payload, ui64 cookie = 0);

    // Expects TEvProxyError at the edge with the specified cookie and status
    void ExpectProxyError(const TActorId& edge, ui64 cookie, Ydb::StatusIds::StatusCode status);

    void CreateSemaphore(ui64 proxyId, const TString& name, ui64 limit, const TString& data,
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    void CreateSemaphore(ui64 proxyId, const TString& name, ui64 limit, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        CreateSemaphore(proxyId, name, limit, TString(), status);
    }

    void DeleteSemaphore(ui64 proxyId, const TString& name, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    // Returns a cookie used for the session
    ui64 SendAttachSession(
        ui64 proxyId, const TActorId& edge, ui64 sessionId, ui64 timeoutMillis = 0,
        const TString& description = TString(), ui64 seqNo = 0);

    // Returns sessionId from the result
    ui64 ExpectAttachSessionResult(const TActorId& edge, ui64 cookie, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    // Describes and verifies info about semaphores

    struct TSimpleSemaphoreDescription {
        ui64 Limit;
        TString Data;
        bool Ephemeral;
        THashMap<ui64, ui64> Owners;
        THashMap<ui64, ui64> Waiters;
    };

    TSimpleSemaphoreDescription DescribeSemaphore(ui64 proxyId, const TString& name, bool includeWaiters = false);
    void VerifySemaphoreNotFound(ui64 proxyId, const TString& name);
    void VerifySemaphoreParams(ui64 proxyId, const TString& name, ui64 limit, const TString& data = TString(), bool ephemeral = false);
    void VerifySemaphoreOwners(ui64 proxyId, const TString& name, const THashSet<ui64>& sessionIds, bool ephemeral = false);
    void VerifySemaphoreWaiters(ui64 proxyId, const TString& name, const THashSet<ui64>& sessionIds, bool ephemeral = false);
};

}
}
