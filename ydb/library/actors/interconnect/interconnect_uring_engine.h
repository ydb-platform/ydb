#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/util/rc_buf.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include "events/events.h"
#include "interconnect_stream.h"
#include "types.h"

#include <memory>
#include <vector>

namespace NActors {

    class TActorSystem;
    class IReceiveCallback;
    struct TInterconnectProxyCommon;

    class IUringEngine : public TThrRefBase {
    public:
        // Binds the engine to the actor system and arranges (via DeferPreStop) for the reaper threads to be
        // stopped before the actor system is torn down. The engine is created before the actor system exists
        // (see CreateUringEngine), so this must be called exactly once it is available, before any Register.
        // Idempotent and thread-safe.
        virtual void SetActorSystem(TActorSystem* actorSystem) = 0;

        // Registers a connected socket, arms receiving, and returns an opaque non-zero handle (0 on
        // failure). The engine holds a reference to the socket until Unregister fully drains. Thread-safe.
        virtual ui64 Register(TIntrusivePtr<NInterconnect::TStreamSocket> socket, const TActorId& sessionActorId,
            TScopeId peerScopeId, std::function<void(TDisconnectReason)> onDisconnectCallback,
            bool sendPings, std::shared_ptr<std::atomic<int64_t>> clockSkew, std::shared_ptr<std::atomic<uint64_t>> pingRTT) = 0;

        // Put a message into outgoing queue. Thread-safe.
        virtual void Send(ui64 conn, std::unique_ptr<IEventHandle> ev, TIntrusivePtr<IReceiveCallback> callback = nullptr) = 0;

        // Cancels the connection's IO and tears it down; TEvUringV2Unregistered is posted once every
        // outstanding operation has drained. Thread-safe.
        virtual void Unregister(ui64 conn) = 0;

        // Register receive callback (for IDirectSession). Null callback deregisters it.
        virtual void RegisterReceiveCallback(ui64 conn, TActorId localActorId, TIntrusivePtr<IReceiveCallback> callback) = 0;

        // Stops every reaper thread. Must be called before the actor system is torn down.
        virtual void Stop() = 0;

        // Issue monitoring request.
        virtual void IssueMonRequest(ui64 conn, NMon::TEvHttpInfoRes::TPtr ev) = 0;

        // Get total output queue size (in bytes).
        virtual ui64 GetTotalOutputQueueSize(ui64 conn) = 0;
    };

    using TUringEnginePtr = TIntrusivePtr<IUringEngine>;

    // Creates the engine, taking every knob it needs from `common` (which it keeps a reference to, so
    // sessions can read the shared settings): Settings.V2.UringEngineThreads worker threads (clamped to
    // >= 1), each driving Settings.V2.UringEngineRingsPerShard io_uring rings (clamped to >= 1). With
    // SQPOLL, each ring has its own kernel poll thread, so rings-per-shard scales submission-polling
    // capacity independently of serialization workers.
    // The engine is created without an actor system (it does not exist yet at startup); bind it later via
    // SetActorSystem before the first Register. Returns nullptr when io_uring is unavailable (non-Linux or a
    // kernel that cannot create a ring).
    //
    // When the kernel SQPOLL thread is saturated while shard workers still have headroom, try
    // EnableSQPOLL=false (submit from the underutilized worker) or raise UringEngineRingsPerShard. When
    // workers are the bottleneck, raise UringEngineThreads / lower UringEngineRingsPerShard.
    // With EnableFixedFiles, each ring reserves UringEngineFixedFilesPerRing slots via
    // io_uring_register_files (sparse); session sockets are bound with register_files_update and
    // submitted with IOSQE_FIXED_FILE. Unsupported kernels or a full table fall back to plain fds.
    // With EnableProvidedBuffers, idle/min-size sessions recv via BUFFER_SELECT from a shared pool
    // (buf_ring when available, else provide_buffers). YDB_IC_V2_DISABLE_BUF_RING forces the non-buf_ring
    // pool path for testing.
    //
    // Common holds the engine via an intrusive pointer too; the resulting cycle is broken by Stop().
    TUringEnginePtr CreateUringEngine(TIntrusivePtr<TInterconnectProxyCommon> common);

}
