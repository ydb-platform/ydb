#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/interconnect/logging/logging.h>
#include <ydb/library/actors/interconnect/poller/poller_tcp.h>
#include <ydb/library/actors/util/datetime.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/metrics/metric_registry.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/ptr.h>
#include <util/system/datetime.h>
#include <util/system/mutex.h>

#include "event_filter.h"

#include <atomic>

namespace NInterconnect::NRdma {
    class IMemPool;
}

namespace NActors {
    enum class EEncryptionMode {
        DISABLED, // no encryption is required at all
        OPTIONAL, // encryption is enabled when supported by both peers
        REQUIRED, // encryption is mandatory
    };

    enum class ESocketSendOptimization {
        DISABLED,
        IC_MSG_ZEROCOPY,
    };

    struct TInterconnectSettings {
        TDuration Handshake;
        TDuration DeadPeer;
        TDuration CloseOnIdle;
        ui32 SendBufferDieLimitInMB = 0;
        ui64 OutputBuffersTotalSizeLimitInMB = 0;
        ui32 TotalInflightAmountOfData = 0;
        bool MergePerPeerCounters = false;
        bool MergePerHostCounters = false;
        bool MergePerDataCenterCounters = false;
        bool MergePerScopeClassCounters = false;
        ui32 TCPSocketBufferSize = 0;
        TDuration PingPeriod = TDuration::Seconds(3);
        TDuration ForceConfirmPeriod = TDuration::Seconds(1);
        TDuration LostConnection;
        TDuration BatchPeriod;
        bool BindOnAllAddresses = true;
        EEncryptionMode EncryptionMode = EEncryptionMode::DISABLED;
        bool TlsAuthOnly = false;
        TString Certificate; // certificate data in PEM format
        TString PrivateKey; // private key for the certificate in PEM format
        TString CaFilePath; // path to certificate authority file
        TString CipherList; // encryption algorithms
        THashSet<TString> ForbiddenSignatureAlgorithms;
        TDuration MessagePendingTimeout = TDuration::Seconds(1); // timeout for which messages are queued while in PendingConnection state
        ui64 MessagePendingSize = Max<ui64>(); // size of the queue
        ui32 MaxSerializedEventSize = NActors::EventMaxByteSize;
        ui32 PreallocatedBufferSize = 8 << 10; // 8 KB
        ui32 NumPreallocatedBuffers = 16;
        bool EnableExternalDataChannel = true;
        bool EnableKernelLiveness = false;
        TDuration KernelKeepAliveIdle = TDuration::Seconds(5);
        TDuration KernelKeepAliveInterval = TDuration::Seconds(1);
        ui32 KernelKeepAliveProbes = 5;
        TDuration KernelUserTimeout = TDuration::Seconds(10);
        // Period for user-space ping/clock probes that keep clock-skew metrics up to date
        // when kernel keepalive mode disables user-space dead-peer logic.
        TDuration ClockSkewPingTimeout = TDuration::Minutes(1);
        bool ValidateIncomingPeerViaDirectLookup = false;
        ui32 SocketBacklogSize = 0; // SOMAXCONN if zero
        TDuration FirstErrorSleep = TDuration::MilliSeconds(10);
        TDuration MaxErrorSleep = TDuration::Seconds(1);
        double ErrorSleepRetryMultiplier = 4.0;
        TDuration EventDelay = TDuration::Zero();
        ESocketSendOptimization SocketSendOptimization = ESocketSendOptimization::DISABLED;
        bool RdmaChecksum = true;
        bool EnableRdmaSendReceive = false;
        ui32 RdmaPayloadCopySizeThreshold = 64 << 10;
        // 5s * 2^8 = 1280s, about 21 minutes with the current RDMA retry base delay.
        ui32 MaxRdmaRetryBackoffLevel = 8;
        bool CollectSubscriptionStackTrace = false;
        bool UseUring = false;
        bool EnableUringSQPOLL = false; // only effective when UseUring is set
        // Enables negotiation and usage of TInterconnectSessionTCPv2 (no session continuation, no encryption).
        // v2 is used only when both peers have this enabled and encryption is not in effect.
        bool EnableInterconnectSessionV2 = false;
        bool ChecksumInterconnectSessionV2 = false;
        // Use io_uring SQPOLL mode for the v2 data-plane rings (kernel-side submission polling).
        bool EnableSQPOLLv2 = true;
        // Preserialize outgoing events on the session mailbox before handing them to the v2 engine (moves
        // serialization cost off the engine's shard worker thread).
        bool EnablePreserializeInV2 = false;
    };

    struct TWhiteboardSessionStatus {
        enum class EFlag {
            GREEN,
            YELLOW,
            ORANGE,
            RED,
        };

        TActorSystem* ActorSystem;
        ui32 PeerNodeId;
        TString PeerName;
        bool Connected;
        // oneof {
        bool SessionClosed = false;
        bool SessionPendingConnection = false;
        bool SessionConnected = false;
        // }
        EFlag ConnectStatus;
        i64 ClockSkewUs;
        bool SameScope;
        ui64 PingTimeUs;
        NActors::TScopeId ScopeId;
        double Utilization;
        ui64 ConnectTime;
        ui64 BytesWrittenToSocket;
        TString PeerBridgePileName;
    };

    struct TChannelSettings {
        ui16 Weight;
    };

    typedef TMap<ui16, TChannelSettings> TChannelsConfig;

    using TRegisterMonPageCallback = std::function<void(const TString& path, const TString& title,
                                                        TActorSystem* actorSystem, const TActorId& actorId)>;

    using TInitWhiteboardCallback = std::function<void(ui16 icPort, TActorSystem* actorSystem)>;

    using TUpdateWhiteboardCallback = std::function<void(const TWhiteboardSessionStatus& data)>;

    class IUringEngine; // shared v2 io_uring data-plane engine (see interconnect_uring_engine.h)

    struct TInterconnectProxyCommon : TAtomicRefCount<TInterconnectProxyCommon> {
        TActorId NameserviceId;
        NMonitoring::TDynamicCounterPtr MonCounters;
        std::shared_ptr<NMonitoring::IMetricRegistry> Metrics;
        TChannelsConfig ChannelsConfig;
        TInterconnectSettings Settings;
        TRegisterMonPageCallback RegisterMonPage;
        TActorId DestructorId;
        std::shared_ptr<std::atomic<TAtomicBase>> DestructorQueueSize;
        TAtomicBase MaxDestructorQueueSize = 1024 * 1024 * 1024;
        TString ClusterUUID;
        TVector<TString> AcceptUUID;
        ui64 StartTime = GetCycleCountFast();
        TString TechnicalSelfHostName;
        TInitWhiteboardCallback InitWhiteboard;
        TUpdateWhiteboardCallback UpdateWhiteboard;
        ui32 HandshakeBallastSize = 0;
        TAtomic StartedSessionKiller = 0;
        TScopeId LocalScopeId;
        std::shared_ptr<TEventFilter> EventFilter;
        TString Cookie; // unique random identifier of a node instance (generated randomly at every start)
        std::unordered_map<ui16, TString> ChannelName;
        std::optional<ui32> OutgoingHandshakeInflightLimit;
        std::vector<TActorId> ConnectionCheckerActorIds; // a list of actors used for checking connection params

        std::atomic_uint64_t NumSessionsWithDataInQueue = 0;
        std::atomic_uint64_t CyclesOnLastSwitch = 0;
        std::atomic_uint64_t CyclesWithNonzeroSessions = 0;
        std::atomic_uint64_t CyclesWithZeroSessions = 0;

        std::atomic_uint64_t ErrorStateLogLastMicroSeconds = 0;
        std::atomic_uint64_t ErrorStateLogSuppressed = 0;

        double CalculateNetworkUtilization();
        void AddSessionWithDataInQueue();
        void RemoveSessionWithDataInQueue();
        TActorId MetricsAggregatorId;

        struct TVersionInfo {
            TString Tag; // version tag for this node
            TSet<TString> AcceptedTags; // we accept all enlisted version tags of peer nodes, but no others; empty = accept all
        };

        // obsolete compatibility control
        TMaybe<TVersionInfo> VersionInfo;

        std::optional<TString> CompatibilityInfo;
        std::function<bool(const TString&, TString&)> ValidateCompatibilityInfo;
        std::function<bool(const TInterconnectProxyCommon::TVersionInfo&, TString&)> ValidateCompatibilityOldFormat;

        std::shared_ptr<NInterconnect::NRdma::IMemPool> RdmaMemPool;

        // Shared v2 io_uring data-plane engine for the node (created once at startup when v2 + io_uring
        // are enabled, and bound to the actor system once it exists). Sessions fetch it and call it
        // directly.
        TIntrusivePtr<IUringEngine> UringEngineV2;

        // Out-of-line so translation units that construct/destroy Common do not need the complete
        // IUringEngine type (it is only complete in interconnect_common.cpp).
        TInterconnectProxyCommon();
        ~TInterconnectProxyCommon();

        using TPtr = TIntrusivePtr<TInterconnectProxyCommon>;
    };

}
