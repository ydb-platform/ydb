#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/util/datetime.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/metrics/metric_registry.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/system/datetime.h>

#include "poller_tcp.h"
#include "logging.h"
#include "event_filter.h"

#include <atomic>

namespace NActors {
    enum class EEncryptionMode {
        DISABLED, // no encryption is required at all
        OPTIONAL, // encryption is enabled when supported by both peers
        REQUIRED, // encryption is mandatory
    };

    struct TInterconnectSettings {
        TDuration Handshake;
        TDuration DeadPeer;
        TDuration CloseOnIdle;
        ui32 SendBufferDieLimitInMB = 0;
        ui64 OutputBuffersTotalSizeLimitInMB = 0;
        ui32 TotalInflightAmountOfData = 0;
        bool MergePerPeerCounters = false;
        bool MergePerDataCenterCounters = false;
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
        bool EnableExternalDataChannel = false;
        bool ValidateIncomingPeerViaDirectLookup = false;
        ui32 SocketBacklogSize = 0; // SOMAXCONN if zero
        TDuration FirstErrorSleep = TDuration::MilliSeconds(10);
        TDuration MaxErrorSleep = TDuration::Seconds(1);
        double ErrorSleepRetryMultiplier = 4.0;
        TDuration EventDelay = TDuration::Zero();

        ui32 GetSendBufferSize() const {
            ui32 res = 512 * 1024; // 512 kb is the default value for send buffer
            if (TCPSocketBufferSize) {
                res = TCPSocketBufferSize;
            }
            return res;
        }
    };

    struct TWhiteboardSessionStatus {
        TActorSystem* ActorSystem;
        ui32 PeerId;
        TString Peer;
        bool Connected;
        bool Green;
        bool Yellow;
        bool Orange;
        bool Red;
        i64 ClockSkew;
        bool ReportClockSkew;

        TWhiteboardSessionStatus(TActorSystem* actorSystem, ui32 peerId, const TString& peer, bool connected,
                                        bool green, bool yellow, bool orange, bool red, i64 clockSkew, bool reportClockSkew)
            : ActorSystem(actorSystem)
            , PeerId(peerId)
            , Peer(peer)
            , Connected(connected)
            , Green(green)
            , Yellow(yellow)
            , Orange(orange)
            , Red(red)
            , ClockSkew(clockSkew)
            , ReportClockSkew(reportClockSkew)
            {}
    };

    struct TChannelSettings {
        ui16 Weight;
    };

    typedef TMap<ui16, TChannelSettings> TChannelsConfig;

    using TRegisterMonPageCallback = std::function<void(const TString& path, const TString& title,
                                                        TActorSystem* actorSystem, const TActorId& actorId)>;

    using TInitWhiteboardCallback = std::function<void(ui16 icPort, TActorSystem* actorSystem)>;

    using TUpdateWhiteboardCallback = std::function<void(const TWhiteboardSessionStatus& data)>;

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

        struct TVersionInfo {
            TString Tag; // version tag for this node
            TSet<TString> AcceptedTags; // we accept all enlisted version tags of peer nodes, but no others; empty = accept all
        };

        // obsolete compatibility control
        TMaybe<TVersionInfo> VersionInfo;

        std::optional<TString> CompatibilityInfo;
        std::function<bool(const TString&, TString&)> ValidateCompatibilityInfo;
        std::function<bool(const TInterconnectProxyCommon::TVersionInfo&, TString&)> ValidateCompatibilityOldFormat;

        using TPtr = TIntrusivePtr<TInterconnectProxyCommon>;
    };

}
