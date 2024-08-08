#pragma once

#include "packet.h"
#include "dispatcher_impl.h"
#include "ssl_helpers.h"

#include <yt/yt/core/bus/private.h>
#include <yt/yt/core/bus/bus.h>

#include <yt/yt_proto/yt/core/bus/proto/bus.pb.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/mpsc_stack.h>
#include <yt/yt/core/misc/ring_queue.h>
#include <yt/yt/core/misc/atomic_ptr.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/concurrency/pollable_detail.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/network/init.h>

#include <openssl/ssl.h>

#ifdef _win_
#include <winsock2.h>

#include <stddef.h>
#include <sys/uio.h>
#include <fcntl.h>
#endif

#include <atomic>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETcpConnectionState,
    (None)
    (Resolving)
    (Opening)
    (Open)
    (Closed)
    (Aborted)
);

DEFINE_ENUM(EPacketState,
    (Queued)
    (Encoded)
    (Canceled)
);

DEFINE_ENUM(ESslSessionState,
    (None)
    (Established)
    (Error)
    (Closed)
    (Aborted)
);

class TTcpConnection
    : public IBus
    , public NConcurrency::TPollableBase
{
public:
    TTcpConnection(
        TBusConfigPtr config,
        EConnectionType connectionType,
        TConnectionId id,
        SOCKET socket,
        EMultiplexingBand multiplexingBand,
        const TString& endpointDescription,
        const NYTree::IAttributeDictionary& endpointAttributes,
        const NNet::TNetworkAddress& endpointNetworkAddress,
        const std::optional<TString>& endpointAddress,
        const std::optional<TString>& unixDomainSocketPath,
        IMessageHandlerPtr handler,
        NConcurrency::IPollerPtr poller,
        IPacketTranscoderFactory* packetTranscoderFactory,
        IMemoryUsageTrackerPtr memoryUsageTracker);

    ~TTcpConnection();

    void Start();
    void RunPeriodicCheck();

    TConnectionId GetId() const;
    TBusNetworkStatistics GetBusStatistics() const;

    // IPollable implementation.
    const TString& GetLoggingTag() const override;
    void OnEvent(NConcurrency::EPollControl control) override;
    void OnShutdown() override;

    // IBus implementation.
    const TString& GetEndpointDescription() const override;
    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override;
    const TString& GetEndpointAddress() const override;
    const NNet::TNetworkAddress& GetEndpointNetworkAddress() const override;
    bool IsEndpointLocal() const override;
    bool IsEncrypted() const override;
    TBusNetworkStatistics GetNetworkStatistics() const override;
    TFuture<void> GetReadyFuture() const override;
    TFuture<void> Send(TSharedRefArray message, const TSendOptions& options) override;
    void SetTosLevel(TTosLevel tosLevel) override;
    void Terminate(const TError& error) override;

    DECLARE_SIGNAL_OVERRIDE(void(const TError&), Terminated);

private:
    using EState = ETcpConnectionState;

    using ESslState = ESslSessionState;

    struct TQueuedMessage
    {
        TQueuedMessage() = default;

        TQueuedMessage(TSharedRefArray message, const TSendOptions& options)
            : Promise((options.TrackingLevel != EDeliveryTrackingLevel::None || options.EnableSendCancelation)
                ? NewPromise<void>()
                : std::nullopt)
            , Message(std::move(message))
            , PayloadSize(GetByteSize(Message))
            , Options(options)
            , PacketId(TPacketId::Create())
        { }

        TPromise<void> Promise;
        TSharedRefArray Message;
        size_t PayloadSize;
        TSendOptions Options;
        TPacketId PacketId;
    };

    struct TPacket final
    {
        TPacket(
            EPacketType type,
            EPacketFlags flags,
            int checksummedPartCount,
            TPacketId packetId,
            TSharedRefArray message,
            size_t payloadSize,
            size_t packetSize)
            : Type(type)
            , Flags(flags)
            , ChecksummedPartCount(checksummedPartCount)
            , PacketId(packetId)
            , Message(std::move(message))
            , PayloadSize(payloadSize)
            , PacketSize(packetSize)
        { }

        EPacketType Type;
        EPacketFlags Flags;
        int ChecksummedPartCount;
        TPacketId PacketId;

        TSharedRefArray Message;

        size_t PayloadSize;
        size_t PacketSize;

        std::atomic<EPacketState> State = EPacketState::Queued;
        TPromise<void> Promise;
        TTcpConnectionPtr Connection;

        bool MarkEncoded();
        void OnCancel(const TError& error);
        void EnableCancel(TTcpConnectionPtr connection);
    };

    using TPacketPtr = TIntrusivePtr<TPacket>;

    const TBusConfigPtr Config_;
    const EConnectionType ConnectionType_;
    const TConnectionId Id_;
    const TString EndpointDescription_;
    const NYTree::IAttributeDictionaryPtr EndpointAttributes_;
    const NNet::TNetworkAddress EndpointNetworkAddress_;
    const std::optional<TString> EndpointAddress_;
    const std::optional<TString> UnixDomainSocketPath_;
    const std::optional<TString> AbstractUnixDomainSocketName_;
    const IMessageHandlerPtr Handler_;
    const NConcurrency::IPollerPtr Poller_;

    const TString LoggingTag_;
    const NLogging::TLogger Logger;

    const TPromise<void> ReadyPromise_ = NewPromise<void>();

    TString NetworkName_;
    // Endpoint host name is used for peer's certificate verification.
    TString EndpointHostName_;

    TBusNetworkCounters BusCounters_;
    TBusNetworkCounters BusCountersDelta_;
    TAtomicPtr<TBusNetworkCounters, /*EnableAcquireHazard*/ true> NetworkCounters_;

    bool GenerateChecksums_ = true;

    // Only used by client sockets.
    int Port_ = 0;

    std::atomic<EState> State_ = EState::None;

    // Actually stores NConcurrency::EPollControl.
    std::atomic<ui64> PendingControl_ = static_cast<ui64>(NConcurrency::EPollControl::Offline);

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    SOCKET Socket_ = INVALID_SOCKET;

    std::atomic<EMultiplexingBand> MultiplexingBand_ = EMultiplexingBand::Default;

    EMultiplexingBand ActualMultiplexingBand_ = EMultiplexingBand::Default;

    TAtomicObject<TError> Error_;

    NNet::IAsyncDialerSessionPtr DialerSession_;

    TSingleShotCallbackList<void(const TError&)> Terminated_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, QueuedMessagesDiscardLock_);
    TMpscStack<TQueuedMessage> QueuedMessages_;
    std::atomic<size_t> PendingOutPayloadBytes_ = 0;

    std::unique_ptr<IPacketDecoder> Decoder_;
    const NProfiling::TCpuDuration ReadStallTimeout_;
    std::atomic<NProfiling::TCpuInstant> LastIncompleteReadTime_ = std::numeric_limits<NProfiling::TCpuInstant>::max();
    TMemoryTrackedBlob ReadBuffer_;

    TRingQueue<TPacketPtr> QueuedPackets_;
    TRingQueue<TPacketPtr> EncodedPackets_;
    TRingQueue<TPacketPtr> UnackedPackets_;

    std::unique_ptr<IPacketEncoder> Encoder_;
    const NProfiling::TCpuDuration WriteStallTimeout_;
    std::atomic<NProfiling::TCpuInstant> LastIncompleteWriteTime_ = std::numeric_limits<NProfiling::TCpuInstant>::max();
    std::vector<TMemoryTrackedBlob> WriteBuffers_;
    TRingQueue<TRef> EncodedFragments_;
    TRingQueue<size_t> EncodedPacketSizes_;

    std::vector<struct iovec> SendVector_;

    std::atomic<TTosLevel> TosLevel_ = DefaultTosLevel;

    i64 LastRetransmitCount_ = 0;

    bool HandshakeEnqueued_ = false;
    bool HandshakeReceived_ = false;
    bool HandshakeSent_ = false;

    // How many bytes of SSL ACK packet remains to be read.
    size_t RemainingSslAckPacketBytes_ = 0;
    // TLS/SSL connection needs to be set up.
    bool EstablishSslSession_ = false;
    // TLS/SSL handshake has been started but hasn't been completed yet.
    bool PendingSslHandshake_ = false;
    bool SslAckEnqueued_ = false;
    bool SslAckReceived_ = false;
    bool SslAckSent_ = false;
    std::unique_ptr<SSL, TDeleter> Ssl_;
    std::atomic<ESslState> SslState_ = ESslState::None;

    const EEncryptionMode EncryptionMode_;
    const EVerificationMode VerificationMode_;

    const IMemoryUsageTrackerPtr MemoryUsageTracker_;

    NYTree::IAttributeDictionaryPtr PeerAttributes_;

    size_t MaxFragmentsPerWrite_ = 256;

    void Open(TGuard<NThreading::TSpinLock>& guard);
    void Close();
    void CloseSslSession(ESslState newSslState);

    void Abort(const TError& error, NLogging::ELogLevel logLevel = NLogging::ELogLevel::Debug);
    bool AbortIfNetworkingDisabled();
    void AbortSslSession();

    void InitBuffers();

    int GetSocketPort();

    void ConnectSocket(const NNet::TNetworkAddress& address);
    void OnDialerFinished(const TErrorOr<SOCKET>& socketOrError);

    void ResolveAddress();
    void OnAddressResolveFinished(const TErrorOr<NNet::TNetworkAddress>& result);
    void OnAddressResolved(const NNet::TNetworkAddress& address);
    void SetupNetwork(const NNet::TNetworkAddress& address);

    int GetSocketError() const;
    bool IsSocketError(ssize_t result);

    void CloseSocket();

    void ArmPoller();
    void UnarmPoller();

    void OnSocketRead();
    bool HasUnreadData() const;
    bool ReadSocket(char* buffer, size_t size, size_t* bytesRead);
    bool CheckReadError(ssize_t result);
    bool AdvanceDecoder(size_t size);
    bool OnPacketReceived() noexcept;
    bool OnAckPacketReceived();
    bool OnMessagePacketReceived();
    bool OnHandshakePacketReceived();
    bool OnSslAckPacketReceived();

    TPacket* EnqueuePacket(
        EPacketType type,
        EPacketFlags flags,
        int checksummedPartCount,
        TPacketId packetId,
        TSharedRefArray message = {},
        size_t payloadSize = 0);

    void OnSocketWrite();
    bool HasUnsentData() const;
    bool WriteFragments(size_t* bytesWritten);
    void FlushWrittenFragments(size_t bytesWritten);
    void FlushWrittenPackets(size_t bytesWritten);
    bool MaybeEncodeFragments();
    bool CheckWriteError(ssize_t result);
    void OnPacketSent();
    void OnAckPacketSent(const TPacket& packet);
    void OnMessagePacketSent(const TPacket& packet);
    void OnHandshakePacketSent();
    void OnSslAckPacketSent();
    void OnTerminate();
    void ProcessQueuedMessages();
    void DiscardOutcomingMessages();
    void DiscardUnackedMessages();

    void TryEnqueueHandshake();
    void TryEnqueueSslAck();
    TSharedRefArray MakeHandshakeMessage(const NProto::THandshake& handshake);
    std::optional<NProto::THandshake> TryParseHandshakeMessage(const TSharedRefArray& message);

    void UpdateConnectionCount(int delta);

    void IncrementPendingOut(i64 packetSize);
    void DecrementPendingOut(i64 packetSize);

    void FlushStatistics();

    template <class T, class U>
    i64 UpdateBusCounter(T TBusNetworkBandCounters::* field, U delta);

    void UpdateTcpStatistics();
    void FlushBusStatistics();

    void InitSocketTosLevel(int tosLevel);

    bool CheckSslReadError(ssize_t result);
    bool CheckSslWriteError(ssize_t result);
    bool CheckTcpReadError(ssize_t result);
    bool CheckTcpWriteError(ssize_t result);
    bool DoSslHandshake();
    size_t GetSslAckPacketSize();
    void TryEstablishSslSession();

    ssize_t DoReadSocket(char* buffer, size_t size);
    ssize_t DoWriteFragments(const std::vector<struct iovec>& vec);
};

DEFINE_REFCOUNTED_TYPE(TTcpConnection)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
