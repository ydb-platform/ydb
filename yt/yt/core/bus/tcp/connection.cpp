#include "connection.h"

#include "config.h"
#include "server.h"
#include "dispatcher_impl.h"
#include "ssl_context.h"
#include "ssl_helpers.h"

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/net/socket.h>
#include <yt/yt/core/net/dialer.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <util/system/error.h>
#include <util/system/guard.h>
#include <util/system/hostname.h>

#ifdef __linux__
#include <netinet/tcp.h>
#endif

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <cerrno>

namespace NYT::NBus {

using namespace NConcurrency;
using namespace NFS;
using namespace NNet;
using namespace NYTree;
using namespace NYson;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static constexpr size_t ReadBufferSize = 16_KB;
static constexpr size_t MaxBatchReadSize = 64_KB;
static constexpr auto ReadTimeWarningThreshold = TDuration::MilliSeconds(100);

static constexpr size_t WriteBufferSize = 16_KB;
static constexpr size_t MaxBatchWriteSize = 64_KB;
static constexpr size_t MaxWriteCoalesceSize = 1_KB;
static constexpr auto WriteTimeWarningThreshold = TDuration::MilliSeconds(100);

static constexpr auto HandshakePacketId = TPacketId(1, 0, 0, 0);
static constexpr auto SslAckPacketId = TPacketId(2, 0, 0, 0);

static constexpr i64 PendingOutBytesFlushThreshold = 1_MBs;

////////////////////////////////////////////////////////////////////////////////

struct TTcpServerConnectionReadBufferTag { };
struct TTcpServerConnectionWriteBufferTag { };

struct TTcpClientConnectionReadBufferTag { };
struct TTcpClientConnectionWriteBufferTag { };

////////////////////////////////////////////////////////////////////////////////

bool TTcpConnection::TPacket::MarkEncoded()
{
    auto expected = EPacketState::Queued;
    return State.compare_exchange_strong(expected, EPacketState::Encoded);
}

void TTcpConnection::TPacket::OnCancel(const TError& /*error*/)
{
    auto expected = EPacketState::Queued;
    if (!State.compare_exchange_strong(expected, EPacketState::Canceled)) {
        return;
    }

    Message.Reset();
    if (Connection) {
        Connection->DecrementPendingOut(PacketSize);
    }
}

void TTcpConnection::TPacket::EnableCancel(TTcpConnectionPtr connection)
{
    Connection = std::move(connection);
    if (!Promise.OnCanceled(BIND(&TPacket::OnCancel, MakeWeak(this)))) {
        OnCancel(TError());
    }
}

////////////////////////////////////////////////////////////////////////////////

TTcpConnection::TTcpConnection(
    TBusConfigPtr config,
    EConnectionType connectionType,
    TConnectionId id,
    SOCKET socket,
    EMultiplexingBand multiplexingBand,
    const TString& endpointDescription,
    const IAttributeDictionary& endpointAttributes,
    const TNetworkAddress& endpointNetworkAddress,
    const std::optional<TString>& endpointAddress,
    const std::optional<TString>& unixDomainSocketPath,
    IMessageHandlerPtr handler,
    IPollerPtr poller,
    IPacketTranscoderFactory* packetTranscoderFactory,
    IMemoryUsageTrackerPtr memoryUsageTracker)
    : Config_(std::move(config))
    , ConnectionType_(connectionType)
    , Id_(id)
    , EndpointDescription_(endpointDescription)
    , EndpointAttributes_(endpointAttributes.Clone())
    , EndpointNetworkAddress_(endpointNetworkAddress)
    , EndpointAddress_(endpointAddress)
    , UnixDomainSocketPath_(unixDomainSocketPath)
    , Handler_(std::move(handler))
    , Poller_(std::move(poller))
    , LoggingTag_(Format("ConnectionId: %v, ConnectionType: %v, RemoteAddress: %v, EncryptionMode: %v, VerificationMode: %v",
        Id_,
        ConnectionType_,
        EndpointDescription_,
        Config_->EncryptionMode,
        Config_->VerificationMode))
    , Logger(BusLogger().WithRawTag(LoggingTag_))
    , GenerateChecksums_(Config_->GenerateChecksums)
    , Socket_(socket)
    , MultiplexingBand_(multiplexingBand)
    , Decoder_(packetTranscoderFactory->CreateDecoder(Logger, Config_->VerifyChecksums))
    , ReadStallTimeout_(NProfiling::DurationToCpuDuration(Config_->ReadStallTimeout))
    , Encoder_(packetTranscoderFactory->CreateEncoder(Logger))
    , WriteStallTimeout_(NProfiling::DurationToCpuDuration(Config_->WriteStallTimeout))
    , EncryptionMode_(Config_->EncryptionMode)
    , VerificationMode_(Config_->VerificationMode)
    , MemoryUsageTracker_(std::move(memoryUsageTracker))
{ }

TTcpConnection::~TTcpConnection()
{
    Close();
}

void TTcpConnection::Close()
{
    CloseSslSession(ESslState::Closed);

    {
        auto guard = Guard(Lock_);

        if (Error_.Load().IsOK()) {
            Error_.Store(TError(NBus::EErrorCode::TransportError, "Bus terminated")
                << *EndpointAttributes_);
        }

        if (State_ == EState::Open) {
            UpdateConnectionCount(-1);
        }

        UpdateTcpStatistics();

        UnarmPoller();

        CloseSocket();

        State_ = EState::Closed;
        PendingControl_.store(static_cast<ui64>(EPollControl::Offline));
    }

    DiscardOutcomingMessages();
    DiscardUnackedMessages();

    while (!QueuedPackets_.empty()) {
        const auto& packet = QueuedPackets_.front();
        if (packet->Connection) {
            packet->OnCancel(TError());
        } else {
            DecrementPendingOut(packet->PacketSize);
        }
        QueuedPackets_.pop();
    }

    while (!EncodedPackets_.empty()) {
        const auto& packet = EncodedPackets_.front();
        DecrementPendingOut(packet->PacketSize);
        EncodedPackets_.pop();
    }

    EncodedFragments_.clear();

    {
        auto guard = Guard(Lock_);
        FlushStatistics();
    }
}

void TTcpConnection::Start()
{
    YT_LOG_DEBUG("Starting TCP connection");

    // Offline in PendingControl_ prevents retrying events until end of Open().
    YT_VERIFY(Any(static_cast<EPollControl>(PendingControl_.load()) & EPollControl::Offline));

    if (AbortIfNetworkingDisabled()) {
        return;
    }

    if (!Poller_->TryRegister(this)) {
        Abort(TError(NBus::EErrorCode::TransportError, "Cannot register connection pollable"));
        return;
    }

    TTcpDispatcher::TImpl::Get()->RegisterConnection(this);

    try {
        InitBuffers();
    } catch (const std::exception& ex) {
        Abort(TError(NBus::EErrorCode::TransportError, "I/O buffers allocation error")
            << ex);
        return;
    }

    if (Config_->ConnectionStartDelay) {
        YT_LOG_WARNING("Delay in opening activation of the test connection (Delay: %v)", Config_->ConnectionStartDelay);
        TDelayedExecutor::WaitForDuration(Config_->ConnectionStartDelay.value());
    }

    switch (ConnectionType_) {
        case EConnectionType::Client:
            YT_VERIFY(Socket_ == INVALID_SOCKET);
            State_ = EState::Resolving;
            ResolveAddress();
            break;

        case EConnectionType::Server: {
            auto guard = Guard(Lock_);
            YT_VERIFY(Socket_ != INVALID_SOCKET);
            State_ = EState::Opening;
            SetupNetwork(EndpointNetworkAddress_);
            Open(guard);
            break;
        }

        default:
            YT_ABORT();
    }
}

void TTcpConnection::RunPeriodicCheck()
{
    if (State_ != EState::Open) {
        return;
    }

    {
        auto guard = Guard(Lock_);
        FlushStatistics();
    }

    auto now = NProfiling::GetCpuInstant();

    if (LastIncompleteWriteTime_.load(std::memory_order::relaxed) < now - WriteStallTimeout_) {
        UpdateBusCounter(&TBusNetworkBandCounters::StalledWrites, 1);
        Terminate(TError(
            NBus::EErrorCode::TransportError,
            "Socket write stalled")
            << TErrorAttribute("timeout", Config_->WriteStallTimeout)
            << TErrorAttribute("pending_control", static_cast<EPollControl>(PendingControl_.load())));
        return;
    }

    if (LastIncompleteReadTime_.load(std::memory_order::relaxed) < now - ReadStallTimeout_) {
        UpdateBusCounter(&TBusNetworkBandCounters::StalledReads, 1);
        Terminate(TError(
            NBus::EErrorCode::TransportError,
            "Socket read stalled")
            << TErrorAttribute("timeout", Config_->ReadStallTimeout)
            << TErrorAttribute("pending_control", static_cast<EPollControl>(PendingControl_.load())));
        return;
    }
}

const TString& TTcpConnection::GetLoggingTag() const
{
    return LoggingTag_;
}

void TTcpConnection::TryEnqueueHandshake()
{
    if (std::exchange(HandshakeEnqueued_, true)) {
        return;
    }

    NProto::THandshake handshake;
    ToProto(handshake.mutable_connection_id(), Id_);
    if (ConnectionType_ == EConnectionType::Client) {
        handshake.set_multiplexing_band(ToProto<int>(MultiplexingBand_.load()));
    }
    handshake.set_encryption_mode(ToProto<int>(EncryptionMode_));
    handshake.set_verification_mode(ToProto<int>(VerificationMode_));

    auto message = MakeHandshakeMessage(handshake);
    auto messageSize = GetByteSize(message);

    EnqueuePacket(
        // COMPAT(babenko)
        EPacketType::Message,
        EPacketFlags::None,
        1,
        HandshakePacketId,
        std::move(message),
        messageSize);

    YT_LOG_DEBUG("Handshake enqueued");
}

TSharedRefArray TTcpConnection::MakeHandshakeMessage(const NProto::THandshake& handshake)
{
    auto protoSize = handshake.ByteSizeLong();
    auto totalSize = sizeof(HandshakeMessageSignature) + protoSize;

    TSharedRefArrayBuilder builder(1, totalSize);
    auto ref = builder.AllocateAndAdd(totalSize);
    char* ptr = ref.Begin();

    *reinterpret_cast<ui32*>(ptr) = HandshakeMessageSignature;
    ptr += sizeof(ui32);

    SerializeProtoToRef(handshake, TMutableRef(ptr, protoSize));
    ptr += protoSize;

    return builder.Finish();
}

std::optional<NProto::THandshake> TTcpConnection::TryParseHandshakeMessage(const TSharedRefArray& message)
{
    if (message.Size() != 1) {
        YT_LOG_ERROR("Handshake packet contains invalid number of parts (PartCount: %v)",
            message.Size());
        return {};
    }

    const auto& part = message[0];
    if (part.Size() < sizeof(HandshakeMessageSignature)) {
        YT_LOG_ERROR("Handshake packet size is too small (Size: %v)",
            part.Size());
        return {};
    }

    auto signature = *reinterpret_cast<const ui32*>(part.Begin());
    if (signature != HandshakeMessageSignature) {
        YT_LOG_ERROR("Invalid handshake packet signature (Expected: %x, Actual: %x)",
            HandshakeMessageSignature,
            signature);
        return {};
    }

    NProto::THandshake handshake;
    if (!TryDeserializeProto(&handshake, part.Slice(sizeof(HandshakeMessageSignature), part.Size()))) {
        YT_LOG_ERROR("Error deserializing handshake packet");
        return {};
    }

    return handshake;
}

void TTcpConnection::UpdateConnectionCount(int delta)
{
    switch (ConnectionType_) {
        case EConnectionType::Client:
            UpdateBusCounter(&TBusNetworkBandCounters::ClientConnections, delta);
            break;

        case EConnectionType::Server:
            UpdateBusCounter(&TBusNetworkBandCounters::ServerConnections, delta);
            break;

        default:
            YT_ABORT();
    }
}

void TTcpConnection::IncrementPendingOut(i64 packetSize)
{
    UpdateBusCounter(&TBusNetworkBandCounters::PendingOutPackets, +1);
    if (UpdateBusCounter(&TBusNetworkBandCounters::PendingOutBytes, packetSize) > PendingOutBytesFlushThreshold) {
        FlushBusStatistics();
    }
}

void TTcpConnection::DecrementPendingOut(i64 packetSize)
{
    UpdateBusCounter(&TBusNetworkBandCounters::PendingOutPackets, -1);
    UpdateBusCounter(&TBusNetworkBandCounters::PendingOutBytes, -packetSize);
}

TConnectionId TTcpConnection::GetId() const
{
    return Id_;
}

void TTcpConnection::Open(TGuard<NThreading::TSpinLock>& guard)
{
    State_ = EState::Open;

    YT_LOG_DEBUG("TCP connection has been established (LocalPort: %v)", GetSocketPort());

    if (LastIncompleteWriteTime_ != std::numeric_limits<NProfiling::TCpuInstant>::max()) {
        // Rewind stall detection if already armed by pending send
        LastIncompleteWriteTime_ = NProfiling::GetCpuInstant();
    }

    UpdateConnectionCount(+1);
    FlushStatistics();

    // Go online and start event processing.
    auto previousPendingControl = static_cast<EPollControl>(PendingControl_.fetch_and(~static_cast<ui64>(EPollControl::Offline)));

    ArmPoller();

    guard.Release();

    // Something might be pending already, for example Terminate.
    if (Any(previousPendingControl & ~EPollControl::Offline)) {
        YT_LOG_TRACE("Retrying event processing for Open (PendingControl: %v)", previousPendingControl);
        Poller_->Retry(this);
    }
}

void TTcpConnection::ResolveAddress()
{
    if (UnixDomainSocketPath_) {
        if (!IsLocalBusTransportEnabled()) {
            Abort(TError(NBus::EErrorCode::TransportError, "Local bus transport is not available"));
            return;
        }

        NetworkName_ = LocalNetworkName;
        try {
            EndpointHostName_ = FQDNHostName();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to resolve local host name");
            EndpointHostName_ = "localhost";
        }

        // NB(gritukan): Unix domain socket path cannot be longer than 108 symbols, so let's try to shorten it.
        OnAddressResolved(
            TNetworkAddress::CreateUnixDomainSocketAddress(GetShortestPath(*UnixDomainSocketPath_)));
    } else {
        TStringBuf hostName;
        try {
            ParseServiceAddress(*EndpointAddress_, &hostName, &Port_);
        } catch (const std::exception& ex) {
            Abort(TError(ex).SetCode(NBus::EErrorCode::TransportError));
            return;
        }

        EndpointHostName_ = hostName;

        TAddressResolver::Get()->Resolve(TString(hostName)).Subscribe(
            BIND(&TTcpConnection::OnAddressResolveFinished, MakeStrong(this))
                .Via(Poller_->GetInvoker()));
    }
}

void TTcpConnection::OnAddressResolveFinished(const TErrorOr<TNetworkAddress>& result)
{
    if (!result.IsOK()) {
        Abort(result);
        return;
    }

    TNetworkAddress address(result.Value(), Port_);
    OnAddressResolved(address);

    YT_LOG_DEBUG("Connection network address resolved (Address: %v, NetworkName: %v)",
        address,
        NetworkName_);
}

void TTcpConnection::OnAddressResolved(const TNetworkAddress& address)
{
    State_ = EState::Opening;
    SetupNetwork(address);
    ConnectSocket(address);
}

void TTcpConnection::SetupNetwork(const TNetworkAddress& address)
{
    NetworkName_ = TTcpDispatcher::TImpl::Get()->GetNetworkNameForAddress(address);
    NetworkCounters_ = TTcpDispatcher::TImpl::Get()->GetCounters(NetworkName_, IsEncrypted());

    // Suppress checksum generation for local traffic.
    if (NetworkName_ == LocalNetworkName) {
        GenerateChecksums_ = false;
    }
}

void TTcpConnection::Abort(const TError& error)
{
    AbortSslSession();

    // Fast path.
    if (State_ == EState::Aborted || State_ == EState::Closed) {
        return;
    }

    // Construct a detailed error.
    YT_VERIFY(!error.IsOK());
    auto detailedError = error << *EndpointAttributes_;
    if (PeerAttributes_) {
        detailedError <<= *PeerAttributes_;
    }

    {
        auto guard = Guard(Lock_);

        if (State_ == EState::Aborted || State_ == EState::Closed) {
            return;
        }

        UnarmPoller();

        if (State_ == EState::Open) {
            UpdateConnectionCount(-1);
        }

        State_ = EState::Aborted;

        Error_.Store(detailedError);

        // Prevent starting new OnSocketRead/OnSocketWrite and Retry.
        // Already running will continue, Unregister will drain them.
        PendingControl_.fetch_or(static_cast<ui64>(EPollControl::Shutdown));
    }

    YT_LOG_DEBUG(detailedError, "Connection aborted");

    // OnShutdown() will be called after draining events from thread pools.
    YT_UNUSED_FUTURE(Poller_->Unregister(this));

    ReadyPromise_.TrySet(detailedError);
}

bool TTcpConnection::AbortIfNetworkingDisabled()
{
    if (!TTcpDispatcher::Get()->IsNetworkingDisabled()) {
        return false;
    }

    YT_LOG_DEBUG("Aborting connection since networking is disabled");

    Abort(TError(NBus::EErrorCode::TransportError, "Networking is disabled"));
    return true;
}

void TTcpConnection::InitBuffers()
{
    ReadBuffer_ = TMemoryTrackedBlob::Build(
        MemoryUsageTracker_,
        ConnectionType_ == EConnectionType::Server
            ? GetRefCountedTypeCookie<TTcpServerConnectionReadBufferTag>()
            : GetRefCountedTypeCookie<TTcpClientConnectionReadBufferTag>());
    ReadBuffer_
        .TryResize(
            ReadBufferSize,
            /*initializeStorage*/ false)
        .ThrowOnError();

    auto trackedBlob = TMemoryTrackedBlob::Build(
        MemoryUsageTracker_,
        ConnectionType_ == EConnectionType::Server
            ? GetRefCountedTypeCookie<TTcpServerConnectionWriteBufferTag>()
            : GetRefCountedTypeCookie<TTcpClientConnectionWriteBufferTag>());
    trackedBlob
        .TryReserve(WriteBufferSize)
        .ThrowOnError();
    WriteBuffers_.push_back(std::move(trackedBlob));
}

int TTcpConnection::GetSocketPort()
{
    TNetworkAddress address;
    auto* sockAddr = address.GetSockAddr();
    socklen_t sockAddrLen = address.GetLength();
    int result = getsockname(Socket_, sockAddr, &sockAddrLen);
    if (result < 0) {
        return -1;
    }

    switch (sockAddr->sa_family) {
        case AF_INET:
            return ntohs(reinterpret_cast<sockaddr_in*>(sockAddr)->sin_port);

        case AF_INET6:
            return ntohs(reinterpret_cast<sockaddr_in6*>(sockAddr)->sin6_port);

        default:
            return -1;
    }
}

void TTcpConnection::ConnectSocket(const TNetworkAddress& address)
{
    auto dialer = CreateAsyncDialer(
        Config_,
        Poller_,
        Logger);
    DialerSession_ = dialer->CreateSession(
        address,
        BIND(&TTcpConnection::OnDialerFinished, MakeWeak(this)));
    DialerSession_->Dial();
}

void TTcpConnection::OnDialerFinished(const TErrorOr<SOCKET>& socketOrError)
{
    YT_LOG_DEBUG("Dialer finished");

    DialerSession_.Reset();

    if (!socketOrError.IsOK()) {
        Abort(TError(
            NBus::EErrorCode::TransportError,
            "Error connecting to %v",
            EndpointDescription_)
            << socketOrError);
        return;
    }

    {
        auto guard = Guard(Lock_);

        if (State_ != EState::Opening) {
            return;
        }

        Socket_ = socketOrError.Value();

        auto tosLevel = TosLevel_.load();
        if (tosLevel != DefaultTosLevel) {
            InitSocketTosLevel(tosLevel);
        }

        Open(guard);
    }
}

const TString& TTcpConnection::GetEndpointDescription() const
{
    return EndpointDescription_;
}

const IAttributeDictionary& TTcpConnection::GetEndpointAttributes() const
{
    return *EndpointAttributes_;
}

const TString& TTcpConnection::GetEndpointAddress() const
{
    if (EndpointAddress_) {
        return *EndpointAddress_;
    } else {
        static const TString EmptyAddress;
        return EmptyAddress;
    }
}

const TNetworkAddress& TTcpConnection::GetEndpointNetworkAddress() const
{
    return EndpointNetworkAddress_;
}

bool TTcpConnection::IsEndpointLocal() const
{
    return false;
}

bool TTcpConnection::IsEncrypted() const
{
    return SslState_ != ESslState::None;
}

TBusNetworkStatistics TTcpConnection::GetNetworkStatistics() const
{
    if (auto networkCounters = NetworkCounters_.AcquireHazard()) {
        return networkCounters->ToStatistics();
    } else {
        return {};
    }
}

TBusNetworkStatistics TTcpConnection::GetBusStatistics() const
{
    return BusCounters_.ToStatistics();
}

TFuture<void> TTcpConnection::GetReadyFuture() const
{
    return ReadyPromise_.ToFuture();
}

TFuture<void> TTcpConnection::Send(TSharedRefArray message, const TSendOptions& options)
{
    if (TTcpDispatcher::Get()->IsNetworkingDisabled()) {
        return MakeFuture(TError(NBus::EErrorCode::TransportError, "Networking is disabled"));
    }

    if (message.Size() > MaxMessagePartCount) {
        return MakeFuture<void>(TError(
            NRpc::EErrorCode::TransportError,
            "Message exceeds part count limit: %v > %v",
            message.Size(),
            MaxMessagePartCount));
    }

    for (size_t index = 0; index < message.Size(); ++index) {
        const auto& part = message[index];
        if (part.Size() > MaxMessagePartSize) {
            return MakeFuture<void>(TError(
                NRpc::EErrorCode::TransportError,
                "Message part %v exceeds size limit: %v > %v",
                index,
                part.Size(),
                MaxMessagePartSize));
        }
    }

    TQueuedMessage queuedMessage(std::move(message), options);
    auto promise = queuedMessage.Promise;
    auto pendingOutPayloadBytes = PendingOutPayloadBytes_.fetch_add(queuedMessage.PayloadSize);

    // Log first to avoid producing weird traces.
    YT_LOG_DEBUG("Outcoming message enqueued (PacketId: %v, PendingOutPayloadBytes: %v)",
        queuedMessage.PacketId,
        pendingOutPayloadBytes);

    if (LastIncompleteWriteTime_ == std::numeric_limits<NProfiling::TCpuInstant>::max()) {
        // Arm stall detection.
        LastIncompleteWriteTime_ = NProfiling::GetCpuInstant();
    }

    QueuedMessages_.Enqueue(std::move(queuedMessage));

    // Wake up the event processing if needed.
    {
        auto previousPendingControl = static_cast<EPollControl>(PendingControl_.fetch_or(static_cast<ui64>(EPollControl::Write)));
        if (None(previousPendingControl)) {
            YT_LOG_TRACE("Retrying event processing for Send");
            Poller_->Retry(this);
        }
    }

    // Double-check the state not to leave any dangling outcoming messages.
    if (State_.load() == EState::Closed) {
        DiscardOutcomingMessages();
    }

    return promise;
}

void TTcpConnection::SetTosLevel(TTosLevel tosLevel)
{
    if (TosLevel_.load() == tosLevel) {
        return;
    }

    {
        auto guard = Guard(Lock_);
        if (Socket_ != INVALID_SOCKET) {
            InitSocketTosLevel(tosLevel);
        }
    }

    TosLevel_.store(tosLevel);
}

void TTcpConnection::Terminate(const TError& error)
{
    // Construct a detailed error.
    YT_VERIFY(!error.IsOK());
    auto detailedError = error << *EndpointAttributes_;
    if (PeerAttributes_) {
        detailedError <<= *PeerAttributes_;
    }

    auto guard = Guard(Lock_);

    if (!Error_.Load().IsOK() ||
        State_ == EState::Aborted ||
        State_ == EState::Closed)
    {
        YT_LOG_DEBUG("Connection is already terminated, termination request ignored (State: %v, PendingControl: %v, PendingOutPayloadBytes: %v)",
            State_.load(),
            static_cast<EPollControl>(PendingControl_.load()),
            PendingOutPayloadBytes_.load());
        return;
    }

    YT_LOG_DEBUG("Sending termination request");

    // Save error for OnTerminate().
    Error_.Store(detailedError);

    // Arm calling OnTerminate() from OnEvent().
    auto previousPendingControl = static_cast<EPollControl>(PendingControl_.fetch_or(static_cast<ui64>(EPollControl::Terminate)));

    guard.Release();

    // To recover from bogus state always retry processing unless socket is offline
    if (None(previousPendingControl & EPollControl::Offline)) {
        YT_LOG_TRACE("Retrying event processing for Terminate (PendingControl: %v)", previousPendingControl);
        Poller_->Retry(this);
    }
}

void TTcpConnection::SubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    Terminated_.Subscribe(callback);
}

void TTcpConnection::UnsubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    Terminated_.Unsubscribe(callback);
}

void TTcpConnection::OnEvent(EPollControl control)
{
    auto multiplexingBand = MultiplexingBand_.load();
    if (multiplexingBand != ActualMultiplexingBand_) {
        Poller_->SetExecutionPool(this, FormatEnum(multiplexingBand));
        ActualMultiplexingBand_ = multiplexingBand;
    }

    EPollControl action;
    {
        auto rawPendingControl = PendingControl_.load(std::memory_order::acquire);
        while (true) {
            auto pendingControl = static_cast<EPollControl>(rawPendingControl);
            // New events could come while previous handler is still running.
            if (Any(pendingControl & (EPollControl::Running | EPollControl::Shutdown))) {
                if (!PendingControl_.compare_exchange_weak(rawPendingControl, static_cast<ui64>(pendingControl | control))) {
                    continue;
                }
                // CAS succeeded, bail out.
                YT_LOG_TRACE("Event handler is already running (PendingControl: %v)",
                    pendingControl);
                return;
            }

            action = pendingControl | control;

            // Clear Read/Write before operation. Consequent event will raise it
            // back and retry handling. OnSocketRead() always consumes all backlog
            // or aborts connection if something went wrong, otherwise if something
            // left then handling should raise Read in PendingControl_ back.
            if (!PendingControl_.compare_exchange_weak(rawPendingControl, static_cast<ui64>(EPollControl::Running))) {
                continue;
            }

            // CAS succeeded, bail out.
            break;
        }
    }

    // OnEvent should never be called for an offline socket.
    YT_VERIFY(None(action & EPollControl::Offline));

    if (AbortIfNetworkingDisabled()) {
        return;
    }

    if (Any(action & EPollControl::Terminate)) {
        OnTerminate();
        // Leave Running flag set in PendingControl_ to drain further events and
        // prevent Retry which could race with Unregister()/OnShutdown().
        return;
    }

    YT_LOG_TRACE("Event processing started");

    // Proceed with pending ssl handshake prior to reads or writes.
    if (PendingSslHandshake_) {
        PendingSslHandshake_ = DoSslHandshake();
    }

    // NB: Try to read from the socket before writing into it to avoid
    // getting SIGPIPE when the other party closes the connection.
    if (Any(action & EPollControl::Read)) {
        OnSocketRead();
    }

    if (State_ == EState::Open) {
        if (ConnectionType_ == EConnectionType::Client) {
            // Client initiates a handshake.
            TryEnqueueHandshake();
        }
        ProcessQueuedMessages();
        OnSocketWrite();
    }

    YT_LOG_TRACE("Event processing finished (HasUnsentData: %v)",
        HasUnsentData());

    FlushBusStatistics();

    // Finally, clear Running flag and recheck new pending events.
    //
    // Looping here around one pollable could cause starvation for others and
    // increase latency for events already picked by this thread. So, put it
    // away into retry queue without waking other threads. This or any other
    // thread will handle it on next iteration after handling picked events.
    //
    // Do not retry processing if socket is already started shutdown sequence.
    // Retry request could be picked by thread which already passed draining.
    {
        auto previousPendingControl = static_cast<EPollControl>(PendingControl_.fetch_and(~static_cast<ui64>(EPollControl::Running)));
        YT_ASSERT(Any(previousPendingControl & EPollControl::Running));
        if (Any(previousPendingControl & ~EPollControl::Running) && None(previousPendingControl & EPollControl::Shutdown)) {
            YT_LOG_TRACE("Retrying event processing for OnEvent (PendingControl: %v)", previousPendingControl);
            Poller_->Retry(this);
        }
    }
}

void TTcpConnection::OnShutdown()
{
    // Perform the initial cleanup (the final one will be in dtor).
    Close();

    auto error = Error_.Load();
    YT_LOG_DEBUG(error, "Connection terminated");

    Terminated_.Fire(error);
}

void TTcpConnection::OnSocketRead()
{
    YT_LOG_TRACE("Started serving read request");

    bool readOnlySslAckPacket = RemainingSslAckPacketBytes_ > 0;

    size_t bytesReadTotal = 0;
    while (true) {
        // Check if the decoder is expecting a chunk of large enough size.
        auto decoderChunk = Decoder_->GetFragment();
        size_t decoderChunkSize = decoderChunk.Size();

        if (decoderChunkSize >= ReadBufferSize) {
            // Read directly into the decoder buffer.
            size_t bytesToRead = std::min(decoderChunkSize, MaxBatchReadSize);

            if (RemainingSslAckPacketBytes_ > 0) {
                bytesToRead = std::min(bytesToRead, RemainingSslAckPacketBytes_);
            }

            YT_LOG_TRACE("Reading from socket into decoder (BytesToRead: %v)", bytesToRead);

            size_t bytesRead;
            if (!ReadSocket(decoderChunk.Begin(), bytesToRead, &bytesRead)) {
                break;
            }

            bytesReadTotal += bytesRead;

            if (RemainingSslAckPacketBytes_ > 0) {
                RemainingSslAckPacketBytes_ -= bytesRead;
            }

            if (!AdvanceDecoder(bytesRead)) {
                return;
            }
        } else {
            // Read a chunk into the read buffer.
            size_t bytesToRead = ReadBuffer_.Size();

            if (RemainingSslAckPacketBytes_ > 0) {
                bytesToRead = std::min(bytesToRead, RemainingSslAckPacketBytes_);
            }

            YT_LOG_TRACE("Reading from socket into buffer (BytesToRead: %v)", bytesToRead);

            size_t bytesRead;
            if (!ReadSocket(ReadBuffer_.Begin(), bytesToRead, &bytesRead)) {
                break;
            }

            bytesReadTotal += bytesRead;

            if (RemainingSslAckPacketBytes_ > 0) {
                RemainingSslAckPacketBytes_ -= bytesRead;
            }

            // Feed the read buffer to the decoder.
            const char* recvBegin = ReadBuffer_.Begin();
            size_t recvRemaining = bytesRead;
            while (recvRemaining != 0) {
                decoderChunk = Decoder_->GetFragment();
                decoderChunkSize = decoderChunk.Size();
                size_t bytesToCopy = std::min(recvRemaining, decoderChunkSize);
                YT_LOG_TRACE("Feeding buffer into decoder (DecoderNeededBytes: %v, RemainingBufferBytes: %v, BytesToCopy: %v)",
                    decoderChunkSize,
                    recvRemaining,
                    bytesToCopy);
                std::copy(recvBegin, recvBegin + bytesToCopy, decoderChunk.Begin());
                if (!AdvanceDecoder(bytesToCopy)) {
                    return;
                }
                recvBegin += bytesToCopy;
                recvRemaining -= bytesToCopy;
            }
            YT_LOG_TRACE("Buffer exhausted");
        }

        if (readOnlySslAckPacket && RemainingSslAckPacketBytes_ == 0) {
            break;
        }
    }

    LastIncompleteReadTime_ = HasUnreadData()
        ? NProfiling::GetCpuInstant()
        : std::numeric_limits<NProfiling::TCpuInstant>::max();

    YT_LOG_TRACE("Finished serving read request (BytesReadTotal: %v)",
        bytesReadTotal);
}

bool TTcpConnection::HasUnreadData() const
{
    return Decoder_->IsInProgress();
}

ssize_t TTcpConnection::DoReadSocket(char* buffer, size_t size)
{
    switch (SslState_) {
        case ESslState::None:
            return HandleEintr(recv, Socket_, buffer, size, 0);
        case ESslState::Established: {
            auto result = SSL_read(Ssl_.get(), buffer, size);
            if (PendingSslHandshake_ && result > 0) {
                YT_LOG_DEBUG("TLS/SSL connection has been established by SSL_read");
                PendingSslHandshake_ = false;
                ReadyPromise_.TrySet();
            }
            return result;
        }
        default:
            break;
    }

    return 0;
}

bool TTcpConnection::ReadSocket(char* buffer, size_t size, size_t* bytesRead)
{
    NProfiling::TWallTimer timer;
    auto result = DoReadSocket(buffer, size);
    auto elapsed = timer.GetElapsedTime();
    if (elapsed > ReadTimeWarningThreshold) {
        YT_LOG_DEBUG("Socket read took too long (Elapsed: %v)",
            elapsed);
    }

    if (!CheckReadError(result)) {
        *bytesRead = 0;
        return false;
    }

    *bytesRead = static_cast<size_t>(result);
    UpdateBusCounter(&TBusNetworkBandCounters::InBytes, result);

    YT_LOG_TRACE("Socket read (BytesRead: %v)", *bytesRead);

    if (Config_->EnableQuickAck) {
        if (!TrySetSocketEnableQuickAck(Socket_)) {
            YT_LOG_TRACE("Failed to set socket quick ack option");
        }
    }

    return true;
}

bool TTcpConnection::CheckTcpReadError(ssize_t result)
{
    if (result == 0) {
        Abort(TError(NBus::EErrorCode::TransportError, "Socket was closed"));
        return false;
    }

    if (result < 0) {
        int error = LastSystemError();
        if (IsSocketError(error)) {
            UpdateBusCounter(&TBusNetworkBandCounters::ReadErrors, 1);
            Abort(TError(NBus::EErrorCode::TransportError, "Socket read error")
                << TError::FromSystem(error));
        }
        return false;
    }

    return true;
}

bool TTcpConnection::CheckReadError(ssize_t result)
{
    if (SslState_ == ESslState::None) {
        return CheckTcpReadError(result);
    }

    return CheckSslReadError(result);
}

bool TTcpConnection::AdvanceDecoder(size_t size)
{
    if (!Decoder_->Advance(size)) {
        UpdateBusCounter(&TBusNetworkBandCounters::DecoderErrors, 1);
        Abort(TError(NBus::EErrorCode::TransportError, "Error decoding incoming packet"));
        return false;
    }

    if (Config_->PacketDecoderDelay) {
        YT_LOG_WARNING("Test delay in tcp connection packet decoder (Delay: %v)", Config_->PacketDecoderDelay);
        TDelayedExecutor::WaitForDuration(Config_->PacketDecoderDelay.value());
    }

    if (Decoder_->IsFinished()) {
        bool result = OnPacketReceived();
        Decoder_->Restart();
        return result;
    }

    return true;
}

bool TTcpConnection::OnPacketReceived() noexcept
{
    UpdateBusCounter(&TBusNetworkBandCounters::InPackets, 1);
    switch (Decoder_->GetPacketType()) {
        case EPacketType::Ack:
            return OnAckPacketReceived();
        case EPacketType::SslAck:
            return OnSslAckPacketReceived();
        case EPacketType::Message:
            return OnMessagePacketReceived();
        default:
            YT_LOG_ERROR("Packet of unknown type received, ignored (PacketId: %v, PacketType: %v)",
                Decoder_->GetPacketId(),
                Decoder_->GetPacketType());
            break;
    }

    return false;
}

bool TTcpConnection::OnAckPacketReceived()
{
    if (UnackedPackets_.empty()) {
        Abort(TError(NBus::EErrorCode::TransportError, "Unexpected ack received"));
        return false;
    }

    auto& unackedMessage = UnackedPackets_.front();

    if (Decoder_->GetPacketId() != unackedMessage->PacketId) {
        Abort(TError(
            NBus::EErrorCode::TransportError,
            "Ack for invalid packet ID received: expected %v, found %v",
            unackedMessage->PacketId,
            Decoder_->GetPacketId()));
        return false;
    }

    YT_LOG_DEBUG("Ack received (PacketId: %v)", Decoder_->GetPacketId());

    if (unackedMessage->Promise) {
        unackedMessage->Promise.TrySet(TError());
    }

    UnackedPackets_.pop();

    return true;
}

bool TTcpConnection::OnMessagePacketReceived()
{
    // COMPAT(babenko)
    if (Decoder_->GetPacketId() == HandshakePacketId) {
        return OnHandshakePacketReceived();
    }

    YT_LOG_DEBUG("Incoming message received (PacketId: %v, PacketSize: %v, PacketFlags: %v)",
        Decoder_->GetPacketId(),
        Decoder_->GetPacketSize(),
        Decoder_->GetPacketFlags());

    if (Any(Decoder_->GetPacketFlags() & EPacketFlags::RequestAcknowledgement)) {
        EnqueuePacket(EPacketType::Ack, EPacketFlags::None, 0, Decoder_->GetPacketId());
    }

    auto message = Decoder_->GrabMessage();
    Handler_->HandleMessage(std::move(message), this);

    return true;
}

bool TTcpConnection::OnHandshakePacketReceived()
{
    YT_LOG_DEBUG("Handshake received");

    auto optionalHandshake = TryParseHandshakeMessage(Decoder_->GrabMessage());
    if (!optionalHandshake) {
        return false;
    }

    const auto& handshake = *optionalHandshake;
    auto optionalMultiplexingBand = handshake.has_multiplexing_band()
        ? std::make_optional(FromProto<EMultiplexingBand>(handshake.multiplexing_band()))
        : std::nullopt;

    PeerAttributes_ = ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("peer_connection_id").Value(FromProto<TConnectionId>(handshake.connection_id()))
                .Item("peer_encryption_mode").Value(FromProto<EEncryptionMode>(handshake.encryption_mode()))
                .Item("peer_verification_mode").Value(FromProto<EVerificationMode>(handshake.verification_mode()))
            .EndMap());

    YT_LOG_DEBUG("Handshake received (PeerConnectionId: %v, PeerEncryptionMode: %v, PeerVerificationMode: %v, MultiplexingBand: %v)",
        PeerAttributes_->Get<TString>("peer_connection_id"),
        PeerAttributes_->Get<TString>("peer_encryption_mode"),
        PeerAttributes_->Get<TString>("peer_verification_mode"),
        optionalMultiplexingBand);

    if (ConnectionType_ == EConnectionType::Server && optionalMultiplexingBand) {
        auto guard = Guard(Lock_);
        UpdateConnectionCount(-1);
        MultiplexingBand_.store(*optionalMultiplexingBand);
        UpdateConnectionCount(+1);
    }

    HandshakeReceived_ = true;

    if (ConnectionType_ == EConnectionType::Server) {
        // Server responds to client's handshake.
        TryEnqueueHandshake();
    }

    auto otherEncryptionMode = handshake.has_encryption_mode() ? FromProto<EEncryptionMode>(handshake.encryption_mode()) : EEncryptionMode::Disabled;

    if (EncryptionMode_ == EEncryptionMode::Required || otherEncryptionMode == EEncryptionMode::Required) {
        if (EncryptionMode_ == EEncryptionMode::Disabled || otherEncryptionMode == EEncryptionMode::Disabled) {
            Abort(TError(NBus::EErrorCode::SslError, "TLS/SSL client/server encryption mode compatibility error")
                << TErrorAttribute("mode", EncryptionMode_)
                << TErrorAttribute("other_mode", otherEncryptionMode));
        } else {
            EstablishSslSession_ = true;
            // Expect ssl ack from the other side.
            RemainingSslAckPacketBytes_ = GetSslAckPacketSize();
            if (ConnectionType_ == EConnectionType::Client) {
                // Client initiates ssl ack.
                TryEnqueueSslAck();
            }

            // ReadyPromise_ is set either after successful establishment of ssl session or in Abort().
        }
    } else {
        ReadyPromise_.TrySet();
    }

    return true;
}

TTcpConnection::TPacket* TTcpConnection::EnqueuePacket(
    EPacketType type,
    EPacketFlags flags,
    int checksummedPartCount,
    TPacketId packetId,
    TSharedRefArray message,
    size_t payloadSize)
{
    size_t packetSize = Encoder_->GetPacketSize(type, message, payloadSize);
    auto packetHolder = New<TPacket>(
        type,
        flags,
        checksummedPartCount,
        packetId,
        std::move(message),
        payloadSize,
        packetSize);
    auto* packet = packetHolder.Get();
    QueuedPackets_.push(std::move(packetHolder));
    IncrementPendingOut(packetSize);
    return packet;
}

void TTcpConnection::OnSocketWrite()
{
    YT_LOG_TRACE("Started serving write request");

    size_t bytesWrittenTotal = 0;
    while (true) {
        if (!HasUnsentData()) {
            // Unarm stall detection at end of write
            LastIncompleteWriteTime_ = std::numeric_limits<NProfiling::TCpuInstant>::max();
            break;
        }

        if (!MaybeEncodeFragments()) {
            break;
        }

        size_t bytesWritten;
        bool success = WriteFragments(&bytesWritten);
        bytesWrittenTotal += bytesWritten;

        FlushWrittenFragments(bytesWritten);
        FlushWrittenPackets(bytesWritten);

        if (bytesWritten) {
            // Rearm stall detection after progress.
            LastIncompleteWriteTime_ = NProfiling::GetCpuInstant();
        }

        if (!success) {
            break;
        }
    }

    YT_LOG_TRACE("Finished serving write request (BytesWrittenTotal: %v)", bytesWrittenTotal);
}

bool TTcpConnection::HasUnsentData() const
{
    return !EncodedFragments_.empty() || !QueuedPackets_.empty() || !EncodedPackets_.empty();
}

ssize_t TTcpConnection::DoWriteFragments(const std::vector<struct iovec>& vec)
{
    if (vec.empty()) {
        return 0;
    }

    switch (SslState_) {
        case ESslState::None:
            return HandleEintr(::writev, Socket_, vec.data(), vec.size());
        case ESslState::Established: {
            YT_ASSERT(vec.size() == 1);
            auto result = SSL_write(Ssl_.get(), vec[0].iov_base, vec[0].iov_len);
            if (PendingSslHandshake_ && result > 0) {
                YT_LOG_DEBUG("TLS/SSL connection has been established by SSL_write");
                PendingSslHandshake_ = false;
                ReadyPromise_.TrySet();
            }
            return result;
        }
        default:
            break;
    }

    return 0;
}

bool TTcpConnection::WriteFragments(size_t* bytesWritten)
{
    YT_LOG_TRACE("Writing fragments (EncodedFragments: %v)", EncodedFragments_.size());

    auto fragmentIt = EncodedFragments_.begin();
    auto fragmentEnd = EncodedFragments_.end();

    SendVector_.clear();
    size_t bytesAvailable = MaxBatchWriteSize;

    while (fragmentIt != fragmentEnd &&
           SendVector_.size() < MaxFragmentsPerWrite_ &&
           bytesAvailable > 0)
    {
        const auto& fragment = *fragmentIt;
        size_t size = std::min(fragment.Size(), bytesAvailable);
        struct iovec item;
        item.iov_base = const_cast<char*>(fragment.Begin());
        item.iov_len = size;
        SendVector_.push_back(item);
        EncodedFragments_.move_forward(fragmentIt);
        bytesAvailable -= size;
    }

    NProfiling::TWallTimer timer;
    auto result = DoWriteFragments(SendVector_);
    auto elapsed = timer.GetElapsedTime();
    if (elapsed > WriteTimeWarningThreshold) {
        YT_LOG_DEBUG("Socket write took too long (Elapsed: %v)",
            elapsed);
    }

    *bytesWritten = result >= 0 ? static_cast<size_t>(result) : 0;
    bool isOK = CheckWriteError(result);
    if (isOK) {
        UpdateBusCounter(&TBusNetworkBandCounters::OutBytes, *bytesWritten);
        YT_LOG_TRACE("Socket written (BytesWritten: %v)", *bytesWritten);
    }
    return isOK;
}

void TTcpConnection::FlushWrittenFragments(size_t bytesWritten)
{
    size_t bytesToFlush = bytesWritten;
    YT_LOG_TRACE("Flushing fragments (BytesWritten: %v)", bytesWritten);

    while (bytesToFlush != 0) {
        YT_ASSERT(!EncodedFragments_.empty());
        auto& fragment = EncodedFragments_.front();

        if (fragment.Size() > bytesToFlush) {
            size_t bytesRemaining = fragment.Size() - bytesToFlush;
            YT_LOG_TRACE("Partial write (Size: %v, RemainingSize: %v)",
                fragment.Size(),
                bytesRemaining);
            fragment = TRef(fragment.End() - bytesRemaining, bytesRemaining);
            break;
        }

        YT_LOG_TRACE("Full write (Size: %v)", fragment.Size());

        bytesToFlush -= fragment.Size();
        EncodedFragments_.pop();
    }
}

void TTcpConnection::FlushWrittenPackets(size_t bytesWritten)
{
    size_t bytesToFlush = bytesWritten;
    YT_LOG_TRACE("Flushing packets (BytesWritten: %v)", bytesWritten);

    while (bytesToFlush != 0) {
        YT_ASSERT(!EncodedPacketSizes_.empty());
        auto& packetSize = EncodedPacketSizes_.front();

        if (packetSize > bytesToFlush) {
            size_t bytesRemaining = packetSize - bytesToFlush;
            YT_LOG_TRACE("Partial write (Size: %v, RemainingSize: %v)",
                packetSize,
                bytesRemaining);
            packetSize = bytesRemaining;
            break;
        }

        YT_LOG_TRACE("Full write (Size: %v)", packetSize);

        bytesToFlush -= packetSize;
        OnPacketSent();
        EncodedPacketSizes_.pop();
    }
}

bool TTcpConnection::MaybeEncodeFragments()
{
    if (!EncodedFragments_.empty() || QueuedPackets_.empty()) {
        return true;
    }

    // Discard all buffers except for a single one.
    WriteBuffers_.resize(1);
    auto* buffer = &WriteBuffers_.back();
    buffer->Clear();

    size_t encodedSize = 0;
    size_t coalescedSize = 0;

    auto flushCoalesced = [&] {
        if (coalescedSize > 0) {
            EncodedFragments_.push(TRef(buffer->End() - coalescedSize, coalescedSize));
            coalescedSize = 0;
        }
    };

    auto coalesce = [&] (TRef fragment) {
        if (buffer->Size() + fragment.Size() > buffer->Capacity()) {
            // Make sure we never reallocate.
            flushCoalesced();

            auto size = std::max(WriteBufferSize, fragment.Size());

            auto trackedBlob = TMemoryTrackedBlob::Build(
                MemoryUsageTracker_,
                ConnectionType_ == EConnectionType::Server
                    ? GetRefCountedTypeCookie<TTcpServerConnectionWriteBufferTag>()
                    : GetRefCountedTypeCookie<TTcpClientConnectionWriteBufferTag>());
            trackedBlob.Reserve(size);

            WriteBuffers_.push_back(std::move(trackedBlob));
            buffer = &WriteBuffers_.back();
        }

        buffer->Append(fragment);
        coalescedSize += fragment.Size();
    };

    while (EncodedFragments_.size() < MaxFragmentsPerWrite_ &&
           encodedSize <= MaxBatchWriteSize &&
           !QueuedPackets_.empty())
    {
        auto& queuedPacket = QueuedPackets_.front();
        YT_LOG_TRACE("Checking packet cancel state (PacketId: %v)", queuedPacket->PacketId);
        if (!queuedPacket->MarkEncoded()) {
            YT_LOG_TRACE("Packet was canceled (PacketId: %v)", queuedPacket->PacketId);
            QueuedPackets_.pop();
            continue;
        }

        // Move the packet from queued to encoded.
        if (Any(queuedPacket->Flags & EPacketFlags::RequestAcknowledgement)) {
            UnackedPackets_.push(queuedPacket);
        }
        EncodedPackets_.push(std::move(queuedPacket));
        QueuedPackets_.pop();

        const auto& packet = EncodedPackets_.back();

        // Encode the packet.
        YT_LOG_TRACE("Starting encoding packet (PacketId: %v)", packet->PacketId);

        bool encodeResult = Encoder_->Start(
            packet->Type,
            packet->Flags,
            GenerateChecksums_,
            packet->ChecksummedPartCount,
            packet->PacketId,
            packet->Message);
        if (!encodeResult) {
            UpdateBusCounter(&TBusNetworkBandCounters::EncoderErrors, 1);
            Abort(TError(NBus::EErrorCode::TransportError, "Error encoding outcoming packet"));
            return false;
        }

        do {
            auto fragment = Encoder_->GetFragment();
            if (!Encoder_->IsFragmentOwned() || fragment.Size() <= MaxWriteCoalesceSize) {
                coalesce(fragment);
            } else {
                flushCoalesced();
                EncodedFragments_.push(fragment);
            }
            YT_LOG_TRACE("Fragment encoded (Size: %v)", fragment.Size());
            Encoder_->NextFragment();
        } while (!Encoder_->IsFinished());

        EncodedPacketSizes_.push(packet->PacketSize);
        encodedSize += packet->PacketSize;

        YT_LOG_TRACE("Finished encoding packet (PacketId: %v)", packet->PacketId);
    }

    flushCoalesced();

    return true;
}

bool TTcpConnection::CheckTcpWriteError(ssize_t result)
{
    if (result < 0) {
        int error = LastSystemError();
        if (IsSocketError(error)) {
            UpdateBusCounter(&TBusNetworkBandCounters::WriteErrors, 1);
            Abort(TError(NBus::EErrorCode::TransportError, "Socket write error")
                << TError::FromSystem(error));
        }
        return false;
    }

    return true;
}

bool TTcpConnection::CheckWriteError(ssize_t result)
{
    if (SslState_ == ESslState::None) {
        return CheckTcpWriteError(result);
    }

    return CheckSslWriteError(result);
}

void TTcpConnection::OnPacketSent()
{
    const auto& packet = EncodedPackets_.front();
    switch (packet->Type) {
        case EPacketType::Ack:
            OnAckPacketSent(*packet);
            break;

        case EPacketType::SslAck:
            OnSslAckPacketSent();
            break;

        case EPacketType::Message:
            // COMPAT(babenko)
            if (packet->PacketId == HandshakePacketId) {
                OnHandshakePacketSent();
            } else {
                OnMessagePacketSent(*packet);
            }
            break;

        default:
            YT_ABORT();
    }

    DecrementPendingOut(packet->PacketSize);
    UpdateBusCounter(&TBusNetworkBandCounters::OutPackets, 1);

    EncodedPackets_.pop();
}

void  TTcpConnection::OnAckPacketSent(const TPacket& packet)
{
    YT_LOG_DEBUG("Ack sent (PacketId: %v)",
        packet.PacketId);
}

void TTcpConnection::OnMessagePacketSent(const TPacket& packet)
{
    YT_LOG_DEBUG("Outcoming message sent (PacketId: %v)",
        packet.PacketId);

    PendingOutPayloadBytes_.fetch_sub(packet.PayloadSize);

    // Arm read stall timeout for incoming ACK.
    if (Any(packet.Flags & EPacketFlags::RequestAcknowledgement) &&
        LastIncompleteReadTime_ == std::numeric_limits<NProfiling::TCpuInstant>::max())
    {
        LastIncompleteReadTime_ = NProfiling::GetCpuInstant();
    }
}

void TTcpConnection::OnHandshakePacketSent()
{
    YT_LOG_DEBUG("Handshake sent");

    HandshakeSent_ = true;
}

void TTcpConnection::OnTerminate()
{
    if (State_ == EState::Aborted || State_ == EState::Closed) {
        return;
    }

    YT_LOG_DEBUG("Termination request received");

    Abort(Error_.Load());
}

void TTcpConnection::ProcessQueuedMessages()
{
    auto messages = QueuedMessages_.DequeueAll();

    for (auto it = messages.rbegin(); it != messages.rend(); ++it) {
        auto& queuedMessage = *it;

        auto packetId = queuedMessage.PacketId;
        auto flags = queuedMessage.Options.TrackingLevel == EDeliveryTrackingLevel::Full
            ? EPacketFlags::RequestAcknowledgement
            : EPacketFlags::None;

        auto* packet = EnqueuePacket(
            EPacketType::Message,
            flags,
            GenerateChecksums_ ? queuedMessage.Options.ChecksummedPartCount : 0,
            packetId,
            std::move(queuedMessage.Message),
            queuedMessage.PayloadSize);

        packet->Promise = queuedMessage.Promise;
        if (queuedMessage.Options.EnableSendCancelation) {
            packet->EnableCancel(MakeStrong(this));
        }

        YT_LOG_DEBUG("Outcoming message dequeued (PacketId: %v, PacketSize: %v, Flags: %v)",
            packetId,
            packet->PacketSize,
            flags);

        if (queuedMessage.Promise && !queuedMessage.Options.EnableSendCancelation && !Any(flags & EPacketFlags::RequestAcknowledgement)) {
            queuedMessage.Promise.TrySet();
        }
    }
}

void TTcpConnection::DiscardOutcomingMessages()
{
    auto error = Error_.Load();

    auto guard = Guard(QueuedMessagesDiscardLock_);
    auto queuedMessages = QueuedMessages_.DequeueAll();
    guard.Release();

    for (const auto& queuedMessage : queuedMessages) {
        YT_LOG_DEBUG("Outcoming message discarded (PacketId: %v)",
            queuedMessage.PacketId);
        if (queuedMessage.Promise) {
            queuedMessage.Promise.TrySet(error);
        }
    }
}

void TTcpConnection::DiscardUnackedMessages()
{
    auto error = Error_.Load();


    while (!UnackedPackets_.empty()) {
        auto& message = UnackedPackets_.front();
        if (message->Promise) {
            message->Promise.TrySet(error);
        }
        UnackedPackets_.pop();
    }
}

int TTcpConnection::GetSocketError() const
{
    return NNet::GetSocketError(Socket_);
}

bool TTcpConnection::IsSocketError(ssize_t result)
{
    return
        result != EWOULDBLOCK &&
        result != EAGAIN &&
        result != EINPROGRESS;
}

void TTcpConnection::CloseSocket()
{
    VERIFY_SPINLOCK_AFFINITY(Lock_);

    if (Socket_ != INVALID_SOCKET) {
        NNet::CloseSocket(Socket_);
        Socket_ = INVALID_SOCKET;
    }
}

void TTcpConnection::ArmPoller()
{
    VERIFY_SPINLOCK_AFFINITY(Lock_);
    YT_VERIFY(Socket_ != INVALID_SOCKET);

    Poller_->Arm(Socket_, this, EPollControl::Read | EPollControl::Write | EPollControl::EdgeTriggered);
}

void TTcpConnection::UnarmPoller()
{
    VERIFY_SPINLOCK_AFFINITY(Lock_);

    if (Socket_ != INVALID_SOCKET) {
        Poller_->Unarm(Socket_, this);
    }
}

void TTcpConnection::InitSocketTosLevel(TTosLevel tosLevel)
{
    if (TosLevel_ == BlackHoleTosLevel && tosLevel != BlackHoleTosLevel) {
        if (!TrySetSocketInputFilter(Socket_, false)) {
            YT_LOG_DEBUG("Failed to remove socket input filter");
        }
    }

    if (tosLevel == BlackHoleTosLevel) {
        if (TrySetSocketInputFilter(Socket_, true)) {
            YT_LOG_DEBUG("Socket TOS level set to BlackHole");
        } else {
            YT_LOG_DEBUG("Failed to set socket input filter");
        }
    } else if (TrySetSocketTosLevel(Socket_, tosLevel)) {
        YT_LOG_DEBUG("Socket TOS level set (TosLevel: %x)",
            tosLevel);
    } else {
        YT_LOG_DEBUG("Failed to set socket TOS level");
    }
}

void TTcpConnection::FlushStatistics()
{
    VERIFY_SPINLOCK_AFFINITY(Lock_);
    UpdateTcpStatistics();
    FlushBusStatistics();
}

template <class T, class U>
i64 TTcpConnection::UpdateBusCounter(T TBusNetworkBandCounters::* field, U delta)
{
    auto band = MultiplexingBand_.load(std::memory_order::relaxed);
    (BusCountersDelta_.PerBandCounters[band].*field).fetch_add(delta, std::memory_order::relaxed);
    return (BusCounters_.PerBandCounters[band].*field).fetch_add(delta, std::memory_order::relaxed) + delta;
}

void TTcpConnection::UpdateTcpStatistics()
{
#ifdef _linux_
    if (Socket_ != INVALID_SOCKET) {
        tcp_info info;
        socklen_t len = sizeof(info);
        int ret = ::getsockopt(Socket_, IPPROTO_TCP, TCP_INFO, &info, &len);
        if (ret == 0) {
            // Handle counter overflow.
            i64 delta = info.tcpi_total_retrans < LastRetransmitCount_
                ? info.tcpi_total_retrans + (Max<ui32>() - LastRetransmitCount_)
                : info.tcpi_total_retrans - LastRetransmitCount_;
            UpdateBusCounter(&TBusNetworkBandCounters::Retransmits, delta);
            LastRetransmitCount_ = info.tcpi_total_retrans;
        }
    }
#else
    Y_UNUSED(LastRetransmitCount_);
#endif
}

void TTcpConnection::FlushBusStatistics()
{
    auto networkCounters = NetworkCounters_.Acquire();
    if (!networkCounters) {
        return;
    }

    for (auto band : TEnumTraits<EMultiplexingBand>::GetDomainValues()) {
#define XX(camelCaseField, snakeCaseField) networkCounters->PerBandCounters[band].camelCaseField.fetch_add(BusCountersDelta_.PerBandCounters[band].camelCaseField.exchange(0));
        ITERATE_BUS_NETWORK_STATISTICS_FIELDS(XX)
#undef XX
    }
}

////////////////////////////////////////////////////////////////////////////////

void TTcpConnection::AbortSslSession()
{
    CloseSslSession(ESslState::Aborted);
}

bool TTcpConnection::CheckSslReadError(ssize_t result)
{
    switch (SSL_get_error(Ssl_.get(), result)) {
        case SSL_ERROR_NONE:
            return true;
        case SSL_ERROR_WANT_READ:
        case SSL_ERROR_WANT_WRITE:
            // Try again.
            break;
        case SSL_ERROR_SYSCALL:
        case SSL_ERROR_SSL:
            // This check is probably unnecessary in new versions of openssl.
            if (errno == EINTR || errno == EWOULDBLOCK || errno == EAGAIN) {
                break;
            }
            SslState_ = ESslState::Error;
            [[fallthrough]];
        default:
            UpdateBusCounter(&TBusNetworkBandCounters::ReadErrors, 1);
            Abort(TError(NBus::EErrorCode::SslError, "TLS/SSL read error")
                << TErrorAttribute("ssl_error", GetLastSslErrorString())
                << TErrorAttribute("sys_error", TError::FromSystem(LastSystemError())));
            break;
    }

    return false;
}

bool TTcpConnection::CheckSslWriteError(ssize_t result)
{
    switch (SSL_get_error(Ssl_.get(), result)) {
        case SSL_ERROR_NONE:
            return true;
        case SSL_ERROR_WANT_READ:
        case SSL_ERROR_WANT_WRITE:
            // Try again.
            break;
        case SSL_ERROR_SYSCALL:
        case SSL_ERROR_SSL:
            // This check is probably unnecessary in new versions of openssl.
            if (errno == EINTR || errno == EWOULDBLOCK || errno == EAGAIN) {
                break;
            }
            SslState_ = ESslState::Error;
            [[fallthrough]];
        default:
            UpdateBusCounter(&TBusNetworkBandCounters::WriteErrors, 1);
            Abort(TError(NBus::EErrorCode::SslError, "TLS/SSL write error")
                << TErrorAttribute("ssl_error", GetLastSslErrorString())
                << TErrorAttribute("sys_error", TError::FromSystem(LastSystemError())));
            break;
    }

    return false;
}

void TTcpConnection::CloseSslSession(ESslState newSslState)
{
    switch (SslState_) {
        case ESslState::None:
        case ESslState::Aborted:
        case ESslState::Closed:
            // Nothing to do.
            return;
        case ESslState::Established:
            SSL_shutdown(Ssl_.get());
            break;
        case ESslState::Error:
            break;
        default:
            YT_ABORT();
    }

    SslState_ = newSslState;
}

bool TTcpConnection::DoSslHandshake()
{
    auto result = SSL_do_handshake(Ssl_.get());
    switch (SSL_get_error(Ssl_.get(), result)) {
        case SSL_ERROR_NONE:
            YT_LOG_DEBUG("TLS/SSL connection has been established by SSL_do_handshake");
            MaxFragmentsPerWrite_ = 1;
            SslState_ = ESslState::Established;
            ReadyPromise_.TrySet();
            return false;
        case SSL_ERROR_WANT_READ:
        case SSL_ERROR_WANT_WRITE:
            MaxFragmentsPerWrite_ = 1;
            SslState_ = ESslState::Established;
            // Ssl session establishment will be finished by the following SSL_do_handshake()/SSL_read()/SSL_write() calls.
            // Since SSL handshake is pending, don't set the ReadyPromise_ yet.
            return true;
        case SSL_ERROR_ZERO_RETURN:
            // The TLS/SSL peer has closed the TLS/SSL session.
            break;
        case SSL_ERROR_SYSCALL:
        case SSL_ERROR_SSL:
            SslState_ = ESslState::Error;
            break;
        default:
            // Use default handler for other error types.
            break;
    }

    Abort(TError(NBus::EErrorCode::SslError, "Failed to establish TLS/SSL session")
        << TErrorAttribute("ssl_error", GetLastSslErrorString())
        << TErrorAttribute("sys_error", TError::FromSystem(LastSystemError())));
    return false;
}

size_t TTcpConnection::GetSslAckPacketSize()
{
    return Encoder_->GetPacketSize(EPacketType::SslAck, {}, 0);
}

void TTcpConnection::TryEnqueueSslAck()
{
    if (!HandshakeSent_ || !HandshakeReceived_ || !EstablishSslSession_) {
        return;
    }

    if (std::exchange(SslAckEnqueued_, true)) {
        return;
    }

    EnqueuePacket(
        // COMPAT(babenko)
        EPacketType::SslAck,
        EPacketFlags::None,
        0,
        SslAckPacketId);

    YT_LOG_DEBUG("TLS/SSL acknowledgement enqueued");
}

void TTcpConnection::TryEstablishSslSession()
{
    if (!SslAckReceived_ || !SslAckSent_ || Ssl_) {
        return;
    }

    YT_LOG_DEBUG("Starting TLS/SSL connection");

    if (Config_->LoadCertsFromBusCertsDirectory && !TTcpDispatcher::TImpl::Get()->GetBusCertsDirectoryPath()) {
        Abort(TError(NBus::EErrorCode::SslError, "bus_certs_directory_path is not set in tcp_dispatcher config"));
        return;
    }

    Ssl_.reset(SSL_new(TSslContext::Get()->GetSslCtx()));
    if (!Ssl_) {
        Abort(TError(NBus::EErrorCode::SslError, "Failed to create a new SSL structure: %v", GetLastSslErrorString()));
        return;
    }

    if (SSL_set_fd(Ssl_.get(), Socket_) != 1) {
        Abort(TError(NBus::EErrorCode::SslError, "Failed to bind socket to SSL handle: %v", GetLastSslErrorString()));
        return;
    }

    if (Config_->CipherList) {
        if (SSL_set_cipher_list(Ssl_.get(), Config_->CipherList->data()) != 1) {
            Abort(TError(NBus::EErrorCode::SslError, "Failed to set cipher list: %v", GetLastSslErrorString()));
            return;
        }
    }

#define GET_CERT_FILE_PATH(file)    \
    (Config_->LoadCertsFromBusCertsDirectory ? JoinPaths(*TTcpDispatcher::TImpl::Get()->GetBusCertsDirectoryPath(), (file)) : (file))

    if (ConnectionType_ == EConnectionType::Server) {
        SSL_set_accept_state(Ssl_.get());

        if (!Config_->CertificateChain) {
            Abort(TError(NBus::EErrorCode::SslError, "Certificate chain file is not set in bus config"));
            return;
        }

        if (Config_->CertificateChain->FileName) {
            const auto& certChainFile = GET_CERT_FILE_PATH(*Config_->CertificateChain->FileName);
            if (SSL_use_certificate_chain_file(Ssl_.get(), certChainFile.data()) != 1) {
                Abort(TError(NBus::EErrorCode::SslError, "Failed to load certificate chain file: %v", GetLastSslErrorString()));
                return;
            }
        } else {
            if (!UseCertificateChain(*Config_->CertificateChain->Value, Ssl_.get())) {
                Abort(TError(NBus::EErrorCode::SslError, "Failed to load certificate chain: %v", GetLastSslErrorString()));
                return;
            }
        }

        if (!Config_->PrivateKey) {
            Abort(TError(NBus::EErrorCode::SslError, "The private key file is not set in bus config"));
            return;
        }

        if (Config_->PrivateKey->FileName) {
            const auto& privateKeyFile = GET_CERT_FILE_PATH(*Config_->PrivateKey->FileName);
            if (SSL_use_PrivateKey_file(Ssl_.get(), privateKeyFile.data(), SSL_FILETYPE_PEM) != 1) {
                Abort(TError(NBus::EErrorCode::SslError, "Failed to load private key file: %v", GetLastSslErrorString()));
                return;
            }
        } else {
            if (!UsePrivateKey(*Config_->PrivateKey->Value, Ssl_.get())) {
                Abort(TError(NBus::EErrorCode::SslError, "Failed to load private key: %v", GetLastSslErrorString()));
                return;
            }
        }

        if (SSL_check_private_key(Ssl_.get()) != 1) {
            Abort(TError(NBus::EErrorCode::SslError, "Failed to check the consistency of a private key with the corresponding certificate: %v", GetLastSslErrorString()));
            return;
        }
    } else {
        SSL_set_connect_state(Ssl_.get());
    }

    switch (VerificationMode_) {
        case EVerificationMode::Full:
            // Because of the implementation of check_id() from libs/openssl/crypto/x509/x509_vfy.c,
            // we can not set both IP and host checks. So we separate them as follows.
            if (Config_->PeerAlternativeHostName) {
                // Set hostname for peer certificate verification.
                if (SSL_set1_host(Ssl_.get(), EndpointHostName_.c_str()) != 1) {
                    Abort(TError(NBus::EErrorCode::SslError, "Failed to set hostname %v for peer certificate verification", EndpointHostName_));
                    return;
                }

                // Add alternative hostname for peer certificate verification.
                if (SSL_add1_host(Ssl_.get(), Config_->PeerAlternativeHostName->c_str()) != 1) {
                    Abort(TError(NBus::EErrorCode::SslError, "Failed to add alternative hostname %v for peer certificate verification", *Config_->PeerAlternativeHostName));
                    return;
                }
            } else if (auto networkAddress = TNetworkAddress::TryParse(EndpointHostName_); networkAddress.IsOK() && networkAddress.Value().IsIP()) {
                // Set IP address for peer certificate verification.
                auto address = ToString(networkAddress.Value(), {.IncludePort = false, .IncludeTcpProtocol = false});
                if (X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(Ssl_.get()), address.c_str()) != 1) {
                    Abort(TError(NBus::EErrorCode::SslError, "Failed to set IP address %v for peer certificate verification", address));
                    return;
                }
            } else {
                // Set hostname for peer certificate verification.
                if (SSL_set1_host(Ssl_.get(), EndpointHostName_.c_str()) != 1) {
                    Abort(TError(NBus::EErrorCode::SslError, "Failed to set hostname %v for peer certificate verification", EndpointHostName_));
                    return;
                }
            }
            [[fallthrough]];
        case EVerificationMode::Ca: {
            if (!Config_->CA) {
                Abort(TError(NBus::EErrorCode::SslError, "CA file is not set in bus config"));
                return;
            }

            if (Config_->CA->FileName) {
                const auto& caFile = GET_CERT_FILE_PATH(*Config_->CA->FileName);
                TSslContext::Get()->LoadCAFileIfNotLoaded(caFile);
            } else {
                TSslContext::Get()->UseCAIfNotUsed(*Config_->CA->Value);
            }

            // Enable verification of the peer's certificate with the CA.
            SSL_set_verify(Ssl_.get(), SSL_VERIFY_PEER, /*callback*/ nullptr);
            break;
        }
        case EVerificationMode::None:
            break;
        default:
            YT_ABORT();
    }

    PendingSslHandshake_ = DoSslHandshake();

    // Check that connection hasn't been aborted in DoSslHandshake().
    if (State_ == EState::Open) {
        UpdateConnectionCount(-1);
        FlushBusStatistics();
        NetworkCounters_.Exchange(TTcpDispatcher::TImpl::Get()->GetCounters(NetworkName_, IsEncrypted()));
        UpdateConnectionCount(1);
    }

#undef GET_CERT_FILE_PATH
}

bool TTcpConnection::OnSslAckPacketReceived()
{
    YT_LOG_DEBUG("TLS/SSL acknowledgement received");

    SslAckReceived_ = true;

    if (ConnectionType_ == EConnectionType::Server) {
        // Server responds to client's ssl ack.
        TryEnqueueSslAck();
    }

    TryEstablishSslSession();

    return true;
}

void TTcpConnection::OnSslAckPacketSent()
{
    YT_LOG_DEBUG("TLS/SSL acknowledgement sent");

    SslAckSent_ = true;

    TryEstablishSslSession();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
