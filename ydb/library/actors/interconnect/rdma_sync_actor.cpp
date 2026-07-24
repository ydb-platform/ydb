#include "events_local.h"
#include "rdma_sync_actor.h"
#include "interconnect_tcp_session.h"
#include "interconnect_tcp_proxy.h"
#include "packet.h"

#include <ydb/library/actors/core/actor_coroutine.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/protos/interconnect.pb.h>

#include <variant>

namespace NActors {

    TInterconnectSessionRdma* TRdmaHandshakeResult::ReleasePreinitedSession() noexcept {
        auto ok = std::get_if<TOk>(&Result);
        Y_ABORT_UNLESS(ok);
        return ok->PreinitedSession.release();
    }

    TEvRdmaSyncResult::TEvRdmaSyncResult(TRdmaPreinitedSessionPtr session)
        : Session(std::move(session))
    {}

    TEvRdmaSyncResult::TEvRdmaSyncResult(TString error)
        : Session(std::unexpected(std::move(error)))
    {}

    std::optional<TString> TEvRdmaSyncResult::Error() const {
        if (!Session.has_value()) {
            return Session.error();
        }
        return {};
    }

    TRdmaPreinitedSessionPtr TEvRdmaSyncResult::ExtractSession() {
        return std::move(Session.value());
    }
}

namespace NInterconnect::NRdma {

namespace {
    using namespace NActors;
    static constexpr ui32 StackSize = 64 * 1024;
    static constexpr ui32 SyncProtocolVersion = 1;
    static constexpr ui64 SyncFeatureMask = 0;
    static constexpr size_t DataSyncPayloadSize = 64;

    enum class ESyncTcpMessageType : ui32 {
        StartSync = 1,
        FinSync = 2,
        ErrSync = 3,
    };

    enum class ESyncRdmaMessageType : ui32 {
        DataSync = 1,
        AckSync = 2,
    };

    struct TSyncMsgHeader {
        static constexpr ui32 MaxSize = 1024 * 1024;

        ui64 Checksum = 0;
        ui32 Size = 0;
        ui32 MessageType = 0;

        ui64 Y_NO_INLINE CalculateChecksum(const void* data, size_t len) const {
            XXH3_state_t state;
            XXH3_64bits_reset(&state);
            XXH3_64bits_update(&state, &Size, sizeof(Size));
            XXH3_64bits_update(&state, &MessageType, sizeof(MessageType));
            XXH3_64bits_update(&state, data, len);
            return XXH3_64bits_digest(&state);
        }

        void Sign(const void* data, size_t len) {
            Checksum = CalculateChecksum(data, len);
        }

        bool Check(const void* data, size_t len) const {
            return Checksum == CalculateChecksum(data, len);
        }
    };

    struct TRdmaSyncPacket {
        ESyncRdmaMessageType MessageType;
        ui64 Checksum = 0;
        TString Data;
    };

    class TSessionCreatorDelegate final : public NActors::TEvProxyCall {
    public:
        TSessionCreatorDelegate(
                TActorId syncActor,
                NInterconnect::NRdma::TQueuePair::TPtr qp,
                NInterconnect::NRdma::ICq::TPtr cq,
                ui32 peerNodeId)
            : SyncActor(syncActor)
            , Qp(std::move(qp))
            , Cq(std::move(cq))
            , PeerNodeId(peerNodeId)
        {}

        const TString* GetError() const noexcept {
            return std::get_if<TString>(&Res);
        }

        TRdmaPreinitedSessionPtr ExtractSession() noexcept {
            return std::move(std::get<TRdmaPreinitedSessionPtr>(Res));
        }

    private:
        // Destroy preinit session in any case from proxy context
        class TSessionDestroyerDelegate final : public NActors::TEvProxyCall {
        public:
            TSessionDestroyerDelegate(NActors::TInterconnectSessionRdma* session)
                : Session(session)
            {}
            void Call(TInterconnectProxyTCP* const) override {
                if (!Session) {
                    return;
                }
                IActor::InvokeOtherActor(*Session, &TInterconnectSessionRdma::AbortPreInit);
                Session = nullptr;
            }
            void ReportError(TString) override {
                if (!Session) {
                    return;
                }
                IActor::InvokeOtherActor(*Session, &TInterconnectSessionRdma::AbortPreInit);
                Session = nullptr;
            }
        private:
            NActors::TInterconnectSessionRdma* Session;
        };

        // calling from proxy context
        void Call(TInterconnectProxyTCP* const proxy) override {
            Y_DEBUG_ABORT_UNLESS(std::holds_alternative<std::monostate>(Res));
            auto session = new TInterconnectSessionRdma(proxy, Qp, Cq);
            TlsActivationContext->AsActorContext().RegisterWithSameMailbox(session);
            if (IActor::InvokeOtherActor(*session, &TInterconnectSessionRdma::ToSyncMode, SyncActor, Cq)) {
                auto* actorSystem = TActivationContext::ActorSystem();
                const ui32 peerNodeId = PeerNodeId;
                Res.emplace<TRdmaPreinitedSessionPtr>(session, [actorSystem, peerNodeId](TInterconnectSessionRdma* p) {
                    if (p) {
                        actorSystem->Send(actorSystem->InterconnectProxy(peerNodeId), new TSessionDestroyerDelegate(p));
                    }
                });
            } else {
                Res = "Unable to move session to sync mode";
                IActor::InvokeOtherActor(*session, &TInterconnectSessionRdma::AbortPreInit);
            }
        }
        void ReportError(TString error) override {
            Y_DEBUG_ABORT_UNLESS(std::holds_alternative<std::monostate>(Res));
            Res = std::move(error);
        }
        const TActorId SyncActor;
        NInterconnect::NRdma::TQueuePair::TPtr Qp;
        NInterconnect::NRdma::ICq::TPtr Cq;
        const ui32 PeerNodeId;

        std::variant<
            std::monostate,
            std::unique_ptr<NActors::TInterconnectSessionRdma, std::function<void(TInterconnectSessionRdma*)>>,
            TString> Res;
    };

    class TSwitchToTransitionModeDelegate final : public NActors::TEvProxyCall {
    public:
        TSwitchToTransitionModeDelegate(NActors::TInterconnectSessionRdma* session)
            : Session(session)
        {}
        void virtual Call(TInterconnectProxyTCP* const /*proxy*/) override {
            IActor::InvokeOtherActor(*Session, &TInterconnectSessionRdma::ToTransitionMode);
        }
        void virtual ReportError(TString error) override {
            Err = std::move(error);
        }
        const TString& GetError() const noexcept {
            return Err;
        }
    private:
        NActors::TInterconnectSessionRdma* Session;
        TString Err;
    };

    class TRdmaSyncActor
       : public NActors::TActorCoroImpl
       , public NActors::TInterconnectLoggingBase
    {
        NActors::TInterconnectProxyCommon::TPtr Common;
        const TActorId SelfVirtualId;
        const TActorId PeerVirtualId;
        const ui32 PeerNodeId;
        TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
        TQueuePair::TPtr Qp;
        ICq::TPtr Cq;
        const bool Incoming;
        TActorId Creator;
        TPollerToken::TPtr PollerToken;

        struct TPendingDataSync {
            NActorsInterconnect::TRdmaDataSync Sync;
            ui64 Checksum = 0;
        };
        std::optional<TPendingDataSync> PendingDataSync;

        struct TPendingAckSync {
            NActorsInterconnect::TRdmaAckSync Sync;
            ui64 Checksum = 0;
            TMonotonic ReceivedAt;
        };
        std::optional<TPendingAckSync> PendingAckSync;

        struct TPendingFinSync {
            NActorsInterconnect::TRdmaFinSync Sync;
        };
        std::optional<TPendingFinSync> PendingFinSync;

        struct TTcpControlReadState {
            TSyncMsgHeader Header;
            size_t HeaderBytes = 0;
            TString Data;
            size_t DataBytes = 0;
            bool ReadRequested = false;

            void Reset() {
                Header = {};
                HeaderBytes = 0;
                Data.clear();
                DataBytes = 0;
                ReadRequested = false;
            }
        };
        TTcpControlReadState TcpControlRead;

        THolder<TEvRdmaIoDone::THandle> PendingIoDone;

    public:
        TRdmaSyncActor(
                NActors::TInterconnectProxyCommon::TPtr common,
                TActorId selfVirtualId,
                TActorId peerVirtualId,
                ui32 peerNodeId,
                TIntrusivePtr<NInterconnect::TStreamSocket> socket,
                TQueuePair::TPtr qp,
                ICq::TPtr cq,
                bool incoming)
            : TActorCoroImpl(NActors::UsePooledStack<StackSize>(), true)
            , Common(std::move(common))
            , SelfVirtualId(selfVirtualId)
            , PeerVirtualId(peerVirtualId)
            , PeerNodeId(peerNodeId)
            , Socket(std::move(socket))
            , Qp(std::move(qp))
            , Cq(std::move(cq))
            , Incoming(incoming)
        {
            Creator = TlsActivationContext->AsActorContext().SelfID;
        }

        void Run() override {
            SetPrefix(Sprintf("RdmaSync %s [node %" PRIu32 " %s]",
                SelfActorId.ToString().data(), PeerNodeId, Incoming ? "incoming" : "outgoing"));
            LOG_LOG_IC_X(NActorsServices::INTERCONNECT, "ICRDMA", NActors::NLog::PRI_DEBUG,
                "starting rdma sync actor");

            Send(GetActorSystem()->InterconnectProxy(PeerNodeId),
                new TSessionCreatorDelegate(SelfActorId, Qp, Cq, PeerNodeId));
            auto ev = TActorCoroImpl::WaitForEvent();
            auto* result = static_cast<TSessionCreatorDelegate*>(ev->GetBase());
            if (const TString* error = result->GetError()) {
                LOG_LOG_IC_X(NActorsServices::INTERCONNECT, "ICRDMA", NActors::NLog::PRI_ERROR,
                    "unable to create rdma sync session: %s", error->data());
                Finish(*error);
                return;
            }
            auto session = result->ExtractSession();

            TString error;
            NActorsInterconnect::TRdmaStartSync peerStartSync;
            ui64 localStartSyncChecksum = 0;
            ui64 peerStartSyncChecksum = 0;
            if (!DoStart(peerStartSync, localStartSyncChecksum, peerStartSyncChecksum, error)) {
                LOG_LOG_IC_X(NActorsServices::INTERCONNECT, "ICRDMA", NActors::NLog::PRI_ERROR,
                    "RDMA StartSync failed: %s", error.data());
                Finish(std::move(error));
                return;
            }

            NActorsInterconnect::TRdmaDataSync localDataSync;
            ui64 localDataSyncChecksum = 0;
            const TMonotonic rdmaStart = TActivationContext::Monotonic();
            if (!DoData(peerStartSync, peerStartSyncChecksum, localDataSync, localDataSyncChecksum, error)) {
                LOG_LOG_IC_X(NActorsServices::INTERCONNECT, "ICRDMA", NActors::NLog::PRI_ERROR,
                    "RDMA DataSync failed: %s", error.data());
                Finish(std::move(error));
                return;
            }

            NActorsInterconnect::TRdmaDataSync peerDataSync;
            ui64 peerDataSyncChecksum = 0;
            if (!DoReceiveData(localStartSyncChecksum, peerDataSync, peerDataSyncChecksum, error)) {
                LOG_LOG_IC_X(NActorsServices::INTERCONNECT, "ICRDMA", NActors::NLog::PRI_ERROR,
                    "RDMA receive DataSync failed: %s", error.data());
                Finish(std::move(error));
                return;
            }

            NActorsInterconnect::TRdmaAckSync localAckSync;
            ui64 localAckSyncChecksum = 0;
            if (!DoAck(peerDataSyncChecksum, localAckSync, localAckSyncChecksum, error)) {
                LOG_LOG_IC_X(NActorsServices::INTERCONNECT, "ICRDMA", NActors::NLog::PRI_ERROR,
                    "RDMA AckSync failed: %s", error.data());
                Finish(std::move(error));
                return;
            }

            NActorsInterconnect::TRdmaAckSync peerAckSync;
            ui64 peerAckSyncChecksum = 0;
            TMonotonic peerAckReceivedAt;
            if (!DoReceiveAck(localDataSyncChecksum, peerAckSync, peerAckSyncChecksum, peerAckReceivedAt, error)) {
                LOG_LOG_IC_X(NActorsServices::INTERCONNECT, "ICRDMA", NActors::NLog::PRI_ERROR,
                    "RDMA receive AckSync failed: %s", error.data());
                Finish(std::move(error));
                return;
            }
            const ui64 rdmaRtt = (peerAckReceivedAt - rdmaStart).NanoSeconds();

            if (!DoSwitchToTransitionMode(session.get(), error)) {
                LOG_LOG_IC_X(NActorsServices::INTERCONNECT, "ICRDMA", NActors::NLog::PRI_ERROR,
                    "unable to switch RDMA session to data mode: %s", error.data());
                Finish(std::move(error));
                return;
            }

            if (!DoFin(peerAckSyncChecksum, rdmaRtt, error)) {
                LOG_LOG_IC_X(NActorsServices::INTERCONNECT, "ICRDMA", NActors::NLog::PRI_ERROR,
                    "RDMA FinSync failed: %s", error.data());
                Finish(std::move(error));
                return;
            }

            if (!DoReceiveFin(localAckSyncChecksum, error)) {
                LOG_LOG_IC_X(NActorsServices::INTERCONNECT, "ICRDMA", NActors::NLog::PRI_ERROR,
                    "RDMA receive FinSync failed: %s", error.data());
                Finish(std::move(error));
                return;
            }

            Send(Creator, new TEvRdmaSyncResult(std::move(session)));
        }

    private:
        void Finish(TString error) {
            Send(Creator, new TEvRdmaSyncResult(std::move(error)));
        }

        bool RegisterSocketInPoller(TString& error) {
            if (PollerToken) {
                return true;
            }

            if (!Send(MakePollerActorId(), new TEvPollerRegister(Socket, SelfActorId, SelfActorId))) {
                error = "unable to send TEvPollerRegister";
                return false;
            }

            auto ev = TActorCoroImpl::WaitForEvent();
            if (!ev || ev->GetTypeRewrite() != TEvPollerRegisterResult::EventType) {
                error = Sprintf("unexpected event while waiting for TEvPollerRegisterResult: 0x%08" PRIx32,
                    ev ? ev->GetTypeRewrite() : 0);
                return false;
            }

            PollerToken = std::move(ev->Get<TEvPollerRegisterResult>()->PollerToken);
            if (!PollerToken) {
                error = "empty poller token";
                return false;
            }

            return true;
        }

        bool WaitPoller(bool read, bool write, const char* state, TString& error) {
            if (!PollerToken->RequestNotificationAfterWouldBlock(read, write)) {
                auto ev = TActorCoroImpl::WaitForEvent();
                if (!ev || ev->GetTypeRewrite() != TEvPollerReady::EventType) {
                    error = Sprintf("unexpected event while waiting for TEvPollerReady in %s: 0x%08" PRIx32,
                        state, ev ? ev->GetTypeRewrite() : 0);
                    return false;
                }
            }

            return true;
        }

        bool SendData(const void* buffer, size_t len, const char* state, TString& error) {
            const char* ptr = static_cast<const char*>(buffer);
            size_t processed = 0;

            while (len) {
                TString err;
                const ssize_t nbytes = Socket->Send(ptr, len, &err);
                if (nbytes > 0) {
                    ptr += nbytes;
                    len -= nbytes;
                    processed += nbytes;
                } else if (-nbytes == EAGAIN || -nbytes == EWOULDBLOCK) {
                    if (!WaitPoller(false, true, state, error)) {
                        return false;
                    }
                } else if (!nbytes) {
                    error = Sprintf("connection unexpectedly closed while sending %s, processed# %zu", state, processed);
                    return false;
                } else if (-nbytes != EINTR) {
                    error = Sprintf("socket send error in %s: %s", state, (err ? err : TString(strerror(-nbytes))).data());
                    return false;
                }
            }

            return true;
        }

        bool ReceiveData(void* buffer, size_t len, const char* state, TString& error) {
            char* ptr = static_cast<char*>(buffer);
            size_t processed = 0;

            while (len) {
                TString err;
                const ssize_t nbytes = Socket->Recv(ptr, len, &err);
                if (nbytes > 0) {
                    ptr += nbytes;
                    len -= nbytes;
                    processed += nbytes;
                } else if (-nbytes == EAGAIN || -nbytes == EWOULDBLOCK) {
                    if (!WaitPoller(true, false, state, error)) {
                        return false;
                    }
                } else if (!nbytes) {
                    error = Sprintf("connection unexpectedly closed while receiving %s, processed# %zu", state, processed);
                    return false;
                } else if (-nbytes != EINTR) {
                    error = Sprintf("socket receive error in %s: %s", state, (err ? err : TString(strerror(-nbytes))).data());
                    return false;
                }
            }

            return true;
        }

        bool SendTcpProto(
                const google::protobuf::MessageLite& proto,
                ESyncTcpMessageType messageType,
                const char* what,
                TString& error,
                ui64* checksum = nullptr)
        {
            TString data;
            Y_PROTOBUF_SUPPRESS_NODISCARD proto.SerializeToString(&data);
            if (data.size() > TSyncMsgHeader::MaxSize) {
                error = Sprintf("%s protobuf is too large: %zu", what, data.size());
                return false;
            }

            TSyncMsgHeader header;
            header.Size = data.size();
            header.MessageType = static_cast<ui32>(messageType);
            header.Sign(data.data(), data.size());
            if (checksum) {
                *checksum = header.Checksum;
            }

            return SendData(&header, sizeof(header), Sprintf("Send%sHeader", what).data(), error)
                && SendData(data.data(), data.size(), Sprintf("Send%sData", what).data(), error);
        }

        std::optional<TString> Y_NO_INLINE ReceiveTcpMsg(
                ESyncTcpMessageType expectedMessageType,
                const char* what,
                TString& error,
                ui64* checksum)
        {
            TSyncMsgHeader header;
            if (!ReceiveData(&header, sizeof(header), Sprintf("Receive%sHeader", what).data(), error)) {
                return {};
            }

            if (header.Size > TSyncMsgHeader::MaxSize) {
                error = Sprintf("%s protobuf size is too large: %" PRIu32, what, header.Size);
                return {};
            }

            TString data;
            data.resize(header.Size);
            if (!ReceiveData(data.Detach(), data.size(), Sprintf("Receive%sData", what).data(), error)) {
                return {};
            }

            if (!header.Check(data.data(), data.size())) {
                error = Sprintf("%s protobuf checksum mismatch", what);
                return {};
            }
            if (checksum) {
                *checksum = header.Checksum;
            }

            if (header.MessageType == static_cast<ui32>(ESyncTcpMessageType::ErrSync)) {
                NActorsInterconnect::TRdmaErrSync errSync;
                if (!errSync.ParseFromString(data)) {
                    error = "unable to parse TRdmaErrSync protobuf";
                    return {};
                }
                error = Sprintf("peer RDMA sync error: Code# %" PRIu32 " Text# %s",
                    static_cast<ui32>(errSync.GetCode()), errSync.GetText().data());
                return {};
            }

            if (header.MessageType != static_cast<ui32>(expectedMessageType)) {
                error = Sprintf("unexpected %s message type: %" PRIu32, what, header.MessageType);
                return {};
            }

            return data;
        }

        template <typename TProto>
        bool ReceiveTcpProto(
                ESyncTcpMessageType expectedMessageType,
                const char* what,
                TString& error,
                ui64* checksum,
                TProto& proto)
        {
            if (auto maybeData = ReceiveTcpMsg(expectedMessageType, what, error, checksum)) {
                if (!proto.ParseFromString(*maybeData)) {
                    error = Sprintf("unable to parse %s protobuf", what);
                    return false;
                }
                return true;
            }
            return false;
        }

        bool SendErrSync(NActorsInterconnect::ERdmaSyncErrorCode code, const TString& text, TString& error) {
            NActorsInterconnect::TRdmaErrSync errSync;
            errSync.SetCode(code);
            errSync.SetText(text);
            return SendTcpProto(errSync, ESyncTcpMessageType::ErrSync, "TRdmaErrSync", error);
        }

        bool ReportRdmaSendError(TString& error) {
            TString sendErrSyncError;
            if (!SendErrSync(NActorsInterconnect::SYNC_ERR_RDMA_FAILED, error, sendErrSyncError)) {
                error = TStringBuilder()
                    << error
                    << "; unable to send TRdmaErrSync: "
                    << sendErrSyncError;
            }
            return false;
        }

        bool ReportRdmaReceiveError(NActorsInterconnect::ERdmaSyncErrorCode code, TString& error) {
            TString sendErrSyncError;
            if (!SendErrSync(code, error, sendErrSyncError)) {
                error = TStringBuilder()
                    << error
                    << "; unable to send TRdmaErrSync: "
                    << sendErrSyncError;
            }
            return false;
        }

        bool Y_NO_INLINE HandleEvent(THolder<IEventHandle> ev, TString& error) {
            if (!ev) {
                error = "unable to wait RDMA sync event";
                return false;
            }

            switch (ev->GetTypeRewrite()) {
                case TEvRdmaIoDone::EventType:
                    if (PendingIoDone) {
                        error = "duplicate RDMA SEND completion";
                        return false;
                    }
                    PendingIoDone.Reset(static_cast<TEvRdmaIoDone::THandle*>(ev.Release()));
                    return true;
                case TEvRdmaIoReceiveDone::EventType:
                    return HandleRdmaReceiveEvent(
                        THolder<TEvRdmaIoReceiveDone::THandle>(static_cast<TEvRdmaIoReceiveDone::THandle*>(ev.Release())),
                        error);
                case TEvPollerReady::EventType:
                    TcpControlRead.ReadRequested = false;
                    return TryReadTcpControlNoWait(error);
                default:
                    error = Sprintf("unexpected RDMA sync event: 0x%08" PRIx32, ev->GetTypeRewrite());
                    return false;
            }
        }

        template <typename TReady>
        bool PumpUntil(TReady ready, TString& error) {
            for (;;) {
                if (ready()) {
                    return true;
                }

                // Arm TCP read notification before sleeping, so peer ErrSync/FinSync
                // can wake us while we are waiting for an RDMA event.
                if (!TryReadTcpControlNoWait(error)) {
                    return false;
                }

                if (ready()) {
                    return true;
                }

                if (!HandleEvent(TActorCoroImpl::WaitForEvent(), error)) {
                    return false;
                }
            }
        }

        bool ProcessTcpControlMessage(TString& error) {
            const auto& header = TcpControlRead.Header;
            const auto& data = TcpControlRead.Data;

            if (!header.Check(data.data(), data.size())) {
                error = "TCP control protobuf checksum mismatch";
                return false;
            }

            if (header.MessageType == static_cast<ui32>(ESyncTcpMessageType::ErrSync)) {
                NActorsInterconnect::TRdmaErrSync errSync;
                if (!errSync.ParseFromString(data)) {
                    error = "unable to parse TRdmaErrSync protobuf";
                    return false;
                }
                error = Sprintf("peer RDMA sync error: Code# %" PRIu32 " Text# %s",
                    static_cast<ui32>(errSync.GetCode()), errSync.GetText().data());
                return false;
            }

            if (header.MessageType == static_cast<ui32>(ESyncTcpMessageType::FinSync)) {
                if (PendingFinSync) {
                    error = "duplicate RDMA FinSync";
                    return false;
                }

                NActorsInterconnect::TRdmaFinSync finSync;
                if (!finSync.ParseFromString(data)) {
                    error = "unable to parse TRdmaFinSync protobuf";
                    return false;
                }

                PendingFinSync.emplace(TPendingFinSync{
                    std::move(finSync),
                });
                return true;
            }

            error = Sprintf("unexpected TCP control message type in RDMA phase: %" PRIu32, header.MessageType);
            return false;
        }

        bool Y_NO_INLINE TryReadTcpControlNoWait(TString& error) {
            if (!Socket) {
                error = "TCP control socket is not initialized";
                return false;
            }
            if (!PollerToken) {
                error = "TCP control poller token is not initialized";
                return false;
            }

            for (;;) {
                char* ptr = nullptr;
                size_t len = 0;

                if (TcpControlRead.HeaderBytes != sizeof(TSyncMsgHeader)) {
                    ptr = reinterpret_cast<char*>(&TcpControlRead.Header) + TcpControlRead.HeaderBytes;
                    len = sizeof(TSyncMsgHeader) - TcpControlRead.HeaderBytes;
                } else {
                    ptr = TcpControlRead.Data.Detach() + TcpControlRead.DataBytes;
                    len = TcpControlRead.Data.size() - TcpControlRead.DataBytes;
                }

                TString err;
                const ssize_t nbytes = Socket->Recv(ptr, len, &err);
                if (nbytes > 0) {
                    TcpControlRead.ReadRequested = false;
                    if (TcpControlRead.HeaderBytes != sizeof(TSyncMsgHeader)) {
                        TcpControlRead.HeaderBytes += nbytes;
                        if (TcpControlRead.HeaderBytes != sizeof(TSyncMsgHeader)) {
                            continue;
                        }
                        if (TcpControlRead.Header.Size > TSyncMsgHeader::MaxSize) {
                            error = Sprintf("TCP control protobuf size is too large: %" PRIu32, TcpControlRead.Header.Size);
                            return false;
                        }
                        TcpControlRead.Data.resize(TcpControlRead.Header.Size);
                        TcpControlRead.DataBytes = 0;
                        if (!TcpControlRead.Header.Size) {
                            if (!ProcessTcpControlMessage(error)) {
                                return false;
                            }
                            TcpControlRead.Reset();
                        }
                    } else {
                        TcpControlRead.DataBytes += nbytes;
                        if (TcpControlRead.DataBytes != TcpControlRead.Data.size()) {
                            continue;
                        }
                        if (!ProcessTcpControlMessage(error)) {
                            return false;
                        }
                        TcpControlRead.Reset();
                    }
                } else if (-nbytes == EAGAIN || -nbytes == EWOULDBLOCK) {
                    if (!TcpControlRead.ReadRequested) {
                        if (PollerToken->RequestReadNotificationAfterWouldBlock()) {
                            // Read readiness raced with arming the notification; try recv again.
                            continue;
                        }
                        TcpControlRead.ReadRequested = true;
                    }
                    return true;
                } else if (!nbytes) {
                    error = "connection unexpectedly closed while reading TCP control message";
                    return false;
                } else if (-nbytes != EINTR) {
                    error = Sprintf("socket receive error while reading TCP control message: %s",
                        (err ? err : TString(strerror(-nbytes))).data());
                    return false;
                }
            }
        }

        THolder<TEvRdmaIoDone::THandle> WaitRdmaIoDone(TString& error) {
            if (!PumpUntil([this] { return static_cast<bool>(PendingIoDone); }, error)) {
                return nullptr;
            }

            return std::move(PendingIoDone);
        }

        bool ParseRdmaPacket(const TRcBuf& received, TRdmaSyncPacket& packet, TString& error) {
            if (received.GetSize() < sizeof(TSyncMsgHeader)) {
                error = Sprintf("RDMA sync packet is too small: %zu", received.GetSize());
                return false;
            }

            TSyncMsgHeader header;
            ::memcpy(&header, received.GetData(), sizeof(header));

            if (header.Size > TSyncMsgHeader::MaxSize) {
                error = Sprintf("RDMA sync packet protobuf size is too large: %" PRIu32, header.Size);
                return false;
            }

            if (received.GetSize() != sizeof(TSyncMsgHeader) + header.Size) {
                error = Sprintf("unexpected RDMA sync packet size: expected# %zu received# %zu",
                    sizeof(TSyncMsgHeader) + static_cast<size_t>(header.Size), received.GetSize());
                return false;
            }

            const char* data = received.GetData() + sizeof(TSyncMsgHeader);
            if (!header.Check(data, header.Size)) {
                error = "RDMA sync packet checksum mismatch";
                return false;
            }

            switch (static_cast<ESyncRdmaMessageType>(header.MessageType)) {
                case ESyncRdmaMessageType::DataSync:
                case ESyncRdmaMessageType::AckSync:
                    packet.MessageType = static_cast<ESyncRdmaMessageType>(header.MessageType);
                    packet.Checksum = header.Checksum;
                    packet.Data.assign(data, header.Size);
                    return true;
            }

            error = Sprintf("unexpected RDMA sync packet type: %" PRIu32, header.MessageType);
            return false;
        }

        bool HandleRdmaReceiveEvent(THolder<TEvRdmaIoReceiveDone::THandle> ev, TString& error) {
            if (!ev->Get()->IsSuccess()) {
                error = TStringBuilder()
                    << "RDMA sync RECEIVE failed, err source: " << ev->Get()->GetErrSource()
                    << ", code: " << ev->Get()->GetErrCode();
                return false;
            }

            const auto& received = std::get<TEvRdmaIoReceiveDone::TSuccess>(ev->Get()->Record).Buf;

            TRdmaSyncPacket packet;
            if (!ParseRdmaPacket(received, packet, error)) {
                return false;
            }

            if (packet.MessageType == ESyncRdmaMessageType::DataSync) {
                NActorsInterconnect::TRdmaDataSync dataSync;
                if (!dataSync.ParseFromString(packet.Data)) {
                    error = "unable to parse TRdmaDataSync protobuf";
                    return false;
                }
                PendingDataSync.emplace(TPendingDataSync{
                    std::move(dataSync),
                    packet.Checksum,
                });
                return true;
            }

            NActorsInterconnect::TRdmaAckSync ackSync;
            if (!ackSync.ParseFromString(packet.Data)) {
                error = "unable to parse TRdmaAckSync protobuf";
                return false;
            }
            PendingAckSync.emplace(TPendingAckSync{
                std::move(ackSync),
                packet.Checksum,
                TActivationContext::Monotonic(),
            });
            return true;
        }

        ui64 CalculateSessionHash() const {
            TString first = SelfVirtualId.ToString();
            TString second = PeerVirtualId.ToString();
            if (second < first) {
                first.swap(second);
            }

            XXH3_state_t state;
            XXH3_64bits_reset(&state);
            XXH3_64bits_update(&state, first.data(), first.size());
            constexpr char Separator = '\0';
            XXH3_64bits_update(&state, &Separator, sizeof(Separator));
            XXH3_64bits_update(&state, second.data(), second.size());
            return XXH3_64bits_digest(&state);
        }

        NActorsInterconnect::TRdmaStartSync MakeStartSync() const {
            NActorsInterconnect::TRdmaStartSync startSync;
            startSync.SetVersion(SyncProtocolVersion);
            startSync.SetFeatureMask(SyncFeatureMask);
            startSync.SetSessionHash(CalculateSessionHash());
            return startSync;
        }

        NActorsInterconnect::TRdmaDataSync MakeDataSync(
                const NActorsInterconnect::TRdmaStartSync& peerStartSync,
                ui64 peerStartSyncChecksum) const
        {
            NActorsInterconnect::TRdmaDataSync dataSync;
            dataSync.SetHashChain(peerStartSyncChecksum);
            dataSync.SetFeatureMask(peerStartSync.GetFeatureMask() & SyncFeatureMask);
            dataSync.SetPayload(TString(DataSyncPayloadSize, '\0'));
            return dataSync;
        }

        NActorsInterconnect::TRdmaAckSync MakeAckSync(ui64 peerDataSyncChecksum) const {
            NActorsInterconnect::TRdmaAckSync ackSync;
            ackSync.SetHashChain(peerDataSyncChecksum);
            return ackSync;
        }

        NActorsInterconnect::TRdmaFinSync MakeFinSync(ui64 peerAckSyncChecksum, ui64 rdmaRtt) const {
            NActorsInterconnect::TRdmaFinSync finSync;
            finSync.SetHashChain(peerAckSyncChecksum);
            finSync.SetRdmaRtt(rdmaRtt);
            return finSync;
        }

        bool Y_NO_INLINE PostRdmaProto(
                const google::protobuf::MessageLite& proto,
                ESyncRdmaMessageType messageType,
                const char* what,
                TString& error,
                ui64* checksum = nullptr)
        {
            TString data;
            Y_PROTOBUF_SUPPRESS_NODISCARD proto.SerializeToString(&data);

            if (data.size() > TSyncMsgHeader::MaxSize) {
                error = Sprintf("RDMA %s protobuf is too large: %zu", what, data.size());
                return ReportRdmaSendError(error);
            }

            if (!Common->RdmaMemPool) {
                error = "RDMA memory pool is not initialized";
                return ReportRdmaSendError(error);
            }

            TSyncMsgHeader header;
            header.Size = data.size();
            header.MessageType = static_cast<ui32>(messageType);
            header.Sign(data.data(), data.size());
            if (checksum) {
                *checksum = header.Checksum;
            }

            auto sendBuf = Common->RdmaMemPool->AllocRcBuf(sizeof(header) + data.size(), IMemPool::EMPTY);
            if (!sendBuf) {
                error = Sprintf("unable to allocate RDMA %s send buffer", what);
                return ReportRdmaSendError(error);
            }

            char* ptr = sendBuf->UnsafeGetDataMut();
            ::memcpy(ptr, &header, sizeof(header));
            ::memcpy(ptr + sizeof(header), data.data(), data.size());

            auto cb = [actorId = SelfActorId, sendBuf = *sendBuf](NActors::TActorSystem* as, TEvRdmaIoDone* ev) mutable {
                as->Send(actorId, ev);
                Y_UNUSED(sendBuf);
            };

            auto builder = CreateIbVerbsBuilder(1);
            builder->AddSendVerb(*sendBuf, std::move(cb));

            if (Cq->DoWrBatchAsync(Qp, std::move(builder))) {
                error = Sprintf("unable to post RDMA %s SEND work request", what);
                return ReportRdmaSendError(error);
            }

            auto ev = WaitRdmaIoDone(error);
            if (!ev) {
                if (!error) {
                    error = Sprintf("unable to wait RDMA %s SEND completion", what);
                }
                return ReportRdmaSendError(error);
            }

            if (!ev->Get()->IsSuccess()) {
                error = TStringBuilder()
                    << "RDMA " << what << " SEND failed, err source: " << ev->Get()->GetErrSource()
                    << ", code: " << ev->Get()->GetErrCode();
                return ReportRdmaSendError(error);
            }

            return true;
        }

        bool DoStart(
                NActorsInterconnect::TRdmaStartSync& peerStartSync,
                ui64& localStartSyncChecksum,
                ui64& peerStartSyncChecksum,
                TString& error)
        {
            if (!RegisterSocketInPoller(error)) {
                return false;
            }

            const auto local = MakeStartSync();
            if (!SendTcpProto(local, ESyncTcpMessageType::StartSync, "TRdmaStartSync", error, &localStartSyncChecksum)) {
                return false;
            }

            if (!ReceiveTcpProto(ESyncTcpMessageType::StartSync, "TRdmaStartSync", error, &peerStartSyncChecksum, peerStartSync)) {
                return false;
            }

            if (peerStartSync.GetVersion() != SyncProtocolVersion) {
                error = Sprintf("unsupported RDMA sync protocol version: %" PRIu32, peerStartSync.GetVersion());
                return false;
            }

            if (peerStartSync.GetSessionHash() != local.GetSessionHash()) {
                error = Sprintf("unexpected RDMA sync session hash: local# %" PRIu64 " peer# %" PRIu64,
                    local.GetSessionHash(), peerStartSync.GetSessionHash());
                return false;
            }

            LOG_LOG_IC_X(NActorsServices::INTERCONNECT, "ICRDMA", NActors::NLog::PRI_DEBUG,
                "RDMA StartSync exchanged, FeatureMask# %" PRIu64, peerStartSync.GetFeatureMask());

            return true;
        }

        bool DoData(
                const NActorsInterconnect::TRdmaStartSync& peerStartSync,
                ui64 peerStartSyncChecksum,
                NActorsInterconnect::TRdmaDataSync& localDataSync,
                ui64& localDataSyncChecksum,
                TString& error)
        {
            localDataSync = MakeDataSync(peerStartSync, peerStartSyncChecksum);
            return PostRdmaProto(localDataSync, ESyncRdmaMessageType::DataSync, "DataSync", error, &localDataSyncChecksum);
        }

        bool DoReceiveData(
                ui64 localStartSyncChecksum,
                NActorsInterconnect::TRdmaDataSync& dataSync,
                ui64& dataSyncChecksum,
                TString& error)
        {
            if (!PumpUntil([this] { return PendingDataSync.has_value(); }, error)) {
                return ReportRdmaReceiveError(NActorsInterconnect::SYNC_ERR_RDMA_FAILED, error);
            }

            dataSync = std::move(PendingDataSync->Sync);
            dataSyncChecksum = PendingDataSync->Checksum;
            PendingDataSync.reset();

            if (dataSync.GetHashChain() != localStartSyncChecksum) {
                error = Sprintf("unexpected RDMA DataSync hash chain: expected# %" PRIu64 " received# %" PRIu64,
                    localStartSyncChecksum, dataSync.GetHashChain());
                return ReportRdmaReceiveError(NActorsInterconnect::SYNC_ERR_HASH_CHAIN_MISMATCH, error);
            }

            if (dataSync.GetFeatureMask() != SyncFeatureMask) {
                error = Sprintf("unexpected RDMA DataSync feature mask: expected# %" PRIu64 " received# %" PRIu64,
                    SyncFeatureMask, dataSync.GetFeatureMask());
                return ReportRdmaReceiveError(NActorsInterconnect::SYNC_ERR_UNEXPECTED_SESSION, error);
            }

            LOG_LOG_IC_X(NActorsServices::INTERCONNECT, "ICRDMA", NActors::NLog::PRI_DEBUG,
                "RDMA DataSync received, PayloadSize# %zu", dataSync.GetPayload().size());

            return true;
        }

        bool DoAck(
                ui64 peerDataSyncChecksum,
                NActorsInterconnect::TRdmaAckSync& localAckSync,
                ui64& localAckSyncChecksum,
                TString& error)
        {
            localAckSync = MakeAckSync(peerDataSyncChecksum);
            return PostRdmaProto(localAckSync, ESyncRdmaMessageType::AckSync, "AckSync", error, &localAckSyncChecksum);
        }

        bool DoReceiveAck(
                ui64 localDataSyncChecksum,
                NActorsInterconnect::TRdmaAckSync& ackSync,
                ui64& ackSyncChecksum,
                TMonotonic& receivedAt,
                TString& error)
        {
            if (!PumpUntil([this] { return PendingAckSync.has_value(); }, error)) {
                return ReportRdmaReceiveError(NActorsInterconnect::SYNC_ERR_RDMA_FAILED, error);
            }

            ackSync = std::move(PendingAckSync->Sync);
            ackSyncChecksum = PendingAckSync->Checksum;
            receivedAt = PendingAckSync->ReceivedAt;
            PendingAckSync.reset();

            if (ackSync.GetHashChain() != localDataSyncChecksum) {
                error = Sprintf("unexpected RDMA AckSync hash chain: expected# %" PRIu64 " received# %" PRIu64,
                    localDataSyncChecksum, ackSync.GetHashChain());
                return ReportRdmaReceiveError(NActorsInterconnect::SYNC_ERR_HASH_CHAIN_MISMATCH, error);
            }

            LOG_LOG_IC_X(NActorsServices::INTERCONNECT, "ICRDMA", NActors::NLog::PRI_DEBUG,
                "RDMA AckSync received");

            return true;
        }

        bool DoSwitchToTransitionMode(NActors::TInterconnectSessionRdma* session, TString& error) {
            Send(GetActorSystem()->InterconnectProxy(PeerNodeId), new TSwitchToTransitionModeDelegate(session));

            for (;;) {
                auto ev = TActorCoroImpl::WaitForEvent();
                if (!ev) {
                    error = "unable to wait switch to transition mode result";
                    return false;
                }

                if (ev->GetTypeRewrite() == TEvProxyCall::EventType) {
                    auto* result = static_cast<TSwitchToTransitionModeDelegate*>(ev->GetBase());
                    if (const TString& switchError = result->GetError()) {
                        error = switchError;
                        return false;
                    }
                    return true;
                }

                if (!HandleEvent(std::move(ev), error)) {
                    return false;
                }
            }
        }

        bool DoFin(ui64 peerAckSyncChecksum, ui64 rdmaRtt, TString& error) {
            return SendTcpProto(MakeFinSync(peerAckSyncChecksum, rdmaRtt), ESyncTcpMessageType::FinSync, "TRdmaFinSync", error);
        }

        bool DoReceiveFin(ui64 localAckSyncChecksum, TString& error) {
            NActorsInterconnect::TRdmaFinSync finSync;
            if (PendingFinSync) {
                finSync = std::move(PendingFinSync->Sync);
                PendingFinSync.reset();
            } else {
                if (!ReceiveTcpProto(ESyncTcpMessageType::FinSync, "TRdmaFinSync", error, nullptr, finSync)) {
                    return false;
                }
            }

            if (finSync.GetHashChain() != localAckSyncChecksum) {
                error = Sprintf("unexpected RDMA FinSync hash chain: expected# %" PRIu64 " received# %" PRIu64,
                    localAckSyncChecksum, finSync.GetHashChain());
                return false;
            }

            LOG_LOG_IC_X(NActorsServices::INTERCONNECT, "ICRDMA", NActors::NLog::PRI_DEBUG,
                "RDMA FinSync received, RdmaRtt# %" PRIu64, finSync.GetRdmaRtt());

            return true;
        }
    };
}

NActors::IActor* CreateRdmaOutgoingSyncActor(
    NActors::TInterconnectProxyCommon::TPtr common,
    const TActorId& selfVirtualId,
    const TActorId& peerVirtualId,
    ui32 peerNodeId,
    TIntrusivePtr<NInterconnect::TStreamSocket> socket,
    TQueuePair::TPtr qp,
    ICq::TPtr cq)
{
    return new NActors::TActorCoro(MakeHolder<TRdmaSyncActor>(
        std::move(common), selfVirtualId, peerVirtualId, peerNodeId, std::move(socket), std::move(qp), std::move(cq), false));
}

NActors::IActor* CreateRdmaIncommingSyncActor(
    NActors::TInterconnectProxyCommon::TPtr common,
    const TActorId& selfVirtualId,
    const TActorId& peerVirtualId,
    ui32 peerNodeId,
    TIntrusivePtr<NInterconnect::TStreamSocket> socket,
    TQueuePair::TPtr qp,
    ICq::TPtr cq)
{
    return new NActors::TActorCoro(MakeHolder<TRdmaSyncActor>(
        std::move(common), selfVirtualId, peerVirtualId, peerNodeId, std::move(socket), std::move(qp), std::move(cq), true));
}

}
