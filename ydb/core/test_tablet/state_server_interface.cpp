#include "load_actor_impl.h"

namespace NKikimr::NTestShard {

    class TStateServerInterfaceActor : public TActor<TStateServerInterfaceActor> {
        struct TServerContext : TThrRefBase {
            const TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
            const TString Host;
            i32 Port;
            std::unordered_set<TActorId, THash<TActorId>> Subscribers;

            TServerContext(const TString& host, i32 port, const TActorIdentity& self)
                : Socket(NInterconnect::TStreamSocket::Make(AF_INET6))
                , Host(host)
                , Port(port)
            {
                Y_ABORT_UNLESS(*Socket != INVALID_SOCKET);
                SetNonBlock(*Socket);
                SetNoDelay(*Socket, true);
                Socket->Connect(NInterconnect::TAddress(host, port));
                self.Send(MakePollerActorId(), new TEvPollerRegister(Socket, self, self));
            }

            bool IsConnected = false;
            TPollerToken::TPtr PollerToken;
            bool NeedRead = false, NeedWrite = false;

            bool Action(TStateServerInterfaceActor *self) {
                if (IsConnected ? Read(self) && Write() : CheckConnect(self)) {
                    if (NeedRead || NeedWrite) {
                        PollerToken->Request(NeedRead, NeedWrite);
                    }
                    return true;
                } else {
                    return false;
                }
            }

            bool CheckConnect(TStateServerInterfaceActor *self) {
                NeedRead = NeedWrite = false;
                int err = Socket->GetConnectStatus();
                if (err == EAGAIN || err == EINPROGRESS) {
                    NeedWrite = true;
                } else if (!err) {
                    STLOG(PRI_INFO, TEST_SHARD, TS06, "successfully connected to state server");
                    IsConnected = true;
                    return Read(self) && Write();
                } else {
                    STLOG(PRI_ERROR, TEST_SHARD, TS01, "failed to establish connection to state server",
                        (Error, strerror(err)));
                    return false;
                }
                return true;
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // read queue

            enum class EReadState {
                INITIAL,
                LENGTH,
                DATA,
            } ReadState = EReadState::INITIAL;
            ui32 ReadLength;
            TString ReadBuffer;
            char *ReadBufferPtr = nullptr;
            char *ReadBufferEnd = nullptr;

            bool Read(TStateServerInterfaceActor *self) {
                NeedRead = false;
                for (;;) {
                    if (size_t num = ReadBufferEnd - ReadBufferPtr) {
                        ssize_t read = Socket->Recv(ReadBufferPtr, num);
                        if (read > 0) {
                            ReadBufferPtr += read;
                        } else if (-read == EAGAIN || -read == EWOULDBLOCK) {
                            NeedRead = true;
                            break;
                        } else if (-read == EINTR) {
                            continue;
                        } else {
                            STLOG(PRI_ERROR, TEST_SHARD, TS02, "failed to receive data from state server",
                                (Error, strerror(-read)));
                            return false;
                        }
                    } else {
                        switch (ReadState) {
                            case EReadState::LENGTH:
                                ReadState = EReadState::DATA;
                                Y_ABORT_UNLESS(ReadLength <= 64 * 1024 * 1024);
                                ReadBuffer = TString::Uninitialized(ReadLength);
                                ReadBufferPtr = ReadBuffer.Detach();
                                ReadBufferEnd = ReadBufferPtr + ReadLength;
                                break;

                            case EReadState::DATA:
                                ProcessPacket(self);
                                [[fallthrough]];
                            case EReadState::INITIAL:
                                ReadState = EReadState::LENGTH;
                                ReadBufferPtr = reinterpret_cast<char*>(&ReadLength);
                                ReadBufferEnd = ReadBufferPtr + sizeof(ReadLength);
                                break;
                        }
                    }
                }
                return true;
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // write queue

            std::deque<TString> WriteQ;
            size_t WriteOffset = 0;

            bool Write() {
                NeedWrite = false;
                while (!WriteQ.empty()) {
                    const TString& s = WriteQ.front();
                    if (WriteOffset == s.size()) {
                        WriteQ.pop_front();
                        WriteOffset = 0;
                    } else {
                        ssize_t written = Socket->Send(s.data() + WriteOffset, s.size() - WriteOffset);
                        if (written > 0) {
                            WriteOffset += written;
                        } else if (-written == EAGAIN || -written == EWOULDBLOCK) {
                            NeedWrite = true;
                            break;
                        } else if (-written == EINTR) {
                            continue;
                        } else {
                            STLOG(PRI_ERROR, TEST_SHARD, TS03, "failed to send data to state server",
                                (Error, strerror(-written)));
                            return false;
                        }
                    }
                }
                return true;
            }

            struct TResponseInfo {
                const TActorId Sender;
                const ui64 Cookie;
                const ui32 Type;
            };
            std::deque<TResponseInfo> ResponseQ;

            void Push(TEvStateServerRequest::TPtr ev) {
                auto& record = ev->Get()->Record;
                const ui32 type = record.HasWrite() ? TEvTestShard::EvStateServerWriteResult :
                    record.HasRead() ? TEvTestShard::EvStateServerReadResult : 0;
                Y_ABORT_UNLESS(type);
                ResponseQ.push_back(TResponseInfo{ev->Sender, ev->Cookie, type});
                auto buffers = ev->ReleaseChainBuffer();
                Y_ABORT_UNLESS(!buffers->GetSerializationInfo().IsExtendedFormat);
                const ui32 len = buffers->GetSize();
                Y_ABORT_UNLESS(len <= 64 * 1024 * 1024);
                TString w = TString::Uninitialized(sizeof(ui32) + len);
                char *p = w.Detach();
                auto append = [&](const void *buffer, size_t len) { memcpy(std::exchange(p, p + len), buffer, len); };
                append(&len, sizeof(len));
                for (auto iter = buffers->GetBeginIter(); iter.Valid(); iter.AdvanceToNextContiguousBlock()) {
                    append(iter.ContiguousData(), iter.ContiguousSize());
                }
                WriteQ.push_back(std::move(w));
            }

            void ProcessPacket(TStateServerInterfaceActor *self) {
                Y_ABORT_UNLESS(!ResponseQ.empty());
                TResponseInfo& response = ResponseQ.front();
                TActivationContext::Send(new IEventHandle(response.Type, 0, response.Sender, self->SelfId(),
                    MakeIntrusive<TEventSerializedData>(std::move(ReadBuffer), TEventSerializationInfo{}),
                    response.Cookie));
                ResponseQ.pop_front();
            }

            using TPtr = TIntrusivePtr<TServerContext>;
        };

        std::unordered_map<TActorId, TServerContext::TPtr, THash<TActorId>> Servers;
        std::unordered_map<std::pair<TString, i32>, TServerContext::TPtr> HostMap;
        std::unordered_map<int, TServerContext::TPtr> SocketMap;

        TTestShardContext::TPtr Context;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::STATE_SERVER_INTERFACE_ACTOR;
        }

        TStateServerInterfaceActor(TTestShardContext::TPtr context)
            : TActor(&TThis::StateFunc)
            , Context(std::move(context))
        {}

        STRICT_STFUNC(StateFunc,
            hFunc(TEvStateServerConnect, Handle);
            hFunc(TEvStateServerDisconnect, Handle);
            hFunc(TEvPollerRegisterResult, Handle);
            hFunc(TEvPollerReady, Handle);
            hFunc(TEvStateServerRequest, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        )

        void Handle(TEvStateServerConnect::TPtr ev) {
            if (Context) {
                Send(ev->Sender, new TEvStateServerStatus(true));
                return;
            }

            auto *msg = ev->Get();
            const auto& key = std::make_pair(msg->Host, msg->Port);
            auto& ctx = HostMap[key];
            if (!ctx) {
                ctx = MakeIntrusive<TServerContext>(msg->Host, msg->Port, SelfId());
                SocketMap.emplace(*ctx->Socket, ctx);
            }
            bool inserted = Servers.emplace(ev->Sender, ctx).second;
            Y_ABORT_UNLESS(inserted);
            if (ctx->IsConnected) {
                Send(ev->Sender, new TEvStateServerStatus(true));
            }
            inserted = ctx->Subscribers.insert(ev->Sender).second;
            Y_ABORT_UNLESS(inserted);
        }

        void Handle(TEvStateServerDisconnect::TPtr ev) {
            if (Context) {
                return;
            }

            const auto it = Servers.find(ev->Sender);
            Y_ABORT_UNLESS(it != Servers.end());
            const size_t num = it->second->Subscribers.erase(ev->Sender);
            Y_ABORT_UNLESS(num);
            if (it->second->Subscribers.empty()) {
                HostMap.erase(std::make_pair(it->second->Host, it->second->Port));
                SocketMap.erase(*it->second->Socket);
            }
            Servers.erase(it);
        }

        void Handle(TEvPollerRegisterResult::TPtr ev) {
            if (const auto it = SocketMap.find(ev->Get()->Socket->GetDescriptor()); it != SocketMap.end()) {
                it->second->PollerToken = std::move(ev->Get()->PollerToken);
                Action(it->second);
            }
        }

        void Handle(TEvPollerReady::TPtr ev) {
            if (const auto it = SocketMap.find(ev->Get()->Socket->GetDescriptor()); it != SocketMap.end()) {
                Action(it->second);
            }
        }

        void Action(const TServerContext::TPtr& ctx) {
            std::optional<bool> notify;
            const bool wasConnected = ctx->IsConnected;
            if (!ctx->Action(this)) {
                notify = false;
            } else if (!wasConnected && ctx->IsConnected) {
                notify = true;
            }
            if (notify) {
                for (TActorId subscriberId : ctx->Subscribers) {
                    Send(subscriberId, new TEvStateServerStatus(*notify));
                }
            }
        }

        void Handle(TEvStateServerRequest::TPtr ev) {
            if (Context) {
                Context->Data->Action(SelfId(), ev);
                return;
            }

            const auto it = Servers.find(ev->Sender);
            Y_ABORT_UNLESS(it != Servers.end());
            it->second->Push(ev);
            Action(it->second);
        }
    };

    IActor *CreateStateServerInterfaceActor(TTestShardContext::TPtr context) {
        return new TStateServerInterfaceActor(std::move(context));
    }

} // NKikimr::NTestShard
