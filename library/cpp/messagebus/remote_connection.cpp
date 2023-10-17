#include "remote_connection.h"

#include "key_value_printer.h"
#include "mb_lwtrace.h"
#include "network.h"
#include "remote_client_connection.h"
#include "remote_client_session.h"
#include "remote_server_session.h"
#include "session_impl.h"

#include <library/cpp/messagebus/actor/what_thread_does.h>

#include <util/generic/cast.h>
#include <util/network/init.h>
#include <library/cpp/deprecated/atomic/atomic.h>

LWTRACE_USING(LWTRACE_MESSAGEBUS_PROVIDER)

using namespace NActor;
using namespace NBus;
using namespace NBus::NPrivate;

namespace NBus {
    namespace NPrivate {
        TRemoteConnection::TRemoteConnection(TRemoteSessionPtr session, ui64 connectionId, TNetAddr addr)
            : TActor<TRemoteConnection, TWriterTag>(session->Queue->WorkQueue.Get())
            , TActor<TRemoteConnection, TReaderTag>(session->Queue->WorkQueue.Get())
            , TScheduleActor<TRemoteConnection, TWriterTag>(&session->Queue->Scheduler)
            , Session(session)
            , Proto(session->Proto)
            , Config(session->Config)
            , RemovedFromSession(false)
            , ConnectionId(connectionId)
            , PeerAddr(addr)
            , PeerAddrSocketAddr(addr)
            , CreatedTime(TInstant::Now())
            , ReturnConnectFailedImmediately(false)
            , GranStatus(Config.Secret.StatusFlushPeriod)
            , QuotaMsg(!Session->IsSource_, Config.PerConnectionMaxInFlight, 0)
            , QuotaBytes(!Session->IsSource_, Config.PerConnectionMaxInFlightBySize, 0)
            , MaxBufferSize(session->Config.MaxBufferSize)
            , ShutdownReason(MESSAGE_OK)
        {
            WriterData.Status.ConnectionId = connectionId;
            WriterData.Status.PeerAddr = PeerAddr;
            ReaderData.Status.ConnectionId = connectionId;

            const TInstant now = TInstant::Now();

            WriterFillStatus();

            GranStatus.Writer.Update(WriterData.Status, now, true);
            GranStatus.Reader.Update(ReaderData.Status, now, true);
        }

        TRemoteConnection::~TRemoteConnection() {
            Y_ABORT_UNLESS(ReplyQueue.IsEmpty());
        }

        TRemoteConnection::TWriterData::TWriterData()
            : Down(0)
            , SocketVersion(0)
            , InFlight(0)
            , AwakeFlags(0)
            , State(WRITER_FILLING)
        {
        }

        TRemoteConnection::TWriterData::~TWriterData() {
            Y_ABORT_UNLESS(AtomicGet(Down));
            Y_ABORT_UNLESS(SendQueue.Empty());
        }

        bool TRemoteConnection::TReaderData::HasBytesInBuf(size_t bytes) noexcept {
            size_t left = Buffer.Size() - Offset;

            return (MoreBytes = left >= bytes ? 0 : bytes - left) == 0;
        }

        void TRemoteConnection::TWriterData::SetChannel(NEventLoop::TChannelPtr channel) {
            Y_ABORT_UNLESS(!Channel, "must not have channel");
            Y_ABORT_UNLESS(Buffer.GetBuffer().Empty() && Buffer.LeftSize() == 0, "buffer must be empty");
            Y_ABORT_UNLESS(State == WRITER_FILLING, "state must be initial");
            Channel = channel;
        }

        void TRemoteConnection::TReaderData::SetChannel(NEventLoop::TChannelPtr channel) {
            Y_ABORT_UNLESS(!Channel, "must not have channel");
            Y_ABORT_UNLESS(Buffer.Empty(), "buffer must be empty");
            Channel = channel;
        }

        void TRemoteConnection::TWriterData::DropChannel() {
            if (!!Channel) {
                Channel->Unregister();
                Channel.Drop();
            }

            Buffer.Reset();
            State = WRITER_FILLING;
        }

        void TRemoteConnection::TReaderData::DropChannel() {
            // TODO: make Drop call Unregister
            if (!!Channel) {
                Channel->Unregister();
                Channel.Drop();
            }
            Buffer.Reset();
            Offset = 0;
        }

        TRemoteConnection::TReaderData::TReaderData()
            : Down(0)
            , SocketVersion(0)
            , Offset(0)
            , MoreBytes(0)
        {
        }

        TRemoteConnection::TReaderData::~TReaderData() {
            Y_ABORT_UNLESS(AtomicGet(Down));
        }

        void TRemoteConnection::Send(TNonDestroyingAutoPtr<TBusMessage> msg) {
            BeforeSendQueue.Enqueue(msg.Release());
            AtomicIncrement(WriterData.InFlight);
            ScheduleWrite();
        }

        void TRemoteConnection::ClearOutgoingQueue(TMessagesPtrs& result, bool reconnect) {
            if (!reconnect) {
                // Do not clear send queue if reconnecting
                WriterData.SendQueue.Clear(&result);
            }
        }

        void TRemoteConnection::Shutdown(EMessageStatus status) {
            ScheduleShutdown(status);

            ReaderData.ShutdownComplete.WaitI();
            WriterData.ShutdownComplete.WaitI();
        }

        void TRemoteConnection::TryConnect() {
            Y_ABORT("TryConnect is client connection only operation");
        }

        void TRemoteConnection::ScheduleRead() {
            GetReaderActor()->Schedule();
        }

        void TRemoteConnection::ScheduleWrite() {
            GetWriterActor()->Schedule();
        }

        void TRemoteConnection::WriterRotateCounters() {
            if (!WriterData.TimeToRotateCounters.FetchTask()) {
                return;
            }

            WriterData.Status.DurationCounterPrev = WriterData.Status.DurationCounter;
            Reset(WriterData.Status.DurationCounter);
        }

        void TRemoteConnection::WriterSendStatus(TInstant now, bool force) {
            GranStatus.Writer.Update(std::bind(&TRemoteConnection::WriterGetStatus, this), now, force);
        }

        void TRemoteConnection::ReaderSendStatus(TInstant now, bool force) {
            GranStatus.Reader.Update(std::bind(&TRemoteConnection::ReaderFillStatus, this), now, force);
        }

        const TRemoteConnectionReaderStatus& TRemoteConnection::ReaderFillStatus() {
            ReaderData.Status.BufferSize = ReaderData.Buffer.Capacity();
            ReaderData.Status.QuotaMsg = QuotaMsg.Tokens();
            ReaderData.Status.QuotaBytes = QuotaBytes.Tokens();

            return ReaderData.Status;
        }

        void TRemoteConnection::ProcessItem(TReaderTag, ::NActor::TDefaultTag, TWriterToReaderSocketMessage readSocket) {
            if (AtomicGet(ReaderData.Down)) {
                ReaderData.Status.Fd = INVALID_SOCKET;
                return;
            }

            ReaderData.DropChannel();

            ReaderData.Status.Fd = readSocket.Socket;
            ReaderData.SocketVersion = readSocket.SocketVersion;

            if (readSocket.Socket != INVALID_SOCKET) {
                ReaderData.SetChannel(Session->ReadEventLoop.Register(readSocket.Socket, this, ReadCookie));
                ReaderData.Channel->EnableRead();
            }
        }

        void TRemoteConnection::ProcessItem(TWriterTag, TReconnectTag, ui32 socketVersion) {
            Y_ABORT_UNLESS(socketVersion <= WriterData.SocketVersion, "something weird");

            if (WriterData.SocketVersion != socketVersion) {
                return;
            }
            Y_ABORT_UNLESS(WriterData.Status.Connected, "must be connected at this point");
            Y_ABORT_UNLESS(!!WriterData.Channel, "must have channel at this point");

            WriterData.Status.Connected = false;
            WriterData.DropChannel();
            WriterData.Status.MyAddr = TNetAddr();
            ++WriterData.SocketVersion;
            LastConnectAttempt = TInstant();

            TMessagesPtrs cleared;
            ClearOutgoingQueue(cleared, true);
            WriterErrorMessages(cleared, MESSAGE_DELIVERY_FAILED);

            FireClientConnectionEvent(TClientConnectionEvent::DISCONNECTED);

            ReaderGetSocketQueue()->EnqueueAndSchedule(TWriterToReaderSocketMessage(INVALID_SOCKET, WriterData.SocketVersion));
        }

        void TRemoteConnection::ProcessItem(TWriterTag, TWakeReaderTag, ui32 awakeFlags) {
            WriterData.AwakeFlags |= awakeFlags;

            ReadQuotaWakeup();
        }

        void TRemoteConnection::Act(TReaderTag) {
            TInstant now = TInstant::Now();

            ReaderData.Status.Acts += 1;

            ReaderGetSocketQueue()->DequeueAllLikelyEmpty();

            if (AtomicGet(ReaderData.Down)) {
                ReaderData.DropChannel();

                ReaderProcessStatusDown();
                ReaderData.ShutdownComplete.Signal();

            } else if (!!ReaderData.Channel) {
                Y_ASSERT(ReaderData.ReadMessages.empty());

                for (int i = 0;; ++i) {
                    if (i == 100) {
                        // perform other tasks
                        GetReaderActor()->AddTaskFromActorLoop();
                        break;
                    }

                    if (NeedInterruptRead()) {
                        ReaderData.Channel->EnableRead();
                        break;
                    }

                    if (!ReaderFillBuffer())
                        break;

                    if (!ReaderProcessBuffer())
                        break;
                }

                ReaderFlushMessages();
            }

            ReaderSendStatus(now);
        }

        bool TRemoteConnection::QuotaAcquire(size_t msg, size_t bytes) {
            ui32 wakeFlags = 0;

            if (!QuotaMsg.Acquire(msg))
                wakeFlags |= WAKE_QUOTA_MSG;

            else if (!QuotaBytes.Acquire(bytes))
                wakeFlags |= WAKE_QUOTA_BYTES;

            if (wakeFlags) {
                ReaderData.Status.QuotaExhausted++;

                WriterGetWakeQueue()->EnqueueAndSchedule(wakeFlags);
            }

            return wakeFlags == 0;
        }

        void TRemoteConnection::QuotaConsume(size_t msg, size_t bytes) {
            QuotaMsg.Consume(msg);
            QuotaBytes.Consume(bytes);
        }

        void TRemoteConnection::QuotaReturnSelf(size_t items, size_t bytes) {
            if (QuotaReturnValues(items, bytes))
                ReadQuotaWakeup();
        }

        void TRemoteConnection::QuotaReturnAside(size_t items, size_t bytes) {
            if (QuotaReturnValues(items, bytes) && !AtomicGet(WriterData.Down))
                WriterGetWakeQueue()->EnqueueAndSchedule(0x0);
        }

        bool TRemoteConnection::QuotaReturnValues(size_t items, size_t bytes) {
            bool rMsg = QuotaMsg.Return(items);
            bool rBytes = QuotaBytes.Return(bytes);

            return rMsg || rBytes;
        }

        void TRemoteConnection::ReadQuotaWakeup() {
            const ui32 mask = WriterData.AwakeFlags & WriteWakeFlags();

            if (mask && mask == WriterData.AwakeFlags) {
                WriterData.Status.ReaderWakeups++;
                WriterData.AwakeFlags = 0;

                ScheduleRead();
            }
        }

        ui32 TRemoteConnection::WriteWakeFlags() const {
            ui32 awakeFlags = 0;

            if (QuotaMsg.IsAboveWake())
                awakeFlags |= WAKE_QUOTA_MSG;

            if (QuotaBytes.IsAboveWake())
                awakeFlags |= WAKE_QUOTA_BYTES;

            return awakeFlags;
        }

        bool TRemoteConnection::ReaderProcessBuffer() {
            TInstant now = TInstant::Now();

            for (;;) {
                if (!ReaderData.HasBytesInBuf(sizeof(TBusHeader))) {
                    break;
                }

                TBusHeader header(MakeArrayRef(ReaderData.Buffer.Data() + ReaderData.Offset, ReaderData.Buffer.Size() - ReaderData.Offset));

                if (header.Size < sizeof(TBusHeader)) {
                    LWPROBE(Error, ToString(MESSAGE_HEADER_CORRUPTED), ToString(PeerAddr), ToString(header.Size));
                    ReaderData.Status.Incremental.StatusCounter[MESSAGE_HEADER_CORRUPTED] += 1;
                    ScheduleShutdownOnServerOrReconnectOnClient(MESSAGE_HEADER_CORRUPTED, false);
                    return false;
                }

                if (!IsVersionNegotiation(header) && !IsBusKeyValid(header.Id)) {
                    LWPROBE(Error, ToString(MESSAGE_HEADER_CORRUPTED), ToString(PeerAddr), ToString(header.Size));
                    ReaderData.Status.Incremental.StatusCounter[MESSAGE_HEADER_CORRUPTED] += 1;
                    ScheduleShutdownOnServerOrReconnectOnClient(MESSAGE_HEADER_CORRUPTED, false);
                    return false;
                }

                if (header.Size > Config.MaxMessageSize) {
                    LWPROBE(Error, ToString(MESSAGE_MESSAGE_TOO_LARGE), ToString(PeerAddr), ToString(header.Size));
                    ReaderData.Status.Incremental.StatusCounter[MESSAGE_MESSAGE_TOO_LARGE] += 1;
                    ScheduleShutdownOnServerOrReconnectOnClient(MESSAGE_MESSAGE_TOO_LARGE, false);
                    return false;
                }

                if (!ReaderData.HasBytesInBuf(header.Size)) {
                    if (ReaderData.Offset == 0) {
                        ReaderData.Buffer.Reserve(header.Size);
                    }
                    break;
                }

                if (!QuotaAcquire(1, header.Size))
                    return false;

                if (!MessageRead(MakeArrayRef(ReaderData.Buffer.Data() + ReaderData.Offset, header.Size), now)) {
                    return false;
                }

                ReaderData.Offset += header.Size;
            }

            ReaderData.Buffer.ChopHead(ReaderData.Offset);
            ReaderData.Offset = 0;

            if (ReaderData.Buffer.Capacity() > MaxBufferSize && ReaderData.Buffer.Size() <= MaxBufferSize) {
                ReaderData.Status.Incremental.BufferDrops += 1;

                TBuffer temp;
                // probably should use another constant
                temp.Reserve(Config.DefaultBufferSize);
                temp.Append(ReaderData.Buffer.Data(), ReaderData.Buffer.Size());

                ReaderData.Buffer.Swap(temp);
            }

            return true;
        }

        bool TRemoteConnection::ReaderFillBuffer() {
            if (!ReaderData.BufferMore())
                return true;

            if (ReaderData.Buffer.Avail() == 0) {
                if (ReaderData.Buffer.Size() == 0) {
                    ReaderData.Buffer.Reserve(Config.DefaultBufferSize);
                } else {
                    ReaderData.Buffer.Reserve(ReaderData.Buffer.Size() * 2);
                }
            }

            Y_ASSERT(ReaderData.Buffer.Avail() > 0);

            ssize_t bytes;
            {
                TWhatThreadDoesPushPop pp("recv syscall");
                bytes = SocketRecv(ReaderData.Channel->GetSocket(), TArrayRef<char>(ReaderData.Buffer.Pos(), ReaderData.Buffer.Avail()));
            }

            if (bytes < 0) {
                if (WouldBlock()) {
                    ReaderData.Channel->EnableRead();
                    return false;
                } else {
                    ReaderData.Channel->DisableRead();
                    ScheduleShutdownOnServerOrReconnectOnClient(MESSAGE_DELIVERY_FAILED, false);
                    return false;
                }
            }

            if (bytes == 0) {
                ReaderData.Channel->DisableRead();
                // TODO: incorrect: it is possible that only input is shutdown, and output is available
                ScheduleShutdownOnServerOrReconnectOnClient(MESSAGE_DELIVERY_FAILED, false);
                return false;
            }

            ReaderData.Status.Incremental.NetworkOps += 1;

            ReaderData.Buffer.Advance(bytes);
            ReaderData.MoreBytes = 0;
            return true;
        }

        void TRemoteConnection::ClearBeforeSendQueue(EMessageStatus reason) {
            BeforeSendQueue.DequeueAll(std::bind(&TRemoteConnection::WriterBeforeWriteErrorMessage, this, std::placeholders::_1, reason));
        }

        void TRemoteConnection::ClearReplyQueue(EMessageStatus reason) {
            TVectorSwaps<TBusMessagePtrAndHeader> replyQueueTemp;
            Y_ASSERT(replyQueueTemp.empty());
            ReplyQueue.DequeueAllSingleConsumer(&replyQueueTemp);

            TVector<TBusMessage*> messages;
            for (TVectorSwaps<TBusMessagePtrAndHeader>::reverse_iterator message = replyQueueTemp.rbegin();
                 message != replyQueueTemp.rend(); ++message) {
                messages.push_back(message->MessagePtr.Release());
            }

            WriterErrorMessages(messages, reason);

            replyQueueTemp.clear();
        }

        void TRemoteConnection::ProcessBeforeSendQueueMessage(TBusMessage* message, TInstant now) {
            // legacy clients expect this field to be set
            if (!Session->IsSource_) {
                message->SendTime = now.MilliSeconds();
            }

            WriterData.SendQueue.PushBack(message);
        }

        void TRemoteConnection::ProcessBeforeSendQueue(TInstant now) {
            BeforeSendQueue.DequeueAll(std::bind(&TRemoteConnection::ProcessBeforeSendQueueMessage, this, std::placeholders::_1, now));
        }

        void TRemoteConnection::WriterFillInFlight() {
            // this is hack for TLoadBalancedProtocol
            WriterFillStatus();
            AtomicSet(WriterData.InFlight, WriterData.Status.GetInFlight());
        }

        const TRemoteConnectionWriterStatus& TRemoteConnection::WriterGetStatus() {
            WriterRotateCounters();
            WriterFillStatus();

            return WriterData.Status;
        }

        void TRemoteConnection::WriterFillStatus() {
            if (!!WriterData.Channel) {
                WriterData.Status.Fd = WriterData.Channel->GetSocket();
            } else {
                WriterData.Status.Fd = INVALID_SOCKET;
            }
            WriterData.Status.BufferSize = WriterData.Buffer.Capacity();
            WriterData.Status.SendQueueSize = WriterData.SendQueue.Size();
            WriterData.Status.State = WriterData.State;
        }

        void TRemoteConnection::WriterProcessStatusDown() {
            Session->GetDeadConnectionWriterStatusQueue()->EnqueueAndSchedule(WriterData.Status.Incremental);
            Reset(WriterData.Status.Incremental);
        }

        void TRemoteConnection::ReaderProcessStatusDown() {
            Session->GetDeadConnectionReaderStatusQueue()->EnqueueAndSchedule(ReaderData.Status.Incremental);
            Reset(ReaderData.Status.Incremental);
        }

        void TRemoteConnection::ProcessWriterDown() {
            if (!RemovedFromSession) {
                Session->GetRemoveConnectionQueue()->EnqueueAndSchedule(this);

                if (Session->IsSource_) {
                    if (WriterData.Status.Connected) {
                        FireClientConnectionEvent(TClientConnectionEvent::DISCONNECTED);
                    }
                }

                LWPROBE(Disconnected, ToString(PeerAddr));
                RemovedFromSession = true;
            }

            WriterData.DropChannel();

            DropEnqueuedData(ShutdownReason, MESSAGE_SHUTDOWN);

            WriterProcessStatusDown();

            WriterData.ShutdownComplete.Signal();
        }

        void TRemoteConnection::DropEnqueuedData(EMessageStatus reason, EMessageStatus reasonForQueues) {
            ClearReplyQueue(reasonForQueues);
            ClearBeforeSendQueue(reasonForQueues);
            WriterGetReconnectQueue()->Clear();
            WriterGetWakeQueue()->Clear();

            TMessagesPtrs cleared;
            ClearOutgoingQueue(cleared, false);

            if (!Session->IsSource_) {
                for (auto& i : cleared) {
                    TBusMessagePtrAndHeader h(i);
                    CheckedCast<TRemoteServerSession*>(Session.Get())->ReleaseInWorkResponses(MakeArrayRef(&h, 1));
                    // assignment back is weird
                    i = h.MessagePtr.Release();
                    // and this part is not batch
                }
            }

            WriterErrorMessages(cleared, reason);
        }

        void TRemoteConnection::BeforeTryWrite() {
        }

        void TRemoteConnection::Act(TWriterTag) {
            TInstant now = TInstant::Now();

            WriterData.Status.Acts += 1;

            if (Y_UNLIKELY(AtomicGet(WriterData.Down))) {
                // dump status must work even if WriterDown
                WriterSendStatus(now, true);
                ProcessWriterDown();
                return;
            }

            ProcessBeforeSendQueue(now);

            BeforeTryWrite();

            WriterFillInFlight();

            WriterGetReconnectQueue()->DequeueAllLikelyEmpty();

            if (!WriterData.Status.Connected) {
                TryConnect();
            } else {
                for (int i = 0;; ++i) {
                    if (i == 100) {
                        // perform other tasks
                        GetWriterActor()->AddTaskFromActorLoop();
                        break;
                    }

                    if (WriterData.State == WRITER_FILLING) {
                        WriterFillBuffer();

                        if (WriterData.State == WRITER_FILLING) {
                            WriterData.Channel->DisableWrite();
                            break;
                        }

                        Y_ASSERT(!WriterData.Buffer.Empty());
                    }

                    if (WriterData.State == WRITER_FLUSHING) {
                        WriterFlushBuffer();

                        if (WriterData.State == WRITER_FLUSHING) {
                            break;
                        }
                    }
                }
            }

            WriterGetWakeQueue()->DequeueAllLikelyEmpty();

            WriterSendStatus(now);
        }

        void TRemoteConnection::WriterFlushBuffer() {
            Y_ASSERT(WriterData.State == WRITER_FLUSHING);
            Y_ASSERT(!WriterData.Buffer.Empty());

            WriterData.CorkUntil = TInstant::Zero();

            while (!WriterData.Buffer.Empty()) {
                ssize_t bytes;
                {
                    TWhatThreadDoesPushPop pp("send syscall");
                    bytes = SocketSend(WriterData.Channel->GetSocket(), TArrayRef<const char>(WriterData.Buffer.LeftPos(), WriterData.Buffer.Size()));
                }

                if (bytes < 0) {
                    if (WouldBlock()) {
                        WriterData.Channel->EnableWrite();
                        return;
                    } else {
                        WriterData.Channel->DisableWrite();
                        ScheduleShutdownOnServerOrReconnectOnClient(MESSAGE_DELIVERY_FAILED, true);
                        return;
                    }
                }

                WriterData.Status.Incremental.NetworkOps += 1;

                WriterData.Buffer.LeftProceed(bytes);
            }

            WriterData.Buffer.Clear();
            if (WriterData.Buffer.Capacity() > MaxBufferSize) {
                WriterData.Status.Incremental.BufferDrops += 1;
                WriterData.Buffer.Reset();
            }

            WriterData.State = WRITER_FILLING;
        }

        void TRemoteConnection::ScheduleShutdownOnServerOrReconnectOnClient(EMessageStatus status, bool writer) {
            if (Session->IsSource_) {
                WriterGetReconnectQueue()->EnqueueAndSchedule(writer ? WriterData.SocketVersion : ReaderData.SocketVersion);
            } else {
                ScheduleShutdown(status);
            }
        }

        void TRemoteConnection::ScheduleShutdown(EMessageStatus status) {
            ShutdownReason = status;

            AtomicSet(ReaderData.Down, 1);
            ScheduleRead();

            AtomicSet(WriterData.Down, 1);
            ScheduleWrite();
        }

        void TRemoteConnection::CallSerialize(TBusMessage* msg, TBuffer& buffer) const {
            size_t posForAssertion = buffer.Size();
            Proto->Serialize(msg, buffer);
            Y_ABORT_UNLESS(buffer.Size() >= posForAssertion,
                     "incorrect Serialize implementation, pos before serialize: %d, pos after serialize: %d",
                     int(posForAssertion), int(buffer.Size()));
        }

        namespace {
            inline void WriteHeader(const TBusHeader& header, TBuffer& data) {
                data.Reserve(data.Size() + sizeof(TBusHeader));
                /// \todo hton instead of memcpy
                memcpy(data.Data() + data.Size(), &header, sizeof(TBusHeader));
                data.Advance(sizeof(TBusHeader));
            }

            inline void WriteDummyHeader(TBuffer& data) {
                data.Resize(data.Size() + sizeof(TBusHeader));
            }

        }

        void TRemoteConnection::SerializeMessage(TBusMessage* msg, TBuffer* data, TMessageCounter* counter) const {
            size_t pos = data->Size();

            size_t dataSize;

            bool compressionRequested = msg->IsCompressed();

            if (compressionRequested) {
                TBuffer compdata;
                TBuffer plaindata;
                CallSerialize(msg, plaindata);

                dataSize = sizeof(TBusHeader) + plaindata.Size();

                NCodecs::TCodecPtr c = Proto->GetTransportCodec();
                c->Encode(TStringBuf{plaindata.data(), plaindata.size()}, compdata);

                if (compdata.Size() < plaindata.Size()) {
                    plaindata.Clear();
                    msg->GetHeader()->Size = sizeof(TBusHeader) + compdata.Size();
                    WriteHeader(*msg->GetHeader(), *data);
                    data->Append(compdata.Data(), compdata.Size());
                } else {
                    compdata.Clear();
                    msg->SetCompressed(false);
                    msg->GetHeader()->Size = sizeof(TBusHeader) + plaindata.Size();
                    WriteHeader(*msg->GetHeader(), *data);
                    data->Append(plaindata.Data(), plaindata.Size());
                }
            } else {
                WriteDummyHeader(*data);
                CallSerialize(msg, *data);

                dataSize = msg->GetHeader()->Size = data->Size() - pos;

                data->Proceed(pos);
                WriteHeader(*msg->GetHeader(), *data);
                data->Proceed(pos + msg->GetHeader()->Size);
            }

            Y_ASSERT(msg->GetHeader()->Size == data->Size() - pos);
            counter->AddMessage(dataSize, data->Size() - pos, msg->IsCompressed(), compressionRequested);
        }

        TBusMessage* TRemoteConnection::DeserializeMessage(TArrayRef<const char> dataRef, const TBusHeader* header, TMessageCounter* messageCounter, EMessageStatus* status) const {
            size_t dataSize;

            TBusMessage* message;
            if (header->FlagsInternal & MESSAGE_COMPRESS_INTERNAL) {
                TBuffer msg;
                {
                    TBuffer plaindata;
                    NCodecs::TCodecPtr c = Proto->GetTransportCodec();
                    try {
                        TArrayRef<const char> payload = TBusMessage::GetPayload(dataRef);
                        c->Decode(TStringBuf{payload.data(), payload.size()}, plaindata);
                    } catch (...) {
                        // catch all, because
                        // http://nga.at.yandex-team.ru/replies.xml?item_no=3884
                        *status = MESSAGE_DECOMPRESS_ERROR;
                        return nullptr;
                    }

                    msg.Append(dataRef.data(), sizeof(TBusHeader));
                    msg.Append(plaindata.Data(), plaindata.Size());
                }
                TArrayRef<const char> msgRef(msg.Data(), msg.Size());
                dataSize = sizeof(TBusHeader) + msgRef.size();
                // TODO: track error types
                message = Proto->Deserialize(header->Type, msgRef.Slice(sizeof(TBusHeader))).Release();
                if (!message) {
                    *status = MESSAGE_DESERIALIZE_ERROR;
                    return nullptr;
                }
                *message->GetHeader() = *header;
                message->SetCompressed(true);
            } else {
                dataSize = dataRef.size();
                message = Proto->Deserialize(header->Type, dataRef.Slice(sizeof(TBusHeader))).Release();
                if (!message) {
                    *status = MESSAGE_DESERIALIZE_ERROR;
                    return nullptr;
                }
                *message->GetHeader() = *header;
            }

            messageCounter->AddMessage(dataSize, dataRef.size(), header->FlagsInternal & MESSAGE_COMPRESS_INTERNAL, false);

            return message;
        }

        void TRemoteConnection::ResetOneWayFlag(TArrayRef<TBusMessage*> messages) {
            for (auto message : messages) {
                message->LocalFlags &= ~MESSAGE_ONE_WAY_INTERNAL;
            }
        }

        void TRemoteConnection::ReaderFlushMessages() {
            if (!ReaderData.ReadMessages.empty()) {
                Session->OnMessageReceived(this, ReaderData.ReadMessages);
                ReaderData.ReadMessages.clear();
            }
        }

        // @return false if actor should break
        bool TRemoteConnection::MessageRead(TArrayRef<const char> readDataRef, TInstant now) {
            TBusHeader header(readDataRef);

            Y_ASSERT(readDataRef.size() == header.Size);

            if (header.GetVersionInternal() != YBUS_VERSION) {
                ReaderProcessMessageUnknownVersion(readDataRef);
                return true;
            }

            EMessageStatus deserializeFailureStatus = MESSAGE_OK;
            TBusMessage* r = DeserializeMessage(readDataRef, &header, &ReaderData.Status.Incremental.MessageCounter, &deserializeFailureStatus);

            if (!r) {
                Y_ABORT_UNLESS(deserializeFailureStatus != MESSAGE_OK, "state check");
                LWPROBE(Error, ToString(deserializeFailureStatus), ToString(PeerAddr), "");
                ReaderData.Status.Incremental.StatusCounter[deserializeFailureStatus] += 1;
                ScheduleShutdownOnServerOrReconnectOnClient(deserializeFailureStatus, false);
                return false;
            }

            LWPROBE(Read, r->GetHeader()->Size);

            r->ReplyTo = PeerAddrSocketAddr;

            TBusMessagePtrAndHeader h(r);
            r->RecvTime = now;

            QuotaConsume(1, header.Size);

            ReaderData.ReadMessages.push_back(h);
            if (ReaderData.ReadMessages.size() >= 100) {
                ReaderFlushMessages();
            }

            return true;
        }

        void TRemoteConnection::WriterFillBuffer() {
            Y_ASSERT(WriterData.State == WRITER_FILLING);

            Y_ASSERT(WriterData.Buffer.LeftSize() == 0);

            if (Y_UNLIKELY(!WrongVersionRequests.IsEmpty())) {
                TVector<TBusHeader> headers;
                WrongVersionRequests.DequeueAllSingleConsumer(&headers);
                for (TVector<TBusHeader>::reverse_iterator header = headers.rbegin();
                     header != headers.rend(); ++header) {
                    TBusHeader response = *header;
                    response.SendTime = NBus::Now();
                    response.Size = sizeof(TBusHeader);
                    response.FlagsInternal = 0;
                    response.SetVersionInternal(YBUS_VERSION);
                    WriteHeader(response, WriterData.Buffer.GetBuffer());
                }

                Y_ASSERT(!WriterData.Buffer.Empty());
                WriterData.State = WRITER_FLUSHING;
                return;
            }

            TTempTlsVector<TBusMessagePtrAndHeader, void, TVectorSwaps> writeMessages;

            for (;;) {
                THolder<TBusMessage> writeMessage(WriterData.SendQueue.PopFront());
                if (!writeMessage) {
                    break;
                }

                if (Config.Cork != TDuration::Zero()) {
                    if (WriterData.CorkUntil == TInstant::Zero()) {
                        WriterData.CorkUntil = TInstant::Now() + Config.Cork;
                    }
                }

                size_t sizeBeforeSerialize = WriterData.Buffer.Size();

                TMessageCounter messageCounter = WriterData.Status.Incremental.MessageCounter;

                SerializeMessage(writeMessage.Get(), &WriterData.Buffer.GetBuffer(), &messageCounter);

                size_t written = WriterData.Buffer.Size() - sizeBeforeSerialize;
                if (written > Config.MaxMessageSize) {
                    WriterData.Buffer.GetBuffer().EraseBack(written);
                    WriterBeforeWriteErrorMessage(writeMessage.Release(), MESSAGE_MESSAGE_TOO_LARGE);
                    continue;
                }

                WriterData.Status.Incremental.MessageCounter = messageCounter;

                TBusMessagePtrAndHeader h(writeMessage.Release());
                writeMessages.GetVector()->push_back(h);

                Y_ASSERT(!WriterData.Buffer.Empty());
                if (WriterData.Buffer.Size() >= Config.SendThreshold) {
                    break;
                }
            }

            if (!WriterData.Buffer.Empty()) {
                if (WriterData.Buffer.Size() >= Config.SendThreshold) {
                    WriterData.State = WRITER_FLUSHING;
                } else if (WriterData.CorkUntil == TInstant::Zero()) {
                    WriterData.State = WRITER_FLUSHING;
                } else if (TInstant::Now() >= WriterData.CorkUntil) {
                    WriterData.State = WRITER_FLUSHING;
                } else {
                    // keep filling
                    Y_ASSERT(WriterData.State == WRITER_FILLING);
                    GetWriterSchedulerActor()->ScheduleAt(WriterData.CorkUntil);
                }
            } else {
                // keep filling
                Y_ASSERT(WriterData.State == WRITER_FILLING);
            }

            size_t bytes = MessageSize(*writeMessages.GetVector());

            QuotaReturnSelf(writeMessages.GetVector()->size(), bytes);

            // This is called before `send` syscall inducing latency
            MessageSent(*writeMessages.GetVector());
        }

        size_t TRemoteConnection::MessageSize(TArrayRef<TBusMessagePtrAndHeader> messages) {
            size_t size = 0;
            for (const auto& message : messages)
                size += message.MessagePtr->RequestSize;

            return size;
        }

        size_t TRemoteConnection::GetInFlight() {
            return AtomicGet(WriterData.InFlight);
        }

        size_t TRemoteConnection::GetConnectSyscallsNumForTest() {
            return WriterData.Status.ConnectSyscalls;
        }

        void TRemoteConnection::WriterBeforeWriteErrorMessage(TBusMessage* message, EMessageStatus status) {
            if (Session->IsSource_) {
                CheckedCast<TRemoteClientSession*>(Session.Get())->ReleaseInFlight({message});
                WriterErrorMessage(message, status);
            } else {
                TBusMessagePtrAndHeader h(message);
                CheckedCast<TRemoteServerSession*>(Session.Get())->ReleaseInWorkResponses(MakeArrayRef(&h, 1));
                WriterErrorMessage(h.MessagePtr.Release(), status);
            }
        }

        void TRemoteConnection::WriterErrorMessage(TNonDestroyingAutoPtr<TBusMessage> m, EMessageStatus status) {
            TBusMessage* released = m.Release();
            WriterErrorMessages(MakeArrayRef(&released, 1), status);
        }

        void TRemoteConnection::WriterErrorMessages(const TArrayRef<TBusMessage*> ms, EMessageStatus status) {
            ResetOneWayFlag(ms);

            WriterData.Status.Incremental.StatusCounter[status] += ms.size();
            for (auto m : ms) {
                Session->InvokeOnError(m, status);
            }
        }

        void TRemoteConnection::FireClientConnectionEvent(TClientConnectionEvent::EType type) {
            Y_ABORT_UNLESS(Session->IsSource_, "state check");
            TClientConnectionEvent event(type, ConnectionId, PeerAddr);
            TRemoteClientSession* session = CheckedCast<TRemoteClientSession*>(Session.Get());
            session->ClientHandler->OnClientConnectionEvent(event);
        }

        bool TRemoteConnection::IsAlive() const {
            return !AtomicGet(WriterData.Down);
        }

    }
}
