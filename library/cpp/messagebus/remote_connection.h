#pragma once

#include "async_result.h"
#include "defs.h"
#include "event_loop.h"
#include "left_right_buffer.h"
#include "lfqueue_batch.h"
#include "message_ptr_and_header.h"
#include "nondestroying_holder.h"
#include "remote_connection_status.h"
#include "scheduler_actor.h"
#include "socket_addr.h"
#include "storage.h"
#include "vector_swaps.h"
#include "ybus.h"
#include "misc/granup.h"
#include "misc/tokenquota.h"

#include <library/cpp/messagebus/actor/actor.h>
#include <library/cpp/messagebus/actor/executor.h>
#include <library/cpp/messagebus/actor/queue_for_actor.h>
#include <library/cpp/messagebus/actor/queue_in_actor.h>
#include <library/cpp/messagebus/scheduler/scheduler.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/event.h>
#include <util/thread/lfstack.h>

namespace NBus {
    namespace NPrivate {
        class TRemoteConnection;

        typedef TIntrusivePtr<TRemoteConnection> TRemoteConnectionPtr;
        typedef TIntrusivePtr<TBusSessionImpl> TRemoteSessionPtr;

        static void* const WriteCookie = (void*)1;
        static void* const ReadCookie = (void*)2;

        enum {
            WAKE_QUOTA_MSG = 0x01,
            WAKE_QUOTA_BYTES = 0x02
        };

        struct TWriterTag {};
        struct TReaderTag {};
        struct TReconnectTag {};
        struct TWakeReaderTag {};

        struct TWriterToReaderSocketMessage {
            TSocket Socket;
            ui32 SocketVersion;

            TWriterToReaderSocketMessage(TSocket socket, ui32 socketVersion)
                : Socket(socket)
                , SocketVersion(socketVersion)
            {
            }
        };

        class TRemoteConnection
           : public NEventLoop::IEventHandler,
              public ::NActor::TActor<TRemoteConnection, TWriterTag>,
              public ::NActor::TActor<TRemoteConnection, TReaderTag>,
              private ::NActor::TQueueInActor<TRemoteConnection, TWriterToReaderSocketMessage, TReaderTag>,
              private ::NActor::TQueueInActor<TRemoteConnection, ui32, TWriterTag, TReconnectTag>,
              private ::NActor::TQueueInActor<TRemoteConnection, ui32, TWriterTag, TWakeReaderTag>,
              public TScheduleActor<TRemoteConnection, TWriterTag> {
            friend struct TBusSessionImpl;
            friend class TRemoteClientSession;
            friend class TRemoteServerSession;
            friend class ::NActor::TQueueInActor<TRemoteConnection, TWriterToReaderSocketMessage, TReaderTag>;
            friend class ::NActor::TQueueInActor<TRemoteConnection, ui32, TWriterTag, TReconnectTag>;
            friend class ::NActor::TQueueInActor<TRemoteConnection, ui32, TWriterTag, TWakeReaderTag>;

        protected:
            ::NActor::TQueueInActor<TRemoteConnection, TWriterToReaderSocketMessage, TReaderTag>* ReaderGetSocketQueue() {
                return this;
            }

            ::NActor::TQueueInActor<TRemoteConnection, ui32, TWriterTag, TReconnectTag>* WriterGetReconnectQueue() {
                return this;
            }

            ::NActor::TQueueInActor<TRemoteConnection, ui32, TWriterTag, TWakeReaderTag>* WriterGetWakeQueue() {
                return this;
            }

        protected:
            TRemoteConnection(TRemoteSessionPtr session, ui64 connectionId, TNetAddr addr);
            ~TRemoteConnection() override;

            virtual void ClearOutgoingQueue(TMessagesPtrs&, bool reconnect /* or shutdown */);

        public:
            void Send(TNonDestroyingAutoPtr<TBusMessage> msg);
            void Shutdown(EMessageStatus status);

            inline const TNetAddr& GetAddr() const noexcept;

        private:
            friend class TScheduleConnect;
            friend class TWorkIO;

        protected:
            static size_t MessageSize(TArrayRef<TBusMessagePtrAndHeader>);
            bool QuotaAcquire(size_t msg, size_t bytes);
            void QuotaConsume(size_t msg, size_t bytes);
            void QuotaReturnSelf(size_t items, size_t bytes);
            bool QuotaReturnValues(size_t items, size_t bytes);

            bool ReaderProcessBuffer();
            bool ReaderFillBuffer();
            void ReaderFlushMessages();

            void ReadQuotaWakeup();
            ui32 WriteWakeFlags() const;

            virtual bool NeedInterruptRead() {
                return false;
            }

        public:
            virtual void TryConnect();
            void ProcessItem(TReaderTag, ::NActor::TDefaultTag, TWriterToReaderSocketMessage);
            void ProcessItem(TWriterTag, TReconnectTag, ui32 socketVersion);
            void ProcessItem(TWriterTag, TWakeReaderTag, ui32 awakeFlags);
            void Act(TReaderTag);
            inline void WriterBeforeWriteErrorMessage(TBusMessage*, EMessageStatus);
            void ClearBeforeSendQueue(EMessageStatus reasonForQueues);
            void ClearReplyQueue(EMessageStatus reasonForQueues);
            inline void ProcessBeforeSendQueueMessage(TBusMessage*, TInstant now);
            void ProcessBeforeSendQueue(TInstant now);
            void WriterProcessStatusDown();
            void ReaderProcessStatusDown();
            void ProcessWriterDown();
            void DropEnqueuedData(EMessageStatus reason, EMessageStatus reasonForQueues);
            const TRemoteConnectionWriterStatus& WriterGetStatus();
            virtual void WriterFillStatus();
            void WriterFillInFlight();
            virtual void BeforeTryWrite();
            void Act(TWriterTag);
            void ScheduleRead();
            void ScheduleWrite();
            void ScheduleShutdownOnServerOrReconnectOnClient(EMessageStatus status, bool writer);
            void ScheduleShutdown(EMessageStatus status);
            void WriterFlushBuffer();
            void WriterFillBuffer();
            void ReaderSendStatus(TInstant now, bool force = false);
            const TRemoteConnectionReaderStatus& ReaderFillStatus();
            void WriterRotateCounters();
            void WriterSendStatus(TInstant now, bool force = false);
            void WriterSendStatusIfNecessary(TInstant now);
            void QuotaReturnAside(size_t items, size_t bytes);
            virtual void ReaderProcessMessageUnknownVersion(TArrayRef<const char> dataRef) = 0;
            bool MessageRead(TArrayRef<const char> dataRef, TInstant now);
            virtual void MessageSent(TArrayRef<TBusMessagePtrAndHeader> messages) = 0;

            void CallSerialize(TBusMessage* msg, TBuffer& buffer) const;
            void SerializeMessage(TBusMessage* msg, TBuffer* data, TMessageCounter* counter) const;
            TBusMessage* DeserializeMessage(TArrayRef<const char> dataRef, const TBusHeader* header, TMessageCounter* messageCounter, EMessageStatus* status) const;

            void ResetOneWayFlag(TArrayRef<TBusMessage*>);

            inline ::NActor::TActor<TRemoteConnection, TWriterTag>* GetWriterActor() {
                return this;
            }
            inline ::NActor::TActor<TRemoteConnection, TReaderTag>* GetReaderActor() {
                return this;
            }
            inline TScheduleActor<TRemoteConnection, TWriterTag>* GetWriterSchedulerActor() {
                return this;
            }

            void WriterErrorMessage(TNonDestroyingAutoPtr<TBusMessage> m, EMessageStatus status);
            // takes ownership of ms
            void WriterErrorMessages(const TArrayRef<TBusMessage*> ms, EMessageStatus status);

            void FireClientConnectionEvent(TClientConnectionEvent::EType);

            size_t GetInFlight();
            size_t GetConnectSyscallsNumForTest();

            bool IsReturnConnectFailedImmediately() {
                return (bool)AtomicGet(ReturnConnectFailedImmediately);
            }

            bool IsAlive() const;

            TRemoteSessionPtr Session;
            TBusProtocol* const Proto;
            TBusSessionConfig const Config;
            bool RemovedFromSession;
            const ui64 ConnectionId;
            const TNetAddr PeerAddr;
            const TBusSocketAddr PeerAddrSocketAddr;

            const TInstant CreatedTime;
            TInstant LastConnectAttempt;
            TAtomic ReturnConnectFailedImmediately;

        protected:
            ::NActor::TQueueForActor<TBusMessage*> BeforeSendQueue;
            TLockFreeStack<TBusHeader> WrongVersionRequests;

            struct TWriterData {
                TAtomic Down;

                NEventLoop::TChannelPtr Channel;
                ui32 SocketVersion;

                TRemoteConnectionWriterStatus Status;
                TInstant StatusLastSendTime;

                TLocalTasks TimeToRotateCounters;

                TAtomic InFlight;

                TTimedMessages SendQueue;
                ui32 AwakeFlags;
                EWriterState State;
                TLeftRightBuffer Buffer;
                TInstant CorkUntil;

                TSystemEvent ShutdownComplete;

                void SetChannel(NEventLoop::TChannelPtr channel);
                void DropChannel();

                TWriterData();
                ~TWriterData();
            };

            struct TReaderData {
                TAtomic Down;

                NEventLoop::TChannelPtr Channel;
                ui32 SocketVersion;

                TRemoteConnectionReaderStatus Status;
                TInstant StatusLastSendTime;

                TBuffer Buffer;
                size_t Offset;    /* offset in read buffer */
                size_t MoreBytes; /* more bytes required from socket */
                TVectorSwaps<TBusMessagePtrAndHeader> ReadMessages;

                TSystemEvent ShutdownComplete;

                bool BufferMore() const noexcept {
                    return MoreBytes > 0;
                }

                bool HasBytesInBuf(size_t bytes) noexcept;
                void SetChannel(NEventLoop::TChannelPtr channel);
                void DropChannel();

                TReaderData();
                ~TReaderData();
            };

            // owned by session status actor
            struct TGranStatus {
                TGranStatus(TDuration gran)
                    : Writer(gran)
                    , Reader(gran)
                {
                }

                TGranUp<TRemoteConnectionWriterStatus> Writer;
                TGranUp<TRemoteConnectionReaderStatus> Reader;
            };

            TWriterData WriterData;
            TReaderData ReaderData;
            TGranStatus GranStatus;
            TTokenQuota QuotaMsg;
            TTokenQuota QuotaBytes;

            size_t MaxBufferSize;

            // client connection only
            TLockFreeQueueBatch<TBusMessagePtrAndHeader, TVectorSwaps> ReplyQueue;

            EMessageStatus ShutdownReason;
        };

        inline const TNetAddr& TRemoteConnection::GetAddr() const noexcept {
            return PeerAddr;
        }

        typedef TIntrusivePtr<TRemoteConnection> TRemoteConnectionPtr;

    }
}
