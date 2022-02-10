#pragma once

#include "acceptor_status.h"
#include "async_result.h"
#include "event_loop.h"
#include "netaddr.h"
#include "remote_connection.h"
#include "remote_connection_status.h"
#include "session_job_count.h"
#include "shutdown_state.h"
#include "ybus.h"

#include <library/cpp/messagebus/actor/actor.h>
#include <library/cpp/messagebus/actor/queue_in_actor.h>
#include <library/cpp/messagebus/monitoring/mon_proto.pb.h>

#include <library/cpp/threading/future/legacy_future.h>

#include <util/generic/array_ref.h>
#include <util/generic/string.h>

namespace NBus {
    namespace NPrivate {
        typedef TIntrusivePtr<TRemoteClientConnection> TRemoteClientConnectionPtr;
        typedef TIntrusivePtr<TRemoteServerConnection> TRemoteServerConnectionPtr;

        typedef TIntrusivePtr<TRemoteServerSession> TRemoteServerSessionPtr;

        typedef TIntrusivePtr<TAcceptor> TAcceptorPtr;
        typedef TVector<TAcceptorPtr> TAcceptorsPtrs;

        struct TConnectionsAcceptorsSnapshot {
            TVector<TRemoteConnectionPtr> Connections;
            TVector<TAcceptorPtr> Acceptors;
            ui64 LastConnectionId;
            ui64 LastAcceptorId;

            TConnectionsAcceptorsSnapshot();
        };

        typedef TAtomicSharedPtr<TConnectionsAcceptorsSnapshot> TConnectionsAcceptorsSnapshotPtr;

        struct TOnAccept {
            SOCKET s;
            TNetAddr addr;
            TInstant now;
        };

        struct TStatusTag {};
        struct TConnectionTag {};

        struct TDeadConnectionTag {};
        struct TRemoveTag {};

        struct TBusSessionImpl
           : public virtual TBusSession,
              private ::NActor::TActor<TBusSessionImpl, TStatusTag>,
              private ::NActor::TActor<TBusSessionImpl, TConnectionTag>

            ,
              private ::NActor::TQueueInActor<TBusSessionImpl, TRemoteConnectionWriterIncrementalStatus, TStatusTag, TDeadConnectionTag>,
              private ::NActor::TQueueInActor<TBusSessionImpl, TRemoteConnectionReaderIncrementalStatus, TStatusTag, TDeadConnectionTag>,
              private ::NActor::TQueueInActor<TBusSessionImpl, TAcceptorStatus, TStatusTag, TDeadConnectionTag>

            ,
              private ::NActor::TQueueInActor<TBusSessionImpl, TOnAccept, TConnectionTag>,
              private ::NActor::TQueueInActor<TBusSessionImpl, TRemoteConnectionPtr, TConnectionTag, TRemoveTag> {
            friend class TAcceptor;
            friend class TRemoteConnection;
            friend class TRemoteServerConnection;
            friend class ::NActor::TActor<TBusSessionImpl, TStatusTag>;
            friend class ::NActor::TActor<TBusSessionImpl, TConnectionTag>;
            friend class ::NActor::TQueueInActor<TBusSessionImpl, TRemoteConnectionWriterIncrementalStatus, TStatusTag, TDeadConnectionTag>;
            friend class ::NActor::TQueueInActor<TBusSessionImpl, TRemoteConnectionReaderIncrementalStatus, TStatusTag, TDeadConnectionTag>;
            friend class ::NActor::TQueueInActor<TBusSessionImpl, TAcceptorStatus, TStatusTag, TDeadConnectionTag>;
            friend class ::NActor::TQueueInActor<TBusSessionImpl, TOnAccept, TConnectionTag>;
            friend class ::NActor::TQueueInActor<TBusSessionImpl, TRemoteConnectionPtr, TConnectionTag, TRemoveTag>;

        public:
            ::NActor::TQueueInActor<TBusSessionImpl, TOnAccept, TConnectionTag>* GetOnAcceptQueue() {
                return this;
            }

            ::NActor::TQueueInActor<TBusSessionImpl, TRemoteConnectionPtr, TConnectionTag, TRemoveTag>* GetRemoveConnectionQueue() {
                return this;
            }

            ::NActor::TActor<TBusSessionImpl, TConnectionTag>* GetConnectionActor() {
                return this;
            }

            typedef TGuard<TMutex> TConnectionsGuard;

            TBusSessionImpl(bool isSource, TBusMessageQueue* queue, TBusProtocol* proto,
                            IBusErrorHandler* handler,
                            const TBusSessionConfig& config, const TString& name);

            ~TBusSessionImpl() override;

            void Shutdown() override;
            bool IsDown();

            size_t GetInFlightImpl(const TNetAddr& addr) const;
            size_t GetConnectSyscallsNumForTestImpl(const TNetAddr& addr) const;

            void GetInFlightBulk(TArrayRef<const TNetAddr> addrs, TArrayRef<size_t> results) const override;
            void GetConnectSyscallsNumBulkForTest(TArrayRef<const TNetAddr> addrs, TArrayRef<size_t> results) const override;

            virtual void FillStatus();
            TSessionDumpStatus GetStatusRecordInternal() override;
            TString GetStatus(ui16 flags = YBUS_STATUS_CONNS) override;
            TConnectionStatusMonRecord GetStatusProtobuf() override;
            TString GetStatusSingleLine() override;

            void ProcessItem(TStatusTag, TDeadConnectionTag, const TRemoteConnectionWriterIncrementalStatus&);
            void ProcessItem(TStatusTag, TDeadConnectionTag, const TRemoteConnectionReaderIncrementalStatus&);
            void ProcessItem(TStatusTag, TDeadConnectionTag, const TAcceptorStatus&);
            void ProcessItem(TStatusTag, ::NActor::TDefaultTag, const TAcceptorStatus&);
            void ProcessItem(TConnectionTag, ::NActor::TDefaultTag, const TOnAccept&);
            void ProcessItem(TConnectionTag, TRemoveTag, TRemoteConnectionPtr);
            void ProcessConnectionsAcceptorsShapshotQueueItem(TAtomicSharedPtr<TConnectionsAcceptorsSnapshot>);
            void StatusUpdateCachedDump();
            void StatusUpdateCachedDumpIfNecessary(TInstant now);
            void Act(TStatusTag);
            void Act(TConnectionTag);

            TBusProtocol* GetProto() const noexcept override;
            const TBusSessionConfig* GetConfig() const noexcept override;
            TBusMessageQueue* GetQueue() const noexcept override;
            TString GetNameInternal() override;

            virtual void OnMessageReceived(TRemoteConnection* c, TVectorSwaps<TBusMessagePtrAndHeader>& newMsg) = 0;

            void Listen(int port, TBusMessageQueue* q);
            void Listen(const TVector<TBindResult>& bindTo, TBusMessageQueue* q);
            TBusConnection* Accept(SOCKET listen);

            inline ::NActor::TActor<TBusSessionImpl, TStatusTag>* GetStatusActor() {
                return this;
            }
            inline ::NActor::TActor<TBusSessionImpl, TConnectionTag>* GetConnectionsActor() {
                return this;
            }

            typedef THashMap<TBusSocketAddr, TRemoteConnectionPtr> TAddrRemoteConnections;

            void SendSnapshotToStatusActor();

            void InsertConnectionLockAcquired(TRemoteConnection* connection);
            void InsertAcceptorLockAcquired(TAcceptor* acceptor);

            void GetConnections(TVector<TRemoteConnectionPtr>*);
            void GetAcceptors(TVector<TAcceptorPtr>*);
            void GetConnectionsLockAquired(TVector<TRemoteConnectionPtr>*);
            void GetAcceptorsLockAquired(TVector<TAcceptorPtr>*);

            TRemoteConnectionPtr GetConnection(const TBusSocketAddr& addr, bool create);
            TRemoteConnectionPtr GetConnectionById(ui64 id);
            TAcceptorPtr GetAcceptorById(ui64 id);

            void InvokeOnError(TNonDestroyingAutoPtr<TBusMessage>, EMessageStatus);

            void Cron();

            TBusSessionJobCount JobCount;

            // TODO: replace with actor
            TMutex ConnectionsLock;

            struct TImpl;
            THolder<TImpl> Impl;

            const bool IsSource_;

            TBusMessageQueue* const Queue;
            TBusProtocol* const Proto;
            // copied to be available after Proto dies
            const TString ProtoName;

            IBusErrorHandler* const ErrorHandler;
            TUseCountHolder HandlerUseCountHolder;
            TBusSessionConfig Config; // TODO: make const

            NEventLoop::TEventLoop WriteEventLoop;
            NEventLoop::TEventLoop ReadEventLoop;
            THolder<NThreading::TLegacyFuture<void, false>> ReadEventLoopThread;
            THolder<NThreading::TLegacyFuture<void, false>> WriteEventLoopThread;

            THashMap<ui64, TRemoteConnectionPtr> ConnectionsById;
            TAddrRemoteConnections Connections;
            TAcceptorsPtrs Acceptors;

            struct TStatusData {
                TAtomicSharedPtr<TConnectionsAcceptorsSnapshot> ConnectionsAcceptorsSnapshot;
                ::NActor::TQueueForActor<TAtomicSharedPtr<TConnectionsAcceptorsSnapshot>> ConnectionsAcceptorsSnapshotsQueue;

                TAtomicShutdownState ShutdownState;

                TBusSessionStatus Status;

                TSessionDumpStatus StatusDumpCached;
                TMutex StatusDumpCachedMutex;
                TInstant StatusDumpCachedLastUpdate;

                TStatusData();
            };
            TStatusData StatusData;

            struct TConnectionsData {
                TAtomicShutdownState ShutdownState;

                TConnectionsData();
            };
            TConnectionsData ConnectionsData;

            ::NActor::TQueueInActor<TBusSessionImpl, TRemoteConnectionWriterIncrementalStatus,
                                    TStatusTag, TDeadConnectionTag>*
            GetDeadConnectionWriterStatusQueue() {
                return this;
            }

            ::NActor::TQueueInActor<TBusSessionImpl, TRemoteConnectionReaderIncrementalStatus,
                                    TStatusTag, TDeadConnectionTag>*
            GetDeadConnectionReaderStatusQueue() {
                return this;
            }

            ::NActor::TQueueInActor<TBusSessionImpl, TAcceptorStatus,
                                    TStatusTag, TDeadConnectionTag>*
            GetDeadAcceptorStatusQueue() {
                return this;
            }

            template <typename TItem>
            ::NActor::IQueueInActor<TItem>* GetQueue() {
                return this;
            }

            ui64 LastAcceptorId;
            ui64 LastConnectionId;

            TAtomic Down;
            TSystemEvent ShutdownCompleteEvent;
        };

        inline TBusProtocol* TBusSessionImpl::GetProto() const noexcept {
            return Proto;
        }

        inline const TBusSessionConfig* TBusSessionImpl::GetConfig() const noexcept {
            return &Config;
        }

        inline TBusMessageQueue* TBusSessionImpl::GetQueue() const noexcept {
            return Queue;
        }

    }
}
