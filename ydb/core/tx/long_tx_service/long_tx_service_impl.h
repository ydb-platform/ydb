#pragma once
#include "long_tx_service.h"

#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/util/intrusive_heap.h>
#include <ydb/core/util/ulid.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>

namespace NKikimr {
namespace NLongTxService {

    class TLongTxServiceActor : public TActorBootstrapped<TLongTxServiceActor> {
    private:
        class TCommitActor;
        class TAcquireSnapshotActor;

    private:
        enum class ETxState {
            Uninitialized,
            Active,
            Committing,
        };

        struct TSenderId {
            TActorId Sender;
            ui64 Cookie;
        };

        struct TTransaction {
            TLongTxId TxId;
            TString DatabaseName;
            ETxState State = ETxState::Uninitialized;
            // Maps column shards to known write ids by partId
            using TShardWriteIds = std::vector<ui64>;
            THashMap<ui64, TShardWriteIds> ColumnShardWrites;
            // A list of currently known committers
            TVector<TSenderId> Committers;
            // The currently running commit actor
            TActorId CommitActor;
        };

        enum class ERequestType {
            Commit,
            Rollback,
            AttachColumnShardWrites,
        };

        enum class ERequestState {
            Pending,
            Sent,
        };

        enum class EProxyLockState {
            Unknown,
            Subscribed,
            Unavailable,
        };

        enum class EProxyState {
            Disconnected,
            Connecting,
            Connected,
        };

        struct TProxyRequestState {
            ERequestType Type;
            ERequestState State;
            TActorId Sender;
            ui64 Cookie;
        };

        struct TProxyPendingRequest {
            THolder<IEventHandle> Ev;
            TProxyRequestState* Request = nullptr;
        };

        struct TProxyLockState {
            EProxyLockState State = EProxyLockState::Unknown;
            ui64 LockId = 0;
            ui64 Cookie = 0;
            THashMap<TActorId, ui64> NewSubscribers;
            THashMap<TActorId, ui64> RepliedSubscribers;
            // Intrusive heap support
            size_t HeapIndex = -1;
            TMonotonic ExpiresAt;

            bool Empty() const {
                return NewSubscribers.empty() && RepliedSubscribers.empty();
            }

            struct THeapIndex {
                size_t& operator()(TProxyLockState& value) const {
                    return value.HeapIndex;
                }
            };

            struct THeapLess {
                bool operator()(const TProxyLockState& a, const TProxyLockState& b) const {
                    return a.ExpiresAt < b.ExpiresAt;
                }
            };
        };

        struct TProxyNodeState {
            EProxyState State = EProxyState::Disconnected;
            ui32 NodeId = 0;
            // Currently connected interconnect session
            TActorId Session;
            // Cookie to an active request
            THashMap<ui64, TProxyRequestState> ActiveRequests;
            // Pending events, waiting for the node to become connected
            TVector<TProxyPendingRequest> Pending;
            // Locks requested by local subscribers
            THashMap<ui64, TProxyLockState> Locks;
            TIntrusiveHeap<TProxyLockState, TProxyLockState::THeapIndex, TProxyLockState::THeapLess> LockExpireQueue;
            THashMap<ui64, ui64> CookieToLock;
        };

        struct TAcquireSnapshotUserRequest {
            TActorId Sender;
            ui64 Cookie;
            NLWTrace::TOrbit Orbit;
        };

        struct TAcquireSnapshotBeginTxRequest {
            TLongTxId TxId;
            TActorId Sender;
            ui64 Cookie;
        };

        struct TAcquireSnapshotState {
            TString DatabaseName;
            TVector<TAcquireSnapshotUserRequest> UserRequests;
            TVector<TAcquireSnapshotBeginTxRequest> BeginTxRequests;
        };

        struct TDatabaseSnapshotState {
            THashSet<ui64> ActiveRequests;
            TVector<TAcquireSnapshotUserRequest> PendingUserRequests;
            TVector<TAcquireSnapshotBeginTxRequest> PendingBeginTxRequests;
            bool FlushPending = false;
        };

        struct TLockState {
            ui64 RefCount = 0;

            THashMap<TActorId, ui64> LocalSubscribers;
            THashMap<TActorId, THashMap<TActorId, ui64>> RemoteSubscribers;
        };

        struct TSessionState {
            THashSet<ui64> SubscribedLocks;
        };

    private:
        struct TEvPrivate {
            enum EEv {
                EvCommitFinished = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvAcquireSnapshotFlush,
                EvAcquireSnapshotFinished,
                EvReconnect,
            };

            struct TEvCommitFinished : public TEventLocal<TEvCommitFinished, EvCommitFinished> {
                TLongTxId TxId;
                Ydb::StatusIds::StatusCode Status;
                NYql::TIssues Issues;

                TEvCommitFinished(const TLongTxId& txId, Ydb::StatusIds::StatusCode status)
                    : TxId(txId)
                    , Status(status)
                { }

                TEvCommitFinished(const TLongTxId& txId, Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues)
                    : TxId(txId)
                    , Status(status)
                    , Issues(std::move(issues))
                { }
            };

            struct TEvAcquireSnapshotFlush : public TEventLocal<TEvAcquireSnapshotFlush, EvAcquireSnapshotFlush> {
                TString DatabaseName;

                explicit TEvAcquireSnapshotFlush(TString databaseName)
                    : DatabaseName(std::move(databaseName))
                { }
            };

            struct TEvAcquireSnapshotFinished : public TEventLocal<TEvAcquireSnapshotFinished, EvAcquireSnapshotFinished> {
                TRowVersion Snapshot;
                Ydb::StatusIds::StatusCode Status;
                NYql::TIssues Issues;

                explicit TEvAcquireSnapshotFinished(const TRowVersion& snapshot)
                    : Snapshot(snapshot)
                    , Status(Ydb::StatusIds::SUCCESS)
                { }

                TEvAcquireSnapshotFinished(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues)
                    : Status(status)
                    , Issues(std::move(issues))
                { }
            };

            struct TEvReconnect : public TEventLocal<TEvReconnect, EvReconnect> {
                const ui32 NodeId;

                explicit TEvReconnect(ui32 nodeId)
                    : NodeId(nodeId)
                { }
            };
        };

    private:
        class TSessionSubscribeActor : public TActor<TSessionSubscribeActor> {
            friend class TLongTxServiceActor;

        public:
            TSessionSubscribeActor(TLongTxServiceActor* self)
                : TActor(&TThis::StateWork)
                , Self(self)
            { }

            ~TSessionSubscribeActor() {
                if (Self) {
                    Self->SessionSubscribeActor = nullptr;
                    Self = nullptr;
                }
            }

            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::LONG_TX_SERVICE;
            }

            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvInterconnect::TEvNodeConnected, Handle);
                    hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
                    hFunc(TEvents::TEvUndelivered, Handle);
                }
            }

            void Subscribe(const TActorId& sessionId);
            void Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev);
            void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
            void Handle(TEvents::TEvUndelivered::TPtr& ev);

        private:
            TLongTxServiceActor* Self;
        };

    public:
        TLongTxServiceActor(const TLongTxServiceSettings& settings)
            : Settings(settings)
        {
            Y_UNUSED(Settings); // TODO
        }

        ~TLongTxServiceActor() {
            if (SessionSubscribeActor) {
                SessionSubscribeActor->Self = nullptr;
                SessionSubscribeActor = nullptr;
            }
        }

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::LONG_TX_SERVICE;
        }

        void Bootstrap();

    private:
        TSessionState& SubscribeToSession(const TActorId& sessionId);
        void OnSessionDisconnected(const TActorId& sessionId);

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                sFunc(TEvents::TEvPoison, HandlePoison);
                hFunc(TEvLongTxService::TEvBeginTx, Handle);
                hFunc(TEvLongTxService::TEvCommitTx, Handle);
                hFunc(TEvLongTxService::TEvCommitTxResult, Handle);
                hFunc(TEvPrivate::TEvCommitFinished, Handle);
                hFunc(TEvLongTxService::TEvRollbackTx, Handle);
                hFunc(TEvLongTxService::TEvRollbackTxResult, Handle);
                hFunc(TEvLongTxService::TEvAttachColumnShardWrites, Handle);
                hFunc(TEvLongTxService::TEvAttachColumnShardWritesResult, Handle);
                hFunc(TEvLongTxService::TEvAcquireReadSnapshot, Handle);
                hFunc(TEvPrivate::TEvAcquireSnapshotFlush, Handle);
                hFunc(TEvPrivate::TEvAcquireSnapshotFinished, Handle);
                hFunc(TEvLongTxService::TEvRegisterLock, Handle);
                hFunc(TEvLongTxService::TEvUnregisterLock, Handle);
                hFunc(TEvLongTxService::TEvSubscribeLock, Handle);
                hFunc(TEvLongTxService::TEvLockStatus, Handle);
                hFunc(TEvLongTxService::TEvUnsubscribeLock, Handle);
                hFunc(TEvInterconnect::TEvNodeConnected, Handle);
                hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
                hFunc(TEvPrivate::TEvReconnect, Handle);
                hFunc(TEvents::TEvUndelivered, Handle);
            }
        }

        void HandlePoison();
        void Handle(TEvLongTxService::TEvBeginTx::TPtr& ev);
        void Handle(TEvLongTxService::TEvCommitTx::TPtr& ev);
        void Handle(TEvLongTxService::TEvCommitTxResult::TPtr& ev);
        void Handle(TEvPrivate::TEvCommitFinished::TPtr& ev);
        void Handle(TEvLongTxService::TEvRollbackTx::TPtr& ev);
        void Handle(TEvLongTxService::TEvRollbackTxResult::TPtr& ev);
        void Handle(TEvLongTxService::TEvAttachColumnShardWrites::TPtr& ev);
        void Handle(TEvLongTxService::TEvAttachColumnShardWritesResult::TPtr& ev);
        void Handle(TEvLongTxService::TEvAcquireReadSnapshot::TPtr& ev);
        void Handle(TEvPrivate::TEvAcquireSnapshotFlush::TPtr& ev);
        void Handle(TEvPrivate::TEvAcquireSnapshotFinished::TPtr& ev);
        void Handle(TEvLongTxService::TEvRegisterLock::TPtr& ev);
        void Handle(TEvLongTxService::TEvUnregisterLock::TPtr& ev);
        void Handle(TEvLongTxService::TEvSubscribeLock::TPtr& ev);
        void Handle(TEvLongTxService::TEvLockStatus::TPtr& ev);
        void Handle(TEvLongTxService::TEvUnsubscribeLock::TPtr& ev);

    private:
        void SendViaSession(const TActorId& sessionId, const TActorId& recipient,
                IEventBase* event, ui32 flags = 0, ui64 cookie = 0);

        void SendReply(ERequestType type, TActorId sender, ui64 cookie,
                Ydb::StatusIds::StatusCode status, TStringBuf details);
        void SendReplyIssues(ERequestType type, TActorId sender, ui64 cookie,
                Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues);
        void SendReplyUnavailable(ERequestType type, TActorId sender, ui64 cookie, TStringBuf details);

        TProxyNodeState& ConnectProxyNode(ui32 nodeId);
        void SendProxyRequest(ui32 nodeId, ERequestType type, THolder<IEventHandle> ev);

        void Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev);
        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
        void OnNodeDisconnected(ui32 nodeId, const TActorId& sender);
        void Handle(TEvPrivate::TEvReconnect::TPtr& ev);
        void Handle(TEvents::TEvUndelivered::TPtr& ev);

    private:
        void RemoveUnavailableLock(TProxyNodeState& node, TProxyLockState& lock);

    private:
        void StartCommitActor(TTransaction& tx);
        void ScheduleAcquireSnapshot(const TString& databaseName, TDatabaseSnapshotState& state);
        void StartAcquireSnapshotActor(const TString& databaseName, TDatabaseSnapshotState& state);

    private:
        const TString& GetDatabaseNameOrLegacyDefault(const TString& databaseName);

    private:
        const TLongTxServiceSettings Settings;
        TString LogPrefix;
        TSessionSubscribeActor* SessionSubscribeActor = nullptr;
        THashMap<TULID, TTransaction> Transactions;
        TULIDGenerator IdGenerator;
        THashMap<ui32, TProxyNodeState> ProxyNodes;
        THashMap<TString, TDatabaseSnapshotState> DatabaseSnapshots;
        THashMap<ui64, TAcquireSnapshotState> AcquireSnapshotInFlight;
        TString DefaultDatabaseName;
        THashMap<ui64, TLockState> Locks;
        THashMap<TActorId, TSessionState> Sessions;
        ui64 LastCookie = 0;
    };

} // namespace NLongTxService
} // namespace NKikimr
