#pragma once
#include "long_tx_service.h"

#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/util/ulid.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/interconnect.h>

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
            // Maps column shards to known write ids
            THashMap<ui64, ui64> ColumnShardWrites;
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

        enum class EProxyState {
            Unknown,
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

        struct TProxyNodeState {
            EProxyState State = EProxyState::Unknown;
            // Currently connected interconnect session
            TActorId Session;
            // Cookie to an active request
            THashMap<ui64, TProxyRequestState> ActiveRequests;
            // Pending events, waiting for the node to become connected
            TVector<TProxyPendingRequest> Pending;
        };

        struct TAcquireSnapshotUserRequest {
            TActorId Sender;
            ui64 Cookie;
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

    private:
        struct TEvPrivate {
            enum EEv {
                EvCommitFinished = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvAcquireSnapshotFlush,
                EvAcquireSnapshotFinished,
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
        };

    public:
        TLongTxServiceActor(const TLongTxServiceSettings& settings)
            : Settings(settings)
        {
            Y_UNUSED(Settings); // TODO
        }

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::LONG_TX_SERVICE;
        }

        void Bootstrap();

    private:
        STFUNC(StateWork) {
            Y_UNUSED(ctx);
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
                hFunc(TEvInterconnect::TEvNodeConnected, Handle);
                hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
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

    private:
        void SendReply(ERequestType type, TActorId sender, ui64 cookie,
                Ydb::StatusIds::StatusCode status, TStringBuf details);
        void SendReplyIssues(ERequestType type, TActorId sender, ui64 cookie,
                Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues);
        void SendReplyUnavailable(ERequestType type, TActorId sender, ui64 cookie, TStringBuf details);

        void SendProxyRequest(ui32 nodeId, ERequestType type, THolder<IEventHandle> ev);

        void Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev);
        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
        void OnNodeDisconnected(ui32 nodeId, const TActorId& sender);
        void Handle(TEvents::TEvUndelivered::TPtr& ev);

    private:
        void StartCommitActor(TTransaction& tx);
        void ScheduleAcquireSnapshot(const TString& databaseName, TDatabaseSnapshotState& state);
        void StartAcquireSnapshotActor(const TString& databaseName, TDatabaseSnapshotState& state);

    private:
        const TString& GetDatabaseNameOrLegacyDefault(const TString& databaseName);

    private:
        const TLongTxServiceSettings Settings;
        TString LogPrefix;
        THashMap<TULID, TTransaction> Transactions;
        TULIDGenerator IdGenerator;
        THashMap<ui32, TProxyNodeState> ProxyNodes;
        THashMap<TString, TDatabaseSnapshotState> DatabaseSnapshots;
        THashMap<ui64, TAcquireSnapshotState> AcquireSnapshotInFlight;
        TString DefaultDatabaseName;
        ui64 LastCookie = 0;
    };

} // namespace NLongTxService
} // namespace NKikimr
