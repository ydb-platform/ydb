#include <ydb/core/backup/common/table_checksum_factory.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/request_types.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/service_table.h>
#include <ydb/core/kqp/common/simple/mvcc_version.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/query_actor/query_actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <google/protobuf/util/time_util.h>
#include <library/cpp/protobuf/interop/cast.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/string/builder.h>
#include <util/string/escape.h>
#include <util/string/subst.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <optional>

namespace NKikimr::NBackup {
namespace {

    using namespace NActors;
    using namespace NGRpcService;
    using namespace NRpcService;

    struct TEvAliasWakeup : TEventLocal<TEvAliasWakeup, TEvTableChecksum::EvInternalWakeup> {
        TActorId Alias;

        explicit TEvAliasWakeup(TActorId alias)
            : Alias(alias)
        {}
    };

    struct TEvSnapshotRequestTimeout
        : TEventLocal<TEvSnapshotRequestTimeout, TEvTableChecksum::EvSnapshotRequestTimeout> {
        TActorId Alias;

        explicit TEvSnapshotRequestTimeout(TActorId alias)
            : Alias(alias)
        {}
    };

    struct TEvSessionCreated : TEventLocal<TEvSessionCreated, TEvTableChecksum::EvInternalSessionCreated> {
        Ydb::Table::CreateSessionResponse Response;

        explicit TEvSessionCreated(Ydb::Table::CreateSessionResponse response)
            : Response(std::move(response))
        {}
    };

    struct TEvQueryCompleted : TEventLocal<TEvQueryCompleted, TEvTableChecksum::EvInternalQueryCompleted> {
        Ydb::Table::ExecuteDataQueryResponse Response;

        explicit TEvQueryCompleted(Ydb::Table::ExecuteDataQueryResponse response)
            : Response(std::move(response))
        {}
    };

    void DeleteSession(TActorSystem* system, const TString& database, const TString& session) {
        using TRpc = TGrpcRequestOperationCall<Ydb::Table::DeleteSessionRequest, Ydb::Table::DeleteSessionResponse>;
        Ydb::Table::DeleteSessionRequest request;
        request.set_session_id(session);
        DoLocalRpc<TRpc>(std::move(request), database, Nothing(), system, true);
    }

    // The callback can complete after the owning uploader or SchemeShard has stopped
    class TSessionAdmission {
    public:
        TSessionAdmission(TActorSystem* system, TString database)
            : System(system)
            , Database(std::move(database))
        {}

        bool Publish(TString session) {
            Session = std::move(session);
            auto expected = EState::Waiting;
            if (State.compare_exchange_strong(expected, EState::Ready)) {
                return true;
            }
            CloseSession();
            return false;
        }

        TString Take() {
            return State.exchange(EState::Closed) == EState::Ready ? std::move(Session) : TString{};
        }

        void Cancel() {
            if (State.exchange(EState::Closed) == EState::Ready) {
                CloseSession();
            }
        }

    private:
        void CloseSession() const {
            if (!Session.empty()) {
                DeleteSession(System, Database, Session);
            }
        }

        enum class EState {
            Waiting,
            Ready,
            Closed,
        };

        TActorSystem* const System;
        const TString Database;
        TString Session;
        std::atomic<EState> State{ EState::Waiting };
    };

    TString GetIssues(const Ydb::Operations::Operation& operation) {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(operation.issues(), issues);
        return issues.Empty() ? TString(Ydb::StatusIds::StatusCode_Name(operation.status())) : issues.ToOneLineString();
    }

    bool IsLegacyBackupReadRejection(const Ydb::Issue::IssueMessage& issue) {
        const TStringBuf message(issue.message());
        return (message.StartsWith("Can't read from a backup table (shard# ") && message.EndsWith(")")) ||
               AnyOf(issue.issues(), IsLegacyBackupReadRejection);
    }

    class TChecksumItem {
    public:
        TChecksumItem(TActorId alias, TActorId hostId, TActorId replyTo, TEvTableChecksum::TOwnerPtr owner,
            TEvTableChecksum::TItem item)
            : Alias(alias)
            , HostId(hostId)
            , ReplyTo(replyTo)
            , Owner(std::move(owner))
            , OperationId(item.OperationId)
            , ItemIdx(item.ItemIdx)
            , Database(std::move(item.Database))
            , TablePath(std::move(item.TablePath))
            , Importing(item.Importing)
            , TableId(std::move(item.TableId))
            , Snapshot(item.Snapshot)
            , Algorithm(std::move(item.Algorithm))
        {}

        enum class EState {
            Idle,
            AcquiringSnapshot,
            WaitingForSnapshot,
            Querying,
            Reported,
        };

        static constexpr TDuration RetryWindow = TDuration::Minutes(5);
        static constexpr TDuration InitialRetryDelay = TDuration::Seconds(1);
        static constexpr TDuration MaxRetryDelay = TDuration::Seconds(10);

        void ReplyError(Ydb::StatusIds::StatusCode status, const TString& error) {
            SendResult(new TEvTableChecksum::TEvResult(OperationId, ItemIdx, status, {}, error));
            FinishResult();
        }

        void FinishResult() {
            // The owner stops us after committing the comparison or finishing the upload
            SnapshotHandle.reset();
            State = EState::Reported;
        }

        void Prepare() {
            if (!Importing) {
                return;
            }
            if (!TableId.PathId || !Snapshot.Step || !Snapshot.TxId) {
                SnapshotError = "restore snapshot checkpoint is unavailable";
                return;
            }
            State = EState::AcquiringSnapshot;
            Send(NLongTxService::MakeLongTxServiceID(Alias.NodeId()),
                new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(
                    Database, TVector<TTableId>{ TableId }, Snapshot));
            const auto timeout =
                TDuration::Seconds(AppData()->LongTxServiceConfig.GetLocalSnapshotPromotionTimeSeconds());
            Schedule(timeout, new TEvSnapshotRequestTimeout(Alias));
        }

        void Handle(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr& ev) {
            if (State != EState::AcquiringSnapshot && State != EState::WaitingForSnapshot) {
                return;
            }
            auto& result = *ev->Get();
            if (result.Status != Ydb::StatusIds::SUCCESS) {
                const auto issues = result.Issues.ToOneLineString();
                SnapshotError = issues.empty() ? TString(Ydb::StatusIds::StatusCode_Name(result.Status)) : issues;
            } else if (result.Snapshot != Snapshot || !result.SnapshotHandle) {
                SnapshotError = "LongTxService did not retain the requested restore snapshot";
            } else {
                SnapshotHandle.emplace(std::move(result.SnapshotHandle));
            }
            CompleteSnapshotRequest();
        }

        void SnapshotRequestTimedOut() {
            if (State == EState::AcquiringSnapshot || State == EState::WaitingForSnapshot) {
                SnapshotError = "restore snapshot retention request timed out";
                CompleteSnapshotRequest();
            }
        }

        void CompleteSnapshotRequest() {
            const bool start = State == EState::WaitingForSnapshot;
            State = EState::Idle;
            if (start) {
                Start();
            }
        }

        bool HasLiveSnapshot() const {
            return !Importing || (SnapshotHandle && static_cast<bool>(*SnapshotHandle));
        }

        void FailSnapshotValidation() {
            ReplyError(Ydb::StatusIds::PRECONDITION_FAILED,
                SnapshotError.empty() ? TString("restore snapshot retention was lost") : SnapshotError);
        }

        void Start() {
            if (State == EState::AcquiringSnapshot) {
                State = EState::WaitingForSnapshot;
                return;
            }
            if (State != EState::Idle) {
                return;
            }
            if (!HasLiveSnapshot()) {
                return FailSnapshotValidation();
            }
            if (TablePath.empty()) {
                return ReplyError(Ydb::StatusIds::PRECONDITION_FAILED, "Table checksum checkpoint is missing");
            }
            QueryStartedAt = TActivationContext::Monotonic();
            State = EState::Querying;
            CreateSession();
        }

        bool Retry(Ydb::StatusIds::StatusCode status) {
            if (TQueryRetryActorBase::Retryable(status) == ERetryErrorClass::NoRetry) {
                return false;
            }
            const auto now = TActivationContext::Monotonic();
            if (!RetryDeadline) {
                RetryDeadline = now + RetryWindow;
            }
            if (now >= RetryDeadline) {
                return false;
            }
            Schedule(Min(RetryBackoff.Next(), RetryDeadline - now), new TEvAliasWakeup(Alias));
            return true;
        }

        void Handle(TEvTableChecksum::TEvResult::TPtr& ev) {
            if (ev->Get()->OperationId != OperationId || ev->Get()->ItemIdx != ItemIdx ||
                (State != EState::Idle && State != EState::Querying)) {
                return;
            }
            if (!HasLiveSnapshot()) {
                return FailSnapshotValidation();
            }
            if (TActivationContext::Monotonic() - QueryStartedAt >= RetryWindow) {
                // A long-running attempt separates consecutive failure bursts
                RetryDeadline = TMonotonic();
                RetryBackoff.Reset();
            }
            State = EState::Idle;
            if (!Retry(ev->Get()->Status)) {
                SendResult(ev->Release().Release());
                FinishResult();
            }
        }

        void CreateSession() {
            using TRpc = TGrpcRequestOperationCall<Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse>;
            auto* system = TActivationContext::ActorSystem();
            if (!system->LookupLocalService(NKqp::MakeKqpProxyID(HostId.NodeId()))) {
                Send(Alias, new TEvTableChecksum::TEvResult(OperationId, ItemIdx, Ydb::StatusIds::UNSUPPORTED, {},
                                "table checksum query requires KQP on this node"));
                return;
            }
            Admission = std::make_shared<TSessionAdmission>(system, Database);
            DoLocalRpc<TRpc>({}, Database, Nothing(), system, true).Subscribe([system, alias = Alias,
                                                                                  admission = Admission](auto future) {
                auto response = future.ExtractValue();
                Ydb::Table::CreateSessionResult result;
                response.operation().result().UnpackTo(&result);
                if (admission->Publish(std::move(*result.mutable_session_id()))) {
                    system->Send(alias, new TEvSessionCreated(std::move(response)));
                }
            });
        }

        void Handle(TEvSessionCreated::TPtr& ev) {
            TString session = Admission->Take();
            Admission.reset();
            const auto& event = *ev->Get();
            const auto& operation = event.Response.operation();
            if (operation.status() != Ydb::StatusIds::SUCCESS || session.empty()) {
                const auto status =
                    operation.status() == Ydb::StatusIds::SUCCESS ? Ydb::StatusIds::INTERNAL_ERROR : operation.status();
                const TString error = TStringBuilder() << "Failed to create session: " << GetIssues(operation);
                LOG_WARN_S(TActivationContext::AsActorContext(),
                    Importing ? NKikimrServices::IMPORT : NKikimrServices::EXPORT, error);
                Send(Alias, new TEvTableChecksum::TEvResult(OperationId, ItemIdx, status, {}, error));
                return;
            }

            Session = std::move(session);
            if (!HasLiveSnapshot()) {
                CloseSession();
                return FailSnapshotValidation();
            }
            using TRpc =
                TGrpcRequestOperationCall<Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse>;
            TString path = TablePath;
            SubstGlobal(path, "\\", "\\\\");
            SubstGlobal(path, "`", "\\`");
            TStringBuilder query;
            query << "CHECKSUM TABLE `" << path << "`";
            if (!Algorithm.empty()) {
                query << " WITH (ALGORITHM = \"" << EscapeC(Algorithm) << "\")";
            }
            query << ";";

            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(Session);
            request.mutable_query()->set_yql_text(query);
            request.mutable_query_cache_policy()->set_keep_in_cache(true);
            TMap<TString, TString> metadata;
            auto* begin = request.mutable_tx_control()->mutable_begin_tx();
            if (Importing) {
                begin->mutable_snapshot_read_only();
                metadata.emplace(NKqp::YDB_MVCC_VERSION_HEADER, NKqp::FormatMvccVersion(Snapshot));
            } else {
                // A completed private copy is immutable across shards
                begin->mutable_online_read_only()->set_allow_inconsistent_reads(true);
            }
            request.mutable_tx_control()->set_commit_tx(true);
            const auto maxTimeout = TDuration::Seconds(google::protobuf::util::TimeUtil::kDurationMaxSeconds);
            *request.mutable_operation_params()->mutable_operation_timeout() = NProtoInterop::CastToProto(maxTimeout);
            const auto token = NACLib::TSystemUsers::Metadata().SerializeAsString();
            auto* system = TActivationContext::ActorSystem();
            DoLocalRpc<TRpc>(
                std::move(request), Database, token, TString(NRequestTypes::ChecksumTable), system, metadata, true)
                .Subscribe([system, alias = Alias](auto future) {
                    system->Send(alias, new TEvQueryCompleted(future.ExtractValue()));
                });
        }

        void Handle(TEvQueryCompleted::TPtr& ev) {
            CloseSession();
            const auto& operation = ev->Get()->Response.operation();
            auto status = operation.status();
            TString error;
            TTableChecksum checksum;
            if (status == Ydb::StatusIds::SUCCESS) {
                try {
                    Ydb::Table::ExecuteQueryResult response;
                    Y_ENSURE(operation.result().UnpackTo(&response) && response.result_sets_size() == 1,
                        "invalid result set count");
                    NYdb::TResultSet result(std::move(*response.mutable_result_sets(0)));
                    Y_ENSURE(result.RowsCount() == 1 && result.ColumnsCount() == 4, "invalid result shape");
                    NYdb::TResultSetParser parser(result);
                    Y_ENSURE(parser.TryNextRow(), "empty result");
                    Y_ENSURE(
                        parser.ColumnParser("version").GetUint32() == TTableChecksum::Version, "unsupported version");
                    checksum.Algorithm = parser.ColumnParser("algorithm").GetUtf8();
                    Y_ENSURE(!checksum.Algorithm.empty(), "empty algorithm");
                    checksum.Count = parser.ColumnParser("count").GetUint64();
                    checksum.Hash = parser.ColumnParser("hash").GetString();
                    Y_ENSURE(checksum.Count || checksum.Hash.find_first_not_of('0') == TString::npos,
                        "nonzero checksum for an empty table");
                } catch (const std::exception& ex) {
                    status = Ydb::StatusIds::GENERIC_ERROR;
                    error = TStringBuilder() << "invalid table checksum query result: " << ex.what();
                }
            } else {
                error = GetIssues(operation);
            }
            if (!Importing && status != Ydb::StatusIds::SUCCESS &&
                AnyOf(operation.issues(), IsLegacyBackupReadRejection)) {
                status = Ydb::StatusIds::PRECONDITION_FAILED;
            }
            Send(Alias, new TEvTableChecksum::TEvResult(OperationId, ItemIdx, status, checksum, error));
        }

        void CloseSession() {
            if (Admission) {
                Admission->Cancel();
                Admission.reset();
            }
            if (!Session.empty()) {
                DeleteSession(TActivationContext::ActorSystem(), Database, std::exchange(Session, {}));
            }
        }

        void Send(TActorId recipient, IEventBase* event, ui32 flags = 0, ui64 cookie = 0) const {
            TActivationContext::AsActorContext().MakeFor(Alias).Send(recipient, event, flags, cookie);
        }

        void SendResult(TEvTableChecksum::TEvResult* result) const {
            TActivationContext::AsActorContext().Send(ReplyTo, result);
        }

        void Schedule(TDuration delay, IEventBase* event) const {
            TActivationContext::AsActorContext().Schedule(delay, event);
        }

        const TActorId Alias;
        const TActorId HostId;
        const TActorId ReplyTo;
        const TEvTableChecksum::TOwnerPtr Owner;
        const ui64 OperationId;
        const ui32 ItemIdx;
        const TString Database;
        const TString TablePath;
        const bool Importing;
        const TTableId TableId;
        TRowVersion Snapshot;
        const TString Algorithm;
        EState State = EState::Idle;
        std::optional<NKqp::TSnapshotHandle> SnapshotHandle;
        TString SnapshotError;
        TString Session;
        std::shared_ptr<TSessionAdmission> Admission;
        TMonotonic QueryStartedAt;
        TMonotonic RetryDeadline;
        TBackoff RetryBackoff{ InitialRetryDelay, MaxRetryDelay };
    };

    class TChecksumExecutor final : public ITableChecksumJob {
        using TKey = std::tuple<TActorId, ui64, ui32>;

    public:
        explicit TChecksumExecutor(TChecksumMailboxAliases aliases)
            : Aliases(std::move(aliases))
        {}

        bool Handle(IEventHandle::TPtr& ev) override {
            if (ev->GetTypeRewrite() != TEvAliasWakeup::EventType &&
                ev->GetTypeRewrite() != TEvSnapshotRequestTimeout::EventType && !Items.contains(ev->Recipient)) {
                return false;
            }
            Dispatch(ev);
            return true;
        }

        bool IsIdle() const {
            return Items.empty();
        }

        void Stop() override {
            while (!Items.empty()) {
                RemoveItem(Items.begin()->first);
            }
        }

        TActorId Prepare(TActorId owner, TEvTableChecksum::TItem item, TEvTableChecksum::TOwnerPtr lifetime) {
            if (lifetime && !lifetime->Alive) {
                return {};
            }
            auto& alias = ItemIds[TKey{ owner, item.OperationId, item.ItemIdx }];
            if (!alias) {
                alias = Aliases.Register();
                const bool inserted = Items
                                          .try_emplace(alias, alias, TActivationContext::AsActorContext().SelfID, owner,
                                              std::move(lifetime), std::move(item))
                                          .second;
                Y_DEBUG_ABORT_UNLESS(inserted);
                Items.at(alias).Prepare();
            }
            return alias;
        }

        void Run(TActorId owner, TEvTableChecksum::TItem item, TEvTableChecksum::TOwnerPtr lifetime = {}) {
            if (const auto alias = Prepare(owner, std::move(item), std::move(lifetime))) {
                TActivationContext::AsActorContext().Send(alias, new TEvTableChecksum::TEvStart());
            }
        }

    private:
        void RemoveItem(TActorId alias) {
            auto& item = Items.at(alias);
            item.CloseSession();
            ItemIds.erase(TKey{ item.ReplyTo, item.OperationId, item.ItemIdx });
            Items.erase(alias);
            Aliases.Unregister(alias);
        }

    public:
        void Handle(TEvTableChecksum::TEvFinishItem::TPtr& ev) {
            const auto* request = ev->Get();
            const auto it = ItemIds.find(TKey{ ev->Sender, request->OperationId, request->ItemIdx });
            if (it != ItemIds.end()) {
                RemoveItem(it->second);
            }
        }

        void Handle(TEvTableChecksum::TEvStopOperation::TPtr& ev) {
            const auto op = ev->Get()->OperationId;
            auto it = ItemIds.lower_bound(TKey{ ev->Sender, op, 0 });
            while (it != ItemIds.end() && std::get<0>(it->first) == ev->Sender && std::get<1>(it->first) == op) {
                RemoveItem((it++)->second);
            }
        }

    private:
        TChecksumItem* FindLiveItem(TActorId alias) {
            auto* item = Items.FindPtr(alias);
            return item && (!item->Owner || item->Owner->Alive) ? item : nullptr;
        }

        void Dispatch(IEventHandle::TPtr& ev) {
            auto alias = ev->Recipient;
            if (ev->GetTypeRewrite() == TEvAliasWakeup::EventType) {
                alias = ev->Get<TEvAliasWakeup>()->Alias;
            } else if (ev->GetTypeRewrite() == TEvSnapshotRequestTimeout::EventType) {
                alias = ev->Get<TEvSnapshotRequestTimeout>()->Alias;
            }
            auto* item = FindLiveItem(alias);
            if (!item) {
                return;
            }

            switch (ev->GetTypeRewrite()) {
                sFunc(TEvTableChecksum::TEvStart, item->Start);
                hFunc(TEvTableChecksum::TEvResult, item->Handle);
                hFunc(TEvSessionCreated, item->Handle);
                hFunc(TEvQueryCompleted, item->Handle);
                sFunc(TEvAliasWakeup, item->Start);
                sFunc(TEvSnapshotRequestTimeout, item->SnapshotRequestTimedOut);
                hFunc(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult, item->Handle);
            }
        }

        const TChecksumMailboxAliases Aliases;
        TMap<TKey, TActorId> ItemIds;
        THashMap<TActorId, TChecksumItem> Items;
    };

    class TImportChecksumActor final : public TActorBootstrapped<TImportChecksumActor> {
    public:
        void Bootstrap() {
            Become(&TThis::StateWork);
        }

    private:
        void Handle(TEvTableChecksum::TEvPrepareChecksum::TPtr& ev) {
            auto& request = *ev->Get();
            Executor.Prepare(ev->Sender, std::move(request.Item), std::move(request.Owner));
        }

        void Handle(TEvTableChecksum::TEvRunItem::TPtr& ev) {
            auto& request = *ev->Get();
            Executor.Run(ev->Sender, std::move(request.Item), std::move(request.Owner));
        }

        void FinishItem(TEvTableChecksum::TEvFinishItem::TPtr& ev) {
            Executor.Handle(ev);
            NotifyIdle();
        }

        void StopOperation(TEvTableChecksum::TEvStopOperation::TPtr& ev) {
            Executor.Handle(ev);
            if (ev->Cookie) {
                IdleRecipient = ev->Sender;
                IdleCookie = ev->Cookie;
            }
            NotifyIdle();
        }

        void NotifyIdle() {
            if (IdleRecipient && Executor.IsIdle()) {
                Send(std::exchange(IdleRecipient, {}), new TEvTableChecksum::TEvImportChecksumIdle(), 0, IdleCookie);
            }
        }

        void PassAway() override {
            Executor.Stop();
            TActorBootstrapped::PassAway();
        }

        STATEFN(StateWork) {
            if (Executor.Handle(ev)) {
                NotifyIdle();
                return;
            }
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTableChecksum::TEvPrepareChecksum, Handle);
                hFunc(TEvTableChecksum::TEvRunItem, Handle);
                hFunc(TEvTableChecksum::TEvFinishItem, FinishItem);
                hFunc(TEvTableChecksum::TEvStopOperation, StopOperation);
                sFunc(TEvents::TEvPoisonPill, PassAway);
            }
        }

        TActorId IdleRecipient;
        ui64 IdleCookie = 0;
        TChecksumExecutor Executor{ {
            [this] {
                return RegisterAlias();
            },
            [this](TActorId alias) {
                UnregisterAlias(alias);
            },
        } };
    };

    class TTableChecksumFactory final : public ITableChecksumFactory {
    public:
        std::unique_ptr<ITableChecksumJob> StartExport(
            TEvTableChecksum::TItem item, TChecksumMailboxAliases aliases) const override {
            auto executor = std::make_unique<TChecksumExecutor>(std::move(aliases));
            executor->Run(TActivationContext::AsActorContext().SelfID, std::move(item));
            return executor;
        }

        IActor* CreateImportActor() const override {
            return new TImportChecksumActor;
        }
    };

} // namespace

const ITableChecksumFactory& GetTableChecksumFactory() {
    static const TTableChecksumFactory factory;
    return factory;
}

} // namespace NKikimr::NBackup
