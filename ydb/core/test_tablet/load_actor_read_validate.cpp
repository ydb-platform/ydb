#include "load_actor_impl.h"
#include "scheme.h"

namespace NKikimr::NTestShard {

    class TLoadActor::TValidationActor : public TActorBootstrapped<TValidationActor> {
        const NKikimrClient::TTestShardControlRequest::TCmdInitialize Settings;
        const ui64 TabletId;
        const TActorId TabletActorId;
        const ui32 Generation;
        const bool InitialCheck;
        TActorId ParentId;
        std::optional<TString> LastKey;
        std::unordered_map<TString, ::NTestShard::TStateServer::EEntityState> State;
        TString ReadStateCookie;
        bool StateReadComplete = false, KeyValueReadComplete = false, StateValidated = false;
        std::unordered_map<TString, TKeyInfo> KeysBefore;
        std::unordered_map<TString, TKeyInfo> Keys;
        std::deque<TKey*> TransitionInFlight;
        std::unordered_map<ui64, TString> QueriesInFlight;
        ui64 LastCookie = 0;
        std::deque<TString> KeysPending;

        bool IssueReadMode = false; // true - via EvRequest, false - via EvReadRequest
        bool IssueReadRangeMode = false; // true - via EvRequest, false - via EvReadRequest

        ui32 WaitedReadsViaEvResponse = 0;
        ui32 WaitedReadsViaEvReadResponse = 0;
        ui32 WaitedReadRangesViaEvResponse = 0;
        ui32 WaitedReadRangesViaEvReadRangeResponse = 0;

        // read retry logic
        std::unordered_set<TString> KeyReadsWaitingForRetry;
        ui32 RetryCount = 0;
        bool SendRetriesPending = false;

        enum {
            EvRetryKeyReads = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::TEST_SHARD_VALIDATION_ACTOR;
        }

        TValidationActor(TLoadActor& self, bool initialCheck)
            : Settings(self.Settings)
            , TabletId(self.TabletId)
            , TabletActorId(self.TabletActorId)
            , Generation(self.Generation)
            , InitialCheck(initialCheck)
            , KeysBefore(std::exchange(self.Keys, {}))
        {
            // ensure no concurrent operations are running
            Y_ABORT_UNLESS(self.WritesInFlight.empty());
            Y_ABORT_UNLESS(self.PatchesInFlight.empty());
            Y_ABORT_UNLESS(self.DeletesInFlight.empty());
            Y_ABORT_UNLESS(self.TransitionInFlight.empty());
            for (auto& [key, info] : KeysBefore) {
                Y_ABORT_UNLESS(info.ConfirmedState == info.PendingState);
                info.ConfirmedKeyIndex = Max<size_t>();
            }
            self.ConfirmedKeys.clear();
        }

        void Bootstrap(const TActorId& parentId) {
            ParentId = parentId;
            if (Settings.HasStorageServerHost()) {
                Send(MakeStateServerInterfaceActorId(), new TEvStateServerConnect(Settings.GetStorageServerHost(),
                    Settings.GetStorageServerPort()));
            } else {
                StateReadComplete = true;
            }
            IssueNextReadRangeQuery();
            STLOG(PRI_INFO, TEST_SHARD, TS07, "starting read&validate", (TabletId, TabletId));
            Become(&TThis::StateFunc);
        }

        void PassAway() override {
            if (Settings.HasStorageServerHost()) {
                Send(MakeStateServerInterfaceActorId(), new TEvStateServerDisconnect);
            }
            TActorBootstrapped::PassAway();
        }

        void Handle(TEvStateServerStatus::TPtr ev) {
            if (ev->Get()->Connected) {
                IssueNextStateServerQuery();
            } else {
                STLOG(PRI_ERROR, TEST_SHARD, TS05, "state server disconnected", (TabletId, TabletId));
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, TabletActorId, SelfId(), nullptr, 0));
                PassAway();
            }
        }

        void IssueNextReadRangeQuery() {
            std::unique_ptr<IEventBase> ev;

            IssueReadRangeMode = !IssueReadRangeMode;
            if (IssueReadRangeMode) {
                auto request = std::make_unique<TEvKeyValue::TEvRequest>();
                request->Record.SetTabletId(TabletId);
                request->Record.SetCookie(0);
                auto *read = request->Record.AddCmdReadRange();
                auto *r = read->MutableRange();
                if (LastKey) {
                    r->SetFrom(*LastKey);
                    r->SetIncludeFrom(false);
                }
                ++WaitedReadRangesViaEvResponse;
                ev = std::move(request);
            } else {
                auto request = std::make_unique<TEvKeyValue::TEvReadRange>();
                request->Record.set_tablet_id(TabletId);
                auto *range = request->Record.mutable_range();
                if (LastKey) {
                    range->set_from_key_exclusive(*LastKey);
                }
                ++WaitedReadRangesViaEvReadRangeResponse;
                ev = std::move(request);
            }

            Send(TabletActorId, ev.release());
        }

        TString PopQueryByCookie(ui64 cookie) {
            auto node = QueriesInFlight.extract(cookie);
            Y_ABORT_UNLESS(node);
            return node.mapped();
        }

        void Handle(TEvKeyValue::TEvResponse::TPtr ev) {
            auto& r = ev->Get()->Record;
            if (r.GetCookie()) {
                WaitedReadsViaEvResponse--;

                if (r.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
                    ProcessReadResult(r.GetCookie(), TStringBuilder() << "Status# " << r.GetStatus()
                            << " ErrorReason# " << r.GetErrorReason(), EReadOutcome::IMMEDIATE_RETRY, {});
                    IssueMoreReads();
                    FinishIfPossible();
                    return;
                }

                Y_ABORT_UNLESS(r.ReadResultSize() == 1);
                const auto& res = r.GetReadResult(0);
                const auto status = static_cast<NKikimrProto::EReplyStatus>(res.GetStatus());

                ProcessReadResult(r.GetCookie(), TStringBuilder() << "Status# " << NKikimrProto::EReplyStatus_Name(status)
                    << " Message# " << res.GetMessage(), status == NKikimrProto::OK ? EReadOutcome::OK :
                    status == NKikimrProto::ERROR ? EReadOutcome::RETRY : EReadOutcome::ERROR, res.GetValue());
            } else {
                WaitedReadRangesViaEvResponse--;
                if (r.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
                    STLOG(PRI_ERROR, TEST_SHARD, TS17, "CmdRangeRead failed", (TabletId, TabletId), (Status, r.GetStatus()),
                        (ErrorReason, r.GetErrorReason()));
                    return IssueNextReadRangeQuery();
                }
                Y_ABORT_UNLESS(r.ReadRangeResultSize() == 1);
                const auto& res = r.GetReadRangeResult(0);
                const auto status = static_cast<NKikimrProto::EReplyStatus>(res.GetStatus());
                Y_VERIFY_S(status == NKikimrProto::OK || status == NKikimrProto::NODATA || status == NKikimrProto::OVERRUN,
                    "TabletId# " << TabletId << " CmdReadRange failed Status# " << NKikimrProto::EReplyStatus_Name(status));

                for (const auto& pair : res.GetPair()) {
                    const TString& key = pair.GetKey();
                    LastKey = key;
                    KeysPending.push_back(key);
                }
                if (res.GetPair().empty()) {
                    STLOG(PRI_INFO, TEST_SHARD, TS11, "finished reading from KeyValue tablet", (TabletId, TabletId));
                    KeyValueReadComplete = true;
                } else {
                    IssueNextReadRangeQuery();
                }
            }

            IssueMoreReads();
            FinishIfPossible();
        }

        void Handle(TEvKeyValue::TEvReadResponse::TPtr ev) {
            WaitedReadsViaEvReadResponse--;

            auto& record = ev->Get()->Record;
            const NKikimrKeyValue::Statuses::ReplyStatus status = record.status();
            const TString message = TStringBuilder()
                << "Status# " << NKikimrKeyValue::Statuses::ReplyStatus_Name(status)
                << (record.msg() ? TStringBuilder() << " Message# " << record.msg() : TString());
            const EReadOutcome outcome =
                status == NKikimrKeyValue::Statuses::RSTATUS_TIMEOUT        ? EReadOutcome::IMMEDIATE_RETRY :
                status == NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR ? EReadOutcome::RETRY           :
                status == NKikimrKeyValue::Statuses::RSTATUS_OK             ? EReadOutcome::OK              :
                                                                              EReadOutcome::ERROR;

            ProcessReadResult(record.cookie(), message, outcome, record.value());

            IssueMoreReads();
            FinishIfPossible();
        }

        enum class EReadOutcome {
            IMMEDIATE_RETRY,
            RETRY,
            OK,
            ERROR,
        };

        void ProcessReadResult(ui64 cookie, const TString& message, EReadOutcome outcome, const TString& value) {
            const TString& key = PopQueryByCookie(cookie);

            if (outcome == EReadOutcome::IMMEDIATE_RETRY) {
                STLOG(PRI_ERROR, TEST_SHARD, TS23, "read immediate retry", (TabletId, TabletId), (Message, message));
                KeysPending.push_back(key);
                return;
            }

            if (outcome == EReadOutcome::RETRY && RetryCount < 32) {
                const bool inserted = KeyReadsWaitingForRetry.insert(key).second;
                Y_ABORT_UNLESS(inserted);
                STLOG(PRI_ERROR, TEST_SHARD, TS24, "read key failed -- going to retry", (TabletId, TabletId),
                    (Key, key), (Message, message));
            } else {
                Y_VERIFY_S(outcome == EReadOutcome::OK, "Message# " << message << " Key# " << key << " Outcome# "
                    << (int)outcome << " RetryCount# " << RetryCount);

                const bool inserted = Keys.try_emplace(key, value.size()).second;
                Y_ABORT_UNLESS(inserted);

                ui64 len, seed, id;
                StringSplitter(key).Split(',').CollectInto(&len, &seed, &id);
                TString data = FastGenDataForLZ4(len, seed);

                Y_VERIFY_S(value == data, "TabletId# " << TabletId << " Key# " << key << " value mismatch"
                    << " value.size# " << value.size());

                STLOG(PRI_DEBUG, TEST_SHARD, TS25, "read key", (TabletId, TabletId), (Key, key));
            }
        }

        void Handle(TEvKeyValue::TEvReadRangeResponse::TPtr ev) {
            WaitedReadRangesViaEvReadRangeResponse--;
            auto& record = ev->Get()->Record;
            const NKikimrKeyValue::Statuses::ReplyStatus status = record.status();

            if (status == NKikimrKeyValue::Statuses::RSTATUS_TIMEOUT) {
                STLOG(PRI_ERROR, TEST_SHARD, TS19, "CmdRangeRead failed", (TabletId, TabletId), (Status, status),
                    (ErrorReason, record.msg()));
                return IssueNextReadRangeQuery();
            }

            Y_VERIFY_S(status == NKikimrKeyValue::Statuses::RSTATUS_OK
                || status == NKikimrKeyValue::Statuses::RSTATUS_OVERRUN,
                "TabletId# " << TabletId << " CmdReadRange failed"
                << " Status# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(status));

            for (const auto& pair : record.pair()) {
                const TString& key = pair.key();
                LastKey = key;
                KeysPending.push_back(key);
            }
            if (!record.pair_size()) {
                STLOG(PRI_INFO, TEST_SHARD, TS20, "finished reading from KeyValue tablet", (TabletId, TabletId));
                KeyValueReadComplete = true;
            } else {
                IssueNextReadRangeQuery();
            }

            IssueMoreReads();
            FinishIfPossible();
        }

        void IssueMoreReads() {
            while (!KeysPending.empty() && QueriesInFlight.size() < 8) {
                IssueRead(KeysPending.front());
                KeysPending.pop_front();
            }
        }

        void IssueRead(const TString& key) {
            const ui64 cookie = ++LastCookie;
            const bool inserted = QueriesInFlight.try_emplace(cookie, key).second;
            Y_ABORT_UNLESS(inserted);

            std::unique_ptr<IEventBase> ev;

            IssueReadMode = !IssueReadMode;
            if (IssueReadMode) {
                auto request = std::make_unique<TEvKeyValue::TEvRequest>();
                request->Record.SetTabletId(TabletId);
                request->Record.SetCookie(cookie);
                auto *cmdRead = request->Record.AddCmdRead();
                cmdRead->SetKey(key);
                ++WaitedReadsViaEvResponse;
                ev = std::move(request);
            } else {
                auto request = std::make_unique<TEvKeyValue::TEvRead>();
                request->Record.set_tablet_id(TabletId);
                request->Record.set_cookie(cookie);
                request->Record.set_key(key);
                ++WaitedReadsViaEvReadResponse;
                ev = std::move(request);
            }

            Send(TabletActorId, ev.release());
        }

        void IssueNextStateServerQuery() {
            auto request = std::make_unique<TEvStateServerRequest>();
            auto& r = request->Record;
            auto *read = r.MutableRead();
            read->SetTabletId(TabletId);
            read->SetGeneration(Generation);
            read->SetCookie(ReadStateCookie);
            Send(MakeStateServerInterfaceActorId(), request.release());
        }

        void Handle(TEvStateServerReadResult::TPtr ev) {
            STLOG(PRI_INFO, TEST_SHARD, TS13, "received TEvStateServerReadResult", (TabletId, TabletId));
            auto& r = ev->Get()->Record;
            switch (r.GetStatus()) {
                case ::NTestShard::TStateServer::OK:
                    break;

                case ::NTestShard::TStateServer::ERROR:
                    Y_FAIL_S("ERROR from StateServer TabletId# " << TabletId);

                case ::NTestShard::TStateServer::RACE:
                    TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, TabletActorId, SelfId(), nullptr, 0));
                    PassAway();
                    return;

                default:
                    Y_ABORT();
            }

            if (!r.ItemsSize()) {
                STLOG(PRI_INFO, TEST_SHARD, TS10, "finished reading from state server", (TabletId, TabletId));
                StateReadComplete = true;
                FinishIfPossible();
            } else {
                for (const auto& item : r.GetItems()) {
                    STLOG(PRI_DEBUG, TEST_SHARD, TS21, "read state", (TabletId, TabletId), (Key, item.GetKey()),
                        (State, item.GetState()));
                    const auto& [it, inserted] = State.try_emplace(item.GetKey(), item.GetState());
                    Y_ABORT_UNLESS(inserted);
                }
                ReadStateCookie = r.GetCookie();
                IssueNextStateServerQuery();
            }
        }

        void FinishIfPossible() {
            if (StateReadComplete && KeyValueReadComplete && QueriesInFlight.empty() && KeysPending.empty() && !SendRetriesPending) {
                Y_VERIFY_S(WaitedReadRangesViaEvResponse + WaitedReadRangesViaEvReadRangeResponse +
                    WaitedReadsViaEvResponse + WaitedReadsViaEvReadResponse == 0,
                    "WaitedReadRangesViaEvResponse# " << WaitedReadRangesViaEvResponse
                    << " WaitedReadsViaEvResponse# " << WaitedReadsViaEvResponse
                    << " WaitedReadsViaEvReadResponse# " << WaitedReadsViaEvReadResponse
                    << " WaitedReadRangesViaEvReadRangeResponse# " << WaitedReadRangesViaEvReadRangeResponse);

                if (!KeyReadsWaitingForRetry.empty()) {
                    SendRetriesPending = true;
                    TActivationContext::Schedule(TDuration::Seconds(10), new IEventHandle(EvRetryKeyReads, 0, SelfId(),
                        {}, nullptr, 0));
                    return;
                }
                if (!StateValidated) {
                    ValidateState();
                    StateValidated = true;
                }
                if (TransitionInFlight.empty()) {
                    STLOG(PRI_INFO, TEST_SHARD, TS08, "finished read&validate", (TabletId, TabletId));
                    for (auto& [key, info] : Keys) {
                        Y_ABORT_UNLESS(info.ConfirmedState == info.PendingState);
                        Y_ABORT_UNLESS(info.ConfirmedState == ::NTestShard::TStateServer::CONFIRMED);
                    }
                    Send(ParentId, new TEvValidationFinished(std::move(Keys), InitialCheck));
                    PassAway();
                }
            }
        }

        void SendRetries() {
            Y_ABORT_UNLESS(SendRetriesPending);
            SendRetriesPending = false;
            ++RetryCount;
            for (TString key : std::exchange(KeyReadsWaitingForRetry, {})) {
                KeysPending.push_back(key);
            }
            IssueMoreReads();
        }

        void ValidateState() {
            const bool emptyState = State.empty();

            // the main idea is to iterate over Keys found in the KV tablet and compare them to KeysBefore unless this is
            // initial check, and State; if the State is empty, then we have to populate the state (state server has its
            // data lost)
            for (auto& [key, info] : Keys) {
                if (emptyState) {
                    // key's state will switch to CONFIRMED eventually and this will happen before this actor terminates
                    RegisterTransition(key, ::NTestShard::TStateServer::ABSENT, ::NTestShard::TStateServer::CONFIRMED);
                } else if (const auto it = State.find(key); it != State.end()) {
                    info.ConfirmedState = info.PendingState = it->second;
                    switch (it->second) {
                        case ::NTestShard::TStateServer::WRITE_PENDING: // confirm written key
                        case ::NTestShard::TStateServer::DELETE_PENDING: // reinstate key previously scheduled for deletion
                            RegisterTransition(key, it->second, ::NTestShard::TStateServer::CONFIRMED);
                            break;

                        case ::NTestShard::TStateServer::CONFIRMED: // do nothing -- key is in exact state
                            break;

                        default:
                            Y_ABORT("unexpected key state in State dict");
                    }
                    State.erase(it);
                } else {
                    Y_FAIL_S("extra Key# " << key << " not in State dict TabletId# " << TabletId);
                }

                // if this is not initial check, then validate state against last known state
                if (const auto it = KeysBefore.find(key); it != KeysBefore.end()) {
                    KeysBefore.erase(it);
                } else if (!InitialCheck) {
                    Y_FAIL_S("excessive Key# " << key << " emerged on validation process TabletId# " << TabletId);
                }
            }

            // traverse State to find keys not enlisted in Keys set
            for (const auto& [key, state] : State) {
                if (state == ::NTestShard::TStateServer::CONFIRMED) {
                    Y_FAIL_S("Key# " << key << " is listed in State, but not found in KV tablet -- key is lost TabletId# "
                        << TabletId);
                }
            }

            // scan KeysBefore to find missing keys
            for (const auto& [key, info] : KeysBefore) {
                if (info.ConfirmedState == ::NTestShard::TStateServer::CONFIRMED) {
                    Y_FAIL_S("Key# " << key << " is listed in KeysBefore, but not found in KV tablet -- key is lost"
                        " TabletId# " << TabletId);
                } else { // there should be no in flight requests while doing validation
                    Y_FAIL_S("Key# " << key << " is in incorrect ConfirmedState# "
                        << ::NTestShard::TStateServer::EEntityState_Name(info.ConfirmedState) << " in KeysBefore"
                        " TabletId# " << TabletId);
                }
            }
        }

        void RegisterTransition(TString key, ::NTestShard::TStateServer::EEntityState from, ::NTestShard::TStateServer::EEntityState to) {
            const auto it = Keys.find(key);
            Y_ABORT_UNLESS(it != Keys.end());
            it->second.PendingState = to;

            if (!Settings.HasStorageServerHost()) {
                it->second.ConfirmedState = to;
                return;
            }

            auto request = std::make_unique<TEvStateServerRequest>();
            auto& r = request->Record;
            auto *write = r.MutableWrite();
            write->SetTabletId(TabletId);
            write->SetGeneration(Generation);
            write->SetKey(key);
            write->SetOriginState(from);
            write->SetTargetState(to);
            Send(MakeStateServerInterfaceActorId(), request.release());

            TransitionInFlight.push_back(&*it);
        }

        void Handle(TEvStateServerWriteResult::TPtr ev) {
            // check response
            auto& r = ev->Get()->Record;
            switch (r.GetStatus()) {
                case ::NTestShard::TStateServer::OK:
                    break;

                case ::NTestShard::TStateServer::ERROR:
                    Y_FAIL_S("ERROR from StateServer TabletId# " << TabletId);

                case ::NTestShard::TStateServer::RACE:
                    TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, TabletActorId, SelfId(), nullptr, 0));
                    PassAway();
                    return;

                default:
                    Y_ABORT();
            }

            Y_ABORT_UNLESS(!TransitionInFlight.empty());
            auto& key = *TransitionInFlight.front();
            TransitionInFlight.pop_front();
            key.second.ConfirmedState = key.second.PendingState;

            if (TransitionInFlight.empty()) {
                FinishIfPossible();
            }
        }

        TString RenderHtml() {
            TStringStream str;
            HTML(str) {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        str << "Validation Actor";
                    }
                    DIV_CLASS("panel-body") {
                        TABLE_CLASS("table table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { str << "Parameter"; }
                                    TABLEH() { str << "Value"; }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { str << "StateReadComplete"; }
                                    TABLED() { str << (StateReadComplete ? "true" : "false"); }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { str << "KeyValueReadComplete"; }
                                    TABLED() { str << (KeyValueReadComplete ? "true" : "false"); }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { str << "StateValidated"; }
                                    TABLED() { str << (StateValidated ? "true" : "false"); }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { str << "Keys.size"; }
                                    TABLED() { str << Keys.size(); }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { str << "KeysPending.size"; }
                                    TABLED() { str << KeysPending.size(); }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { str << "KeysBefore.size"; }
                                    TABLED() { str << KeysBefore.size(); }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { str << "State.size"; }
                                    TABLED() { str << State.size(); }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { str << "TransitionInFlight.size"; }
                                    TABLED() { str << TransitionInFlight.size(); }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { str << "QueriesInFlight.size"; }
                                    TABLED() { str << QueriesInFlight.size(); }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { str << "WaitedReadsViaEvResponse"; }
                                    TABLED() { str << WaitedReadsViaEvResponse; }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { str << "WaitedReadsViaEvReadResponse"; }
                                    TABLED() { str << WaitedReadsViaEvReadResponse; }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { str << "WaitedReadRangesViaEvResponse"; }
                                    TABLED() { str << WaitedReadRangesViaEvResponse; }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { str << "WaitedReadRangesViaEvReadRangeResponse"; }
                                    TABLED() { str << WaitedReadRangesViaEvReadRangeResponse; }
                                }
                            }
                        }
                    }
                }
            }
            return str.Str();
        }

        void Handle(NMon::TEvRemoteHttpInfo::TPtr ev) {
            Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(RenderHtml()), 0, ev->Cookie);
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvKeyValue::TEvReadResponse, Handle);
            hFunc(TEvKeyValue::TEvReadRangeResponse, Handle);
            hFunc(TEvKeyValue::TEvResponse, Handle);
            hFunc(TEvStateServerStatus, Handle);
            hFunc(TEvStateServerReadResult, Handle);
            hFunc(TEvStateServerWriteResult, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
            hFunc(NMon::TEvRemoteHttpInfo, Handle);
            cFunc(EvRetryKeyReads, SendRetries);
        )
    };

    void TLoadActor::RunValidation(bool initialCheck) {
        Send(TabletActorId, new TTestShard::TEvSwitchMode(TTestShard::EMode::READ_VALIDATE));
        Y_ABORT_UNLESS(!ValidationActorId);
        ValidationActorId = RegisterWithSameMailbox(new TValidationActor(*this, initialCheck));
        ValidationRunningCount++;
    }

    void TLoadActor::Handle(TEvValidationFinished::TPtr ev) {
        Send(TabletActorId, new TTestShard::TEvSwitchMode(TTestShard::EMode::WRITE));
        ValidationActorId = {};
        BytesProcessed = 0;
        ClearKeys();
        Keys = std::move(ev->Get()->Keys);
        BytesOfData = 0;
        for (auto& [key, info] : Keys) {
            BytesOfData += info.Len;
            Y_ABORT_UNLESS(info.ConfirmedKeyIndex == Max<size_t>());
            if (info.ConfirmedState == ::NTestShard::TStateServer::CONFIRMED) {
                info.ConfirmedKeyIndex = ConfirmedKeys.size();
                ConfirmedKeys.push_back(key);
            }
        }
        Action();

        if (Settings.RestartPeriodsSize() && ev->Get()->InitialCheck) {
            TActivationContext::Schedule(GenerateRandomInterval(Settings.GetRestartPeriods()), new IEventHandle(
                TEvents::TSystem::Wakeup, 0, SelfId(), {}, nullptr, 0));
        }
    }

    bool TLoadActor::IssueRead() {
        if (ConfirmedKeys.empty()) {
            return false;
        }

        const size_t index = RandomNumber(ConfirmedKeys.size());
        const TString& key = ConfirmedKeys[index];
        ui64 len, seed, id;
        StringSplitter(key).Split(',').CollectInto(&len, &seed, &id);

        auto request = CreateRequest();
        if (RandomNumber(2u)) {
            request->Record.SetUsePayloadInResponse(true);
        }

        std::vector<std::tuple<ui32, ui32>> items;
        auto addQuery = [&](ui32 offset, ui32 size) {
            auto *cmdRead = request->Record.AddCmdRead();
            cmdRead->SetKey(key);
            cmdRead->SetOffset(offset);
            cmdRead->SetSize(size);
            items.emplace_back(offset, size);
            STLOG(PRI_INFO, TEST_SHARD, TS16, "reading key", (TabletId, TabletId), (Key, key), (Offset, offset), (Size, size));
        };

        if (len) {
            const ui32 temp = RandomNumber(100u);
            if (temp >= 99) {
                const ui32 numQueries = 2 + RandomNumber(1000u);
                for (ui32 i = 0; i < numQueries; ++i) {
                    const ui32 offset = RandomNumber(len);
                    const ui32 size = 1 + RandomNumber(len - offset);
                    addQuery(offset, size);
                }
            } else if (temp >= 98) {
                ui32 offset = 0;
                while (offset < len) {
                    const ui32 size = Min<ui32>(RandomNumber(3073u) + 1024u, len - offset);
                    addQuery(offset, size);
                    offset += size;
                }
            } else {
                const ui32 offset = RandomNumber(len);
                const ui32 size = 1 + RandomNumber(len - offset);
                addQuery(offset, size);
            }
        } else {
            addQuery(0, 0);
        }

        ReadsInFlight.try_emplace(request->Record.GetCookie(), key, TActivationContext::Monotonic(),
            request->Record.GetUsePayloadInResponse(), std::move(items));
        ++KeysBeingRead[key];
        Send(TabletActorId, request.release());

        return true;
    }

    void TLoadActor::ProcessReadResult(ui64 cookie, const NProtoBuf::RepeatedPtrField<NKikimrClient::TKeyValueResponse::TReadResult>& results,
            TEvKeyValue::TEvResponse& event) {
        auto node = ReadsInFlight.extract(cookie);
        if (!node) { // wasn't a read request
            return;
        }

        auto& [key, timestamp, payloadInResponse, items] = node.mapped();
        size_t index = 0;
        bool ok = true;
        ui64 sizeRead = 0;

        auto it = KeysBeingRead.find(key);
        Y_ABORT_UNLESS(it != KeysBeingRead.end() && it->second);
        if (!--it->second) {
            KeysBeingRead.erase(key);
        }

        ui64 len, seed, id;
        StringSplitter(key).Split(',').CollectInto(&len, &seed, &id);
        TString data = FastGenDataForLZ4(len, seed);

        for (const auto& result : results) {
            Y_ABORT_UNLESS(index < items.size());
            auto& [offset, size] = items[index++];

            STLOG(PRI_INFO, TEST_SHARD, TS18, "read key", (TabletId, TabletId), (Key, key), (Offset, offset), (Size, size),
                (Status, NKikimrProto::EReplyStatus_Name(result.GetStatus())));

            Y_ABORT_UNLESS(result.GetStatus() == NKikimrProto::OK || result.GetStatus() == NKikimrProto::ERROR ||
                result.GetStatus() == NKikimrProto::OVERRUN);

            if (result.GetStatus() == NKikimrProto::OK) {
                TRope value;
                if (payloadInResponse) {
                    Y_ABORT_UNLESS(result.GetDataCase() == NKikimrClient::TKeyValueResponse::TReadResult::kPayloadId);
                    value = event.GetPayload(result.GetPayloadId());
                } else {
                    Y_ABORT_UNLESS(result.GetDataCase() == NKikimrClient::TKeyValueResponse::TReadResult::kValue);
                    value = TRope(result.GetValue());
                }

                Y_VERIFY_S((offset < len || !len) && size <= len - offset && value.size() == size &&
                        TContiguousSpan(data).SubSpan(offset, size) == value,
                    "TabletId# " << TabletId << " Key# " << key << " value mismatch"
                    << " value.size# " << value.size() << " offset# " << offset << " size# " << size);

                sizeRead += size;
            } else {
                ok = false;
            }
        }

        if (ok) {
            const TMonotonic now = TActivationContext::Monotonic();
            ReadLatency.Add(TActivationContext::Monotonic(), now - timestamp);
            ReadSpeed.Add(TActivationContext::Now(), sizeRead);
        }
    }

} // NKikimr::NTestShard
