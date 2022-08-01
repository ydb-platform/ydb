#include "load_actor_impl.h"

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
        TValidationActor(TLoadActor& self, bool initialCheck)
            : Settings(self.Settings)
            , TabletId(self.TabletId)
            , TabletActorId(self.TabletActorId)
            , Generation(self.Generation)
            , InitialCheck(initialCheck)
            , KeysBefore(std::exchange(self.Keys, {}))
        {
            // ensure no concurrent operations are running
            Y_VERIFY(self.WritesInFlight.empty());
            Y_VERIFY(self.DeletesInFlight.empty());
            Y_VERIFY(self.TransitionInFlight.empty());
            for (const auto& [key, info] : KeysBefore) {
                Y_VERIFY(info.ConfirmedState == info.PendingState);
            }
        }

        void Bootstrap(const TActorId& parentId) {
            ParentId = parentId;
            Send(MakeStateServerInterfaceActorId(), new TEvStateServerConnect(Settings.GetStorageServerHost(),
                Settings.GetStorageServerPort()));
            IssueNextReadRangeQuery();
            STLOG(PRI_INFO, TEST_SHARD, TS07, "starting read&validate", (TabletId, TabletId));
            Become(&TThis::StateFunc);
        }

        void PassAway() override {
            Send(MakeStateServerInterfaceActorId(), new TEvStateServerDisconnect);
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

        void SendReadRangeViaEvRequest() {
            auto request = std::make_unique<TEvKeyValue::TEvRequest>();
            auto& record = request->Record;
            record.SetTabletId(TabletId);
            record.SetCookie(0);
            auto *read = record.AddCmdReadRange();
            auto *r = read->MutableRange();
            if (LastKey) {
                r->SetFrom(*LastKey);
                r->SetIncludeFrom(false);
            }
            Send(TabletActorId, request.release());
        }

        void SendReadRangeViaEvReadRange() {
            auto request = std::make_unique<TEvKeyValue::TEvReadRange>();
            auto& record = request->Record;
            record.set_tablet_id(TabletId);
            record.set_cookie(0);
            auto *range = record.mutable_range();
            range->set_from_key_exclusive(*LastKey);
            Send(TabletActorId, request.release());
        }

        void IssueNextReadRangeQuery() {
            IssueReadRangeMode = !IssueReadRangeMode;
            if (IssueReadRangeMode) {
                WaitedReadRangesViaEvResponse++;
                SendReadRangeViaEvRequest();
            } else {
                WaitedReadRangesViaEvReadRangeResponse++;
                SendReadRangeViaEvReadRange();
            }
        }

        TString PopQueryByCookie(ui64 cookie) {
            auto it = QueriesInFlight.find(cookie);
            Y_VERIFY(it != QueriesInFlight.end());
            TString key = std::move(it->second);
            QueriesInFlight.erase(it);
            return key;
        }

        void Handle(TEvKeyValue::TEvResponse::TPtr ev) {
            auto& r = ev->Get()->Record;
            if (r.GetCookie()) {
                WaitedReadsViaEvResponse--;
                TString key = PopQueryByCookie(r.GetCookie());

                if (r.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
                    STLOG(PRI_ERROR, TEST_SHARD, TS18, "CmdRead failed", (TabletId, TabletId), (Status, r.GetStatus()),
                        (ErrorReason, r.GetErrorReason()));
                    return IssueRead(key);
                }

                Y_VERIFY(r.ReadResultSize() == 1);
                const auto& res = r.GetReadResult(0);
                const auto status = static_cast<NKikimrProto::EReplyStatus>(res.GetStatus());

                if (status == NKikimrProto::ERROR && RetryCount < 10) {
                    const bool inserted = KeyReadsWaitingForRetry.insert(key).second;
                    Y_VERIFY(inserted);
                    STLOG(PRI_ERROR, TEST_SHARD, TS22, "read key failed -- going to retry", (TabletId, TabletId), (Key, key));
                    return;
                }

                Y_VERIFY_S(status == NKikimrProto::OK, "Status# " << NKikimrProto::EReplyStatus_Name(status) << " Message# "
                    << res.GetMessage());

                const TString& value = res.GetValue();
                const bool inserted = Keys.try_emplace(key, value.size()).second;
                Y_VERIFY(inserted);
                Y_VERIFY_S(MD5::Calc(value) == key, "TabletId# " << TabletId << " Key# " << key << " digest mismatch"
                    " actual# " << MD5::Calc(value) << " len# " << value.size());
                STLOG(PRI_DEBUG, TEST_SHARD, TS16, "read key", (TabletId, TabletId), (Key, key));
            } else {
                WaitedReadRangesViaEvResponse--;
                if (r.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
                    STLOG(PRI_ERROR, TEST_SHARD, TS17, "CmdRangeRead failed", (TabletId, TabletId), (Status, r.GetStatus()),
                        (ErrorReason, r.GetErrorReason()));
                    return IssueNextReadRangeQuery();
                }
                Y_VERIFY(r.ReadRangeResultSize() == 1);
                const auto& res = r.GetReadRangeResult(0);
                const auto status = static_cast<NKikimrProto::EReplyStatus>(res.GetStatus());
                Y_VERIFY_S(status == NKikimrProto::OK || status == NKikimrProto::NODATA || status == NKikimrProto::OVERRUN,
                    "TabletId# " << TabletId << " CmdReadRange failed Status# " << NKikimrProto::EReplyStatus_Name(status));

                for (const auto& pair : res.GetPair()) {
                    const TString& key = pair.GetKey();
                    LastKey = key;
                    IssueRead(key);
                }
                if (res.GetPair().empty()) {
                    STLOG(PRI_INFO, TEST_SHARD, TS11, "finished reading from KeyValue tablet", (TabletId, TabletId));
                    KeyValueReadComplete = true;
                } else {
                    IssueNextReadRangeQuery();
                }
            }
            FinishIfPossible();
        }

        void Handle(TEvKeyValue::TEvReadResponse::TPtr ev) {
            auto& record = ev->Get()->Record;
            WaitedReadsViaEvReadResponse--;

            const TString key = PopQueryByCookie(record.cookie());
            const NKikimrKeyValue::Statuses::ReplyStatus status = record.status();

            if (status == NKikimrKeyValue::Statuses::RSTATUS_TIMEOUT) {
                STLOG(PRI_ERROR, TEST_SHARD, TS23, "CmdRead failed", (TabletId, TabletId), (Status, status),
                    (ErrorReason, record.msg()));
                return IssueRead(key);
            }

            if (status == NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR && RetryCount < 10) {
                const bool inserted = KeyReadsWaitingForRetry.insert(key).second;
                Y_VERIFY(inserted);
                STLOG(PRI_ERROR, TEST_SHARD, TS24, "read key failed -- going to retry", (TabletId, TabletId),
                    (Key, key), (ErrorReason, record.msg()));
                return;
            }

            Y_VERIFY_S(status == NKikimrKeyValue::Statuses::RSTATUS_OK,
                "Status# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(status)
                << " Cookie# " << record.cookie()
                << " Message# " << record.msg());

            const TString& value = record.value();
            const bool inserted = Keys.try_emplace(key, value.size()).second;
            Y_VERIFY(inserted);
            Y_VERIFY_S(MD5::Calc(value) == key, "TabletId# " << TabletId << " Key# " << key << " digest mismatch"
                " actual# " << MD5::Calc(value) << " len# " << value.size());
            STLOG(PRI_DEBUG, TEST_SHARD, TS25, "read key", (TabletId, TabletId), (Key, key));
            FinishIfPossible();
        }

        void Handle(TEvKeyValue::TEvReadRangeResponse::TPtr ev) {
            WaitedReadRangesViaEvReadRangeResponse--;
            auto& record = ev->Get()->Record;
            const NKikimrKeyValue::Statuses::ReplyStatus status = record.status();

            if (status != NKikimrKeyValue::Statuses::RSTATUS_TIMEOUT) {
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
                IssueRead(key);
            }
            if (!record.pair_size()) {
                STLOG(PRI_INFO, TEST_SHARD, TS20, "finished reading from KeyValue tablet", (TabletId, TabletId));
                KeyValueReadComplete = true;
            } else {
                IssueNextReadRangeQuery();
            }
            FinishIfPossible();
        }

        ui64 SendReadViaEvRequest(const TString& key) {
            auto request = std::make_unique<TEvKeyValue::TEvRequest>();
            auto& record = request->Record;
            record.SetTabletId(TabletId);
            const ui64 cookie = ++LastCookie;
            record.SetCookie(cookie);
            auto *read = record.AddCmdRead();
            read->SetKey(key);
            Send(TabletActorId, request.release());
            return cookie;
        }

        ui64 SendReadViaEvReadRequest(const TString& key) {
            auto request = std::make_unique<TEvKeyValue::TEvRead>();
            auto& record = request->Record;
            record.set_tablet_id(TabletId);
            const ui64 cookie = ++LastCookie;
            record.set_cookie(cookie);
            record.set_key(key);
            Send(TabletActorId, request.release());
            return cookie;
        }

        ui64 SendRead(const TString& key) {
            IssueReadMode = !IssueReadMode;
            if (IssueReadMode) {
                WaitedReadsViaEvResponse++;
                return SendReadViaEvRequest(key);
            } else {
                WaitedReadsViaEvReadResponse++;
                return SendReadViaEvReadRequest(key);
            }
        }

        void IssueRead(const TString& key) {
            const ui64 cookie = SendRead(key);
            const bool inserted = QueriesInFlight.emplace(cookie, key).second;
            Y_VERIFY(inserted);
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
                    Y_FAIL();
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
                    Y_VERIFY(inserted);
                }
                ReadStateCookie = r.GetCookie();
                IssueNextStateServerQuery();
            }
        }

        void FinishIfPossible() {
            if (StateReadComplete && KeyValueReadComplete && QueriesInFlight.empty() && !SendRetriesPending) {
                if (!KeyReadsWaitingForRetry.empty()) {
                    SendRetriesPending = true;
                    TActivationContext::Schedule(TDuration::Seconds(10), new IEventHandle(EvRetryKeyReads, 0, SelfId(), {}, nullptr, 0));
                    return;
                }
                if (!StateValidated) {
                    ValidateState();
                    StateValidated = true;
                }
                if (TransitionInFlight.empty()) {
                    STLOG(PRI_INFO, TEST_SHARD, TS08, "finished read&validate", (TabletId, TabletId));
                    for (auto& [key, info] : Keys) {
                        Y_VERIFY(info.ConfirmedState == info.PendingState);
                        Y_VERIFY(info.ConfirmedState == ::NTestShard::TStateServer::CONFIRMED);
                    }
                    Send(ParentId, new TEvValidationFinished(std::move(Keys)));
                    PassAway();
                }
            }
        }

        void SendRetries() {
            Y_VERIFY(SendRetriesPending);
            SendRetriesPending = false;
            ++RetryCount;
            for (TString key : std::exchange(KeyReadsWaitingForRetry, {})) {
                IssueRead(key);
            }
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
                            Y_FAIL("unexpected key state in State dict");
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
            auto request = std::make_unique<TEvStateServerRequest>();
            auto& r = request->Record;
            auto *write = r.MutableWrite();
            write->SetTabletId(TabletId);
            write->SetGeneration(Generation);
            write->SetKey(key);
            write->SetOriginState(from);
            write->SetTargetState(to);
            Send(MakeStateServerInterfaceActorId(), request.release());

            const auto it = Keys.find(key);
            Y_VERIFY(it != Keys.end());
            it->second.PendingState = to;
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
                    Y_FAIL();
            }

            Y_VERIFY(!TransitionInFlight.empty());
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
        Y_VERIFY(!ValidationActorId);
        ValidationActorId = RegisterWithSameMailbox(new TValidationActor(*this, initialCheck));
        ValidationRunningCount++;
    }

    void TLoadActor::Handle(TEvValidationFinished::TPtr ev) {
        Send(TabletActorId, new TTestShard::TEvSwitchMode(TTestShard::EMode::WRITE));
        ValidationActorId = {};
        BytesProcessed = 0;
        Keys = std::move(ev->Get()->Keys);
        BytesOfData = 0;
        for (const auto& [key, info] : Keys) {
            BytesOfData += info.Len;
        }
        Action();
    }

} // NKikimr::NTestShard
