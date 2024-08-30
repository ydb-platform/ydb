#include "queue.h"
#include "queue_backpressure_client.h"
#include "queue_backpressure_server.h"
#include "unisched.h"
#include "common.h"

//#define BSQUEUE_EVENT_COUNTERS 1

namespace NKikimr::NBsQueue {

////////////////////////////////////////////////////////////////////////////
// TVDiskBackpressureClientActor
////////////////////////////////////////////////////////////////////////////
class TVDiskBackpressureClientActor : public TActorBootstrapped<TVDiskBackpressureClientActor> {
    enum {
        EvQueueWatchdog = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvRequestReadiness,
    };

    struct TEvQueueWatchdog : TEventLocal<TEvQueueWatchdog, EvQueueWatchdog> {};

    struct TEvRequestReadiness : TEventLocal<TEvRequestReadiness, EvRequestReadiness> {
        ui64 Cookie;

        TEvRequestReadiness(ui64 cookie)
            : Cookie(cookie)
        {}
    };

    TBSProxyContextPtr BSProxyCtx;
    TString LogPrefix;
    const TString QueueName;
    const ::NMonitoring::TDynamicCounterPtr Counters;
    TBlobStorageQueue Queue;
    TActorId BlobStorageProxy;
    const TVDiskIdShort VDiskIdShort;
    TActorId RemoteVDisk;
    TVDiskID VDiskId;
    ui32 VDiskOrderNumber;
    NKikimrBlobStorage::EVDiskQueueId QueueId;
    const TDuration QueueWatchdogTimeout;
    ui64 CheckReadinessCookie = 1;
    TIntrusivePtr<NBackpressure::TFlowRecord> FlowRecord;
    const ui32 InterconnectChannel;
    TActorId SessionId;
    std::optional<NKikimrBlobStorage::TGroupInfo> RecentGroup;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TBlobStorageGroupType GType;
    TInstant ConnectionFailureTime;

    enum class EState {
        INITIAL,
        CHECK_READINESS_SENT,
        EXPECT_READY_NOTIFY,
        READY
    };
    EState State = EState::INITIAL;

    const char *GetStateName() const {
        switch (State) {
            case EState::INITIAL: return "INITIAL";
            case EState::CHECK_READINESS_SENT: return "CHECK_READINESS_SENT";
            case EState::EXPECT_READY_NOTIFY: return "EXPECT_READY_NOTIFY";
            case EState::READY: return "READY";
        }
        return "<unknown>";
    }

    bool ExtraBlockChecksSupport = false;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_QUEUE_ACTOR;
    }

    TVDiskBackpressureClientActor(const TIntrusivePtr<TBlobStorageGroupInfo>& info, TVDiskIdShort vdiskId,
            NKikimrBlobStorage::EVDiskQueueId queueId,const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            const TBSProxyContextPtr& bspctx, const NBackpressure::TQueueClientId& clientId, const TString& queueName,
            ui32 interconnectChannel, bool /*local*/, TDuration watchdogTimeout,
            TIntrusivePtr<NBackpressure::TFlowRecord> &flowRecord, NMonitoring::TCountableBase::EVisibility visibility,
            bool useActorSystemTime)
        : BSProxyCtx(bspctx)
        , QueueName(queueName)
        , Counters(counters->GetSubgroup("queue", queueName))
        , Queue(Counters, LogPrefix, bspctx, clientId, interconnectChannel,
                (info ? info->Type : TErasureType::ErasureNone), visibility, useActorSystemTime)
        , VDiskIdShort(vdiskId)
        , QueueId(queueId)
        , QueueWatchdogTimeout(watchdogTimeout)
        , FlowRecord(flowRecord)
        , InterconnectChannel(interconnectChannel)
        , Info(info)
        , GType(info->Type)
    {
        Y_ABORT_UNLESS(Info);
    }

    void Bootstrap(const TActorId& parent, const TActorContext& ctx) {
        ApplyGroupInfo(*std::exchange(Info, nullptr));
        QLOG_INFO_S("BSQ01", "starting parent# " << parent);
        InitCounters();
        RegisteredInUniversalScheduler = RegisterActorInUniversalScheduler(SelfId(), FlowRecord, ctx.ExecutorThread.ActorSystem);
        Y_ABORT_UNLESS(!BlobStorageProxy);
        BlobStorageProxy = parent;
        RequestReadiness(nullptr, ctx);
        UpdateRequestTrackingStats(ctx);
        Become(&TThis::StateFuncWrapper);
    }

private:
    void ApplyGroupInfo(const TBlobStorageGroupInfo& info) {
        Y_ABORT_UNLESS(info.Type.GetErasure() == GType.GetErasure());
        VDiskId = info.CreateVDiskID(VDiskIdShort);
        RemoteVDisk = info.GetActorId(VDiskIdShort);
        VDiskOrderNumber = info.GetOrderNumber(VDiskIdShort);
        LogPrefix = Sprintf("[%s TargetVDisk# %s Queue# %s]", SelfId().ToString().data(), VDiskId.ToString().data(), QueueName.data());
        RecentGroup = info.Group;
    }

    ////////////////////////////////////////////////////////////////////////
    // UPDATE SECTOR
    ////////////////////////////////////////////////////////////////////////

    bool IsReady() const {
        return State == EState::READY;
    }

    void Pump(const TActorContext &ctx) {
        // if in 'Running' state, then send messages to VDisk
        if (IsReady()) {
            Queue.SendToVDisk(ctx, RemoteVDisk, VDiskOrderNumber);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // WATCHDOG TIMER SECTOR
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    TInstant WatchdogBarrier = TInstant::Max();
    bool WatchdogTimerScheduled = false;

    void ArmWatchdogTimer(const TActorContext& ctx) {
        const TInstant now = ctx.Now();
        ResetWatchdogTimer(now, true);
        if (!std::exchange(WatchdogTimerScheduled, true)) {
            ctx.Schedule(WatchdogBarrier - now, new TEvQueueWatchdog);
        }
    }

    void ResetWatchdogTimer(TInstant now, bool inverse = false) {
        if ((WatchdogBarrier == TInstant::Max()) == inverse) {
            WatchdogBarrier = now + QueueWatchdogTimeout;
        }
    }

    void DisarmWatchdogTimer() {
        WatchdogBarrier = TInstant::Max();
    }

    void HandleWatchdog(const TActorContext& ctx) {
        Y_ABORT_UNLESS(WatchdogTimerScheduled);
        WatchdogTimerScheduled = false;
        const TInstant now = ctx.Now();
        if (now >= WatchdogBarrier) {
            // The disk is not responsive, because we had no advance in progress since it was scheduled/reset.
            QLOG_CRIT_S("BSQ19", "watchdog timer hit"
                << " InFlightCount# " << Queue.InFlightCount()
                << " StatusRequests.size# " << StatusRequests.size());

            // reset the connection
            ResetConnection(ctx, NKikimrProto::ERROR, "watchdog timer hit", TDuration::Seconds(1));
        } else if (WatchdogBarrier != TInstant::Max()) {
            // re-schedule event as the barrier has moved during past period
            ArmWatchdogTimer(ctx);
        }
    }

    void UpdateWatchdogStatus(const TActorContext& ctx) {
        if (Queue.InFlightCount() || StatusRequests.size()) {
            ArmWatchdogTimer(ctx);
        } else {
            DisarmWatchdogTimer();
        }
    }

    ////////////////////////////////////////////////////////////////////////
    // FORWARD SECTOR
    ////////////////////////////////////////////////////////////////////////


    template <typename P, typename T, typename R>
    void HandleRequest(P &ev, const TActorContext &ctx) {
        const auto& record = ev->Get()->Record;

        TInstant deadline = TInstant::Max();
        if (record.HasMsgQoS() && record.GetMsgQoS().HasDeadlineSeconds()) {
            deadline = TInstant::Seconds(record.GetMsgQoS().GetDeadlineSeconds());
        }

        QLOG_DEBUG_S("BSQ05", "T# " << TypeName<decltype(*ev->Get())>()
            << " deadline# " << deadline.ToString() << " State# " << GetStateName()
            << " cookie# " << ev->Cookie);

        if (IsReady()) {
            Queue.Enqueue(ctx, ev, deadline, RemoteVDisk.NodeId() == SelfId().NodeId());
            Pump(ctx);
            UpdateRequestTrackingStats(ctx);
        } else {
            auto reply = std::make_unique<R>();
            reply->MakeError(NKikimrProto::NOTREADY, "BS_QUEUE is not ready", record);
            ctx.Send(ev->Sender, reply.release(), 0, ev->Cookie);
        }
    }

    ////////////////////////////////////////////////////////////////////////
    // BACKWARD SECTOR
    ////////////////////////////////////////////////////////////////////////
    template<typename T>
    bool CheckReply(T& ev, const TActorContext& ctx) {
        if (ev->InterconnectSession) { // analyze only messages coming through IC
            ui32 expected = -1;
            switch (const ui32 type = ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvVMovedPatchResult:
                case TEvBlobStorage::EvVPatchFoundParts:
                case TEvBlobStorage::EvVPatchXorDiffResult:
                case TEvBlobStorage::EvVPatchResult:
                case TEvBlobStorage::EvVPutResult:
                case TEvBlobStorage::EvVMultiPutResult:
                case TEvBlobStorage::EvVBlockResult:
                case TEvBlobStorage::EvVGetBlockResult:
                case TEvBlobStorage::EvVCollectGarbageResult:
                case TEvBlobStorage::EvVGetBarrierResult:
                    expected = TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG;
                    break;

                case TEvBlobStorage::EvVGetResult:
                case TEvBlobStorage::EvVStatusResult:
                case TEvBlobStorage::EvVAssimilateResult:
                    expected = InterconnectChannel;
                    break;

                default:
                    Y_ABORT("unexpected message type 0x%08" PRIx32, type);
            }

            if (ev->GetChannel() != expected) {
                QLOG_CRIT_S("BSQ40", "CheckReply"
                    << " T# " << TypeName<decltype(*ev->Get())>()
                    << " reply channel mismatch"
                    << " received# " << ev->GetChannel()
                    << " expected# " << expected);
                Y_DEBUG_ABORT_UNLESS(false);
            }
        }

        return ev->InterconnectSession == SessionId;
    }

    template <typename T>
    void HandleResponse(T &ev, const TActorContext &ctx) {
        if (!CheckReply(ev, ctx)) {
            return;
        }
        UpdateRequestTrackingStats(ctx);

        if (!IsReady()) {
            // may be this is message from the previous interconnect iteration -- we can drop it as we do not expect
            // this message
            QLOG_DEBUG_S("BSQ36", "T# " << TypeName<decltype(*ev->Get())>() << " NOT READY");
            return;
        }

        auto *msg = ev->Get();
        using TEv = decltype(*msg);
        const auto& record = msg->Record;
        NKikimrProto::EReplyStatus status;
        ui64 msgId, sequenceId;

        struct TExFatal : yexception {};

        try {
            auto getField = [&](const auto& base, auto hasfn, auto getfn, const char *baseName, const char *fieldName) {
                if (!(base.*hasfn)()) {
                    throw TExFatal() << "missing " << fieldName << " in " << baseName << " T# " << TypeName<TEv>();
                }
                return (base.*getfn)();
            };

#define F(BASE, NAME) getField((BASE), &std::decay_t<decltype(BASE)>::Has##NAME, &std::decay_t<decltype(BASE)>::Get##NAME, #BASE, #NAME)

            // parse the record
            status = F(record, Status);
            const auto& qos = F(record, MsgQoS);
            const auto& qosMsgId = F(qos, MsgId);
            msgId = F(qosMsgId, MsgId);
            sequenceId = F(qosMsgId, SequenceId);

            if (!Queue.Expecting(msgId, sequenceId)) {
                // this is none of expected responses, so we drop it without processing -- this may happen when interconnect
                // fails and after reconnecting we receive reply from the old query, but this query was already replied
                // to sender with ERROR status code upon disconnecting
                QLOG_DEBUG_S("BSQ37", "T# " << TypeName<TEv>() << " unexpected message"
                    << " MsgId# " << msgId
                    << " SequenceId# " << sequenceId);
                return;
            }

            // update cost model if received
            if (qos.HasCostSettings()) {
                Queue.UpdateCostModel(ctx.Now(), qos.GetCostSettings(), GType);
            }
            if (qos.HasWindow()) {
                HandleWindow(ctx, qos.GetWindow());
            }

            switch (status) {
                case NKikimrProto::TRYLATER_TIME: // this reply is never expected
                    throw TExFatal() << "TRYLATER_TIME is never used";

                case NKikimrProto::NOTREADY:
                    return ResetConnection(ctx, NKikimrProto::ERROR, "NOTREADY status in response", TDuration::Zero());

                case NKikimrProto::VDISK_ERROR_STATE:
                    return ResetConnection(ctx, status, "VDISK_ERROR_STATE status in response", TDuration::Seconds(10));

                case NKikimrProto::TRYLATER_SIZE: // watermark overflow
                    ResetWatchdogTimer(ctx.Now());
                    [[fallthrough]];
                case NKikimrProto::TRYLATER: { // unexpected MsgId/SequenceId on the remote end
                    const auto& window = F(qos, Window);

                    ////////////////////////////////////////////////////////////////////////////////////////////////////
                    // check the window status
                    ////////////////////////////////////////////////////////////////////////////////////////////////////
                    const NKikimrBlobStorage::TWindowFeedback::EStatus ws = F(window, Status);

                    const NKikimrBlobStorage::TWindowFeedback::EStatus expected = status == NKikimrProto::TRYLATER
                        ? NKikimrBlobStorage::TWindowFeedback::IncorrectMsgId
                        : NKikimrBlobStorage::TWindowFeedback::HighWatermarkOverflow;

                    if (ws != expected) {
                        throw TExFatal() << "window Status# " << NKikimrBlobStorage::TWindowFeedback_EStatus_Name(ws)
                            << " != expected Status# " << NKikimrBlobStorage::TWindowFeedback_EStatus_Name(expected);
                    }

                    ////////////////////////////////////////////////////////////////////////////////////////////////////
                    // check that the failed message id is correct
                    ////////////////////////////////////////////////////////////////////////////////////////////////////
                    const auto& windowFailedMsgId = F(window, FailedMsgId);
                    const ui64 failedMsgId = F(windowFailedMsgId, MsgId);
                    const ui64 failedSequenceId = F(windowFailedMsgId, SequenceId);
                    if (failedMsgId != msgId || failedSequenceId != sequenceId) {
                        throw TExFatal() << "received msgId# " << msgId << " sequenceId# " << sequenceId
                            << " != failedMsgId# " << failedMsgId << " failedSequenceId# " << failedSequenceId;
                    }

                    ////////////////////////////////////////////////////////////////////////////////////////////////////
                    // extract expected message id
                    ////////////////////////////////////////////////////////////////////////////////////////////////////
                    const auto& windowExpectedMsgId = F(window, ExpectedMsgId);
                    const ui64 expectedMsgId = F(windowExpectedMsgId, MsgId);
                    const ui64 expectedSequenceId = F(windowExpectedMsgId, SequenceId);

                    QLOG_DEBUG_S("BSQ06", "T# " << TypeName<TEv>()
                        << " failed message"
                        << " msgId# " << msgId << " sequenceId# " << sequenceId
                        << " expectedMsgId# " << expectedMsgId << " expectedSequenceId# " << expectedSequenceId
                        << " status# " << NKikimrProto::EReplyStatus_Name(status)
                        << " ws# " << NKikimrBlobStorage::TWindowFeedback_EStatus_Name(ws)
                        << " InFlightCost# " << Queue.GetInFlightCost()
                        << " InFlightCount# " << Queue.InFlightCount()
                        << " ItemsWaiting# " << Queue.GetItemsWaiting()
                        << " BytesWaiting# " << Queue.GetBytesWaiting());

                    switch (ws) {
                        case NKikimrBlobStorage::TWindowFeedback::IncorrectMsgId:
                            ++*Queue.QueueItemsIncorrectMsgId;
                            break;

                        case NKikimrBlobStorage::TWindowFeedback::HighWatermarkOverflow:
                            ++*Queue.QueueItemsWatermarkOverflow;
                            break;

                        default:
                            Y_ABORT();
                    }

                    Queue.Unwind(msgId, sequenceId, expectedMsgId, expectedSequenceId);
                    Pump(ctx);
                    return;
                }

                default:
                    break;
            }
        } catch (const TExFatal& ex) {
            const TString msg = TStringBuilder() << "fatal error: " << ex.what();
            QLOG_CRIT_S("BSQ38", msg);
            Y_DEBUG_ABORT("%s %s", LogPrefix.data(), msg.data());
            ResetConnection(ctx, NKikimrProto::ERROR, msg, TDuration::Zero());
            return;
        }

        // reset watchdog back to default interval -- we've got successful response
        ResetWatchdogTimer(ctx.Now());

        TActorId sender;
        ui64 cookie = 0;
        TDuration processingTime;
        bool isOk = Queue.OnResponse(msgId, sequenceId, ev->Cookie, &sender, &cookie, &processingTime);
        if (isOk) {
            NWilson::TTraceId traceId = std::move(ev->TraceId);
            ctx.Send(sender, ev->Release().Release(), 0, cookie, std::move(traceId));
        }
        QLOG_DEBUG_S("BSQ08", "T# " << TypeName<TEv>()
            << " sequenceId# " << sequenceId << " msgId# " << msgId
            << " status# " << NKikimrProto::EReplyStatus_Name(status)
            << " processingTime# " << processingTime
            << " isOk# " << isOk);

        Pump(ctx);
    }

    ////////////////////////////////////////////////////////////////////////
    // CONTROL SECTOR
    ////////////////////////////////////////////////////////////////////////
    void HandleWindow(const TActorContext& ctx, const NKikimrBlobStorage::TWindowFeedback& window) {
        if (window.HasMaxWindowSize()) {
            const ui64 maxWindowSize = window.GetMaxWindowSize();
            if (Queue.SetMaxWindowSize(maxWindowSize)) {
                QLOG_DEBUG_S("BSQ10", "maxWindowSize# " << maxWindowSize);
            }
        }
    }

    void Handle(TEvBlobStorage::TEvVWindowChange::TPtr &ev, const TActorContext &ctx) {
        QLOG_DEBUG_S("BSQ09", "WindowChange# " << ev->Get()->ToString());
        auto record = ev->Get()->Record;
        if (record.GetDropConnection()) {
            ResetConnection(ctx, NKikimrProto::VDISK_ERROR_STATE, "VDisk disconnected due to error", TDuration::Seconds(10));
        } else if (record.HasWindow()) {
            const auto& window = record.GetWindow();
            HandleWindow(ctx, window);
            ResetWatchdogTimer(ctx.Now());
            Pump(ctx);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // CONNECT SECTOR
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void ResetConnection(const TActorContext& ctx, NKikimrProto::EReplyStatus status, const TString& errorReason,
            TDuration timeout) {
        switch (State) {
            case EState::INITIAL:
            case EState::CHECK_READINESS_SENT:
            case EState::EXPECT_READY_NOTIFY:
                break;

            case EState::READY:
                QLOG_NOTICE_S("BSQ96", "connection lost status# " << NKikimrProto::EReplyStatus_Name(status)
                    << " errorReason# " << errorReason << " timeout# " << timeout);
                ctx.Send(BlobStorageProxy, new TEvProxyQueueState(VDiskId, QueueId, false, false, nullptr));
                Queue.DrainQueue(status, TStringBuilder() << "BS_QUEUE: " << errorReason, ctx);
                DrainStatus(status, ctx);
                DrainAssimilate(status, errorReason, ctx);
                break;
        }
        State = EState::INITIAL;
        ++CheckReadinessCookie;
        if (timeout != TDuration::Zero()) {
            ctx.Schedule(timeout, new TEvRequestReadiness(CheckReadinessCookie));
        } else {
            RequestReadiness(nullptr, ctx);
        }
    }

    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr ev, const TActorContext& ctx) {
        if (ev->Get()->NodeId == RemoteVDisk.NodeId()) {
            Y_ABORT_UNLESS(!SessionId || SessionId == ev->Sender, "SessionId# %s Sender# %s", SessionId.ToString().data(),
                ev->Sender.ToString().data());
            SessionId = ev->Sender;

            if (ConnectionFailureTime) {
                QLOG_INFO_S("BSQ20", "TEvNodeConnected NodeId# " << ev->Get()->NodeId
                    << " ConnectionFailureTime# " << ConnectionFailureTime
                    << " connection was recovered");
                ConnectionFailureTime = TInstant();
            }
        }
    }

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev, const TActorContext &ctx) {
        if (ev->Get()->NodeId == RemoteVDisk.NodeId()) {
            if (!ConnectionFailureTime) {
                ConnectionFailureTime = ctx.Now();
                QLOG_INFO_S("BSQ13", "TEvNodeDisconnected NodeId# " << ev->Get()->NodeId
                    << " ConnectionFailureTime# " << ConnectionFailureTime);
            }

            ResetConnection(ctx, NKikimrProto::ERROR, "node disconnected", TDuration::Seconds(1));
            Y_ABORT_UNLESS(!SessionId || SessionId == ev->Sender);
            SessionId = {};
        }
    }

    // issue TEvVCheckReadiness message to VDisk; we ask for session subscription and delivery tracking, thus there are
    // the following cases:
    // * message reaches original VDisk and the TEvVCheckReadinessResult message is issued
    // * message is not delivered and we receive TEvUndelivered from Interconnect
    // * connection to the node is lost and we get TEvNodeDisconnected
    void RequestReadiness(TEvRequestReadiness::TPtr ev, const TActorContext& ctx) {
        if (ev && ev->Get()->Cookie != CheckReadinessCookie) {
            return; // obsolete scheduled event
        }

        QLOG_INFO_S("BSQ16", "called"
            << " CheckReadinessCookie# " << CheckReadinessCookie
            << " State# " << GetStateName()
            << (ConnectionFailureTime ? " ConnectionFailureTime# " + ConnectionFailureTime.ToString() : ""));

        if (State != EState::INITIAL && State != EState::EXPECT_READY_NOTIFY) {
            return;
        }

        const bool notifyIfNotReady = State == EState::INITIAL; // only in initial state we want to subscribe for ready notifications

        const ui32 flags = IEventHandle::MakeFlags(InterconnectChannel,
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);

        auto checkEv = std::make_unique<TEvBlobStorage::TEvVCheckReadiness>(notifyIfNotReady);
        auto& record = checkEv->Record;
        VDiskIDFromVDiskID(VDiskId, record.MutableVDiskID());
        if (RecentGroup) {
            record.MutableRecentGroup()->CopyFrom(*RecentGroup);
        }
        auto *qos = record.MutableQoS();
        qos->SetExtQueueId(QueueId);
        Queue.GetClientId().Serialize(qos);
        ctx.Send(RemoteVDisk, checkEv.release(), flags, CheckReadinessCookie);
        State = EState::CHECK_READINESS_SENT;

        // after sending TEvVCheckReadiness we can either get:
        // 1. TEvVCheckReadinessResult in case when everything is okay or almost okay
        // 2. TEvNodeDisconnected when something gone wrong
        // 3. TEvUndelivered when message couldn't be delivered
    }

    void HandleCheckReadiness(TEvBlobStorage::TEvVCheckReadinessResult::TPtr& ev, const TActorContext& ctx) {
        QLOG_INFO_S("BSQ17", "TEvVCheckReadinessResult"
            << " Cookie# " << ev->Cookie
            << " CheckReadinessCookie# " << CheckReadinessCookie
            << " State# " << GetStateName()
            << " Status# " << NKikimrProto::EReplyStatus_Name(ev->Get()->Record.GetStatus()));

        if (State != EState::CHECK_READINESS_SENT || ev->Cookie != CheckReadinessCookie) {
            return; // we don't expect this message right now, or this is some race reply
        }

        const auto& record = ev->Get()->Record;
        if (record.GetStatus() != NKikimrProto::NOTREADY) {
            ExtraBlockChecksSupport = record.GetExtraBlockChecksSupport();
            if (record.HasExpectedMsgId()) {
                Queue.SetMessageId(NBackpressure::TMessageId(record.GetExpectedMsgId()));
            }
            if (record.HasCostSettings()) {
                Queue.UpdateCostModel(ctx.Now(), record.GetCostSettings(), GType);
            }
            ctx.Send(BlobStorageProxy, new TEvProxyQueueState(VDiskId, QueueId, true, ExtraBlockChecksSupport,
                Queue.GetCostModel()));
            Queue.OnConnect();
            State = EState::READY;
        } else {
            // we're still not ready, but we're expecting notification; however, target VDisk could be killed and wiped
            // out and we will never get this notification
            ctx.Schedule(TDuration::Seconds(10), new TEvRequestReadiness(CheckReadinessCookie));
            State = EState::EXPECT_READY_NOTIFY;
        }
    }

    void HandleReadyNotify(const TActorContext& ctx) {
        QLOG_INFO_S("BSQ18", "received TEvVReadyNotify"
            << " State# " << GetStateName());

        if (State == EState::EXPECT_READY_NOTIFY) {
            RequestReadiness(nullptr, ctx);
        }
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        if (!ConnectionFailureTime) {
            ConnectionFailureTime = ctx.Now();
        }

        QLOG_INFO_S("BSQ02", "TEvUndelivered"
            << " Sender# " << ev->Sender
            << " RemoteVDisk# " << RemoteVDisk
            << " SourceType# " << ev->Get()->SourceType
            << " Cookie# " << ev->Cookie
            << " CheckReadinessCookie# " << CheckReadinessCookie
            << " State# " << GetStateName()
            << " Reason# " << ev->Get()->Reason
            << " ConnectionFailureTime# " << ConnectionFailureTime);

        if (ev->Sender == RemoteVDisk) {
            ResetConnection(ctx, NKikimrProto::ERROR, "event undelivered", TDuration::Seconds(1));
        }
    }

    ////////////////////////////////////////////////////////////////////////
    // STATUS SECTOR
    ////////////////////////////////////////////////////////////////////////

    struct TStatusRequestItem {
        TActorId Sender;
        ui64 Cookie;
    };

    TDeque<TStatusRequestItem> StatusRequests;
    ui64 StatusRequestCookie = 0; // 0 means there are no pending requests
    ui64 NextStatusRequestCookie = 1;

    void Handle(TEvBlobStorage::TEvVStatus::TPtr& ev, const TActorContext& ctx) {
        if (IsReady()) {
            if (!StatusRequests) {
                StatusRequestCookie = NextStatusRequestCookie++;
                SendStatusRequest(StatusRequestCookie, ctx);
            }
            StatusRequests.emplace_back(TStatusRequestItem{ev->Sender, ev->Cookie});
        } else {
            SendStatusErrorResponse(ev->Sender, ev->Cookie, NKikimrProto::NOTREADY, ctx);
        }
    }

    void SendStatusRequest(ui64 cookie, const TActorContext& ctx) {
        QLOG_DEBUG_S("BSQ33", "RemoteVDisk# " << RemoteVDisk
            << " VDiskId# " << VDiskId
            << " Cookie# " << cookie);
        const ui32 flags = IEventHandle::MakeFlags(InterconnectChannel, IEventHandle::FlagTrackDelivery);
        ctx.Send(RemoteVDisk, new TEvBlobStorage::TEvVStatus(VDiskId), flags, cookie);
    }

    void DrainStatus(NKikimrProto::EReplyStatus status, const TActorContext& ctx) {
        for (auto& request : std::exchange(StatusRequests, {})) {
            SendStatusErrorResponse(request.Sender, request.Cookie, status, ctx);
        }
        StatusRequestCookie = 0;
    }

    void Handle(TEvBlobStorage::TEvVStatusResult::TPtr& ev, const TActorContext& ctx) {
        if (!CheckReply(ev, ctx)) {
            return;
        }
        if (ev->Cookie != StatusRequestCookie) {
            // It's a response for an already replied request, just ignore it
            return;
        }
        for (auto& request : std::exchange(StatusRequests, {})) {
            auto response = std::make_unique<TEvBlobStorage::TEvVStatusResult>();
            response->Record = ev->Get()->Record;
            ctx.Send(request.Sender, response.release(), 0, request.Cookie);
        }
        StatusRequestCookie = 0;

        ResetWatchdogTimer(ctx.Now());
        Pump(ctx);
    }

    void SendStatusErrorResponse(const TActorId& sender, ui64 cookie, NKikimrProto::EReplyStatus status,
            const TActorContext& ctx) {
        QLOG_DEBUG_S("BSQ34", "Status# " << status
            << " VDiskId# " << VDiskId
            << " Cookie# " << cookie);
        auto response = std::make_unique<TEvBlobStorage::TEvVStatusResult>(status, VDiskId, false, false, 0);
        ctx.Send(sender, response.release(), 0, cookie);
    }

    THashMap<ui64, std::pair<TActorId, ui64>> AssimilateRequests;
    ui64 NextAssimilateRequestCookie = 1;

    void Handle(TEvBlobStorage::TEvVAssimilate::TPtr& ev, const TActorContext& ctx) {
        if (IsReady()) {
            const ui64 id = NextAssimilateRequestCookie++;
            ctx.Send(RemoteVDisk, ev->Release().Release(), IEventHandle::MakeFlags(InterconnectChannel,
                IEventHandle::FlagTrackDelivery), id, std::move(ev->TraceId));
            AssimilateRequests.emplace(id, std::make_pair(ev->Sender, ev->Cookie));
        } else {
            ctx.Send(ev->Sender, new TEvBlobStorage::TEvVAssimilateResult(NKikimrProto::NOTREADY, "no connection",
                VDiskId), 0, ev->Cookie);
        }
    }

    void DrainAssimilate(NKikimrProto::EReplyStatus status, TString errorReason, const TActorContext& ctx) {
        for (const auto& [id, ep] : std::exchange(AssimilateRequests, {})) {
            const auto& [sender, cookie] = ep;
            ctx.Send(sender, new TEvBlobStorage::TEvVAssimilateResult(status, errorReason, VDiskId), 0, cookie);
        }
    }

    void Handle(TEvBlobStorage::TEvVAssimilateResult::TPtr& ev, const TActorContext& ctx) {
        if (!CheckReply(ev, ctx)) {
            return;
        }
        if (const auto it = AssimilateRequests.find(ev->Cookie); it != AssimilateRequests.end()) {
            const auto& [sender, cookie] = it->second;
            ctx.Send(sender, ev->Release().Release(), 0, cookie, std::move(ev->TraceId));
            AssimilateRequests.erase(it);
        }
        ResetWatchdogTimer(ctx.Now());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // REQUEST TIME TRACKING AND REPORTING SYSTEM
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////0

    TInstant RequestTimeTrackingUpdateTimestamp;
    static constexpr TDuration RequestTimeTrackingUpdateDuration = TDuration::MilliSeconds(100);
    static constexpr size_t RequestTimeTrackingHistoryItems = 128;
    std::array<TDuration, RequestTimeTrackingHistoryItems> RequestTimeTrackingQ;
    size_t RequestTimeTrackingIndex = 0;
    bool UpdateRequestTrackingStatsScheduled = false;
    TDuration WorstDuration = TDuration::Zero();
    bool RegisteredInUniversalScheduler = false;

    void HandleUpdateRequestTrackingStats(const TActorContext& ctx) {
        Y_ABORT_UNLESS(UpdateRequestTrackingStatsScheduled || RegisteredInUniversalScheduler);
        UpdateRequestTrackingStatsScheduled = false;
        UpdateRequestTrackingStats(ctx);
    }

    void UpdateRequestTrackingStats(const TActorContext& ctx) {
        const TInstant now = ctx.Now();
        if (!RequestTimeTrackingUpdateTimestamp) {
            RequestTimeTrackingUpdateTimestamp = now;
        }

        // set to true whether we should recalculate the predicted delay
        bool doUpdate = false;

        if (now > RequestTimeTrackingUpdateTimestamp) {
            // calculate number of periods we have to advance
            if (const size_t numPeriods = (now - RequestTimeTrackingUpdateTimestamp) / RequestTimeTrackingUpdateDuration) {
                RequestTimeTrackingUpdateTimestamp += RequestTimeTrackingUpdateDuration * numPeriods;
                if (numPeriods < RequestTimeTrackingHistoryItems) {
                    for (size_t k = 0; k < numPeriods; ++k) {
                        ++RequestTimeTrackingIndex;
                        RequestTimeTrackingIndex %= RequestTimeTrackingHistoryItems;
                        auto& m = RequestTimeTrackingQ[RequestTimeTrackingIndex];
                        if (m == WorstDuration) {
                            doUpdate = true; // recalculate the worst duration as we have dropped the maximum one from the queue
                        }
                        m = TDuration::Zero();
                    }
                } else {
                    RequestTimeTrackingQ.fill(TDuration::Zero());
                    WorstDuration = TDuration::Zero();
                }
            }
        }

        // update the worst request time, if any
        auto& m = RequestTimeTrackingQ[RequestTimeTrackingIndex];
        if (const auto time = Queue.GetWorstRequestProcessingTime(); time && *time > m) {
            m = *time;
            WorstDuration = Max(WorstDuration, m);
        }

        if (doUpdate) {
            WorstDuration = *std::max_element(RequestTimeTrackingQ.begin(), RequestTimeTrackingQ.end());
        }

        // set the value
        FlowRecord->SetPredictedDelayNs(WorstDuration.NanoSeconds());

        // set timer if we have job to do
        if (!RegisteredInUniversalScheduler && WorstDuration && !UpdateRequestTrackingStatsScheduled) {
            ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
            UpdateRequestTrackingStatsScheduled = true;
        }
    }

    ////////////////////////////////////////////////////////////////////////
    // STATES SECTOR
    ////////////////////////////////////////////////////////////////////////
    void Die(const TActorContext& ctx) override {
        QLOG_DEBUG_S("BSQ99", "terminating queue actor");
        if (RegisteredInUniversalScheduler) {
            RegisterActorInUniversalScheduler(SelfId(), nullptr, ctx.ExecutorThread.ActorSystem);
        }
        Unsubscribe(RemoteVDisk.NodeId(), ctx);
        return TActor::Die(ctx);
    }

    void Unsubscribe(const ui32 nodeId, const TActorContext& ctx) {
        ctx.Send(ctx.ExecutorThread.ActorSystem->InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe);
        SessionId = {};
    }

    void Handle(TEvRequestProxyQueueState::TPtr &ev, const TActorContext &ctx) {
        bool isConnected = State == EState::READY;
        QLOG_DEBUG_S("BSQ35", "RequestProxyQueueState"
            << " RemoteVDisk# " << RemoteVDisk
            << " VDiskId# " << VDiskId
            << " IsConnected# " << isConnected);
        ctx.Send(ev->Sender, new TEvProxyQueueState(VDiskId, QueueId, isConnected, isConnected && ExtraBlockChecksSupport,
            Queue.GetCostModel()));
    }

#define QueueRequestHFunc(TEvType) \
    case TEvType::EventType: { \
        TEvType::TPtr *x = reinterpret_cast<TEvType::TPtr *>(&ev); \
        HandleRequest<TEvType::TPtr, TEvType, TEvType##Result>(*x, this->ActorContext()); \
        break; \
    }
#define QueueRequestSpecialHFunc(TEvType, TEvResult) \
    case TEvType::EventType: { \
        TEvType::TPtr *x = reinterpret_cast<TEvType::TPtr *>(&ev); \
        HandleRequest<TEvType::TPtr, TEvType, TEvResult>(*x, this->ActorContext()); \
        break; \
    }

    void ChangeGroupInfo(const TBlobStorageGroupInfo& info, const TActorContext& ctx) {
        if (info.GroupGeneration > VDiskId.GroupGeneration) { // ignore any possible races with old generations
            const TActorId prevActorId = RemoteVDisk;
            ApplyGroupInfo(info);
            QLOG_DEBUG_S("BSQ97", "ChangeGroupInfo"
                << " Reconnect# " << (prevActorId != RemoteVDisk ? "true" : "false")
                << " State# " << GetStateName());
            if (prevActorId != RemoteVDisk) {
                Unsubscribe(prevActorId.NodeId(), ctx);
                ResetConnection(ctx, NKikimrProto::RACE, "target disk changed", TDuration::Zero());
            }
        }
    }

    void HandleGenerationChange(TEvVGenerationChange::TPtr& ev, const TActorContext& ctx) {
        QLOG_DEBUG_S("BSQ98", "ev# " << ev->Get()->ToString());
        ChangeGroupInfo(*ev->Get()->NewInfo, ctx);
    }

    void StateFuncWrapper(STFUNC_SIG) {
        StateFunc(ev);
        UpdateWatchdogStatus(this->ActorContext());
    }

#if BSQUEUE_EVENT_COUNTERS

#define DEFINE_EVENTS(XX) \
    XX(TEvBlobStorage::EvVMovedPatch, EvVMovedPatch) \
    XX(TEvBlobStorage::EvVPut, EvVPut) \
    XX(TEvBlobStorage::EvVMultiPut, EvVMultiPut) \
    XX(TEvBlobStorage::EvVGet, EvVGet) \
    XX(TEvBlobStorage::EvVBlock, EvVBlock) \
    XX(TEvBlobStorage::EvVGetBlock, EvVGetBlock) \
    XX(TEvBlobStorage::EvVCollectGarbage, EvVCollectGarbage) \
    XX(TEvBlobStorage::EvVGetBarrier, EvVGetBarrier) \
    XX(TEvBlobStorage::EvVMovedPatchResult, EvVMovedPatchResult) \
    XX(TEvBlobStorage::EvVPutResult, EvVPutResult) \
    XX(TEvBlobStorage::EvVMultiPutResult, EvVMultiPutResult) \
    XX(TEvBlobStorage::EvVGetResult, EvVGetResult) \
    XX(TEvBlobStorage::EvVBlockResult, EvVBlockResult) \
    XX(TEvBlobStorage::EvVGetBlockResult, EvVGetBlockResult) \
    XX(TEvBlobStorage::EvVCollectGarbageResult, EvVCollectGarbageResult) \
    XX(TEvBlobStorage::EvVGetBarrierResult, EvVGetBarrierResult) \
    XX(EvRequestReadiness, EvRequestReadiness) \
    XX(TEvBlobStorage::EvVCheckReadinessResult, EvVCheckReadinessResult) \
    XX(TEvBlobStorage::EvVReadyNotify, EvVReadyNotify) \
    XX(TEvBlobStorage::EvVStatus, EvVStatus) \
    XX(TEvBlobStorage::EvVStatusResult, EvVStatusResult) \
    XX(TEvBlobStorage::EvVAssimilate, EvVAssimilate) \
    XX(TEvBlobStorage::EvVAssimilateResult, EvVAssimilateResult) \
    XX(TEvBlobStorage::EvRequestProxyQueueState, EvRequestProxyQueueState) \
    XX(TEvBlobStorage::EvVWindowChange, EvVWindowChange) \
    XX(TEvInterconnect::EvNodeConnected, EvNodeConnected) \
    XX(TEvInterconnect::EvNodeDisconnected, EvNodeDisconnected) \
    XX(TEvents::TSystem::Undelivered, Undelivered) \
    XX(EvQueueWatchdog, EvQueueWatchdog) \
    XX(TEvents::TSystem::Wakeup, Wakeup) \
    XX(TEvBlobStorage::EvVGenerationChange, EvVGenerationChange) \
    XX(TEvents::TSystem::Poison, Poison) \
    // END

#define XX(EVENT, NAME) ::NMonitoring::TDynamicCounters::TCounterPtr EventCounter##NAME;
    DEFINE_EVENTS(XX)
#undef XX

    void InitCounters() {
        auto group = Counters->GetSubgroup("subsystem", "events");
#define XX(EVENT, NAME) EventCounter##NAME = group->GetCounter(#NAME, true);
        DEFINE_EVENTS(XX)
#undef XX
    }

#else

    void InitCounters()
    {}

#endif

    STFUNC(StateFunc) {
        const ui32 type = ev->GetTypeRewrite();
#if BSQUEUE_EVENT_COUNTERS
        switch (type) {
#define XX(EVENT, NAME) case EVENT: ++*EventCounter##NAME; break;
            DEFINE_EVENTS(XX)
#undef XX
            default:
                Y_ABORT("unexpected event Type# 0x%08" PRIx32, type);
        }
#endif
        switch (type) {
            QueueRequestHFunc(TEvBlobStorage::TEvVMovedPatch)
            QueueRequestSpecialHFunc(TEvBlobStorage::TEvVPatchStart, TEvBlobStorage::TEvVPatchFoundParts)
            QueueRequestSpecialHFunc(TEvBlobStorage::TEvVPatchDiff, TEvBlobStorage::TEvVPatchResult)
            QueueRequestHFunc(TEvBlobStorage::TEvVPatchXorDiff)
            QueueRequestHFunc(TEvBlobStorage::TEvVPut)
            QueueRequestHFunc(TEvBlobStorage::TEvVMultiPut)
            QueueRequestHFunc(TEvBlobStorage::TEvVGet)
            QueueRequestHFunc(TEvBlobStorage::TEvVBlock)
            QueueRequestHFunc(TEvBlobStorage::TEvVGetBlock)
            QueueRequestHFunc(TEvBlobStorage::TEvVCollectGarbage)
            QueueRequestHFunc(TEvBlobStorage::TEvVGetBarrier)

            HFunc(TEvBlobStorage::TEvVMovedPatchResult, HandleResponse)
            HFunc(TEvBlobStorage::TEvVPatchFoundParts, HandleResponse)
            HFunc(TEvBlobStorage::TEvVPatchXorDiffResult, HandleResponse)
            HFunc(TEvBlobStorage::TEvVPatchResult, HandleResponse)
            HFunc(TEvBlobStorage::TEvVPutResult, HandleResponse)
            HFunc(TEvBlobStorage::TEvVMultiPutResult, HandleResponse)
            HFunc(TEvBlobStorage::TEvVGetResult, HandleResponse)
            HFunc(TEvBlobStorage::TEvVBlockResult, HandleResponse)
            HFunc(TEvBlobStorage::TEvVGetBlockResult, HandleResponse)
            HFunc(TEvBlobStorage::TEvVCollectGarbageResult, HandleResponse)
            HFunc(TEvBlobStorage::TEvVGetBarrierResult, HandleResponse)

            HFunc(TEvRequestReadiness, RequestReadiness)
            HFunc(TEvBlobStorage::TEvVCheckReadinessResult, HandleCheckReadiness)
            CFunc(TEvBlobStorage::EvVReadyNotify, HandleReadyNotify)

            HFunc(TEvBlobStorage::TEvVStatus, Handle)
            HFunc(TEvBlobStorage::TEvVStatusResult, Handle)
            HFunc(TEvBlobStorage::TEvVAssimilate, Handle)
            HFunc(TEvBlobStorage::TEvVAssimilateResult, Handle)

            HFunc(TEvRequestProxyQueueState, Handle)

            HFunc(TEvBlobStorage::TEvVWindowChange, Handle)

            HFunc(TEvInterconnect::TEvNodeConnected, HandleConnected)
            HFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected)
            HFunc(TEvents::TEvUndelivered, HandleUndelivered)

            CFunc(EvQueueWatchdog, HandleWatchdog)
            CFunc(TEvents::TSystem::Wakeup, HandleUpdateRequestTrackingStats)

            HFunc(TEvVGenerationChange, HandleGenerationChange)

            CFunc(NActors::TEvents::TSystem::PoisonPill, Die)

            default:
                Y_ABORT("unexpected event Type# 0x%08" PRIx32, type);
        }
    }
};

} // NKikimr::NBsQueue

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////
// QUEUE CREATOR
////////////////////////////////////////////////////////////////////////////
IActor* CreateVDiskBackpressureClient(const TIntrusivePtr<TBlobStorageGroupInfo>& info, TVDiskIdShort vdiskId,
        NKikimrBlobStorage::EVDiskQueueId queueId,const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
        const TBSProxyContextPtr& bspctx, const NBackpressure::TQueueClientId& clientId, const TString& queueName,
        ui32 interconnectChannel, bool local, TDuration watchdogTimeout,
        TIntrusivePtr<NBackpressure::TFlowRecord> &flowRecord, NMonitoring::TCountableBase::EVisibility visibility,
        bool useActorSystemTime) {
    return new NBsQueue::TVDiskBackpressureClientActor(info, vdiskId, queueId, counters, bspctx, clientId, queueName,
        interconnectChannel, local, watchdogTimeout, flowRecord, visibility, useActorSystemTime);
}

} // NKikimr
