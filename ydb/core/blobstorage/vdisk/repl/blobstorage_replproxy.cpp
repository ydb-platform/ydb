#include "blobstorage_replproxy.h"
#include "blobstorage_replbroker.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/library/actors/core/interconnect.h>
#include <util/generic/fwd.h>
#include <util/generic/queue.h>
#include <util/generic/deque.h>

using namespace NKikimrServices;
using namespace NKikimr::NRepl;

namespace NKikimr {

    namespace NRepl {

        // forward declaration
        IActor *CreateVDiskProxyActor(
                std::shared_ptr<TReplCtx> replCtx,
                TTrackableVector<TVDiskProxy::TScheduledBlob>&& ids,
                const TVDiskID& vdiskId,
                const TActorId& serviceId);

        ////////////////////////////////////////////////////////////////////////////
        // TVDiskProxy
        ////////////////////////////////////////////////////////////////////////////
        TVDiskProxy::TVDiskProxy(std::shared_ptr<TReplCtx> replCtx, const TVDiskID& vdisk, const TActorId& serviceId)
            : ReplCtx(std::move(replCtx))
            , VDiskId(vdisk)
            , ServiceId(serviceId)
            , Ids(TMemoryConsumer(ReplCtx->VCtx->Replication))
            , DataPortion(TMemoryConsumer(ReplCtx->VCtx->Replication))
        {}

        TActorId TVDiskProxy::Run(const TActorId& parentId) {
            Y_DEBUG_ABORT_UNLESS(State == Initial);
            State = RunProxy;
            STLOG(PRI_DEBUG, BS_REPL, BSVR19, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "TVDiskProxy::Run"));
            ParentId = parentId;
            ProxyId = TActivationContext::Register(CreateVDiskProxyActor(ReplCtx, std::move(Ids), VDiskId, ServiceId), ParentId);
            return ProxyId;
        }

        void TVDiskProxy::SendNextRequest() {
            TActorIdentity(ParentId).Send(ProxyId, new TEvReplProxyNext);
        }

        void TVDiskProxy::HandleNext(TEvReplProxyNextResult::TPtr &ev) {
            // transit to Ok state on first message
            if (State == RunProxy) {
                State = Ok;
            }
            Y_ABORT_UNLESS(State == Ok);
            HandlePortion(ev->Get()->Portion);
            Stat = ev->Get()->Stat;
            HasTransientErrors = HasTransientErrors || ev->Get()->HasTransientErrors;
        }

        void TVDiskProxy::HandlePortion(TNextPortion &portion) {
            switch (portion.Status) {
                case TNextPortion::Ok:
                    State = Ok;
                    Y_ABORT_UNLESS(portion.DataPortion.Valid());
                    break;
                case TNextPortion::Eof:
                    State = Eof;
                    break;
                case TNextPortion::Error:
                    State = Error;
                    break;
                default:
                    Y_ABORT("Unexpected value: %d", portion.Status);
            }

            Y_ABORT_UNLESS(!DataPortion.Valid());
            DataPortion = std::move(portion.DataPortion);
        }


        ////////////////////////////////////////////////////////////////////////////
        // Proxy Actor
        // TVDiskProxy uses this actor to communicate with a concrete VDisk.
        // TVDiskProxy sends the following messages:
        //   * TEvReplProxyNext
        //
        // TVDiskProxy receives the following messages:
        //   * TEvReplProxyNextResult
        //
        ////////////////////////////////////////////////////////////////////////////
        class TVDiskProxyActor : public TActorBootstrapped<TVDiskProxyActor> {
            using TQueueItem = std::unique_ptr<TEvBlobStorage::TEvVGetResult>;

            struct TCompare {
                bool operator ()(const TQueueItem& left, const TQueueItem& right) const {
                    return left->Record.GetCookie() > right->Record.GetCookie();
                }
            };

            enum {
                EvProcessDelayedEvent = EventSpaceBegin(TEvents::ES_PRIVATE),
            };
            struct TEvProcessDelayedEvent : TEventLocal<TEvProcessDelayedEvent, EvProcessDelayedEvent> {
                std::unique_ptr<TEvBlobStorage::TEvVGetResult> Event;
                TEvProcessDelayedEvent(std::unique_ptr<TEvBlobStorage::TEvVGetResult> event)
                    : Event(std::move(event))
                {}
            };

            using TResultQueue = TPriorityQueue<TQueueItem, TVector<TQueueItem>, TCompare>;

            std::shared_ptr<TReplCtx> ReplCtx;
            const TBlobStorageGroupType GType;
            TActorId Recipient;
            TTrackableVector<TVDiskProxy::TScheduledBlob> Ids;
            TVDiskID VDiskId;
            TActorId ServiceId;
            TProxyStat Stat;
            ui32 SendIdx;
            ui32 CurPosIdx;
            ui32 RequestsInFlight;
            const ui32 MaxRequestsInFlight;
            TNextPortion Prefetch;
            ui32 PrefetchDataSize;
            bool RequestFromVDiskProxyPending;
            bool Finished;
            bool HasTransientErrors = false;
            ui64 NextSendCookie;
            ui64 NextReceiveCookie;
            TResultQueue ResultQueue;
            std::shared_ptr<TMessageRelevanceTracker> Tracker = std::make_shared<TMessageRelevanceTracker>();

            TQueue<std::unique_ptr<TEvBlobStorage::TEvVGet>> SchedulerRequestQ;
            THashMap<ui64, TReplMemTokenId> RequestTokens;

            friend class TActorBootstrapped<TVDiskProxyActor>;

            void Bootstrap(const TActorId& parentId) {
                STLOG(PRI_DEBUG, BS_REPL, BSVR20, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "TVDiskProxyActor::Bootstrap"));

                // remember parent actor id
                Recipient = parentId;

                // ensure we have LogoBlobs to fetch
                Y_ABORT_UNLESS(!Ids.empty());

                // send initial request
                Become(&TThis::StateFunc);
                ProcessPendingRequests();
            }

            void SendRequest() {
                // query timestamp
                TInstant timestamp = TAppData::TimeProvider->Now();

                // create new VGet request and fill in basic parameters
                TInstant deadline = ReplCtx->VDiskCfg->ReplRequestTimeout.ToDeadLine(timestamp);
                const ui64 getCookie = NextSendCookie++;
                auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskId, deadline,
                        NKikimrBlobStorage::EGetHandleClass::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None,
                        getCookie);
                req->MessageRelevanceTracker = Tracker;

                ui64 maxResponseSize = ReplCtx->VDiskCfg->ReplMaxResponseSize;
                if (const auto& quoter = ReplCtx->VCtx->ReplNodeRequestQuoter) {
                    maxResponseSize = Min(maxResponseSize, quoter->GetMaxPacketSize());
                }

                // prepare a set of extreme queries
                Y_ABORT_UNLESS(SendIdx < Ids.size());
                ui32 numIDsRemain = Min<size_t>(Ids.size() - SendIdx, ReplCtx->VDiskCfg->ReplRequestElements);
                ui32 responseSize = 0;
                ui64 bytes = 0;
                for (ui32 i = 0; i < numIDsRemain; ++i) {
                    const TLogoBlobID& id = Ids[SendIdx].Id;
                    // calculate worst case response data size for this kind of request
                    responseSize += GType.GetExpectedVGetReplyProtobufSize(id);
                    if (responseSize > maxResponseSize && i != 0) {
                        // break only if we have sent at least one request
                        break;
                    }

                    ui64 cookie = SendIdx;
                    req->AddExtremeQuery(id, 0, 0, &cookie);
                    bytes += Ids[SendIdx].ExpectedReplySize;
                    ++SendIdx;
                }

                if (Send(MakeBlobStorageReplBrokerID(), new TEvQueryReplMemToken(bytes), 0, getCookie)) {
                    SchedulerRequestQ.push(std::move(req));
                } else {
                    Send(ServiceId, req.release());
                }
                ++RequestsInFlight;

                // update stats
                Stat.VDiskReqs++;

                STLOG(PRI_DEBUG, BS_REPL, BSVR21, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "TVDiskProxyActor::SendRequest"));
            }

            void Handle(TEvReplMemToken::TPtr& ev) {
                // send scheduled item and remember result token for this request
                Y_ABORT_UNLESS(SchedulerRequestQ);
                auto& item = SchedulerRequestQ.front();
                Send(ServiceId, item.release());
                SchedulerRequestQ.pop();
                RequestTokens.emplace(ev->Cookie, ev->Get()->Token);
            }

            void Handle(TEvReplProxyNext::TPtr& /*ev*/) {
                STLOG(PRI_DEBUG, BS_REPL, BSVR22, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "TVDiskProxyActor::Handle(TEvReplProxyNext)"));

                // increase number of unsatisfied TEvReplProxyNext requests by one more request
                Y_ABORT_UNLESS(!RequestFromVDiskProxyPending);
                RequestFromVDiskProxyPending = true;

                // try to resolve this request via prefetch
                ProcessPendingRequests();
            }

            void ProcessPendingRequests() {
                // if there are unsatisfied requests, try to satisfy them as far as we have data in prefetch
                if (RequestFromVDiskProxyPending && Prefetch.Valid()) {
                    Send(Recipient, new TEvReplProxyNextResult(VDiskId, std::move(Prefetch), Stat, HasTransientErrors));
                    Prefetch.Reset();
                    PrefetchDataSize = 0;
                    RequestFromVDiskProxyPending = false;
                    if (Finished) {
                        Send(MakeBlobStorageReplBrokerID(), new TEvPruneQueue);
                        RequestTokens.clear();
                        return PassAway(); // TODO(alexvru): check correctness of invocations
                    }
                }
                // send request(s) if prefetch queue is not full
                while (!Finished
                        && Prefetch.DataPortion.GetNumItems() < ReplCtx->VDiskCfg->ReplPrefetchElements
                        && PrefetchDataSize < ReplCtx->VDiskCfg->ReplPrefetchDataSize
                        && SendIdx != Ids.size()
                        && RequestsInFlight < MaxRequestsInFlight) {
                    SendRequest();
                }
            }

            void PutResponseQueueItem(TNextPortion&& portion) {
                // we consider ourself finished when last status is either EOF or ERROR; such response must be ultimately last
                Y_ABORT_UNLESS(!Finished);
                Finished = portion.Status != TNextPortion::Ok;

                // update prefetch cumulative data size
                PrefetchDataSize += portion.DataPortion.GetItemsDataTotalSize();

                // update status; the only possible situation is when we change OK status to EOF/ERROR
                Prefetch.Status = portion.Status;

                // put new data after existing one
                Prefetch.AppendDataPortion(std::move(portion.DataPortion));

                // reply to VDiskProxy and make additional requests to VDisk if required
                ProcessPendingRequests();
            }

            void Handle(TEvBlobStorage::TEvVGetResult::TPtr& ev) {
                STLOG(PRI_DEBUG, BS_REPL, BSVR23, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "TVDiskProxyActor::Handle(TEvVGetResult)"),
                    (Msg, ev->Get()->ToString()));

                // update actual memory usage
                const ui64 actualBytes = ev->Get()->GetCachedByteSize();
                const auto& record = ev->Get()->Record;
                if (const auto it = RequestTokens.find(record.GetCookie()); it != RequestTokens.end()) {
                    Send(MakeBlobStorageReplBrokerID(), new TEvUpdateReplMemToken(it->second, actualBytes));
                }

                // limit bandwidth
                const auto& quoter = ReplCtx->VCtx->ReplNodeRequestQuoter;
                const TDuration duration = quoter
                    ? quoter->Take(TActivationContext::Now(), actualBytes)
                    : TDuration::Zero();
                std::unique_ptr<TEvBlobStorage::TEvVGetResult> event(ev->Release().Release());
                if (duration != TDuration::Zero()) {
                    Schedule(duration, new TEvProcessDelayedEvent(std::move(event)));
                } else {
                    ProcessScheduledResult(event);
                }
            }

            void Handle(TEvProcessDelayedEvent::TPtr ev) {
                ProcessScheduledResult(ev->Get()->Event);
            }

            void ProcessScheduledResult(std::unique_ptr<TEvBlobStorage::TEvVGetResult>& ev) {
                ReplCtx->MonGroup.ReplVGetBytesReceived() += ev->GetCachedByteSize();

                // if result came out-of-order, then put it into result queue, otherwise process message and any
                // possible pending messages which came out-of-order before this one
                TEvBlobStorage::TEvVGetResult *msg = ev.get();
                if (msg->Record.GetCookie() == NextReceiveCookie) {
                    ui64 cookie = NextReceiveCookie;
                    ProcessResult(msg);
                    ReleaseMemToken(cookie);
                    while (!ResultQueue.empty()) {
                        const TQueueItem& top = ResultQueue.top();
                        if (top->Record.GetCookie() != NextReceiveCookie) {
                            break;
                        }
                        ui64 cookie = NextReceiveCookie;
                        ProcessResult(top.get());
                        ReleaseMemToken(cookie);
                        ResultQueue.pop();
                    }
                } else {
                    ResultQueue.push(std::move(ev));
                }
            }

            void ReleaseMemToken(ui64 cookie) {
                if (RequestTokens) {
                    auto it = RequestTokens.find(cookie);
                    Y_ABORT_UNLESS(it != RequestTokens.end());
                    Send(MakeBlobStorageReplBrokerID(), new TEvReleaseReplMemToken(it->second));
                    RequestTokens.erase(it);
                }
            }

            void ProcessResult(TEvBlobStorage::TEvVGetResult *msg) {
                const ui64 cookie = msg->Record.GetCookie();
                Y_ABORT_UNLESS(cookie == NextReceiveCookie);
                ++NextReceiveCookie;

                Y_ABORT_UNLESS(RequestsInFlight > 0);
                --RequestsInFlight;

                // ignore any further results if already finished
                if (Finished) {
                    return;
                }

                // process VGetResult status
                TNextPortion portion(TNextPortion::Error, TMemoryConsumer(ReplCtx->VCtx->Replication));
                const NKikimrBlobStorage::TEvVGetResult &rec = msg->Record;
                switch (rec.GetStatus()) {
                    case NKikimrProto::OK:
                        portion.Status = TNextPortion::Ok;
                        ++Stat.VDiskRespOK;
                        break;
                    case NKikimrProto::RACE:
                        ++Stat.VDiskRespRACE;
                        HasTransientErrors = true;
                        break;
                    case NKikimrProto::ERROR:
                    case NKikimrProto::NOTREADY:
                        ++Stat.VDiskRespERROR;
                        HasTransientErrors = true;
                        break;
                    case NKikimrProto::DEADLINE:
                        ++Stat.VDiskRespDEADLINE;
                        break;
                    case NKikimrProto::TRYLATER:
                    case NKikimrProto::TRYLATER_TIME:
                    case NKikimrProto::TRYLATER_SIZE:
                        Y_ABORT("unexpected Status# %s from BS_QUEUE", EReplyStatus_Name(rec.GetStatus()).data());
                    default:
                        ++Stat.VDiskRespOther;
                        STLOG(PRI_DEBUG, BS_REPL, BSVR24, VDISKP(ReplCtx->VCtx->VDiskLogPrefix,
                            "TVDiskProxyActor::Handle(TEvVGetResult)"), (Status, rec.GetStatus()));
                        break;
                }
                if (portion.Status != TNextPortion::Ok) {
                    STLOG(PRI_DEBUG, BS_REPL, BSVR25, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "EvVGet failed"),
                        (Status, rec.GetStatus()));
                    PutResponseQueueItem(std::move(portion));
                } else {
                    // handle Ok status
                    ui32 size = rec.GetResult().size();
                    for (ui32 i = 0; i < size; i++) {
                        const NKikimrBlobStorage::TQueryResult &q = rec.GetResult(i);
                        ui64 cookie = q.GetCookie();

                        Y_ABORT_UNLESS(cookie == CurPosIdx || (CurPosIdx && cookie == CurPosIdx - 1),
                               "i# %" PRIu32 " cookie# %" PRIu64 " CurPosIdx %" PRIu32,
                               i, cookie, CurPosIdx);

                        // ensure we received correctly ordered LogoBlob ID
                        const TLogoBlobID id = LogoBlobIDFromLogoBlobID(q.GetBlobID());
                        const TLogoBlobID genId = Ids[cookie].Id.PartId() ? id : TLogoBlobID(id, 0);
                        Y_ABORT_UNLESS(genId == Ids[cookie].Id);
                        if (CurPosIdx == cookie)
                            ++CurPosIdx;

                        if (q.GetStatus() == NKikimrProto::OK) {
                            Y_DEBUG_ABORT_UNLESS(msg->HasBlob(q));
                            TRope buffer = msg->GetBlobData(q);
                            if (buffer.size() != GType.PartSize(id)) {
                                TString message = VDISKP(ReplCtx->VCtx->VDiskLogPrefix,
                                    "Received incorrect data BlobId# %s Buffer.size# %zu;"
                                    " VDISK CAN NOT REPLICATE A BLOB BECAUSE HAS FOUND INCONSISTENCY IN BLOB SIZE",
                                    id.ToString().data(), buffer.size());
                                STLOG(PRI_CRIT, BS_REPL, BSVR26, message, (BlobId, id), (BufferSize, buffer.size()));
                                Y_DEBUG_ABORT("%s", message.data());

                                // count this blob as erroneous one
                                portion.DataPortion.AddError(id, NKikimrProto::ERROR);
                            } else {
                                Stat.LogoBlobGotIt++;
                                Stat.LogoBlobDataSize += buffer.size();
                                portion.DataPortion.Add(id, std::move(buffer));
                            }
                        } else {
                            portion.DataPortion.AddError(id, q.GetStatus());
                            if (q.GetStatus() == NKikimrProto::OVERRUN) { // FIXME: implement overrun in query exec code
                                Stat.OverflowedMsgs++;
                            } else if (q.GetStatus() == NKikimrProto::NODATA) {
                                Stat.LogoBlobNoData++;
                            } else {
                                Stat.LogoBlobNotOK++;
                            }
                        }
                    }
                    Y_ABORT_UNLESS(CurPosIdx <= Ids.size());
                    if (CurPosIdx == Ids.size())
                        portion.Status = TNextPortion::Eof;

                    // if we haven't received actual data items at all, then send next request
                    if (!portion.DataPortion.Valid() && portion.Status == TNextPortion::Ok)
                        ProcessPendingRequests();
                    else
                        PutResponseQueueItem(std::move(portion));
                }
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvReplProxyNext, Handle)
                hFunc(TEvReplMemToken, Handle)
                hFunc(TEvBlobStorage::TEvVGetResult, Handle)
                cFunc(TEvents::TSystem::Poison, PassAway)
                hFunc(TEvProcessDelayedEvent, Handle)
            )

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_VDISK_REPL_PROXY;
            }

            TVDiskProxyActor(
                    std::shared_ptr<TReplCtx> replCtx,
                    TTrackableVector<TVDiskProxy::TScheduledBlob>&& ids,
                    const TVDiskID& vdiskId,
                    const TActorId& serviceId)
                : TActorBootstrapped<TVDiskProxyActor>()
                , ReplCtx(std::move(replCtx))
                , GType(ReplCtx->VCtx->Top->GType)
                , Ids(std::move(ids))
                , VDiskId(vdiskId)
                , ServiceId(serviceId)
                , Stat()
                , SendIdx(0)
                , CurPosIdx(0)
                , RequestsInFlight(0)
                , MaxRequestsInFlight(3)
                , Prefetch(TNextPortion::Unknown, TMemoryConsumer(ReplCtx->VCtx->Replication))
                , PrefetchDataSize(0)
                , RequestFromVDiskProxyPending(true)
                , Finished(false)
                , NextSendCookie(1)
                , NextReceiveCookie(1)
            {}

            ~TVDiskProxyActor() {}
        };

        IActor *CreateVDiskProxyActor(std::shared_ptr<TReplCtx> replCtx,
                TTrackableVector<TVDiskProxy::TScheduledBlob>&& ids,
                const TVDiskID& vdiskId,
                const TActorId& serviceId) {
            return new TVDiskProxyActor(std::move(replCtx), std::move(ids), vdiskId, serviceId);
        }

    } // NRepl

} // NKikimr
