#include "handoff_proxy.h"
#include "handoff_delegate.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

using namespace NKikimrServices;
using namespace NKikimr::NHandoff;

namespace NKikimr {

    // FIXME: Small blobs we should put to the buffer, but for large blobs we can save their id and read it later.
    //        This will allow us to handle spikes gracefully.
    ////////////////////////////////////////////////////////////////////////////
    // Handoff Proxy Actor
    ////////////////////////////////////////////////////////////////////////////
    class THandoffProxyActor : public TActor<THandoffProxyActor> {

        struct TDestroy {
            static void Destroy(TEvLocalHandoff *item) {
                delete item;
            }
        };

        typedef TIntrusiveListWithAutoDelete<TEvLocalHandoff, TDestroy> TListType;


        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        TProxiesPtr ProxiesPtr;
        TVDiskInfo *VDiskInfoPtr;
        NHandoff::TCounters Counters;
        const TString VDiskLogPrefix;
        const THandoffParams Params;
        TListType WaitQueue;
        TListType InFlightQueue;
        TPrivateProxyState State;
        TDuration BadStateTimeouts[TPrivateProxyState::BADNESS_MAX];

        bool WaitQueueIsEmpty() const {
            bool empty = WaitQueue.Empty();
            Y_VERIFY_DEBUG(empty && State.WaitQueueSize == 0 || !empty && State.WaitQueueSize != 0);
            return empty;
        }

        bool WaitQueueIsFull(ui32 size) const {
            if (State.WaitQueueSize == 0)
                return false; // allow single item of any size to be put in empty queue

            return (State.WaitQueueSize + 1 >= Params.MaxWaitQueueSize)
                || (State.WaitQueueByteSize + size >= Params.MaxWaitQueueByteSize);
        }

        bool InFlightQueueIsEmpty() const {
            bool empty = InFlightQueue.Empty();
            Y_VERIFY_DEBUG(empty && State.InFlightQueueSize == 0 || !empty && State.InFlightQueueSize != 0);
            return empty;
        }

        bool InFlightQueueIsFull(ui32 size) const {
            if (State.InFlightQueueSize == 0)
                return false; // allow single item of any size to be put in empty queue

            return (State.InFlightQueueSize + 1 >= Params.MaxInFlightSize)
                || (State.InFlightQueueByteSize + size >= Params.MaxInFlightByteSize);
        }

        void SendItem(const TActorContext &ctx, std::unique_ptr<TEvLocalHandoff> item) {
            auto vd = Info->GetVDiskId(VDiskInfoPtr->OrderNumber);
            auto aid = Info->GetActorId(VDiskInfoPtr->OrderNumber);
            auto msg = std::make_unique<TEvBlobStorage::TEvVPut>(item->Id, TRope(item->Data), vd, true, &item->Cookie, //FIXME(innokentii): RopeFromString
                TInstant::Max(), NKikimrBlobStorage::AsyncBlob);
            State.InFlightQueueSize++;
            State.InFlightQueueByteSize += item->ByteSize();
            InFlightQueue.PushBack(item.release());

            State.LastSendTime = TAppData::TimeProvider->Now();
            ctx.Send(aid, msg.release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
        }

        void DisposeFrontItem() {
            std::unique_ptr<TEvLocalHandoff> item(InFlightQueue.PopFront());
            ui32 byteSize = item->ByteSize();
            State.InFlightQueueSize--;
            State.InFlightQueueByteSize -= byteSize;

            // update global monitor
            VDiskInfoPtr->Get().FreeElement(byteSize);
        }

        void PutToWaitQueue(std::unique_ptr<TEvLocalHandoff> item) {
            State.WaitQueueSize++;
            State.WaitQueueByteSize += item->ByteSize();
            WaitQueue.PushBack(item.release());
        }

        std::unique_ptr<TEvLocalHandoff> GetFromWaitQueue() {
            std::unique_ptr<TEvLocalHandoff> item(WaitQueue.PopFront());
            State.WaitQueueSize--;
            State.WaitQueueByteSize -= item->ByteSize();
            return item;
        }

        void SendQueuedMessagesUntilAllowed(const TActorContext &ctx) {
            // while in bad state send only one message to test availability of other side
            ui32 maxCntr = State.BadnessState == TPrivateProxyState::GOOD ? Max<ui32>() : 1;
            ui32 cntr = 0;
            while (cntr < maxCntr && !WaitQueueIsEmpty() && !InFlightQueueIsFull(WaitQueue.Front()->ByteSize())) {
                SendItem(ctx, GetFromWaitQueue());
                cntr++;
            }
        }

        ui64 GenerateCookie() {
            return State.CookieCounter++;
        }

        void Handle(TEvLocalHandoff::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_HANDOFF,
                      VDISKP(VDiskLogPrefix,
                             "THandoffProxyActor(%s)::Handle(TEvLocalHandoff)",
                             VDiskInfoPtr->VDiskIdShort.ToString().data()));

            std::unique_ptr<TEvLocalHandoff> item(ev->Release().Release());
            ui32 byteSize = item->ByteSize();

            SendQueuedMessagesUntilAllowed(ctx);

            if (!InFlightQueueIsFull(byteSize)) {
                //Y_VERIFY_DEBUG(WaitQueueIsEmpty()); // FIXME: it seems that assert is invalid
                item->Cookie = GenerateCookie();
                SendItem(ctx, std::move(item));
                Counters.LocalHandoffSendRightAway++;
            } else if (!WaitQueueIsFull(byteSize)) {
                item->Cookie = GenerateCookie();
                PutToWaitQueue(std::move(item));
                Counters.LocalHandoffPostpone++;
            } else {
                Counters.LocalHandoffDiscard++;
            }
        }

        void SwitchToBadState(const TActorContext &ctx) {
            // update state
            if (State.BadnessState == TPrivateProxyState::GOOD) {
                LOG_NOTICE(ctx, BS_HANDOFF,
                           VDISKP(VDiskLogPrefix,
                                  "THandoffProxyActor(%s)::SwitchToBadState",
                                  VDiskInfoPtr->VDiskIdShort.ToString().data()));
                Counters.StateGoodToBadTransition++;
            }
            State.BadnessState = TPrivateProxyState::NextBad(State.BadnessState);

            // move all in flight messages to the wait queue for retrying
            while (!InFlightQueueIsEmpty()) {
                std::unique_ptr<TEvLocalHandoff> item(InFlightQueue.PopBack());
                ui32 byteSize = item->ByteSize();
                State.InFlightQueueSize--;
                State.InFlightQueueByteSize -= byteSize;

                WaitQueue.PushFront(item.release());
                State.WaitQueueSize++;
                State.WaitQueueByteSize += byteSize;
            }

            // schedule retry
            if (State.WakeupCounter == 0) {
                State.WakeupCounter++;
                ctx.Schedule(BadStateTimeouts[State.BadnessState], new TEvents::TEvWakeup());
            }
        }

        void SwitchToGoodState(const TActorContext &ctx) {
            Y_UNUSED(ctx);
            if (State.BadnessState != TPrivateProxyState::GOOD) {
                LOG_NOTICE(ctx, BS_HANDOFF,
                           VDISKP(VDiskLogPrefix,
                                  "THandoffProxyActor(%s)::SwitchToGoodState",
                                  VDiskInfoPtr->VDiskIdShort.ToString().data()));
                Counters.StateBadToGoodTransition++;
            }
            State.BadnessState = TPrivateProxyState::GOOD;
        }

        void Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_HANDOFF,
                      VDISKP(VDiskLogPrefix,
                             "THandoffProxyActor(%s)::Handle(TEvVPutResult)",
                             VDiskInfoPtr->VDiskIdShort.ToString().data()));
            const NKikimrBlobStorage::TEvVPutResult &record = ev->Get()->Record;
            const TVDiskID vdisk(VDiskIDFromVDiskID(record.GetVDiskID()));

            if (vdisk != Info->GetVDiskId(VDiskInfoPtr->OrderNumber)) {
                // ignore this message and log the fact
                LOG_ERROR(ctx, BS_HANDOFF,
                          VDISKP(VDiskLogPrefix, "THandoffProxyActor(%s)::Handle(TEvVPutResult): "
                                 "unexpected vdisk; vdisk# %s",
                                 VDiskInfoPtr->VDiskIdShort.ToString().data(), vdisk.ToString().data()));
                Counters.ReplyInvalidVDisk++;
                return;
            }

            if (record.GetStatus() != NKikimrProto::OK) {
                // we got some bad status here, switch to bad state
                LOG_ERROR(ctx, BS_HANDOFF,
                          VDISKP(VDiskLogPrefix,
                                 "THandoffProxyActor(%s)::Handle(TEvVPutResult): "
                                 "bad result status; vdisk# %s msg# %s",
                                 VDiskInfoPtr->VDiskIdShort.ToString().data(), vdisk.ToString().data(),
                                 TEvBlobStorage::TEvVPutResult::ToString(record).data()));
                Counters.ReplyBadStatusResult++;
                SwitchToBadState(ctx);
                return;
            }

            if (record.GetStatus() == NKikimrProto::OK) {
                if (State.InFlightQueueSize == 0) {
                    // in flight queue is empty (we have restarted?)
                    Y_VERIFY_DEBUG(InFlightQueue.Empty());
                    // just ignore this message (update counters and log)
                    LOG_ERROR(ctx, BS_HANDOFF,
                              VDISKP(VDiskLogPrefix,
                                     "THandoffProxyActor(%s)::Handle(TEvVPutResult): "
                                     "orphand ok result; vdisk# %s msg# %s",
                                     VDiskInfoPtr->VDiskIdShort.ToString().data(),
                                     vdisk.ToString().data(),
                                     TEvBlobStorage::TEvVPutResult::ToString(record).data()));
                    Counters.ReplyOrphanOKResult++;
                } else {
                    // check if cookies match
                    ui64 expectedCookie = InFlightQueue.Front()->Cookie;
                    if (expectedCookie == record.GetCookie()) {
                        Counters.ReplyOKResult++;
                        DisposeFrontItem();
                        SwitchToGoodState(ctx);
                        SendQueuedMessagesUntilAllowed(ctx);
                    } else {
                        LOG_ERROR(ctx, BS_HANDOFF,
                                  VDISKP(VDiskLogPrefix,
                                         "THandoffProxyActor(%s)::Handle(TEvVPutResult): "
                                         "cookie mismatch; vdisk# %s expectedCookie# %"
                                         PRIu64 " msg# %s",
                                         VDiskInfoPtr->VDiskIdShort.ToString().data(),
                                         vdisk.ToString().data(), expectedCookie,
                                         TEvBlobStorage::TEvVPutResult::ToString(record).data()));
                        Counters.ReplyCookieMismatch++;
                        SwitchToBadState(ctx);
                    }
                }
                return;
            }

            Y_FAIL("Unexpected case");
        }

        void Handle(TEvents::TEvUndelivered::TPtr& ev, const TActorContext& ctx) {
            if (ev->Get()->SourceType == TEvBlobStorage::EvVPut) {
                // Undelivered
                LOG_DEBUG(ctx, BS_HANDOFF,
                          VDISKP(VDiskLogPrefix,
                                 "THandoffProxyActor(%s)::Handle(TEvUndelivered)",
                                 VDiskInfoPtr->VDiskIdShort.ToString().data()));
                Counters.ReplyUndelivered++;
                SwitchToBadState(ctx);
            } else
                Y_FAIL("Unknown undelivered");
        }

        void HandleWakeup(const TActorContext &ctx) {
            Y_UNUSED(ctx);
            State.WakeupCounter--;

            if (State.BadnessState == TPrivateProxyState::GOOD) {
                // ignore
                Counters.WakeupsAlreadyGood++;
            } else {
                Counters.WakeupsStillBad++;
                SendQueuedMessagesUntilAllowed(ctx);
            }
        }

        void Handle(TEvHandoffProxyMon::TPtr &ev, const TActorContext &ctx) {
            ctx.Send(ev->Sender, new TEvHandoffProxyMonResult(Counters, State,
                                                              Info->GetVDiskId(VDiskInfoPtr->OrderNumber)));
        }

        void Handle(TEvBlobStorage::TEvVWindowChange::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Y_UNUSED(ctx);
            // ignore TEvVWindowChange
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvLocalHandoff, Handle)
            HFunc(TEvBlobStorage::TEvVPutResult, Handle)
            HFunc(TEvents::TEvUndelivered, Handle)
            HFunc(TEvHandoffProxyMon, Handle)
            HFunc(TEvBlobStorage::TEvVWindowChange, Handle)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HANDOFF_PROXY;
        }


        THandoffProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info,
                           TProxiesPtr proxiesPtr,
                           TVDiskInfo *vdInfoPtr,
                           const THandoffParams &params)
            : TActor<THandoffProxyActor>(&TThis::StateFunc)
            , Info(info)
            , ProxiesPtr(proxiesPtr)
            , VDiskInfoPtr(vdInfoPtr)
            , Counters()
            , VDiskLogPrefix(params.VDiskLogPrefix)
            , Params(params)
            , WaitQueue()
            , InFlightQueue()
            , State()
        {
            BadStateTimeouts[0] = TDuration::Seconds(0);
            BadStateTimeouts[1] = TDuration::MilliSeconds(500);
            BadStateTimeouts[2] = TDuration::Seconds(3);
            BadStateTimeouts[3] = TDuration::Seconds(10);
        }
    };

    IActor *CreateHandoffProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info,
                           TProxiesPtr proxiesPtr,
                           TVDiskInfo *vdInfoPtr,
                           const THandoffParams &params) {
        return new THandoffProxyActor(info, proxiesPtr, vdInfoPtr, params);
    }

} // NKikimr

