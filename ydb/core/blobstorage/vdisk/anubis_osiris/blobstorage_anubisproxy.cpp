#include "blobstorage_anubisproxy.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_mon.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/blobstorage/base/utility.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/protos/blobstorage.pb.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TAnubisProxyActor
    // The implementation is very naive, only one request at a time (in fligh),
    // should be enough for our purposes.
    ////////////////////////////////////////////////////////////////////////////
    class TAnubisProxyActor : public TActorBootstrapped<TAnubisProxyActor> {
        TIntrusivePtr<TVDiskContext> VCtx;
        TVDiskIdShort TargetVDiskIdShort;
        TVDiskID TargetVDiskId;
        TVDiskID SelfVDiskId;
        TActiveActors ActiveActors;
        TActorId CliId; // backpressure client id
        std::unique_ptr<IActor> Actor; // backpressure client actor
        TActorId RequestFrom;

        friend class TActorBootstrapped<TAnubisProxyActor>;

        void Bootstrap(const TActorContext &ctx) {
            CliId = ctx.Register(Actor.release());
            ActiveActors.Insert(CliId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            Become(&TThis::StateFunc);
        }

        void Handle(TEvAnubisVGet::TPtr &ev, const TActorContext &ctx) {
            using TEvVGet = TEvBlobStorage::TEvVGet;

            Y_ABORT_UNLESS(RequestFrom == TActorId());
            const auto eclass = NKikimrBlobStorage::EGetHandleClass::AsyncRead;
            auto msg = TEvVGet::CreateExtremeIndexQuery(TargetVDiskId, TInstant::Max(), eclass);
            msg->Record.SetSuppressBarrierCheck(true);

            for (const auto &x : ev->Get()->Candidates) {
                msg->AddExtremeQuery(x, 0, 0, nullptr);
            }

            ctx.Send(CliId, msg.release());
        }

        void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
            Y_ABORT_UNLESS(RequestFrom != TActorId());

            // check for RACE and update status if required
            NKikimrBlobStorage::TEvVGetResult &record = ev->Get()->Record;
            NKikimrProto::EReplyStatus status = record.GetStatus();
            if (status == NKikimrProto::OK && !SelfVDiskId.SameDisk(record.GetVDiskID())) {
                record.SetStatus(NKikimrProto::RACE);
            }
            if (status == NKikimrProto::NOTREADY) {
                record.SetStatus(NKikimrProto::ERROR);
            }

            // reply with result
            ctx.Send(RequestFrom, new TEvAnubisVGetResult(TargetVDiskIdShort, ev));
            RequestFrom = TActorId();
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
                      HFunc(TEvAnubisVGet, Handle)
                      HFunc(TEvBlobStorage::TEvVGetResult, Handle)
                      HFunc(TEvents::TEvPoisonPill, HandlePoison)
                      )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_ANUBIS;
        }

        TAnubisProxyActor(const TIntrusivePtr<TVDiskContext> &vctx,
                          const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo,
                          const TVDiskIdShort &vd,
                          ui32 replInterconnectChannel)
            : TActorBootstrapped<TAnubisProxyActor>()
            , VCtx(vctx)
            , TargetVDiskIdShort(vd)
            , TargetVDiskId(ginfo->GetVDiskId(vd))
            , SelfVDiskId(ginfo->GetVDiskId(VCtx->ShortSelfVDisk))
        {
            using namespace NBackpressure;
            auto queueId = NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead;
            auto clientId = TQueueClientId(NBackpressure::EQueueClientType::ReplJob,
                                           VCtx->Top->GetOrderNumber(VCtx->ShortSelfVDisk));
            auto monGroup = VCtx->VDiskCounters->GetSubgroup("subsystem", "synceranubis");
            TIntrusivePtr<TFlowRecord> flowRecord(new TFlowRecord);
            Actor.reset(CreateVDiskBackpressureClient(ginfo,
                                                      TargetVDiskIdShort,
                                                      queueId,
                                                      monGroup,
                                                      VCtx,
                                                      clientId,
                                                      "Get",
                                                      replInterconnectChannel,
                                                      false,
                                                      TDuration::Minutes(1),
                                                      flowRecord,
                                                      NMonitoring::TCountableBase::EVisibility::Private));
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // CreateAnubisProxy
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateAnubisProxy(const TIntrusivePtr<TVDiskContext> &vctx,
                              const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo,
                              const TVDiskIdShort &vd,
                              ui32 replInterconnectChannel) {
        return new TAnubisProxyActor(vctx, ginfo, vd, replInterconnectChannel);
    }

} // NKikimr
