#include "handoff_mon.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_mon.h>

#include <library/cpp/actors/core/log.h>

using namespace NKikimrServices;
using namespace NKikimr::NHandoff;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // Handoff Mon Request Actor
    ////////////////////////////////////////////////////////////////////////////
    class THandoffMonRequestActor : public TActorBootstrapped<THandoffMonRequestActor> {

        struct TInfo {
            NHandoff::TCountersPtr Cntr;
            NHandoff::TPrivateProxyStatePtr PrState;

            TInfo() = default;
            TInfo(IInputStream &) {
                Y_FAIL("Not supported");
            }
            TInfo(const NHandoff::TCountersPtr &c, const NHandoff::TPrivateProxyStatePtr &p)
                : Cntr(c)
                , PrState(p)
            {}
        };

        typedef NSync::TVDiskNeighbors<TInfo> TCells;

        const TActorId ParentId;
        TProxiesPtr ProxiesPtr;
        NMon::TEvHttpInfo::TPtr Ev;
        TCells Cells;
        unsigned Counter;


        friend class TActorBootstrapped<THandoffMonRequestActor>;

        void Bootstrap(const TActorContext &ctx) {
            // send requests to all proxies
            for (const auto &it : *ProxiesPtr) {
                if (!it.Myself) {
                    ctx.Send(it.Get().ProxyID, new TEvHandoffProxyMon());
                    Counter++;
                }
            }

            if (Counter) {
                // set up timeout, after which we reply
                ctx.Schedule(TDuration::MilliSeconds(700), new TEvents::TEvWakeup());

                // switch state
                Become(&TThis::StateFunc);
            } else {
                Finish(ctx);
            }
        }

        void Handle(TEvHandoffProxyMonResult::TPtr &ev, const TActorContext &ctx) {
            Y_VERIFY_DEBUG(Counter > 0);

            auto d = ev->Get();
            Cells[d->VDiskID].Get() = TInfo(d->CountersPtr, d->PrivateProxyStatePtr);

            --Counter;
            if (Counter == 0)
                Finish(ctx);
        }

        void Finish(const TActorContext &ctx) {
            TStringStream str;
            TPrinter printer;
            Cells.OutputHtml(str, printer, "Handoff", "panel panel-success");
            ctx.Send(Ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::HandoffMonId));
            ctx.Send(ParentId, new TEvents::TEvActorDied);
            Die(ctx);
        }


        void HandleWakeup(const TActorContext &ctx) {
            Finish(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvHandoffProxyMonResult, Handle)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

        // outputs state in HTML
        struct TPrinter {
            void operator() (IOutputStream &str, TCells::TConstIterator it) {
                if (it->Myself) {
                    HTML(str) {
                        PARA_CLASS("text-info") {str << "Self";}
                        SMALL() {str << it->VDiskIdShort.ToString();}
                    }
                } else {
                    it->Get().PrState->OutputHtml(str);
                    it->Get().Cntr->OutputHtml(str);
                }
            }
        };

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HANDOFF_MON_REQUEST;
        }

        THandoffMonRequestActor(const TActorId &parentId,
                                const TVDiskID &selfVDisk,
                                const std::shared_ptr<TBlobStorageGroupInfo::TTopology> &top,
                                TProxiesPtr proxiesPtr,
                                NMon::TEvHttpInfo::TPtr &ev)
            : TActorBootstrapped<THandoffMonRequestActor>()
            , ParentId(parentId)
            , ProxiesPtr(proxiesPtr)
            , Ev(ev)
            , Cells(selfVDisk, top)
            , Counter(0)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // Handoff Mon Actor
    ////////////////////////////////////////////////////////////////////////////
    class THandoffMonActor : public TActor<THandoffMonActor> {
        const TVDiskID SelfVDisk;
        const std::shared_ptr<TBlobStorageGroupInfo::TTopology> Top;
        TProxiesPtr ProxiesPtr;
        TActiveActors ActiveActors;

        void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
            Y_VERIFY_DEBUG(ev->Get()->SubRequestId == TDbMon::HandoffMonId);
            auto actor = std::make_unique<THandoffMonRequestActor>(ctx.SelfID, SelfVDisk, Top, ProxiesPtr, ev);
            auto aid = ctx.Register(actor.release());
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        void Handle(TEvents::TEvActorDied::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            ActiveActors.Erase(ev->Sender);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvents::TEvActorDied, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HANDOFF_MON;
        }

        THandoffMonActor(const TVDiskID &selfVDisk,
                std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                TProxiesPtr proxiesPtr)
            : TActor<THandoffMonActor>(&TThis::StateFunc)
            , SelfVDisk(selfVDisk)
            , Top(top)
            , ProxiesPtr(proxiesPtr)
        {}
    };

    IActor *CreateHandoffMonActor(const TVDiskID &selfVDisk,
            std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
            TProxiesPtr proxiesPtr) {
        return new THandoffMonActor(selfVDisk, top, proxiesPtr);
    }

} // NKikimr

