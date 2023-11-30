#include "skeleton_mon_dbmainpage.h"
#include "skeleton_mon_util.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_dbtype.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/defrag/defrag_actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TMonDbMainPageActor
    ////////////////////////////////////////////////////////////////////////////
    class TMonDbMainPageActor : public TActorBootstrapped<TMonDbMainPageActor> {
        const TVDiskID SelfVDiskId;
        const TActorId NotifyId;
        const TActorId SkeletonFrontId;
        const TActorId SkeletonId;
        NMon::TEvHttpInfo::TPtr Ev;
        TString DbName;
        TString HullCompactionAnswer;
        TString DefragAnswer;

        friend class TActorBootstrapped<TMonDbMainPageActor>;

        TDbMon::ESubRequestID ConvertToSubRequestId(const TString &str) {
            if (str == "LogoBlobs") {
                return TDbMon::DbMainPageLogoBlobs;
            } else if (str == "Blocks") {
                return TDbMon::DbMainPageBlocks;
            } else if (str == "Barriers") {
                return TDbMon::DbMainPageBarriers;
            } else {
                Y_ABORT("Unexpected value: %s", str.data());
            }
        }

        void Redirect(const TActorContext &ctx) {
            // modify params to redirect to the same url but without "&action=compact" param,
            // i.e return to database main page
            auto cgiParamsCopy = Ev->Get()->Request.GetParams();
            cgiParamsCopy.erase("action");

            // create a custom response -- 303 redirect to database main page
            TStringStream response;
            response << "HTTP/1.1 303 See Other\r\n";
            response << "Location: " << Ev->Get()->Request.GetPath() << "?" << cgiParamsCopy.Print() << "\r\n";
            response << "\r\n";

            Finish(ctx, new NMon::TEvHttpInfoRes(response.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            // actor id dead at this moment
        }

        void Bootstrap(const TActorContext &ctx) {
            const TCgiParameters& cgi = Ev->Get()->Request.GetParams();
            const TString &dbname = cgi.Get("dbname");
            const TString &action = cgi.Get("action");

            auto r = NMonUtil::ParseDbName(dbname);
            if (r.Status == NMonUtil::EParseRes::Error || r.Status == NMonUtil::EParseRes::Empty) {
                auto s = Sprintf("Unsupported value '%s' for CGI parameter 'dbname'", r.StrVal.data());
                Finish(ctx, NMonUtil::PrepareError(s));
                return;
            }
            DbName = r.StrVal;
            if (action == "compact") {
                // Send message that starts compaction
                auto dbType = StringToEHullDbType(DbName);
                ctx.Send(SkeletonId, TEvCompactVDisk::Create(dbType));
                Redirect(ctx);
                // actor id dead at this moment
            } else if (action == "defrag" && DbName == "LogoBlobs") {
                // we send local message, so we don't set correct TVDiskID, it's not necessary
                ctx.Send(SkeletonId, new TEvBlobStorage::TEvVDefrag(TVDiskID(), false));
                Redirect(ctx);
                // actor id dead at this moment
            } else {
                // get info about compaction
                ctx.Send(SkeletonId, new NMon::TEvHttpInfo(Ev->Get()->Request, ConvertToSubRequestId(DbName)));
                // get info about defrag
                if (DbName == "LogoBlobs") {
                    ctx.Send(SkeletonId, new NMon::TEvHttpInfo(Ev->Get()->Request, TDbMon::Defrag));
                } else {
                    // not empty result
                    DefragAnswer = " ";
                }
                // set up timeout, after which we reply
                ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
                // switch state
                Become(&TThis::StateFunc);
            }
        }

        void HandleWakeup(const TActorContext &ctx) {
            Finish(ctx);
        }

        void Handle(NMon::TEvHttpInfoRes::TPtr &ev, const TActorContext &ctx) {
            NMon::TEvHttpInfoRes *ptr = dynamic_cast<NMon::TEvHttpInfoRes*>(ev->Get());
            Y_DEBUG_ABORT_UNLESS(ptr);
            if (ptr->SubRequestId == ConvertToSubRequestId(DbName)) {
                HullCompactionAnswer = ptr->Answer;
            } else if (ptr->SubRequestId == TDbMon::Defrag) {
                DefragAnswer = ptr->Answer;
            } else {
                Y_ABORT("unexpected SubRequestId# %d", int(ptr->SubRequestId));
            }

            if (HullCompactionAnswer && DefragAnswer) {
                Finish(ctx);
            }
        }

        void Output(const TString &html, IOutputStream &str, const char *name) {
            if (!html.empty())
                str << html;
            else
                str << "<strong><strong>No info is available for " << name << "</strong></strong><br>";
        }

        void Finish(const TActorContext &ctx) {
            TStringStream str;
            HTML(str) {
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-6") {Output(HullCompactionAnswer, str, "HullCompactionAnswer");}
                    if (DbName == "LogoBlobs") {
                        DIV_CLASS("col-md-6") {Output(DefragAnswer, str, "DefragAnswer");}
                    }
                    // uses column wrapping (sum is greater than 12)
                }
            }

            Finish(ctx, new NMon::TEvHttpInfoRes(str.Str()));
        }

        void Finish(const TActorContext &ctx, IEventBase *ev) {
            ctx.Send(NotifyId, new TEvents::TEvActorDied);
            ctx.Send(Ev->Sender, ev);
            Die(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &, const TActorContext &ctx) {
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(NMon::TEvHttpInfoRes, Handle)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            IgnoreFunc(TEvBlobStorage::TEvVDefragResult)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_MON_SF_LBSTAT;
        }

        TMonDbMainPageActor(
                const TVDiskID &selfVDiskId,
                const TActorId &notifyId,
                const TActorId &skeletonFrontId,
                const TActorId &skeletonId,
                NMon::TEvHttpInfo::TPtr &ev)
            : TActorBootstrapped<TMonDbMainPageActor>()
            , SelfVDiskId(selfVDiskId)
            , NotifyId(notifyId)
            , SkeletonFrontId(skeletonFrontId)
            , SkeletonId(skeletonId)
            , Ev(ev)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // CreateMonDbMainPageActor
    ////////////////////////////////////////////////////////////////////////////////////////////////
    IActor *CreateMonDbMainPageActor(
            const TVDiskID &selfVDiskId,
            const TActorId &notifyId,
            const TActorId &skeletonFrontId,
            const TActorId &skeletonId,
            NMon::TEvHttpInfo::TPtr &ev) {
        return new TMonDbMainPageActor(selfVDiskId, notifyId, skeletonFrontId, skeletonId, ev);
    }

} // NKikimr

