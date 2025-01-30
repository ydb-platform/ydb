#include "blobstorage_monactors.h"
#include "blobstorage_db.h"
#include "skeleton_mon_dbmainpage.h"
#include "skeleton_mon_util.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TMonErrorActor
    ////////////////////////////////////////////////////////////////////////////
    class TMonErrorActor : public TActorBootstrapped<TMonErrorActor> {
        const TActorId NotifyId;
        NMon::TEvHttpInfo::TPtr Ev;
        const TString Explanation;

        friend class TActorBootstrapped<TMonErrorActor>;

        void Bootstrap(const TActorContext &ctx) {
            ctx.Send(NotifyId, new TEvents::TEvActorDied);
            ctx.Send(Ev->Sender, NMonUtil::PrepareError(Explanation));
            Die(ctx);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_MON_ERROR;
        }

        TMonErrorActor(const TActorId &notifyId,
                       NMon::TEvHttpInfo::TPtr &ev,
                       const TString &explanation)
            : TActorBootstrapped<TMonErrorActor>()
            , NotifyId(notifyId)
            , Ev(ev)
            , Explanation(explanation)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TSkeletonMonMainPageActor
    ////////////////////////////////////////////////////////////////////////////
    class TSkeletonMonMainPageActor : public TActorBootstrapped<TSkeletonMonMainPageActor> {
        TIntrusivePtr<TDb> Db;
        NMon::TEvHttpInfo::TPtr Ev;
        const TActorId NotifyId;
        const TActorId LocalRecovActorID;
        unsigned Counter;

        TString SkeletonState;
        TString HullInfo;
        TString SyncerInfo;
        TString SyncLogInfo;
        TString ReplInfo;
        TString LogCutterInfo;
        TString HugeKeeperInfo;
        TString HandoffInfo;
        TString DskSpaceTrackerInfo;
        TString LocalRecovInfo;
        TString AnubisRunnerInfo;
        TString DelayedCompactionDeleterInfo;
        TString ScrubInfo;

        friend class TActorBootstrapped<TSkeletonMonMainPageActor>;

        void Bootstrap(const TActorContext &ctx) {
            // send requests to all actors
            if (bool(TActorId(Db->SkeletonID))) {
                ctx.Send(Db->SkeletonID, new NMon::TEvHttpInfo(Ev->Get()->Request, TDbMon::SkeletonStateId));
                Counter++;
                ctx.Send(Db->SkeletonID, new NMon::TEvHttpInfo(Ev->Get()->Request, TDbMon::HullInfoId));
                Counter++;
                ctx.Send(Db->SkeletonID, new NMon::TEvHttpInfo(Ev->Get()->Request, TDbMon::DelayedCompactionDeleterId));
                Counter++;
            }

            if (bool(TActorId(Db->SyncerID))) {
                ctx.Send(Db->SyncerID, new NMon::TEvHttpInfo(Ev->Get()->Request, TDbMon::SyncerInfoId));
                Counter++;
            }

            if (bool(TActorId(Db->SyncLogID))) {
                ctx.Send(Db->SyncLogID, new NMon::TEvHttpInfo(Ev->Get()->Request, TDbMon::SyncLogId));
                Counter++;
            }

            if (bool(TActorId(Db->ReplID))) {
                ctx.Send(Db->ReplID, new NMon::TEvHttpInfo(Ev->Get()->Request, TDbMon::ReplId));
                Counter++;
            }

            if (bool(TActorId(Db->LogCutterID))) {
                ctx.Send(Db->LogCutterID, new NMon::TEvHttpInfo(Ev->Get()->Request, TDbMon::LogCutterId));
                Counter++;
            }

            if (bool(TActorId(Db->HugeKeeperID))) {
                ctx.Send(Db->HugeKeeperID, new NMon::TEvHttpInfo(Ev->Get()->Request, TDbMon::HugeKeeperId));
                Counter++;
            }

            if (bool(TActorId(Db->DskSpaceTrackerID))) {
                ctx.Send(Db->DskSpaceTrackerID, new NMon::TEvHttpInfo(Ev->Get()->Request, TDbMon::DskSpaceTrackerId));
                Counter++;
            }

            if (bool(LocalRecovActorID)) {
                ctx.Send(LocalRecovActorID, new NMon::TEvHttpInfo(Ev->Get()->Request, TDbMon::LocalRecovInfoId));
                Counter++;
            }

            if (bool(TActorId(Db->AnubisRunnerID))) {
                ctx.Send(Db->AnubisRunnerID, new NMon::TEvHttpInfo(Ev->Get()->Request, TDbMon::AnubisRunnerId));
                Counter++;
            }

            ctx.Send(Db->SkeletonID, new NMon::TEvHttpInfo(Ev->Get()->Request, TDbMon::ScrubId));
            Counter++;

            if (Counter) {
                // set up timeout, after which we reply
                ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());

                // switch state
                Become(&TThis::StateFunc);
            } else {
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
                    DIV_CLASS("col-md-6") {Db->VCtx->VDiskCounters->OutputHtml(str);}
                    DIV_CLASS("col-md-6") {Output(SkeletonState, str, "Skeleton State");}
                    DIV_CLASS("col-md-6") {Output(LogCutterInfo, str, "Log Cutter");}
                    DIV_CLASS("col-md-6") {Output(HugeKeeperInfo, str, "Huge Blob Keeper");}
                    DIV_CLASS("col-md-6") {Output(DskSpaceTrackerInfo, str, "Disk Space Tracker");}
                    DIV_CLASS("col-md-6") {Output(LocalRecovInfo, str, "Local Recovery Info");}
                    DIV_CLASS("col-md-6") {Output(DelayedCompactionDeleterInfo, str, "Delayed Compaction Deleter Info");}
                    DIV_CLASS("col-md-6") {Output(ScrubInfo, str, "Scrub Info");}
                    // uses column wrapping (sum is greater than 12)
                }
                Output(HullInfo, str, "Hull");
                Output(SyncLogInfo, str, "Sync Log");
                Output(SyncerInfo, str, "Syncer");
                Output(ReplInfo, str, "Repl");
                Output(AnubisRunnerInfo, str, "Anubis");
                Output(HandoffInfo, str, "Handoff");
            }

            ctx.Send(NotifyId, new TEvents::TEvActorDied);
            ctx.Send(Ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
            Die(ctx);
        }


        void HandleWakeup(const TActorContext &ctx) {
            Finish(ctx);
        }

        void Handle(NMon::TEvHttpInfoRes::TPtr &ev, const TActorContext &ctx) {
            Y_DEBUG_ABORT_UNLESS(Counter > 0);
            NMon::TEvHttpInfoRes *ptr = dynamic_cast<NMon::TEvHttpInfoRes*>(ev->Get());
            Y_DEBUG_ABORT_UNLESS(ptr);

            static const std::unordered_map<int, TString TThis::*> names{
                {TDbMon::SkeletonStateId,          &TThis::SkeletonState},
                {TDbMon::HullInfoId,               &TThis::HullInfo},
                {TDbMon::SyncerInfoId,             &TThis::SyncerInfo},
                {TDbMon::SyncLogId,                &TThis::SyncLogInfo},
                {TDbMon::ReplId,                   &TThis::ReplInfo},
                {TDbMon::LogCutterId,              &TThis::LogCutterInfo},
                {TDbMon::HugeKeeperId,             &TThis::HugeKeeperInfo},
                {TDbMon::HandoffMonId,             &TThis::HandoffInfo},
                {TDbMon::DskSpaceTrackerId,        &TThis::DskSpaceTrackerInfo},
                {TDbMon::LocalRecovInfoId,         &TThis::LocalRecovInfo},
                {TDbMon::AnubisRunnerId,           &TThis::AnubisRunnerInfo},
                {TDbMon::DelayedCompactionDeleterId, &TThis::DelayedCompactionDeleterInfo},
                {TDbMon::ScrubId,                  &TThis::ScrubInfo},
            };

            const auto it = names.find(ptr->SubRequestId);
            Y_ABORT_UNLESS(it != names.end());
            this->*it->second = ptr->Answer;
            --Counter;
            if (Counter == 0) {
                Finish(ctx);
            }
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(NMon::TEvHttpInfoRes, Handle)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_MON_MAIN_PAGE;
        }

        TSkeletonMonMainPageActor(TIntrusivePtr<TDb> &db,
                                  NMon::TEvHttpInfo::TPtr &ev,
                                  const TActorId notifyId,
                                  const TActorId &localRecovActorID)
            : TActorBootstrapped<TSkeletonMonMainPageActor>()
            , Db(db)
            , Ev(ev)
            , NotifyId(notifyId)
            , LocalRecovActorID(localRecovActorID)
            , Counter(0)
        {}
    };

    IActor* CreateSkeletonMonRequestHandler(TIntrusivePtr<TDb> &db,
                                            NMon::TEvHttpInfo::TPtr &ev,
                                            const TActorId notifyId,
                                            const TActorId &localRecovActorID) {
        return new TSkeletonMonMainPageActor(db, ev, notifyId, localRecovActorID);
    }

    ////////////////////////////////////////////////////////////////////////////
    // TMonQueryBaseActor
    ////////////////////////////////////////////////////////////////////////////
    class TMonQueryBaseActor {
    protected:
        TString BuildForm(const TString &dbnameParam, const TString &itemName, const TString &placeholder, bool index) {
            TStringStream str;

            HTML(str) {
                FORM_CLASS("form-horizontal") {
                    // hidden forms to pass current cgi params
                    str << "<input type=\"hidden\" name=\"type\" value=\"query\">";
                    str << "<input type=\"hidden\" name=\"dbname\" value=\"" << dbnameParam << "\">";

                    str << "<p>Input key format: " << placeholder << " in decimal</p>";

                    DIV_CLASS("control-group") {
                        LABEL_CLASS_FOR("control-label", "inputFrom") {str << "From (" << itemName << ")";}
                        DIV_CLASS("controls") {
                            str << "<input size=64 type=\"text\" id=\"inputFrom\" "
                            "placeholder=\"" << placeholder << "\" name=\"from\">";
                        }
                    }

                    DIV_CLASS("control-group") {
                        LABEL_CLASS_FOR("control-label", "inputTo") {str << "To (" << itemName << ")";}
                        DIV_CLASS("controls") {
                            str << "<input size=64 type=\"text\" id=\"inputTo\" "
                            "placeholder=\"" << placeholder << "\" name=\"to\">";
                        }
                    }

                    DIV_CLASS("control-group") {
                        DIV_CLASS("controls") {
                            if (index) {
                                LABEL_CLASS("checkbox") {
                                    str << "<input type=\"checkbox\" name=\"IndexOnly\" checked>Index Only</input>";
                                }
                            }
                            LABEL_CLASS("checkbox") {
                                str << "<input type=\"checkbox\" name=\"Internals\" checked>Show Internals</input>";
                            }
                            str << "<button type=\"submit\" name=\"submit\" class=\"btn btn-default\">Submit</button>";
                            str << "<strong> or </strong>";
                            str << "<button type=\"submit\" name=\"all\" class=\"btn btn-default\">Browse DB</button>";
                        }
                    }
                }
            }

            return str.Str();
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TSkeletonFrontMonLogoBlobsQueryActor
    ////////////////////////////////////////////////////////////////////////////

    struct TSkeletonFrontMonLogoBlobsQueryParams {
        TString DbName;
        TString Form;
        TMaybe<TLogoBlobID> From;
        TMaybe<TLogoBlobID> To;
        bool IndexOnly;
        bool ShowInternals;
        bool SubmitButton;
        bool AllButton;

        TString ErrorMsg;

        static TSkeletonFrontMonLogoBlobsQueryParams PullOut(NMon::TEvHttpInfo::TPtr &ev) {
            const TCgiParameters& cgi = ev->Get()->Request.GetParams();

            TSkeletonFrontMonLogoBlobsQueryParams params{
                .DbName = cgi.Get("dbname"),
                .Form = cgi.Get("form"),
                .IndexOnly = cgi.Has("IndexOnly"),
                .ShowInternals = cgi.Has("Internals"),
                .SubmitButton = cgi.Has("submit"),
                .AllButton = cgi.Has("all"),
            };

            if (!params.AllButton) {
                TString fromParam = cgi.Get("from");
                if (!fromParam) {
                    params.ErrorMsg = "Expected 'all' or 'from' fields: ";
                    return params;
                }

                TString errorExplanation;
                params.From = TLogoBlobID();
                bool good = TLogoBlobID::Parse(*params.From, fromParam, errorExplanation);
                if (!good) {
                    params.ErrorMsg = "Failed to parse 'from' field: " + errorExplanation;
                    return params;
                }

                TString toParam = cgi.Get("to");
                if (toParam) {
                    params.To = TLogoBlobID();
                    good = TLogoBlobID::Parse(*params.To, toParam, errorExplanation);
                    if (!good) {
                        params.ErrorMsg = "Failed to parse 'to' field: " + errorExplanation;
                        return params;
                    }
                }
            }

            return params;
        }

        static TSkeletonFrontMonLogoBlobsQueryParams PullOut(TEvGetLogoBlobRequest::TPtr &ev) {
            NKikimrVDisk::GetLogoBlobRequest &record = ev->Get()->Record;
            TSkeletonFrontMonLogoBlobsQueryParams params{
                .DbName = "",
                .Form = "",
                .IndexOnly = true,
                .ShowInternals = record.show_internals(),
                .SubmitButton = true,
                .AllButton = false,
            };

            auto convert = [] (auto &proto_id) {
                return TLogoBlobID(proto_id.raw_x1(), proto_id.raw_x2(), proto_id.raw_x3());
            };

            auto &range = record.range();
            params.From = convert(range.from());
            params.To = convert(range.to());

            return params;
        }
    };

    template <typename TEvent>
    class TSkeletonFrontMonLogoBlobsQueryActor : public TActorBootstrapped<TSkeletonFrontMonLogoBlobsQueryActor<TEvent>>, public TMonQueryBaseActor {
        const TVDiskID SelfVDiskId;
        TIntrusivePtr<TVDiskConfig> Cfg;
        std::shared_ptr<TBlobStorageGroupInfo::TTopology> Top;
        const TActorId NotifyId;
        const TActorId SkeletonFrontID;
        typename TEvent::TPtr Ev;
        TLogoBlobID From;
        TLogoBlobID To;
        bool IsRangeQuery;
        bool IndexOnly;
        bool ShowInternals;

        static constexpr bool IsHttpInfo = std::is_same_v<TEvent, NMon::TEvHttpInfo>;

        friend class TActorBootstrapped<TSkeletonFrontMonLogoBlobsQueryActor<TEvent>>;

        void OutputForm(const TActorContext &ctx, const TString &dbnameParam) {
            TString html = BuildForm(dbnameParam, "LogoBlob", "[tablet:gen:step:channel:cookie:blobsize:partid]", true);
            Finish(ctx, new NMon::TEvHttpInfoRes(html));
        }

        void Bootstrap(const TActorContext &ctx) {
            auto params = TSkeletonFrontMonLogoBlobsQueryParams::PullOut(Ev);
            ShowInternals = params.ShowInternals;

            if constexpr (IsHttpInfo) {
                if (params.Form != TString()) {
                    OutputForm(ctx, params.DbName);
                    return;
                }
            }

            if (params.ErrorMsg) {
                HandleErrorAndDie(ctx, params.ErrorMsg);
                return;
            }

            // FIXME: how to turn pages?

            std::unique_ptr<TEvBlobStorage::TEvVGet> req;
            const auto flags = ShowInternals ? TEvBlobStorage::TEvVGet::EFlags::ShowInternals : TEvBlobStorage::TEvVGet::EFlags::None;

            if (params.SubmitButton) {
                // check that 'from' field is not empty

                if (params.To.Empty()) {
                    // exact query
                    IsRangeQuery = false;
                    if (IndexOnly) {
                        req = TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(SelfVDiskId, TInstant::Max(),
                                NKikimrBlobStorage::EGetHandleClass::AsyncRead, flags, {}, {*params.From});
                    } else {
                        req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(SelfVDiskId, TInstant::Max(),
                                NKikimrBlobStorage::EGetHandleClass::AsyncRead, flags, {}, {*params.From});
                    }
                } else {
                    // range query
                    IsRangeQuery = true;
                    req = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(SelfVDiskId, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::AsyncRead, flags, {}, *params.From, *params.To, 1000);
                }
            } else if (params.AllButton) {
                // browse database
                IsRangeQuery = true;
                From = Min<TLogoBlobID>();
                To = Max<TLogoBlobID>();
                req = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(SelfVDiskId, TInstant::Max(),
                        NKikimrBlobStorage::EGetHandleClass::AsyncRead, flags, {}, From, To, 1000);
            } else {
                HandleErrorAndDie(ctx, "Unknown button");
                return;
            }

            if (req) {
                req->SetIsLocalMon();
                ctx.Send(SkeletonFrontID, req.release());
            }

            // set up timeout, after which we reply
            ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());

            // switch state
            this->Become(&TSkeletonFrontMonLogoBlobsQueryActor::StateFunc);
        }

        void OutputOneQueryResultToHtml(IOutputStream &str, const NKikimrBlobStorage::TQueryResult &q,
                TEvBlobStorage::TEvVGetResult& ev) {
            HTML(str) {
                DIV_CLASS("well well-small") {
                    const TLogoBlobID id = LogoBlobIDFromLogoBlobID(q.GetBlobID());
                    const TIngress ingress(q.GetIngress());
                    str << "Status: " << NKikimrProto::EReplyStatus_Name(q.GetStatus()) << "<br>";
                    str << "Id: " << id.ToString() << "<br>";
                    if (ShowInternals) {
                        str << "Ingress: " << ingress.ToString(Top.get(), TVDiskIdShort(SelfVDiskId), id) << "<br>";
                    }
                    if (!IndexOnly) {
                        str << "FullDataSize: " << q.GetFullDataSize() << "<br>";
                        str << "Data: " << EscapeC(ev.GetBlobData(q).ConvertToString()) << "<br>";
                    }
                }
            }
        }

        void OutputOneQueryResultToProto(NKikimrVDisk::GetLogoBlobResponse::LogoBlob *blob, const NKikimrBlobStorage::TQueryResult &q) {
            TLogoBlobID id = LogoBlobIDFromLogoBlobID(q.GetBlobID());
            blob->set_id(id.ToString());
            blob->set_status(NKikimrProto::EReplyStatus_Name(q.GetStatus()));
            if (ShowInternals) {
                TIngress ingress(q.GetIngress());
                blob->set_ingress(ingress.ToString(Top.get(), TVDiskIdShort(SelfVDiskId), id));
            }
        }


        void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
            const NKikimrBlobStorage::TEvVGetResult &rec = ev->Get()->Record;
            const TVDiskID vdisk(VDiskIDFromVDiskID(rec.GetVDiskID()));
            ui32 size = rec.ResultSize();

            if constexpr (IsHttpInfo) {
                TStringStream str;
                HTML(str) {
                    DIV_CLASS("row") {
                        STRONG() {
                            str << "From: " << From.ToString() << "<br>";
                            if (IsRangeQuery)
                                str << "To: " << To.ToString() << "<br>";
                            str << "IndexOnly: " << (IndexOnly ? "true" : "false") << "<br>";
                            str << "ShowInternals: " << (ShowInternals ? "true" : "false") << "<br>";
                            str << "Status: " << NKikimrProto::EReplyStatus_Name(rec.GetStatus()) << "<br>";
                            str << "VDisk: " << vdisk.ToString() << "<br>";
                            str << "Result size: " << size << "<br>";
                        }
                    }
                    DIV_CLASS("row") {
                        for (ui32 i = 0; i < size; i++) {
                            const NKikimrBlobStorage::TQueryResult &q = rec.GetResult(i);
                            OutputOneQueryResultToHtml(str, q, *ev->Get());
                        }
                    }
                }
                Finish(ctx, new NMon::TEvHttpInfoRes(str.Str()));
            } else {
                auto res = std::make_unique<TEvGetLogoBlobResponse>();
                for (ui32 i = 0; i < size; i++) {
                    OutputOneQueryResultToProto(res->Record.add_logoblobs(), rec.GetResult(i));
                }
                Finish(ctx, res.release());
            }
        }

        void HandleErrorAndDie(const TActorContext &ctx, TString error) {
            if constexpr (IsHttpInfo) {
                TStringStream str;
                str << "<strong><strong>" << error << "</strong></strong>";
                Finish(ctx, new NMon::TEvHttpInfoRes(str.Str()));
            } else {
                auto res = std::make_unique<TEvGetLogoBlobResponse>();
                res->Record.set_error_msg(error);
                Finish(ctx, res.release());
            }
        }

        void HandleWakeup(const TActorContext &ctx) {
            HandleErrorAndDie(ctx, "Timeout");
        }

        void Finish(const TActorContext &ctx, IEventBase *ev) {
            ctx.Send(NotifyId, new TEvents::TEvActorDied);
            ctx.Send(Ev->Sender, ev);
            this->Die(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            this->Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvBlobStorage::TEvVGetResult, Handle)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_MON_SF_LOGOBLOBS;
        }

        TSkeletonFrontMonLogoBlobsQueryActor(const TVDiskID &selfVDiskId,
                                             const TActorId &notifyId,
                                             TIntrusivePtr<TVDiskConfig> cfg,
                                             const std::shared_ptr<TBlobStorageGroupInfo::TTopology> &top,
                                             const TActorId &skeletonFrontID,
                                             typename TEvent::TPtr &ev)
            : TActorBootstrapped<TSkeletonFrontMonLogoBlobsQueryActor>()
            , SelfVDiskId(selfVDiskId)
            , Cfg(cfg)
            , Top(top)
            , NotifyId(notifyId)
            , SkeletonFrontID(skeletonFrontID)
            , Ev(ev)
            , From()
            , To()
            , IsRangeQuery(false)
            , IndexOnly(false)
            , ShowInternals(false)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // TSkeletonFrontMonBarriersQueryActor
    ////////////////////////////////////////////////////////////////////////////
    class TSkeletonFrontMonBarriersQueryActor : public TActorBootstrapped<TSkeletonFrontMonBarriersQueryActor>, public TMonQueryBaseActor {
        const TVDiskID SelfVDiskId;
        TIntrusivePtr<TVDiskConfig> Cfg;
        std::shared_ptr<TBlobStorageGroupInfo::TTopology> Top;
        const TActorId NotifyId;
        const TActorId SkeletonFrontID;
        NMon::TEvHttpInfo::TPtr Ev;
        TKeyBarrier From;
        TKeyBarrier To;
        bool IsRangeQuery;
        bool ShowInternals;

        friend class TActorBootstrapped<TSkeletonFrontMonBarriersQueryActor>;

        void OutputForm(const TActorContext &ctx, const TString &dbnameParam) {
            TString html = BuildForm(dbnameParam, "Barrier", "[tablet:channel:gen:gencounter]", false);
            Finish(ctx, new NMon::TEvHttpInfoRes(html));
        }

        void Bootstrap(const TActorContext &ctx) {
            const TCgiParameters& cgi = Ev->Get()->Request.GetParams();

            TString dbnameParam = cgi.Get("dbname");
            TString formParam = cgi.Get("form");
            TString fromParam = cgi.Get("from");
            TString toParam = cgi.Get("to");
            ShowInternals = cgi.Has("Internals");
            bool submitButton = cgi.Has("submit");
            bool allButton = cgi.Has("all");

            if (formParam != TString()) {
                OutputForm(ctx, dbnameParam);
                return;
            }

            ui32 maxResults = 1000;
            std::unique_ptr<TEvBlobStorage::TEvVGetBarrier> req;
            if (submitButton) {
                // check that 'from' field is not empty
                if (fromParam.empty()) {
                    Finish(ctx, NMonUtil::PrepareError("'From' field is empty"));
                    return;
                }

                // parse 'from' field
                TString errorExplanation;
                bool good = TKeyBarrier::Parse(From, fromParam, errorExplanation);
                if (!good) {
                    Finish(ctx, NMonUtil::PrepareError("Failed to parse 'from' field: " + errorExplanation));
                    return;
                }

                if (toParam.empty()) {
                    // exact query
                    IsRangeQuery = false;
                    req = std::make_unique<TEvBlobStorage::TEvVGetBarrier>(SelfVDiskId, From, From, &maxResults, ShowInternals);
                    ctx.Send(SkeletonFrontID, req.release());
                } else {
                    // range query
                    IsRangeQuery = true;

                    // parse 'to' field
                    good = TKeyBarrier::Parse(To, toParam, errorExplanation);
                    if (!good) {
                        Finish(ctx, NMonUtil::PrepareError("Failed to parse 'to' field: " + errorExplanation));
                        return;
                    }

                    req = std::make_unique<TEvBlobStorage::TEvVGetBarrier>(SelfVDiskId, From, To, &maxResults, ShowInternals);
                    ctx.Send(SkeletonFrontID, req.release());
                }
            } else if (allButton) {
                // browse database
                IsRangeQuery = true;
                From = TKeyBarrier::First();
                To = TKeyBarrier::Inf();
                req = std::make_unique<TEvBlobStorage::TEvVGetBarrier>(SelfVDiskId, From, To, &maxResults, ShowInternals);
                ctx.Send(SkeletonFrontID, req.release());
            }

            // set up timeout, after which we reply
            ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());

            // switch state
            Become(&TThis::StateFunc);
        }

        void OutputOneResult(IOutputStream &str, const NKikimrBlobStorage::TBarrierKey &k,
                             const NKikimrBlobStorage::TBarrierVal &v) {
            TIngressCachePtr ingressCache = TIngressCache::Create(Top, SelfVDiskId);
            HTML(str) {
                DIV_CLASS("well well-small") {
                    TKeyBarrier key(k);
                    str << "Key: " << key.ToString() << "<br>";
                    str << "CollectGen: " << v.GetCollectGen() << "<br>";
                    str << "CollectStep: " << v.GetCollectStep() << "<br>";
                    if (v.HasIngress()) {
                        const TBarrierIngress ingress(TBarrierIngress::CreateFromRaw(v.GetIngress()));
                        str << "Ingress: " << ingress.ToString(ingressCache.Get()) << "<br>";
                    }
                }
            }
        }

        void Handle(TEvBlobStorage::TEvVGetBarrierResult::TPtr &ev, const TActorContext &ctx) {
            const NKikimrBlobStorage::TEvVGetBarrierResult &rec = ev->Get()->Record;
            const TVDiskID vdisk(VDiskIDFromVDiskID(rec.GetVDiskID()));

            if (rec.KeysSize() != rec.ValuesSize()) {
                Finish(ctx, NMonUtil::PrepareError("Keys size and values size mismatch"));
                return;
            }

            const ui32 size = rec.KeysSize();;
            TStringStream str;

            HTML(str) {
                DIV_CLASS("row") {
                    STRONG() {
                        str << "From: " << From.ToString() << "<br>";
                        if (IsRangeQuery)
                            str << "To: " << To.ToString() << "<br>";
                        str << "ShowInternals: " << (ShowInternals ? "true" : "false") << "<br>";
                        str << "Status: " << NKikimrProto::EReplyStatus_Name(rec.GetStatus()) << "<br>";
                        str << "VDisk: " << vdisk.ToString() << "<br>";
                        str << "Result size: " << size << "<br>";
                    }
                }
                DIV_CLASS("row") {
                    for (ui32 i = 0; i < size; i++) {
                        OutputOneResult(str, rec.GetKeys(i), rec.GetValues(i));
                    }
                }
            }

            Finish(ctx, new NMon::TEvHttpInfoRes(str.Str()));
        }

        void HandleWakeup(const TActorContext &ctx) {
            TStringStream str;
            str << "<strong><strong>Timeout</strong></strong>";
            Finish(ctx, new NMon::TEvHttpInfoRes(str.Str()));
        }

        void Finish(const TActorContext &ctx, IEventBase *ev) {
            ctx.Send(NotifyId, new TEvents::TEvActorDied);
            ctx.Send(Ev->Sender, ev);
            Die(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvBlobStorage::TEvVGetBarrierResult, Handle)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_MON_SF_BARRIERS;
        }

        TSkeletonFrontMonBarriersQueryActor(const TVDiskID &selfVDiskId,
                                            const TActorId &notifyId,
                                            TIntrusivePtr<TVDiskConfig> cfg,
                                            const std::shared_ptr<TBlobStorageGroupInfo::TTopology> &top,
                                            const TActorId &skeletonFrontID,
                                            NMon::TEvHttpInfo::TPtr &ev)
            : TActorBootstrapped<TSkeletonFrontMonBarriersQueryActor>()
            , SelfVDiskId(selfVDiskId)
            , Cfg(cfg)
            , Top(top)
            , NotifyId(notifyId)
            , SkeletonFrontID(skeletonFrontID)
            , Ev(ev)
            , From()
            , To()
            , IsRangeQuery(false)
            , ShowInternals(false)
        {
            Y_UNUSED(ShowInternals);
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TSkeletonFrontMonDbStatActor
    ////////////////////////////////////////////////////////////////////////////
    class TSkeletonFrontMonDbStatActor : public TActorBootstrapped<TSkeletonFrontMonDbStatActor> {
        const TVDiskID SelfVDiskId;
        TIntrusivePtr<TVDiskConfig> Cfg;
        const TActorId NotifyId;
        const TActorId SkeletonFrontID;
        NMon::TEvHttpInfo::TPtr Ev;
        const NKikimrBlobStorage::EDbStatAction Action;
        const TString Dbname;

        friend class TActorBootstrapped<TSkeletonFrontMonDbStatActor>;

        bool PrettyPrint() const {
            const TCgiParameters& cgi = Ev->Get()->Request.GetParams();
            // format numbers pretty print
            const TString &numbers = cgi.Get("numbers");
            return (numbers == "pretty" || numbers == ""); // pretty by default
        }

        NMonUtil::TParseResult<ui64> ParseNumber(const char *name) const {
            const TCgiParameters& cgi = Ev->Get()->Request.GetParams();
            // format numbers pretty print
            const TString &str = cgi.Get(name);
            if (str.empty())
                return {NMonUtil::EParseRes::Empty, TString(), 0};

            // parse str
            char *endptr = nullptr;
            ui64 num = strtoll(str.data(), &endptr, 10);
            if (!(endptr && *endptr == '\0')) {
                return {NMonUtil::EParseRes::Error, str, 0};
            } else {
                return {NMonUtil::EParseRes::OK, str, num};
            }
        }

        NMonUtil::TParseResult<ui64> ParseTabletId() const {
            return ParseNumber("tabletid");
        }

        NMonUtil::TParseResult<ui64> ParseChannel() const {
            return ParseNumber("channel");
        }

        struct TMessage {
            bool Error;                 // was an error?
            std::unique_ptr<IEventBase> Msg;    // in case of error contains reply, or a request to VDisk otherwise
        };

        TMessage CreateDumpDbMessageOK(NKikimrBlobStorage::EDbStatType dbStatType, bool pretty) {
            auto tabletIdParseRes = ParseTabletId();
            auto channelParseRes = ParseChannel();
            if (tabletIdParseRes.Status == NMonUtil::EParseRes::Error) {
                auto s = Sprintf("Unsupported value '%s' for CGI parameter 'tabletid'", tabletIdParseRes.StrVal.data());
                return TMessage {true, std::unique_ptr<IEventBase>(NMonUtil::PrepareError(s))};
            }
            if (channelParseRes.Status == NMonUtil::EParseRes::Error) {
                auto s = Sprintf("Unsupported value '%s' for CGI parameter 'channel'", channelParseRes.StrVal.data());
                return TMessage {true, std::unique_ptr<IEventBase>(NMonUtil::PrepareError(s))};
            }
            if (tabletIdParseRes.Status != channelParseRes.Status) {
                auto s = Sprintf("CGI parameters 'tabletid' and 'channel' must be both OK or empty");
                return TMessage {true, std::unique_ptr<IEventBase>(NMonUtil::PrepareError(s))};
            }

            auto msg = std::make_unique<TEvBlobStorage::TEvVDbStat>(SelfVDiskId, Action, dbStatType, pretty);
            if (tabletIdParseRes.Status == NMonUtil::EParseRes::OK) {
                // set up constraint
                ui64 tabletId = tabletIdParseRes.Value;
                ui32 channel = channelParseRes.Value;
                msg->Record.MutableConstraint()->SetTabletId(tabletId);
                msg->Record.MutableConstraint()->SetChannel(channel);
            }
            return TMessage {false, std::move(msg)};
        }

        TMessage CreateStatDbMessageOK(NKikimrBlobStorage::EDbStatType dbStatType, bool pretty) {
            return TMessage {false, std::make_unique<TEvBlobStorage::TEvVDbStat>(SelfVDiskId, Action, dbStatType, pretty)};
        }

        TMessage CreateStatDumpDbMessage() {
            auto r = NMonUtil::ParseDbName(Dbname);
            if (r.Status == NMonUtil::EParseRes::Error || r.Status == NMonUtil::EParseRes::Empty) {
                auto s = Sprintf("Unsupported value '%s' for CGI parameter 'dbname'", r.StrVal.data());
                return TMessage {true, std::unique_ptr<IEventBase>(NMonUtil::PrepareError(s))};
            } else {
                // send db stat request
                NKikimrBlobStorage::EDbStatType dbStatType = r.Value;
                const bool pretty = PrettyPrint();
                switch (Action) {
                    case NKikimrBlobStorage::DumpDb:
                        return CreateDumpDbMessageOK(dbStatType, pretty);
                    case NKikimrBlobStorage::StatDb:
                        return CreateStatDbMessageOK(dbStatType, pretty);
                    default:
                        Y_ABORT("Unexpected case");
                };
            }
        }

        TMessage CreateStatTabletMessage() {
            auto r = ParseTabletId();
            if (r.Status == NMonUtil::EParseRes::OK) {
                const bool pretty = PrettyPrint();
                return TMessage {false, std::make_unique<TEvBlobStorage::TEvVDbStat>(SelfVDiskId, r.Value, pretty)};
            } else {
                auto s = Sprintf("Unsupported value '%s' for CGI parameter 'tabletid'", r.StrVal.data());
                return TMessage {true, std::unique_ptr<IEventBase>(NMonUtil::PrepareError(s))};
            }
        }

        TMessage CreateStatHugeMessage() {
            const bool pretty = PrettyPrint();
            return TMessage {
                false,
                std::make_unique<TEvBlobStorage::TEvVDbStat>(SelfVDiskId, NKikimrBlobStorage::StatHugeAction,
                NKikimrBlobStorage::StatHugeType, pretty)};
        }

        // creates a message
        TMessage CreateMessage() {
            switch (Action) {
                case NKikimrBlobStorage::DumpDb:
                case NKikimrBlobStorage::StatDb:
                    return CreateStatDumpDbMessage();
                case NKikimrBlobStorage::StatTabletAction:
                    return CreateStatTabletMessage();
                case NKikimrBlobStorage::StatHugeAction:
                    return CreateStatHugeMessage();
                default:
                    Y_ABORT("Unexpected case");
            }
        }

        void Bootstrap(const TActorContext &ctx) {
            TMessage msg(CreateMessage());
            if (msg.Error) {
                // error creating a message, finish with error
                Finish(ctx, msg.Msg.release());
            } else {
                // send TEvDbStat message
                ctx.Send(SkeletonFrontID, msg.Msg.release());
                // set up timeout, after which we reply
                ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
                // switch state
                Become(&TThis::StateFunc);
            }
        }

        void Handle(TEvBlobStorage::TEvVDbStatResult::TPtr &ev, const TActorContext &ctx) {
            const NKikimrBlobStorage::TEvVDbStatResult &rec = ev->Get()->Record;
            Finish(ctx, new NMon::TEvHttpInfoRes(rec.GetData()));
        }

        void HandleWakeup(const TActorContext &ctx) {
            TStringStream str;
            str << "<strong><strong>Timeout</strong></strong>";
            Finish(ctx, new NMon::TEvHttpInfoRes(str.Str()));
        }

        void Finish(const TActorContext &ctx, IEventBase *ev) {
            ctx.Send(NotifyId, new TEvents::TEvActorDied);
            ctx.Send(Ev->Sender, ev);
            Die(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvBlobStorage::TEvVDbStatResult, Handle)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_MON_SF_LBSTAT;
        }

        TSkeletonFrontMonDbStatActor(const TVDiskID &selfVDiskId,
                                     const TActorId &notifyId,
                                     TIntrusivePtr<TVDiskConfig> cfg,
                                     const TActorId &skeletonFrontID,
                                     NMon::TEvHttpInfo::TPtr &ev,
                                     NKikimrBlobStorage::EDbStatAction action,
                                     const TString &dbname)
            : TActorBootstrapped<TSkeletonFrontMonDbStatActor>()
            , SelfVDiskId(selfVDiskId)
            , Cfg(cfg)
            , NotifyId(notifyId)
            , SkeletonFrontID(skeletonFrontID)
            , Ev(ev)
            , Action(action)
            , Dbname(dbname)
        {}
    };

    class TRestartVDiskActor : public TActorBootstrapped<TRestartVDiskActor> {
        const ui32 PDiskId;
        const TVDiskID VDiskId;
        const TActorId WardenId;
        const TActorId NotifyId;
        const TActorId Sender;

        friend class TActorBootstrapped<TRestartVDiskActor>;

        void Bootstrap(const TActorContext &ctx) {
            ctx.Send(WardenId, new TEvBlobStorage::TEvAskRestartVDisk(PDiskId, VDiskId));
            ctx.Send(NotifyId, new TEvents::TEvActorDied);
            ctx.Send(Sender, new NMon::TEvHttpInfoRes(MakeReply()));
            Die(ctx);
        }

        TString MakeReply() const {
            TStringStream str;
            HTML(str) {
                str << "VDisk restart request has been sent <br>\n"
                    << "<a class=\"btn btn-default\" href=\"?\">Go back to the main VDisk page</a>";
            }
            return str.Str();
        }

    public:
        TRestartVDiskActor(
            const ui32 pDiskId,
            const TVDiskID &vDiskId,
            const TActorId &wardenId,
            const TActorId &notifyId,
            const TActorId &sender
        )
            : TActorBootstrapped<TRestartVDiskActor>()
            , PDiskId(pDiskId)
            , VDiskId(vDiskId)
            , WardenId(wardenId)
            , NotifyId(notifyId)
            , Sender(sender)
        {
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TSkeletonFrontMonMainPageActor
    ////////////////////////////////////////////////////////////////////////////
    class TSkeletonFrontMonMainPageActor : public TActorBootstrapped<TSkeletonFrontMonMainPageActor> {
        const TActorId NotifyId;
        const TActorId SkeletonID;
        NMon::TEvHttpInfo::TPtr Ev;
        TString SkeletonFrontAnswer;
        TString SkeletonAnswer;


        friend class TActorBootstrapped<TSkeletonFrontMonMainPageActor>;

        void Bootstrap(const TActorContext &ctx) {
            if (!bool(SkeletonID)) {
                SkeletonAnswer = "<strong><strong>Skeleton is not ready</strong></strong><br>";
                Finish(ctx);
            } else {
                ctx.Send(SkeletonID, new NMon::TEvHttpInfo(Ev->Get()->Request));

                // set up timeout, after which we reply
                ctx.Schedule(TDuration::Seconds(15), new TEvents::TEvWakeup());

                // switch state
                Become(&TThis::StateFunc);
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
                Output(SkeletonFrontAnswer, str, "SkeletonFront");
                Output(SkeletonAnswer, str, "Skeleton");
            }

            ctx.Send(NotifyId, new TEvents::TEvActorDied);
            ctx.Send(Ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
            Die(ctx);
        }

        void HandleWakeup(const TActorContext &ctx) {
            Finish(ctx);
        }

        void Handle(NMon::TEvHttpInfoRes::TPtr &ev, const TActorContext &ctx) {
            NMon::TEvHttpInfoRes *ptr = dynamic_cast<NMon::TEvHttpInfoRes*>(ev->Get());
            Y_DEBUG_ABORT_UNLESS(ptr);
            Y_DEBUG_ABORT_UNLESS(ptr->SubRequestId == 0);
            SkeletonAnswer = ptr->Answer;
            Finish(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(NMon::TEvHttpInfoRes, Handle)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_MON_SF_MAIN_PAGE;
        }

        TSkeletonFrontMonMainPageActor(const TActorId &notifyId,
                                       const TActorId &skeletonID,
                                       NMon::TEvHttpInfo::TPtr &ev,
                                       const TString &frontHtml)
            : TActorBootstrapped<TSkeletonFrontMonMainPageActor>()
            , NotifyId(notifyId)
            , SkeletonID(skeletonID)
            , Ev(ev)
            , SkeletonFrontAnswer(frontHtml)
        {}
    };

    bool IsVDiskRestartAllowed(NKikimrWhiteboard::EVDiskState state) {
        return state == NKikimrWhiteboard::EVDiskState::PDiskError;
    }

    ////////////////////////////////////////////////////////////////////////////
    // SKELETON FRONT MON REQUEST HANDLER
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateFrontSkeletonMonRequestHandler(const TVDiskID &selfVDiskId,
                                                 const TActorId &notifyId,
                                                 const TActorId &skeletonID,
                                                 const TActorId &skeletonFrontID,
                                                 TIntrusivePtr<TVDiskConfig> cfg,
                                                 const std::shared_ptr<TBlobStorageGroupInfo::TTopology> &top,
                                                 NMon::TEvHttpInfo::TPtr &ev,
                                                 const TString &frontHtml,
                                                 const NMonGroup::TVDiskStateGroup& vDiskMonGroup) {
        const TCgiParameters& cgi = ev->Get()->Request.GetParams();

        const TString &type = cgi.Get("type");
        const TString &dbname = cgi.Get("dbname");
        if (type == TString()) {
            return new TSkeletonFrontMonMainPageActor(notifyId, skeletonID, ev, frontHtml);
        } else if (type == "query") {
            if (dbname == "LogoBlobs") {
                return new TSkeletonFrontMonLogoBlobsQueryActor<NMon::TEvHttpInfo>(selfVDiskId, notifyId, cfg, top, skeletonID, ev);
            } else if(dbname == "Barriers") {
                return new TSkeletonFrontMonBarriersQueryActor(selfVDiskId, notifyId, cfg, top, skeletonID, ev);
            } else {
                auto s = Sprintf("Unsupported value '%s' for CGI parameter 'dbname'", dbname.data());
                return new TMonErrorActor(notifyId, ev, s);
            }
        } else if (type == "stat") {
            return new TSkeletonFrontMonDbStatActor(selfVDiskId, notifyId, cfg, skeletonFrontID,
                    ev, NKikimrBlobStorage::StatDb, dbname);
        } else if (type == "dump") {
            return new TSkeletonFrontMonDbStatActor(selfVDiskId, notifyId, cfg, skeletonFrontID,
                    ev, NKikimrBlobStorage::DumpDb, dbname);
        } else if (type == "tabletstat") {
            return new TSkeletonFrontMonDbStatActor(selfVDiskId, notifyId, cfg, skeletonFrontID,
                    ev, NKikimrBlobStorage::StatTabletAction, dbname);
        } else if (type == "hugestat") {
            return new TSkeletonFrontMonDbStatActor(selfVDiskId, notifyId, cfg, skeletonFrontID,
                    ev, NKikimrBlobStorage::StatHugeAction, dbname);
        } else if (type == "dbmainpage") {
            return CreateMonDbMainPageActor(selfVDiskId, notifyId, skeletonFrontID, skeletonID, ev);
        } else if (type == "restart") {
            if (IsVDiskRestartAllowed(vDiskMonGroup.VDiskState())) {
                return new TRestartVDiskActor(
                    cfg->BaseInfo.PDiskId, selfVDiskId, MakeBlobStorageNodeWardenID(skeletonFrontID.NodeId()), notifyId, ev->Sender
                );
            } else {
                return new TMonErrorActor(notifyId, ev,
                    "VDisk restart in the normal state is not allowed <br>\n"
                    "<a class=\"btn btn-default\" href=\"?\">Go back to the main VDisk page</a>"
                );
            }
        } else {
            auto s = Sprintf("Unknown value '%s' for CGI parameter 'type'", type.data());
            return new TMonErrorActor(notifyId, ev, s);
        }
    }

    IActor* CreateFrontSkeletonGetLogoBlobRequestHandler(const TVDiskID &selfVDiskId,
                                                         const TActorId &notifyId,
                                                         const TActorId &skeletonID,
                                                         TIntrusivePtr<TVDiskConfig> cfg,
                                                         const std::shared_ptr<TBlobStorageGroupInfo::TTopology> &top,
                                                         TEvGetLogoBlobRequest::TPtr &ev) {

        return new TSkeletonFrontMonLogoBlobsQueryActor<TEvGetLogoBlobRequest>(selfVDiskId, notifyId, cfg, top, skeletonID, ev);
    }

} // NKikimr
