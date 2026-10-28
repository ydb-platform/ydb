#include "blobstorage_monactors.h"
#include "blobstorage_db.h"
#include "skeleton_mon_dbmainpage.h"
#include "skeleton_mon_util.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/html/escape/escape.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>

#include <util/stream/format.h>

#include <array>

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
            ctx.Send(NotifyId, new TEvents::TEvGone);
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
        TString Shred;

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

            ctx.Send(Db->SkeletonID, new NMon::TEvHttpInfo(Ev->Get()->Request, TDbMon::Shred));
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

        template <typename F>
        void OutputCollapsible(IOutputStream &str, const TString &id, const TString &buttonText, F&& body) {
            str << R"(<p><a class="btn btn-default" data-toggle="collapse" href="#)" << id
                << R"(" role="button" aria-expanded="false" aria-controls=")" << id << R"(">)"
                << buttonText << R"(</a></p>)";
            str << R"(<div class="collapse" id=")" << id << R"(")";
            str << ">";
            body();
            str << "</div>";
        }

        void Finish(const TActorContext &ctx) {
            TStringStream str;
            HTML(str) {
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-6") {
                        OutputCollapsible(str, "vdisk-counters", "Show/Hide counters", [&] {
                            Db->VCtx->VDiskCounters->OutputHtml(str);
                        });
                    }
                    DIV_CLASS("col-md-6") {
                        OutputCollapsible(str, "vdisk-skeleton-state", "Show/Hide Skeleton State", [&] {
                            Output(SkeletonState, str, "Skeleton State");
                        });
                    }
                    DIV_CLASS("col-md-6") {
                        OutputCollapsible(str, "vdisk-logcutter", "Show/Hide LogCutter", [&] {
                            Output(LogCutterInfo, str, "Log Cutter");
                        });
                    }
                    DIV_CLASS("col-md-6") {
                        OutputCollapsible(str, "vdisk-huge-keeper", "Show/Hide Huge Blob Keeper", [&] {
                            Output(HugeKeeperInfo, str, "Huge Blob Keeper");
                        });
                    }
                    DIV_CLASS("col-md-6") {
                        OutputCollapsible(str, "vdisk-disk-space", "Show/Hide Disk Space Tracker", [&] {
                            Output(DskSpaceTrackerInfo, str, "Disk Space Tracker");
                        });
                    }
                    DIV_CLASS("col-md-6") {
                        OutputCollapsible(str, "vdisk-local-recov", "Show/Hide Local Recovery Info", [&] {
                            Output(LocalRecovInfo, str, "Local Recovery Info");
                        });
                    }
                    DIV_CLASS("col-md-6") {
                        OutputCollapsible(str, "vdisk-delayed-deleter", "Show/Hide Delayed Compaction Deleter", [&] {
                            Output(DelayedCompactionDeleterInfo, str, "Delayed Compaction Deleter Info");
                        });
                    }
                    DIV_CLASS("col-md-6") {
                        OutputCollapsible(str, "vdisk-scrub-info", "Show/Hide Scrub Info", [&] {
                            Output(ScrubInfo, str, "Scrub Info");
                        });
                    }
                    DIV_CLASS("col-md-6") {Output(Shred, str, "Shred State");}
                    // uses column wrapping (sum is greater than 12)
                }
                Output(HullInfo, str, "Hull");
                Output(SyncLogInfo, str, "Sync Log");
                Output(SyncerInfo, str, "Syncer");
                Output(ReplInfo, str, "Repl");
                Output(AnubisRunnerInfo, str, "Anubis");
                Output(HandoffInfo, str, "Handoff");
            }

            ctx.Send(NotifyId, new TEvents::TEvGone);
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
                {TDbMon::Shred,                    &TThis::Shred},
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
                .IndexOnly = !record.need_data(),
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
            if (params.To == params.From) {
                params.To.Clear();
            }

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

        void OutputOneQueryResultToProto(NKikimrVDisk::GetLogoBlobResponse::LogoBlob *blob, const NKikimrBlobStorage::TQueryResult &q,
                TEvBlobStorage::TEvVGetResult *ev) {
            TLogoBlobID id = LogoBlobIDFromLogoBlobID(q.GetBlobID());
            blob->set_id(id.ToString());
            blob->set_status(NKikimrProto::EReplyStatus_Name(q.GetStatus()));
            if (ShowInternals) {
                TIngress ingress(q.GetIngress());
                blob->set_ingress(ingress.ToString(Top.get(), TVDiskIdShort(SelfVDiskId), id));
            }
            if (TRope data = ev->GetBlobData(q)) {
                blob->set_data_base64(Base64Encode(data.ConvertToString()));
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
                    OutputOneQueryResultToProto(res->Record.add_logoblobs(), rec.GetResult(i), ev->Get());
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
            ctx.Send(NotifyId, new TEvents::TEvGone);
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
            ctx.Send(NotifyId, new TEvents::TEvGone);
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
            ctx.Send(NotifyId, new TEvents::TEvGone);
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

    ////////////////////////////////////////////////////////////////////////////
    // TSkeletonFrontMonSpaceReportActor
    ////////////////////////////////////////////////////////////////////////////
    class TSkeletonFrontMonSpaceReportActor : public TActorBootstrapped<TSkeletonFrontMonSpaceReportActor> {
    public:
        enum class EOutputFormat {
            RawProto,
            Visualization,
        };

    private:
        struct TBreakdownItem {
            const char* Name;
            const char* Group;
            ui64 Bytes;
            const char* ProgressBarClass;
        };

        static constexpr size_t BreakdownItemCount = 15;
        using TBreakdownItems = std::array<TBreakdownItem, BreakdownItemCount>;

        enum EWakeupTag : ui64 {
            Retry = 1,
            Timeout = 2,
        };

        const TActorId NotifyId;
        const TActorId SkeletonFrontID;
        NMon::TEvHttpInfo::TPtr Ev;
        const EOutputFormat OutputFormat;
        const bool ForceRecalculation;

        friend class TActorBootstrapped<TSkeletonFrontMonSpaceReportActor>;

        static TString FormatBytes(ui64 bytes) {
            return TStringBuilder() << HumanReadableSize(bytes, SF_BYTES) << " (" << bytes << " bytes)";
        }

        static TString FormatBytes(i64 bytes) {
            return TStringBuilder() << HumanReadableSize(bytes, SF_BYTES) << " (" << bytes << " bytes)";
        }

        static TString FormatTimestamp(ui64 unixMs) {
            return unixMs ? TInstant::MilliSeconds(unixMs).ToString() : TString("Unknown");
        }

        static TString FormatAge(ui64 unixMs) {
            const ui64 nowMs = TInstant::Now().MilliSeconds();
            return unixMs && unixMs <= nowMs
                ? TDuration::MilliSeconds(nowMs - unixMs).ToString()
                : TString("Unknown");
        }

        static TBreakdownItems GetBreakdownItems(
                const NKikimrVDisk::TVDiskSpaceBreakdown& breakdown)
        {
            return {{
                {"Useful blob data", "Useful data", breakdown.GetUsefulBlobDataBytes(),
                    "progress-bar progress-bar-success"},
                {"Live metadata", "Metadata", breakdown.GetLiveMetadataBytes(),
                    "progress-bar progress-bar-info"},
                {"Live auxiliary data", "System data", breakdown.GetLiveAuxiliaryDataBytes(),
                    "progress-bar"},
                {"GC-dead blob data", "Garbage", breakdown.GetGcDeadBlobDataBytes(),
                    "progress-bar progress-bar-danger"},
                {"GC-dead metadata", "Garbage", breakdown.GetGcDeadMetadataBytes(),
                    "progress-bar progress-bar-danger"},
                {"Merge-redundant blob data", "Garbage", breakdown.GetMergeRedundantBlobDataBytes(),
                    "progress-bar progress-bar-danger"},
                {"Merge-redundant metadata", "Garbage", breakdown.GetMergeRedundantMetadataBytes(),
                    "progress-bar progress-bar-danger"},
                {"Write padding", "Fragmentation", breakdown.GetWritePaddingBytes(),
                    "progress-bar progress-bar-warning"},
                {"Slot internal fragmentation", "Fragmentation", breakdown.GetSlotInternalFragmentationBytes(),
                    "progress-bar progress-bar-warning"},
                {"Free slot space", "Fragmentation", breakdown.GetFreeSlotBytes(),
                    "progress-bar progress-bar-warning"},
                {"Free stripe space", "Fragmentation", breakdown.GetFreeStripeBytes(),
                    "progress-bar progress-bar-warning"},
                {"Chunk tail", "Other", breakdown.GetChunkTailBytes(),
                    "progress-bar progress-bar-striped"},
                {"Free chunk reserve", "Other", breakdown.GetFreeChunkReserveBytes(),
                    "progress-bar progress-bar-striped"},
                {"Locked or quarantined", "Other", breakdown.GetLockedOrQuarantinedBytes(),
                    "progress-bar progress-bar-striped"},
                {"Unclassified", "Other", breakdown.GetUnclassifiedBytes(),
                    "progress-bar progress-bar-striped"},
            }};
        }

        static ui64 GetAccountedBytes(const NKikimrVDisk::TVDiskSpaceBreakdown& breakdown) {
            ui64 result = 0;
            for (const TBreakdownItem& item : GetBreakdownItems(breakdown)) {
                result += item.Bytes;
            }
            return result;
        }

        static double GetPercentage(ui64 bytes, ui64 totalBytes) {
            return totalBytes ? 100.0 * static_cast<double>(bytes) / static_cast<double>(totalBytes) : 0.0;
        }

        static void RenderBreakdownMarker(IOutputStream& str, const TBreakdownItem& item) {
            str << "<span class=\"progress\" aria-hidden=\"true\" "
                << "style=\"display:inline-block;width:1em;height:1em;margin:0 0.25em 0 0;vertical-align:middle\">"
                << "<span class=\"" << item.ProgressBarClass
                << "\" style=\"width:100%;height:100%\"></span></span>";
        }

        static void RenderBreakdownBar(
                IOutputStream& str,
                const NKikimrVDisk::TVDiskSpaceBreakdown& breakdown,
                ui64 totalBytes)
        {
            const auto items = GetBreakdownItems(breakdown);
            totalBytes = Max(totalBytes, GetAccountedBytes(breakdown));
            str << "<div class=\"progress\" style=\"margin-bottom:0\">";
            for (const TBreakdownItem& item : items) {
                if (!item.Bytes) {
                    continue;
                }
                const double percentage = GetPercentage(item.Bytes, totalBytes);
                str << "<div class=\"" << item.ProgressBarClass
                    << "\" role=\"progressbar\" style=\"width:" << Sprintf("%.6f", percentage)
                    << "%\" title=\"" << item.Name << ": " << FormatBytes(item.Bytes)
                    << " (" << Sprintf("%.2f", percentage) << "%)\"></div>";
            }
            str << "</div>";
        }

        static void RenderSummaryRow(IOutputStream& str, const char* name, const TString& value) {
            HTML(str) {
                TABLER() {
                    TABLED() { str << name; }
                    TABLED() { str << value; }
                }
            }
        }

        static void RenderComponentRow(
                IOutputStream& str,
                const TString& name,
                const NKikimrVDisk::TVDiskSpaceComponent& component)
        {
            const ui64 accountedBytes = GetAccountedBytes(component.GetBreakdown());
            HTML(str) {
                TABLER() {
                    TABLED() { str << name; }
                    TABLED_ATTRS({{"data-text", ToString(component.GetChunkCount())}}) {
                        str << component.GetChunkCount();
                    }
                    TABLED_ATTRS({{"data-text", ToString(component.GetAllocatedBytes())}}) {
                        str << FormatBytes(component.GetAllocatedBytes());
                    }
                    TABLED_ATTRS({{"data-text", ToString(component.GetStripedBytes())}}) {
                        str << FormatBytes(component.GetStripedBytes());
                    }
                    TABLED_ATTRS({{"data-text", ToString(accountedBytes)}}) {
                        str << FormatBytes(accountedBytes);
                    }
                    TABLED_ATTRS({{"data-text", ToString(accountedBytes)}}) {
                        RenderBreakdownBar(str, component.GetBreakdown(), component.GetAllocatedBytes());
                    }
                }
            }
        }

        static void RenderVisualization(const NKikimrVDisk::TGetVDiskSpaceReportResponse& response, IOutputStream& str) {
            HTML(str) {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        str << "VDisk Space Report"
                            << "<a class=\"btn btn-primary btn-xs navbar-right\""
                            << " href=\"?type=spacereport\">Raw Proto</a>"
                            << "<a class=\"btn btn-warning btn-xs navbar-right\" style=\"margin-right:5px\""
                            << " href=\"?type=spacereportvisual&force=1\">Recalculate</a>";
                    }
                    DIV_CLASS("panel-body") {
                        const bool ok = response.GetStatus() == NKikimrProto::EReplyStatus_Name(NKikimrProto::OK);
                        DIV_CLASS(ok ? "alert alert-success" : "alert alert-warning") {
                            STRONG() { str << "Status: "; }
                            str << NHtml::EscapeText(response.GetStatus());
                            if (response.GetErrorReason()) {
                                str << "<br>" << NHtml::EscapeText(response.GetErrorReason());
                            }
                        }

                        if (!response.HasReport()) {
                            return;
                        }

                        const auto& report = response.GetReport();
                        H4_CLASS("text-info") { str << "Summary"; }
                        TABLE_CLASS("table table-condensed") {
                            TABLEBODY() {
                                RenderSummaryRow(str, "Chunk size", FormatBytes(report.GetChunkSizeBytes()));
                                RenderSummaryRow(str, "PDisk allocated chunks", ToString(report.GetPDiskAllocatedChunks()));
                                RenderSummaryRow(str, "PDisk allocated space", FormatBytes(report.GetPDiskAllocatedBytes()));
                                RenderSummaryRow(str, "Accounted space", FormatBytes(report.GetAccountedBytes()));
                                RenderSummaryRow(str, "Reconciliation delta", FormatBytes(report.GetReconciliationDeltaBytes()));
                                RenderSummaryRow(str, "Collection started",
                                    FormatTimestamp(report.GetCollectionStartedAtUnixMs()));
                                RenderSummaryRow(str, "Collection completed",
                                    FormatTimestamp(report.GetCollectionCompletedAtUnixMs()));
                                if (report.GetCollectionStartedAtUnixMs()
                                        && report.GetCollectionCompletedAtUnixMs() >= report.GetCollectionStartedAtUnixMs()) {
                                    RenderSummaryRow(str, "Collection duration", TDuration::MilliSeconds(
                                        report.GetCollectionCompletedAtUnixMs()
                                            - report.GetCollectionStartedAtUnixMs()).ToString());
                                }
                                RenderSummaryRow(str, "Report age",
                                    FormatAge(report.GetCollectionCompletedAtUnixMs()));
                            }
                        }
                        DIV_CLASS("text-muted") {
                            str << "This is the latest report cached by the VDisk. Values are sampled without a global "
                                << "snapshot; reconciliation delta and unclassified space may be nonzero.";
                        }

                        H4_CLASS("text-info") { str << "Total breakdown"; }
                        RenderBreakdownBar(str, report.GetTotal(), report.GetPDiskAllocatedBytes());
                        TABLE_SORTABLE_CLASS("table table-condensed table-striped") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { str << "Category"; }
                                    TABLEH() { str << "Group"; }
                                    TABLEH() { str << "Space"; }
                                    TABLEH() { str << "% of PDisk allocation"; }
                                }
                            }
                            TABLEBODY() {
                                for (const TBreakdownItem& item : GetBreakdownItems(report.GetTotal())) {
                                    TABLER() {
                                        TABLED() {
                                            RenderBreakdownMarker(str, item);
                                            str << item.Name;
                                        }
                                        TABLED() { str << item.Group; }
                                        TABLED_ATTRS({{"data-text", ToString(item.Bytes)}}) {
                                            str << FormatBytes(item.Bytes);
                                        }
                                        const double percentage = GetPercentage(item.Bytes, report.GetPDiskAllocatedBytes());
                                        TABLED_ATTRS({{"data-text", Sprintf("%.12f", percentage)}}) {
                                            str << Sprintf("%.2f%%", percentage);
                                        }
                                    }
                                }
                            }
                        }

                        H4_CLASS("text-info") { str << "Components"; }
                        TABLE_SORTABLE_CLASS("table table-condensed table-striped") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { str << "Component"; }
                                    TABLEH() { str << "Chunks"; }
                                    TABLEH() { str << "Allocated"; }
                                    TABLEH() { str << "Striped"; }
                                    TABLEH() { str << "Accounted"; }
                                    TABLEH() { str << "Breakdown"; }
                                }
                            }
                            TABLEBODY() {
                                RenderComponentRow(str, "LogoBlobs / Inplace", report.GetLogoBlobs());
                                RenderComponentRow(str, "LogoBlobs / Huge", report.GetHuge().GetTotal());
                                RenderComponentRow(str, "Blocks", report.GetBlocks());
                                RenderComponentRow(str, "Barriers", report.GetBarriers());
                                RenderComponentRow(str, "SyncLog", report.GetSyncLog());
                                for (const auto& chunkKeeper : report.GetChunkKeeper()) {
                                    RenderComponentRow(str,
                                        TStringBuilder() << "ChunkKeeper " << chunkKeeper.GetSubsystemId(),
                                        chunkKeeper.GetTotal());
                                }
                                RenderComponentRow(str, "Unattributed", report.GetUnattributed());
                            }
                        }

                        if (report.HasStripeHeap()) {
                            const auto& stripeHeap = report.GetStripeHeap();
                            H4_CLASS("text-info") { str << "Stripe heap"; }
                            TABLE_CLASS("table table-condensed") {
                                TABLEBODY() {
                                    RenderSummaryRow(str, "Chunks", ToString(stripeHeap.GetChunkCount()));
                                    RenderSummaryRow(str, "Allocated", FormatBytes(stripeHeap.GetAllocatedBytes()));
                                    RenderSummaryRow(str, "Used", FormatBytes(stripeHeap.GetUsedBytes()));
                                    RenderSummaryRow(str, "Free", FormatBytes(stripeHeap.GetFreeBytes()));
                                    RenderSummaryRow(str, "Locked free", FormatBytes(stripeHeap.GetLockedFreeBytes()));
                                }
                            }
                            DIV_CLASS("text-muted") {
                                str << "Allocator summary only; these bytes are already attributed to components above.";
                            }
                        }

                        if (report.GetHuge().SizeClassesSize()) {
                            H4_CLASS("text-info") { str << "LogoBlobs / Huge size classes"; }
                            TABLE_CLASS("table table-condensed table-striped") {
                                TABLEHEAD() {
                                    TABLER() {
                                        TABLEH() { str << "Slot size"; }
                                        TABLEH() { str << "Slots/chunk"; }
                                        TABLEH() { str << "Chunks"; }
                                        TABLEH() { str << "Live slots"; }
                                        TABLEH() { str << "GC-dead slots"; }
                                        TABLEH() { str << "Merge-redundant slots"; }
                                        TABLEH() { str << "Unclassified slots"; }
                                        TABLEH() { str << "Accounted"; }
                                    }
                                }
                                TABLEBODY() {
                                    for (const auto& sizeClass : report.GetHuge().GetSizeClasses()) {
                                        TABLER() {
                                            TABLED() { str << FormatBytes(sizeClass.GetSlotSizeBytes()); }
                                            TABLED() { str << sizeClass.GetSlotsPerChunk(); }
                                            TABLED() { str << sizeClass.GetChunkCount(); }
                                            TABLED() { str << sizeClass.GetLiveSlotCount(); }
                                            TABLED() { str << sizeClass.GetGcDeadSlotCount(); }
                                            TABLED() { str << sizeClass.GetMergeRedundantSlotCount(); }
                                            TABLED() { str << sizeClass.GetUnclassifiedSlotCount(); }
                                            TABLED() { str << FormatBytes(GetAccountedBytes(sizeClass.GetBreakdown())); }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        void SendRequest(const TActorContext& ctx) {
            auto request = std::make_unique<TEvGetVDiskSpaceReportRequest>();
            request->Record.SetForceRecalculation(ForceRecalculation);
            ctx.Send(SkeletonFrontID, request.release());
        }

        void Bootstrap(const TActorContext &ctx) {
            SendRequest(ctx);
            ctx.Schedule(TDuration::Minutes(1), new TEvents::TEvWakeup(Timeout));
            Become(&TThis::StateFunc);
        }

        void Handle(TEvGetVDiskSpaceReportResponse::TPtr &ev, const TActorContext &ctx) {
            const auto& response = ev->Get()->Record;
            if (OutputFormat == EOutputFormat::RawProto) {
                TStringStream str;
                str << NMonitoring::HTTPOKTEXT << response.DebugString();
                Finish(ctx, new NMon::TEvHttpInfoRes(
                    str.Str(), Ev->Get()->SubRequestId, NMon::TEvHttpInfoRes::Custom));
            } else if (ForceRecalculation
                    && response.GetStatus() == NKikimrProto::EReplyStatus_Name(NKikimrProto::NOTREADY)
                    && !response.HasReport()) {
                ctx.Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup(Retry));
            } else {
                TStringStream str;
                RenderVisualization(response, str);
                Finish(ctx, new NMon::TEvHttpInfoRes(str.Str(), Ev->Get()->SubRequestId));
            }
        }

        void HandleWakeup(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx) {
            switch (ev->Get()->Tag) {
                case Retry:
                    SendRequest(ctx);
                    break;
                case Timeout:
                    Finish(ctx, new NMon::TEvHttpInfoRes(
                        "<strong>VDisk space report cache is not ready after one minute</strong>"));
                    break;
                default:
                    Y_ABORT("Unexpected VDisk space report wakeup tag");
            }
        }

        void Finish(const TActorContext &ctx, IEventBase *ev) {
            ctx.Send(NotifyId, new TEvents::TEvGone);
            ctx.Send(Ev->Sender, ev);
            Die(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvGetVDiskSpaceReportResponse, Handle)
            HFunc(TEvents::TEvWakeup, HandleWakeup)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_MON_SF_LBSTAT;
        }

        TSkeletonFrontMonSpaceReportActor(
                const TActorId &notifyId,
                const TActorId &skeletonFrontID,
                NMon::TEvHttpInfo::TPtr &ev,
                EOutputFormat outputFormat,
                bool forceRecalculation)
            : TActorBootstrapped<TSkeletonFrontMonSpaceReportActor>()
            , NotifyId(notifyId)
            , SkeletonFrontID(skeletonFrontID)
            , Ev(ev)
            , OutputFormat(outputFormat)
            , ForceRecalculation(forceRecalculation)
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
            ctx.Send(NotifyId, new TEvents::TEvGone);
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

            ctx.Send(NotifyId, new TEvents::TEvGone);
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
        const bool forceSpaceReportRecalculation = cgi.Get("force") == "1";
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
        } else if (type == "spacereport") {
            return new TSkeletonFrontMonSpaceReportActor(
                notifyId, skeletonFrontID, ev, TSkeletonFrontMonSpaceReportActor::EOutputFormat::RawProto,
                forceSpaceReportRecalculation);
        } else if (type == "spacereportvisual") {
            return new TSkeletonFrontMonSpaceReportActor(
                notifyId, skeletonFrontID, ev, TSkeletonFrontMonSpaceReportActor::EOutputFormat::Visualization,
                forceSpaceReportRecalculation);
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
