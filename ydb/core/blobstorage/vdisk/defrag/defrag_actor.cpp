#include "defrag_actor.h"
#include "defrag_quantum.h"
#include "defrag_search.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/circlebufstream.h>
#include <ydb/core/blobstorage/vdisk/common/sublog.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.h>
#include <ydb/core/blobstorage/vdisk/skeleton/blobstorage_takedbsnap.h>
#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/core/invoke.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TDefragCtx
    ////////////////////////////////////////////////////////////////////////////
    TDefragCtx::TDefragCtx(
            const TIntrusivePtr<TVDiskContext> &vctx,
            const TIntrusivePtr<TVDiskConfig> &vconfig,
            const std::shared_ptr<THugeBlobCtx> &hugeBlobCtx,
            const TPDiskCtxPtr &pdiskCtx,
            const TActorId &skeletonId,
            const TActorId &hugeKeeperId,
            bool runDefrageBySchedule)
        : VCtx(vctx)
        , VCfg(vconfig)
        , HugeBlobCtx(hugeBlobCtx)
        , PDiskCtx(pdiskCtx)
        , SkeletonId(skeletonId)
        , HugeKeeperId(hugeKeeperId)
        , DefragMonGroup(VCtx->VDiskCounters, "subsystem", "defrag")
        , RunDefragBySchedule(runDefrageBySchedule)
    {}

    TDefragCtx::~TDefragCtx() = default;

    struct TEvDefragStartQuantum : TEventLocal<TEvDefragStartQuantum, TEvBlobStorage::EvDefragStartQuantum> {
        TChunksToDefrag ChunksToDefrag;

        TEvDefragStartQuantum(TChunksToDefrag chunksToDefrag)
            : ChunksToDefrag(std::move(chunksToDefrag))
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // HugeHeapDefragmentationRequired
    // We calculate allowd percent of garbage as a percent of chunks
    // that can be freed to number of chunks used by VDisk
    ////////////////////////////////////////////////////////////////////////////
    bool HugeHeapDefragmentationRequired(
            const TOutOfSpaceState& oos,
            ui32 hugeCanBeFreedChunks,
            ui32 hugeTotalChunks,
            double defaultPercent) {

        if (hugeCanBeFreedChunks < 10)
            return false;

        double percentOfGarbage = static_cast<double>(hugeCanBeFreedChunks) / hugeTotalChunks;

        if (oos.GetLocalColor() > TSpaceColor::CYAN) {
            // For anything worse than CYAN
            return percentOfGarbage >= Min(0.02, defaultPercent);
        } else if (oos.GetLocalColor() > TSpaceColor::GREEN) {
            // For CYAN
            return percentOfGarbage >= Min(0.15, defaultPercent);
        } else {
            // For GREEN
            return percentOfGarbage >= Min(0.30, defaultPercent);
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // TDefragLocalScheduler
    // We use statistics about free space share and numbe of used/canBeFreed chunks
    // from Huge Heap to decide if to run defragmentation.
    // TODO: think about running compaction in case of inactivity
    ////////////////////////////////////////////////////////////////////////////
    class TDefragLocalScheduler : public TActorBootstrapped<TDefragLocalScheduler> {
        friend class TActorBootstrapped<TDefragLocalScheduler>;
        std::shared_ptr<TDefragCtx> DCtx;
        const TActorId DefragActorId;
        TActorId PlannerId;
        TDuration PauseMin = TDuration::Minutes(5);
        TDuration PauseMax = PauseMin + TDuration::Seconds(30);

        enum {
            EvResume = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        class TDefragPlannerActor : public TActorBootstrapped<TDefragPlannerActor> {
            std::shared_ptr<TDefragCtx> DCtx;
            TActorId ParentId;

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_DEFRAG_PLANNER;
            }

            TDefragPlannerActor(std::shared_ptr<TDefragCtx> dctx)
                : DCtx(std::move(dctx))
            {}

            void Bootstrap(const TActorId parentId) {
                STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD01, VDISKP(DCtx->VCtx->VDiskLogPrefix, "Bootstrap"));
                ParentId = parentId;
                Send(DCtx->SkeletonId, new TEvTakeHullSnapshot(false));
                Become(&TThis::StateFunc);
            }

            void Handle(TEvTakeHullSnapshotResult::TPtr ev) {
                TDefragCalcStat calcStat(std::move(ev->Get()->Snap), DCtx->HugeBlobCtx);
                std::unique_ptr<IEventBase> res;
                if (calcStat.Scan(NDefrag::MaxSnapshotHoldDuration)) {
                    STLOG(PRI_ERROR, BS_VDISK_DEFRAG, BSVDD05, VDISKP(DCtx->VCtx->VDiskLogPrefix, "scan timed out"));
                } else {
                    const ui32 totalChunks = calcStat.GetTotalChunks();
                    const ui32 usefulChunks = calcStat.GetUsefulChunks();
                    const auto& oos = DCtx->VCtx->GetOutOfSpaceState();
                    Y_ABORT_UNLESS(usefulChunks <= totalChunks);
                    const ui32 canBeFreedChunks = totalChunks - usefulChunks;
                    double defaultPercent = DCtx->VCfg->DefaultHugeGarbagePerMille / 1000.0;
                    if (HugeHeapDefragmentationRequired(oos, canBeFreedChunks, totalChunks, defaultPercent)) {
                        TChunksToDefrag chunksToDefrag = calcStat.GetChunksToDefrag(DCtx->MaxChunksToDefrag);
                        Y_ABORT_UNLESS(chunksToDefrag);
                        STLOG(PRI_INFO, BS_VDISK_DEFRAG, BSVDD03, VDISKP(DCtx->VCtx->VDiskLogPrefix, "scan finished"),
                            (TotalChunks, totalChunks), (UsefulChunks, usefulChunks),
                            (LocalColor, NKikimrBlobStorage::TPDiskSpaceColor_E_Name(oos.GetLocalColor())),
                            (ChunksToDefrag, chunksToDefrag.ToString()));
                        res = std::make_unique<TEvDefragStartQuantum>(std::move(chunksToDefrag));
                    } else {
                        STLOG(PRI_INFO, BS_VDISK_DEFRAG, BSVDD04, VDISKP(DCtx->VCtx->VDiskLogPrefix, "scan finished"),
                            (TotalChunks, totalChunks), (UsefulChunks, usefulChunks),
                            (LocalColor, NKikimrBlobStorage::TPDiskSpaceColor_E_Name(oos.GetLocalColor())));
                    }
                }
                if (!res) {
                    res = std::make_unique<TEvDefragStartQuantum>(TChunksToDefrag());
                }
                Send(ParentId, res.release());
                PassAway();
            }

            void PassAway() override {
                STLOG(PRI_DEBUG, BS_VDISK_DEFRAG, BSVDD02, VDISKP(DCtx->VCtx->VDiskLogPrefix, "PassAway"));
                TActorBootstrapped::PassAway();
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvTakeHullSnapshotResult, Handle);
                cFunc(TEvents::TSystem::Poison, PassAway);
            )
        };

        void RunDefragPlanner(const TActorContext &ctx) {
            Y_ABORT_UNLESS(!PlannerId);
            PlannerId = RunInBatchPool(ctx, new TDefragPlannerActor(DCtx));
        }

        TDuration GeneratePause() const {
            const TDuration delta = PauseMax - PauseMin;
            return PauseMin + TDuration::FromValue(RandomNumber<ui64>(delta.GetValue() + 1));
        }

        void Handle(TEvDefragStartQuantum::TPtr ev, const TActorContext& ctx) {
            Y_ABORT_UNLESS(ev->Sender == PlannerId);
            PlannerId = {};
            if (ev->Get()->ChunksToDefrag) {
                ctx.Send(new IEventHandle(DefragActorId, SelfId(), ev->ReleaseBase().Release()));
            } else {
                ctx.Schedule(GeneratePause(), new TEvents::TEvWakeup);
            }
        }

        void Bootstrap(const TActorContext &ctx) {
            Become(&TThis::StateFunc, ctx, TDuration::FromValue(RandomNumber<ui64>(PauseMin.GetValue() + 1)), new TEvents::TEvWakeup);
        }

        void Die(const TActorContext& ctx) override {
            if (PlannerId) {
                ctx.Send(new IEventHandle(TEvents::TSystem::Poison, 0, PlannerId, {}, nullptr, 0));
            }
            TActorBootstrapped::Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            CFunc(TEvents::TSystem::Poison, Die);
            CFunc(TEvents::TSystem::Wakeup, RunDefragPlanner);
            HFunc(TEvDefragStartQuantum, Handle);
            CFunc(TEvBlobStorage::EvVDefragResult, RunDefragPlanner);
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_DEFRAG_SCHEDULER;
        }

        TDefragLocalScheduler(const std::shared_ptr<TDefragCtx> &dCtx, const TActorId &defragActorId)
            : TActorBootstrapped<TDefragLocalScheduler>()
            , DCtx(dCtx)
            , DefragActorId(defragActorId)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TDefragActor
    ////////////////////////////////////////////////////////////////////////////
    class TDefragActor : public TActorBootstrapped<TDefragActor>
    {
        // defrag statistics
        using TStat = TEvDefragQuantumResult::TStat;

        // Task for database defrag
        struct TTask {
            std::variant<TEvBlobStorage::TEvVDefrag::TPtr, TEvDefragStartQuantum::TPtr> Request;
            TStat Stat;
            bool FirstQuantum = true; // true, if we run a first quantum with this task

            template<typename T>
            TTask(T&& req)
                : Request(std::forward<T>(req))
            {}
        };

        std::shared_ptr<TDefragCtx> DCtx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        ui64 TotalDefragRuns = 0;
        bool InProgress = false;
        std::deque<TTask> WaitQueue;
        TActiveActors ActiveActors;
        TSublog<TCircleBufStringStream<81920>> Sublog = { true };

        friend class TActorBootstrapped<TDefragActor>;

        void RunDefragIfAny(const TActorContext &ctx) {
            if (InProgress) {
                return;
            }

            if (WaitQueue.empty()) {
                return;
            }

            auto &task = WaitQueue.front();
            if (task.FirstQuantum) {
                task.FirstQuantum = false;
                Sublog.Log() << "=== Starting Defrag ===\n";
            }

            Sublog.Log() << "Defrag quantum started\n";
            ++TotalDefragRuns;
            InProgress = true;
            ActiveActors.Insert(ctx.Register(CreateDefragQuantumActor(DCtx,
                GInfo->GetVDiskId(DCtx->VCtx->ShortSelfVDisk),
                std::visit([](auto& r) { return GetChunksToDefrag(r); }, task.Request))), __FILE__, __LINE__,
                ctx, NKikimrServices::BLOBSTORAGE);
        }

        static std::optional<TChunksToDefrag> GetChunksToDefrag(TEvBlobStorage::TEvVDefrag::TPtr& /*ev*/) {
            return std::nullopt;
        }

        static std::optional<TChunksToDefrag> GetChunksToDefrag(TEvDefragStartQuantum::TPtr& ev) {
            return std::move(ev->Get()->ChunksToDefrag);
        }

        void Bootstrap(const TActorContext &ctx) {
            // create a local scheduler for defrag
            if (DCtx->RunDefragBySchedule) {
                auto scheduler = std::make_unique<TDefragLocalScheduler>(DCtx, ctx.SelfID);
                auto aid = ctx.Register(scheduler.release());
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            }
            Become(&TThis::StateFunc);
        }

        void Handle(TEvDefragQuantumResult::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            InProgress = false;
            Sublog.Log() << "Defrag quantum has been finished\n";

            auto *msg = ev->Get();
            Y_ABORT_UNLESS(msg->Stat.Eof || msg->Stat.FreedChunks.size() == DCtx->MaxChunksToDefrag);

            auto &task = WaitQueue.front();

            // update stat
            task.Stat.FoundChunksToDefrag += msg->Stat.FoundChunksToDefrag;
            task.Stat.RewrittenRecs += msg->Stat.RewrittenRecs;
            task.Stat.RewrittenBytes += msg->Stat.RewrittenBytes;
            task.Stat.Eof = msg->Stat.Eof;
            task.Stat.FreedChunks.insert(task.Stat.FreedChunks.end(), msg->Stat.FreedChunks.begin(), msg->Stat.FreedChunks.end());

            if (std::visit([&](auto& r) { return ProcessQuantumResult(r, task); }, task.Request)) {
                WaitQueue.pop_front();
                Sublog.Log() << "=== Defrag Finished ===\n";
            }

            RunDefragIfAny(ctx);
        }

        bool ProcessQuantumResult(TEvBlobStorage::TEvVDefrag::TPtr& ev, TTask& task) {
            const auto& record = ev->Get()->Record;
            auto reply = std::make_unique<TEvBlobStorage::TEvVDefragResult>(NKikimrProto::OK, record.GetVDiskID());
            reply->Record.SetFoundChunksToDefrag(task.Stat.FoundChunksToDefrag);
            reply->Record.SetRewrittenRecs(task.Stat.RewrittenRecs);
            reply->Record.SetRewrittenBytes(task.Stat.RewrittenBytes);
            reply->Record.SetEof(task.Stat.Eof);
            for (const auto& x : task.Stat.FreedChunks) {
                reply->Record.MutableFreedChunks()->Add(x.ChunkId);
            }
            Send(ev->Sender, reply.release());
            return task.Stat.Eof || !record.GetFull();
        }

        bool ProcessQuantumResult(TEvDefragStartQuantum::TPtr& ev, TTask& /*task*/) {
            Send(ev->Sender, new TEvBlobStorage::TEvVDefragResult);
            return true; // this is always final quantum
        }

        void Die(const TActorContext& ctx) override {
            ActiveActors.KillAndClear(ctx);
            TActorBootstrapped::Die(ctx);
        }

        void Handle(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            Sublog.Log() << "Generation changed\n";
            auto *msg = ev->Get();
            GInfo = msg->NewInfo;
        }

        void Handle(TEvDefragStartQuantum::TPtr ev, const TActorContext& ctx) {
            WaitQueue.emplace_back(ev);
            RunDefragIfAny(ctx);
        }

        void Handle(TEvBlobStorage::TEvVDefrag::TPtr &ev, const TActorContext &ctx) {
            Sublog.Log() << "Defrag request\n";
            WaitQueue.emplace_back(ev);
            RunDefragIfAny(ctx);
        }

        void Handle(TEvSublogLine::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            Sublog.Log() << ev->Get()->GetLine();
        }

        void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
            auto subrequest = ev->Get()->SubRequestId;
            Y_ABORT_UNLESS(subrequest == TDbMon::Defrag);
            TStringStream str;
            RenderHtml(str);
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), subrequest));
        }

        void RenderHtml(IOutputStream &str) const {
            HTML(str) {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        str << "Database Defrag";
                    }
                    DIV_CLASS("panel-body") {
                        TABLE_CLASS("table table-condensed") {
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() {str << "State";}
                                    TABLED() {
                                        if (InProgress) {
                                            THtmlLightSignalRenderer(NKikimrWhiteboard::EFlag::Yellow, "In progress")
                                                .Output(str);
                                        } else {
                                            THtmlLightSignalRenderer(NKikimrWhiteboard::EFlag::Green, "No defrag")
                                                .Output(str);
                                        }
                                    }
                                }
                                TABLER() {
                                    TABLED() {str << "Wait Queue Size";}
                                    TABLED() {str << WaitQueue.size(); }
                                }
                                TABLER() {
                                    TABLED() {str << "DCtx->RunDefragBySchedule";}
                                    TABLED() {str << DCtx->RunDefragBySchedule;}
                                }
                                TABLER() {
                                    TABLED() {str << "TotalDefragRuns";}
                                    TABLED() {str << TotalDefragRuns;}
                                }
                                TABLER() {
                                    TABLED() {str << "FreeSpaceShare/Threshold";}
                                    TABLED() {str << DCtx->VCtx->GetOutOfSpaceState().GetFreeSpaceShare();}
                                }
                                TABLER() {
                                    TABLED() {str << "CanBeFreed/Used Huge Heap Chunks";}
                                    TABLED() {
                                        auto stat = DCtx->VCtx->GetHugeHeapFragmentation().Get();
                                        str << stat.CanBeFreedChunks << " / " << stat.CurrentlyUsedChunks;
                                    }
                                }
                                TABLER() {
                                    TABLED() {str << "VDisk Used Chunks";}
                                    TABLED() {
                                        str << DCtx->VCtx->GetOutOfSpaceState().GetLocalUsedChunks();
                                    }
                                }
                            }
                        }

                        COLLAPSED_BUTTON_CONTENT("defragid", "Log") {
                            PRE() {str << Sublog.Get();}
                        }

                        // Full compaction button
                        str << "<a class=\"btn btn-primary btn-xs navbar-right\""
                            << " href=\"?type=dbmainpage&dbname=LogoBlobs"
                            << "&action=defrag\">Run Defrag</a>";
                    }
                }
            }
        }

        STRICT_STFUNC(StateFunc,
            CFunc(TEvents::TSystem::Poison, Die)
            HFunc(TEvVGenerationChange, Handle)
            HFunc(TEvBlobStorage::TEvVDefrag, Handle)
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvSublogLine, Handle)
            HFunc(TEvDefragStartQuantum, Handle)
            HFunc(TEvDefragQuantumResult, Handle)
        );

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_DEFRAG;
        }

        TDefragActor(const std::shared_ptr<TDefragCtx> &dCtx, const TIntrusivePtr<TBlobStorageGroupInfo> &info)
            : TActorBootstrapped<TDefragActor>()
            , DCtx(dCtx)
            , GInfo(info)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // CreateDefragActor
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateDefragActor(
            const std::shared_ptr<TDefragCtx> &dCtx,
            const TIntrusivePtr<TBlobStorageGroupInfo> &info) {
        return new TDefragActor(dCtx, info);
    }

} // NKikimr
