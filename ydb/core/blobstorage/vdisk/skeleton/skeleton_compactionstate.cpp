#include "skeleton_compactionstate.h"

#include <ydb/core/blobstorage/base/html.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

    TVDiskCompactionState::TVDiskCompactionState(
            TActorId logoBlobsActorId,
            TActorId blocksActorId,
            TActorId barriersActorId)
        : LogoBlobsActorId(logoBlobsActorId)
        , BlocksActorId(blocksActorId)
        , BarriersActorId(barriersActorId)
    {}

    void TVDiskCompactionState::SendLocalCompactCmd(const TActorContext &ctx, TCompactionReq cReq) {
        ui64 requestId = ++RequestIdCounter;
        const auto mode = cReq.Mode;
        auto insRes = Requests.insert({requestId, std::move(cReq)});
        Y_ABORT_UNLESS(insRes.second);
        auto &req = insRes.first->second;

        if (req.CompactLogoBlobs) {
            ctx.Send(LogoBlobsActorId, new TEvHullCompact(EHullDbType::LogoBlobs, requestId, mode, req.TablesToCompact));
        }
        if (req.CompactBlocks) {
            ctx.Send(BlocksActorId, new TEvHullCompact(EHullDbType::Blocks, requestId, mode, req.TablesToCompact));
        }
        if (req.CompactBarriers) {
            ctx.Send(BarriersActorId, new TEvHullCompact(EHullDbType::Barriers, requestId, mode, req.TablesToCompact));
        }
    }

    void TVDiskCompactionState::Setup(const TActorContext &ctx, std::optional<ui64> lsn, TCompactionReq cReq) {
        Y_ABORT_UNLESS(!cReq.AllDone());
        if (!lsn && WaitQueue.empty()) {
            SendLocalCompactCmd(ctx, std::move(cReq));
        } else {
            WaitQueue.emplace_back(lsn.value_or(0), std::move(cReq));
        }
    }

    void TVDiskCompactionState::Compacted(
            const TActorContext &ctx,
            i64 reqId,
            EHullDbType dbType,
            const TIntrusivePtr<TVDiskContext>& vCtx) {
        auto it = Requests.find(reqId);
        Y_ABORT_UNLESS(it != Requests.end());
        auto &req = it->second;

        switch (dbType) {
            case EHullDbType::LogoBlobs:  req.CompactLogoBlobs = false; break;
            case EHullDbType::Blocks:     req.CompactBlocks = false; break;
            case EHullDbType::Barriers:   req.CompactBarriers = false; break;
            default: Y_ABORT("Unexpected case: %d", int(dbType));
        }

        if (req.AllDone()) {
            SendVDiskResponse(ctx, req.ClientId, req.Reply.release(), req.ClientCookie, vCtx);
            // delete req from Request, we handled it
            Requests.erase(it);
        }
    }

    void TVDiskCompactionState::RenderHtml(IOutputStream &str, TDbMon::ESubRequestID subId) const {
        struct {
            size_t ReqsInWaitQueue = 0;
            size_t ReqsInProgress = 0;
        } info;
        auto extractLogoBlobs = [] (const TCompactionReq &req) { return req.CompactLogoBlobs; };
        auto extractBlocks = [] (const TCompactionReq &req) { return req.CompactBlocks; };
        auto extractBarriers = [] (const TCompactionReq &req) { return req.CompactBarriers; };
        auto increment = [&info] (bool value, bool isWaitQueue) {
            if (value) {
                if (isWaitQueue)
                    ++info.ReqsInWaitQueue;
                else
                    ++info.ReqsInProgress;
            }
        };

        auto traverse = [&](const auto& extract) {
            for (const auto& pair : Requests) {
                increment(extract(pair.second), false);
            }
            for (const auto& [waitingLsn, req] : WaitQueue) {
                increment(extract(req), true);
            }
        };

        // fill in info structure for the database identified by subId
        switch (subId) {
            case TDbMon::DbMainPageLogoBlobs:   traverse(extractLogoBlobs); break;
            case TDbMon::DbMainPageBlocks:      traverse(extractBlocks); break;
            case TDbMon::DbMainPageBarriers:    traverse(extractBarriers); break;
            default: Y_ABORT("Unxepected case");
        }

        // convert subId to database name
        auto getDbName = [] (TDbMon::ESubRequestID subId) {
            switch (subId) {
                case TDbMon::DbMainPageLogoBlobs:   return "LogoBlobs";
                case TDbMon::DbMainPageBlocks:      return "Blocks";
                case TDbMon::DbMainPageBarriers:    return "Barriers";
                default: Y_ABORT("Unxepected case");
            }
        };

        HTML(str) {
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Database Full Compaction";
                }
                DIV_CLASS("panel-body") {
                    TABLE_CLASS("table table-condensed") {
                        TABLEBODY() {
                            TABLER() {
                                TABLED() {str << "State";}
                                TABLED() {
                                    if (info.ReqsInWaitQueue + info.ReqsInProgress) {
                                        THtmlLightSignalRenderer(NKikimrWhiteboard::EFlag::Yellow, "In progress")
                                            .Output(str);
                                    } else {
                                        THtmlLightSignalRenderer(NKikimrWhiteboard::EFlag::Green, "No compaction")
                                            .Output(str);
                                    }
                                }
                            }
                            TABLER() {
                                TABLED() {str << "Commit Wait Queue";}
                                TABLED() {str << info.ReqsInWaitQueue; }
                            }
                            TABLER() {
                                TABLED() {str << "Requests in progress";}
                                TABLED() {str << info.ReqsInProgress; }
                            }
                        }
                    }

                    // Full compaction button
                    str << "<a class=\"btn btn-primary btn-xs navbar-right\""
                        << " href=\"?type=dbmainpage&dbname=" << getDbName(subId)
                        << "&action=compact\">Run Full Compaction</a>";
                }
            }
        }
    }

} // NKikimr
