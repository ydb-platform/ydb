#include "vdisk_context.h"

namespace NKikimr {

    static TLogger ActorSystemLogger(TActorSystem *as) {
        Y_ABORT_UNLESS(as);
        auto logger = [as] (NLog::EPriority p, NLog::EComponent c, const TString &s) {
            LOG_LOG(*as, p, c, s);
        };
        return logger;
    }

    static TLogger DevNullLogger() {
        auto logger = [] (NLog::EPriority p, NLog::EComponent c, const TString &s) {
            Y_UNUSED(p);
            Y_UNUSED(c);
            Y_UNUSED(s);
        };
        return logger;
    }

    TVDiskContext::TVDiskContext(
                const TActorId &vdiskActorId,
                std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                const TIntrusivePtr<::NMonitoring::TDynamicCounters>& vdiskCounters,
                const TVDiskID &selfVDisk,
                TActorSystem *as, // as can be nullptr for tests
                NPDisk::EDeviceType type,
                bool donorMode,
                TReplQuoter::TPtr replPDiskReadQuoter,
                TReplQuoter::TPtr replPDiskWriteQuoter,
                TReplQuoter::TPtr replNodeRequestQuoter,
                TReplQuoter::TPtr replNodeResponseQuoter)
        : TBSProxyContext(vdiskCounters->GetSubgroup("subsystem", "memhull"))
        , VDiskActorId(vdiskActorId)
        , Top(std::move(top))
        , VDiskCounters(vdiskCounters)
        , VDiskMemCounters(vdiskCounters->GetSubgroup("subsystem", "memhull"))
        , Histograms(VDiskCounters, type)
        , IFaceMonGroup(std::make_shared<NMonGroup::TVDiskIFaceGroup>(VDiskCounters, "subsystem", "interface"))
        , GroupId(selfVDisk.GroupID)
        , ShortSelfVDisk(selfVDisk)
        , VDiskLogPrefix(GenerateVDiskLogPrefix(selfVDisk, donorMode))
        , NodeId(as ? as->NodeId : 0)
        , FreshIndex(VDiskMemCounters->GetCounter("MemTotal:FreshIndex"))
        , FreshData(VDiskMemCounters->GetCounter("MemTotal:FreshData"))
        , SstIndex(VDiskMemCounters->GetCounter("MemTotal:SstIndex"))
        , CompDataFresh(VDiskMemCounters->GetCounter("MemTotal:CompDataFresh"))
        , CompIndexFresh(VDiskMemCounters->GetCounter("MemTotal:CompIndexFresh"))
        , CompData(VDiskMemCounters->GetCounter("MemTotal:CompData"))
        , CompIndex(VDiskMemCounters->GetCounter("MemTotal:CompIndex"))
        , IteratorsCache(VDiskMemCounters->GetCounter("MemTotal:IteratorsCache"))
        , Replication(VDiskMemCounters->GetCounter("MemTotal:Replication"))
        , SyncLogCache(VDiskMemCounters->GetCounter("MemTotal:SyncLogCache"))
        , ActorSystem(as)
        , ReplPDiskReadQuoter(std::move(replPDiskReadQuoter))
        , ReplPDiskWriteQuoter(std::move(replPDiskWriteQuoter))
        , ReplNodeRequestQuoter(std::move(replNodeRequestQuoter))
        , ReplNodeResponseQuoter(std::move(replNodeResponseQuoter))
        , CostTracker()
        , OOSMonGroup(std::make_shared<NMonGroup::TOutOfSpaceGroup>(VDiskCounters, "subsystem", "oos"))
        , OutOfSpaceState(Top->GetTotalVDisksNum(), Top->GetOrderNumber(ShortSelfVDisk))
        , CostMonGroup(vdiskCounters, "subsystem", "cost")
        , Logger(as ? ActorSystemLogger(as) : DevNullLogger())
    {
        Y_ABORT_UNLESS(!VDiskLogPrefix.empty());
    }

    TString TVDiskContext::FormatMessage(
            NKikimrProto::EReplyStatus status,
            const TString &errorReason,
            NPDisk::TStatusFlags statusFlags,
            const TString &message)
    {
        TStringStream str;
        str << "Status# '" << NKikimrProto::EReplyStatus_Name(status)
            << "' StatusFlags# '" << NPDisk::StatusFlagsToString(statusFlags)
            << "' ErrorReason# '" << errorReason << "'";
        if (message) {
            str << " Message# '" << message << "'";
        }
        return str.Str();
    }

} // NKikimr
