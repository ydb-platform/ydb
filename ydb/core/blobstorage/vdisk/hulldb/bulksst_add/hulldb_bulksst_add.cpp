#include "hulldb_bulksst_add.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////////
    // TAddBulkSstEssence
    ////////////////////////////////////////////////////////////////////////////////
    void TAddBulkSstEssence::DestructiveMerge(TAddBulkSstEssence &&e) {
        if (!e.LogoBlobsAdditions.empty()) {
            Y_ABORT_UNLESS(LogoBlobsAdditions.empty());
            LogoBlobsAdditions = std::move(e.LogoBlobsAdditions);
        }
        if (!e.BlocksAdditions.empty()) {
            Y_ABORT_UNLESS(BlocksAdditions.empty());
            BlocksAdditions = std::move(e.BlocksAdditions);
        }
        if (!e.BarriersAdditions.empty()) {
            Y_ABORT_UNLESS(BarriersAdditions.empty());
            BarriersAdditions = std::move(e.BarriersAdditions);
        }
    }

    void TAddBulkSstEssence::SerializeToProto(NKikimrVDiskData::TAddBulkSstRecoveryLogRec &pb) const {
        for (const auto &x : LogoBlobsAdditions) {
            SerializeToProto(x, pb.AddLogoBlobsAdditions());
        }
        for (const auto &x : BlocksAdditions) {
            SerializeToProto(x, pb.AddBlocksAdditions());
        }
        for (const auto &x : BarriersAdditions) {
            SerializeToProto(x, pb.AddBarriersAdditions());
        }
    }

    void TAddBulkSstEssence::Output(IOutputStream &str) const {

        // output to stream one Db addition, works with different Db types
        auto renderOneDbAdditions = [&str] (auto &additions, const char *name) {
            str << name << "# ";
            FormatList(str, additions);
        };

        str << "TAddBulkSstEssence# {";
        renderOneDbAdditions(LogoBlobsAdditions, "LogoBlobs");
        str << " ";
        renderOneDbAdditions(BlocksAdditions, "Blocks");
        str << " ";
        renderOneDbAdditions(BarriersAdditions, "Barriers");
        str << "}";
    }

    TString TAddBulkSstEssence::ToString() const {
        TStringStream str;
        Output(str);
        return str.Str();
    }

    ////////////////////////////////////////////////////////////////////////////////
    // TEvAddBulkSst
    ////////////////////////////////////////////////////////////////////////////////
    TEvAddBulkSst::TEvAddBulkSst(
            TVector<ui32>&& reservedChunks,
            TVector<ui32>&& chunksToCommit,
            TLogoBlobsSstPtr &&oneLogoBlobsSst,
            ui32 oneLogoBlobsSstRecsNum)
        : ReservedChunks(std::move(reservedChunks))
        , ChunksToCommit(std::move(chunksToCommit))
        , Essence(std::move(oneLogoBlobsSst), oneLogoBlobsSstRecsNum)
    {}

    TString TEvAddBulkSst::Serialize(NPDisk::TCommitRecord &commitRecord) {
        // serialized message
        TString data;

        // serialize message
        NKikimrVDiskData::TAddBulkSstRecoveryLogRec logRec;
        Essence.SerializeToProto(logRec);
        bool success = logRec.SerializeToString(&data);
        Y_ABORT_UNLESS(success);

        // copy
        commitRecord.CommitChunks = ChunksToCommit;
        commitRecord.DeleteChunks = ReservedChunks;
        commitRecord.IsStartingPoint = false;
        commitRecord.FirstLsnToKeep = 0; // not set

        return data;
    }

} // NKikimr

