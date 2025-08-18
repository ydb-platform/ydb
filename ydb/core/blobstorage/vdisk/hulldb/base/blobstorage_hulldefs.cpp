#include "blobstorage_hulldefs.h"
#include <ydb/core/base/blobstorage_grouptype.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // THullCtx
    ////////////////////////////////////////////////////////////////////////////
    void THullCtx::UpdateSpaceCounters(const NHullComp::TSstRatio& prev, const NHullComp::TSstRatio& current) {
         LsmHullSpaceGroup.DskSpaceCurIndex()         += current.IndexBytesTotal   - prev.IndexBytesTotal;
         LsmHullSpaceGroup.DskSpaceCurInplacedData()  += current.InplacedDataTotal - prev.InplacedDataTotal;
         LsmHullSpaceGroup.DskSpaceCurHugeData()      += current.HugeDataTotal     - prev.HugeDataTotal;
         LsmHullSpaceGroup.DskSpaceCompIndex()        += current.IndexBytesKeep    - prev.IndexBytesKeep;
         LsmHullSpaceGroup.DskSpaceCompInplacedData() += current.InplacedDataKeep  - prev.InplacedDataKeep;
         LsmHullSpaceGroup.DskSpaceCompHugeData()     += current.HugeDataKeep      - prev.HugeDataKeep;
    }

    ////////////////////////////////////////////////////////////////////////////
    // TPutRecoveryLogRecOpt
    ////////////////////////////////////////////////////////////////////////////
    static_assert(sizeof(TLogoBlobID) == 24, "TLogoBlobID size has changed");

    TString TPutRecoveryLogRecOpt::Serialize(const TBlobStorageGroupType &gtype, const TLogoBlobID &id,
            const TRope &rope, bool issueKeepFlag) {
        Y_ABORT_UNLESS(id.PartId() && rope.GetSize() == gtype.PartSize(id),
            "id# %s rope.GetSize()# %zu", id.ToString().data(), rope.GetSize());

        TString res = TString::Uninitialized(sizeof(id) + rope.GetSize() + issueKeepFlag);
        char* const begin = res.Detach();
        char *buf = begin;
        memcpy(buf, id.GetRaw(), sizeof(TLogoBlobID));
        buf += sizeof(TLogoBlobID);
        rope.Begin().ExtractPlainDataAndAdvance(buf, rope.GetSize());
        buf += rope.GetSize();
        if (issueKeepFlag) {
            *buf++ = 1;
        }
        Y_ABORT_UNLESS(buf == begin + res.size());
        return res;
    }

    TRcBuf TPutRecoveryLogRecOpt::SerializeZeroCopy(const TBlobStorageGroupType &gtype, const TLogoBlobID &id,
            TRope &&rope, bool issueKeepFlag) {
        rope.Compact(sizeof(TLogoBlobID), issueKeepFlag);
        return SerializeZeroCopy(gtype, id, TRcBuf(rope), issueKeepFlag);
    }

    TRcBuf TPutRecoveryLogRecOpt::SerializeZeroCopy(const TBlobStorageGroupType &gtype, const TLogoBlobID &id,
            TRcBuf &&data, bool issueKeepFlag) {
        Y_ABORT_UNLESS(id.PartId() && data.GetSize() == gtype.PartSize(id),
            "id# %s rope.GetSize()# %zu", id.ToString().data(), data.GetSize());

        data.GrowFront(sizeof(id));
        memcpy(data.UnsafeGetDataMut(), id.GetRaw(), sizeof(id));
        if (issueKeepFlag) {
            data.GrowBack(1);
            *(char*)(data.UnsafeGetDataMut() + data.size() - 1) = 1;
        }

        return data;
    }

    bool TPutRecoveryLogRecOpt::ParseFromArray(const TBlobStorageGroupType &gtype,
                                               const char* data,
                                               size_t size) {
        const char *pos = data;
        const char *end = data + size;
        if (size_t(end - pos) < 24)
            return false;

        const ui64 *raw = (const ui64 *)pos;
        Id = TLogoBlobID(raw[0], raw[1], raw[2]);
        pos += 24;

        IssueKeepFlag = false;

        ui64 partSize = gtype.PartSize(Id);

        if (size_t(end - pos) < partSize)
            return false;
        Data = TString(pos, partSize);

        pos += partSize;
        if (pos + 1 == end) {
            IssueKeepFlag = true;
            ++pos;
        }

        if (pos != end) {
            return false;
        }

        return true;
    }

    bool TPutRecoveryLogRecOpt::ParseFromString(const TBlobStorageGroupType &gtype,
                                                const TString &data) {
        return ParseFromArray(gtype, data.data(), data.size());
    }

    TString TPutRecoveryLogRecOpt::ToString() const {
        TStringStream str;
        Output(str);
        return str.Str();
    }

    void TPutRecoveryLogRecOpt::Output(IOutputStream &str) const {
        str << "{Id# " << Id << " IssueKeepFlag# " << IssueKeepFlag << "}";
    }

    THullCtx::THullCtx(TVDiskContextPtr vctx, const TIntrusivePtr<TVDiskConfig> vcfg, ui32 chunkSize, ui32 compWorthReadSize,
            bool freshCompaction, bool gcOnlySynced, bool allowKeepFlags, bool barrierValidation, ui32 hullSstSizeInChunksFresh,
            ui32 hullSstSizeInChunksLevel, double hullCompFreeSpaceThreshold, double hullCompReadBatchEfficiencyThreshold,
            TDuration hullCompStorageRatioCalcPeriod, TDuration hullCompStorageRatioMaxCalcDuration, bool addHeader,
            ui32 hullCompLevel0MaxSstsAtOnce, ui32 hullCompSortedPartsNum)
        : VCtx(std::move(vctx))
        , VCfg(vcfg)
        , IngressCache(TIngressCache::Create(VCtx->Top, VCtx->ShortSelfVDisk))
        , ChunkSize(chunkSize)
        , CompWorthReadSize(compWorthReadSize)
        , FreshCompaction(freshCompaction)
        , GCOnlySynced(gcOnlySynced)
        , AllowKeepFlags(allowKeepFlags)
        , BarrierValidation(barrierValidation)
        , HullSstSizeInChunksFresh(hullSstSizeInChunksFresh)
        , HullSstSizeInChunksLevel(hullSstSizeInChunksLevel)
        , HullCompFreeSpaceThreshold(hullCompFreeSpaceThreshold)
        , HullCompReadBatchEfficiencyThreshold(hullCompReadBatchEfficiencyThreshold)
        , HullCompStorageRatioCalcPeriod(hullCompStorageRatioCalcPeriod)
        , HullCompStorageRatioMaxCalcDuration(hullCompStorageRatioMaxCalcDuration)
        , AddHeader(addHeader)
        , HullCompLevel0MaxSstsAtOnce(hullCompLevel0MaxSstsAtOnce)
        , HullCompSortedPartsNum(hullCompSortedPartsNum)
        , CompactionStrategyGroup(VCtx->VDiskCounters, "subsystem", "compstrategy")
        , LsmHullGroup(VCtx->VDiskCounters, "subsystem", "lsmhull")
        , LsmHullSpaceGroup(VCtx->VDiskCounters, "subsystem", "outofspace")
    {}

} // NKikimr
