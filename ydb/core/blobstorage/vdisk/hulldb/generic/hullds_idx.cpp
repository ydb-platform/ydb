#include "hullds_idx.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

    TLevelIndexBase::TLevelIndexBase(const TLevelIndexSettings &settings)
        : Settings(settings)
        , StartTime(TAppData::TimeProvider->Now())
    {}

    const char *TLevelIndexBase::LevelCompStateToStr(ELevelCompState s) {
        switch (s) {
            case StateNoComp:           return "No Compaction";
            case StateCompPolicyAtWork: return "Policy At Work";
            case StateCompInProgress:   return "Compaction In Progress";
            case StateWaitCommit:       return "Committing";
            default:                    return "UNKNOWN";
        }
    }

    TLevelIndexBase::ELevelCompState TLevelIndexBase::GetCompState() const {
        return LevelCompState;
    }

    TString TLevelIndexBase::GetCompStateStr(ui32 readsInFlight, ui32 writesInFlight) const {
        TStringStream str;
        str << LevelCompStateToStr(LevelCompState) << " R:" << readsInFlight << "/W:" << writesInFlight << " "
            << ToStringLocalTimeUpToSeconds(StartTime);
        return str.Str();
    }

    void TLevelIndexBase::SetCompState(ELevelCompState state) {
        LevelCompState = state;
        StartTime = TAppData::TimeProvider->Now();
    }

    bool TLevelIndexBase::CheckEntryPoint(const char *begin, const char *end, size_t keySizeOf, ui32 signature,
                                          TString &explanation) {
        // OLD Data Format
        // data ::= [signature=4b] [nextSstId=8b] deleted_chunks level0 otherLevels;
        // deleted_chunks ::= [size=4b] [chunkIdx=4b]*
        // level0 ::= [size=4b] [TDiskPart=12b] * size
        // otherLevels ::= levelX *
        // levelX ::= [lastCompacted=keySizeOfb] [size=4b] [TDiskPart=12b] * size
        const char *pos = begin;

        // check signature
        if (size_t(end - pos) < sizeof(ui32)) {
            explanation = "signature size";
            return false;
        }
        if (*(const ui32*)pos != signature) {
            TStringStream str;
            str << "signature value mismatch: expected=" << signature << " got=" << *(const ui32*)pos;
            explanation = str.Str();
            return false;
        }
        pos += sizeof(ui32);

        // skip next SST index
        pos += sizeof(ui64);

        // check deleted_chunks
        if (size_t(end - pos) < sizeof(ui32)) {
            explanation = "deleted chunks size";
            return false;
        }
        ui32 chunks = *(const ui32*)pos;
        pos += sizeof(ui32);
        if (size_t(end - pos) < sizeof(ui32) * chunks) {
            explanation = "deleted chunks array size";
            return false;
        }
        pos += sizeof(ui32) * chunks;

        // check level0
        {
            if (size_t(end - pos) < sizeof(ui32)) {
                explanation = "level0 size";
                return false;
            }

            ui32 n = *(const ui32 *)(pos);
            const char *levelBegin = pos + sizeof(ui32);
            const char *levelEnd = levelBegin + n * sizeof(TDiskPart);
            if (levelEnd > end) {
                explanation = "level0 array size";
                return false;
            }

            pos = levelEnd;
        }

        // check levels
        while (pos != end) {
            if (size_t(end - pos) < keySizeOf + sizeof(ui32)) {
                explanation = "levelX size";
                return false;
            }

            pos += keySizeOf;
            ui32 n = *(const ui32 *)(pos);
            const char *levelBegin = pos + sizeof(ui32);
            const char *levelEnd = levelBegin + n * sizeof(TDiskPart);
            if (levelEnd > end) {
                explanation = "levelX array size";
                return false;
            }

            pos = levelEnd;
        }

        return true;
    }

    void TLevelIndexBase::ConvertToProto(NKikimrVDiskData::TLevelIndex &pb,
                                         const char *begin,
                                         const char *end,
                                         size_t keySizeOf) {
        const char *pos = begin;
        pos += sizeof(ui32); // signature

        // next SST index
        ui64 nextSstId = *(const ui64*)pos;
        pb.SetNextSstId(nextSstId);
        pos += sizeof(ui64);

        // convert deleted_chunks
        ui32 chunks = *(const ui32*)pos;
        pos += sizeof(ui32); // array size
        for (ui32 i = 0; i < chunks; i++) {
            pb.AddDeletedChunks(*reinterpret_cast<const ui32 *>(pos));
            pos += sizeof(ui32);
        }

        // convert level0
        {
            auto level0 = pb.MutableLevel0();
            ui32 n = *(const ui32 *)(pos);
            pos += sizeof(ui32); // array size
            for (ui32 i = 0; i < n; i++) {
                auto p = level0->AddSsts();
                const TDiskPart *diskPart = (const TDiskPart *)pos;
                diskPart->SerializeToProto(*p);
                pos += sizeof(TDiskPart);
            }
        }

        // convert other levels
        while (pos != end) {
            auto levelX = pb.AddOtherLevels();
            levelX->MutableLastCompacted()->append(pos, keySizeOf);
            pos += keySizeOf;
            ui32 n = *(const ui32 *)(pos);
            pos += sizeof(ui32);

            for (ui32 i = 0; i < n; i++) {
                auto p = levelX->AddSsts();
                const TDiskPart *diskPart = (const TDiskPart *)pos;
                diskPart->SerializeToProto(*p);
                pos += sizeof(TDiskPart);
            }
        }
    }


    bool TLevelIndexBase::CheckBulkFormedSegments(const char *begin, const char *end) {
        // empty string is correct bulk-formed segment set
        if (begin == end)
            return true;

        TMemoryInput stream(begin, end - begin);
        return NKikimrVDiskData::TBulkFormedSstInfoSet().ParseFromArcadiaStream(&stream);
    }

    template <class TKey, class TMemRec>
    void TLevelIndex<TKey, TMemRec>::OutputHtml(const TString &name, IOutputStream &str) const
    {
        HTML(str) {
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << name;
                    // output main db page
                    str << "<a class=\"btn btn-primary btn-xs navbar-right\""
                        << " href=\"?type=dbmainpage&dbname=" << name << "\">Database Main Page</a>";
                    // stat db
                    str << "<a class=\"btn btn-primary btn-xs navbar-right\""
                        << " href=\"?type=stat&dbname=" << name << "\">Database Stat</a>";
                    // dump db
                    str << "<a class=\"btn btn-primary btn-xs navbar-right\""
                        << " href=\"?type=dump&dbname=" << name << "\">Dump Database</a>";
                    OutputHugeStatButton(name, str);
                    OutputQueryDbButton(name, str);
                }
                DIV_CLASS("panel-body") {
                    DIV_CLASS("row") {
                        DIV_CLASS("col-md-1") {
                            TAG(TH5) { str << "Fresh"; }
                            const char *comp = Fresh.CompactionInProgress() ? "Compaction In Progress" : "No Compaction";
                            H6_CLASS ("text-info") {str << comp << " W:" << FreshCompWritesInFlight;}
                        }
                        DIV_CLASS("col-md-11") {Fresh.OutputHtml(str);}
                    }
                    DIV_CLASS("row") {
                        DIV_CLASS("col-md-1") {
                            TAG(TH5) { str << "Levels"; }
                            H6_CLASS ("text-info") {str << GetCompStateStr(HullCompReadsInFlight, HullCompWritesInFlight);}
                        }
                        DIV_CLASS("col-md-11") {CurSlice->OutputHtml(str);}
                    }

                }
            }
        }
    }
    template <class TKey, class TMemRec>
    void TLevelIndex<TKey, TMemRec>::OutputProto(NKikimrVDisk::LevelIndexStat *stat) const {
        NKikimrVDisk::FreshStat *fresh = stat->mutable_fresh();
        fresh->set_compaction_writes_in_flight(FreshCompWritesInFlight);
        Fresh.OutputProto(fresh);
        NKikimrVDisk::SliceStat *slice = stat->mutable_slice();
        slice->set_compaction_writes_in_flight(HullCompWritesInFlight);
        slice->set_compaction_reads_in_flight(HullCompReadsInFlight);
        CurSlice->OutputProto(slice->mutable_levels());
    }

    template class TLevelIndex<TKeyLogoBlob, TMemRecLogoBlob>;
    template class TLevelIndex<TKeyBarrier, TMemRecBarrier>;
    template class TLevelIndex<TKeyBlock, TMemRecBlock>;

} // NKikimr
