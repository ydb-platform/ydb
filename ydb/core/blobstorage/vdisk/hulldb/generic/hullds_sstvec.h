#pragma once

#include "defs.h"
#include "hullds_sst.h"
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hulloptlsn.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TOrderedLevelSegments
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct TOrderedLevelSegments : public TThrRefBase {
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef TIntrusivePtr<TLevelSegment> TLevelSegmentPtr;
        typedef typename TLevelSegment::TLevelSstPtr TLevelSstPtr;
        typedef TOrderedLevelSegments<TKey, TMemRec> TThis;
        typedef TVector<TLevelSegmentPtr> TSegments;

        TSegments Segments;

        TOrderedLevelSegments()
            : Segments()
        {}

        TOrderedLevelSegments(TVDiskContextPtr vctx, const TDiskPart *begin, const TDiskPart *end) {
            if (end - begin > 1)
                Segments.reserve(end - begin);

            for (const TDiskPart *pos = begin; pos < end; pos++)
                Segments.push_back(MakeIntrusive<TLevelSegment>(vctx, *pos));
        }

        TOrderedLevelSegments(TVDiskContextPtr vctx,
                              const NProtoBuf::RepeatedPtrField<NKikimrVDiskData::TDiskPart> &pb) {
            Segments.reserve(pb.size());
            for (const auto &x : pb) {
                Segments.push_back(MakeIntrusive<TLevelSegment>(vctx, x));
            }
        }

        TOrderedLevelSegments(const TLevelSegmentPtr &oneSeg)
            : Segments()
        {
            Segments.push_back(oneSeg);
        }

        TOrderedLevelSegments(typename TSegments::const_iterator first, typename TSegments::const_iterator last)
            : Segments()
        {
            for (typename TSegments::const_iterator i = first; i != last; ++i)
                Segments.push_back(*i);
        }

        struct TVectorLess {
            bool operator () (const TLevelSegmentPtr &segPtr, const TKey &key) const {
                return segPtr->FirstKey() < key;
            }
            bool operator () (const TKey &key, const TLevelSegmentPtr &segPtr) const {
                return key < segPtr->FirstKey();
            }
        };

        ui64 GetFirstLsn() const {
            Y_DEBUG_ABORT_UNLESS(!Empty());
            ui64 firstLsn = ui64(-1);
            for (const auto &x : Segments)
                firstLsn = Min(firstLsn, x->Info.FirstLsn);
            return firstLsn;
        }

        ui64 GetLastLsn() const {
            Y_DEBUG_ABORT_UNLESS(!Empty());
            ui64 lastLsn = 0;
            for (const auto &x : Segments)
                lastLsn = Max(lastLsn, x->Info.LastLsn);
            return lastLsn;
        }

        // append cur seg chunk ids (index and data) to the vector
        void FillInChunkIds(TVector<ui32> &vec) const {
            for (const auto &x : Segments)
                x->FillInChunkIds(vec);
        }

        void GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            for (const TLevelSegmentPtr& seg : Segments) {
                seg->GetOwnedChunks(chunks);
            }
        }

        void SerializeToProto(NKikimrVDiskData::TLevelX &pb) const {
            for (const auto &x : Segments) {
                auto sst = pb.AddSsts();
                x->SerializeToProto(*sst);
            }
        }

        bool Empty() const {
            return Segments.empty();
        }

        // how many chunks we own
        ui32 ChunksNum() const {
            ui32 num = 0;
            for (const auto s : Segments)
                num += s->AllChunks.size();
            return num;
        }

        TString ChunksToString() const {
            TString str;
            for (const auto &x : Segments) {
                str += x->ChunksToString();
            }
            return str;
        }

        bool IsLoaded() const {
            for (typename TSegments::const_iterator it = Segments.begin(), e = Segments.end(); it != e; ++it) {
                if (!(*it)->IsLoaded())
                    return false;
            }
            return true;
        }

        void Put(TLevelSegmentPtr &ptr) {
            Segments.push_back(ptr);
        }

        // NOTE: this is the obsolete way of discovering Last Compacted Lsn
        TOptLsn ObsoleteLastCompactedLsn() const {
            Y_ABORT_UNLESS(!Empty());
            TOptLsn lastLsn;
            for (const auto &x : Segments) {
                if (!x->Info.IsCreatedByRepl()) {
                    lastLsn.SetMax(x->Info.LastLsn);
                }
            }
            return lastLsn;
        }

        // number of elements in all ssts
        ui64 Elements() const {
            ui64 elements = 0;
            for (const auto &x : Segments) {
                elements += x->Elements();
            }
            return elements;
        }

        void OutputHtml(ui32 &index, ui32 level, IOutputStream &str, TIdxDiskPlaceHolder::TInfo &sum) const {
            for (typename TSegments::const_iterator it = Segments.begin(), e = Segments.end(); it != e; ++it) {
                (*it)->OutputHtml(index, level, str, sum);
            }
        }

        void OutputProto(ui32 level, google::protobuf::RepeatedPtrField<NKikimrVDisk::LevelStat> *rows) const {
            for (typename TSegments::const_iterator it = Segments.begin(), e = Segments.end(); it != e; ++it) {
                (*it)->OutputProto(level, rows);
            }
        }

        // dump all accessible data
        void DumpAll(IOutputStream &str) const {
            for (const auto &x : Segments)
                x->DumpAll(str);
        }

        struct TReadIterator;
        class TSstIterator;     // iterate via ssts
    };

    extern template struct TOrderedLevelSegments<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template struct TOrderedLevelSegments<TKeyBarrier, TMemRecBarrier>;
    extern template struct TOrderedLevelSegments<TKeyBlock, TMemRecBlock>;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TUnorderedLevelSegments
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct TUnorderedLevelSegments : public TThrRefBase {
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef TIntrusivePtr<TLevelSegment> TLevelSegmentPtr;
        typedef typename TLevelSegment::TLevelSstPtr TLevelSstPtr;
        typedef ::NKikimr::TOrderedLevelSegments<TKey, TMemRec> TOrderedLevelSegments;
        typedef TIntrusivePtr<TOrderedLevelSegments> TOrderedLevelSegmentsPtr;
        typedef ::NKikimr::TUnorderedLevelSegments<TKey, TMemRec> TThis;
        typedef TList<TLevelSegmentPtr> TSegments;

        TSegments Segments;
        ui32 Num;

        TUnorderedLevelSegments(TVDiskContextPtr)
            : Segments()
            , Num(0)
        {}

        TUnorderedLevelSegments(TVDiskContextPtr vctx,
                                const NProtoBuf::RepeatedPtrField<NKikimrVDiskData::TDiskPart> &pb)
            : Segments()
            , Num(0)
        {
            ui64 volatileOrderId = 0;
            for (const auto &x : pb) {
                auto sst = MakeIntrusive<TLevelSegment>(vctx, x);
                Put(sst, ++volatileOrderId);
            }
        }

        ui32 CurSstsNum() const {
            return Num;
        }

        void Put(const TLevelSegmentPtr &ptr, ui64 volatileOrderId) {
            Y_ABORT_UNLESS(Segments.empty() || Segments.back()->VolatileOrderId < volatileOrderId);
            ptr->VolatileOrderId = volatileOrderId;
            Segments.push_back(ptr);
            Num++;
        }

        void SerializeToProto(NKikimrVDiskData::TLevel0 &pb) const {
            Y_ABORT_UNLESS(Num == Segments.size());
            for (const auto &x : Segments) {
                auto sst = pb.AddSsts();
                x->SerializeToProto(*sst);
            }
        }

        bool Empty() const {
            return Segments.empty();
        }

        // how many chunks we own
        ui32 ChunksNum(ui32 numLimit) const {
            TSstIterator it(this, numLimit);
            it.SeekToFirst();
            ui32 num = 0;
            while (it.Valid()) {
                num += it.Get()->AllChunks.size();
                it.Next();
            }
            return num;
        }

        // NOTE: this is the obsolete way of discovering Last Compacted Lsn
        TOptLsn ObsoleteLastCompactedLsn() const {
            Y_ABORT_UNLESS(!Empty());
            TOptLsn lastLsn;
            for (const auto &x : Segments) {
                if (!x->Info.IsCreatedByRepl()) {
                    lastLsn.SetMax(x->Info.LastLsn);
                }
            }
            return lastLsn;
        }

        void OutputHtml(ui32 &index, IOutputStream &str, TIdxDiskPlaceHolder::TInfo &sum) const {
            for (const auto &x : Segments) {
                if (x)
                    x->OutputHtml(index, 0, str, sum);
            }
        }

        void OutputProto(google::protobuf::RepeatedPtrField<NKikimrVDisk::LevelStat> *rows) const {
            for (const auto &x : Segments) {
                if (x) {
                    x->OutputProto(0, rows);
                }
            }
        }

        void GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            for (const TLevelSegmentPtr& seg : Segments) {
                seg->GetOwnedChunks(chunks);
            }
        }

        // iterator through ssts
        class TSstIterator;
    };

    extern template struct TUnorderedLevelSegments<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template struct TUnorderedLevelSegments<TKeyBarrier, TMemRecBarrier>;
    extern template struct TUnorderedLevelSegments<TKeyBlock, TMemRecBlock>;

} // NKikimr
