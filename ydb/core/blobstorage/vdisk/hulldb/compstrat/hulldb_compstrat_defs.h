#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sstslice.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_leveledssts.h>

namespace NKikimr {
    namespace NHullComp {

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::EAction
        ////////////////////////////////////////////////////////////////////////////
        enum EAction {
            ActNothing,             // nothing to compact
            ActDeleteSsts,          // delete some ssts w/o read/write of chunks
            ActMoveSsts,            // move some ssts between levels w/o read/write of chunks
            ActCompactSsts          // run heavy compaction (read/write chunks)
        };

        inline const char *ActionToStr(EAction act) {
            switch (act) {
                case ActNothing:        return "Nothing";
                case ActDeleteSsts:     return "DeleteSsts";
                case ActMoveSsts:       return "MoveSsts";
                case ActCompactSsts:    return "CompactSsts";
                default:                return "UNKNOWN";
            }
        }

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TFullCompactionAttrs
        // Contains full compaction attributes, which are used to understand
        // * what to compact
        // * when to stop
        ////////////////////////////////////////////////////////////////////////////
        struct TFullCompactionAttrs {
            // compact all ssts with lsns under this one
            ui64 FullCompactionLsn = 0;
            // time compaction request was issued
            TInstant CompactionStartTime;

            TFullCompactionAttrs(ui64 lsn, TInstant startTime)
                : FullCompactionLsn(lsn)
                , CompactionStartTime(startTime)
            {}

            bool operator==(const TFullCompactionAttrs &attrs) const {
                return FullCompactionLsn == attrs.FullCompactionLsn
                    && CompactionStartTime == attrs.CompactionStartTime;
            }
        };


        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TTask
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        struct TTask {
            using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
            using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;
            using TSegments = TVector<TLevelSegmentPtr>;
            using TOrderedLevelSegments = ::NKikimr::TOrderedLevelSegments<TKey, TMemRec>;
            using TOrderedLevelSegmentsPtr = TIntrusivePtr<TOrderedLevelSegments>;
            using TCompactionChains = TVector<TOrderedLevelSegmentsPtr>;
            using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;
            using TLeveledSsts = ::NKikimr::TLeveledSsts<TKey, TMemRec>;
            using TLeveledSstsIterator = typename TLeveledSsts::TIterator;

            ////////////////////////////////////////////////////////////////////////
            struct TBase {
                // ssts to delete from slice
                TLeveledSsts TablesToDelete;
                // ssts to add to slice
                TLeveledSsts TablesToAdd;
                // huge blobs to delete
                TDiskPartVec HugeBlobsToDelete;
                // is data finalized
                bool Finalized = false;

                virtual ~TBase() = default;

                void Clear() {
                    TablesToDelete.Clear();
                    TablesToAdd.Clear();
                    HugeBlobsToDelete.Clear();
                    Finalized = false;
                }

                const TLeveledSsts &GetSstsToDelete() const {
                    Y_ABORT_UNLESS(Finalized);
                    return TablesToDelete;
                }

                const TLeveledSsts &GetSstsToAdd() const {
                    Y_ABORT_UNLESS(Finalized);
                    return TablesToAdd;
                }

                const TDiskPartVec &GetHugeBlobsToDelete() const {
                    Y_ABORT_UNLESS(Finalized);
                    return HugeBlobsToDelete;
                }

                TDiskPartVec ExtractHugeBlobsToDelete() {
                    Y_ABORT_UNLESS(Finalized);
                    return std::move(HugeBlobsToDelete);
                }

                void Finalize() {
                    TablesToDelete.Sort();
                    TablesToAdd.Sort();
                    Finalized = true;
                }

                virtual bool CollectDeletedSsts() const = 0;

                virtual void Output(IOutputStream &str) const {
                    if (!TablesToDelete.Empty()) {
                        TLeveledSstsIterator it(&TablesToDelete);
                        it.SeekToFirst();
                        str << " DEL:";
                        while (it.Valid()) {
                            str << " " << it.Get().ToString();
                            it.Next();
                        }
                    }
                    if (!TablesToAdd.Empty()) {
                        TLeveledSstsIterator it(&TablesToAdd);
                        it.SeekToFirst();
                        str << " ADD:";
                        while (it.Valid()) {
                            str << " " << it.Get().ToString();
                            it.Next();
                        }
                    }
                    if (!HugeBlobsToDelete.Empty()) {
                        str << " HUGEBLOBS: " << HugeBlobsToDelete.Size();
                    }
                }
            };

            ////////////////////////////////////////////////////////////////////////
            struct TDeleteSsts : public TBase {
                using TBase::TablesToDelete;
                using TBase::TablesToAdd;
                using TBase::HugeBlobsToDelete;

                void Clear() {
                    TBase::Clear();
                }

                bool CollectDeletedSsts() const override { return true; }

                // for complete sst deletion
                void DeleteSst(ui32 level, TLevelSegmentPtr s) {
                    // find huge blobs that this sst stores and put them into HugeBlobsToDelete
                    FindHugeBlobsForRemoval(s.Get());
                    // put Sst itself
                    TablesToDelete.PushBack(TLevelSstPtr(level, s));
                }

            private:
                void FindHugeBlobsForRemoval(const TLevelSegment *seg) {
                    typename TLevelSegment::TMemIterator it(seg);
                    it.SeekToFirst();
                    TDiskDataExtractor extr;
                    while (it.Valid()) {
                        it.GetDiskData(&extr);
                        if (extr.BlobType == TBlobType::HugeBlob || extr.BlobType == TBlobType::ManyHugeBlobs) {
                            for (const TDiskPart *part = extr.Begin; part < extr.End; ++part) {
                                if (!part->Empty()) {
                                    HugeBlobsToDelete.PushBack(*part);
                                }
                            }
                        }
                        extr.Clear();
                        it.Next();
                    }
                }
            };

            ////////////////////////////////////////////////////////////////////////
            struct TMoveSsts : public TBase {
                using TBase::TablesToDelete;
                using TBase::TablesToAdd;
                using TBase::HugeBlobsToDelete;

                void Clear() {
                    TBase::Clear();
                }

                bool CollectDeletedSsts() const override { return false; }

                void MoveSst(ui32 fromLevel, ui32 toLevel, TLevelSegmentPtr s) {
                    TablesToDelete.PushBack(TLevelSstPtr(fromLevel, s));
                    TablesToAdd.PushBack(TLevelSstPtr(toLevel, s));
                }
            };

            ////////////////////////////////////////////////////////////////////////
            struct TCompactSsts : public TBase {
                using TBase::TablesToDelete;
                using TBase::TablesToAdd;
                using TBase::HugeBlobsToDelete;

                ui32 TargetLevel = (ui32)(-1);
                TKey LastCompactedKey = TKey::First();
                // vector of TOrderedLevelSegmentsPtr to compact
                TCompactionChains CompactionChains;

                void Clear() {
                    TBase::Clear();
                    TargetLevel = (ui32)(-1);
                    LastCompactedKey = TKey::First();
                    CompactionChains.clear();
                }

                bool CollectDeletedSsts() const override { return true; }

                // for levels starting from 1
                void PushSstFromLevelX(ui32 level, typename TSegments::const_iterator first,
                                       typename TSegments::const_iterator last) {
                    ui32 tables = 0;
                    for (typename TSegments::const_iterator i = first; i != last; ++i) {
                        TablesToDelete.PushBack(TLevelSstPtr(level, *i));
                        tables++;
                    }

                    if (tables) {
                        TOrderedLevelSegmentsPtr chain(new TOrderedLevelSegments(first, last));
                        CompactionChains.push_back(chain);
                    }
                }

                // for pushing one sst (level0 or partially sorted level)
                void PushOneSst(ui32 level, TLevelSegmentPtr s) {
                    TablesToDelete.PushBack(TLevelSstPtr(level, s));
                    TOrderedLevelSegmentsPtr chain(new TOrderedLevelSegments(s));
                    CompactionChains.push_back(chain);
                }

                void CompactionFinished(
                        TOrderedLevelSegmentsPtr &&segVec,
                        TDiskPartVec &&hugeBlobsToDelete,
                        bool aborted)
                {
                    if (aborted) {
                        // no change at all
                        TablesToDelete.Clear();
                        TablesToAdd.Clear();
                        HugeBlobsToDelete.Clear();
                    } else {
                        Y_ABORT_UNLESS(!TablesToDelete.Empty());
                        HugeBlobsToDelete = std::move(hugeBlobsToDelete);
                        if (segVec) {
                            TLeveledSsts tmp(TargetLevel, *segVec);
                            TablesToAdd.Swap(tmp);
                            TablesToAdd.Sort();
                        } else {
                            TablesToAdd.Clear();
                        }
                    }
                }

                void Output(IOutputStream &str) const override {
                    str << " TARGET: " << TargetLevel;
                    str << " LastCompacted: " << LastCompactedKey.ToString();
                    TBase::Output(str);
                }

                TString ToString() const {
                    TStringStream str;
                    Output(str);
                    return str.Str();
                }

                void Finalize() {
                    TBase::Finalize();
                    Y_ABORT_UNLESS(TargetLevel != (ui32)(-1));
                }
            };

            ////////////////////////////////////////////////////////////////////////

            EAction Action;
            TDeleteSsts DeleteSsts;
            TMoveSsts MoveSsts;
            TCompactSsts CompactSsts;
            bool IsFullCompaction = false;
            // this field contains
            // * original std::optional<TFullCompactionAttrs>
            // * if 'first' was set, than result of full compaction: second=true -- full compaction has been finished
            std::pair<std::optional<TFullCompactionAttrs>, bool> FullCompactionInfo;

            TTask() {
                Clear();
            }

            void Clear() {
                SetupAction(ActNothing);
                DeleteSsts.Clear();
                MoveSsts.Clear();
                CompactSsts.Clear();
                IsFullCompaction = false;
                FullCompactionInfo.first.reset();
                FullCompactionInfo.second = false;
            }

            void SetupAction(EAction act) {
                Action = act;
                if (auto ptr = const_cast<TBase*>(GetPtr()))
                    ptr->Finalize();
            }

            const TLeveledSsts &GetSstsToDelete() const {
                return GetPtr()->GetSstsToDelete();
            }

            const TLeveledSsts &GetSstsToAdd() const {
                return GetPtr()->GetSstsToAdd();
            }

            const TDiskPartVec &GetHugeBlobsToDelete() const {
                return GetPtr()->GetHugeBlobsToDelete();
            }

            TDiskPartVec ExtractHugeBlobsToDelete() {
                return const_cast<TBase*>(GetPtr())->ExtractHugeBlobsToDelete();
            }

            bool CollectDeletedSsts() const {
                return GetPtr()->CollectDeletedSsts();
            }

            TString ToString() const {
                TStringStream str;
                str << "{" << ActionToStr(Action);
                if (auto *ptr = GetPtr()) {
                    ptr->Output(str);
                }
                str << "}";
                return str.Str();
            }

        private:
            const TBase *GetPtr() const {
                switch (Action) {
                    case ActNothing:        return nullptr;
                    case ActDeleteSsts:     return &DeleteSsts;
                    case ActMoveSsts:       return &MoveSsts;
                    case ActCompactSsts:    return &CompactSsts;
                }
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TBoundaries
        // Precalculated compaction boundaries
        ////////////////////////////////////////////////////////////////////////////
        class TBoundaries : public TThrRefBase {
        public:
            static const ui64 MaxPossibleDiskSize;
            const ui32 Level0MaxSstsAtOnce;
            const ui32 SortedParts;
            const bool DregForLevel0;

            TBoundaries(ui64 chunkSize,
                        ui32 level0MaxSstsAtOnce,
                        ui32 sortedParts,
                        bool dregForLevel0);

            double GetRate(ui32 virtualLevel, ui32 chunks) const {
                Y_ABORT_UNLESS(virtualLevel != 1); // i.e. several physical levels, partially sorted parts
                return double(chunks) / double(BoundaryPerLevel[virtualLevel]);
            }

        private:
            TVector<ui32> BoundaryPerLevel;
        };

        using TBoundariesConstPtr = TIntrusiveConstPtr<TBoundaries>;

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TSelectorParams
        ////////////////////////////////////////////////////////////////////////////
        struct TSelectorParams {
            // Precalculated (cached) boundaries
            TBoundariesConstPtr Boundaries;
            // Threshold for balance strategy, selects what segments to compact if any
            double RankThreshold;
            // All SSTs with CTime less than SqueezeBefore must be squeezed
            TInstant SqueezeBefore;
            // Full compact LevelIndex before this lsn
            std::optional<TFullCompactionAttrs> FullCompactionAttrs;
        };

    } // NHullComp
} // NKikimr
