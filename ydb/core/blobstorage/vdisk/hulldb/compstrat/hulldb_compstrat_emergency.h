#pragma once

#include "defs.h"
#include "hulldb_compstrat_utils.h"
#include <util/generic/algorithm.h>

namespace NKikimr {
    namespace NHullComp {

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TStrategyEmergency
        // Pick a small SST set whose estimated output fits FreeChunksBudget and
        // is expected to free index chunks (or a chunk of huge-blob garbage).
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TStrategyEmergency {
        public:
            using TTask = ::NKikimr::NHullComp::TTask<TKey, TMemRec>;
            using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
            using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;
            using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
            using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec>;
            using TSortedLevel = typename TLevelSliceSnapshot::TSortedLevel;
            using TSegments = typename TLevelSliceSnapshot::TSegments;
            using TUtils = ::NKikimr::NHullComp::TUtils<TKey, TMemRec>;

            TStrategyEmergency(
                    TIntrusivePtr<THullCtx> hullCtx,
                    const TSelectorParams &params,
                    const TLevelIndexSnapshot &levelSnap,
                    TTask *task)
                : HullCtx(std::move(hullCtx))
                , Params(params)
                , LevelSnap(levelSnap)
                , Task(task)
                , MaxSsts(ui32(HullCtx->VCfg->HullCompEmergencyMaxSsts))
                , ChunkSize(HullCtx->ChunkSize)
            {}

            EAction Select() {
                TInstant startTime(TAppData::TimeProvider->Now());
                EAction action = ActNothing;
                if (MaxSsts > 0) {
                    action = Choose();
                }
                if (action != ActNothing) {
                    Task->SetupAction(action);
                }

                TInstant finishTime(TAppData::TimeProvider->Now());
                if (HullCtx->VCtx->ActorSystem) {
                    YDB_LOG_CTX_COMP(*HullCtx->VCtx->ActorSystem,
                        action == ActNothing ? NLog::PRI_DEBUG : NLog::PRI_INFO,
                        NKikimrServices::BS_HULLCOMP,
                        VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                            "%s: Emergency: action# %s timeSpent# %s budget# %" PRIu32
                            " maxSsts# %" PRIu32 " candidate# %s",
                            PDiskSignatureForHullDbKey<TKey>().ToString().data(),
                            ActionToStr(action),
                            (finishTime - startTime).ToString().data(),
                            Params.FreeChunksBudget,
                            MaxSsts,
                            Best.ToString().data()));
                }
                return action;
            }

        private:
            struct TLess {
                bool operator()(const TKey &key, const TLevelSegmentPtr &ptr) const {
                    return key < ptr->FirstKey();
                }
                bool operator()(const TLevelSegmentPtr &ptr, const TKey &key) const {
                    return ptr->FirstKey() < key;
                }
            };

            struct TCandidate {
                enum class EKind {
                    None,
                    Pack,
                    Squeeze,
                    CrossLevel,
                };

                EKind Kind = EKind::None;
                ui32 TargetLevel = 0;
                ui32 SrcLevel = 0;
                ui32 InputChunks = 0;
                ui32 OutputChunks = 0;
                ui64 HugeGarbage = 0;
                typename TSegments::const_iterator SrcFirst{};
                typename TSegments::const_iterator SrcLast{};
                typename TSegments::const_iterator NextFirst{};
                typename TSegments::const_iterator NextLast{};

                i64 NetIndexChunks() const {
                    return i64(InputChunks) - i64(OutputChunks);
                }

                bool BetterThan(const TCandidate &o) const {
                    if (NetIndexChunks() != o.NetIndexChunks()) {
                        return NetIndexChunks() > o.NetIndexChunks();
                    }
                    if (HugeGarbage != o.HugeGarbage) {
                        return HugeGarbage > o.HugeGarbage;
                    }
                    return OutputChunks < o.OutputChunks;
                }

                TString ToString() const {
                    TStringStream str;
                    str << "{Kind# ";
                    switch (Kind) {
                        case EKind::None: str << "None"; break;
                        case EKind::Pack: str << "Pack"; break;
                        case EKind::Squeeze: str << "Squeeze"; break;
                        case EKind::CrossLevel: str << "CrossLevel"; break;
                    }
                    str << " SrcLevel# " << SrcLevel
                        << " TargetLevel# " << TargetLevel
                        << " InputChunks# " << InputChunks
                        << " OutputChunks# " << OutputChunks
                        << " HugeGarbage# " << HugeGarbage
                        << "}";
                    return str.Str();
                }
            };

            TIntrusivePtr<THullCtx> HullCtx;
            const TSelectorParams &Params;
            const TLevelIndexSnapshot &LevelSnap;
            TTask *Task;
            const ui32 MaxSsts;
            const ui32 ChunkSize;
            TCandidate Best;

            bool FitsBudget(ui32 outputChunks) const {
                return outputChunks <= Params.FreeChunksBudget;
            }

            bool IsUseful(const TCandidate &c) const {
                return c.NetIndexChunks() > 0 || c.HugeGarbage >= ChunkSize;
            }

            void Consider(TCandidate &&c) {
                if (c.Kind == TCandidate::EKind::None) {
                    return;
                }
                if (!FitsBudget(c.OutputChunks) || !IsUseful(c)) {
                    return;
                }
                if (Best.Kind == TCandidate::EKind::None || c.BetterThan(Best)) {
                    Best = std::move(c);
                }
            }

            void AddRangeMetrics(
                    typename TSegments::const_iterator first,
                    typename TSegments::const_iterator last,
                    ui32 &inputChunks,
                    ui64 &keepBytes,
                    ui64 &hugeGarbage) const
            {
                for (auto it = first; it != last; ++it) {
                    inputChunks += TUtils::SstInputChunks(**it);
                    keepBytes += TUtils::SstKeepBytes(**it);
                    hugeGarbage += TUtils::SstHugeGarbageBytes(**it);
                }
            }

            void ScanSameLevel(ui32 levelIdx) {
                const TLevelSliceSnapshot &slice = LevelSnap.SliceSnap;
                const TSortedLevel &sorted = slice.GetLevelXRef(levelIdx);
                const TSegments &segs = sorted.Segs->Segments;
                if (segs.size() < 1) {
                    return;
                }

                const ui32 level = levelIdx + 1;
                const ui32 n = segs.size();
                for (ui32 i = 0; i < n; ++i) {
                    ui32 inputChunks = 0;
                    ui64 keepBytes = 0;
                    ui64 hugeGarbage = 0;
                    const ui32 maxW = Min(MaxSsts, n - i);
                    for (ui32 w = 1; w <= maxW; ++w) {
                        const auto sst = segs[i + w - 1];
                        inputChunks += TUtils::SstInputChunks(*sst);
                        keepBytes += TUtils::SstKeepBytes(*sst);
                        hugeGarbage += TUtils::SstHugeGarbageBytes(*sst);

                        TCandidate c;
                        c.SrcLevel = level;
                        c.TargetLevel = level;
                        c.SrcFirst = segs.begin() + i;
                        c.SrcLast = segs.begin() + i + w;
                        c.InputChunks = inputChunks;
                        c.OutputChunks = TUtils::EstimateOutputChunks(keepBytes, ChunkSize);
                        c.HugeGarbage = hugeGarbage;
                        c.Kind = (w == 1) ? TCandidate::EKind::Squeeze : TCandidate::EKind::Pack;
                        Consider(std::move(c));
                    }
                }
            }

            std::pair<typename TSegments::const_iterator, typename TSegments::const_iterator>
            FindOverlap(const TSegments &segs, const TKey &firstKey, const TKey &lastKey) const {
                if (segs.empty()) {
                    return {segs.end(), segs.end()};
                }
                auto firstIt = ::LowerBound(segs.begin(), segs.end(), firstKey, TLess());
                if (firstIt != segs.begin()) {
                    --firstIt;
                    if ((*firstIt)->LastKey() < firstKey) {
                        ++firstIt;
                    }
                }
                auto endIt = ::UpperBound(segs.begin(), segs.end(), lastKey, TLess());
                return {firstIt, endIt};
            }

            void ScanCrossLevel(ui32 srcLevelIdx) {
                const TLevelSliceSnapshot &slice = LevelSnap.SliceSnap;
                if (srcLevelIdx + 1 >= slice.GetLevelXNumber()) {
                    return;
                }
                const TSegments &srcSegs = slice.GetLevelXRef(srcLevelIdx).Segs->Segments;
                const TSegments &nextSegs = slice.GetLevelXRef(srcLevelIdx + 1).Segs->Segments;
                if (srcSegs.empty() || nextSegs.empty()) {
                    return;
                }

                const ui32 srcLevel = srcLevelIdx + 1;
                for (auto srcIt = srcSegs.begin(); srcIt != srcSegs.end(); ++srcIt) {
                    auto [nextFirst, nextLast] = FindOverlap(nextSegs,
                        (*srcIt)->FirstKey(), (*srcIt)->LastKey());
                    const ui32 overlap = static_cast<ui32>(nextLast - nextFirst);
                    if (overlap == 0) {
                        continue; // PromoteSsts handles non-intersecting SSTs
                    }
                    if (overlap + 1 > MaxSsts) {
                        continue;
                    }

                    TCandidate c;
                    c.Kind = TCandidate::EKind::CrossLevel;
                    c.SrcLevel = srcLevel;
                    c.TargetLevel = srcLevel + 1;
                    c.SrcFirst = srcIt;
                    c.SrcLast = srcIt + 1;
                    c.NextFirst = nextFirst;
                    c.NextLast = nextLast;
                    ui64 keepBytes = 0;
                    AddRangeMetrics(c.SrcFirst, c.SrcLast, c.InputChunks, keepBytes, c.HugeGarbage);
                    AddRangeMetrics(c.NextFirst, c.NextLast, c.InputChunks, keepBytes, c.HugeGarbage);
                    c.OutputChunks = TUtils::EstimateOutputChunks(keepBytes, ChunkSize);
                    Consider(std::move(c));
                }
            }

            void ApplyBest() {
                auto &compactSsts = Task->CompactSsts;
                if (Best.Kind == TCandidate::EKind::CrossLevel) {
                    compactSsts.TargetLevel = Best.TargetLevel;
                    compactSsts.PushSstFromLevelX(Best.SrcLevel, Best.SrcFirst, Best.SrcLast);
                    compactSsts.LastCompactedKey = (*Best.SrcFirst)->LastKey();
                    compactSsts.PushSstFromLevelX(Best.SrcLevel + 1, Best.NextFirst, Best.NextLast);
                } else {
                    TUtils::CompactContiguousSsts(
                        LevelSnap.SliceSnap,
                        Best.SrcLevel,
                        Best.SrcFirst,
                        Best.SrcLast,
                        compactSsts);
                }
            }

            EAction Choose() {
                const TLevelSliceSnapshot &slice = LevelSnap.SliceSnap;
                const ui32 nLevels = slice.GetLevelXNumber();
                if (nLevels == 0) {
                    return ActNothing;
                }

                // Prefer last populated level first (typical "all data on 17/18" case).
                for (i32 i = static_cast<i32>(nLevels) - 1; i >= 0; --i) {
                    ScanSameLevel(static_cast<ui32>(i));
                }
                for (i32 i = static_cast<i32>(nLevels) - 2; i >= 0; --i) {
                    ScanCrossLevel(static_cast<ui32>(i));
                }

                if (Best.Kind == TCandidate::EKind::None) {
                    return ActNothing;
                }

                if (HullCtx->VCtx->ActorSystem) {
                    YDB_LOG_INFO_CTX_COMP(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                        "TStrategyEmergency decided to compact",
                        {"VDiskLogPrefix", HullCtx->VCtx->VDiskLogPrefix},
                        {"candidate", Best.ToString()},
                        {"budget", Params.FreeChunksBudget});
                }
                ApplyBest();
                return ActCompactSsts;
            }
        };

    } // NHullComp
} // NKikimr
