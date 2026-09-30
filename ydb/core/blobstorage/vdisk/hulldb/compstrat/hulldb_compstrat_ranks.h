#pragma once

#include "hulldb_compstrat_defs.h"

namespace NKikimr::NHullComp {

    // LSM pressure calculated without selecting or modifying a compaction task.
    struct TLevelRanks {
        std::vector<double> Ranks;
        ui32 FreePartiallySortedLevelsNum = 0;
        ui32 VirtualLevelToCompact = 0;

        template <class TKey, class TMemRec>
        TLevelRanks(const TBoundaries &boundaries, const TLevelSliceSnapshot<TKey, TMemRec> &sliceSnap) {
            Ranks.reserve(8);
            Ranks.push_back(boundaries.GetRate(0, sliceSnap.GetLevel0ChunksNum()));

            const ui32 totalPsl = boundaries.SortedParts * 2;
            const ui32 otherLevelsNum = sliceSnap.GetLevelXNumber();
            for (ui32 i = 0; i < totalPsl; ++i) {
                if (i >= otherLevelsNum || sliceSnap.GetLevelXRef(i).Empty()) {
                    ++FreePartiallySortedLevelsNum;
                }
            }

            double pslRank = 0.0;
            if (FreePartiallySortedLevelsNum == totalPsl) {
                pslRank = 0.0;
            } else if (FreePartiallySortedLevelsNum == 0) {
                pslRank = 1000000.0;
            } else {
                const double step = 1.0 / totalPsl;
                pslRank = step * (totalPsl - FreePartiallySortedLevelsNum);
            }
            Ranks.push_back(pslRank);

            for (ui32 i = totalPsl; i < otherLevelsNum; ++i) {
                const ui32 virtualLevel = i - totalPsl + 2;
                const double rank = boundaries.GetRate(virtualLevel, sliceSnap.GetLevelXChunksNum(i));
                Ranks.push_back(rank);
            }

            for (ui32 i = 1; i < Ranks.size(); ++i) {
                if (Ranks[i] > Ranks[VirtualLevelToCompact]) {
                    VirtualLevelToCompact = i;
                }
            }

        }

        double GetMaxRank() const {
            return Ranks[VirtualLevelToCompact];
        }

        TString ToString() const {
            TStringStream str;
            str << "{VirtualLevelToCompact# " << VirtualLevelToCompact
                << " FreePartiallySortedLevelsNum# " << FreePartiallySortedLevelsNum
                << " Ranks# ";
            for (const auto &rank : Ranks) {
                str << " " << rank;
            }
            str << "}";
            return str.Str();
        }
    };

} // NKikimr::NHullComp
