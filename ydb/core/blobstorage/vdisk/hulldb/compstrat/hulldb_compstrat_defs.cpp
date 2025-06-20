#include "hulldb_compstrat_defs.h"

namespace NKikimr {
    namespace NHullComp {

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TBoundaries
        ////////////////////////////////////////////////////////////////////////////
        TBoundaries::TBoundaries(
                ui64 chunkSize,
                ui32 level0MaxSstsAtOnce,
                ui32 sortedParts,
                bool dregForLevel0)
            : Level0MaxSstsAtOnce(level0MaxSstsAtOnce)
            , SortedParts(sortedParts)
            , DregForLevel0(dregForLevel0)
        {
            // calculate boundaries table

            // level 0
            ui32 maxSstsLevel0 = Level0MaxSstsAtOnce;
            if (DregForLevel0) {
                // When dreg option is on, we double number of ssts at the level 0;
                // Level 0 compaction takes only half of the level, other ssts waits
                // for thier turn
                maxSstsLevel0 = Level0MaxSstsAtOnce * 2;
            }
            BoundaryPerLevel.push_back(maxSstsLevel0); // level0

            // level 1, virtual
            BoundaryPerLevel.push_back(0); // partially sorted level (NOT USED)

            // other levels
            ui32 curChunksNum = SortedParts * Level0MaxSstsAtOnce * 2;
            while (curChunksNum * chunkSize < MaxPossibleDiskSize) {
                BoundaryPerLevel.push_back(curChunksNum);
                curChunksNum *= 4;
            }
        }

    } // NHullComp
} // NKikimr
