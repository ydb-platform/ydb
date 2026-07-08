#pragma once

#include <util/generic/size_literals.h>
#include <util/system/types.h>

#include <cmath>

namespace NKikimr::NSqsTopic::V1::NBilling {

    // Block sizes used to convert a transferred payload into a number of
    // Request Units (one block == one RU). Kept in sync with the persqueue
    // read/write session actors. Reads are billed in coarser 8 KiB blocks than
    // writes.
    constexpr ui64 WRITE_BLOCK_SIZE = 4_KB;
    constexpr ui64 READ_BLOCK_SIZE = 8_KB;

    // Costs are expressed as floating-point RU amounts so that fractional
    // per-block / base prices can be configured. The final charge
    // is rounded to a whole number of Request Units before it is sent to the
    // rate limiter.

    // Base RU cost charged for a request regardless of the transferred amount.
    constexpr double WRITE_BASE_COST = 2.0;
    constexpr double READ_BASE_COST = 2.0;
    constexpr double DELETE_BASE_COST = 2.0;

    // RU cost charged per payload block (see WRITE_BLOCK_SIZE / READ_BLOCK_SIZE).
    constexpr double WRITE_COST_PER_BLOCK = 1.0;
    constexpr double READ_COST_PER_BLOCK = 1.0;

    // FIFO ordering and content-based deduplication require extra work on the
    // server side, so the corresponding requests are charged more.
    constexpr double FIFO_COST_ADJUNCT = 1.0;
    constexpr double DEDUP_COST_ADJUNCT = 1.0;

    inline double CostAdjunct(bool fifo, bool dedup) {
        double adjunct = 0.0;
        if (fifo) {
            adjunct += FIFO_COST_ADJUNCT;
        }
        if (dedup) {
            adjunct += DEDUP_COST_ADJUNCT;
        }
        return adjunct;
    }

    // Rounds a floating-point RU amount to the whole number of Request Units
    // that is actually charged.
    inline ui64 RoundRu(double ru) {
        if (ru <= 0.0) {
            return 0;
        }
        return static_cast<ui64>(std::llround(ru));
    }

    // payloadBlocks is the block-based consumption produced by
    // TRlHelpers::CalcRuConsumption(payloadSize). 
    inline ui64 CalcRu(ui64 payloadBlocks, double baseCost, double costPerBlock, bool fifo, bool dedup) {
        const double ru = baseCost
                + payloadBlocks * costPerBlock + CostAdjunct(fifo, dedup);
        return RoundRu(ru);
    }

} // namespace NKikimr::NSqsTopic::V1::NBilling
