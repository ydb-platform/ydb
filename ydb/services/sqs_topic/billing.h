#pragma once

#include <util/generic/size_literals.h>
#include <util/system/types.h>

#include <cmath>

namespace NKikimr::NSqsTopic::V1::NBilling {

    // Block sizes used to convert a transferred payload into a number of
    // Request Units (one block == one RU). Kept in sync with the persqueue
    // read/write session actors. Reads are billed in coarser 8 KiB blocks than
    // writes.
    inline constexpr ui64 WRITE_BLOCK_SIZE = 4_KB;
    inline constexpr ui64 READ_BLOCK_SIZE = 8_KB;

    // Costs are expressed as floating-point RU amounts so that fractional
    // per-message / per-block / base prices can be configured. The final charge
    // is rounded to a whole number of Request Units before it is sent to the
    // rate limiter.

    // Base RU cost charged for a request regardless of the transferred amount.
    inline constexpr double WRITE_BASE_COST = 1.0;
    inline constexpr double READ_BASE_COST = 1.0;
    inline constexpr double DELETE_BASE_COST = 1.0;

    // RU cost charged per payload block (see WRITE_BLOCK_SIZE / READ_BLOCK_SIZE).
    inline constexpr double WRITE_COST_PER_BLOCK = 1.0;
    inline constexpr double READ_COST_PER_BLOCK = 1.0;

    // Flat RU cost charged per message handled by the request, on top of the
    // base and block-based payload costs. Charged for every successfully written
    // message (write) or every returned message (read).
    inline constexpr double WRITE_COST_PER_MESSAGE = 1.0;
    inline constexpr double READ_COST_PER_MESSAGE = 1.0;

    // Flat RU cost charged per successfully deleted message.
    inline constexpr double DELETE_COST_PER_MESSAGE = 1.0;

    // FIFO ordering and content-based deduplication require extra work on the
    // server side, so the corresponding requests are charged more.
    inline constexpr double FIFO_COST_MULTIPLIER = 2.0;
    inline constexpr double DEDUP_COST_MULTIPLIER = 2.0;

    inline double CostMultiplier(bool fifo, bool dedup) {
        double multiplier = 1.0;
        if (fifo) {
            multiplier *= FIFO_COST_MULTIPLIER;
        }
        if (dedup) {
            multiplier *= DEDUP_COST_MULTIPLIER;
        }
        return multiplier;
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
    // TRlHelpers::CalcRuConsumption(payloadSize). messageCount is the number of
    // messages handled by the request, charged on top of the base and payload
    // costs.
    inline ui64 CalcWriteRu(ui64 payloadBlocks, ui64 messageCount, bool fifo, bool dedup) {
        const double ru = (WRITE_BASE_COST
                + payloadBlocks * WRITE_COST_PER_BLOCK
                + messageCount * WRITE_COST_PER_MESSAGE)
            * CostMultiplier(fifo, dedup);
        return RoundRu(ru);
    }

    inline ui64 CalcReadRu(ui64 payloadBlocks, ui64 messageCount, bool fifo, bool dedup) {
        const double ru = (READ_BASE_COST
                + payloadBlocks * READ_COST_PER_BLOCK
                + messageCount * READ_COST_PER_MESSAGE)
            * CostMultiplier(fifo, dedup);
        return RoundRu(ru);
    }

    inline ui64 CalcDeleteRu(ui64 messageCount, bool fifo, bool dedup) {
        const double ru = (DELETE_BASE_COST + messageCount * DELETE_COST_PER_MESSAGE)
            * CostMultiplier(fifo, dedup);
        return RoundRu(ru);
    }

} // namespace NKikimr::NSqsTopic::V1::NBilling
