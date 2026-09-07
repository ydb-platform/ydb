#pragma once

#include <ydb/core/metering/bill_record.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
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
    // Flat RU cost for SQS-over-topic methods that are not payload-metered.
    constexpr double DEFAULT_REQUEST_COST = 2.0;

    // RU cost charged per payload block (see WRITE_BLOCK_SIZE / READ_BLOCK_SIZE).
    constexpr double WRITE_COST_PER_BLOCK = 1.0;
    constexpr double READ_COST_PER_BLOCK = 1.0;

    // FIFO ordering requires extra work on the server side, so the
    // corresponding requests are charged more. Content-based deduplication
    // does not add an extra RU.
    constexpr double FIFO_COST_ADJUNCT = 1.0;

    inline double CostAdjunct(bool fifo) {
        return fifo ? FIFO_COST_ADJUNCT : 0.0;
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
    inline ui64 CalcRu(ui64 payloadBlocks, double baseCost, double costPerBlock, bool fifo = false) {
        const double ru = baseCost + payloadBlocks * costPerBlock + CostAdjunct(fifo);
        return RoundRu(ru);
    }

    // SQS-over-topic request-unit bills. Native Topics API / Kesus accounting
    // keep using ydb.serverless.requests.v1 from the shared rate-limiter resource.
    inline constexpr TStringBuf REQUEST_UNITS_SCHEMA = "yds.serverless.requests.v1";

    struct TMeteringIds {
        TString CloudId;
        TString FolderId;
        TString DatabaseId;

        bool IsComplete() const {
            return !CloudId.empty() && !FolderId.empty() && !DatabaseId.empty();
        }
    };

    inline TString MakeRequestUnitsBill(const TMeteringIds& ids, ui64 ru, TInstant now, const TString& id) {
        return TBillRecord()
            .Id(id)
            .Schema(TString(REQUEST_UNITS_SCHEMA))
            .CloudId(ids.CloudId)
            .FolderId(ids.FolderId)
            .ResourceId(ids.DatabaseId)
            .SourceWt(now)
            .Usage(TBillRecord::RequestUnits(ru, now))
            .ToString();
    }

} // namespace NKikimr::NSqsTopic::V1::NBilling
