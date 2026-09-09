#pragma once

#include <yql/essentials/core/histogram/proto/eq_height_histogram.pb.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/ylimits.h>
#include <util/system/types.h>

namespace NKikimr {

// Equi-height histogram over memcomparable byte keys (memcmp order = value
// order). Callers encode keys with NMiniKQL::TPresortEncoder; this class sees
// opaque bytes. Buckets hold about N/NumBuckets rows and are cut at real keys,
// so only order is needed — strings, numbers, and composite keys use the same
// code.
//
// This is a mergeable summary (Greenwald–Khanna / Agarwal): independent scans
// produce partial states that Merge combines in any order. Each entry stores
// Count (rows in its range) and RankUncertainty (how far the rank of
// UpperBound may be off). The rank error of a cut is RankUncertainty, not
// Count. Without it, merging would add error at every step.
//
// Sorted keys open a new entry every Capacity() rows and never split a run of
// equal keys. Unsorted keys are buffered, sorted, and merged the same way.
// Non-overlapping ranges merge with RankUncertainty left at 0.
//
// Capacity is n / EmissionRate for this state's own row count. After merge or
// flush, Compact fuses neighbouring entries so RankUncertainty stays within
// about 2 * Capacity. Add, Flush, and Merge compact at MaxStateBytes / 2 so a
// following Add still has room.
class TEqHeightHistogramBuilder {
public:
    static constexpr ui32 MIN_ENTRIES = 16; // lower bound on summary size for a useful histogram

    struct TParams {
        ui32 NumBuckets = 1;           // buckets in the finished histogram
        ui32 EmissionRate = 8;         // how often a new summary entry is opened
        ui64 MaxStateBytes = 4U << 20; // memory budget; Compact uses half
    };

    struct TEntry {
        TString UpperBound;       // last key in this range
        ui64 Count = 0;           // rows in this range
        ui64 RankUncertainty = 0; // possible error in the rank of UpperBound
        bool SingleKey = false;   // all rows in this range have the same key
    };

    class TSummary {
    public:
        TSummary() = default;
        TSummary(const TSummary& other);
        TSummary(TSummary&&) noexcept = default;
        TSummary& operator=(const TSummary& other);
        TSummary& operator=(TSummary&&) noexcept = default;
        explicit TSummary(TVector<TEntry> entries, bool budgetForced = false);

        const TVector<TEntry>& Entries() const {
            return Entries_;
        }
        ui64 Bytes() const {
            return Bytes_;
        }
        bool BudgetForced() const {
            return BudgetForced_;
        }

        void RecountBytes();
        ui64 MaxRankUncertainty() const;
        bool IsSorted() const;

    private:
        friend class TEqHeightHistogramBuilder;
        friend class TEqHeightHistogramBuilderTestApi;

        TVector<TEntry> Entries_;
        ui64 Bytes_ = 0;            // size of Entries in the serialized form
        bool BudgetForced_ = false; // true if Compact hit MaxStateBytes
    };

    explicit TEqHeightHistogramBuilder(const TParams& params);
    explicit TEqHeightHistogramBuilder(const TEqHeightHistogramIntermediateState& state);

    void Add(TStringBuf key);

    void Merge(const TEqHeightHistogramBuilder& other);

    TEqHeightHistogramIntermediateState Serialize() const;

    TMaybe<TEqHeightHistogramResult> Finalize() const;

    ui64 GetTotalCount() const;

private:
    friend class TEqHeightHistogramBuilderTestApi;

    TMaybe<TStringBuf> MaxKey() const;

    void Flush();
    ui64 SoftBudget() const;
    ui64 StateBytes() const;
    ui64 OverheadBytes() const;
    ui64 Capacity() const;

    void AddSorted(TStringBuf key);
    TSummary Fold() const;
    static TVector<TEntry> CollapseSorted(TVector<TString> keys);
    static void PushEntry(TSummary& summary, TStringBuf key);
    static void InterleaveInto(TSummary& dst, TStringBuf dstMinKey,
                               TVector<TEntry>&& other, TStringBuf otherMinKey);
    static void ApplyFusions(TSummary& summary, const TVector<bool>& absorbed);
    static bool FuseLightestPair(TSummary& summary);
    static bool FuseAdmissiblePairs(TSummary& summary, ui64 cap);
    void Compact(TSummary& summary, ui64 budgetBytes) const;
    void ValidateParams() const;

    TParams Params_;
    TSummary Summary_;
    TVector<TString> Staging_;
    TString MinKey_;
    ui64 TotalCount_ = 0;
    ui64 StagingBytes_ = 0;
    ui64 FlushCount_ = 0;
    bool Sorted_ = true;
};

} // namespace NKikimr
