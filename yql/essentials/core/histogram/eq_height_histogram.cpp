#include "eq_height_histogram.h"

#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/utils/yql_panic.h>

#include <google/protobuf/message.h>

#include <utility>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>
#include <util/system/yassert.h>

namespace NKikimr {

namespace {

// Fill fields to max wire size so ByteSizeLong() tracks the schema.
void FillWidest(NProtoBuf::Message& message) {
    const auto* descriptor = message.GetDescriptor();
    const auto* reflection = message.GetReflection();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const auto* field = descriptor->field(i);
        if (field->is_repeated()) {
            continue; // charged per element by the caller
        }
        switch (field->cpp_type()) {
            case NProtoBuf::FieldDescriptor::CPPTYPE_INT32:
                reflection->SetInt32(&message, field, Min<i32>()); // negative varints are widest
                break;
            case NProtoBuf::FieldDescriptor::CPPTYPE_INT64:
                reflection->SetInt64(&message, field, Min<i64>());
                break;
            case NProtoBuf::FieldDescriptor::CPPTYPE_UINT32:
                reflection->SetUInt32(&message, field, Max<ui32>());
                break;
            case NProtoBuf::FieldDescriptor::CPPTYPE_UINT64:
                reflection->SetUInt64(&message, field, Max<ui64>());
                break;
            case NProtoBuf::FieldDescriptor::CPPTYPE_BOOL:
                reflection->SetBool(&message, field, true);
                break;
            case NProtoBuf::FieldDescriptor::CPPTYPE_STRING:
                reflection->SetString(&message, field, TString(1, '\0')); // Strings get one placeholder byte (proto3 omits empty ones).
                break;
            case NProtoBuf::FieldDescriptor::CPPTYPE_MESSAGE:
                FillWidest(*reflection->MutableMessage(&message, field));
                break;
            default:
                YQL_ENSURE(false, "eq-height histogram: cannot size proto field " << field->full_name());
        }
    }
}

// Serialized size of one entry without UpperBound.
ui64 EntryOverhead() {
    static const ui64 overhead = [] {
        TEqHeightHistogramIntermediateState emptyState;
        FillWidest(emptyState);
        const ui64 emptySize = emptyState.ByteSizeLong();

        TEqHeightHistogramIntermediateState oneEntryState;
        FillWidest(oneEntryState);
        FillWidest(*oneEntryState.AddEntries());
        return oneEntryState.ByteSizeLong() - emptySize;
    }();
    return overhead;
}

// Serialized size of the state without MinKey and Entries.
ui64 HeaderOverhead() {
    static const ui64 overhead = [] {
        TEqHeightHistogramIntermediateState state;
        FillWidest(state);
        return state.ByteSizeLong();
    }();
    return overhead;
}

// 128-bit to avoid overflow on large tables: a ui64 sum would wrap and treat
// an expensive pair as cheapest.
NYql::NDecimal::TUint128 FuseCost(const TEqHeightHistogramBuilder::TEntry& left,
                                  const TEqHeightHistogramBuilder::TEntry& right) {
    return static_cast<NYql::NDecimal::TUint128>(right.RankUncertainty) + left.Count + right.Count;
}

bool ExceedsTwiceCap(NYql::NDecimal::TUint128 value, ui64 cap) {
    return value > static_cast<NYql::NDecimal::TUint128>(cap) * 2;
}

// Output bucket for Finalize.
struct TBucketOut {
    TString UpperBound;
    ui64 CumulativeCount;
};

// Walk the folded summary and emit bucket boundaries at the ideal cumulative
// ranks ceil(idx*total/b) for idx = 1..b.  Returns the buckets and the max
// RankUncertainty over the boundary entries (maxRankError).
TBucketOut EmitBucket(const TEqHeightHistogramBuilder::TEntry& entry, ui64 acc) {
    return {entry.UpperBound, acc};
}

bool ReachedThreshold(ui64 acc, ui64 idx, ui64 total, ui32 b) {
    return static_cast<NYql::NDecimal::TUint128>(acc) * b >= static_cast<NYql::NDecimal::TUint128>(idx) * total;
}

TVector<TBucketOut> BuildBuckets(const TEqHeightHistogramBuilder::TSummary& summary,
                                 ui64 total, ui32 b, ui64& maxRankError) {
    TVector<TBucketOut> buckets;
    ui64 acc = 0, bucketIdx = 1;
    maxRankError = 0;

    for (size_t i = 0; i < summary.Entries().size(); ++i) {
        const auto& entry = summary.Entries()[i];
        acc += entry.Count;
        const bool last = (i + 1 == summary.Entries().size());
        if (last || ReachedThreshold(acc, bucketIdx, total, b)) {
            buckets.push_back(EmitBucket(entry, acc));
            maxRankError = Max(maxRankError, entry.RankUncertainty);
            while (bucketIdx <= b && ReachedThreshold(acc, bucketIdx, total, b)) {
                ++bucketIdx;
            }
        }
    }
    return buckets;
}

ui64 EstimateEntryBytes(const TEqHeightHistogramBuilder::TEntry& entry) {
    const ui64 keySize = entry.UpperBound.size();
    const ui64 overhead = EntryOverhead();
    return (keySize > Max<ui64>() - overhead) ? Max<ui64>() : (keySize + overhead);
}

// Cap at Max so compaction still triggers; wrap would look "small" and skip it.
ui64 SaturatedAddBytes(ui64 a, ui64 b) {
    return (a > (Max<ui64>() - b)) ? Max<ui64>() : (a + b);
}

// Row counts must not wrap: silent Max would corrupt optimizer stats.
ui64 CheckedAddCount(ui64 a, ui64 b) {
    YQL_ENSURE(a <= Max<ui64>() - b, "eq-height histogram: row-count overflow");
    return a + b;
}

} // namespace

void TEqHeightHistogramBuilder::TSummary::RecountBytes() {
    Bytes_ = 0;
    for (const auto& entry : Entries_) {
        Bytes_ = SaturatedAddBytes(Bytes_, EstimateEntryBytes(entry));
    }
}

ui64 TEqHeightHistogramBuilder::TSummary::MaxRankUncertainty() const {
    ui64 maxLoad = 0;
    for (const auto& entry : Entries_) {
        maxLoad = Max(maxLoad, entry.RankUncertainty);
    }
    return maxLoad;
}

bool TEqHeightHistogramBuilder::TSummary::IsSorted() const {
    for (size_t i = 1; i < Entries_.size(); ++i) {
        if (!(Entries_[i - 1].UpperBound < Entries_[i].UpperBound)) {
            return false;
        }
    }
    return true;
}

TEqHeightHistogramBuilder::TSummary::TSummary(const TSummary& other)
    : Entries_(other.Entries_)
    , Bytes_(other.Bytes_)
    , BudgetForced_(other.BudgetForced_)
{
    Y_DEBUG_ABORT_UNLESS(IsSorted(), "eq-height histogram: entries must be unique and sorted");
}

TEqHeightHistogramBuilder::TSummary& TEqHeightHistogramBuilder::TSummary::operator=(const TSummary& other) {
    if (this != &other) {
        Entries_ = other.Entries_;
        Bytes_ = other.Bytes_;
        BudgetForced_ = other.BudgetForced_;
        Y_DEBUG_ABORT_UNLESS(IsSorted(), "eq-height histogram: entries must be unique and sorted");
    }
    return *this;
}

TEqHeightHistogramBuilder::TSummary::TSummary(TVector<TEntry> entries, bool budgetForced)
    : Entries_(std::move(entries))
    , BudgetForced_(budgetForced)
{
    Y_DEBUG_ABORT_UNLESS(IsSorted(), "eq-height histogram: entries must be unique and sorted");
    RecountBytes();
}

TEqHeightHistogramBuilder::TEqHeightHistogramBuilder(const TParams& params)
    : Params_(params)
{
    ValidateParams();
}

void TEqHeightHistogramBuilder::ValidateParams() const {
    YQL_ENSURE(Params_.EmissionRate > 0, "TEqHeightHistogramBuilder: EmissionRate must be > 0");
    YQL_ENSURE(Params_.NumBuckets > 0, "TEqHeightHistogramBuilder: NumBuckets must be > 0");
    YQL_ENSURE(Params_.MaxStateBytes > 0, "TEqHeightHistogramBuilder: MaxStateBytes must be > 0");
}

ui64 TEqHeightHistogramBuilder::Capacity() const {
    return Max(ui64{1}, TotalCount_ / Params_.EmissionRate);
}

ui64 TEqHeightHistogramBuilder::GetTotalCount() const {
    return TotalCount_;
}

TMaybe<TStringBuf> TEqHeightHistogramBuilder::MaxKey() const {
    if (Summary_.Entries_.empty()) {
        return Nothing();
    }
    return TStringBuf(Summary_.Entries_.back().UpperBound);
}

ui64 TEqHeightHistogramBuilder::SoftBudget() const {
    return Params_.MaxStateBytes / 2;
}

ui64 TEqHeightHistogramBuilder::StateBytes() const {
    return SaturatedAddBytes(Summary_.Bytes_, OverheadBytes());
}

ui64 TEqHeightHistogramBuilder::OverheadBytes() const {
    return SaturatedAddBytes(MinKey_.size(), HeaderOverhead());
}

TEqHeightHistogramBuilder::TEqHeightHistogramBuilder(const TEqHeightHistogramIntermediateState& state) {
    const bool budgetForced = state.GetBudgetForced();
    const auto& params = state.GetParams();
    Params_.NumBuckets = params.GetNumBuckets();
    Params_.EmissionRate = params.GetEmissionRate();
    Params_.MaxStateBytes = params.GetMaxStateBytes();
    ValidateParams();
    TotalCount_ = state.GetTotalCount();
    MinKey_ = state.GetMinKey();
    TVector<TEntry> entries;
    entries.reserve(state.EntriesSize());
    for (ui32 i = 0; i < state.EntriesSize(); ++i) {
        const auto& protoEntry = state.GetEntries(i);
        TEntry entry{
            .UpperBound = TString(protoEntry.GetUpperBound()),
            .Count = protoEntry.GetCount(),
            .RankUncertainty = protoEntry.GetRankUncertainty(),
            .SingleKey = protoEntry.GetSingleKey(),
        };
        // Results cross process boundaries; validate content.
        YQL_ENSURE(entry.Count >= 1, "malformed eq-height histogram state: zero Count");
        if (i > 0) {
            YQL_ENSURE(entry.UpperBound > entries.back().UpperBound,
                       "malformed eq-height histogram state: entries not strictly increasing");
        }
        entries.push_back(std::move(entry));
    }
    ui64 countSum = 0;
    for (const auto& entry : entries) {
        YQL_ENSURE(entry.Count <= TotalCount_ - countSum,
                   "malformed eq-height histogram state: count sum exceeds total");
        countSum += entry.Count;
    }
    YQL_ENSURE(countSum == TotalCount_, "malformed eq-height histogram state: count sum != total");
    if (!entries.empty()) {
        YQL_ENSURE(MinKey_ <= entries.front().UpperBound,
                   "malformed eq-height histogram state: MinKey exceeds first UpperBound");
    } else {
        YQL_ENSURE(MinKey_.empty(), "malformed eq-height histogram state: empty state with MinKey");
    }
    // After deserialization, entries are already sorted and we have not seen
    // any new input yet, so Sorted_ = true is the correct initial state.  The
    // first Add() will check if the key continues the sorted sequence; if
    // not, it falls back to the unsorted path naturally.
    Sorted_ = true;
    Summary_ = TSummary(std::move(entries), budgetForced);
}

void TEqHeightHistogramBuilder::Add(TStringBuf key) {
    YQL_ENSURE(TotalCount_ < Max<ui64>(), "eq-height histogram TotalCount overflow");
    ++TotalCount_;

    Y_DEBUG_ABORT_UNLESS(Sorted_ || !Summary_.Entries_.empty(),
                         "eq-height histogram invariant: empty summary implies Sorted_");
    if (Sorted_) {
        if (auto maxKey = MaxKey(); !maxKey || key >= *maxKey) {
            AddSorted(key);
            return;
        }
    }
    Sorted_ = false;

    // Deserialized-empty state has MinKey == ""; treat first row as min.
    if (TotalCount_ == 1 || key < MinKey_) {
        MinKey_ = key;
    }
    StagingBytes_ = SaturatedAddBytes(StagingBytes_, SaturatedAddBytes(key.size(), EntryOverhead()));
    Staging_.emplace_back(key);
    // Scale flush batch with |Entries|: Flush rebuilds the whole vector,
    // so flushing every EmissionRate rows is quadratic past that size.
    const size_t flushAt = Max<size_t>(Params_.EmissionRate, Summary_.Entries_.size());
    if (Staging_.size() >= flushAt || SaturatedAddBytes(StagingBytes_, StateBytes()) > SoftBudget()) {
        Flush();
    }
}

void TEqHeightHistogramBuilder::PushEntry(TSummary& summary, TStringBuf key) {
    summary.Entries_.push_back({
        .UpperBound = TString(key),
        .Count = 1,
        .RankUncertainty = 0,
        .SingleKey = true,
    });
    summary.Bytes_ = SaturatedAddBytes(summary.Bytes_, EstimateEntryBytes(summary.Entries_.back()));
}

void TEqHeightHistogramBuilder::AddSorted(TStringBuf key) {
    TSummary& summary = Summary_;
    if (summary.Entries_.empty()) {
        MinKey_ = key;
        PushEntry(summary, key);
        return;
    }
    // Never close mid-run: boundaries fall between distinct keys, so
    // cumulative count is the true rank and RankUncertainty stays 0.
    TEntry& last = summary.Entries_.back();
    if (key == last.UpperBound) {
        ++last.Count;
        return;
    }
    // Open a new entry when full (Count >= Capacity()), or when a SingleKey
    // entry is already at least half-full (Count >= ceil(Capacity()/2)).
    // Folding a different key into it would make it multi-key, setting
    // owed = Count-1 for future merges.  Keeping heavy entries SingleKey
    // preserves merge quality (owed = 0, no rank uncertainty).
    // Capacity() - Capacity()/2 is ceil(Capacity()/2) without overflowing Count*2.
    if (last.Count >= Capacity() || (last.SingleKey && last.Count >= Capacity() - Capacity() / 2)) {
        PushEntry(summary, key);
    } else {
        // Length-delta update: add-then-subtract underflows when the new key
        // is shorter (sorted does not imply nondecreasing length).  In the
        // proto format the per-entry overhead is constant (EntryOverhead()),
        // so only the raw key bytes change.
        const ui64 oldLen = last.UpperBound.size();
        const ui64 newLen = key.size();
        if (newLen >= oldLen) {
            summary.Bytes_ = SaturatedAddBytes(summary.Bytes_, newLen - oldLen);
        } else {
            summary.Bytes_ -= oldLen - newLen;
        }
        last.UpperBound = key;
        last.SingleKey = false;
        ++last.Count;
    }
    if (StateBytes() > SoftBudget()) {
        Compact(summary, SoftBudget());
    }
}

TVector<TEqHeightHistogramBuilder::TEntry>
TEqHeightHistogramBuilder::CollapseSorted(TVector<TString> keys) {
    Sort(keys);
    TVector<TEntry> staged(Reserve(keys.size()));
    for (auto& key : keys) {
        if (!staged.empty() && staged.back().UpperBound == key) {
            ++staged.back().Count;
        } else {
            staged.push_back(TEntry{
                .UpperBound = std::move(key),
                .Count = 1,
                .RankUncertainty = 0,
                .SingleKey = true,
            });
        }
    }
    return staged;
}

void TEqHeightHistogramBuilder::Flush() {
    if (Staging_.empty()) {
        return;
    }
    ++FlushCount_;
    TVector<TEntry> staged = CollapseSorted(std::move(Staging_));
    Staging_.clear();
    StagingBytes_ = 0;
    // Staging and Summary share the builder's MinKey (the global minimum).
    // That over-approximates each side's true lower bound, so RankUncertainty
    // is never under-charged.
    InterleaveInto(Summary_, MinKey_, std::move(staged), MinKey_);
    Compact(Summary_, SoftBudget());
}

TEqHeightHistogramBuilder::TSummary TEqHeightHistogramBuilder::Fold() const {
    TSummary summary = Summary_;
    TVector<TEntry> staged = CollapseSorted(Staging_);
    // Same conservative MinKey as Flush(); see comment there.
    InterleaveInto(summary, MinKey_, std::move(staged), MinKey_);
    Compact(summary, SoftBudget());
    return summary;
}

void TEqHeightHistogramBuilder::InterleaveInto(TSummary& dst, TStringBuf dstMinKey,
                                               TVector<TEntry>&& other, TStringBuf otherMinKey) {
    // Rows owed by the side taken after `entry`: RankUncertainty plus Count-1
    // when the range spans multiple keys. Both vanish for SingleKey, so one
    // predicate governs both.
    auto owed = [](const TEntry& entry) -> ui64 {
        YQL_ENSURE(entry.Count >= 1, "eq-height histogram: Count must be >= 1");
        if (entry.SingleKey) {
            return entry.RankUncertainty;
        }
        return CheckedAddCount(entry.RankUncertainty, entry.Count - 1);
    };

    TVector<TEntry> out(Reserve(dst.Entries_.size() + other.size()));

    size_t i = 0, j = 0;
    while (i < dst.Entries_.size() || j < other.size()) {
        if (i < dst.Entries_.size() && j < other.size() && dst.Entries_[i].UpperBound == other[j].UpperBound) {
            TEntry entry = std::move(dst.Entries_[i++]);
            entry.Count = CheckedAddCount(entry.Count, other[j].Count);
            entry.RankUncertainty = CheckedAddCount(entry.RankUncertainty, other[j].RankUncertainty);
            entry.SingleKey = entry.SingleKey && other[j].SingleKey;
            ++j;
            out.push_back(std::move(entry));
            continue;
        }
        const bool takeLeft = (j == other.size()) || (i < dst.Entries_.size() && dst.Entries_[i].UpperBound < other[j].UpperBound);
        if (takeLeft) {
            TEntry entry = std::move(dst.Entries_[i++]);
            if (j < other.size()) {
                // Lower-bound-aware bump: charge owed(other[j]) only when
                // other's range extends to or below entry.UpperBound.  For j > 0
                // the bound is other[j-1].UpperBound < entry.UpperBound (always
                // applies).  For j == 0 it's otherMinKey; applies when
                // otherMinKey <= entry.UpperBound (>=, not >: at least one row
                // sits on the true minimum).
                if (j > 0 || entry.UpperBound >= otherMinKey) {
                    entry.RankUncertainty = CheckedAddCount(entry.RankUncertainty, owed(other[j]));
                }
            }
            out.push_back(std::move(entry));
        } else {
            TEntry entry = std::move(other[j++]);
            if (i < dst.Entries_.size()) {
                // Symmetric: left's lower bound is dstMinKey for i == 0,
                // or dst.Entries_[i-1].UpperBound for i > 0.
                if (i > 0 || dstMinKey <= entry.UpperBound) {
                    entry.RankUncertainty = CheckedAddCount(entry.RankUncertainty, owed(dst.Entries_[i]));
                }
            }
            out.push_back(std::move(entry));
        }
    }
    dst.Entries_ = std::move(out);
    dst.RecountBytes();
    Y_DEBUG_ABORT_UNLESS(dst.IsSorted(), "eq-height histogram: entries must be unique and sorted");
}

void TEqHeightHistogramBuilder::ApplyFusions(TSummary& summary, const TVector<bool>& absorbed) {
    const size_t n = summary.Entries_.size();
    TVector<TEntry> out(Reserve(n));
    for (size_t i = 0; i < n; ++i) {
        if (absorbed[i]) {
            YQL_ENSURE(i + 1 < n, "eq-height histogram: last entry cannot be absorbed");
            summary.Entries_[i + 1].Count = CheckedAddCount(summary.Entries_[i + 1].Count, summary.Entries_[i].Count);
            summary.Entries_[i + 1].SingleKey = false;
            // Keep the surviving entry's RankUncertainty; adding the absorbed
            // entry's would double-count (see FuseCost).
        } else {
            out.push_back(std::move(summary.Entries_[i]));
        }
    }
    summary.Entries_ = std::move(out);
    summary.RecountBytes();
}

bool TEqHeightHistogramBuilder::FuseLightestPair(TSummary& summary) {
    const size_t n = summary.Entries_.size();
    if (n < 2) {
        return false;
    }
    // Find the lightest adjacent pair.
    size_t best = 0;
    auto bestCost = FuseCost(summary.Entries_[0], summary.Entries_[1]);
    for (size_t i = 1; i + 1 < n; ++i) {
        const auto cost = FuseCost(summary.Entries_[i], summary.Entries_[i + 1]);
        if (cost < bestCost) {
            bestCost = cost;
            best = i;
        }
    }
    TVector<bool> absorbed(n, false);
    absorbed[best] = true;
    ApplyFusions(summary, absorbed);
    return true;
}

bool TEqHeightHistogramBuilder::FuseAdmissiblePairs(TSummary& summary, ui64 cap) {
    const size_t n = summary.Entries_.size();
    if (n < 2) {
        return false;
    }

    struct TPair {
        size_t Left;
        NYql::NDecimal::TUint128 Cost;
    };
    TVector<TPair> pairs(Reserve(n - 1));
    for (size_t i = 0; i + 1 < n; ++i) {
        pairs.push_back({.Left = i, .Cost = FuseCost(summary.Entries_[i], summary.Entries_[i + 1])});
    }

    Sort(pairs, [](const TPair& a, const TPair& b) {
        return a.Cost < b.Cost;
    });

    // Mark entries absorbed into their right neighbour.
    TVector<bool> absorbed(n, false);
    TVector<bool> used(n, false);
    bool anyFused = false;

    for (const auto& pair : pairs) {
        if (used[pair.Left] || used[pair.Left + 1]) {
            continue;
        }
        if (ExceedsTwiceCap(pair.Cost, cap)) {
            break;
        }
        used[pair.Left] = true;
        used[pair.Left + 1] = true;
        absorbed[pair.Left] = true;
        anyFused = true;
    }

    if (!anyFused) {
        return false;
    }

    ApplyFusions(summary, absorbed);
    return true;
}

void TEqHeightHistogramBuilder::Compact(TSummary& summary, ui64 budgetBytes) const {
    Y_DEBUG_ABORT_UNLESS(summary.IsSorted(),
                         "eq-height histogram invariant: entries must be unique and sorted");
    const ui64 cap = Capacity();
    const ui64 overheadBytes = OverheadBytes();
    // Two triggers: byte budget (overBytes) and fusion-admissibility (overCap).
    // overCap is not a MaxRankError invariant: fusion keeps the right entry's
    // RankUncertainty, so a max sitting on the last entry cannot be reduced.
    // Finalize enforces total/B.
    for (;;) {
        const bool overBytes = SaturatedAddBytes(summary.Bytes_, overheadBytes) > budgetBytes;
        const ui64 maxUnc = summary.MaxRankUncertainty();
        const bool overCap = ExceedsTwiceCap(maxUnc, cap);
        if (!overBytes && !overCap) {
            return;
        }
        if (overBytes) {
            summary.BudgetForced_ = true;
        }
        if (summary.Entries_.size() <= 1) {
            return;
        }
        if (!FuseAdmissiblePairs(summary, cap) && !(overBytes && FuseLightestPair(summary))) {
            return;
        }
    }
}

void TEqHeightHistogramBuilder::Merge(const TEqHeightHistogramBuilder& other) {
    // Self-merge would double-count rows.
    YQL_ENSURE(this != &other, "TEqHeightHistogramBuilder::Merge: self-merge is not supported");
    YQL_ENSURE(Params_.NumBuckets == other.Params_.NumBuckets &&
                   Params_.EmissionRate == other.Params_.EmissionRate,
               "TEqHeightHistogramBuilder::Merge: NumBuckets and EmissionRate must match");
    Flush();

    if (other.TotalCount_ == 0) {
        return;
    }

    YQL_ENSURE(other.TotalCount_ <= Max<ui64>() - TotalCount_,
               "eq-height histogram TotalCount overflow");

    TSummary otherSummary = other.Fold();
    const bool otherBudgetForced = otherSummary.BudgetForced_;

    // InterleaveInto's bump handles disjoint ranges (no RankUncertainty when
    // ranges don't overlap).
    InterleaveInto(Summary_, MinKey_, std::move(otherSummary.Entries_), other.MinKey_);
    if (TotalCount_ == 0 || other.MinKey_ < MinKey_) {
        MinKey_ = other.MinKey_;
    }

    TotalCount_ += other.TotalCount_;
    Sorted_ = false;
    Summary_.BudgetForced_ |= otherBudgetForced;
    Compact(Summary_, SoftBudget());
}

TEqHeightHistogramIntermediateState TEqHeightHistogramBuilder::Serialize() const {
    const TSummary summary = Fold();
    TEqHeightHistogramIntermediateState state;
    state.SetBudgetForced(summary.BudgetForced_);
    auto* params = state.MutableParams();
    params->SetNumBuckets(Params_.NumBuckets);
    params->SetEmissionRate(Params_.EmissionRate);
    params->SetMaxStateBytes(Params_.MaxStateBytes);
    state.SetTotalCount(TotalCount_);
    state.SetMinKey(MinKey_);
    for (const auto& entry : summary.Entries_) {
        auto* protoEntry = state.AddEntries();
        protoEntry->SetUpperBound(entry.UpperBound);
        protoEntry->SetCount(entry.Count);
        protoEntry->SetRankUncertainty(entry.RankUncertainty);
        protoEntry->SetSingleKey(entry.SingleKey);
    }
    return state;
}

TMaybe<TEqHeightHistogramResult> TEqHeightHistogramBuilder::Finalize() const {
    const TSummary summary = Fold();
    if (TotalCount_ == 0 || summary.Entries_.empty()) {
        return Nothing();
    }
    // Reject budget-starved, not tiny domains.  BudgetForced separates
    // "budget starved us" from "data is small".
    if (summary.BudgetForced_ && summary.Entries_.size() < Max<ui32>(MIN_ENTRIES, Params_.NumBuckets)) {
        return Nothing();
    }

    const ui64 total = TotalCount_;
    const ui32 buckets = Params_.NumBuckets; // ValidateParams() guarantees > 0

    ui64 maxRankError = 0;
    TVector<TBucketOut> bucketOuts = BuildBuckets(summary, total, buckets, maxRankError);

    if (maxRankError > total / buckets) {
        return Nothing();
    }

    TEqHeightHistogramResult result;
    result.SetNumBuckets(buckets);
    result.SetTotalCount(total);
    result.SetMaxRankError(maxRankError);
    for (const auto& bucket : bucketOuts) {
        auto* protoBucket = result.AddBuckets();
        protoBucket->SetUpperBound(bucket.UpperBound);
        protoBucket->SetCumulativeCount(bucket.CumulativeCount);
    }
    return result;
}

} // namespace NKikimr
