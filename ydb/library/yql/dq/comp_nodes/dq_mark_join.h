#pragma once

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/hashes_calc.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/neumann_hash_table.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/spilled_storage.h>
#include <ydb/library/yql/dq/comp_nodes/type_utils.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>

#include <util/system/unaligned_mem.h>

#include <algorithm>
#include <cstring>
#include <optional>
#include <string>

namespace NKikimr::NMiniKQL {

inline constexpr size_t MaxMarkKeys = 32;

enum class EMarkValue : ui8 {
    True = 0,
    False = 1,
    Null = 2,
};

struct TMarkJoinKeys {
    TVector<ui32> Columns;
    ui32 ProbeNullable = 0;
    ui32 BuildNullable = 0;
};

using TMarkKeyBytes = std::basic_string<char, std::char_traits<char>, TMKQLAllocator<char>>;

class TMarkKeyColumns {
  public:
    TMarkKeyColumns(const NPackedTuple::TTupleLayout* layout, const TMarkJoinKeys& keys)
        : Layout_(layout)
        , Columns_(keys.Columns.begin(), keys.Columns.end())
    {
        for (ui32 column : Columns_) {
            MKQL_ENSURE(column < Layout_->KeyColumnsNum, "mark key column is out of range");
        }
        for (ui32 column = 0; column < Layout_->KeyColumnsNum; ++column) {
            if (std::find(Columns_.begin(), Columns_.end(), column) != Columns_.end()) {
                continue;
            }
            GroupColumns_.push_back(column);
            if (!Layout_->EqualNullsKeyMask.Get(column)) {
                NullRejectingColumns_.push_back(column);
            }
        }
    }

    bool HasGroup() const {
        return !GroupColumns_.empty();
    }

    ui32 AllMarks() const {
        return Columns_.size() == 32 ? ~0u : (1u << Columns_.size()) - 1;
    }

    ui32 NullMarks(TSingleTuple row) const {
        ui32 nulls = 0;
        for (ui32 index = 0; index < Columns_.size(); ++index) {
            if (IsNull(row.PackedData, Columns_[index])) {
                nulls |= 1u << index;
            }
        }
        return nulls;
    }

    bool HasNullGroupKey(TSingleTuple row) const {
        return std::any_of(NullRejectingColumns_.begin(), NullRejectingColumns_.end(),
                           [&](ui32 column) { return IsNull(row.PackedData, column); });
    }

    void SerializeGroup(TSingleTuple row, TMarkKeyBytes& key) const {
        key.clear();
        for (ui32 column : GroupColumns_) {
            AppendColumn(row, column, key);
        }
    }

    ui32 GroupHash(TSingleTuple row, TMarkKeyBytes& key) const {
        SerializeGroup(row, key);
        return NPackedTuple::CalculateCRC32<NSimd::TSimdFallbackTraits>(reinterpret_cast<const ui8*>(key.data()),
                                                                        key.size());
    }

    bool GroupEqual(TSingleTuple lhs, TSingleTuple rhs) const {
        return std::all_of(GroupColumns_.begin(), GroupColumns_.end(),
                           [&](ui32 column) { return ColumnEqual(lhs, rhs, column); });
    }

    bool MarkEqual(TSingleTuple lhs, TSingleTuple rhs, ui32 index) const {
        return ColumnEqual(lhs, rhs, Columns_[index]);
    }

  private:
    bool ColumnEqual(TSingleTuple lhs, TSingleTuple rhs, ui32 column) const {
        const bool lhsNull = IsNull(lhs.PackedData, column);
        if (lhsNull != IsNull(rhs.PackedData, column)) {
            return false;
        }
        if (lhsNull) {
            return true;
        }
        const auto& desc = Layout_->KeyColumns[column];
        const ui8* lhsValue = lhs.PackedData + desc.Offset;
        const ui8* rhsValue = rhs.PackedData + desc.Offset;
        if (column < Layout_->KeyColumnsFixedNum) {
            return std::memcmp(lhsValue, rhsValue, desc.DataSize) == 0;
        }
        const ui8 prefixSize = lhsValue[0];
        if (prefixSize != rhsValue[0]) {
            return false;
        }
        if (prefixSize < 255) {
            return std::memcmp(lhsValue + 1, rhsValue + 1, prefixSize) == 0;
        }
        const ui32 lhsOffset = ReadUnaligned<ui32>(lhsValue + 1);
        const ui32 rhsOffset = ReadUnaligned<ui32>(rhsValue + 1);
        const ui32 size = ReadUnaligned<ui32>(lhsValue + 1 + sizeof(ui32));
        const ui32 inlineOffset = 1 + 2 * sizeof(ui32);
        return size == ReadUnaligned<ui32>(rhsValue + 1 + sizeof(ui32)) &&
               std::memcmp(lhsValue + inlineOffset, rhsValue + inlineOffset, desc.DataSize - inlineOffset) == 0 &&
               std::memcmp(lhs.OverflowBegin + lhsOffset, rhs.OverflowBegin + rhsOffset, size) == 0;
    }

    void AppendColumn(TSingleTuple row, ui32 column, TMarkKeyBytes& key) const {
        if (IsNull(row.PackedData, column)) {
            key.push_back('\0');
            return;
        }
        key.push_back('\1');
        const auto& desc = Layout_->KeyColumns[column];
        const char* value = reinterpret_cast<const char*>(row.PackedData) + desc.Offset;
        if (column < Layout_->KeyColumnsFixedNum) {
            key.append(value, desc.DataSize);
            return;
        }
        const ui8 prefixSize = static_cast<ui8>(value[0]);
        if (prefixSize < 255) {
            key.append(value, prefixSize + 1);
            return;
        }
        const ui32 overflowOffset = ReadUnaligned<ui32>(value + 1);
        const ui32 overflowSize = ReadUnaligned<ui32>(value + 1 + sizeof(ui32));
        key.append(value, 1);
        key.append(value + 1 + sizeof(ui32), sizeof(ui32));
        key.append(value + 1 + 2 * sizeof(ui32), desc.DataSize - 1 - 2 * sizeof(ui32));
        key.append(reinterpret_cast<const char*>(row.OverflowBegin) + overflowOffset, overflowSize);
    }

    bool IsNull(const ui8* row, ui32 column) const {
        return !((row[Layout_->BitmaskOffset + column / 8] >> (column % 8)) & 1u);
    }

    const NPackedTuple::TTupleLayout* Layout_;
    TMKQLVector<ui32> Columns_;
    TMKQLVector<ui32> GroupColumns_;
    TMKQLVector<ui32> NullRejectingColumns_;
};

class TMarkRowMatcher {
  public:
    TMarkRowMatcher(TSides<const NPackedTuple::TTupleLayout*> layouts, const TMarkJoinKeys& keys)
        : Probe(layouts.Probe, keys)
        , Build(layouts.Build, keys)
        , MarksCount_(keys.Columns.size())
    {}

    bool IsUnknown(TSingleTuple probeRow, ui32 probeNulls, TSingleTuple buildRow) {
        const ui32 nulls = probeNulls | Build.NullMarks(buildRow);
        if (nulls == 0) {
            return false;
        }
        for (ui32 index = 0; index < MarksCount_; ++index) {
            if (!(nulls & (1u << index)) && !Build.MarkEqual(probeRow, buildRow, index)) {
                return false;
            }
        }
        return true;
    }

    const TMarkKeyColumns Probe;
    const TMarkKeyColumns Build;

  private:
    const ui32 MarksCount_;
};

class TStoredRows {
  public:
    TStoredRows(ISpiller::TPtr spiller, const NPackedTuple::TTupleLayout* layout, TBuckets buckets)
        : Spiller_(std::move(spiller))
        , Layout_(layout)
    {
        for (TBucket& bucket : buckets) {
            bucket.DetatchBuildingPage();
            for (TPackResult& page : bucket.ReleaseInMemoryPages()) {
                Pages_.push_back(std::move(page));
            }
            if (bucket.SpilledPages) {
                Spilled_.insert(Spilled_.end(), bucket.SpilledPages->begin(), bucket.SpilledPages->end());
            }
        }
    }

    FetchResult<TPackResult> FetchRow() {
        if (auto page = GetBackOrNull(Pages_)) {
            return One<TPackResult>{std::move(*page)};
        }
        StartRead();
        if (!Read_) {
            return Finish{};
        }
        if (!Read_->IsReady()) {
            return Yield{};
        }
        std::optional<NYql::TChunkedBuffer> buffer = Read_->ExtractValueSync();
        Read_.reset();
        MKQL_ENSURE(buffer.has_value(), "spilled page is lost");
        TPackResult page = Parse(std::move(*buffer), Layout_);
        StartRead();
        return One<TPackResult>{std::move(page)};
    }

    bool Finished() const {
        return Pages_.empty() && Spilled_.empty() && !Read_;
    }

  private:
    void StartRead() {
        if (!Read_ && !Spilled_.empty()) {
            Read_ = Spiller_->Extract(*GetBackOrNull(Spilled_));
        }
    }

    ISpiller::TPtr Spiller_;
    const NPackedTuple::TTupleLayout* Layout_;
    TMKQLVector<TPackResult> Pages_;
    TMKQLVector<ISpiller::TKey> Spilled_;
    std::optional<TFuturePage> Read_;
};

template <TSpillerSettings Settings> class TMarkGroups {
    using TGroupTable = NPackedTuple::TNeumannHashTable<false, true, true>;

    struct TBucketGroups {
        TGroupTable NullRows;
        TGroupTable OtherRows;
        const ui8* Overflow = nullptr;
    };

    struct TGroupRows {
        TMKQLVector<ui32> Indices;
        TMKQLVector<ui32> Hashes;
    };

    static constexpr ui32 BucketMask = static_cast<ui32>(Settings.Buckets) - 1;

  public:
    TMarkGroups(TSides<const NPackedTuple::TTupleLayout*> layouts, const TMarkJoinKeys& keys)
        : Matcher_(layouts, keys)
        , BuildLayout_(layouts.Build)
        , Widths_{.Build = layouts.Build->TotalRowSize, .Probe = layouts.Probe->TotalRowSize}
        , HasMark_(!keys.Columns.empty())
        , ScansRows_(keys.Columns.size() > 1)
        , ProbeNullable_(keys.ProbeNullable != 0)
        , AllMarks_(Matcher_.Probe.AllMarks())
        , ByGroup_(HasMark_ && Matcher_.Build.HasGroup())
        , KeepsGroups_(ByGroup_ && (keys.ProbeNullable | keys.BuildNullable) != 0)
    {
        if (KeepsGroups_) {
            for (int bucket = 0; bucket < Settings.Buckets; ++bucket) {
                Groups_.push_back(EmptyGroups());
            }
        }
    }

    void Prepare(TPackResult& pack, ESide side) {
        if (!ByGroup_) {
            return;
        }
        const TMarkKeyColumns& columns = side == ESide::Build ? Matcher_.Build : Matcher_.Probe;
        const ui32 width = Widths_.SelectSide(side);
        for (i64 index = 0; index < pack.NTuples; ++index) {
            ui8* row = pack.PackedTuples.data() + index * width;
            const ui32 group = columns.GroupHash({.PackedData = row, .OverflowBegin = pack.Overflow.data()}, Key_);
            WriteUnaligned<ui32>(row, (ReadUnaligned<ui32>(row) & ~BucketMask) | (group & BucketMask));
        }
    }

    void AddBuildRow(TSingleTuple row) {
        if (ByGroup_ || !HasMark_) {
            return;
        }
        BuildNotEmpty_ = true;
        BuildHasNull_ = BuildHasNull_ || Matcher_.Build.NullMarks(row) != 0;
    }

    void AddBuildTable(int bucket, const auto& table) {
        if (!KeepsGroups_) {
            return;
        }
        const TPackResult& data = table.BuildData();
        const ui8* tuples = data.PackedTuples.data();
        const ui8* overflow = data.Overflow.data();
        TGroupRows nullRows;
        TGroupRows otherRows;
        for (i64 index = 0; index < data.NTuples; ++index) {
            const TSingleTuple row{.PackedData = tuples + index * Widths_.Build, .OverflowBegin = overflow};
            if (Matcher_.Build.HasNullGroupKey(row)) {
                continue;
            }
            const bool hasNull = Matcher_.Build.NullMarks(row) != 0;
            if (!hasNull && !ProbeNullable_) {
                continue;
            }
            TGroupRows& rows = hasNull ? nullRows : otherRows;
            rows.Indices.push_back(static_cast<ui32>(index));
            rows.Hashes.push_back(Matcher_.Build.GroupHash(row, Key_));
        }
        TBucketGroups& groups = Groups_[bucket];
        groups = EmptyGroups();
        groups.Overflow = overflow;
        groups.NullRows.Build(tuples, overflow, nullRows.Indices, nullRows.Hashes);
        groups.OtherRows.Build(tuples, overflow, otherRows.Indices, otherRows.Hashes);
    }

    void ReleaseGroups() {
        for (TBucketGroups& groups : Groups_) {
            groups = EmptyGroups();
        }
    }

    bool SkipsProbeRow(TSingleTuple row) const {
        return Matcher_.Probe.HasNullGroupKey(row) || (!ByGroup_ && MarkNulls(row) != 0);
    }

    bool NeedsLookup(TSingleTuple row) const {
        return MarkNulls(row) == 0;
    }

    EMarkValue Unmatched(TSingleTuple row) {
        return IsNull(row) ? EMarkValue::Null : EMarkValue::False;
    }

  private:
    TBucketGroups EmptyGroups() const {
        return {.NullRows = TGroupTable(BuildLayout_), .OtherRows = TGroupTable(BuildLayout_)};
    }

    bool IsNull(TSingleTuple row) {
        if (!HasMark_ || Matcher_.Probe.HasNullGroupKey(row)) {
            return false;
        }
        const ui32 nulls = MarkNulls(row);
        if (!ByGroup_) {
            return nulls != 0 ? BuildNotEmpty_ : BuildHasNull_;
        }
        if (!KeepsGroups_) {
            return false;
        }
        const TBucketGroups& groups = Groups_[Settings.BucketIndex(row)];
        const ui32 hash = Matcher_.Probe.GroupHash(row, Key_);
        const bool any = !ScansRows_ || nulls == AllMarks_;
        const auto accept = [&](TSingleTuple buildRow) { return any || Matcher_.IsUnknown(row, nulls, buildRow); };
        return Contains(groups, groups.NullRows, row, hash, accept) ||
               (nulls != 0 && Contains(groups, groups.OtherRows, row, hash, accept));
    }

    bool Contains(const TBucketGroups& groups, const TGroupTable& table, TSingleTuple row, ui32 hash,
                  std::predicate<TSingleTuple> auto accept) const {
        if (table.Empty()) {
            return false;
        }
        const auto groupEqual = [&](const ui8* lhs, const ui8* lhsOverflow, const ui8* rhs, const ui8* rhsOverflow) {
            return Matcher_.Build.GroupEqual({.PackedData = lhs, .OverflowBegin = lhsOverflow},
                                             {.PackedData = rhs, .OverflowBegin = rhsOverflow});
        };
        bool found = false;
        table.Apply(row.PackedData, row.OverflowBegin, hash, groupEqual, [&](const ui8* buildRow) {
            found = accept({.PackedData = buildRow, .OverflowBegin = groups.Overflow});
            return !found;
        });
        return found;
    }

    ui32 MarkNulls(TSingleTuple row) const {
        return HasMark_ ? Matcher_.Probe.NullMarks(row) : 0;
    }

    TMarkRowMatcher Matcher_;
    const NPackedTuple::TTupleLayout* BuildLayout_;
    const TSides<ui32> Widths_;
    const bool HasMark_;
    const bool ScansRows_;
    const bool ProbeNullable_;
    const ui32 AllMarks_;
    const bool ByGroup_;
    const bool KeepsGroups_;
    bool BuildNotEmpty_ = false;
    bool BuildHasNull_ = false;
    TMKQLVector<TBucketGroups> Groups_;
    TMarkKeyBytes Key_;
};

template <TSpillerSettings Settings> class TMarkScan {
  public:
    TMarkScan(ISpiller::TPtr spiller, TSides<const NPackedTuple::TTupleLayout*> layouts, const TMarkJoinKeys& keys)
        : Spiller_(std::move(spiller))
        , Layouts_(layouts)
        , Matcher_(layouts, keys)
        , AllMarks_(Matcher_.Probe.AllMarks())
        , ProbeNullable_(keys.ProbeNullable != 0)
        , Rows_(std::in_place, Spiller_, layouts.Build, EBucketAssign::RoundRobin)
        , Deferred_(std::in_place, Spiller_, layouts.Probe, EBucketAssign::RoundRobin)
    {}

    void AddBuildRow(TSingleTuple row) {
        BuildNotEmpty_ = true;
        const ui32 nulls = Matcher_.Build.NullMarks(row);
        BuildAllNull_ = BuildAllNull_ || nulls == AllMarks_;
        BuildHasNull_ = BuildHasNull_ || nulls != 0;
        if (nulls != 0 || ProbeNullable_) {
            Rows_->AddRow(row);
        }
    }

    bool SkipsProbeRow(TSingleTuple row) const {
        return Matcher_.Probe.NullMarks(row) != 0;
    }

    std::optional<EMarkValue> Unmatched(TSingleTuple row) {
        if (!BuildNotEmpty_) {
            return EMarkValue::False;
        }
        const ui32 nulls = Matcher_.Probe.NullMarks(row);
        if (nulls == AllMarks_ || BuildAllNull_) {
            return EMarkValue::Null;
        }
        if (nulls == 0 && !BuildHasNull_) {
            return EMarkValue::False;
        }
        Deferred_->AddRow(row);
        HasDeferred_ = true;
        return std::nullopt;
    }

    [[nodiscard]] ESpillResult SpillWhile(std::predicate auto condition) {
        if (Rows_ && Rows_->SpillWhile(condition) == ESpillResult::Spilling) {
            return ESpillResult::Spilling;
        }
        if (Deferred_ && Deferred_->SpillWhile(condition) == ESpillResult::Spilling) {
            return ESpillResult::Spilling;
        }
        return condition() ? ESpillResult::DontHavePages : ESpillResult::FinishedSpilling;
    }

    EFetchResult Refine(std::invocable<TSingleTuple, EMarkValue> auto emit, std::predicate auto isFull,
                        std::predicate auto lowMemory) {
        const auto never = [] { return false; };
        while (true) {
            if (!Input_) {
                if (!HasDeferred_) {
                    return EFetchResult::Finish;
                }
                if (Deferred_->SpillWhile(never) == ESpillResult::Spilling) {
                    return EFetchResult::Yield;
                }
                if (!Source_) {
                    if (Rows_->SpillWhile(never) == ESpillResult::Spilling) {
                        return EFetchResult::Yield;
                    }
                    Source_.emplace(Spiller_, Layouts_.Build, std::move(Rows_->GetBuckets()));
                    Rows_.reset();
                }
                Input_.emplace(Spiller_, Layouts_.Probe, std::move(Deferred_->GetBuckets()));
                Deferred_.emplace(Spiller_, Layouts_.Probe, EBucketAssign::RoundRobin);
                HasDeferred_ = false;
                Block_.clear();
                BlockLoaded_ = false;
            }
            if (!BlockLoaded_) {
                while (Block_.empty() || !lowMemory()) {
                    FetchResult<TPackResult> page = Source_->FetchRow();
                    const NYql::NUdf::EFetchStatus status = AsStatus(page);
                    if (status == NYql::NUdf::EFetchStatus::Yield) {
                        return EFetchResult::Yield;
                    }
                    if (status == NYql::NUdf::EFetchStatus::Finish) {
                        break;
                    }
                    Block_.push_back(std::move(GetPayload(page)));
                }
                LastBlock_ = Source_->Finished();
                BlockLoaded_ = true;
            }
            if (!Page_) {
                FetchResult<TPackResult> page = Input_->FetchRow();
                const NYql::NUdf::EFetchStatus status = AsStatus(page);
                if (status == NYql::NUdf::EFetchStatus::Yield) {
                    return EFetchResult::Yield;
                }
                if (status == NYql::NUdf::EFetchStatus::Finish) {
                    Input_.reset();
                    if (LastBlock_) {
                        MKQL_ENSURE(!HasDeferred_, "mark join rows are left after the last block");
                        Block_.clear();
                        return EFetchResult::Finish;
                    }
                    continue;
                }
                Page_ = std::move(GetPayload(page));
                PageIndex_ = 0;
            }
            i64 index = 0;
            for (TSingleTuple row : *Page_) {
                if (index++ < PageIndex_) {
                    continue;
                }
                PageIndex_ = index;
                if (HasUnknown(row)) {
                    emit(row, EMarkValue::Null);
                } else if (LastBlock_) {
                    emit(row, EMarkValue::False);
                } else {
                    Deferred_->AddRow(row);
                    HasDeferred_ = true;
                    continue;
                }
                if (isFull()) {
                    return EFetchResult::One;
                }
            }
            Page_.reset();
        }
    }

  private:
    bool HasUnknown(TSingleTuple row) {
        const ui32 nulls = Matcher_.Probe.NullMarks(row);
        for (const TPackResult& page : Block_) {
            for (TSingleTuple buildRow : page) {
                if (Matcher_.IsUnknown(row, nulls, buildRow)) {
                    return true;
                }
            }
        }
        return false;
    }

    ISpiller::TPtr Spiller_;
    TSides<const NPackedTuple::TTupleLayout*> Layouts_;
    TMarkRowMatcher Matcher_;
    const ui32 AllMarks_;
    const bool ProbeNullable_;
    bool BuildNotEmpty_ = false;
    bool BuildAllNull_ = false;
    bool BuildHasNull_ = false;
    std::optional<TBucketsSpiller<Settings>> Rows_;
    std::optional<TBucketsSpiller<Settings>> Deferred_;
    bool HasDeferred_ = false;
    std::optional<TStoredRows> Source_;
    std::optional<TStoredRows> Input_;
    TMKQLVector<TPackResult> Block_;
    bool BlockLoaded_ = false;
    bool LastBlock_ = false;
    std::optional<TPackResult> Page_;
    i64 PageIndex_ = 0;
};

template <TSpillerSettings Settings> class TMarkJoin {
  public:
    TMarkJoin(ISpiller::TPtr spiller, TSides<const NPackedTuple::TTupleLayout*> layouts, const TMarkJoinKeys& keys) {
        if (keys.Columns.size() > 1 && !TMarkKeyColumns(layouts.Build, keys).HasGroup()) {
            Scan_.emplace(std::move(spiller), layouts, keys);
        } else {
            Groups_.emplace(layouts, keys);
        }
    }

    void Prepare(TPackResult& pack, ESide side) {
        if (Groups_) {
            Groups_->Prepare(pack, side);
        }
    }

    void AddBuildRow(TSingleTuple row) {
        if (Scan_) {
            Scan_->AddBuildRow(row);
        } else {
            Groups_->AddBuildRow(row);
        }
    }

    void AddBuildTable(int bucket, const auto& table) {
        if (Groups_) {
            Groups_->AddBuildTable(bucket, table);
        }
    }

    void ReleaseGroups() {
        if (Groups_) {
            Groups_->ReleaseGroups();
        }
    }

    bool SkipsProbeRow(TSingleTuple row) const {
        return Scan_ ? Scan_->SkipsProbeRow(row) : Groups_->SkipsProbeRow(row);
    }

    bool NeedsLookup(TSingleTuple row) const {
        return Scan_ ? !Scan_->SkipsProbeRow(row) : Groups_->NeedsLookup(row);
    }

    std::optional<EMarkValue> Unmatched(TSingleTuple row) {
        return Scan_ ? Scan_->Unmatched(row) : Groups_->Unmatched(row);
    }

    [[nodiscard]] ESpillResult SpillWhile(std::predicate auto condition) {
        if (Scan_) {
            return Scan_->SpillWhile(condition);
        }
        return condition() ? ESpillResult::DontHavePages : ESpillResult::FinishedSpilling;
    }

    EFetchResult Refine(std::invocable<TSingleTuple, EMarkValue> auto emit, std::predicate auto isFull,
                        std::predicate auto lowMemory) {
        return Scan_ ? Scan_->Refine(emit, isFull, lowMemory) : EFetchResult::Finish;
    }

  private:
    std::optional<TMarkScan<Settings>> Scan_;
    std::optional<TMarkGroups<Settings>> Groups_;
};

} // namespace NKikimr::NMiniKQL
