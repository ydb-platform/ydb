#pragma once

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/hashes_calc.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/spilled_storage.h>
#include <ydb/library/yql/dq/comp_nodes/type_utils.h>

#include <util/str_stl.h>
#include <util/system/unaligned_mem.h>

#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace NKikimr::NMiniKQL {

inline constexpr size_t MaxMarkKeys = 32;
inline constexpr int MaxNullableMarkKeys = 5;

struct TMarkJoinKeys {
    TVector<ui32> Columns;
    ui32 ProbeNullable = 0;
    ui32 BuildNullable = 0;
};

using TMarkKeyBytes = std::basic_string<char, std::char_traits<char>, TMKQLAllocator<char>>;

struct TMarkKeyBytesHash {
    size_t operator()(const TMarkKeyBytes& key) const {
        return THash<TStringBuf>{}(TStringBuf(key.data(), key.size()));
    }
};

class TMarkKeyColumns {
  public:
    TMarkKeyColumns(const NPackedTuple::TTupleLayout* layout, const TMarkJoinKeys& keys)
        : Layout_(layout)
        , Columns_(keys.Columns.begin(), keys.Columns.end())
        , Row_(layout->TotalRowSize)
    {
        for (ui32 column : Columns_) {
            MKQL_ENSURE(column < Layout_->KeyColumnsNum, "mark key column is out of range");
        }
        for (ui32 column = 0; column < Layout_->KeyColumnsNum; ++column) {
            if (std::find(Columns_.begin(), Columns_.end(), column) == Columns_.end() &&
                !Layout_->EqualNullsKeyMask.Get(column)) {
                GroupColumns_.push_back(column);
            }
        }
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
        return std::any_of(GroupColumns_.begin(), GroupColumns_.end(),
                           [&](ui32 column) { return IsNull(row.PackedData, column); });
    }

    TSingleTuple WithNullMarks(TSingleTuple row, ui32 marks) {
        std::memcpy(Row_.data(), row.PackedData, Row_.size());
        for (ui32 index = 0; index < Columns_.size(); ++index) {
            if (!(marks & (1u << index))) {
                continue;
            }
            const ui32 column = Columns_[index];
            const auto& desc = Layout_->KeyColumns[column];
            std::memset(Row_.data() + desc.Offset, 0, desc.DataSize);
            Row_[Layout_->BitmaskOffset + column / 8] &= ~static_cast<ui8>(1u << (column % 8));
        }
        Rehash(Row_.data(), row.OverflowBegin);
        return {.PackedData = Row_.data(), .OverflowBegin = row.OverflowBegin};
    }

    void SerializeKey(TSingleTuple row, TMarkKeyBytes& key) const {
        const char* data = reinterpret_cast<const char*>(row.PackedData);
        key.assign(data + Layout_->KeyColumnsOffset, data + Layout_->KeyColumnsFixedEnd);
        for (ui32 column = 0; column < Layout_->KeyColumnsNum; ++column) {
            key.push_back(IsNull(row.PackedData, column) ? '\0' : '\1');
        }
        for (ui32 column = Layout_->KeyColumnsFixedNum; column < Layout_->KeyColumnsNum; ++column) {
            if (IsNull(row.PackedData, column)) {
                continue;
            }
            const auto& desc = Layout_->KeyColumns[column];
            const char* value = data + desc.Offset;
            const ui8 prefixSize = static_cast<ui8>(value[0]);
            if (prefixSize < 255) {
                key.append(value, prefixSize + 1);
                continue;
            }
            const ui32 overflowOffset = ReadUnaligned<ui32>(value + 1);
            const ui32 overflowSize = ReadUnaligned<ui32>(value + 1 + sizeof(ui32));
            key.append(value, 1);
            key.append(value + 1 + sizeof(ui32), sizeof(ui32));
            key.append(value + 1 + 2 * sizeof(ui32), desc.DataSize - 1 - 2 * sizeof(ui32));
            key.append(reinterpret_cast<const char*>(row.OverflowBegin) + overflowOffset, overflowSize);
        }
    }

  private:
    bool IsNull(const ui8* row, ui32 column) const {
        return !((row[Layout_->BitmaskOffset + column / 8] >> (column % 8)) & 1u);
    }

    void Rehash(ui8* row, const ui8* overflow) const {
        using TTraits = NSimd::TSimdFallbackTraits;
        ui32 hash = NPackedTuple::CalculateCRC32<TTraits>(row + Layout_->KeyColumnsOffset,
                                                          Layout_->KeyColumnsFixedEnd - Layout_->KeyColumnsOffset);
        for (ui32 column = Layout_->KeyColumnsFixedNum; column < Layout_->KeyColumnsNum; ++column) {
            if (!Layout_->HashVariableKey(row, column)) {
                continue;
            }
            const auto& desc = Layout_->KeyColumns[column];
            const ui8* value = row + desc.Offset;
            if (value[0] < 255) {
                hash = NPackedTuple::CalculateCRC32<TTraits>(value + 1, value[0], hash);
                continue;
            }
            const ui32 overflowOffset = ReadUnaligned<ui32>(value + 1);
            const ui32 overflowSize = ReadUnaligned<ui32>(value + 1 + sizeof(ui32));
            hash = NPackedTuple::CalculateCRC32<TTraits>(value + 1 + 2 * sizeof(ui32),
                                                         desc.DataSize - 1 - 2 * sizeof(ui32), hash);
            hash = NPackedTuple::CalculateCRC32<TTraits>(overflow + overflowOffset, overflowSize, hash);
        }
        WriteUnaligned<ui32>(row, hash);
    }

    const NPackedTuple::TTupleLayout* Layout_;
    TMKQLVector<ui32> Columns_;
    TMKQLVector<ui32> GroupColumns_;
    TMKQLVector<ui8> Row_;
};

class TMarkStageLookup {
  public:
    TMarkStageLookup(const NPackedTuple::TTupleLayout* layout, const TMarkJoinKeys& keys, ui32 nulls, ui32 probeNulls)
        : Columns_(layout, keys)
        , Nulls_(nulls)
        , ProbeNulls_(probeNulls)
    {}

    bool IsIdentity() const {
        return Nulls_ == 0;
    }

    bool Applicable(TSingleTuple row) const {
        return Columns_.NullMarks(row) == ProbeNulls_;
    }

    TSingleTuple Key(TSingleTuple row) {
        return Nulls_ == 0 ? row : Columns_.WithNullMarks(row, Nulls_);
    }

  private:
    TMarkKeyColumns Columns_;
    const ui32 Nulls_;
    const ui32 ProbeNulls_;
};

class TMarkKeyDedup {
  public:
    bool Insert(const TMarkKeyBytes& key) {
        if (Keys_.contains(key)) {
            return false;
        }
        if (Bytes_ > MaxBytes) {
            Clear();
        }
        Bytes_ += key.size() + sizeof(TMarkKeyBytes);
        Keys_.insert(key);
        return true;
    }

    void Clear() {
        TKeys().swap(Keys_);
        Bytes_ = 0;
    }

  private:
    static constexpr size_t MaxBytes = 1 << 20;
    using TKeys = std::unordered_set<TMarkKeyBytes, TMarkKeyBytesHash, std::equal_to<TMarkKeyBytes>,
                                     TMKQLAllocator<TMarkKeyBytes>>;
    TKeys Keys_;
    size_t Bytes_ = 0;
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

enum class EMarkStage {
    Wait,
    Started,
    Done,
};

template <TSpillerSettings Settings> class TMarkStages {
    struct TStage {
        ui32 Nulls = 0;
        ui32 BuildNulls = 0;

        ui32 ProbeNulls() const {
            return Nulls & ~BuildNulls;
        }
    };

    struct TStageBuild {
        TStageBuild(ISpiller::TPtr spiller, const NPackedTuple::TTupleLayout* layout)
            : Rows(std::move(spiller), layout)
        {}

        TBucketsSpiller<Settings> Rows;
        TMarkKeyDedup Seen;
        ui64 RowCount = 0;
    };

  public:
    TMarkStages(ISpiller::TPtr spiller, TSides<const NPackedTuple::TTupleLayout*> layouts, TMarkJoinKeys keys)
        : Spiller_(std::move(spiller))
        , Layouts_(layouts)
        , Keys_(std::move(keys))
        , ProbeColumns_(layouts.Probe, Keys_)
        , BuildColumns_(layouts.Build, Keys_)
    {
        Stages_.push_back({});
        Builds_.emplace_back();
        const ui32 nullable = Keys_.ProbeNullable | Keys_.BuildNullable;
        for (ui32 nulls = nullable; nulls != 0; nulls = (nulls - 1) & nullable) {
            const ui32 buildNullable = nulls & Keys_.BuildNullable;
            for (ui32 buildNulls = buildNullable;; buildNulls = (buildNulls - 1) & buildNullable) {
                if (((nulls & ~buildNulls) & ~Keys_.ProbeNullable) == 0) {
                    Stages_.push_back({.Nulls = nulls, .BuildNulls = buildNulls});
                    Builds_.push_back(std::make_unique<TStageBuild>(Spiller_, Layouts_.Build));
                }
                if (buildNulls == 0) {
                    break;
                }
            }
        }
        StartStage(0);
    }

    bool IsExact() const {
        return Index_ == 0;
    }

    TMarkStageLookup& Lookup() {
        return *Lookup_;
    }

    void AddBuildRow(TSingleTuple row) {
        if (Stages_.size() == 1 || BuildColumns_.HasNullGroupKey(row)) {
            return;
        }
        const ui32 nulls = BuildColumns_.NullMarks(row);
        for (int index = 1; index < std::ssize(Stages_); ++index) {
            const TStage& stage = Stages_[index];
            if ((stage.BuildNulls & ~nulls) != 0 || (nulls & ~stage.Nulls) != 0) {
                continue;
            }
            const TSingleTuple copy = BuildColumns_.WithNullMarks(row, stage.Nulls);
            BuildColumns_.SerializeKey(copy, KeyBytes_);
            TStageBuild& build = *Builds_[index];
            if (build.Seen.Insert(KeyBytes_)) {
                build.Rows.AddRow(copy);
                ++build.RowCount;
            }
        }
    }

    void FinishBuild() {
        for (int index = 1; index < std::ssize(Stages_); ++index) {
            if (Builds_[index]->RowCount == 0) {
                Builds_[index].reset();
                continue;
            }
            Builds_[index]->Seen.Clear();
            LastStage_[Stages_[index].ProbeNulls()] = index;
        }
    }

    bool Defer(TSingleTuple probeRow) {
        if (ProbeColumns_.HasNullGroupKey(probeRow)) {
            return false;
        }
        const ui32 nulls = ProbeColumns_.NullMarks(probeRow);
        const auto it = LastStage_.find(nulls);
        if (it == LastStage_.end() || it->second <= Index_) {
            return false;
        }
        Output_->AddRow(probeRow);
        OutputNulls_.insert(nulls);
        return true;
    }

    [[nodiscard]] ESpillResult SpillWhile(std::predicate auto condition) {
        if (Output_ && Output_->SpillWhile(condition) == ESpillResult::Spilling) {
            return ESpillResult::Spilling;
        }
        for (int index = Index_ + 1; index < std::ssize(Builds_); ++index) {
            if (Builds_[index] && Builds_[index]->Rows.SpillWhile(condition) == ESpillResult::Spilling) {
                return ESpillResult::Spilling;
            }
        }
        return condition() ? ESpillResult::DontHavePages : ESpillResult::FinishedSpilling;
    }

    EMarkStage NextStage() {
        const auto never = [] { return false; };
        if (Output_) {
            if (Output_->SpillWhile(never) == ESpillResult::Spilling) {
                return EMarkStage::Wait;
            }
            Input_.emplace(Spiller_, Layouts_.Probe, std::move(Output_->GetBuckets()));
            Output_.reset();
            InputNulls_ = std::move(OutputNulls_);
            OutputNulls_.clear();
        }
        int next = Index_ + 1;
        while (next < std::ssize(Stages_) && !StageNeeded(next)) {
            Builds_[next].reset();
            ++next;
        }
        if (next == std::ssize(Stages_)) {
            MKQL_ENSURE(!Input_ || Input_->Finished(), "mark join rows are left without a stage");
            return EMarkStage::Done;
        }
        if (Builds_[next]->Rows.SpillWhile(never) == ESpillResult::Spilling) {
            return EMarkStage::Wait;
        }
        Builds_[Index_].reset();
        StartStage(next);
        return EMarkStage::Started;
    }

    TBucketsSpiller<Settings> TakeBuild() {
        return std::move(Builds_[Index_]->Rows);
    }

    TStoredRows TakeProbe() {
        MKQL_ENSURE(Input_, "mark join stage has no probe rows");
        TStoredRows rows = std::move(*Input_);
        Input_.reset();
        return rows;
    }

  private:
    bool StageNeeded(int index) const {
        return Builds_[index] && InputNulls_.contains(Stages_[index].ProbeNulls());
    }

    void StartStage(int index) {
        Index_ = index;
        Output_.emplace(Spiller_, Layouts_.Probe, EBucketAssign::RoundRobin);
        const TStage& stage = Stages_[Index_];
        Lookup_.emplace(Layouts_.Probe, Keys_, stage.Nulls, stage.ProbeNulls());
    }

    ISpiller::TPtr Spiller_;
    TSides<const NPackedTuple::TTupleLayout*> Layouts_;
    TMarkJoinKeys Keys_;
    TMarkKeyColumns ProbeColumns_;
    TMarkKeyColumns BuildColumns_;
    TMKQLVector<TStage> Stages_;
    TMKQLVector<std::unique_ptr<TStageBuild>> Builds_;
    std::unordered_map<ui32, int> LastStage_;
    std::optional<TStoredRows> Input_;
    std::unordered_set<ui32> InputNulls_;
    std::optional<TBucketsSpiller<Settings>> Output_;
    std::unordered_set<ui32> OutputNulls_;
    TMarkKeyBytes KeyBytes_;
    int Index_ = 0;
    std::optional<TMarkStageLookup> Lookup_;
};

} // namespace NKikimr::NMiniKQL
