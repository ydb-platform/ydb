#include "dq_block_hash_join.h"

#include <yql/essentials/minikql/comp_nodes/mkql_blocks.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <arrow/array/concatenate.h>
#include <arrow/scalar.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/bitmap_ops.h>

#include "dq_join_common.h"
#include "dq_join_filters.h"

namespace NKikimr::NMiniKQL {

namespace {

using TDqJoinImplRenames = TDqRenames<ESide>;

constexpr ui32 BaseInputs = 8;

struct TDqBlockJoinContext {
    TSides<TVector<TBlockType*>> InputTypes;
    TSides<TVector<ui32>> KeyColumns;
    TVector<TBlockType*> ResultItemTypes;
    TDqJoinImplRenames Renames;
    EJoinKind Kind;
    TSides<i32> TempStateIndes;
    TBlockHashJoinSettings Settings;
    // Pre-computed during graph construction in WrapDqBlockHashJoin using the
    // program's TTypeEnvironment.  This avoids creating TOptionalType objects
    // at runtime (inside DoCalculate) whose lifetime depends on the
    // TComputationContext – which may differ between iterations/retries.
    TSides<TVector<TType*>> UserTypes;
    TSides<TVector<int>> ColumnPermutation;
};

struct TProbeRefs {
    bool Enabled = false;
    bool ForceFlush = false;
    TVector<TVector<arrow::Datum>> Blocks;
    TVector<size_t> RefBegins;
    const ui8* Begin = nullptr;
    const ui8* End = nullptr;
    ui32 RowSize = 0;
    TVector<TSingleTuple> Build;
    TVector<ui64> Probe;

    void AddBlock(TVector<arrow::Datum> columns, const TPackResult& pack, ui32 rowSize) {
        if (!Blocks.empty() && RefBegins.back() == Probe.size()) {
            Blocks.pop_back();
            RefBegins.pop_back();
        }
        RefBegins.push_back(Probe.size());
        Blocks.push_back(std::move(columns));
        Begin = pack.PackedTuples.data();
        End = Begin + pack.PackedTuples.size();
        RowSize = rowSize;
    }

    bool TryAdd(TSingleTuple build, TSingleTuple probe) {
        if (probe.PackedData < Begin || probe.PackedData >= End) {
            return false;
        }
        Build.push_back(build);
        Probe.push_back((probe.PackedData - Begin) / RowSize);
        return true;
    }

    void Reset() {
        Build.clear();
        Probe.clear();
        ForceFlush = false;
        if (Begin) {
            TVector<arrow::Datum> last = std::move(Blocks.back());
            Blocks.clear();
            RefBegins.clear();
            Blocks.push_back(std::move(last));
            RefBegins.push_back(0);
        } else {
            Blocks.clear();
            RefBegins.clear();
        }
    }
};

class TBlockPackedTupleSource : public NNonCopyable::TMoveOnly {
  public:
    TBlockPackedTupleSource(TComputationContext& ctx, TSides<IComputationNode*> stream,
                            const TDqBlockJoinContext* meta,
                            TSides<std::unique_ptr<IBlockLayoutConverter>>& converters, ESide side,
                            TProbeRefs* refs = nullptr)
        : Side_(side)
        , Meta_(meta)
        , ArrowPool_(&ctx.ArrowMemoryPool)
        , Stream_(stream.SelectSide(side))
        , StreamValues_(Stream_->GetValue(ctx))
        , Buff_(ctx.MutableValues.get() + meta->TempStateIndes.SelectSide(side), meta->InputTypes.SelectSide(side).size())
        , ArrowBlockToInternalConverter_(converters.SelectSide(side).get())
        , ColumnPermutation_(meta->ColumnPermutation.SelectSide(side))
        , Refs_(refs)
    {}

    bool Finished() const {
        return Finished_;
    }

    size_t UserDataCols() const {
        return Buff_.size() - 1;
    }

    FetchResult<IBlockLayoutConverter::TPackResult> FetchRow() {
        if (Finished()) {
            return Finish{};
        }
        auto res = StreamValues_.WideFetch(Buff_.data(), Buff_.size());
        if (res != NYql::NUdf::EFetchStatus::Ok) {
            if (res == NYql::NUdf::EFetchStatus::Finish) {
                Finished_ = true;
                if (Refs_ && Refs_->Enabled) {
                    Refs_->Begin = nullptr;
                    Refs_->End = nullptr;
                    if (!Refs_->Probe.empty()) {
                        Refs_->ForceFlush = true;
                        return Yield{};
                    }
                }
                return Finish{};
            }
            return Yield{};
        }

        IBlockLayoutConverter::TPackResult result;
        const size_t cols = UserDataCols();
        if (cols == 0) {
            MKQL_ENSURE(Meta_->KeyColumns.Build.empty(), "empty payload side is only allowed for a keyless join");
            const auto* layout = ArrowBlockToInternalConverter_->GetTupleLayout();
            const ui64 n = GetBlockCount(Buff_[0]);
            result.PackedTuples.resize(layout->TotalRowSize * n, 0);
            result.NTuples = n;
            return One{std::move(result)};
        }

        TVector<arrow::Datum> columns = ArrowFromUV({Buff_.data(), cols});
        if (!ColumnPermutation_.empty()) {
            TVector<arrow::Datum> permuted(cols);
            for (size_t j = 0; j < cols; ++j) {
                permuted[j] = std::move(columns[ColumnPermutation_[j]]);
            }
            columns = std::move(permuted);
        }
        NormalizeScalarColumns(columns);
        ArrowBlockToInternalConverter_->Pack(columns, result);
        if (Refs_ && Refs_->Enabled) {
            Refs_->AddBlock(std::move(columns), result, ArrowBlockToInternalConverter_->GetTupleLayout()->TotalRowSize);
        }
        return One{std::move(result)};
    }

  private:
    TVector<arrow::Datum> ArrowFromUV(std::span<const NYql::NUdf::TUnboxedValue> UVs) {
        TVector<arrow::Datum> arrow;
        for (const auto& uv : UVs) {
            arrow.push_back(TArrowBlock::From(uv).GetDatum());
        }
        return arrow;
    }

    void NormalizeScalarColumns(TVector<arrow::Datum>& columns) {
        bool hasScalar = false;
        for (const auto& column : columns) {
            if (column.is_scalar()) {
                hasScalar = true;
                break;
            }
        }
        if (!hasScalar) {
            return;
        }

        const ui64 blockLen = GetBlockCount(Buff_[UserDataCols()]);
        MKQL_ENSURE(blockLen > 0, "Got a scalar column in a zero-length block");

        const auto& inputTypes = Meta_->InputTypes.SelectSide(Side_);
        for (size_t j = 0; j < columns.size(); ++j) {
            if (columns[j].is_scalar()) {
                TType* itemType = inputTypes[j]->GetItemType();
                columns[j] = MakeArrayFromScalar(*columns[j].scalar(), blockLen, itemType, *ArrowPool_);
            }
        }
    }

    bool Finished_ = false;
    ESide Side_;
    const TDqBlockJoinContext* Meta_;
    arrow::MemoryPool* ArrowPool_;
    IComputationNode* Stream_;
    NYql::NUdf::TUnboxedValue StreamValues_;
    std::span<NYql::NUdf::TUnboxedValue> Buff_;
    IBlockLayoutConverter* ArrowBlockToInternalConverter_;
    TVector<int> ColumnPermutation_;
    TProbeRefs* Refs_;
};

template<TPhysicalJoin Join>
struct TRenamesPackedTupleOutput : TPackedTupleOutputBase<Join, IBlockLayoutConverter> {
    using TBase = TPackedTupleOutputBase<Join, IBlockLayoutConverter>;

    static constexpr bool ByRef = !Join.IsGrid && (Join.Kind == EJoinKind::Inner ||
        (Join.Preserved == ESide::Probe && (Join.Kind == EJoinKind::Left || LeftSemiOrOnly(Join.Kind))));

    TRenamesPackedTupleOutput(const TDqBlockJoinContext* meta, TSides<IBlockLayoutConverter*> converters,
                              const TVector<TType*>& userNullTypes, arrow::MemoryPool& arrowPool, TProbeRefs* refs)
        : TBase(&meta->Renames, converters)
        , Meta_(meta)
        , Pool_(&arrowPool)
        , Refs_(refs)
        , Layouts_{.Build = converters.Build->GetTupleLayout(), .Probe = converters.Probe->GetTupleLayout()}
        , RefRowBytes_((LeftSemiOrOnly(Join.Kind) ? 0 : Layouts_.Build->TotalRowSize) + Layouts_.Probe->TotalRowSize)
        , BuildRowSize_(LeftSemiOrOnly(Join.Kind) ? 0 : Layouts_.Build->TotalRowSize)
        , BuildVariableBytes_(Layouts_.Build->VariableColumns.size(), 0)
    {
        Refs_->Enabled = ByRef;
        if constexpr (!std::is_same_v<typename TBase::BuildNullIfNeeded, typename TBase::Empty>) {
            TVector<arrow::Datum> nulls;
            for(auto* type:userNullTypes) {
                auto strname = type->GetKindAsStr();
                MKQL_ENSURE(type->IsOptional(), Sprintf("expected every type of right side to be optional when join type is Left, got type №%i: %s  ", nulls.size()+1, strname.data()));
                int blockSize = NMiniKQL::CalcBlockLen(NMiniKQL::CalcMaxBlockItemSize(type));
                auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), type, arrowPool, blockSize, nullptr);
                builder->Add(NYql::NUdf::TBlockItem{});
                nulls.push_back(builder->Build(true));
            }
            this->Converters_.SelectSide(Join.NullSupplying())->Pack(nulls, this->Nulls_);
        }
    }

    struct TFlushResult {
        TVector<arrow::Datum> Columns;
        i64 Rows;
    };

    i64 SizeTuples() const {
        return Refs_->Probe.empty() ? TBase::SizeTuples() : std::ssize(Refs_->Probe);
    }

    i64 SizeBytes() const {
        if (Refs_->Blocks.size() >= MaxRefBlocks) {
            return std::numeric_limits<i64>::max();
        }
        return TBase::SizeBytes() + std::ssize(Refs_->Probe) * RefRowBytes_ + RefOverflowBytes_;
    }

    static i64 OverflowBytes(const NPackedTuple::TTupleLayout* layout, const ui8* row) {
        i64 bytes = 0;
        for (const auto& col : layout->VariableColumns) {
            if (ReadUnaligned<ui8>(row + col.Offset) == 255) {
                bytes += ReadUnaligned<ui32>(row + col.Offset + 1 + sizeof(ui32));
            }
        }
        return bytes;
    }

    bool TryAddRef(TSingleTuple build, TSingleTuple probe) {
        if (!Refs_->TryAdd(build, probe)) {
            return false;
        }
        if (BuildRowSize_ > 64) {
            for (ui32 line = 64; line < BuildRowSize_; line += 64) {
                __builtin_prefetch(build.PackedData + line);
            }
            __builtin_prefetch(build.PackedData + BuildRowSize_ - 1);
        }
        if constexpr (!LeftSemiOrOnly(Join.Kind)) {
            const auto& columns = Layouts_.Build->VariableColumns;
            for (ui32 i = 0; i < columns.size(); ++i) {
                const ui8* cell = build.PackedData + columns[i].Offset;
                const ui32 size = ReadUnaligned<ui8>(cell);
                if (size == 255) {
                    const ui32 overflow = ReadUnaligned<ui32>(cell + 1 + sizeof(ui32));
                    RefOverflowBytes_ += overflow;
                    BuildVariableBytes_[i] += columns[i].DataSize - 1 - 2 * sizeof(ui32) + overflow;
                } else {
                    BuildVariableBytes_[i] += size;
                }
            }
        }
        if (!Layouts_.Probe->VariableColumns.empty()) {
            RefOverflowBytes_ += OverflowBytes(Layouts_.Probe, probe.PackedData);
        }
        return true;
    }

    bool NeedsFlush() const {
        return Refs_->ForceFlush;
    }

    auto MakeConsumeFn() {
        struct ConsumeFn {
            TRenamesPackedTupleOutput& Self;
            decltype(std::declval<TBase&>().MakeConsumeFn()) Base;

            void operator()(TSides<TSingleTuple> tuples) {
                if constexpr (ByRef) {
                    if (Self.TryAddRef(tuples.Build, tuples.Probe)) {
                        return;
                    }
                    MKQL_ENSURE(Self.Refs_->Probe.empty(), "mixed output by reference and by copy");
                }
                Base(tuples);
            }

            void operator()(TSingleTuple tuple) {
                if constexpr (ByRef && Join.Kind == EJoinKind::Left) {
                    const TSingleTuple null{.PackedData = Self.Nulls_.PackedTuples.data(),
                                            .OverflowBegin = Self.Nulls_.Overflow.data()};
                    if (Self.TryAddRef(null, tuple)) {
                        return;
                    }
                    MKQL_ENSURE(Self.Refs_->Probe.empty(), "mixed output by reference and by copy");
                } else if constexpr (ByRef && LeftSemiOrOnly(Join.Kind)) {
                    if (Self.TryAddRef(tuple, tuple)) {
                        return;
                    }
                    MKQL_ENSURE(Self.Refs_->Probe.empty(), "mixed output by reference and by copy");
                }
                Base(tuple);
            }
        };
        return ConsumeFn{*this, TBase::MakeConsumeFn()};
    }

    TFlushResult Flush() {
        if (!Refs_->Probe.empty()) {
            return FlushRefs();
        }
        TFlushResult res;
        res.Rows = this->SizeTuples();
        res.Columns = FlushAndApplyRenames();
        return res;
    }

    template <typename T>
    static void GatherValues(const arrow::ArrayData& src, const ui64* idx, size_t count, T* dst) {
        const T* values = src.GetValues<T>(1);
        for (size_t i = 0; i < count; ++i) {
            dst[i] = values[idx[i]];
        }
    }

    std::optional<arrow::Datum> GatherFixedProbeColumn(ui32 column) {
        const auto& refs = *Refs_;
        const auto& first = *refs.Blocks.front()[column].array();
        const auto* fixed = dynamic_cast<const arrow::FixedWidthType*>(first.type.get());
        if (!fixed || first.type->id() == arrow::Type::BOOL || first.buffers.size() != 2 || !first.child_data.empty()) {
            return std::nullopt;
        }
        const i64 width = fixed->bit_width() / 8;
        if (width != 1 && width != 2 && width != 4 && width != 8) {
            return std::nullopt;
        }
        bool hasNulls = false;
        for (const auto& block : refs.Blocks) {
            const auto& data = *block[column].array();
            if (data.buffers.size() != 2 || !data.type->Equals(*first.type)) {
                return std::nullopt;
            }
            hasNulls |= data.buffers[0] != nullptr && data.GetNullCount() != 0;
        }
        const size_t rows = refs.Probe.size();
        std::shared_ptr<arrow::Buffer> values = ARROW_RESULT(arrow::AllocateBuffer(rows * width, Pool_));
        std::shared_ptr<arrow::Buffer> bitmap;
        ui8* bits = nullptr;
        if (hasNulls) {
            bitmap = ARROW_RESULT(arrow::AllocateBitmap(rows, Pool_));
            bits = bitmap->mutable_data();
        }
        ui8* dst = values->mutable_data();
        for (size_t b = 0; b < refs.Blocks.size(); ++b) {
            const auto& data = *refs.Blocks[b][column].array();
            const size_t from = refs.RefBegins[b];
            const size_t to = b + 1 < refs.Blocks.size() ? refs.RefBegins[b + 1] : rows;
            const ui64* idx = refs.Probe.data() + from;
            const size_t count = to - from;
            if (ProbeRuns_[b]) {
                std::memcpy(dst + from * width, data.GetValues<ui8>(1, (data.offset + idx[0]) * width), count * width);
                if (bits) {
                    if (data.buffers[0]) {
                        arrow::internal::CopyBitmap(data.buffers[0]->data(), data.offset + idx[0], count, bits, from);
                    } else {
                        arrow::BitUtil::SetBitsTo(bits, from, count, true);
                    }
                }
                continue;
            }
            switch (width) {
            case 1: GatherValues<ui8>(data, idx, count, reinterpret_cast<ui8*>(dst) + from); break;
            case 2: GatherValues<ui16>(data, idx, count, reinterpret_cast<ui16*>(dst) + from); break;
            case 4: GatherValues<ui32>(data, idx, count, reinterpret_cast<ui32*>(dst) + from); break;
            case 8: GatherValues<ui64>(data, idx, count, reinterpret_cast<ui64*>(dst) + from); break;
            }
            if (bits) {
                const ui8* srcBits = data.buffers[0] ? data.buffers[0]->data() : nullptr;
                for (size_t i = 0; i < count; ++i) {
                    arrow::BitUtil::SetBitTo(bits, from + i, !srcBits || arrow::BitUtil::GetBit(srcBits, data.offset + idx[i]));
                }
            }
        }
        return arrow::ArrayData::Make(first.type, rows, {bitmap, values}, hasNulls ? arrow::kUnknownNullCount : 0);
    }

    arrow::Datum TakeProbeColumn(ui32 column) {
        if (Refs_->Blocks.size() == 1 && ProbeRuns_[0]) {
            return Refs_->Blocks[0][column].array()->Slice(Refs_->Probe.front(), Refs_->Probe.size());
        }
        if (auto fixed = GatherFixedProbeColumn(column)) {
            return *fixed;
        }
        const auto& refs = *Refs_;
        auto builder = MakeArrayBuilder(TTypeInfoHelper(), Meta_->UserTypes.Probe[column], *Pool_, refs.Probe.size(), nullptr);
        for (size_t b = 0; b < refs.Blocks.size(); ++b) {
            NYql::NUdf::IArrayBuilder::TArrayDataItem item{.Data = refs.Blocks[b][column].array().get(), .StartOffset = 0};
            const size_t from = refs.RefBegins[b];
            const size_t to = b + 1 < refs.Blocks.size() ? refs.RefBegins[b + 1] : refs.Probe.size();
            if (ProbeRuns_[b]) {
                builder->AddMany(&item, 1, refs.Probe[from], to - from);
            } else {
                builder->AddMany(&item, 1, refs.Probe.data() + from, to - from);
            }
        }
        arrow::Datum res = builder->Build(true);
        if (res.is_arraylike() && res.kind() == arrow::Datum::CHUNKED_ARRAY) {
            const auto& chunks = res.chunked_array()->chunks();
            if (chunks.size() == 1) {
                return chunks.front();
            }
            return ARROW_RESULT(arrow::Concatenate(chunks, Pool_));
        }
        return res;
    }

    TFlushResult FlushRefs() {
        TFlushResult res;
        res.Rows = std::ssize(Refs_->Probe);
        TVector<arrow::Datum> build;
        if constexpr (!LeftSemiOrOnly(Join.Kind)) {
            this->Converters_.Build->GatherUnpack(Refs_->Build.data(), Refs_->Build.size(), BuildVariableBytes_.data(), build);
        }
        const auto& refs = *Refs_;
        ProbeRuns_.assign(refs.Blocks.size(), false);
        for (size_t b = 0; b < refs.Blocks.size(); ++b) {
            const size_t from = refs.RefBegins[b];
            const size_t to = b + 1 < refs.Blocks.size() ? refs.RefBegins[b + 1] : refs.Probe.size();
            bool run = from < to;
            for (size_t i = from + 1; run && i < to; ++i) {
                run = refs.Probe[i] == refs.Probe[i - 1] + 1;
            }
            ProbeRuns_[b] = run;
        }
        THashMap<ui32, arrow::Datum> probe;
        for (auto rename : *this->Renames_) {
            if (rename.Side == ESide::Build) {
                res.Columns.push_back(build[rename.Index]);
            } else {
                auto it = probe.find(rename.Index);
                if (it == probe.end()) {
                    it = probe.emplace(rename.Index, TakeProbeColumn(rename.Index)).first;
                }
                res.Columns.push_back(it->second);
            }
        }
        Refs_->Reset();
        RefOverflowBytes_ = 0;
        std::fill(BuildVariableBytes_.begin(), BuildVariableBytes_.end(), 0);
        return res;
    }

    TVector<arrow::Datum> FlushAndApplyRenames() {
        if constexpr(LeftSemiOrOnly(Join.Kind)) {
            TVector<arrow::Datum> out;
            this->Converters_.SelectSide(Join.Preserved)->Unpack(this->Output_.SelectSide(Join.Preserved), out);
            this->Output_.SelectSide(Join.Preserved).Clear();
            TVector<arrow::Datum> renamed;
            for(auto rename: *this->Renames_){
                MKQL_ENSURE(rename.Side == Join.Preserved,
                            "renames in Semi or Only Left Join shouldn't contain columns from the non-preserved side");
                renamed.push_back(out[rename.Index]);
            }
            return renamed;
        } else {
            TSides<TVector<arrow::Datum>> sides;
            for(ESide side: EachSide) {
                this->Converters_.SelectSide(side)->Unpack(this->Output_.SelectSide(side), sides.SelectSide(side));
                this->Output_.SelectSide(side).Clear();
            }
            TVector<arrow::Datum> renamed;
            for (auto rename : *this->Renames_) {
                renamed.push_back(sides.SelectSide(rename.Side)[rename.Index]);
            }
            return renamed;
        }
    }

    static constexpr size_t MaxRefBlocks = 64;
    const TDqBlockJoinContext* Meta_;
    arrow::MemoryPool* Pool_;
    TProbeRefs* Refs_;
    TSides<const NPackedTuple::TTupleLayout*> Layouts_;
    i64 RefRowBytes_;
    ui32 BuildRowSize_;
    TVector<ui64> BuildVariableBytes_;
    TVector<bool> ProbeRuns_;
    i64 RefOverflowBytes_ = 0;
};

template <TPhysicalJoin Join> class TBlockHashJoinWrapper : public TMutableComputationNode<TBlockHashJoinWrapper<Join>> {
  private:
    using TBaseComputation = TMutableComputationNode<TBlockHashJoinWrapper>;

  public:
    TBlockHashJoinWrapper(TComputationMutables& mutables, TDqBlockJoinContext meta, TSides<IComputationNode*> streams,
                          TJoinFilters filters)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Meta_(std::make_unique<TDqBlockJoinContext>(meta))
        , Streams_(streams)
        , Filters_(std::move(filters))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        TTypeInfoHelper helper;
        TSides<std::unique_ptr<IBlockLayoutConverter>> layouts;
        const auto& userTypes = Meta_->UserTypes;
        for(ESide side: EachSide) {
            const auto& keyColumns = Meta_->KeyColumns.SelectSide(side);
            const auto roles = MakeColumnRoles(userTypes.SelectSide(side).size(), keyColumns);
            layouts.SelectSide(side) = MakeBlockLayoutConverter(helper, userTypes.SelectSide(side), roles, &ctx.ArrowMemoryPool);
            layouts.SelectSide(side)->ApplyEqualNulls(
                EqualNullsInputColumns(Meta_->Settings.EqualNullsKeys, keyColumns));
        }
        const auto& userNullTypes = userTypes.SelectSide(Join.NullSupplying());

        return ctx.HolderFactory.Create<TStreamValue>(
            ctx, Streams_, std::move(layouts), Meta_.get(), userNullTypes,
            TPackedTuplePairFilter::TryCreate(ctx, Filters_, userTypes, Meta_->KeyColumns, Meta_->ColumnPermutation));
    }

  private:
    class TStreamValue : public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;
        using JoinType = NJoinPackedTuples::THybridHashJoin<TBlockPackedTupleSource, TestStorageSettings, Join>;

      public:
        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, TSides<IComputationNode*> streams,
                     TSides<std::unique_ptr<IBlockLayoutConverter>> converters, const TDqBlockJoinContext* meta,
                     const TVector<TType*>& userBuildTypes, std::optional<TPackedTuplePairFilter> pairFilter)
            : TBase(memInfo)
            , Meta_(meta)
            , Converters_(std::move(converters))
            , Join_(TSides<TBlockPackedTupleSource>{.Build = {ctx, streams, meta, Converters_, ESide::Build},
                                                    .Probe = {ctx, streams, meta, Converters_, ESide::Probe, &Refs_}},
                    ctx, "BlockHashJoin",
                    TSides<const NPackedTuple::TTupleLayout*>{.Build = Converters_.Build->GetTupleLayout(),
                                                              .Probe = Converters_.Probe->GetTupleLayout()})
            , Ctx_(&ctx)
            , Output_(meta, {.Build = Converters_.Build.get(), .Probe = Converters_.Probe.get()}, userBuildTypes, ctx.ArrowMemoryPool, &Refs_)
            , PairFilter_(std::move(pairFilter))
            , OutputRows_(MakeJoinOutputRowsCounter(ctx))
        {}

        void WriteFlushToOutput(NUdf::TUnboxedValue* output, typename TRenamesPackedTupleOutput<Join>::TFlushResult flush) {
            const int cols = Output_.Columns();
            for (int colIndex = 0; colIndex < cols; ++colIndex) {
                output[colIndex] = Ctx_->HolderFactory.CreateArrowBlock(std::move(flush.Columns[colIndex]), Ctx_->RuntimeSettings.DatumValidation.Get());
            }
            output[cols] = Ctx_->HolderFactory.CreateArrowBlock(arrow::Datum(static_cast<uint64_t>(flush.Rows)), Ctx_->RuntimeSettings.DatumValidation.Get());
            OutputRows_.Add(flush.Rows);
        }

      private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) override {
            size_t expectedSize = Meta_->Renames.size() + 1;
            MKQL_ENSURE(width == expectedSize,
                        Sprintf("runtime(%i) vs compile-time(%i) tuple width mismatch", width, expectedSize));
            const auto flushSink = [&](auto flush) { WriteFlushToOutput(output, std::move(flush)); };
            switch (RunPackedHashJoinBatch(*Ctx_, Join_, Output_, flushSink,
                                           PairFilter_ ? &*PairFilter_ : nullptr)) {
            case EFetchResult::One:
                return NYql::NUdf::EFetchStatus::Ok;
            case EFetchResult::Yield:
                return NYql::NUdf::EFetchStatus::Yield;
            case EFetchResult::Finish:
                return NYql::NUdf::EFetchStatus::Finish;
            default:
                MKQL_ENSURE(false, "unexpected fetch result");
            }
            Y_UNREACHABLE();
        }

      private:
        const TDqBlockJoinContext* Meta_;
        TSides<std::unique_ptr<IBlockLayoutConverter>> Converters_;
        TProbeRefs Refs_;
        JoinType Join_;
        TComputationContext* Ctx_;
        TRenamesPackedTupleOutput<Join> Output_;
        std::optional<TPackedTuplePairFilter> PairFilter_;
        NYql::NUdf::TCounter OutputRows_;
    };

    void RegisterDependencies() const final {
        this->DependsOn(Streams_.Build);
        this->DependsOn(Streams_.Probe);
        Filters_.RegisterDependencies([this](IComputationNode* node) { this->DependsOn(node); },
                                      [this](IComputationExternalNode* node) { this->Own(node); });
    }

    std::unique_ptr<TDqBlockJoinContext> Meta_;
    TSides<IComputationNode*> Streams_;
    TJoinFilters Filters_;
};

} // namespace

IComputationNode* WrapDqBlockHashJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= BaseInputs, "Expected at least " << BaseInputs << " args");
    TDqBlockJoinContext meta;

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsStream(), "Expected WideStream as a resulting stream");
    const auto joinStreamType = AS_TYPE(TStreamType, joinType);
    MKQL_ENSURE(joinStreamType->GetItemType()->IsMulti(), "Expected Multi as a resulting item type");
    const auto joinComponents = GetWideComponents(joinStreamType);
    for (auto* blockType : joinComponents) {
        MKQL_ENSURE(blockType->IsBlock(), "Expected block types as wide components of result stream");
        meta.ResultItemTypes.push_back(AS_TYPE(TBlockType, blockType));
    }

    const auto leftType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(leftType->IsStream(), "Expected WideStream as a left stream");
    const auto leftStreamType = AS_TYPE(TStreamType, leftType);
    MKQL_ENSURE(leftStreamType->GetItemType()->IsMulti(), "Expected Multi as a left stream item type");
    const auto leftStreamComponents = GetWideComponents(leftStreamType);
    for (auto* blockType : leftStreamComponents) {
        MKQL_ENSURE(blockType->IsBlock(), "Expected block types as wide components of left stream");
        meta.InputTypes.Probe.push_back(AS_TYPE(TBlockType, blockType));
    }

    const auto rightType = callable.GetInput(1).GetStaticType();
    MKQL_ENSURE(rightType->IsStream(), "Expected WideStream as a right stream");
    const auto rightStreamType = AS_TYPE(TStreamType, rightType);
    MKQL_ENSURE(rightStreamType->GetItemType()->IsMulti(), "Expected Multi as a right stream item type");
    const auto rightStreamComponents = GetWideComponents(rightStreamType);
    for (auto* blockType : rightStreamComponents) {
        MKQL_ENSURE(blockType->IsBlock(), "Expected block types as wide components of right stream");
        meta.InputTypes.Build.push_back(AS_TYPE(TBlockType, blockType));
    }
    const auto parsed = ParseCommonHashJoinArgs(callable);
    const auto joinKind = parsed.Kind;
    meta.Kind = joinKind;
    meta.KeyColumns = parsed.KeyColumns;
    const bool isGrid = meta.KeyColumns.Build.empty();

    MKQL_ENSURE(!joinComponents.empty(), "Expected at least block length column");
    MKQL_ENSURE(!leftStreamComponents.empty(), "Expected at least block length column");
    MKQL_ENSURE(!rightStreamComponents.empty(), "Expected at least block length column");
    if (!isGrid) {
        MKQL_ENSURE(joinComponents.size() > 1, "Expected at least one data column");
        MKQL_ENSURE(leftStreamComponents.size() > 1, "Expected at least one data column");
        MKQL_ENSURE(rightStreamComponents.size() > 1, "Expected at least one data column");
    }

    const auto leftStream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto rightStream = LocateNode(ctx.NodeLocator, callable, 1);
    ValidateRenames(parsed.UserRenames, joinKind, std::ssize(meta.InputTypes.Probe) - 1,
                    std::ssize(meta.InputTypes.Build) - 1);
    for(ESide side: EachSide) {
        int size = std::ssize(meta.InputTypes.SelectSide(side));
        for(int index = 0; index < size; ++index) {
            TBlockType* thisType = meta.InputTypes.SelectSide(side)[index];
            if (index == size - 1) {
                MKQL_ENSURE(thisType->GetShape() == TBlockType::EShape::Scalar, Sprintf("expected last(%i) column in %s side to be scalar size",index, AsString(side)));
            } else {
                MKQL_ENSURE(thisType->GetShape() == TBlockType::EShape::Many, Sprintf("expected %i column in %s side to be block data",index, AsString(side)));
            }
        }
    }

    meta.Renames = BuildImplRenames(parsed.UserRenames);

    meta.Settings = ParseHashJoinSettingsTuple(callable.GetInput(7));
    const bool leftIsBuild = meta.Settings.LeftIsBuild();
    ApplyLeftBuildSwap(leftIsBuild, meta.InputTypes, meta.KeyColumns, meta.Renames);

    ApplyKeyColumnPermutation(meta.KeyColumns, meta.InputTypes, /* trailingColumns */ 1, meta.Renames,
                              meta.ColumnPermutation);

    for(ESide side: EachSide) {
        meta.TempStateIndes.SelectSide(side) = std::exchange(ctx.Mutables.CurValueIndex, meta.InputTypes.SelectSide(side).size() + ctx.Mutables.CurValueIndex);
    }

    TSides<TVector<TType*>> itemTypes;
    for (ESide side : EachSide) {
        for (int index = 0; index < std::ssize(meta.InputTypes.SelectSide(side)) - 1; ++index) {
            itemTypes.SelectSide(side).push_back(meta.InputTypes.SelectSide(side)[index]->GetItemType());
        }
    }
    const ESide preservedSide = PreservedSideForLeftBuild(leftIsBuild);
    meta.UserTypes = ForceOptionalOnNullableSide(itemTypes, meta.Kind, OtherSide(preservedSide), ctx.Env);

    const auto streams = SidesForLeftBuild(leftIsBuild, leftStream, rightStream);
    TJoinFilters filters = ParseJoinFiltersForBuildSide(ctx, callable, BaseInputs, leftIsBuild);

    return DispatchHashJoinByKind<TBlockHashJoinWrapper, IComputationNode>(
        joinKind, preservedSide, isGrid, "unsupported join type in block hash join", ctx.Mutables,
        std::move(meta), streams, std::move(filters));
}

} // namespace NKikimr::NMiniKQL
