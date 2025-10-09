#include "mkql_block_fuzzer.h"

#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/public/udf/arrow/block_reader.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <util/generic/singleton.h>
#include <util/random/random.h>

namespace NKikimr::NMiniKQL {

namespace {

constexpr double RemoveMaskProbability = 0.5;
constexpr int MaxOffsetShift = 64;

std::shared_ptr<arrow::ArrayData> SynchronizeArrayDataMeta(std::shared_ptr<arrow::ArrayData> result, const arrow::ArrayData& original, i64 extraShift) {
    if (!original.buffers[0]) {
        result->buffers[0] = nullptr;
    }
    result->offset += extraShift;
    result->length -= extraShift;
    result->SetNullCount(original.null_count);
    return result;
}

std::shared_ptr<arrow::Buffer> CreateShiftedBitmask(arrow::MemoryPool& pool, std::shared_ptr<arrow::Buffer> bitmap, size_t bitmapOffset, size_t length, size_t extraShift) {
    if (!bitmap) {
        return bitmap;
    }
    auto resultBitmap = MakeDenseFalseBitmap(length + extraShift, &pool);
    for (size_t i = 0; i < length; i++) {
        arrow::BitUtil::SetBitTo(resultBitmap->mutable_data(), i + extraShift, arrow::BitUtil::GetBit(bitmap->data(), bitmapOffset + i));
    }
    return resultBitmap;
}

ui64 CalculateRandomOffsetShift(IRandomProvider& randomProvider) {
    return randomProvider.Uniform(0, MaxOffsetShift + 1);
}

class IOffsetFuzzer {
public:
    virtual ~IOffsetFuzzer() = default;
    virtual std::shared_ptr<arrow::ArrayData> FuzzArray(const arrow::ArrayData& array,
                                                        arrow::MemoryPool& memoryPool,
                                                        IRandomProvider& randomProvider) const = 0;
};

class TOffsetFuzzerBase: public IOffsetFuzzer {
public:
    using TPtr = std::unique_ptr<TOffsetFuzzerBase>;
    using TBuilderAndReader = std::pair<std::unique_ptr<NYql::NUdf::IBlockReader>, std::unique_ptr<NYql::NUdf::IArrayBuilder>>;

    explicit TOffsetFuzzerBase(const NYql::NUdf::TType* type,
                               bool isTypeOptional,
                               const TTypeEnvironment& env)
        : Type_(type)
    {
        if (isTypeOptional) {
            Type_ = TOptionalType::Create(const_cast<NMiniKQL::TType*>(static_cast<const NMiniKQL::TType*>(Type_)), env);
        }
    }

protected:
    TBuilderAndReader CreateBuilderAndReader(ui64 length, arrow::MemoryPool& memoryPool) const {
        auto reader = NYql::NUdf::MakeBlockReader(NKikimr::NMiniKQL::TTypeInfoHelper(), Type());
        auto builder = NYql::NUdf::MakeArrayBuilder(NKikimr::NMiniKQL::TTypeInfoHelper(), Type(), memoryPool, length, /*pgBuilder=*/nullptr);
        return {std::move(reader), std::move(builder)};
    }

    const NYql::NUdf::TType* Type() const {
        return Type_;
    }

private:
    const NYql::NUdf::TType* Type_;
};

template <bool IsOptional>
class TLeafOffsetFuzzer: public TOffsetFuzzerBase {
public:
    TLeafOffsetFuzzer(const NYql::NUdf::TType* type, const TTypeEnvironment& env)
        : TOffsetFuzzerBase(type, IsOptional, env)
    {
    }

    std::shared_ptr<arrow::ArrayData> FuzzArray(const arrow::ArrayData& array,
                                                arrow::MemoryPool& memoryPool,
                                                IRandomProvider& randomProvider) const override {
        if (array.length == 0) {
            return array.Copy();
        }

        auto extraShift = CalculateRandomOffsetShift(randomProvider);
        auto [reader, builder] = CreateBuilderAndReader(array.length + extraShift, memoryPool);
        for (size_t i = 0; i < extraShift; i++) {
            builder->Add(reader->GetItem(array, 0));
        }
        for (i64 i = 0; i < array.length; i++) {
            builder->Add(reader->GetItem(array, i));
        }
        auto result = builder->Build(/*finish=*/true);
        MKQL_ENSURE(result.is_array(), "An array is expected as the result from the builder. "
                                       "If you see a chunked array, it means that the test data is too large for this fuzzer, "
                                       "and types with variable-size elements (such as strings) are being used."
                                       "Please use smaller data sizes or do not use this fuzzer.");
        return SynchronizeArrayDataMeta(result.array(), array, extraShift);
    }
};

template <bool IsOptional>
class TTupleOffsetFuzzer: public TOffsetFuzzerBase {
public:
    TTupleOffsetFuzzer(TVector<TOffsetFuzzerBase::TPtr>&& children,
                       const NYql::NUdf::TType* type,
                       const TTypeEnvironment& env)
        : TOffsetFuzzerBase(type, IsOptional, env)
        , Children_(std::move(children))
    {
    }

    std::shared_ptr<arrow::ArrayData> FuzzArray(const arrow::ArrayData& array,
                                                arrow::MemoryPool& memoryPool,
                                                IRandomProvider& randomProvider) const override {
        auto result = array.Copy();
        for (size_t i = 0; i < Children_.size(); ++i) {
            result->child_data[i] = Children_[i]->FuzzArray(*array.child_data[i], memoryPool, randomProvider);
        }
        auto extraShift = CalculateRandomOffsetShift(randomProvider);
        result->buffers[0] = CreateShiftedBitmask(memoryPool, result->buffers[0], result->offset, result->length, extraShift);
        result->length += extraShift;
        return SynchronizeArrayDataMeta(result, array, extraShift);
    }

protected:
    TVector<TOffsetFuzzerBase::TPtr> Children_;
};

class TExternalOptionalOffsetFuzzer: public TOffsetFuzzerBase {
public:
    TExternalOptionalOffsetFuzzer(TOffsetFuzzerBase::TPtr base, const NYql::NUdf::TType* type, const TTypeEnvironment& env)
        : TOffsetFuzzerBase(type, /*isOptional=*/true, env)
        , Base_(std::move(base))
    {
    }

    std::shared_ptr<arrow::ArrayData> FuzzArray(const arrow::ArrayData& array,
                                                arrow::MemoryPool& memoryPool,
                                                IRandomProvider& randomProvider) const override {
        auto result = array.Copy();
        result->child_data[0] = Base_->FuzzArray(*array.child_data[0], memoryPool, randomProvider);
        auto extraShift = CalculateRandomOffsetShift(randomProvider);
        result->buffers[0] = CreateShiftedBitmask(memoryPool, result->buffers[0], result->offset, result->length, extraShift);
        result->length += extraShift;
        return SynchronizeArrayDataMeta(result, array, extraShift);
    }

protected:
    TOffsetFuzzerBase::TPtr Base_;
};

struct TFuzzerTraits {
    using TResult = TOffsetFuzzerBase;

    template <bool Nullable>
    using TTuple = TTupleOffsetFuzzer<Nullable>;

    template <typename T, bool Nullable>
    using TFixedSize = TLeafOffsetFuzzer<Nullable>;

    template <typename TStringType, bool Nullable, NKikimr::NUdf::EDataSlot TOriginal>
    using TStrings = TLeafOffsetFuzzer<Nullable>;
    using TExtOptional = TExternalOptionalOffsetFuzzer;
    template <bool Nullable>
    using TResource = TLeafOffsetFuzzer<Nullable>;

    template <typename TTzDate, bool Nullable>
    using TTzDateFuzzer = TLeafOffsetFuzzer<Nullable>;
    using TSingular = TLeafOffsetFuzzer</*IsOptional=*/false>;

    constexpr static bool PassType = true;

    static std::unique_ptr<TResult> MakePg(const NYql::NUdf::TPgTypeDescription& desc,
                                           const NYql::NUdf::IPgBuilder* pgBuilder,
                                           const NYql::NUdf::TType* type,
                                           const TTypeEnvironment& env) {
        Y_UNUSED(desc, pgBuilder);
        return std::make_unique<TLeafOffsetFuzzer</*IsOptional=*/false>>(type, env);
    }

    static std::unique_ptr<TResult> MakeResource(bool isOptional,
                                                 const NYql::NUdf::TType* type,
                                                 const TTypeEnvironment& env) {
        if (isOptional) {
            return std::make_unique<TLeafOffsetFuzzer</*IsOptional=*/true>>(type, env);
        } else {
            return std::make_unique<TLeafOffsetFuzzer</*IsOptional=*/false>>(type, env);
        }
    }

    template <typename TTzDate>
    static std::unique_ptr<TResult> MakeTzDate(bool isOptional,
                                               const NYql::NUdf::TType* type,
                                               const TTypeEnvironment& env) {
        if (isOptional) {
            return std::make_unique<TLeafOffsetFuzzer</*IsOptional=*/true>>(type, env);
        } else {
            return std::make_unique<TLeafOffsetFuzzer</*IsOptional=*/false>>(type, env);
        }
    }

    template <bool IsNull>
    static std::unique_ptr<TResult> MakeSingular(const NYql::NUdf::TType* type,
                                                 const TTypeEnvironment& env) {
        Y_UNUSED(IsNull);
        return std::make_unique<TLeafOffsetFuzzer</*IsOptional=*/false>>(type, env);
    }
};

std::unique_ptr<TFuzzerTraits::TResult> MakeBlockFuzzer(const TTypeInfoHelper& typeInfoHelper,
                                                        const NYql::NUdf::TType* type,
                                                        const TTypeEnvironment& env) {
    return DispatchByArrowTraits<TFuzzerTraits>(typeInfoHelper, type, /*pgBuilder=*/nullptr, env);
}

// Implementation that removes masks when all elements are ones
class TAllOnesRemoveMaskFuzzer: public IFuzzer {
public:
    explicit TAllOnesRemoveMaskFuzzer() = default;

    NYql::NUdf::TUnboxedValue Fuzz(NYql::NUdf::TUnboxedValue input,
                                   const THolderFactory& holderFactory,
                                   arrow::MemoryPool& memoryPool,
                                   IRandomProvider& randomProvider) const override {
        Y_UNUSED(memoryPool);
        const auto& block = TArrowBlock::From(input);
        const auto& datum = block.GetDatum();

        if (!datum.is_array()) {
            return input;
        }

        auto fuzzedArray = FuzzArrayData(*datum.array(), randomProvider);
        auto fuzzedDatum = arrow::Datum(fuzzedArray);

        // Create a new TArrowBlock with the fuzzed data
        return holderFactory.CreateArrowBlock(std::move(fuzzedDatum));
    }

private:
    std::shared_ptr<arrow::ArrayData> FuzzArrayData(const arrow::ArrayData& arrayData, IRandomProvider& randomProvider) const {
        auto result = arrayData.Copy();

        if (result->buffers[0]) {
            int64_t nullCount = result->GetNullCount();
            if (nullCount == 0 && randomProvider.GenRandReal2() < RemoveMaskProbability) {
                result->buffers[0] = nullptr;
            }
        }

        std::vector<std::shared_ptr<arrow::ArrayData>> children;
        for (const auto& child : result->child_data) {
            children.push_back(FuzzArrayData(*child, randomProvider));
        }
        result->child_data = children;

        return result;
    }
};

class TOffsetShiftFuzzer: public IFuzzer {
public:
    explicit TOffsetShiftFuzzer(const TType* type, const TTypeEnvironment& env)
        : OffsetFuzzer_(MakeBlockFuzzer(TTypeInfoHelper(), type, env))
    {
    }

    NYql::NUdf::TUnboxedValue Fuzz(NYql::NUdf::TUnboxedValue input,
                                   const THolderFactory& holderFactory,
                                   arrow::MemoryPool& memoryPool,
                                   IRandomProvider& randomProvider) const override {
        if (!input.HasValue()) {
            return input;
        }

        const auto& block = TArrowBlock::From(input);
        const auto& datum = block.GetDatum();

        if (!datum.is_array()) {
            MKQL_ENSURE(!datum.is_arraylike(), "Chunked arrays are not implemented yet.");
            return input;
        }

        auto fuzzedArray = OffsetFuzzer_->FuzzArray(*datum.array(), memoryPool, randomProvider);
        auto fuzzedDatum = arrow::Datum(fuzzedArray);

        // Create a new TArrowBlock with the fuzzed data
        return holderFactory.CreateArrowBlock(std::move(fuzzedDatum));
    }

private:
    const std::unique_ptr<IOffsetFuzzer> OffsetFuzzer_;
};

} // namespace

TFuzzerHolder::TFuzzerHolder() = default;

TFuzzerHolder::~TFuzzerHolder() = default;

ui64 TFuzzerHolder::ReserveFuzzer() {
    return FuzzerIdx_++;
}

void TFuzzerHolder::CreateFuzzers(TFuzzOptions options, ui64 fuzzerIndex, const TType* type, const TTypeEnvironment& env) {
    TFuzzerList result;
    MKQL_ENSURE(type->IsBlock(), "Expected block type for fuzzer.");
    type = AS_TYPE(TBlockType, type)->GetItemType();
    if (options.FuzzOffsetShift) {
        result.push_back(MakeHolder<TOffsetShiftFuzzer>(type, env));
    }
    if (options.FuzzZeroOptionalBitmaskRemove) {
        result.push_back(MakeHolder<TAllOnesRemoveMaskFuzzer>());
    }
    MKQL_ENSURE(!NodeToFuzzOptions_.contains(fuzzerIndex), "Fuzzer already created.");
    NodeToFuzzOptions_[fuzzerIndex] = std::move(result);
    return;
}

void TFuzzerHolder::ClearFuzzers() {
    NodeToFuzzOptions_.clear();
}

NYql::NUdf::TUnboxedValue TFuzzerHolder::ApplyFuzzers(NYql::NUdf::TUnboxedValue input,
                                                      ui64 fuzzIdx,
                                                      const THolderFactory& holderFactory,
                                                      arrow::MemoryPool& memoryPool,
                                                      IRandomProvider& randomProvider) const {
    auto it = NodeToFuzzOptions_.find(fuzzIdx);
    if (it == NodeToFuzzOptions_.end()) {
        MKQL_ENSURE(fuzzIdx == EmptyFuzzerId, "Fuzzer expected.");
        return input;
    }

    for (const auto& fuzzer : it->second) {
        input = fuzzer->Fuzz(input, holderFactory, memoryPool, randomProvider);
    }

    return input;
}

} // namespace NKikimr::NMiniKQL
