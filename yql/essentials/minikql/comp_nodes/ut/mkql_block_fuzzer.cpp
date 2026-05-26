#include "mkql_block_fuzzer.h"

#include "mkql_block_test_helper.h"

#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/public/udf/arrow/block_reader.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/public/udf/arrow/util.h>

#include <util/generic/singleton.h>
#include <util/random/random.h>

#include <arrow/chunked_array.h>

namespace NKikimr::NMiniKQL {

namespace {

constexpr double RemoveMaskProbability = 0.5;
constexpr double MakeImmutableProbability = 0.5;
constexpr int MaxOffsetShift = 64;

std::shared_ptr<arrow::ArrayData> ValidateDatumAfterFuzzing(std::shared_ptr<arrow::ArrayData> input) {
    ValidateDatum(arrow::Datum(input), Nothing(), nullptr, NYql::NUdf::EValidateDatumMode::Cheap);
    return input;
}

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
                                                        IRandomProvider& randomProvider) const {
        ValidateDatum(arrow::Datum(array.Copy()), Nothing(), nullptr, NYql::NUdf::EValidateDatumMode::Cheap);
        return ValidateDatumAfterFuzzing(DoFuzzArray(array, memoryPool, randomProvider));
    };

private:
    virtual std::shared_ptr<arrow::ArrayData> DoFuzzArray(const arrow::ArrayData& array,
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

    std::shared_ptr<arrow::ArrayData> DoFuzzArray(const arrow::ArrayData& array,
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
        if (!result.is_array()) {
            return array.Copy();
        }
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

    std::shared_ptr<arrow::ArrayData> DoFuzzArray(const arrow::ArrayData& array,
                                                  arrow::MemoryPool& memoryPool,
                                                  IRandomProvider& randomProvider) const override {
        auto result = array.Copy();
        for (size_t i = 0; i < Children_.size(); ++i) {
            result->child_data[i] = Children_[i]->FuzzArray(*array.child_data[i], memoryPool, randomProvider);
        }
        auto extraShift = CalculateRandomOffsetShift(randomProvider);
        result->buffers[0] = CreateShiftedBitmask(memoryPool, result->buffers[0], result->offset, result->length, extraShift);
        result->offset = 0;
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

    std::shared_ptr<arrow::ArrayData> DoFuzzArray(const arrow::ArrayData& array,
                                                  arrow::MemoryPool& memoryPool,
                                                  IRandomProvider& randomProvider) const override {
        auto result = array.Copy();
        result->child_data[0] = Base_->FuzzArray(*array.child_data[0], memoryPool, randomProvider);
        auto extraShift = CalculateRandomOffsetShift(randomProvider);
        result->buffers[0] = CreateShiftedBitmask(memoryPool, result->buffers[0], result->offset, result->length, extraShift);
        result->offset = 0;
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

class TFuzzerBase: public IFuzzer {
public:
    arrow::Datum Fuzz(const arrow::ArrayData& input,
                      arrow::MemoryPool& memoryPool,
                      IRandomProvider& randomProvider) const final {
        ValidateDatum(input, Nothing(), nullptr, NYql::NUdf::EValidateDatumMode::Cheap);
        return DoFuzz(input, memoryPool, randomProvider);
    };

private:
    virtual arrow::Datum DoFuzz(const arrow::ArrayData& input,
                                arrow::MemoryPool& memoryPool,
                                IRandomProvider& randomProvider) const = 0;
};

// Implementation that removes masks when all elements are ones
class TAllOnesRemoveMaskFuzzer: public TFuzzerBase {
public:
    explicit TAllOnesRemoveMaskFuzzer() = default;

    arrow::Datum DoFuzz(const arrow::ArrayData& input,
                        arrow::MemoryPool& memoryPool,
                        IRandomProvider& randomProvider) const override {
        Y_UNUSED(memoryPool);
        return arrow::Datum(FuzzArrayData(input, randomProvider));
    }

private:
    std::shared_ptr<arrow::ArrayData> FuzzArrayData(const arrow::ArrayData& arrayData, IRandomProvider& randomProvider) const {
        auto result = arrayData.Copy();
        MKQL_ENSURE(!result->buffers.empty(), "Expected at least 1 buffer for type: " << arrayData.type->ToString() << "Array length: " << arrayData.length);
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

class TOffsetShiftFuzzer: public TFuzzerBase {
public:
    explicit TOffsetShiftFuzzer(const TType* type, const TTypeEnvironment& env)
        : OffsetFuzzer_(MakeBlockFuzzer(TTypeInfoHelper(), type, env))
    {
    }

    arrow::Datum DoFuzz(const arrow::ArrayData& input,
                        arrow::MemoryPool& memoryPool,
                        IRandomProvider& randomProvider) const override {
        return arrow::Datum(OffsetFuzzer_->FuzzArray(input, memoryPool, randomProvider));
    }

private:
    const std::unique_ptr<IOffsetFuzzer> OffsetFuzzer_;
};

class TImmutableFuzzer: public TFuzzerBase {
public:
    explicit TImmutableFuzzer() = default;

    arrow::Datum DoFuzz(const arrow::ArrayData& input,
                        arrow::MemoryPool& memoryPool,
                        IRandomProvider& randomProvider) const override {
        Y_UNUSED(memoryPool);
        return arrow::Datum(FuzzArrayData(input, randomProvider));
    }

private:
    std::shared_ptr<arrow::ArrayData> FuzzArrayData(const arrow::ArrayData& arrayData, IRandomProvider& randomProvider) const {
        auto result = arrayData.Copy();
        for (auto& buffer : result->buffers) {
            if (!buffer) {
                continue;
            }
            if (randomProvider.GenRandReal2() < MakeImmutableProbability) {
                buffer = std::make_shared<arrow::Buffer>(buffer, 0, buffer->size());
            }
        }
        for (auto& child : result->child_data) {
            child = FuzzArrayData(*child, randomProvider);
        }
        return result;
    }
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
    // NOTE: Order is important here, because some fuzzers can break changes made by other fuzzers.
    if (options.FuzzOffsetShift) {
        result.push_back(MakeHolder<TOffsetShiftFuzzer>(type, env));
    }
    if (options.FuzzZeroOptionalBitmaskRemove) {
        result.push_back(MakeHolder<TAllOnesRemoveMaskFuzzer>());
    }
    if (options.FuzzImmutable) {
        result.push_back(MakeHolder<TImmutableFuzzer>());
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

    const auto& datum = TArrowBlock::From(input).GetDatum();

    if (datum.is_array()) {
        arrow::Datum fuzzedDatum = datum.array()->Copy();
        for (const auto& fuzzer : it->second) {
            fuzzedDatum = fuzzer->Fuzz(*fuzzedDatum.array(), memoryPool, randomProvider);
        }
        return holderFactory.CreateArrowBlock(arrow::Datum(fuzzedDatum));
    } else if (datum.is_arraylike()) {
        TVector<std::shared_ptr<arrow::ArrayData>> fuzzedChunks;
        for (const auto& chunk : datum.chunked_array()->chunks()) {
            auto chunkFuzzed = chunk->data()->Copy();
            for (const auto& fuzzer : it->second) {
                auto fuzzedDatum = fuzzer->Fuzz(*chunkFuzzed, memoryPool, randomProvider);
                MKQL_ENSURE(fuzzedDatum.is_array(), "Expected array from fuzzer for chunk");
                chunkFuzzed = fuzzedDatum.array();
            }
            fuzzedChunks.push_back(chunkFuzzed);
        }
        return holderFactory.CreateArrowBlock(NYql::NUdf::MakeArray(fuzzedChunks));
    } else {
        MKQL_ENSURE(datum.is_scalar(), "Expected scalar");
    }

    return input;
}

} // namespace NKikimr::NMiniKQL
