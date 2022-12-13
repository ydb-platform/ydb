#include "mkql_blocks.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/util/bitmap.h>
#include <arrow/util/bit_util.h>

#include <util/generic/size_literals.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

constexpr size_t MaxBlockSizeInBytes = 1_MB;

class TBlockBuilderBase {
public:
    TBlockBuilderBase(TComputationContext& ctx, const std::shared_ptr<arrow::DataType>& itemType, size_t maxLength)
        : Ctx_(ctx)
        , ItemType_(itemType)
        , MaxLength_(maxLength)
    {}

    virtual ~TBlockBuilderBase() = default;

    inline size_t MaxLength() const noexcept {
        return MaxLength_;
    }

    virtual void Add(const NUdf::TUnboxedValue& value) = 0;
    virtual NUdf::TUnboxedValuePod Build(bool finish) = 0;

protected:
    TComputationContext& Ctx_;
    const std::shared_ptr<arrow::DataType> ItemType_;
    const size_t MaxLength_;
};

template <typename T, typename TBuilder>
class TFixedSizeBlockBuilder : public TBlockBuilderBase {
public:
    TFixedSizeBlockBuilder(TComputationContext& ctx, const std::shared_ptr<arrow::DataType>& itemType)
        : TBlockBuilderBase(ctx, itemType, MaxBlockSizeInBytes / TypeSize(*itemType))
        , Builder_(std::make_unique<TBuilder>(&Ctx_.ArrowMemoryPool))
    {
        this->Reserve();
    }

    void Add(const NUdf::TUnboxedValue& value) override {
        Y_VERIFY_DEBUG(Builder_->length() < MaxLength_);
        if (value) {
            this->Builder_->UnsafeAppend(value.Get<T>());
        } else {
            this->Builder_->UnsafeAppendNull();
        }
    }

    NUdf::TUnboxedValuePod Build(bool finish) override {
        std::shared_ptr<arrow::ArrayData> result;
        ARROW_OK(this->Builder_->FinishInternal(&result));
        Builder_.reset();
        if (!finish) {
            Builder_ = std::make_unique<TBuilder>(&Ctx_.ArrowMemoryPool);
            Reserve();
        }

        return this->Ctx_.HolderFactory.CreateArrowBlock(std::move(result));
    }

private:
    void Reserve() {
        ARROW_OK(this->Builder_->Reserve(MaxLength_));
    }

    static int64_t TypeSize(arrow::DataType& itemType) {
        const auto bits = static_cast<const arrow::FixedWidthType&>(itemType).bit_width();
        return arrow::BitUtil::BytesForBits(bits);
    }

private:
    std::unique_ptr<TBuilder> Builder_;
};

class TTupleBlockBuilder : public TBlockBuilderBase {
public:
    TTupleBlockBuilder(TComputationContext & ctx, const std::shared_ptr<arrow::DataType>& itemType, size_t maxLength,
        TTupleType* tupleType, TVector<std::unique_ptr<TBlockBuilderBase>>&& children)
        : TBlockBuilderBase(ctx, itemType, maxLength)
        , TupleType_(tupleType)
        , Children_(std::move(children))
    {
        bool isOptional;
        MKQL_ENSURE(ConvertArrowType(TupleType_, isOptional, ArrowType_), "Unsupported type");
        Reserve();
    }

    void Add(const NUdf::TUnboxedValue& value) final {
        if (!value) {
            NullBitmapBuilder_->UnsafeAppend(false);
            for (ui32 i = 0; i < TupleType_->GetElementsCount(); ++i) {
                Children_[i]->Add({});
            }

            return;
        }

        NullBitmapBuilder_->UnsafeAppend(true);
        auto elements = value.GetElements();
        if (elements) {
            for (ui32 i = 0; i < TupleType_->GetElementsCount(); ++i) {
                Children_[i]->Add(elements[i]);
            }
        } else {
            for (ui32 i = 0; i < TupleType_->GetElementsCount(); ++i) {
                Children_[i]->Add(value.GetElement(i));
            }
        }
    }

    NUdf::TUnboxedValuePod Build(bool finish) final {
        std::vector<arrow::Datum> childrenValues;
        childrenValues.reserve(TupleType_->GetElementsCount());
        for (ui32 i = 0; i < TupleType_->GetElementsCount(); ++i) {
            childrenValues.emplace_back(TArrowBlock::From(Children_[i]->Build(finish)).GetDatum());
        }

        std::shared_ptr<arrow::Buffer> nullBitmap;
        auto length = NullBitmapBuilder_->length();
        auto nullCount = NullBitmapBuilder_->false_count();
        ARROW_OK(NullBitmapBuilder_->Finish(&nullBitmap));
        auto arrayData = arrow::ArrayData::Make(ArrowType_, length, { nullBitmap }, nullCount, 0);
        for (ui32 i = 0; i < TupleType_->GetElementsCount(); ++i) {
            arrayData->child_data.push_back(childrenValues[i].array());
        }

        arrow::Datum result(arrayData);

        NullBitmapBuilder_ = nullptr;
        if (!finish) {
            Reserve();
        }

        return this->Ctx_.HolderFactory.CreateArrowBlock(std::move(result));
    }

private:
    void Reserve() {
        NullBitmapBuilder_ = std::make_unique<arrow::TypedBufferBuilder<bool>>(&Ctx_.ArrowMemoryPool);
        ARROW_OK(NullBitmapBuilder_->Reserve(MaxLength_));
    }

private:
    TTupleType* TupleType_;
    std::shared_ptr<arrow::DataType> ArrowType_;
    TVector<std::unique_ptr<TBlockBuilderBase>> Children_;
    std::unique_ptr<arrow::TypedBufferBuilder<bool>> NullBitmapBuilder_;
};

std::unique_ptr<TBlockBuilderBase> MakeBlockBuilder(TComputationContext& ctx, TType* type) {
    if (type->IsOptional()) {
        type = AS_TYPE(TOptionalType, type)->GetItemType();
    }

    if (type->IsTuple()) {
        bool isOptional;
        std::shared_ptr<arrow::DataType> arrowType;
        MKQL_ENSURE(ConvertArrowType(type, isOptional, arrowType), "Unsupported type");

        auto tupleType = AS_TYPE(TTupleType, type);
        TVector<std::unique_ptr<TBlockBuilderBase>> children;
        size_t maxLength = MaxBlockSizeInBytes;
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            children.emplace_back(MakeBlockBuilder(ctx, tupleType->GetElementType(i)));
            maxLength = Min(maxLength, children.back()->MaxLength());
        }

        return std::make_unique<TTupleBlockBuilder>(ctx, arrowType, maxLength, tupleType, std::move(children));
    }

    if (type->IsData()) {
        auto slot = *AS_TYPE(TDataType, type)->GetDataSlot();
        switch (slot) {
        case NUdf::EDataSlot::Int8:
            return std::make_unique<TFixedSizeBlockBuilder<i8, arrow::Int8Builder>>(ctx, arrow::int8());
        case NUdf::EDataSlot::Uint8:
        case NUdf::EDataSlot::Bool:
            return std::make_unique<TFixedSizeBlockBuilder<ui8, arrow::UInt8Builder>>(ctx, arrow::uint8());
        case NUdf::EDataSlot::Int16:
            return std::make_unique<TFixedSizeBlockBuilder<i16, arrow::Int16Builder>>(ctx, arrow::int16());
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            return std::make_unique<TFixedSizeBlockBuilder<ui16, arrow::UInt16Builder>>(ctx, arrow::uint16());
        case NUdf::EDataSlot::Int32:
            return std::make_unique<TFixedSizeBlockBuilder<i32, arrow::Int32Builder>>(ctx, arrow::int32());
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return std::make_unique<TFixedSizeBlockBuilder<ui32, arrow::UInt32Builder>>(ctx, arrow::uint32());
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
            return std::make_unique<TFixedSizeBlockBuilder<i64, arrow::Int64Builder>>(ctx, arrow::int64());
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return std::make_unique<TFixedSizeBlockBuilder<ui64, arrow::UInt64Builder>>(ctx, arrow::uint64());
        case NUdf::EDataSlot::Float:
            return std::make_unique<TFixedSizeBlockBuilder<float, arrow::FloatBuilder>>(ctx, arrow::float32());
        case NUdf::EDataSlot::Double:
            return std::make_unique<TFixedSizeBlockBuilder<double, arrow::DoubleBuilder>>(ctx, arrow::float64());
        default:
            MKQL_ENSURE(false, "Unsupported data slot");
        }
    }

    MKQL_ENSURE(false, "Unsupported type");
}

class TToBlocksWrapper : public TStatelessFlowComputationNode<TToBlocksWrapper> {
public:
    explicit TToBlocksWrapper(IComputationNode* flow, TType* itemType)
        : TStatelessFlowComputationNode(flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , ItemType_(itemType)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto builder = MakeBlockBuilder(ctx, ItemType_);

        for (size_t i = 0; i < builder->MaxLength(); ++i) {
            auto result = Flow_->GetValue(ctx);
            if (result.IsFinish() || result.IsYield()) {
                if (i == 0) {
                    return result.Release();
                }
                break;
            }
            builder->Add(result);
        }

        return builder->Build(true);
    }

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

private:
    IComputationNode* const Flow_;
    TType* ItemType_;
};

class TWideToBlocksWrapper : public TStatefulWideFlowComputationNode<TWideToBlocksWrapper> {
public:
    TWideToBlocksWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        TVector<TType*>&& types)
        : TStatefulWideFlowComputationNode(mutables, flow, EValueRepresentation::Any)
        , Flow_(flow)
        , Types_(std::move(types))
        , Width_(Types_.size())
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
        TComputationContext& ctx,
        NUdf::TUnboxedValue*const* output) const
    {
        auto& s = GetState(state, ctx);
        if (s.IsFinished_) {
            return EFetchResult::Finish;
        }

        for (; s.Rows_ < s.MaxLength_; ++s.Rows_) {
            if (const auto result = Flow_->FetchValues(ctx, s.ValuePointers_.data()); EFetchResult::One != result) {
                if (EFetchResult::Finish == result) {
                    s.IsFinished_ = true;
                }

                if (EFetchResult::Yield == result || s.Rows_ == 0) {
                    return result;
                }

                break;
            }
            for (size_t j = 0; j < Width_; ++j) {
                if (output[j] != nullptr) {
                    s.Builders_[j]->Add(s.Values_[j]);
                }
            }
        }

        for (size_t i = 0; i < Width_; ++i) {
            if (auto* out = output[i]; out != nullptr) {
                *out = s.Builders_[i]->Build(s.IsFinished_);
            }
        }

        if (auto* out = output[Width_]; out != nullptr) {
            *out = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(s.Rows_)));
        }

        s.Rows_ = 0;
        return EFetchResult::One;
    }

private:
    struct TState : public TComputationValue<TState> {
        std::vector<NUdf::TUnboxedValue> Values_;
        std::vector<NUdf::TUnboxedValue*> ValuePointers_;
        std::vector<std::unique_ptr<TBlockBuilderBase>> Builders_;
        size_t MaxLength_;
        size_t Rows_ = 0;
        bool IsFinished_ = false;

        TState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TVector<TType*>& types)
            : TComputationValue(memInfo)
            , Values_(types.size())
            , ValuePointers_(types.size())
        {
            for (size_t i = 0; i < types.size(); ++i) {
                ValuePointers_[i] = &Values_[i];
                Builders_.push_back(MakeBlockBuilder(ctx, types[i]));
            }

            MaxLength_ = MaxBlockSizeInBytes;
            for (size_t i = 0; i < types.size(); ++i) {
                MaxLength_ = Min(MaxLength_, Builders_[i]->MaxLength());
            }
        }
    };

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(ctx, Types_);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    IComputationWideFlowNode* Flow_;
    const TVector<TType*> Types_;
    const size_t Width_;
};

class TBlockReaderBase {
public:
    virtual ~TBlockReaderBase() = default;

    virtual NUdf::TUnboxedValuePod Get(const arrow::ArrayData& data, size_t index) = 0;

    virtual NUdf::TUnboxedValuePod GetScalar(const arrow::Scalar& scalar) = 0;
};

template <typename T>
class TFixedSizeBlockReader : public TBlockReaderBase {
public:
    NUdf::TUnboxedValuePod Get(const arrow::ArrayData& data, size_t index) final {
        if (data.GetNullCount() > 0 && !arrow::BitUtil::GetBit(data.GetValues<uint8_t>(0, 0), index + data.offset)) {
            return {};
        }
    
        return NUdf::TUnboxedValuePod(data.GetValues<T>(1)[index]);
    }

    NUdf::TUnboxedValuePod GetScalar(const arrow::Scalar& scalar) final {
        if (!scalar.is_valid) {
            return {};
        }

        return NUdf::TUnboxedValuePod(*static_cast<const T*>(arrow::internal::checked_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).data()));
    }
};

class TTupleBlockReader : public TBlockReaderBase {
public:
    TTupleBlockReader(TVector<std::unique_ptr<TBlockReaderBase>>&& children, const THolderFactory& holderFactory)
        : Children_(std::move(children))
        , HolderFactory_(holderFactory)
    {}

    NUdf::TUnboxedValuePod Get(const arrow::ArrayData& data, size_t index) final {
        if (data.GetNullCount() > 0 && !arrow::BitUtil::GetBit(data.GetValues<uint8_t>(0, 0), index + data.offset)) {
            return {};
        }

        NUdf::TUnboxedValue* items;
        auto result = Cache_.NewArray(HolderFactory_, Children_.size(), items);
        for (ui32 i = 0; i < Children_.size(); ++i) {
            items[i] = Children_[i]->Get(*data.child_data[i], index);
        }

        return result;
    }

    NUdf::TUnboxedValuePod GetScalar(const arrow::Scalar& scalar) final {
        if (!scalar.is_valid) {
            return {};
        }

        const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(scalar);

        NUdf::TUnboxedValue* items;
        auto result = Cache_.NewArray(HolderFactory_, Children_.size(), items);
        for (ui32 i = 0; i < Children_.size(); ++i) {
            items[i] = Children_[i]->GetScalar(*structScalar.value[i]);
        }

        return result;
    }

private:
    TVector<std::unique_ptr<TBlockReaderBase>> Children_;
    const THolderFactory& HolderFactory_;
    TPlainContainerCache Cache_;
};

std::unique_ptr<TBlockReaderBase> MakeBlockReader(TType* type, const THolderFactory& holderFactory) {
    if (type->IsOptional()) {
        type = AS_TYPE(TOptionalType, type)->GetItemType();
    }

    if (type->IsTuple()) {
        auto tupleType = AS_TYPE(TTupleType, type);
        TVector<std::unique_ptr<TBlockReaderBase>> children;
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            children.emplace_back(MakeBlockReader(tupleType->GetElementType(i), holderFactory));
        }

        return std::make_unique<TTupleBlockReader>(std::move(children), holderFactory);
    }

    if (type->IsData()) {
        auto slot = *AS_TYPE(TDataType, type)->GetDataSlot();
        switch (slot) {
        case NUdf::EDataSlot::Int8:
            return std::make_unique<TFixedSizeBlockReader<i8>>();
        case NUdf::EDataSlot::Bool:
        case NUdf::EDataSlot::Uint8:
            return std::make_unique<TFixedSizeBlockReader<ui8>>();
        case NUdf::EDataSlot::Int16:
            return std::make_unique<TFixedSizeBlockReader<i16>>();
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            return std::make_unique<TFixedSizeBlockReader<ui16>>();
        case NUdf::EDataSlot::Int32:
            return std::make_unique<TFixedSizeBlockReader<i32>>();
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return std::make_unique<TFixedSizeBlockReader<ui32>>();
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
            return std::make_unique<TFixedSizeBlockReader<i64>>();
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return std::make_unique<TFixedSizeBlockReader<ui64>>();
        case NUdf::EDataSlot::Float:
            return std::make_unique<TFixedSizeBlockReader<float>>();
        case NUdf::EDataSlot::Double:
            return std::make_unique<TFixedSizeBlockReader<double>>();
        default:
            MKQL_ENSURE(false, "Unsupported data slot");
        }
    }

    MKQL_ENSURE(false, "Unsupported type");
}

class TFromBlocksWrapper : public TMutableComputationNode<TFromBlocksWrapper> {
public:
    TFromBlocksWrapper(TComputationMutables& mutables, IComputationNode* flow, TType* itemType)
        : TMutableComputationNode(mutables)
        , Flow_(flow)
        , ItemType_(itemType)
        , StateIndex_(mutables.CurValueIndex++)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto& state = GetState(ctx);

        if (state.Array_ == nullptr || state.Index_ == state.Array_->length) {
            auto result = Flow_->GetValue(ctx);
            if (result.IsFinish()) {
                return NUdf::TUnboxedValue::MakeFinish();
            }
            if (result.IsYield()) {
                return NUdf::TUnboxedValue::MakeYield();
            }
            state.Array_ = TArrowBlock::From(result).GetDatum().array();
            state.Index_ = 0;
        }

        const auto result = state.GetValue();
        ++state.Index_;
        return result;
    }

private:
    struct TState : public TComputationValue<TState> {
        using TComputationValue::TComputationValue;

        TState(TMemoryUsageInfo* memInfo, TType* itemType, TComputationContext& ctx)
            : TComputationValue(memInfo)
        {
            Reader_ = MakeBlockReader(itemType, ctx.HolderFactory);
        }

        NUdf::TUnboxedValuePod GetValue() const {
            const auto nullCount = Array_->GetNullCount();

            return nullCount == Array_->length || (nullCount > 0 && !HasValue())
                ? NUdf::TUnboxedValuePod()
                : DoGetValue();
        }

    private:
        NUdf::TUnboxedValuePod DoGetValue() const {
            return Reader_->Get(*Array_, Index_);
        }

        bool HasValue() const {
            return arrow::BitUtil::GetBit(Array_->GetValues<uint8_t>(0, 0), Index_ + Array_->offset);
        }

        std::unique_ptr<TBlockReaderBase> Reader_;
    public:
        std::shared_ptr<arrow::ArrayData> Array_{ nullptr };
        size_t Index_{ 0 };
    };

private:
    void RegisterDependencies() const final {
        this->DependsOn(Flow_);
    }

    TState& GetState(TComputationContext& ctx) const {
        auto& result = ctx.MutableValues[StateIndex_];
        if (!result.HasValue()) {
            result = ctx.HolderFactory.Create<TState>(ItemType_, ctx);
        }
        return *static_cast<TState*>(result.AsBoxed().Get());
    }

private:
    IComputationNode* const Flow_;
    TType* ItemType_;
    const ui32 StateIndex_;
};

class TWideFromBlocksWrapper : public TStatefulWideFlowComputationNode<TWideFromBlocksWrapper> {
public:
    TWideFromBlocksWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        TVector<TType*>&& types)
        : TStatefulWideFlowComputationNode(mutables, flow, EValueRepresentation::Any)
        , Flow_(flow)
        , Types_(std::move(types))
        , Width_(Types_.size())
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
        TComputationContext& ctx,
        NUdf::TUnboxedValue*const* output) const
    {
        auto& s = GetState(state, ctx);
        while (s.Index_ == s.Count_) {
            for (size_t i = 0; i < Width_; ++i) {
                s.Arrays_[i] = nullptr;
                s.Scalars_[i] = nullptr;
            }

            auto result = Flow_->FetchValues(ctx, s.ValuePointers_.data());
            if (result != EFetchResult::One) {
                return result;
            }

            s.Index_ = 0;
            for (size_t i = 0; i < Width_; ++i) {
                const auto& datum = TArrowBlock::From(s.Values_[i]).GetDatum();
                if (datum.is_scalar()) {
                    s.Scalars_[i] = datum.scalar();
                } else {
                    s.Arrays_[i] = datum.array();
                }
            }

            s.Count_ = TArrowBlock::From(s.Values_[Width_]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
        }

        for (size_t i = 0; i < Width_; ++i) {
            if (!output[i]) {
                continue;
            }

            const auto& array = s.Arrays_[i];
            if (array) {
                const auto nullCount = array->GetNullCount();
                if (nullCount == array->length || (nullCount > 0 && !arrow::BitUtil::GetBit(array->GetValues<uint8_t>(0, 0), s.Index_ + array->offset))) {
                    *(output[i]) = NUdf::TUnboxedValue();
                } else {
                    *(output[i]) = s.Readers_[i]->Get(*array, s.Index_);
                }
            } else {
                const auto& scalar = s.Scalars_[i];
                if (!scalar->is_valid) {
                    *(output[i]) = NUdf::TUnboxedValue();
                } else {
                    *(output[i]) = s.Readers_[i]->GetScalar(*scalar);
                }
            }
        }

        ++s.Index_;
        return EFetchResult::One;
    }

private:
    struct TState : public TComputationValue<TState> {
        TVector<NUdf::TUnboxedValue> Values_;
        TVector<NUdf::TUnboxedValue*> ValuePointers_;
        TVector<std::shared_ptr<arrow::ArrayData>> Arrays_;
        TVector<std::shared_ptr<arrow::Scalar>> Scalars_;
        TVector<std::unique_ptr<TBlockReaderBase>> Readers_;
        size_t Count_ = 0;
        size_t Index_ = 0;

        TState(TMemoryUsageInfo* memInfo, const TVector<TType*>& types, TComputationContext& ctx)
            : TComputationValue(memInfo)
            , Values_(types.size() + 1)
            , ValuePointers_(types.size() + 1)
            , Arrays_(types.size())
            , Scalars_(types.size())
        {
            for (size_t i = 0; i < types.size() + 1; ++i) {
                ValuePointers_[i] = &Values_[i];
            }

            for (size_t i = 0; i < types.size(); ++i) {
                Readers_.push_back(MakeBlockReader(types[i], ctx.HolderFactory));
            }
        }
    };

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(Types_, ctx);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    IComputationWideFlowNode* Flow_;
    const TVector<TType*> Types_;
    const size_t Width_;
};

class TAsScalarWrapper : public TMutableComputationNode<TAsScalarWrapper> {
public:
    TAsScalarWrapper(TComputationMutables& mutables, IComputationNode* arg, TType* type)
        : TMutableComputationNode(mutables)
        , Arg_(arg)
        , Type_(type)
    {
        bool isOptional;
        std::shared_ptr<arrow::DataType> arrowType;
        MKQL_ENSURE(ConvertArrowType(Type_, isOptional, arrowType), "Unsupported type of scalar");
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto value = Arg_->GetValue(ctx);
        arrow::Datum result = ConvertScalar(Type_, value);
        return ctx.HolderFactory.CreateArrowBlock(std::move(result));
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Arg_);
    }

    arrow::Datum ConvertScalar(TType* type, const NUdf::TUnboxedValuePod& value) const {
        if (!value) {
            bool isOptional;
            std::shared_ptr<arrow::DataType> arrowType;
            MKQL_ENSURE(ConvertArrowType(type, isOptional, arrowType), "Unsupported type of scalar");
            return arrow::MakeNullScalar(arrowType);
        }

        if (type->IsOptional()) {
            type = AS_TYPE(TOptionalType, type)->GetItemType();
        }

        if (type->IsTuple()) {
            auto tupleType = AS_TYPE(TTupleType, type);
            bool isOptional;
            std::shared_ptr<arrow::DataType> arrowType;
            MKQL_ENSURE(ConvertArrowType(type, isOptional, arrowType), "Unsupported type of scalar");

            std::vector<std::shared_ptr<arrow::Scalar>> arrowValue;
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                arrowValue.emplace_back(ConvertScalar(tupleType->GetElementType(i), value.GetElement(i)).scalar());
            }

            return arrow::Datum(std::make_shared<arrow::StructScalar>(arrowValue, arrowType));
        }

        if (type->IsData()) {
            auto slot = *AS_TYPE(TDataType, type)->GetDataSlot();
            switch (slot) {
            case NUdf::EDataSlot::Int8:
                return arrow::Datum(static_cast<int8_t>(value.Get<i8>()));
            case NUdf::EDataSlot::Bool:
            case NUdf::EDataSlot::Uint8:
                return arrow::Datum(static_cast<uint8_t>(value.Get<ui8>()));
            case NUdf::EDataSlot::Int16:
                return arrow::Datum(static_cast<int16_t>(value.Get<i16>()));
            case NUdf::EDataSlot::Uint16:
            case NUdf::EDataSlot::Date:
                return arrow::Datum(static_cast<uint16_t>(value.Get<ui16>()));
            case NUdf::EDataSlot::Int32:
                return arrow::Datum(static_cast<int32_t>(value.Get<i32>()));
            case NUdf::EDataSlot::Uint32:
            case NUdf::EDataSlot::Datetime:
                return arrow::Datum(static_cast<uint32_t>(value.Get<ui32>()));
            case NUdf::EDataSlot::Int64:
            case NUdf::EDataSlot::Interval:
                return arrow::Datum(static_cast<int64_t>(value.Get<i64>()));
            case NUdf::EDataSlot::Uint64:
            case NUdf::EDataSlot::Timestamp:
                return arrow::Datum(static_cast<uint64_t>(value.Get<ui64>()));
            case NUdf::EDataSlot::Float:
                return arrow::Datum(static_cast<float>(value.Get<float>()));
            case NUdf::EDataSlot::Double:
                return arrow::Datum(static_cast<double>(value.Get<double>()));
            default:
                MKQL_ENSURE(false, "Unsupported data slot");
            }
        }

        MKQL_ENSURE(false, "Unsupported type");
    }

private:
    IComputationNode* const Arg_;
    TType* Type_;
};

}

IComputationNode* WrapToBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());
    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    return new TToBlocksWrapper(LocateNode(ctx.NodeLocator, callable, 0), flowType->GetItemType());
}

IComputationNode* WrapWideToBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto tupleType = AS_TYPE(TTupleType, flowType->GetItemType());
    TVector<TType*> items;
    for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
        items.push_back(tupleType->GetElementType(i));
    }

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    return new TWideToBlocksWrapper(ctx.Mutables, wideFlow, std::move(items));
}

IComputationNode* WrapFromBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto blockType = AS_TYPE(TBlockType, flowType->GetItemType());
    return new TFromBlocksWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), blockType->GetItemType());
}

IComputationNode* WrapWideFromBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto tupleType = AS_TYPE(TTupleType, flowType->GetItemType());
    MKQL_ENSURE(tupleType->GetElementsCount() > 0, "Expected at least one column");
    TVector<TType*> items;
    for (ui32 i = 0; i < tupleType->GetElementsCount() - 1; ++i) {
        const auto blockType = AS_TYPE(TBlockType, tupleType->GetElementType(i));
        items.push_back(blockType->GetItemType());
    }

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    return new TWideFromBlocksWrapper(ctx.Mutables, wideFlow, std::move(items));
}

IComputationNode* WrapAsScalar(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    return new TAsScalarWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), callable.GetInput(0).GetStaticType());
}

}
}
