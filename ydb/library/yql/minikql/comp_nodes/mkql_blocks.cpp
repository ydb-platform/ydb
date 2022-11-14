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
    TBlockBuilderBase(TComputationContext& ctx, const std::shared_ptr<arrow::DataType>& itemType)
        : Ctx_(ctx)
        , ItemType_(itemType)
        , MaxLength_(MaxBlockSizeInBytes / TypeSize(*ItemType_))
    {}

    virtual ~TBlockBuilderBase() = default;

    inline size_t MaxLength() const noexcept {
        return MaxLength_;
    }

    virtual void Add(NUdf::TUnboxedValue& value) = 0;
    virtual NUdf::TUnboxedValuePod Build(bool finish) = 0;

private:
    static int64_t TypeSize(arrow::DataType& itemType) {
        const auto bits = static_cast<const arrow::FixedWidthType&>(itemType).bit_width();
        return arrow::BitUtil::BytesForBits(bits);
    }

protected:
    TComputationContext& Ctx_;
    const std::shared_ptr<arrow::DataType> ItemType_;
    const size_t MaxLength_;
};

template <typename T, typename TBuilder>
class TFixedSizeBlockBuilder : public TBlockBuilderBase {
public:
    TFixedSizeBlockBuilder(TComputationContext& ctx, const std::shared_ptr<arrow::DataType>& itemType)
        : TBlockBuilderBase(ctx, itemType)
        , Builder_(std::make_unique<TBuilder>(&Ctx_.ArrowMemoryPool))
    {
        this->Reserve();
    }

    void Add(NUdf::TUnboxedValue& value) override {
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

private:
    std::unique_ptr<TBuilder> Builder_;
};

std::unique_ptr<TBlockBuilderBase> MakeBlockBuilder(TComputationContext& ctx, NUdf::EDataSlot slot) {
    switch (slot) {
    case NUdf::EDataSlot::Bool:
        return std::make_unique<TFixedSizeBlockBuilder<bool, arrow::BooleanBuilder>>(ctx, arrow::boolean());
    case NUdf::EDataSlot::Int8:
        return std::make_unique<TFixedSizeBlockBuilder<i8, arrow::Int8Builder>>(ctx, arrow::int8());
    case NUdf::EDataSlot::Uint8:
        return std::make_unique<TFixedSizeBlockBuilder<ui8, arrow::UInt8Builder>>(ctx, arrow::uint8());
    case NUdf::EDataSlot::Int16:
        return std::make_unique<TFixedSizeBlockBuilder<i16, arrow::Int16Builder>>(ctx, arrow::int16());
    case NUdf::EDataSlot::Uint16:
        return std::make_unique<TFixedSizeBlockBuilder<ui16, arrow::UInt16Builder>>(ctx, arrow::uint16());
    case NUdf::EDataSlot::Int32:
        return std::make_unique<TFixedSizeBlockBuilder<i32, arrow::Int32Builder>>(ctx, arrow::int32());
    case NUdf::EDataSlot::Uint32:
        return std::make_unique<TFixedSizeBlockBuilder<ui32, arrow::UInt32Builder>>(ctx, arrow::uint32());
    case NUdf::EDataSlot::Int64:
        return std::make_unique<TFixedSizeBlockBuilder<i64, arrow::Int64Builder>>(ctx, arrow::int64());
    case NUdf::EDataSlot::Uint64:
        return std::make_unique<TFixedSizeBlockBuilder<ui64, arrow::UInt64Builder>>(ctx, arrow::uint64());
    default:
        MKQL_ENSURE(false, "Unsupported data slot");
    }
}

class TToBlocksWrapper: public TStatelessFlowComputationNode<TToBlocksWrapper> {
public:
    explicit TToBlocksWrapper(IComputationNode* flow, NUdf::EDataSlot slot)
        : TStatelessFlowComputationNode(flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , Slot_(slot)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto builder = MakeBlockBuilder(ctx, Slot_);

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
    NUdf::EDataSlot Slot_;
};

class TWideToBlocksWrapper : public TStatefulWideFlowComputationNode<TWideToBlocksWrapper> {
public:
    TWideToBlocksWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        TVector<NUdf::EDataSlot>&& slots)
        : TStatefulWideFlowComputationNode(mutables, flow, EValueRepresentation::Any)
        , Flow_(flow)
        , Slots_(std::move(slots))
        , Width_(Slots_.size())
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

        TState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TVector<NUdf::EDataSlot>& slots)
            : TComputationValue(memInfo)
            , Values_(slots.size())
            , ValuePointers_(slots.size())
        {
            for (size_t i = 0; i < slots.size(); ++i) {
                ValuePointers_[i] = &Values_[i];
                Builders_.push_back(MakeBlockBuilder(ctx, slots[i]));
            }

            MaxLength_ = MaxBlockSizeInBytes;
            for (size_t i = 0; i < slots.size(); ++i) {
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
            state = ctx.HolderFactory.Create<TState>(ctx, Slots_);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    IComputationWideFlowNode* Flow_;
    const TVector<NUdf::EDataSlot> Slots_;
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
        return NUdf::TUnboxedValuePod(data.GetValues<T>(1)[index]);
    }

    NUdf::TUnboxedValuePod GetScalar(const arrow::Scalar& scalar) final {
        return NUdf::TUnboxedValuePod(*static_cast<const T*>(arrow::internal::checked_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).data()));
    }
};

class TBoolBlockReader : public TBlockReaderBase {
public:
    NUdf::TUnboxedValuePod Get(const arrow::ArrayData& data, size_t index) final {
        return NUdf::TUnboxedValuePod(arrow::BitUtil::GetBit(data.GetValues<uint8_t>(1, 0), index + data.offset));
    }

    NUdf::TUnboxedValuePod GetScalar(const arrow::Scalar& scalar) final {
        return NUdf::TUnboxedValuePod(arrow::internal::checked_cast<const arrow::BooleanScalar&>(scalar).value);
    }
};

std::unique_ptr<TBlockReaderBase> MakeBlockReader(NUdf::EDataSlot slot) {
    switch (slot) {
    case NUdf::EDataSlot::Bool:
        return std::make_unique<TBoolBlockReader>();
    case NUdf::EDataSlot::Int8:
        return std::make_unique<TFixedSizeBlockReader<i8>>();
    case NUdf::EDataSlot::Uint8:
        return std::make_unique<TFixedSizeBlockReader<ui8>>();
    case NUdf::EDataSlot::Int16:
        return std::make_unique<TFixedSizeBlockReader<i16>>();
    case NUdf::EDataSlot::Uint16:
        return std::make_unique<TFixedSizeBlockReader<ui16>>();
    case NUdf::EDataSlot::Int32:
        return std::make_unique<TFixedSizeBlockReader<i32>>();
    case NUdf::EDataSlot::Uint32:
        return std::make_unique<TFixedSizeBlockReader<ui32>>();
    case NUdf::EDataSlot::Int64:
        return std::make_unique<TFixedSizeBlockReader<i64>>();
    case NUdf::EDataSlot::Uint64:
        return std::make_unique<TFixedSizeBlockReader<ui64>>();
    default:
        MKQL_ENSURE(false, "Unsupported data slot");
    }
}

class TFromBlocksWrapper : public TMutableComputationNode<TFromBlocksWrapper> {
public:
    TFromBlocksWrapper(TComputationMutables& mutables, IComputationNode* flow, NUdf::EDataSlot slot)
        : TMutableComputationNode(mutables)
        , Flow_(flow)
        , Slot_(slot)
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

        TState(TMemoryUsageInfo* memInfo, NUdf::EDataSlot slot)
            : TComputationValue(memInfo)
        {
            Reader_ = MakeBlockReader(slot);
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
        std::shared_ptr<arrow::ArrayData> Array_{nullptr};
        size_t Index_{0};
    };

private:
    void RegisterDependencies() const final {
        this->DependsOn(Flow_);
    }

    TState& GetState(TComputationContext& ctx) const {
        auto& result = ctx.MutableValues[StateIndex_];
        if (!result.HasValue()) {
            result = ctx.HolderFactory.Create<TState>(Slot_);
        }
        return *static_cast<TState*>(result.AsBoxed().Get());
    }

private:
    IComputationNode* const Flow_;
    const NUdf::EDataSlot Slot_;
    const ui32 StateIndex_;
};

class TWideFromBlocksWrapper : public TStatefulWideFlowComputationNode<TWideFromBlocksWrapper> {
public:
    TWideFromBlocksWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        TVector<NUdf::EDataSlot>&& slots)
        : TStatefulWideFlowComputationNode(mutables, flow, EValueRepresentation::Any)
        , Flow_(flow)
        , Slots_(std::move(slots))
        , Width_(Slots_.size())
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

        TState(TMemoryUsageInfo* memInfo, const TVector<NUdf::EDataSlot>& slots)
            : TComputationValue(memInfo)
            , Values_(slots.size() + 1)
            , ValuePointers_(slots.size() + 1)
            , Arrays_(slots.size())
            , Scalars_(slots.size())
        {
            for (size_t i = 0; i < slots.size() + 1; ++i) {
                ValuePointers_[i] = &Values_[i];
            }

            for (size_t i = 0; i < slots.size(); ++i) {
                Readers_.push_back(MakeBlockReader(slots[i]));
            }
        }
    };

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(Slots_);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    IComputationWideFlowNode* Flow_;
    const TVector<NUdf::EDataSlot> Slots_;
    const size_t Width_;
};

class TAsScalarWrapper : public TMutableComputationNode<TAsScalarWrapper> {
public:
    TAsScalarWrapper(TComputationMutables& mutables, IComputationNode* arg, TType* type)
        : TMutableComputationNode(mutables)
        , Arg_(arg)
    {
        bool isOptional;
        auto unpacked = UnpackOptionalData(type, isOptional);
        MKQL_ENSURE(ConvertArrowType(unpacked, isOptional, Type_), "Unsupported type of scalar");
        Slot_ = *unpacked->GetDataSlot();
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto value = Arg_->GetValue(ctx);
        arrow::Datum result;
        if (!value) {
            result = arrow::MakeNullScalar(Type_);
        } else {
            switch (Slot_) {
            case NUdf::EDataSlot::Bool:
                result = arrow::Datum(static_cast<bool>(value.Get<bool>()));
                break;
            case NUdf::EDataSlot::Int8:
                result = arrow::Datum(static_cast<int8_t>(value.Get<i8>()));
                break;
            case NUdf::EDataSlot::Uint8:
                result = arrow::Datum(static_cast<uint8_t>(value.Get<ui8>()));
                break;
            case NUdf::EDataSlot::Int16:
                result = arrow::Datum(static_cast<int16_t>(value.Get<i16>()));
                break;
            case NUdf::EDataSlot::Uint16:
                result = arrow::Datum(static_cast<uint16_t>(value.Get<ui16>()));
                break;
            case NUdf::EDataSlot::Int32:
                result = arrow::Datum(static_cast<int32_t>(value.Get<i32>()));
                break;
            case NUdf::EDataSlot::Uint32:
                result = arrow::Datum(static_cast<uint32_t>(value.Get<ui32>()));
                break;
            case NUdf::EDataSlot::Int64:
                result = arrow::Datum(static_cast<int64_t>(value.Get<i64>()));
                break;
            case NUdf::EDataSlot::Uint64:
                result = arrow::Datum(static_cast<uint64_t>(value.Get<ui64>()));
                break;
            default:
                MKQL_ENSURE(false, "Unsupported data slot");
            }
        }

        return ctx.HolderFactory.CreateArrowBlock(std::move(result));
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Arg_);
    }

private:
    IComputationNode* const Arg_;
    std::shared_ptr<arrow::DataType> Type_;
    NUdf::EDataSlot Slot_;
};

}

IComputationNode* WrapToBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());
    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    bool isOptional;
    const auto slot = *UnpackOptionalData(flowType->GetItemType(), isOptional)->GetDataSlot();
    return new TToBlocksWrapper(LocateNode(ctx.NodeLocator, callable, 0), slot);
}

IComputationNode* WrapWideToBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto tupleType = AS_TYPE(TTupleType, flowType->GetItemType());
    TVector<NUdf::EDataSlot> slots;
    for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
        bool isOptional;
        const auto slot = *UnpackOptionalData(tupleType->GetElementType(i), isOptional)->GetDataSlot();
        slots.push_back(slot);
    }

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    return new TWideToBlocksWrapper(ctx.Mutables, wideFlow, std::move(slots));
}

IComputationNode* WrapFromBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto blockType = AS_TYPE(TBlockType, flowType->GetItemType());
    bool isOptional;
    const auto slot = *UnpackOptionalData(blockType->GetItemType(), isOptional)->GetDataSlot();
    return new TFromBlocksWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), slot);
}

IComputationNode* WrapWideFromBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto tupleType = AS_TYPE(TTupleType, flowType->GetItemType());
    MKQL_ENSURE(tupleType->GetElementsCount() > 0, "Expected at least one column");
    TVector<NUdf::EDataSlot> slots;
    for (ui32 i = 0; i < tupleType->GetElementsCount() - 1; ++i) {
        const auto blockType = AS_TYPE(TBlockType, tupleType->GetElementType(i));
        bool isOptional;
        const auto slot = *UnpackOptionalData(blockType->GetItemType(), isOptional)->GetDataSlot();
        slots.push_back(slot);
    }

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    return new TWideFromBlocksWrapper(ctx.Mutables, wideFlow, std::move(slots));
}

IComputationNode* WrapAsScalar(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    return new TAsScalarWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), callable.GetInput(0).GetStaticType());
}

}
}
