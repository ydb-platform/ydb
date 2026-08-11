#include "mkql_listfromrange.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_safe_arithmetic_ops.h>

namespace NKikimr::NMiniKQL {

namespace {

template <typename T>
ui64 ShiftByMaxNegative(T value) {
    static_assert(sizeof(T) <= sizeof(ui64));
    static_assert(std::is_integral_v<T>);
    if constexpr (std::is_signed_v<T>) {
        if (value < 0) {
            return ui64(value + std::numeric_limits<T>::max() + T(1));
        }
        return ui64(value) + ui64(std::numeric_limits<T>::max()) + 1UL;
    }
    return ui64(value);
}

ui64 GetElementsCount(ui64 start, ui64 end, ui64 step) {
    if (step == 0 || start >= end) {
        return 0;
    }

    ui64 diff = end - start;
    ui64 div = diff / step;
    ui64 rem = diff % step;

    return rem ? (div + 1) : div;
}

template <typename T, typename TStep>
ui64 GetElementsCount(T start, T end, TStep step) {
    ui64 newStart = ShiftByMaxNegative(start);
    ui64 newEnd = ShiftByMaxNegative(end);
    ui64 newStep;

    if (step < 0) {
        newStep = (step == std::numeric_limits<TStep>::min()) ? (ui64(std::numeric_limits<TStep>::max()) + 1UL) : ui64(TStep(0) - step);
        std::swap(newStart, newEnd);
    } else {
        newStep = ui64(step);
    }

    return GetElementsCount(newStart, newEnd, newStep);
}

template <typename T, typename TStep = std::make_signed_t<T>, std::conditional_t<std::is_floating_point_v<TStep>, i8, TStep> TConstFactor = 1, bool TzDate = false>
class TListFromRangeWrapper: public TMutableCodegeneratorNode<TListFromRangeWrapper<T, TStep, TConstFactor, TzDate>> {
private:
    using TBaseComputation = TMutableCodegeneratorNode<TListFromRangeWrapper<T, TStep, TConstFactor, TzDate>>;

    class TValue: public TComputationValue<TValue> {
    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, T start, T end, TStep step)
                : TComputationValue<TIterator>(memInfo)
                , Current_(start)
                , Step_(step)
                , Count_(GetElementsCount<T, TStep>(start, end, step))
            {
            }

        protected:
            bool Skip() final {
                if (!Count_) {
                    return false;
                }
                AddStep();
                return true;
            }

            bool Next(NUdf::TUnboxedValue& value) override {
                if (!Count_) {
                    return false;
                }

                value = NUdf::TUnboxedValuePod(Current_);
                AddStep();
                return true;
            }

            T Current_;
            const TStep Step_;
            ui64 Count_;

        private:
            void AddStep() {
                Current_ = SafeAdd(Current_, static_cast<T>(Step_));
                --Count_;
            }
        };

        TValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, T start, T end, TStep step)
            : TComputationValue<TValue>(memInfo)
            , Ctx_(ctx)
            , Start_(start)
            , End_(end)
            , Step_(step)
        {
        }

    protected:
        NUdf::TUnboxedValue GetListIterator() const override {
            if (Step_ != TStep(0)) {
                return Ctx_.HolderFactory.template Create<TIterator>(Start_, End_, Step_);
            } else {
                return Ctx_.HolderFactory.GetEmptyContainerLazy();
            }
        }

        ui64 GetListLength() const final {
            static_assert(std::is_integral_v<T>, "Invalid type");
            return GetElementsCount<T, TStep>(Start_, End_, Step_);
        }

        bool HasListItems() const final {
            if (Step_ > TStep(0)) {
                return Start_ < End_;
            } else if (Step_ < TStep(0)) {
                return Start_ > End_;
            } else {
                return false;
            }
        }

        bool HasFastListLength() const final {
            return true;
        }

        TComputationContext& Ctx_;
        const T Start_;
        const T End_;
        const TStep Step_;
    };

    class TFloatingValue: public TCustomListValue {
        static constexpr ui64 MaxElementsCount = std::numeric_limits<ui64>::max();

    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, T start, T end, TStep step)
                : TComputationValue<TIterator>(memInfo)
                , Start_(start)
                , End_(end)
                , Step_(step)
                , ValidInput_(CheckInput(start, end, step))
                , Index_(0ULL)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                if (!ValidInput_ || Index_ >= MaxElementsCount) {
                    return false;
                }
                const T current = Start_ + Index_ * Step_;
                const bool outOfRange = Step_ > 0 ? current >= End_ : current <= End_;
                if (outOfRange) {
                    return false;
                }
                value = NUdf::TUnboxedValuePod(current);
                Index_++;
                return true;
            }

            static bool CheckInput(T start, T end, TStep step) {
                return !static_cast<bool>(step == T(0) || std::isnan(step) || std::isnan(start) || std::isnan(end) ||
                                          std::isinf(start) || std::isinf(end) || std::isinf(step) || start == end);
            }

            const T Start_;
            const T End_;
            const TStep Step_;
            const bool ValidInput_;
            ui64 Index_;
        };
        TFloatingValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, T start, T end, TStep step)
            : TCustomListValue(memInfo)
            , Ctx_(ctx)
            , Start_(start)
            , End_(end)
            , Step_(step)
        {
        }

    protected:
        NUdf::TUnboxedValue GetListIterator() const override {
            return Ctx_.HolderFactory.template Create<TIterator>(Start_, End_, Step_);
        }

        TComputationContext& Ctx_;
        const T Start_;
        const T End_;
        const TStep Step_;
    };

    class TTzValue: public TValue {
    public:
        class TTzIterator: public TValue::TIterator {
        public:
            using TBase = typename TValue::TIterator;
            TTzIterator(TMemoryUsageInfo* memInfo, T start, T end, TStep step, ui16 Tz)
                : TBase(memInfo, start, end, step)
                , TimezoneId_(Tz)
            {
            }
            bool Next(NUdf::TUnboxedValue& value) final {
                if (TBase::Next(value)) {
                    value.SetTimezoneId(TimezoneId_);
                    return true;
                }
                return false;
            }

        private:
            const ui16 TimezoneId_;
        };
        NUdf::TUnboxedValue GetListIterator() const final {
            if (TValue::Step_ != TStep(0)) {
                return TValue::Ctx_.HolderFactory.template Create<TTzIterator>(TValue::Start_, TValue::End_, TValue::Step_, TimezoneId_);
            } else {
                return TValue::Ctx_.HolderFactory.GetEmptyContainerLazy();
            }
        }
        TTzValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, T start, T end, TStep step, ui16 TimezoneId)
            : TValue(memInfo, ctx, start, end, step)
            , TimezoneId_(TimezoneId)
        {
        }

    private:
        const ui16 TimezoneId_;
    };

public:
    TListFromRangeWrapper(TComputationMutables& mutables, IComputationNode* start, IComputationNode* end, IComputationNode* step)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Start_(start)
        , End_(end)
        , Step_(step)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto start = Start_->GetValue(ctx);
        const auto end = End_->GetValue(ctx);
        auto step = Step_->GetValue(ctx).Get<TStep>();
        if constexpr (TConstFactor > 1) {
            if (step % TConstFactor) {
                step = 0;
            } else {
                step /= TConstFactor;
            }
        }

        if constexpr (TzDate) {
            return MakeList(ctx, start.Get<T>(), end.Get<T>(), step, start.GetTimezoneId());
        } else {
            return MakeList(ctx, start.Get<T>(), end.Get<T>(), step, 0U);
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);

        const auto startv = GetNodeValue(Start_, ctx, block);
        const auto endv = GetNodeValue(End_, ctx, block);
        const auto stepv = GetNodeValue(Step_, ctx, block);

        const auto start = GetterFor<T>(startv, context, block);
        const auto end = GetterFor<T>(endv, context, block);

        auto step = GetterFor<TStep>(stepv, context, block);
        if constexpr (TConstFactor > 1) {
            const auto zero = ConstantInt::get(GetTypeFor<TStep>(context), 0);
            const auto fact = ConstantInt::get(GetTypeFor<TStep>(context), TConstFactor);
            const auto div = BinaryOperator::CreateSDiv(step, fact, "div", block);
            const auto rem = BinaryOperator::CreateSRem(step, fact, "rem", block);
            const auto bad = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, rem, zero, "bad", block);
            step = SelectInst::Create(bad, zero, div, "step", block);
        }

        const auto timezone = TzDate ? GetterForTimezone(context, startv, block) : ConstantInt::get(Type::getInt16Ty(context), 0);

        return EmitFunctionCall<&TListFromRangeWrapper::MakeList>(valueType, {ctx.Ctx, start, end, step, timezone}, ctx, block);
    }
#endif
private:
    static NUdf::TUnboxedValuePod MakeList(TComputationContext& ctx, T start, T end, TStep step, ui16 timezoneId) {
        if constexpr (TzDate) {
            return ctx.HolderFactory.Create<TTzValue>(ctx, start, end, step, timezoneId);
        } else if constexpr (std::is_floating_point_v<T>) {
            return ctx.HolderFactory.Create<TFloatingValue>(ctx, start, end, step);
        } else {
            return ctx.HolderFactory.Create<TValue>(ctx, start, end, step);
        }
    }

    void RegisterDependencies() const final {
        this->DependsOn(Start_);
        this->DependsOn(End_);
        this->DependsOn(Step_);
    }

    IComputationNode* const Start_;
    IComputationNode* const End_;
    IComputationNode* const Step_;
};

} // namespace

IComputationNode* WrapListFromRange(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");

    const auto start = LocateNode(ctx.NodeLocator, callable, 0);
    const auto end = LocateNode(ctx.NodeLocator, callable, 1);
    const auto step = LocateNode(ctx.NodeLocator, callable, 2);
    switch (*AS_TYPE(TDataType, callable.GetInput(0).GetStaticType())->GetDataSlot()) {
        case NUdf::EDataSlot::Uint8:
            return new TListFromRangeWrapper<ui8>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Int8:
            return new TListFromRangeWrapper<i8>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Uint16:
            return new TListFromRangeWrapper<ui16>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Int16:
            return new TListFromRangeWrapper<i16>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Uint32:
            return new TListFromRangeWrapper<ui32>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Int32:
            return new TListFromRangeWrapper<i32>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Uint64:
            return new TListFromRangeWrapper<ui64>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Int64:
            return new TListFromRangeWrapper<i64>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Float:
            return new TListFromRangeWrapper<float, float>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Double:
            return new TListFromRangeWrapper<double, double>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Date:
            return new TListFromRangeWrapper<ui16, i64, 86400000000LL>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Date32:
            return new TListFromRangeWrapper<i32, i64, 86400000000LL>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::TzDate:
            return new TListFromRangeWrapper<ui16, i64, 86400000000LL, true>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Datetime:
            return new TListFromRangeWrapper<ui32, i64, 1000000>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Datetime64:
            return new TListFromRangeWrapper<i64, i64, 1000000>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::TzDatetime:
            return new TListFromRangeWrapper<ui32, i64, 1000000, true>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Timestamp:
            return new TListFromRangeWrapper<ui64, i64, 1>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Timestamp64:
            return new TListFromRangeWrapper<i64, i64, 1>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::TzTimestamp:
            return new TListFromRangeWrapper<ui64, i64, 1, true>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Interval:
        case NUdf::EDataSlot::Interval64:
            return new TListFromRangeWrapper<i64, i64, 1>(ctx.Mutables, start, end, step);
        default:
            MKQL_ENSURE(false, "unexpected");
    }
}

} // namespace NKikimr::NMiniKQL
