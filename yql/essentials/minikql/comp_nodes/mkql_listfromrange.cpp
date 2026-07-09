#include "mkql_listfromrange.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_safe_arithmetic_ops.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename T>
ui64 ShiftByMaxNegative(T value) {
    static_assert(sizeof(T) <= sizeof(ui64));
    static_assert(std::is_integral_v<T>);
    if constexpr (std::is_signed_v<T>) {
        if (value < 0) {
            return ui64(value + std::numeric_limits<T>::max() + T(1));
        }
        return ui64(value) + ui64(std::numeric_limits<T>::max()) + 1ul;
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
        newStep = (step == std::numeric_limits<TStep>::min()) ? (ui64(std::numeric_limits<TStep>::max()) + 1ul) : ui64(TStep(0) - step);
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
                , Current(start)
                , Step(step)
                , Count(GetElementsCount<T, TStep>(start, end, step))
            {
            }

        protected:
            bool Skip() final {
                if (!Count) {
                    return false;
                }
                AddStep();
                return true;
            }

            bool Next(NUdf::TUnboxedValue& value) override {
                if (!Count) {
                    return false;
                }

                value = NUdf::TUnboxedValuePod(Current);
                AddStep();
                return true;
            }

            T Current;
            const TStep Step;
            ui64 Count;

        private:
            void AddStep() {
                Current = SafeAdd(Current, static_cast<T>(Step));
                --Count;
            }
        };

        TValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, T start, T end, TStep step)
            : TComputationValue<TValue>(memInfo)
            , Ctx(ctx)
            , Start(start)
            , End(end)
            , Step(step)
        {
        }

    protected:
        NUdf::TUnboxedValue GetListIterator() const override {
            if (Step != TStep(0)) {
                return Ctx.HolderFactory.template Create<TIterator>(Start, End, Step);
            } else {
                return Ctx.HolderFactory.GetEmptyContainerLazy();
            }
        }

        ui64 GetListLength() const final {
            static_assert(std::is_integral_v<T>, "Invalid type");
            return GetElementsCount<T, TStep>(Start, End, Step);
        }

        bool HasListItems() const final {
            if (Step > TStep(0)) {
                return Start < End;
            } else if (Step < TStep(0)) {
                return Start > End;
            } else {
                return false;
            }
        }

        bool HasFastListLength() const final {
            return true;
        }

        TComputationContext& Ctx;
        const T Start;
        const T End;
        const TStep Step;
    };

    class TFloatingValue: public TCustomListValue {
        static constexpr ui64 MaxElementsCount = std::numeric_limits<ui64>::max();

    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, T start, T end, TStep step)
                : TComputationValue<TIterator>(memInfo)
                , Start(start)
                , End(end)
                , Step(step)
                , ValidInput(CheckInput(start, end, step))
                , Index(0ULL)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                if (!ValidInput || Index >= MaxElementsCount) {
                    return false;
                }
                const T current = Start + Index * Step;
                const bool outOfRange = Step > 0 ? current >= End : current <= End;
                if (outOfRange) {
                    return false;
                }
                value = NUdf::TUnboxedValuePod(current);
                Index++;
                return true;
            }

            static bool CheckInput(T start, T end, TStep step) {
                if (step == T(0) || std::isnan(step) || std::isnan(start) || std::isnan(end) ||
                    std::isinf(start) || std::isinf(end) || std::isinf(step) || start == end) {
                    return false;
                }

                return true;
            }

            const T Start;
            const T End;
            const TStep Step;
            const bool ValidInput;
            ui64 Index;
        };
        TFloatingValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, T start, T end, TStep step)
            : TCustomListValue(memInfo)
            , Ctx(ctx)
            , Start(start)
            , End(end)
            , Step(step)
        {
        }

    protected:
        NUdf::TUnboxedValue GetListIterator() const override {
            return Ctx.HolderFactory.template Create<TIterator>(Start, End, Step);
        }

        TComputationContext& Ctx;
        const T Start;
        const T End;
        const TStep Step;
    };

    class TTzValue: public TValue {
    public:
        class TTzIterator: public TValue::TIterator {
        public:
            using TBase = typename TValue::TIterator;
            TTzIterator(TMemoryUsageInfo* memInfo, T start, T end, TStep step, ui16 Tz)
                : TBase(memInfo, start, end, step)
                , TimezoneId(Tz)
            {
            }
            bool Next(NUdf::TUnboxedValue& value) final {
                if (TBase::Next(value)) {
                    value.SetTimezoneId(TimezoneId);
                    return true;
                }
                return false;
            }

        private:
            const ui16 TimezoneId;
        };
        NUdf::TUnboxedValue GetListIterator() const final {
            if (TValue::Step != TStep(0)) {
                return TValue::Ctx.HolderFactory.template Create<TTzIterator>(TValue::Start, TValue::End, TValue::Step, TimezoneId);
            } else {
                return TValue::Ctx.HolderFactory.GetEmptyContainerLazy();
            }
        }
        TTzValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, T start, T end, TStep step, ui16 TimezoneId)
            : TValue(memInfo, ctx, start, end, step)
            , TimezoneId(TimezoneId)
        {
        }

    private:
        const ui16 TimezoneId;
    };

public:
    TListFromRangeWrapper(TComputationMutables& mutables, IComputationNode* start, IComputationNode* end, IComputationNode* step)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Start(start)
        , End(end)
        , Step(step)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto start = Start->GetValue(ctx);
        const auto end = End->GetValue(ctx);
        auto step = Step->GetValue(ctx).Get<TStep>();
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
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);

        const auto startv = GetNodeValue(Start, ctx, block);
        const auto endv = GetNodeValue(End, ctx, block);
        const auto stepv = GetNodeValue(Step, ctx, block);

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
        this->DependsOn(Start);
        this->DependsOn(End);
        this->DependsOn(Step);
    }

    IComputationNode* const Start;
    IComputationNode* const End;
    IComputationNode* const Step;
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
            return new TListFromRangeWrapper<ui16, i64, 86400000000ll>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::Date32:
            return new TListFromRangeWrapper<i32, i64, 86400000000ll>(ctx.Mutables, start, end, step);
        case NUdf::EDataSlot::TzDate:
            return new TListFromRangeWrapper<ui16, i64, 86400000000ll, true>(ctx.Mutables, start, end, step);
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

} // namespace NMiniKQL
} // namespace NKikimr
