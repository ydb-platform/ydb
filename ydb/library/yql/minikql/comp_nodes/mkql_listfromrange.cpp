#include "mkql_listfromrange.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename T>
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

template<typename T, typename TStep>
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
class TListFromRangeWrapper : public TMutableCodegeneratorNode<TListFromRangeWrapper<T, TStep, TConstFactor, TzDate>> {
private:
    using TBaseComputation = TMutableCodegeneratorNode<TListFromRangeWrapper<T, TStep, TConstFactor, TzDate>>;

    class TValue : public TComputationValue<TValue> {
    public:
        template <bool Asc, bool Float>
        class TIterator;

        template <bool Asc>
        class TIterator<Asc, false> : public TComputationValue<TIterator<Asc, false>> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, T start, T end, TStep step)
                : TComputationValue<TIterator>(memInfo)
                , Current(start)
                , Step(step)
                , Count(GetElementsCount<T, TStep>(start, end, step))
            {}

        protected:
            bool Skip() final {
                if (!Count) {
                    return false;
                }
                Current += Step;
                --Count;
                return true;
            }

            bool Next(NUdf::TUnboxedValue& value) override {
                if (!Count) {
                    return false;
                }

                value = NUdf::TUnboxedValuePod(Current);
                Current += Step;
                --Count;
                return true;
            }

            T Current;
            const TStep Step;
            ui64 Count;
        };

        template <bool Asc>
        class TIterator<Asc, true> : public TComputationValue<TIterator<Asc, true>> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, T start, T end, TStep step)
                : TComputationValue<TIterator>(memInfo)
                , Start(start)
                , Index(-T(1))
                , Limit(end - start)
                , Step(step)
            {}

        private:
            bool Skip() final {
                const auto next = Index + T(1);
                if (Asc ? next * Step < Limit : next * Step > Limit) {
                    Index = next;
                    return true;
                }

                return false;
            }

            bool Next(NUdf::TUnboxedValue& value) final {
                if (!Skip()) {
                    return false;
                }

                value = NUdf::TUnboxedValuePod(Start + Index * Step);
                return true;
            }

            const T Start;
            T Index;
            const T Limit;
            const TStep Step;
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
            if (Step > TStep(0)) {
                return Ctx.HolderFactory.template Create<TIterator<true, std::is_floating_point<T>::value>>(Start, End, Step);
            } else if (Step < TStep(0)) {
                return Ctx.HolderFactory.template Create<TIterator<false, std::is_floating_point<T>::value>>(Start, End, Step);
            } else {
                return Ctx.HolderFactory.GetEmptyContainerLazy();
            }
        }

        ui64 GetListLength() const final {
            if constexpr (std::is_integral_v<T>) {
                return GetElementsCount<T, TStep>(Start, End, Step);
            }

            if (Step > T(0) && Start < End) {
                ui64 len = 0ULL;
                for (T i = 0; i * Step < End - Start; i += T(1)) {
                    ++len;
                }
                return len;
            } else if (Step < T(0) && Start > End) {
                ui64 len = 0ULL;
                for (T i = 0; i * Step > End - Start; i += T(1)) {
                    ++len;
                }
                return len;
            } else {
                return 0ULL;
            }
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
            return std::is_integral<T>();
        }

        TComputationContext& Ctx;
        const T Start;
        const T End;
        const TStep Step;
    };

    class TTzValue : public TValue {
    public:
        template <bool Asc>
        class TTzIterator : public TValue::template TIterator<Asc, false> {
        public:
            using TBase = typename TValue::template TIterator<Asc, false>;
            TTzIterator(TMemoryUsageInfo* memInfo, T start, T end, TStep step, ui16 Tz)
                : TBase(memInfo, start, end, step)
                , TimezoneId(Tz)
            {}
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
            if (TValue::Step > TStep(0)) {
                return TValue::Ctx.HolderFactory.template Create<TTzIterator<true>>(TValue::Start, TValue::End, TValue::Step, TimezoneId);
            } else if (TValue::Step < TStep(0)) {
                return TValue::Ctx.HolderFactory.template Create<TTzIterator<false>>(TValue::Start, TValue::End, TValue::Step, TimezoneId);
            } else {
                return TValue::Ctx.HolderFactory.GetEmptyContainerLazy();
            }
        }
        TTzValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, T start, T end, TStep step, ui16 TimezoneId)
            : TValue(memInfo, ctx, start, end, step), TimezoneId(TimezoneId)
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
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto start = Start->GetValue(ctx);
        const auto end = End->GetValue(ctx);
        auto step = Step->GetValue(ctx).Get<TStep>();
        if constexpr (TConstFactor > 1) {
            if (step % TConstFactor)
                step = 0;
            else
                step /= TConstFactor;
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

        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TListFromRangeWrapper::MakeList));
        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto signature = FunctionType::get(valueType, {ctx.Ctx->getType(), start->getType(), end->getType(), step->getType(), timezone->getType()}, false);
            const auto creator = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(signature), "creator", block);
            const auto output = CallInst::Create(signature, creator, {ctx.Ctx, start, end, step, timezone}, "output", block);
            return output;
        } else {
            const auto place = new AllocaInst(valueType, 0U, "place", block);
            const auto signature = FunctionType::get(Type::getVoidTy(context), {place->getType(), ctx.Ctx->getType(), start->getType(), end->getType(), step->getType(), timezone->getType()}, false);
            const auto creator = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(signature), "creator", block);
            CallInst::Create(signature, creator, {place, ctx.Ctx, start, end, step, timezone}, "", block);
            const auto output = new LoadInst(valueType, place, "output", block);
            return output;
        }
    }
#endif
private:
    static NUdf::TUnboxedValuePod MakeList(TComputationContext& ctx, T start, T end, TStep step, ui16 timezoneId) {
        if constexpr(TzDate)
            return ctx.HolderFactory.Create<TTzValue>(ctx, start, end, step, timezoneId);
        else
            return ctx.HolderFactory.Create<TValue>(ctx, start, end, step);
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

}

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

}
}
