#include "mkql_listfromrange.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_safe_arithmetic_ops.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

namespace NKikimr::NMiniKQL {

namespace {

template <typename T>
constexpr bool IsNonInteger = std::is_floating_point_v<T> || std::is_same_v<T, NYql::NDecimal::TInt128>;

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

template <typename T, typename TStep>
struct TDefaultRangeOperations {
    static constexpr bool UseCustomList = false;

#ifndef MKQL_DISABLE_CODEGEN
    static Type* GetCodegenType(LLVMContext& context) {
        static_assert(sizeof(TDefaultRangeOperations) == 1);
        return Type::getInt8Ty(context);
    }

    static Value* GetCodegenValue(Value* value, LLVMContext& context, BasicBlock* block) {
        return GetterFor<T>(value, context, block);
    }

    static Value* GetCodegenStep(Value* value, LLVMContext& context, BasicBlock* block) {
        return GetterFor<TStep>(value, context, block);
    }
#endif

    static T GetValue(const NUdf::TUnboxedValuePod& value) {
        return value.Get<T>();
    }

    static TStep GetStep(const NUdf::TUnboxedValuePod& value) {
        return value.Get<TStep>();
    }
};

template <typename T>
struct TFloatingRangeOperations: public TDefaultRangeOperations<T, T> {
    static constexpr bool UseCustomList = true;

#ifndef MKQL_DISABLE_CODEGEN
    static Type* GetCodegenType(LLVMContext& context) {
        static_assert(sizeof(TFloatingRangeOperations) == 1);
        return Type::getInt8Ty(context);
    }
#endif

    T Add(T start, T step, ui64 index) const {
        return start + index * step;
    }

    T Sub(T left, T right) const {
        return left - right;
    }

    bool IsNormal(T value) const {
        return !std::isnan(value) && !std::isinf(value);
    }

    bool Equal(T left, T right) const {
        return left == right;
    }

    bool NotEqual(T left, T right) const {
        return left != right;
    }

    bool LessOrEqual(T left, T right) const {
        return left <= right;
    }

    bool Less(T left, T right) const {
        return left < right;
    }

    bool Greater(T left, T right) const {
        return left > right;
    }

    bool GreaterOrEqual(T left, T right) const {
        return left >= right;
    }
};

class TDecimalRangeOperations {
public:
    static constexpr bool UseCustomList = true;

#ifndef MKQL_DISABLE_CODEGEN
    static Type* GetCodegenType(LLVMContext& context) {
        static_assert(sizeof(TDecimalRangeOperations) == sizeof(ui8));
        return Type::getInt8Ty(context);
    }

    static Value* GetCodegenValue(Value* value, LLVMContext&, BasicBlock* block) {
        return GetterForInt128(value, block);
    }

    static Value* GetCodegenStep(Value* value, LLVMContext&, BasicBlock* block) {
        return GetterForInt128(value, block);
    }
#endif

    explicit TDecimalRangeOperations(ui8 precision)
        : Precision_(precision)
    {
    }

    static NYql::NDecimal::TInt128 GetValue(const NUdf::TUnboxedValuePod& value) {
        return value.GetInt128();
    }

    static NYql::NDecimal::TInt128 GetStep(const NUdf::TUnboxedValuePod& value) {
        return value.GetInt128();
    }

    NYql::NDecimal::TInt128 Add(NYql::NDecimal::TInt128 start, NYql::NDecimal::TInt128 step, ui64 index) const {
        return NYql::NDecimal::Add(
            start, NYql::NDecimal::Mul(step, NYql::NDecimal::TInt128(index)), Precision_);
    }

    NYql::NDecimal::TInt128 Sub(NYql::NDecimal::TInt128 left, NYql::NDecimal::TInt128 right) const {
        return NYql::NDecimal::Sub(left, right, Precision_);
    }

    bool IsNormal(NYql::NDecimal::TInt128 value) const {
        return NYql::NDecimal::IsNormal(value);
    }

    bool Equal(NYql::NDecimal::TInt128 left, NYql::NDecimal::TInt128 right) const {
        return NYql::NDecimal::IsEqual(left, right, 0);
    }

    bool NotEqual(NYql::NDecimal::TInt128 left, NYql::NDecimal::TInt128 right) const {
        return NYql::NDecimal::IsNotEqual(left, right, 0);
    }

    bool LessOrEqual(NYql::NDecimal::TInt128 left, NYql::NDecimal::TInt128 right) const {
        return NYql::NDecimal::IsLessOrEqual(left, right, 0);
    }

    bool Less(NYql::NDecimal::TInt128 left, NYql::NDecimal::TInt128 right) const {
        return NYql::NDecimal::IsLess(left, right, 0);
    }

    bool Greater(NYql::NDecimal::TInt128 left, NYql::NDecimal::TInt128 right) const {
        return NYql::NDecimal::IsGreater(left, right, 0);
    }

    bool GreaterOrEqual(NYql::NDecimal::TInt128 left, NYql::NDecimal::TInt128 right) const {
        return NYql::NDecimal::IsGreaterOrEqual(left, right, 0);
    }

private:
    const ui8 Precision_;
};

template <typename T, typename TStep = std::make_signed_t<T>, std::conditional_t<IsNonInteger<TStep>, i8, TStep> TConstFactor = 1, bool TzDate = false, typename TOperations = TDefaultRangeOperations<T, TStep>>
class TListFromRangeWrapper: public TMutableCodegeneratorNode<TListFromRangeWrapper<T, TStep, TConstFactor, TzDate, TOperations>> {
private:
    using TBaseComputation = TMutableCodegeneratorNode<TListFromRangeWrapper<T, TStep, TConstFactor, TzDate, TOperations>>;

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

    class TCustomValue: public TCustomListValue {
        static constexpr ui64 MaxElementsCount = std::numeric_limits<ui64>::max();

    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, T start, T end, TStep step, const TOperations& operations)
                : TComputationValue<TIterator>(memInfo)
                , Start_(start)
                , End_(end)
                , Step_(step)
                , Operations_(operations)
                , ValidInput_(CheckInput(start, end, step, Operations_))
                , Index_(0ULL)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                if (!ValidInput_ || Index_ >= MaxElementsCount) {
                    return false;
                }
                const T current = Operations_.Add(Start_, Step_, Index_);
                const bool outOfRange = Operations_.Greater(Step_, TStep(0)) ? Operations_.GreaterOrEqual(current, End_) : Operations_.LessOrEqual(current, End_);
                if (outOfRange) {
                    return false;
                }
                value = NUdf::TUnboxedValuePod(current);
                Index_++;
                return true;
            }

            static bool CheckInput(T start, T end, TStep step, const TOperations& operations) {
                return operations.NotEqual(step, TStep(0)) &&
                       operations.IsNormal(step) && operations.IsNormal(start) && operations.IsNormal(end) &&
                       operations.NotEqual(start, end);
            }

            const T Start_;
            const T End_;
            const TStep Step_;
            const TOperations& Operations_;
            const bool ValidInput_;
            ui64 Index_;
        };

        TCustomValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, T start, T end, TStep step, const TOperations& operations)
            : TCustomListValue(memInfo)
            , Ctx_(ctx)
            , Start_(start)
            , End_(end)
            , Step_(step)
            , Operations_(operations)
        {
        }

    protected:
        NUdf::TUnboxedValue GetListIterator() const override {
            return Ctx_.HolderFactory.template Create<TIterator>(Start_, End_, Step_, Operations_);
        }

        TComputationContext& Ctx_;
        const T Start_;
        const T End_;
        const TStep Step_;
        const TOperations& Operations_;
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
    TListFromRangeWrapper(TComputationMutables& mutables, IComputationNode* start, IComputationNode* end, IComputationNode* step, TOperations operations)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Start_(start)
        , End_(end)
        , Step_(step)
        , Operations_(std::move(operations))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto start = Start_->GetValue(ctx);
        const auto end = End_->GetValue(ctx);
        auto step = Operations_.GetStep(Step_->GetValue(ctx));
        if constexpr (TConstFactor > 1) {
            if (step % TConstFactor) {
                step = 0;
            } else {
                step /= TConstFactor;
            }
        }

        if constexpr (TzDate) {
            return MakeList(Operations_.GetValue(start), Operations_.GetValue(end), step, start.GetTimezoneId(), &Operations_, ctx);
        } else {
            return MakeList(Operations_.GetValue(start), Operations_.GetValue(end), step, 0U, &Operations_, ctx);
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);

        const auto startv = GetNodeValue(Start_, ctx, block);
        const auto endv = GetNodeValue(End_, ctx, block);
        const auto stepv = GetNodeValue(Step_, ctx, block);

        const auto start = TOperations::GetCodegenValue(startv, context, block);
        const auto end = TOperations::GetCodegenValue(endv, context, block);

        auto step = TOperations::GetCodegenStep(stepv, context, block);
        if constexpr (TConstFactor > 1) {
            const auto zero = ConstantInt::get(GetTypeFor<TStep>(context), 0);
            const auto fact = ConstantInt::get(GetTypeFor<TStep>(context), TConstFactor);
            const auto div = BinaryOperator::CreateSDiv(step, fact, "div", block);
            const auto rem = BinaryOperator::CreateSRem(step, fact, "rem", block);
            const auto bad = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, rem, zero, "bad", block);
            step = SelectInst::Create(bad, zero, div, "step", block);
        }

        const auto timezone = TzDate ? GetterForTimezone(context, startv, block) : ConstantInt::get(Type::getInt16Ty(context), 0);
        const auto operations = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(&Operations_)), PointerType::getUnqual(TOperations::GetCodegenType(context)), "operations", block);

        return EmitFunctionCall<&TListFromRangeWrapper::MakeList>(valueType, {start, end, step, timezone, operations, ctx.Ctx}, ctx, block);
    }
#endif
private:
    static NUdf::TUnboxedValuePod MakeList(T start, T end, TStep step, ui16 timezoneId, const TOperations* operations, TComputationContext& ctx) {
        if constexpr (TzDate) {
            return ctx.HolderFactory.Create<TTzValue>(ctx, start, end, step, timezoneId);
        } else if constexpr (TOperations::UseCustomList) {
            return ctx.HolderFactory.Create<TCustomValue>(ctx, start, end, step, *operations);
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
    const TOperations Operations_;
};

} // namespace

IComputationNode* WrapListFromRange(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");

    const auto start = LocateNode(ctx.NodeLocator, callable, 0);
    const auto end = LocateNode(ctx.NodeLocator, callable, 1);
    const auto step = LocateNode(ctx.NodeLocator, callable, 2);
    switch (*AS_TYPE(TDataType, callable.GetInput(0).GetStaticType())->GetDataSlot()) {
        case NUdf::EDataSlot::Uint8:
            return new TListFromRangeWrapper<ui8>(ctx.Mutables, start, end, step, TDefaultRangeOperations<ui8, i8>{});
        case NUdf::EDataSlot::Int8:
            return new TListFromRangeWrapper<i8>(ctx.Mutables, start, end, step, TDefaultRangeOperations<i8, i8>{});
        case NUdf::EDataSlot::Uint16:
            return new TListFromRangeWrapper<ui16>(ctx.Mutables, start, end, step, TDefaultRangeOperations<ui16, i16>{});
        case NUdf::EDataSlot::Int16:
            return new TListFromRangeWrapper<i16>(ctx.Mutables, start, end, step, TDefaultRangeOperations<i16, i16>{});
        case NUdf::EDataSlot::Uint32:
            return new TListFromRangeWrapper<ui32>(ctx.Mutables, start, end, step, TDefaultRangeOperations<ui32, i32>{});
        case NUdf::EDataSlot::Int32:
            return new TListFromRangeWrapper<i32>(ctx.Mutables, start, end, step, TDefaultRangeOperations<i32, i32>{});
        case NUdf::EDataSlot::Uint64:
            return new TListFromRangeWrapper<ui64>(ctx.Mutables, start, end, step, TDefaultRangeOperations<ui64, i64>{});
        case NUdf::EDataSlot::Int64:
            return new TListFromRangeWrapper<i64>(ctx.Mutables, start, end, step, TDefaultRangeOperations<i64, i64>{});
        case NUdf::EDataSlot::Float:
            return new TListFromRangeWrapper<float, float, 1, false, TFloatingRangeOperations<float>>(ctx.Mutables, start, end, step, TFloatingRangeOperations<float>{});
        case NUdf::EDataSlot::Double:
            return new TListFromRangeWrapper<double, double, 1, false, TFloatingRangeOperations<double>>(ctx.Mutables, start, end, step, TFloatingRangeOperations<double>{});
        case NUdf::EDataSlot::Date:
            return new TListFromRangeWrapper<ui16, i64, 86400000000LL>(ctx.Mutables, start, end, step, TDefaultRangeOperations<ui16, i64>{});
        case NUdf::EDataSlot::Date32:
            return new TListFromRangeWrapper<i32, i64, 86400000000LL>(ctx.Mutables, start, end, step, TDefaultRangeOperations<i32, i64>{});
        case NUdf::EDataSlot::TzDate:
            return new TListFromRangeWrapper<ui16, i64, 86400000000LL, true>(ctx.Mutables, start, end, step, TDefaultRangeOperations<ui16, i64>{});
        case NUdf::EDataSlot::Datetime:
            return new TListFromRangeWrapper<ui32, i64, 1000000>(ctx.Mutables, start, end, step, TDefaultRangeOperations<ui32, i64>{});
        case NUdf::EDataSlot::Datetime64:
            return new TListFromRangeWrapper<i64, i64, 1000000>(ctx.Mutables, start, end, step, TDefaultRangeOperations<i64, i64>{});
        case NUdf::EDataSlot::TzDatetime:
            return new TListFromRangeWrapper<ui32, i64, 1000000, true>(ctx.Mutables, start, end, step, TDefaultRangeOperations<ui32, i64>{});
        case NUdf::EDataSlot::Timestamp:
            return new TListFromRangeWrapper<ui64, i64, 1>(ctx.Mutables, start, end, step, TDefaultRangeOperations<ui64, i64>{});
        case NUdf::EDataSlot::Timestamp64:
            return new TListFromRangeWrapper<i64, i64, 1>(ctx.Mutables, start, end, step, TDefaultRangeOperations<i64, i64>{});
        case NUdf::EDataSlot::TzTimestamp:
            return new TListFromRangeWrapper<ui64, i64, 1, true>(ctx.Mutables, start, end, step, TDefaultRangeOperations<ui64, i64>{});
        case NUdf::EDataSlot::Interval:
        case NUdf::EDataSlot::Interval64:
            return new TListFromRangeWrapper<i64, i64, 1>(ctx.Mutables, start, end, step, TDefaultRangeOperations<i64, i64>{});
        case NUdf::EDataSlot::Decimal: {
            const auto startType = static_cast<TDataDecimalType*>(callable.GetInput(0).GetStaticType());
            const auto endType = static_cast<TDataDecimalType*>(callable.GetInput(1).GetStaticType());
            const auto stepType = static_cast<TDataDecimalType*>(callable.GetInput(2).GetStaticType());
            MKQL_ENSURE(startType->GetParams() == endType->GetParams() &&
                            startType->GetParams() == stepType->GetParams(),
                        "ListFromRange expects Decimal Start, End, and Step to have the same precision and scale");
            const ui8 precision = startType->GetParams().first;
            using TWrapper = TListFromRangeWrapper<NYql::NDecimal::TInt128, NYql::NDecimal::TInt128, 1, false, TDecimalRangeOperations>;
            return new TWrapper(ctx.Mutables, start, end, step, TDecimalRangeOperations(precision));
        }
        default:
            MKQL_ENSURE(false, "unexpected");
    }
}

} // namespace NKikimr::NMiniKQL
