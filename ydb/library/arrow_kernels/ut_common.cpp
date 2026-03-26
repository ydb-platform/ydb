#include "ut_common.h"
#include "operations.h"

#include <util/system/yassert.h>

namespace cp = ::arrow20::compute;


namespace NKikimr::NKernels {

template <typename TType>
struct TTypeWrapper
{
    using T = TType;
};

template <typename TFunc, bool EnableNull = false>
bool SwitchType(arrow20::Type::type typeId, TFunc&& f) {
    switch (typeId) {
        case arrow20::Type::NA: {
            if constexpr (EnableNull) {
                return f(TTypeWrapper<arrow20::NullType>());
            }
            break;
        }
        case arrow20::Type::BOOL:
            return f(TTypeWrapper<arrow20::BooleanType>());
        case arrow20::Type::UINT8:
            return f(TTypeWrapper<arrow20::UInt8Type>());
        case arrow20::Type::INT8:
            return f(TTypeWrapper<arrow20::Int8Type>());
        case arrow20::Type::UINT16:
            return f(TTypeWrapper<arrow20::UInt16Type>());
        case arrow20::Type::INT16:
            return f(TTypeWrapper<arrow20::Int16Type>());
        case arrow20::Type::UINT32:
            return f(TTypeWrapper<arrow20::UInt32Type>());
        case arrow20::Type::INT32:
            return f(TTypeWrapper<arrow20::Int32Type>());
        case arrow20::Type::UINT64:
            return f(TTypeWrapper<arrow20::UInt64Type>());
        case arrow20::Type::INT64:
            return f(TTypeWrapper<arrow20::Int64Type>());
        case arrow20::Type::HALF_FLOAT:
            return f(TTypeWrapper<arrow20::HalfFloatType>());
        case arrow20::Type::FLOAT:
            return f(TTypeWrapper<arrow20::FloatType>());
        case arrow20::Type::DOUBLE:
            return f(TTypeWrapper<arrow20::DoubleType>());
        case arrow20::Type::STRING:
            return f(TTypeWrapper<arrow20::StringType>());
        case arrow20::Type::BINARY:
            return f(TTypeWrapper<arrow20::BinaryType>());
        case arrow20::Type::FIXED_SIZE_BINARY:
            return f(TTypeWrapper<arrow20::FixedSizeBinaryType>());
        case arrow20::Type::DATE32:
            return f(TTypeWrapper<arrow20::Date32Type>());
        case arrow20::Type::DATE64:
            return f(TTypeWrapper<arrow20::Date64Type>());
        case arrow20::Type::TIMESTAMP:
            return f(TTypeWrapper<arrow20::TimestampType>());
        case arrow20::Type::TIME32:
            return f(TTypeWrapper<arrow20::Time32Type>());
        case arrow20::Type::TIME64:
            return f(TTypeWrapper<arrow20::Time64Type>());
        case arrow20::Type::INTERVAL_MONTHS:
            return f(TTypeWrapper<arrow20::MonthIntervalType>());
        case arrow20::Type::DECIMAL:
            return f(TTypeWrapper<arrow20::Decimal128Type>());
        case arrow20::Type::DURATION:
            return f(TTypeWrapper<arrow20::DurationType>());
        case arrow20::Type::LARGE_STRING:
            return f(TTypeWrapper<arrow20::LargeStringType>());
        case arrow20::Type::LARGE_BINARY:
            return f(TTypeWrapper<arrow20::LargeBinaryType>());
        case arrow20::Type::DECIMAL256:
        case arrow20::Type::DENSE_UNION:
        case arrow20::Type::DICTIONARY:
        case arrow20::Type::EXTENSION:
        case arrow20::Type::FIXED_SIZE_LIST:
        case arrow20::Type::INTERVAL_DAY_TIME:
        case arrow20::Type::LARGE_LIST:
        case arrow20::Type::LIST:
        case arrow20::Type::MAP:
        case arrow20::Type::MAX_ID:
        case arrow20::Type::SPARSE_UNION:
        case arrow20::Type::STRUCT:
            break;
    }

    return false;
}

std::shared_ptr<arrow20::Array> NumVecToArray(const std::shared_ptr<arrow20::DataType>& type,
                                            const std::vector<double>& vec,
                                            std::optional<double> nullValue) {
    std::shared_ptr<arrow20::Array> out;
    SwitchType(type->id(), [&](const auto& t) {
        using TWrap = std::decay_t<decltype(t)>;
        using T = typename TWrap::T;

        if constexpr (arrow20::is_number_type<T>::value) {
            typename arrow20::TypeTraits<T>::BuilderType builder;
            for (const auto val : vec) {
                if (nullValue && *nullValue == val) {
                    Y_ABORT_UNLESS(builder.AppendNull().ok());
                } else {
                    Y_ABORT_UNLESS(builder.Append(static_cast<typename T::c_type>(val)).ok());
                }
            }
            Y_ABORT_UNLESS(builder.Finish(&out).ok());
            return true;
        } else if constexpr (arrow20::is_timestamp_type<T>()) {
            typename arrow20::TypeTraits<T>::BuilderType builder(type, arrow20::default_memory_pool());
            for (const auto val : vec) {
                Y_ABORT_UNLESS(builder.Append(static_cast<typename T::c_type>(val)).ok());
            }
            Y_ABORT_UNLESS(builder.Finish(&out).ok());
            return true;
        }
        return false;
    });
    return out;
}

static void RegisterMath(cp::FunctionRegistry* registry) {
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TAcosh>(TAcosh::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TAtanh>(TAtanh::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TCbrt>(TCbrt::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TCosh>(TCosh::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeConstNullary<TE>(TE::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TErf>(TErf::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TErfc>(TErfc::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TExp>(TExp::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TExp2>(TExp2::Name)).ok());
    // Temporarily disabled because of compilation error on Windows.
#if 0
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TExp10>(TExp10::Name)).ok());
#endif
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathBinary<THypot>(THypot::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TLgamma>(TLgamma::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeConstNullary<TPi>(TPi::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TSinh>(TSinh::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TSqrt>(TSqrt::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TTgamma>(TTgamma::Name)).ok());
}

static void RegisterRound(cp::FunctionRegistry* registry) {
    Y_ABORT_UNLESS(registry->AddFunction(MakeArithmeticUnary<TRound>(TRound::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeArithmeticUnary<TRoundBankers>(TRoundBankers::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeArithmeticUnary<TRoundToExp2>(TRoundToExp2::Name)).ok());
}

static void RegisterArithmetic(cp::FunctionRegistry* registry) {
    Y_ABORT_UNLESS(registry->AddFunction(MakeArithmeticIntBinary<TGreatestCommonDivisor>(TGreatestCommonDivisor::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeArithmeticIntBinary<TLeastCommonMultiple>(TLeastCommonMultiple::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeArithmeticBinary<TModulo>(TModulo::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeArithmeticBinary<TModuloOrZero>(TModuloOrZero::Name)).ok());
}

static std::unique_ptr<cp::FunctionRegistry> CreateCustomRegistry() {
    auto registry = cp::FunctionRegistry::Make();
    RegisterMath(registry.get());
    RegisterRound(registry.get());
    RegisterArithmetic(registry.get());
    cp::internal::RegisterScalarCast(registry.get());
    return registry;
}

// Creates singleton custom registry
cp::FunctionRegistry* GetCustomFunctionRegistry() {
    static auto g_registry = CreateCustomRegistry();
    return g_registry.get();
}

// We want to have ExecContext per thread. All these context use one custom registry.
cp::ExecContext* GetCustomExecContext() {
    static thread_local cp::ExecContext context(arrow20::default_memory_pool(), nullptr, GetCustomFunctionRegistry());
    return &context;
}

}
