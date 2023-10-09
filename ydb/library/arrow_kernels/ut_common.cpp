#include "ut_common.h"
#include "operations.h"

#include <util/system/yassert.h>

namespace cp = ::arrow::compute;


namespace NKikimr::NKernels {

template <typename TType>
struct TTypeWrapper
{
    using T = TType;
};

template <typename TFunc, bool EnableNull = false>
bool SwitchType(arrow::Type::type typeId, TFunc&& f) {
    switch (typeId) {
        case arrow::Type::NA: {
            if constexpr (EnableNull) {
                return f(TTypeWrapper<arrow::NullType>());
            }
            break;
        }
        case arrow::Type::BOOL:
            return f(TTypeWrapper<arrow::BooleanType>());
        case arrow::Type::UINT8:
            return f(TTypeWrapper<arrow::UInt8Type>());
        case arrow::Type::INT8:
            return f(TTypeWrapper<arrow::Int8Type>());
        case arrow::Type::UINT16:
            return f(TTypeWrapper<arrow::UInt16Type>());
        case arrow::Type::INT16:
            return f(TTypeWrapper<arrow::Int16Type>());
        case arrow::Type::UINT32:
            return f(TTypeWrapper<arrow::UInt32Type>());
        case arrow::Type::INT32:
            return f(TTypeWrapper<arrow::Int32Type>());
        case arrow::Type::UINT64:
            return f(TTypeWrapper<arrow::UInt64Type>());
        case arrow::Type::INT64:
            return f(TTypeWrapper<arrow::Int64Type>());
        case arrow::Type::HALF_FLOAT:
            return f(TTypeWrapper<arrow::HalfFloatType>());
        case arrow::Type::FLOAT:
            return f(TTypeWrapper<arrow::FloatType>());
        case arrow::Type::DOUBLE:
            return f(TTypeWrapper<arrow::DoubleType>());
        case arrow::Type::STRING:
            return f(TTypeWrapper<arrow::StringType>());
        case arrow::Type::BINARY:
            return f(TTypeWrapper<arrow::BinaryType>());
        case arrow::Type::FIXED_SIZE_BINARY:
            return f(TTypeWrapper<arrow::FixedSizeBinaryType>());
        case arrow::Type::DATE32:
            return f(TTypeWrapper<arrow::Date32Type>());
        case arrow::Type::DATE64:
            return f(TTypeWrapper<arrow::Date64Type>());
        case arrow::Type::TIMESTAMP:
            return f(TTypeWrapper<arrow::TimestampType>());
        case arrow::Type::TIME32:
            return f(TTypeWrapper<arrow::Time32Type>());
        case arrow::Type::TIME64:
            return f(TTypeWrapper<arrow::Time64Type>());
        case arrow::Type::INTERVAL_MONTHS:
            return f(TTypeWrapper<arrow::MonthIntervalType>());
        case arrow::Type::DECIMAL:
            return f(TTypeWrapper<arrow::Decimal128Type>());
        case arrow::Type::DURATION:
            return f(TTypeWrapper<arrow::DurationType>());
        case arrow::Type::LARGE_STRING:
            return f(TTypeWrapper<arrow::LargeStringType>());
        case arrow::Type::LARGE_BINARY:
            return f(TTypeWrapper<arrow::LargeBinaryType>());
        case arrow::Type::DECIMAL256:
        case arrow::Type::DENSE_UNION:
        case arrow::Type::DICTIONARY:
        case arrow::Type::EXTENSION:
        case arrow::Type::FIXED_SIZE_LIST:
        case arrow::Type::INTERVAL_DAY_TIME:
        case arrow::Type::LARGE_LIST:
        case arrow::Type::LIST:
        case arrow::Type::MAP:
        case arrow::Type::MAX_ID:
        case arrow::Type::SPARSE_UNION:
        case arrow::Type::STRUCT:
            break;
    }

    return false;
}

std::shared_ptr<arrow::Array> NumVecToArray(const std::shared_ptr<arrow::DataType>& type,
                                            const std::vector<double>& vec,
                                            std::optional<double> nullValue) {
    std::shared_ptr<arrow::Array> out;
    SwitchType(type->id(), [&](const auto& t) {
        using TWrap = std::decay_t<decltype(t)>;
        using T = typename TWrap::T;

        if constexpr (arrow::is_number_type<T>::value) {
            typename arrow::TypeTraits<T>::BuilderType builder;
            for (const auto val : vec) {
                if (nullValue && *nullValue == val) {
                    Y_ABORT_UNLESS(builder.AppendNull().ok());
                } else {
                    Y_ABORT_UNLESS(builder.Append(static_cast<typename T::c_type>(val)).ok());
                }
            }
            Y_ABORT_UNLESS(builder.Finish(&out).ok());
            return true;
        } else if constexpr (arrow::is_timestamp_type<T>()) {
            typename arrow::TypeTraits<T>::BuilderType builder(type, arrow::default_memory_pool());
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
    static thread_local cp::ExecContext context(arrow::default_memory_pool(), nullptr, GetCustomFunctionRegistry());
    return &context;
}

}
