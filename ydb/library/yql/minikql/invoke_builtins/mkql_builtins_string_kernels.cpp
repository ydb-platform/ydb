#include "mkql_builtins_string_kernels.h"
#include "mkql_builtins_impl.h"  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename Return, typename... Args>
constexpr auto GetArgumentsCount(Return(*)(Args...)) noexcept
{
    return sizeof...(Args);
}

template<typename TInput1, typename TInput2, typename TOutput, class TOp>
struct TBinaryStringExecs : TBinaryKernelExecsBase<TBinaryStringExecs<TInput1, TInput2, TOutput, TOp>>
{
    using TOffset1 = typename TPrimitiveDataType<TInput1>::TResult::offset_type;
    using TOffset2 = typename TPrimitiveDataType<TInput2>::TResult::offset_type;

    static arrow::Status ExecScalarScalar(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        if (!arg1.scalar()->is_valid || !arg2.scalar()->is_valid) {
            *res = arrow::MakeNullScalar(GetPrimitiveDataType<TOutput>());
        } else {
            const auto val1 = GetStringScalarValue(*arg1.scalar());
            const auto val2 = GetStringScalarValue(*arg2.scalar());
            *res = MakeScalarDatum<TOutput>(TOp::Do(val1, val2));
        }

        return arrow::Status::OK();
    }

    static arrow::Status ExecScalarArray(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        static_assert(!std::is_same<TOutput, bool>::value);
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        auto& resArr = *res->array();
        if (arg1.scalar()->is_valid) {
            const auto val1 = GetStringScalarValue(*arg1.scalar());
            const auto& arr2 = *arg2.array();
            auto length = arr2.length;
            auto resValues = resArr.GetMutableValues<TOutput>(1);

            if (val1.empty()) {
                if constexpr (GetArgumentsCount(TOp::DoWithEmptyLeft) == 0) {
                    std::fill(resValues, resValues + length, TOp::DoWithEmptyLeft());
                } else {
                    const TOffset2* offsets2 = arr2.GetValues<TOffset2>(1);
                    for (int64_t i = 0; i < length; ++i) {
                        resValues[i] = TOp::DoWithEmptyLeft(offsets2[i + 1] - offsets2[i]);
                    }
                }
            } else {
                const TOffset2* offsets2 = arr2.GetValues<TOffset2>(1);
                const char* data2 = arr2.GetValues<char>(2, 0);
                for (int64_t i = 0; i < length; ++i) {
                    std::string_view val2(data2 + offsets2[i], offsets2[i + 1] - offsets2[i]);
                    resValues[i] = TOp::Do(val1, val2);
                }
            }
        }

        return arrow::Status::OK();
    }

    static arrow::Status ExecArrayScalar(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        static_assert(!std::is_same<TOutput, bool>::value);
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        auto& resArr = *res->array();
        if (arg2.scalar()->is_valid) {
            const auto val2 = GetStringScalarValue(*arg2.scalar());
            const auto& arr1 = *arg1.array();
            auto length = arr1.length;
            auto resValues = resArr.GetMutableValues<TOutput>(1);

            if (val2.empty()) {
                if constexpr (GetArgumentsCount(TOp::DoWithEmptyRight) == 0) {
                    std::fill(resValues, resValues + length, TOp::DoWithEmptyRight());
                } else {
                    const TOffset1* offsets1 = arr1.GetValues<TOffset1>(1);
                    for (int64_t i = 0; i < length; ++i) {
                        resValues[i] = TOp::DoWithEmptyRight(offsets1[i + 1] - offsets1[i]);
                    }
                }
            } else {
                const TOffset2* offsets1 = arr1.GetValues<TOffset1>(1);
                const char* data1 = arr1.GetValues<char>(2, 0);
                for (int64_t i = 0; i < length; ++i) {
                    std::string_view val1(data1 + offsets1[i], offsets1[i + 1] - offsets1[i]);
                    resValues[i] = TOp::Do(val1, val2);
                }
            }
        }
        return arrow::Status::OK();
    }

    static arrow::Status ExecArrayArray(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        static_assert(!std::is_same<TOutput, bool>::value);
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        const auto& arr1 = *arg1.array();
        const auto& arr2 = *arg2.array();
        auto& resArr = *res->array();
        MKQL_ENSURE(arr1.length == arr2.length, "Expected same length");
        auto length = arr1.length;
        auto resValues = resArr.GetMutableValues<TOutput>(1);

        const TOffset1* offsets1 = arr1.GetValues<TOffset1>(1);
        const TOffset2* offsets2 = arr2.GetValues<TOffset2>(1);
        const char* data1 = arr1.GetValues<char>(2, 0);
        const char* data2 = arr2.GetValues<char>(2, 0);
        for (int64_t i = 0; i < length; ++i) {
            std::string_view val1(data1 + offsets1[i], offsets1[i + 1] - offsets1[i]);
            std::string_view val2(data2 + offsets2[i], offsets2[i + 1] - offsets2[i]);
            resValues[i] = TOp::Do(val1, val2);
        }

        return arrow::Status::OK();
    }
};

template<typename TInput, typename TOutput, class TOp>
struct TUnaryStringExecs : TUnaryKernelExecsBase<TUnaryStringExecs<TInput, TOutput, TOp>>
{
    using TOffset = typename TPrimitiveDataType<TInput>::TResult::offset_type;

    static arrow::Status ExecScalar(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        const auto& arg = batch.values[0];
        if (!arg.scalar()->is_valid) {
            *res = arrow::MakeNullScalar(GetPrimitiveDataType<TOutput>());
        } else {
            const auto val = GetStringScalarValue(*arg.scalar());
            *res = MakeScalarDatum<TOutput>(TOp::Do(val));
        }
        return arrow::Status::OK();
    }

    static arrow::Status ExecArray(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        static_assert(!std::is_same<TOutput, bool>::value);

        const auto& arg = batch.values[0];
        auto& resArr = *res->array();

        const auto& arr = *arg.array();
        auto length = arr.length;
        auto resValues = resArr.GetMutableValues<TOutput>(1);

        const TOffset* offsets = arr.GetValues<TOffset>(1);
        const char* data = arr.GetValues<char>(2, 0);
        for (int64_t i = 0; i < length; ++i) {
            std::string_view val(data + offsets[i], offsets[i + 1] - offsets[i]);
            resValues[i] = TOp::Do(val);
        }
        return arrow::Status::OK();
    }
};

// -------------------------------------------------------------------------------------
// String comparison
// -------------------------------------------------------------------------------------

struct TStrEqualsOp {
    static inline bool Do(std::string_view left, std::string_view right) {
        return left == right;
    }

    static inline bool DoWithEmptyLeft(size_t rightLen) {
        return rightLen == 0;
    }

    static inline bool DoWithEmptyRight(size_t leftLen) {
        return leftLen == 0;
    }
};

struct TStrNotEqualsOp {
    static inline bool Do(std::string_view left, std::string_view right) {
        return left != right;
    }

    static inline bool DoWithEmptyLeft(size_t rightLen) {
        return rightLen != 0;
    }

    static inline bool DoWithEmptyRight(size_t leftLen) {
        return leftLen != 0;
    }
};

struct TStrLessOp {
    static inline bool Do(std::string_view left, std::string_view right) {
        return left < right;
    }

    static inline bool DoWithEmptyLeft(size_t rightLen) {
        return rightLen != 0;
    }

    static constexpr bool DoWithEmptyRight() {
        return false;
    }
};

struct TStrLessOrEqualOp {
    static inline bool Do(std::string_view left, std::string_view right) {
        return left <= right;
    }

    static constexpr bool DoWithEmptyLeft() {
        return true;
    }

    static inline bool DoWithEmptyRight(size_t leftLen) {
        return leftLen == 0;
    }
};

struct TStrGreaterOp {
    static inline bool Do(std::string_view left, std::string_view right) {
        return left > right;
    }

    static constexpr bool DoWithEmptyLeft() {
        return false;
    }

    static inline bool DoWithEmptyRight(size_t leftLen) {
        return leftLen != 0;
    }
};

struct TStrGreaterOrEqualOp {
    static inline bool Do(std::string_view left, std::string_view right) {
        return left >= right;
    }

    static inline bool DoWithEmptyLeft(size_t rightLen) {
        return rightLen == 0;
    }

    static constexpr bool DoWithEmptyRight() {
        return true;
    }
};

struct TStrStartsWithOp {
    static inline bool Do(std::string_view left, std::string_view right) {
        return left.starts_with(right);
    }

    static inline bool DoWithEmptyLeft(size_t rightLen) {
        return rightLen == 0;
    }

    static constexpr bool DoWithEmptyRight() {
        return true;
    }
};

struct TStrEndsWithOp {
    static inline bool Do(std::string_view left, std::string_view right) {
        return left.ends_with(right);
    }

    static inline bool DoWithEmptyLeft(size_t rightLen) {
        return rightLen == 0;
    }

    static constexpr bool DoWithEmptyRight() {
        return true;
    }
};

struct TStrContainsOp {
    static inline bool Do(std::string_view left, std::string_view right) {
        return left.contains(right);
    }

    static inline bool DoWithEmptyLeft(size_t rightLen) {
        return rightLen == 0;
    }

    static constexpr bool DoWithEmptyRight() {
        return true;
    }
};

Y_NO_INLINE void AddCompareStringKernelImpl(TKernelFamilyBase& kernelFamily, NUdf::TDataTypeId type1, NUdf::TDataTypeId type2,
    const arrow::compute::ArrayKernelExec& exec, arrow::compute::InputType&& inputType1, arrow::compute::InputType&& inputType2,
    arrow::compute::OutputType&& outputType) {
    std::vector<NUdf::TDataTypeId> argTypes({ type1, type2 });
    NUdf::TDataTypeId returnType = NUdf::TDataType<bool>::Id;

    auto k = std::make_unique<arrow::compute::ScalarKernel>(std::vector<arrow::compute::InputType>{
        inputType1, inputType2
    }, outputType, exec);
    k->null_handling = arrow::compute::NullHandling::INTERSECTION;
    kernelFamily.Adopt(argTypes, returnType, std::make_unique<TPlainKernel>(kernelFamily, argTypes, returnType, std::move(k), TKernel::ENullMode::Default));
}

template<typename TInput1, typename TInput2, typename TOp>
void AddCompareStringKernel(TKernelFamilyBase& kernelFamily) {
    // ui8 type is used as bool replacement
    using TOutput = ui8;
    using TExecs = TBinaryStringExecs<TInput1, TInput2, TOutput, TOp>;
    AddCompareStringKernelImpl(kernelFamily, NUdf::TDataType<TInput1>::Id, NUdf::TDataType<TInput2>::Id, &TExecs::Exec,
        GetPrimitiveInputArrowType<TInput1>(), GetPrimitiveInputArrowType<TInput2>(), GetPrimitiveOutputArrowType<TOutput>()
    );
}

template<typename TOp>
void AddCompareStringKernels(TKernelFamilyBase& kernelFamily) {
    AddCompareStringKernel<char*,       char*,       TOp>(kernelFamily);
    AddCompareStringKernel<char*,       NUdf::TUtf8, TOp>(kernelFamily);
    AddCompareStringKernel<NUdf::TUtf8, char*,       TOp>(kernelFamily);
    AddCompareStringKernel<NUdf::TUtf8, NUdf::TUtf8, TOp>(kernelFamily);
}


// -------------------------------------------------------------------------------------
// String size
// -------------------------------------------------------------------------------------
template<typename TOutput>
struct TStrSizeOp {
    static inline TOutput Do(std::string_view input) {
        return static_cast<TOutput>(input.size());
    }
};


Y_NO_INLINE void AddSizeStringKernelImpl(TKernelFamilyBase& kernelFamily, NUdf::TDataTypeId type1, NUdf::TDataTypeId returnType,
    const arrow::compute::ArrayKernelExec& exec, arrow::compute::InputType&& inputType1, arrow::compute::OutputType&& outputType) {
    std::vector<NUdf::TDataTypeId> argTypes({ type1 });

    auto k = std::make_unique<arrow::compute::ScalarKernel>(std::vector<arrow::compute::InputType>{
         inputType1
    }, outputType, exec);
    k->null_handling = arrow::compute::NullHandling::INTERSECTION;
    kernelFamily.Adopt(argTypes, returnType, std::make_unique<TPlainKernel>(kernelFamily, argTypes, returnType, std::move(k), TKernel::ENullMode::Default));
}

template<typename TInput>
void AddSizeStringKernel(TKernelFamilyBase& kernelFamily) {
    using TOutput = ui32;
    using TOp = TStrSizeOp<TOutput>;
    using TExecs = TUnaryStringExecs<TInput, TOutput, TOp>;
    AddSizeStringKernelImpl(kernelFamily, NUdf::TDataType<TInput>::Id, NUdf::TDataType<TOutput>::Id, &TExecs::Exec,
        GetPrimitiveInputArrowType<TInput>(), GetPrimitiveOutputArrowType<TOutput>());
}

} // namespace

void RegisterStringKernelEquals(TKernelFamilyBase& kernelFamily) {
    AddCompareStringKernels<TStrEqualsOp>(kernelFamily);
}

void RegisterStringKernelNotEquals(TKernelFamilyBase& kernelFamily) {
    AddCompareStringKernels<TStrNotEqualsOp>(kernelFamily);
}

void RegisterStringKernelLess(TKernelFamilyBase& kernelFamily) {
    AddCompareStringKernels<TStrLessOp>(kernelFamily);
}

void RegisterStringKernelLessOrEqual(TKernelFamilyBase& kernelFamily) {
    AddCompareStringKernels<TStrLessOrEqualOp>(kernelFamily);
}

void RegisterStringKernelGreater(TKernelFamilyBase& kernelFamily) {
    AddCompareStringKernels<TStrGreaterOp>(kernelFamily);
}

void RegisterStringKernelGreaterOrEqual(TKernelFamilyBase& kernelFamily) {
    AddCompareStringKernels<TStrGreaterOrEqualOp>(kernelFamily);
}

void RegisterStringKernelSize(TKernelFamilyBase& kernelFamily) {
    AddSizeStringKernel<char*>(kernelFamily);
    AddSizeStringKernel<NUdf::TUtf8>(kernelFamily);
}

void RegisterStringKernelStartsWith(TKernelFamilyBase& kernelFamily) {
    AddCompareStringKernels<TStrStartsWithOp>(kernelFamily);
}

void RegisterStringKernelEndsWith(TKernelFamilyBase& kernelFamily) {
    AddCompareStringKernels<TStrEndsWithOp>(kernelFamily);
}

void RegisterStringKernelContains(TKernelFamilyBase& kernelFamily) {
    AddCompareStringKernels<TStrContainsOp>(kernelFamily);
}

void RegisterSizeBuiltin(TKernelFamilyMap& kernelFamilyMap) {
    auto family = std::make_unique<TKernelFamilyBase>();

    RegisterStringKernelSize(*family);

    kernelFamilyMap["Size"] = std::move(family);
}

void RegisterWith(TKernelFamilyMap& kernelFamilyMap) {
    auto family = std::make_unique<TKernelFamilyBase>();
    RegisterStringKernelStartsWith(*family);
    kernelFamilyMap["StartsWith"] = std::move(family);

    family = std::make_unique<TKernelFamilyBase>();
    RegisterStringKernelEndsWith(*family);
    kernelFamilyMap["EndsWith"] = std::move(family);

    family = std::make_unique<TKernelFamilyBase>();
    RegisterStringKernelContains(*family);
    kernelFamilyMap["StringContains"] = std::move(family);
}

}
}
