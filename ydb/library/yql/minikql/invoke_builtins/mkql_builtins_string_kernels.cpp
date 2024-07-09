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

using TUntypedStringBinaryScalarFuncPtr = void(*)(std::string_view, std::string_view, void*);
using TUntypedStringBinaryArrayFuncPtr = void(*)(const void* stringOffsets1, const void* data1, const void* stringOffsets2, const void* data2, void* resPtr, int64_t length, int64_t offset1, int64_t offset2);

Y_NO_INLINE arrow::Status ExecStringScalarScalarImpl(const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TPrimitiveDataTypeGetter typeGetter, TPrimitiveDataScalarGetter scalarGetter,
    TUntypedStringBinaryScalarFuncPtr func) {
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    if (!arg1.scalar()->is_valid || !arg2.scalar()->is_valid) {
        *res = arrow::MakeNullScalar(typeGetter());
    } else {
        auto resDatum = scalarGetter();
        const auto resPtr = GetPrimitiveScalarValueMutablePtr(*resDatum.scalar());
        const auto val1 = GetStringScalarValue(*arg1.scalar());
        const auto val2 = GetStringScalarValue(*arg2.scalar());
        func(val1, val2, resPtr);
        *res = resDatum.scalar();
    }

    return arrow::Status::OK();
}

Y_NO_INLINE arrow::Status ExecStringScalarArrayImpl(const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TUntypedStringBinaryArrayFuncPtr func) {
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    auto& resArr = *res->array();
    if (arg1.scalar()->is_valid) {
        const auto val1 = GetStringScalarValue(*arg1.scalar());
        const auto& arr2 = *arg2.array();
        auto length = arr2.length;
        auto resPtr = resArr.buffers[1]->mutable_data();
        const size_t val1Size = val1.size();
        const auto offsets2 = arr2.buffers[1]->data();
        const auto data2 = arr2.buffers[2]->data();
        func(&val1Size, val1.data(), offsets2, data2, resPtr, length, 0, arr2.offset);
    }

    return arrow::Status::OK();
}

Y_NO_INLINE arrow::Status ExecStringArrayScalarImpl(const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TUntypedStringBinaryArrayFuncPtr func) {
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    auto& resArr = *res->array();
    if (arg2.scalar()->is_valid) {
        const auto val2 = GetStringScalarValue(*arg2.scalar());
        const auto& arr1 = *arg1.array();
        auto length = arr1.length;
        auto resPtr = resArr.buffers[1]->mutable_data();
        const size_t val2Size = val2.size();
        const auto offsets1 = arr1.buffers[1]->data();
        const auto data1 = arr1.buffers[2]->data();
        func(offsets1, data1, &val2Size, val2.data(), resPtr, length, arr1.offset, 0);
    }

    return arrow::Status::OK();
}

Y_NO_INLINE arrow::Status ExecStringArrayArrayImpl(const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TUntypedStringBinaryArrayFuncPtr func) {
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    const auto& arr1 = *arg1.array();
    const auto& arr2 = *arg2.array();
    auto& resArr = *res->array();
    MKQL_ENSURE(arr1.length == arr2.length, "Expected same length");
    auto length = arr1.length;
    auto resPtr = resArr.buffers[1]->mutable_data();

    const auto offsets1 = arr1.buffers[1]->data();
    const auto offsets2 = arr2.buffers[1]->data();
    const auto data1 = arr1.buffers[2]->data();
    const auto data2 = arr2.buffers[2]->data();
    func(offsets1, data1, offsets2, data2, resPtr, length, arr1.offset, arr2.offset);
    return arrow::Status::OK();
}

Y_NO_INLINE arrow::Status ExecStringBinaryImpl(const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TPrimitiveDataTypeGetter typeGetter, TPrimitiveDataScalarGetter scalarGetter,
    TUntypedStringBinaryScalarFuncPtr scalarScalarFunc,
    TUntypedStringBinaryArrayFuncPtr scalarArrayFunc,
    TUntypedStringBinaryArrayFuncPtr arrayScalarFunc,
    TUntypedStringBinaryArrayFuncPtr arrayArrayFunc) {
    MKQL_ENSURE(batch.values.size() == 2, "Expected 2 args");
    const auto& arg1 = batch.values[0];
    const auto& arg2 = batch.values[1];
    if (arg1.is_scalar()) {
        if (arg2.is_scalar()) {
            return ExecStringScalarScalarImpl(batch, res, typeGetter, scalarGetter, scalarScalarFunc);
        } else {
            return ExecStringScalarArrayImpl(batch, res, scalarArrayFunc);
        }
    } else {
        if (arg2.is_scalar()) {
            return ExecStringArrayScalarImpl(batch, res, arrayScalarFunc);
        } else {
            return ExecStringArrayArrayImpl(batch, res, arrayArrayFunc);
        }
    }
}

template<typename TInput1, typename TInput2, typename TOutput, class TOp>
struct TBinaryStringExecs
{
    using TOffset1 = typename TPrimitiveDataType<TInput1>::TResult::offset_type;
    using TOffset2 = typename TPrimitiveDataType<TInput2>::TResult::offset_type;

    using TTypedStringBinaryScalarFuncPtr = void(*)(std::string_view, std::string_view, TOutput*);
    using TTypedStringBinaryArrayFuncPtr = void(*)(const TOffset1* stringOffsets1, const char* data1,
        const TOffset2* stringOffsets2, const char* data2, TOutput* resPtr, int64_t length, int64_t offset1, int64_t offset2);

    static void ScalarScalarCore(std::string_view arg1, std::string_view arg2, TOutput* resPtr) {
        *resPtr = TOp::Do(arg1, arg2);
    }

    static void ScalarArrayCore(const TOffset1* stringOffsets1, const char* data1,
        const TOffset2* stringOffsets2, const char* data2, TOutput* resPtr, int64_t length, int64_t offset1, int64_t offset2) {
        Y_UNUSED(offset1);
        const auto val1 = std::string_view(data1, *(const size_t*)stringOffsets1);
        stringOffsets2 += offset2;
        if (val1.empty()) {
            if constexpr (GetArgumentsCount(TOp::DoWithEmptyLeft) == 0) {
                std::fill(resPtr, resPtr + length, TOp::DoWithEmptyLeft());
            } else {
                for (int64_t i = 0; i < length; ++i, ++resPtr, ++stringOffsets2) {
                    *resPtr = TOp::DoWithEmptyLeft(stringOffsets2[1] - stringOffsets2[0]);
                }
            }
        } else {
            for (int64_t i = 0; i < length; ++i, ++resPtr, ++stringOffsets2) {
                std::string_view val2(data2 + stringOffsets2[0], stringOffsets2[1] - stringOffsets2[0]);
                *resPtr = TOp::Do(val1, val2);
            }
        }
    }

    static void ArrayScalarCore(const TOffset1* stringOffsets1, const char* data1,
        const TOffset2* stringOffsets2, const char* data2, TOutput* resPtr, int64_t length, int64_t offset1, int64_t offset2) {
        Y_UNUSED(offset2);
        const auto val2 = std::string_view(data2, *(const size_t*)stringOffsets2);
        stringOffsets1 += offset1;
        if (val2.empty()) {
            if constexpr (GetArgumentsCount(TOp::DoWithEmptyRight) == 0) {
                std::fill(resPtr, resPtr + length, TOp::DoWithEmptyRight());
            } else {
                for (int64_t i = 0; i < length; ++i, ++resPtr, ++stringOffsets1) {
                    *resPtr = TOp::DoWithEmptyRight(stringOffsets1[1] - stringOffsets1[0]);
                }
            }
        } else {
            for (int64_t i = 0; i < length; ++i, ++resPtr, ++stringOffsets1) {
                std::string_view val1(data1 + stringOffsets1[0], stringOffsets1[1] - stringOffsets1[0]);
                *resPtr = TOp::Do(val1, val2);
            }
        }
    }

    static void ArrayArrayCore(const TOffset1* stringOffsets1, const char* data1,
        const TOffset2* stringOffsets2, const char* data2, TOutput* resPtr, int64_t length, int64_t offset1, int64_t offset2) {
        stringOffsets1 += offset1;
        stringOffsets2 += offset2;
        for (int64_t i = 0; i < length; ++i, ++stringOffsets1, ++stringOffsets2, ++resPtr) {
            std::string_view val1(data1 + stringOffsets1[0], stringOffsets1[1] - stringOffsets1[0]);
            std::string_view val2(data2 + stringOffsets2[0], stringOffsets2[1] - stringOffsets2[0]);
            *resPtr = TOp::Do(val1, val2);
        }
    }

    static arrow::Status Exec(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        static_assert(!std::is_same<TOutput, bool>::value);
        TTypedStringBinaryScalarFuncPtr scalarScalarFunc = &ScalarScalarCore;
        TTypedStringBinaryArrayFuncPtr scalarArrayFunc = &ScalarArrayCore;
        TTypedStringBinaryArrayFuncPtr arrayScalarFunc = &ArrayScalarCore;
        TTypedStringBinaryArrayFuncPtr arrayArrayFunc = &ArrayArrayCore;
        return ExecStringBinaryImpl(batch, res, &GetPrimitiveDataType<TOutput>,
            &MakeDefaultScalarDatum<TOutput>, 
            (TUntypedStringBinaryScalarFuncPtr)scalarScalarFunc,
            (TUntypedStringBinaryArrayFuncPtr)scalarArrayFunc,
            (TUntypedStringBinaryArrayFuncPtr)arrayScalarFunc,
            (TUntypedStringBinaryArrayFuncPtr)arrayArrayFunc);
    }
};

using TUntypedStringUnaryScalarFuncPtr = void(*)(std::string_view, void*);
using TUntypedStringUnaryArrayFuncPtr = void(*)(const void* stringOffsets, const void* data, void* resPtr, int64_t length, int64_t offset);

Y_NO_INLINE arrow::Status ExecStringScalarImpl(const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TPrimitiveDataTypeGetter typeGetter, TPrimitiveDataScalarGetter scalarGetter, 
    TUntypedStringUnaryScalarFuncPtr func) {
    const auto& arg = batch.values[0];
    if (!arg.scalar()->is_valid) {
        *res = arrow::MakeNullScalar(typeGetter());
    } else {
        auto resDatum = scalarGetter();
        const auto resPtr = GetPrimitiveScalarValueMutablePtr(*resDatum.scalar());
        const auto val = GetStringScalarValue(*arg.scalar());
        func(val, resPtr);
        *res = resDatum.scalar();
    }
    return arrow::Status::OK();
}

Y_NO_INLINE arrow::Status ExecStringArrayImpl(const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TUntypedStringUnaryArrayFuncPtr func) {

    const auto& arg = batch.values[0];
    auto& resArr = *res->array();

    const auto& arr = *arg.array();
    const auto length = arr.length;
    const auto resValues = resArr.buffers[1]->mutable_data();

    const auto offsets = arr.buffers[1]->data();
    const auto data = arr.buffers[2]->data();
    func(offsets, data, resValues, length, arr.offset);
    return arrow::Status::OK();
}

Y_NO_INLINE arrow::Status ExecStringUnaryImpl(const arrow::compute::ExecBatch& batch, arrow::Datum* res,
    TPrimitiveDataTypeGetter typeGetter, TPrimitiveDataScalarGetter scalarGetter, 
    TUntypedStringUnaryScalarFuncPtr scalarFunc,
    TUntypedStringUnaryArrayFuncPtr arrayFunc) {
    MKQL_ENSURE(batch.values.size() == 1, "Expected single argument");
    const auto& arg = batch.values[0];
    if (arg.is_scalar()) {
        return ExecStringScalarImpl(batch, res, typeGetter, scalarGetter, scalarFunc);
    } else {
        return ExecStringArrayImpl(batch, res, arrayFunc);
    }
}

template<typename TInput, typename TOutput, class TOp>
struct TUnaryStringExecs
{
    using TOffset = typename TPrimitiveDataType<TInput>::TResult::offset_type;

    using TTypedStringUnaryScalarFuncPtr = void(*)(std::string_view, TOutput* resPtr);
    using TTypedStringUnaryArrayFuncPtr = void(*)(const TOffset* offsets, const char* data, TOutput* resPtr, int64_t length, int64_t offset);

    static void ScalarCore(std::string_view arg, TOutput* resPtr) {
        *resPtr = TOp::Do(arg);
    }

    static void ArrayCore(const TOffset* stringOffsets, const char* data, TOutput* resPtr, int64_t length, int64_t offset) {
        stringOffsets += offset;
        for (int64_t i = 0; i < length; ++i, ++stringOffsets, ++resPtr) {
            std::string_view val(data + stringOffsets[0], stringOffsets[1] - stringOffsets[0]);
            *resPtr = TOp::Do(val);
        }
    }

    static arrow::Status Exec(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        TTypedStringUnaryScalarFuncPtr scalarFunc = &ScalarCore;
        TTypedStringUnaryArrayFuncPtr arrayFunc = &ArrayCore;
        return ExecStringUnaryImpl(batch, res, &GetPrimitiveDataType<TOutput>, &MakeDefaultScalarDatum<TOutput>,
            (TUntypedStringUnaryScalarFuncPtr)scalarFunc,
            (TUntypedStringUnaryArrayFuncPtr)arrayFunc);
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
