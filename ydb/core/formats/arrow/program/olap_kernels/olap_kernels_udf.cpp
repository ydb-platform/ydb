#include <ydb/core/formats/arrow/program/ascii_contains/ascii_contains.h>

#include <yql/essentials/public/udf/arrow/udf_arrow_helpers.h>
#include <yql/essentials/public/udf/udf_helpers.h>

using namespace NYql;
using namespace NYql::NUdf;
using NKikimr::NArrow::NSSA::AsciiContainsIgnoreCaseMemchr;

struct TAsciiContainsIgnoreCaseKernelExec: public TBinaryKernelExec<TAsciiContainsIgnoreCaseKernelExec> {
    template <typename TSink>
    static void Process(const IValueBuilder*, TBlockItem arg1, TBlockItem arg2, const TSink& sink) {
        if (!arg1) {
            return sink(TBlockItem(!static_cast<bool>(arg2)));
        }

        const TStringBuf haystack(arg1.AsStringRef());
        const TStringBuf needle(arg2.AsStringRef());
        sink(TBlockItem(AsciiContainsIgnoreCaseMemchr(haystack, needle)));
    }
};

TUnboxedValuePod AsciiContainsIgnoreCaseImpl(const TUnboxedValuePod* args) {
    if (!args[0]) {
        return TUnboxedValuePod(false);
    }

    const TStringBuf haystack(args[0].AsStringRef());
    const TStringBuf needle(args[1].AsStringRef());
    return TUnboxedValuePod(AsciiContainsIgnoreCaseMemchr(haystack, needle));
}

inline constexpr char AsciiContainsIgnoreCaseName[] = "_yql_AsciiContainsIgnoreCase";
inline constexpr char AsciiContainsIgnoreCaseBlocksName[] = "_yql_AsciiContainsIgnoreCase_BlocksImpl";

template <typename TInput>
class TAsciiContainsIgnoreCase: public TBoxedValue {
public:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        Y_UNUSED(valueBuilder);
        return AsciiContainsIgnoreCaseImpl(args);
    }

    static void DeclareSignature(const TStringRef&, TType*, IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        builder.SimpleSignature<bool(TOptional<TInput>, char*)>().IsStrict();
        if (!typesOnly) {
            builder.Implementation(new TAsciiContainsIgnoreCase());
        }
    }
};

template <typename TInput>
class TAsciiContainsIgnoreCaseBlockImpl {
public:
    static void DeclareSignature(const TStringRef& name, TType* userType, IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        builder.IsStrict();
        PrepareSimpleArrowUdf(builder, builder.SimpleSignatureType<bool(TOptional<TInput>, char*)>(), userType,
            TAsciiContainsIgnoreCaseKernelExec::Do, typesOnly, TString(name), arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE);
    }
};

using TAsciiContainsIgnoreCaseScalar = TUserDataTypeFuncFactory<true, false, AsciiContainsIgnoreCaseName,
    TAsciiContainsIgnoreCase, char*, TUtf8>;
using TAsciiContainsIgnoreCaseBlockTypes = TUserDataTypeFuncFactory<true, true, AsciiContainsIgnoreCaseBlocksName,
    TAsciiContainsIgnoreCaseBlockImpl, char*, TUtf8>;

class T_yql_AsciiContainsIgnoreCase: public TAsciiContainsIgnoreCaseScalar { // NOLINT(readability-identifier-naming)
public:
    using TBlockType = TAsciiContainsIgnoreCaseBlockTypes;
};

SIMPLE_MODULE(TOlapKernelsModule, T_yql_AsciiContainsIgnoreCase)

REGISTER_MODULES(TOlapKernelsModule)
