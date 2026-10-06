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

BEGIN_SIMPLE_STRICT_ARROW_UDF(T_yql_AsciiContainsIgnoreCase, bool(TOptional<char*>, char*)) // NOLINT(readability-identifier-naming)
{
    Y_UNUSED(valueBuilder);
    return AsciiContainsIgnoreCaseImpl(args);
}

END_SIMPLE_ARROW_UDF(T_yql_AsciiContainsIgnoreCase, TAsciiContainsIgnoreCaseKernelExec::Do);

SIMPLE_MODULE(TOlapKernelsModule, T_yql_AsciiContainsIgnoreCase)

REGISTER_MODULES(TOlapKernelsModule)
