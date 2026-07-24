#include <yql/essentials/ast/yql_ast.h>

#include <util/generic/string.h>

namespace {

void ExercisePrintVariants(const NYql::TAstNode& root) {
    static constexpr ui32 PrintFlags[] = {
        NYql::TAstPrintFlags::Default,
        NYql::TAstPrintFlags::ShortQuote,
        NYql::TAstPrintFlags::PerLine,
        NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote,
        NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote | NYql::TAstPrintFlags::AdaptArbitraryContent,
    };

    for (ui32 flags : PrintFlags) {
        const TString printed = root.ToString(flags);
        auto reparsed = NYql::ParseAst(printed);
        if (reparsed.Root) {
            (void)reparsed.Root->ToString();
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    const TString input(reinterpret_cast<const char*>(data), size);
    auto parsed = NYql::ParseAst(input);
    if (parsed.Root) {
        ExercisePrintVariants(*parsed.Root);
    }
    return 0;
}
