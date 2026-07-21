#include "../yql_sql_fuzz_common.h"

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/public/issue/yql_issue_manager.h>
#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/format/sql_format.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

namespace {

void CompareAstTexts(const TString& original, const TString& formatted, const NSQLTranslation::TTranslationSettings& settings) {
    auto originalAst = NSQLTranslation::SqlToYql(NFuzzTargets::GetV1OnlyTranslators(), original, settings);
    auto formattedAst = NSQLTranslation::SqlToYql(NFuzzTargets::GetV1OnlyTranslators(), formatted, settings);
    if (!originalAst.Root || !formattedAst.Root) {
        return;
    }

    const TString originalText = originalAst.Root->ToString(NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);
    const TString formattedText = formattedAst.Root->ToString(NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);
    const bool same = originalText == formattedText;
    (void)same;
}

void ExerciseFormatting(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NSQLFormat::EFormatMode mode) {
    auto formatter = NSQLFormat::MakeSqlFormatter(NFuzzTargets::GetV1Lexers(), NFuzzTargets::GetV1Parsers(), settings);

    NYql::TIssues issues;
    TString formatted;
    if (!formatter->Format(query, formatted, issues, mode)) {
        return;
    }

    TString formattedAgain;
    (void)formatter->Format(formatted, formattedAgain, issues, mode);

    auto formattedAst = NSQLTranslation::SqlToYql(NFuzzTargets::GetV1OnlyTranslators(), formatted, settings);
    if (formattedAst.Root) {
        (void)formattedAst.Root->ToString();
    }

    if (mode == NSQLFormat::EFormatMode::Pretty) {
        CompareAstTexts(query, formatted, settings);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 32 * 1024) {
        return 0;
    }

    try {
        FuzzedDataProvider fdp(data, size);
        auto settings = NFuzzTargets::MakeSqlSettings(fdp, true);
        const TString query = fdp.ConsumeRemainingBytesAsString();

        try {
            const TString mutated = NSQLFormat::MutateQuery(NFuzzTargets::GetV1Lexers(), query, settings);
            ExerciseFormatting(mutated, settings, NSQLFormat::EFormatMode::Pretty);
            ExerciseFormatting(mutated, settings, NSQLFormat::EFormatMode::Obfuscate);
        } catch (...) {
        }

        try {
            ExerciseFormatting(query, settings, NSQLFormat::EFormatMode::Pretty);
        } catch (...) {
        }

        try {
            ExerciseFormatting(query, settings, NSQLFormat::EFormatMode::Obfuscate);
        } catch (...) {
        }
    } catch (...) {
    }

    return 0;
}
