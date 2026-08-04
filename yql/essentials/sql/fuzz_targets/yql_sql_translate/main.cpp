#include "../yql_sql_fuzz_common.h"

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/sql/sql.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <google/protobuf/message.h>

#include <memory>
#include <util/stream/str.h>

namespace {

void ExerciseTokenization(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    NYql::TIssues issues;
    auto lexer = NSQLTranslation::SqlLexer(NFuzzTargets::GetSqlTranslators(), query, issues, settings);
    if (!lexer) {
        return;
    }

    NSQLTranslation::TParsedTokenList tokens;
    if (NSQLTranslation::Tokenize(*lexer, query, settings.File, tokens, issues, settings.MaxErrors)) {
        TStringStream out;
        (void)NSQLTranslation::OutputTokens(out, tokens.begin(), tokens.end());
    }
}

void ExerciseProtoAst(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    NYql::TIssues issues;
    ui16 actualSyntaxVersion = 0;
    std::unique_ptr<google::protobuf::Message> protoAst(
        NSQLTranslation::SqlAST(
            NFuzzTargets::GetSqlTranslators(),
            query,
            settings.File,
            issues,
            settings.MaxErrors,
            settings,
            &actualSyntaxVersion));
    if (protoAst) {
        (void)protoAst->SerializeAsString();
    }
}

void ExerciseTranslation(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    ui16 actualSyntaxVersion = 0;
    auto statements = NSQLTranslation::SqlToAstStatements(
        NFuzzTargets::GetSqlTranslators(),
        query,
        settings,
        nullptr,
        &actualSyntaxVersion);
    for (const auto& statement : statements) {
        if (statement.Root) {
            (void)statement.Root->ToString();
        }
    }

    NYql::TStmtParseInfo stmtInfo;
    NSQLTranslation::TTranslationSettings effectiveSettings;
    auto ast = NSQLTranslation::SqlToYql(
        NFuzzTargets::GetSqlTranslators(),
        query,
        settings,
        nullptr,
        &stmtInfo,
        &effectiveSettings);
    if (ast.Root) {
        const TString printed = ast.Root->ToString(NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);
        auto reparsed = NYql::ParseAst(printed);
        if (reparsed.Root) {
            (void)reparsed.Root->ToString();
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    try {
        FuzzedDataProvider fdp(data, size);
        auto settings = NFuzzTargets::MakeSqlSettings(fdp, false);
        const TString query = fdp.ConsumeRemainingBytesAsString();

        try {
            ExerciseTokenization(query, settings);
        } catch (...) {
        }

        try {
            ExerciseProtoAst(query, settings);
        } catch (...) {
        }

        try {
            ExerciseTranslation(query, settings);
        } catch (...) {
        }
    } catch (...) {
    }
    return 0;
}
