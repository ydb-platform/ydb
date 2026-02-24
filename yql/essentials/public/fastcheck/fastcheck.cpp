#include "fastcheck.h"

#include "settings.h"

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/core/user_data/yql_user_data.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/yql_user_data_storage.h>
#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>
#include <yql/essentials/core/langver/yql_core_langver.h>

namespace NYql::NFastCheck {

namespace {

void FillSettings(NSQLTranslation::TTranslationSettings& settings, const TOptions& options) {
    settings.LangVer = options.LangVer;
    settings.ClusterMapping = options.ClusterMapping;
    settings.SyntaxVersion = options.SyntaxVersion;
    settings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
    settings.Flags = TranslationFlags();
}

} // namespace

bool CheckProgram(const TString& program, const TOptions& options, TIssues& errors) {
    TMaybe<TIssue> verIssue;
    auto verCheck = CheckLangVersion(options.LangVer, GetMaxReleasedLangVersion(), verIssue);
    if (verIssue) {
        errors.AddIssue(*verIssue);
    }

    if (!verCheck) {
        return false;
    }

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();

    NSQLTranslation::TTranslators translators(
        nullptr,
        NSQLTranslationV1::MakeTranslator(lexers, parsers),
        NSQLTranslationPG::MakeTranslator());

    TAstParseResult astRes;
    if (options.IsSql) {
        NSQLTranslation::TTranslationSettings settings;
        FillSettings(settings, options);
        settings.EmitReadsForExists = true;
        if (options.IsLibrary) {
            settings.Mode = NSQLTranslation::ESqlMode::LIBRARY;
        }

        astRes = SqlToYql(translators, program, settings);
    } else {
        astRes = ParseAst(program);
    }

    if (!astRes.IsOk()) {
        errors.AddIssues(astRes.Issues);
        return false;
    }

    if (options.IsLibrary) {
        return true;
    }

    if (options.ParseOnly) {
        // parse SQL libs
        for (const auto& x : options.SqlLibs) {
            NSQLTranslation::TTranslationSettings settings;
            FillSettings(settings, options);
            settings.File = x.first;
            settings.Mode = NSQLTranslation::ESqlMode::LIBRARY;

            astRes = SqlToYql(translators, x.second, settings);
            if (!astRes.IsOk()) {
                errors.AddIssues(astRes.Issues);
                return false;
            }
        }

        return true;
    }

    TVector<NUserData::TUserData> userData;
    for (const auto& x : options.SqlLibs) {
        NUserData::TUserData data;
        data.Type = NUserData::EType::LIBRARY;
        data.Disposition = NUserData::EDisposition::INLINE;
        data.Name = x.first;
        data.Content = x.second;
        userData.push_back(data);
    }

    TExprContext libCtx;
    libCtx.IssueManager.AddIssues(std::move(astRes.Issues));
    IModuleResolver::TPtr moduleResolver;
    TUserDataTable userDataTable = GetYqlModuleResolver(libCtx, moduleResolver, userData, options.ClusterMapping, {});
    if (!userDataTable) {
        errors.AddIssues(libCtx.IssueManager.GetIssues());
        libCtx.IssueManager.Reset();
        return false;
    }

    auto userDataStorage = MakeIntrusive<TUserDataStorage>(nullptr, userDataTable, nullptr, nullptr);
    if (auto modules = dynamic_cast<TModuleResolver*>(moduleResolver.get())) {
        modules->AttachUserData(userDataStorage);
    }

    TExprContext exprCtx(libCtx.NextUniqueId);
    TExprNode::TPtr exprRoot;
    if (!CompileExpr(*astRes.Root, exprRoot, exprCtx, moduleResolver.get(), nullptr, false, Max<ui32>(), options.SyntaxVersion)) {
        errors.AddIssues(exprCtx.IssueManager.GetIssues());
        exprCtx.IssueManager.Reset();
        return false;
    }

    return true;
}

} // namespace NYql::NFastCheck
