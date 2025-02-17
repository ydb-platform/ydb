#pragma once

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/parser/lexer_common/hints.h>
#include <yql/essentials/parser/proto_ast/common.h>
#include <yql/essentials/parser/proto_ast/gen/v1_proto_split/SQLv1Parser.pb.main.h>
#include <yql/essentials/public/issue/yql_warning.h>
#include <yql/essentials/public/issue/yql_issue_manager.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/settings/translator.h>

#include <google/protobuf/message.h>

namespace NSQLTranslation {
    struct TTranslationSettings;
}

namespace NSQLTranslationV1 {

    NYql::TAstParseResult SqlToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules = nullptr);

    NYql::TAstParseResult SqlASTToYql(const TString& query, const google::protobuf::Message& protoAst, const NSQLTranslation::TSQLHints& hints, const NSQLTranslation::TTranslationSettings& settings);

    TVector<NYql::TAstParseResult> SqlToAstStatements(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules, TVector<NYql::TStmtParseInfo>* stmtParseInfo = nullptr);

    bool NeedUseForAllStatements(const NSQLv1Generated::TRule_sql_stmt_core::AltCase& subquery);

    bool SplitQueryToStatements(const TString& query, TVector<TString>& statements, NYql::TIssues& issues,
        const NSQLTranslation::TTranslationSettings& settings);

    NSQLTranslation::TTranslatorPtr MakeTranslator();
}  // namespace NSQLTranslationV1
