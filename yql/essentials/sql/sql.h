#pragma once

#include <yql/essentials/parser/lexer_common/hints.h>
#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/parser/proto_ast/common.h>
#include <yql/essentials/public/issue/yql_warning.h>
#include <yql/essentials/public/issue/yql_issue_manager.h>
#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/sql/settings/translation_settings.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <google/protobuf/message.h>

namespace NSQLTranslation {

    NYql::TAstParseResult SqlToYql(const TString& query, const TTranslationSettings& settings,
        NYql::TWarningRules* warningRules = nullptr, NYql::TStmtParseInfo* stmtParseInfo = nullptr,
        TTranslationSettings* effectiveSettings = nullptr);
    google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, NYql::TIssues& issues, size_t maxErrors,
        const TTranslationSettings& settings = {}, ui16* actualSyntaxVersion = nullptr);
    ILexer::TPtr SqlLexer(const TString& query, NYql::TIssues& issues, const TTranslationSettings& settings = {}, ui16* actualSyntaxVersion = nullptr);
    NYql::TAstParseResult SqlASTToYql(const google::protobuf::Message& protoAst, const TSQLHints& hints, const TTranslationSettings& settings);
    TVector<NYql::TAstParseResult> SqlToAstStatements(const TString& query, const TTranslationSettings& settings,
        NYql::TWarningRules* warningRules = nullptr, ui16* actualSyntaxVersion = nullptr, TVector<NYql::TStmtParseInfo>* stmtParseInfo = nullptr);

}  // namespace NSQLTranslationV0
