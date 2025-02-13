#pragma once

#include <yql/essentials/parser/lexer_common/hints.h>
#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/parser/proto_ast/common.h>
#include <yql/essentials/public/issue/yql_warning.h>
#include <yql/essentials/public/issue/yql_issue_manager.h>
#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/settings/translator.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <google/protobuf/message.h>

namespace NSQLTranslation {

    struct TTranslators {
        TTranslatorPtr const V0;
        TTranslatorPtr const V1;
        TTranslatorPtr const PG;

        TTranslators(TTranslatorPtr v0, TTranslatorPtr v1, TTranslatorPtr pg);
    };

    //FIXME drop this function and overloads without translators
    TTranslators MakeAllTranslators();

    NYql::TAstParseResult SqlToYql(const TString& query, const TTranslationSettings& settings,
        NYql::TWarningRules* warningRules = nullptr, NYql::TStmtParseInfo* stmtParseInfo = nullptr,
        TTranslationSettings* effectiveSettings = nullptr);
    NYql::TAstParseResult SqlToYql(const TTranslators& translators, const TString& query, const TTranslationSettings& settings,
        NYql::TWarningRules* warningRules = nullptr, NYql::TStmtParseInfo* stmtParseInfo = nullptr,
        TTranslationSettings* effectiveSettings = nullptr);

    google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, NYql::TIssues& issues, size_t maxErrors,
        const TTranslationSettings& settings = {}, ui16* actualSyntaxVersion = nullptr);
    google::protobuf::Message* SqlAST(const TTranslators& translators, const TString& query, const TString& queryName, NYql::TIssues& issues, size_t maxErrors,
        const TTranslationSettings& settings = {}, ui16* actualSyntaxVersion = nullptr);

    ILexer::TPtr SqlLexer(const TString& query, NYql::TIssues& issues, const TTranslationSettings& settings = {}, ui16* actualSyntaxVersion = nullptr);
    ILexer::TPtr SqlLexer(const TTranslators& translators, const TString& query, NYql::TIssues& issues, const TTranslationSettings& settings = {}, ui16* actualSyntaxVersion = nullptr);

    NYql::TAstParseResult SqlASTToYql(const TString& query, const google::protobuf::Message& protoAst, const TSQLHints& hints, const TTranslationSettings& settings);
    NYql::TAstParseResult SqlASTToYql(const TTranslators& translators, const TString& query, const google::protobuf::Message& protoAst, const TSQLHints& hints, const TTranslationSettings& settings);

    TVector<NYql::TAstParseResult> SqlToAstStatements(const TString& query, const TTranslationSettings& settings,
        NYql::TWarningRules* warningRules = nullptr, ui16* actualSyntaxVersion = nullptr, TVector<NYql::TStmtParseInfo>* stmtParseInfo = nullptr);
    TVector<NYql::TAstParseResult> SqlToAstStatements(const TTranslators& translators, const TString& query, const TTranslationSettings& settings,
        NYql::TWarningRules* warningRules = nullptr, ui16* actualSyntaxVersion = nullptr, TVector<NYql::TStmtParseInfo>* stmtParseInfo = nullptr);
}  // namespace NSQLTranslation
