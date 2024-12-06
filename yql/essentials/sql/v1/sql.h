#pragma once

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/parser/lexer_common/hints.h>
#include <yql/essentials/parser/proto_ast/common.h>
#include <yql/essentials/public/issue/yql_warning.h>
#include <yql/essentials/public/issue/yql_issue_manager.h>
#include <yql/essentials/sql/settings/translation_settings.h>

#include <google/protobuf/message.h>

namespace NSQLTranslation {
    struct TTranslationSettings;
}

namespace NSQLTranslationV1 {

    NYql::TAstParseResult SqlToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules = nullptr);
    NYql::TAstParseResult SqlASTToYql(const google::protobuf::Message& protoAst, const NSQLTranslation::TSQLHints& hints, const NSQLTranslation::TTranslationSettings& settings);
    TVector<NYql::TAstParseResult> SqlToAstStatements(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules, TVector<NYql::TStmtParseInfo>* stmtParseInfo = nullptr);

}  // namespace NSQLTranslationV1
