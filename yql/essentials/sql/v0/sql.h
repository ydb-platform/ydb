#pragma once

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/parser/proto_ast/common.h>
#include <yql/essentials/public/issue/yql_warning.h>
#include <yql/essentials/public/issue/yql_issue_manager.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/settings/translator.h>

#include <google/protobuf/message.h>

namespace NSQLTranslationV0 {

    NYql::TAstParseResult SqlToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules = nullptr);
    google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, NYql::TIssues& err, size_t maxErrors, google::protobuf::Arena* arena = nullptr);
    NYql::TAstParseResult SqlASTToYql(const google::protobuf::Message& protoAst, const NSQLTranslation::TTranslationSettings& settings);
    NSQLTranslation::TTranslatorPtr MakeTranslator();

}  // namespace NSQLTranslationV0
