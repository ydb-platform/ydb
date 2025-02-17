#pragma once

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/parser/proto_ast/common.h>
#include <yql/essentials/public/issue/yql_warning.h>
#include <yql/essentials/public/issue/yql_issue_manager.h>
#include <yql/essentials/sql/settings/translation_settings.h>

#include <google/protobuf/message.h>

namespace NSQLTranslation {
    struct TTranslationSettings;
}

namespace NSQLTranslationV1 {

    google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, 
        NYql::TIssues& err, size_t maxErrors, bool ansiLexer, bool antlr4Parser, bool testAntlr4, google::protobuf::Arena* arena);
    google::protobuf::Message* SqlAST(const TString& query, const TString& queryName,
        NProtoAST::IErrorCollector& err, bool ansiLexer, bool antlr4Parser, bool testAntlr4, google::protobuf::Arena* arena);

}  // namespace NSQLTranslationV1
