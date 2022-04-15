#pragma once

#include <ydb/library/yql/ast/yql_ast.h>
#include <ydb/library/yql/parser/proto_ast/proto_ast.h>
#include <ydb/library/yql/public/issue/yql_warning.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>

#include <google/protobuf/message.h>

namespace NSQLTranslation {
    struct TTranslationSettings;
}

namespace NSQLTranslationV1 {

    google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, NYql::TIssues& err, size_t maxErrors, bool ansiLexer, google::protobuf::Arena* arena = nullptr);
    google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, NProtoAST::IErrorCollector& err, bool ansiLexer, google::protobuf::Arena* arena);

}  // namespace NSQLTranslationV1
