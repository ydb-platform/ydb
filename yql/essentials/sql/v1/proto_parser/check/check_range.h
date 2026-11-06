#pragma once

#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <google/protobuf/message.h>

namespace NSQLTranslationV1 {

bool CheckParserRanges(
    const google::protobuf::Message& tree,
    const NSQLTranslation::TParsedTokenList& tokens,
    NYql::TIssues& issues);

} // namespace NSQLTranslationV1
