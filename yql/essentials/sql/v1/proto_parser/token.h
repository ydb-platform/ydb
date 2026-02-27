#pragma once

#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

TToken Beginning(const TRule_select_stmt& rule);

} // namespace NSQLTranslationV1
