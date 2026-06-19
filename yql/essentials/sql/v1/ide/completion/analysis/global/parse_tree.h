#pragma once

#include <yql/essentials/sql/v1/ide/completion/core/position.h>
#include <yql/essentials/sql/v1/ide/pure_ast/parse_tree.h>

#include <util/generic/maybe.h>

namespace NSQLComplete {

using namespace NSQLPureAST;

TMaybe<std::string> GetName(SQLv1::Bind_parameterContext* ctx);

TPosition GetPosition(SQLv1::Bind_parameterContext* ctx);

} // namespace NSQLComplete
