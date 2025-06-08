#pragma once

#include <util/generic/maybe.h>

#ifdef TOKEN_QUERY // Conflict with the winnt.h
    #undef TOKEN_QUERY
#endif
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4BaseVisitor.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

namespace NSQLComplete {

    using SQLv1 = NALADefaultAntlr4::SQLv1Antlr4Parser;

    using NALADefaultAntlr4::SQLv1Antlr4BaseVisitor;

    TMaybe<std::string> GetId(SQLv1::Bind_parameterContext* ctx);

} // namespace NSQLComplete
