#pragma once

#ifdef TOKEN_QUERY // Conflict with the winnt.h
    #undef TOKEN_QUERY
#endif
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4BaseVisitor.h>

#include <contrib/libs/antlr4_cpp_runtime/src/CommonTokenStream.h>

#include <util/generic/strbuf.h>

namespace NSQLPureAST {

using SQLv1 = NALADefaultAntlr4::SQLv1Antlr4Parser;

using NALADefaultAntlr4::SQLv1Antlr4BaseVisitor;

class IParseTree: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IParseTree>;

    virtual TStringBuf Text() const = 0;
    virtual const antlr4::CommonTokenStream& Tokens() const = 0;
    virtual const SQLv1& Parser() const = 0;
    virtual SQLv1::Sql_queryContext* Root() = 0;
};

} // namespace NSQLPureAST
