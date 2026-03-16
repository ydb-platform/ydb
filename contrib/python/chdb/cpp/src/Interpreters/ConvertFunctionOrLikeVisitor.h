#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB_CHDB
{

class ASTFunction;

/// Replaces all the "or"'s with {i}like to multiMatchAny
class ConvertFunctionOrLikeData
{
public:
    using TypeToVisit = ASTFunction;

    static void visit(ASTFunction & function, ASTPtr & ast);
};

using ConvertFunctionOrLikeVisitor = InDepthNodeVisitor<OneTypeMatcher<ConvertFunctionOrLikeData>, true>;

}
