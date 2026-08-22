#include "yql_co_pgselect.h"

namespace NYql {

TExprNode::TPtr ExpandPgLike(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    Y_UNUSED(optCtx);
    const bool insensitive = node->IsCallable("PgILike");
    if (!insensitive) {
        auto pattern = node->Child(1);
        bool isCasted = false;

        if (pattern->IsCallable("PgCast") &&
            pattern->Head().IsCallable("PgConst") &&
            pattern->Tail().IsCallable("PgType") &&
            pattern->Tail().Head().Content() == "text")
        {
            pattern = pattern->Child(0);
            isCasted = true;
        }

        if (pattern->IsCallable("PgConst") &&
            pattern->Tail().IsCallable("PgType") &&
            (pattern->Tail().Head().Content() == "text" || isCasted)) {
            auto str = pattern->Head().Content();
            auto hasUnderscore = AnyOf(str, [](char c) { return c == '_'; });
            size_t countOfPercents = 0;
            ForEach(str.begin(), str.end(), [&](char c) { countOfPercents += (c == '%');});
            if (!hasUnderscore && countOfPercents == 0) {
                return ctx.Builder(node->Pos())
                    .Callable("PgOp")
                        .Atom(0, "=")
                        .Add(1, node->ChildPtr(0))
                        .Add(2, pattern)
                    .Seal()
                    .Build();
            }

            TStringBuf op;
            TStringBuf arg;
            if (!hasUnderscore && countOfPercents == 1 && str.StartsWith('%')) {
                op = "EndsWith";
                arg = str.SubString(1, str.size() - 1);
            }

            if (!hasUnderscore && countOfPercents == 1 && str.EndsWith('%')) {
                op = "StartsWith";
                arg = str.SubString(0, str.size() - 1);
            }

            if (!hasUnderscore && countOfPercents == 2 && str.StartsWith('%') && str.EndsWith('%')) {
                op = "StringContains";
                arg = str.SubString(1, str.size() - 2);
            }

            if (!op.empty()) {
                return ctx.Builder(node->Pos())
                    .Callable("ToPg")
                        .Callable(0, op)
                            .Callable(0, "FromPg")
                                .Add(0, node->ChildPtr(0))
                            .Seal()
                            .Callable(1, "String")
                                .Atom(0, arg)
                            .Seal()
                        .Seal()
                    .Seal()
                    .Build();
            }
        }
    }

    auto matcher = ctx.Builder(node->Pos())
        .Callable("Udf")
            .Atom(0, "Re2.Match")
            .List(1)
                .Callable(0, "Apply")
                    .Callable(0, "Udf")
                        .Atom(0, "Re2.PatternFromLike")
                    .Seal()
                    .Callable(1, "Coalesce")
                        .Callable(0, "FromPg")
                            .Add(0, node->ChildPtr(1))
                        .Seal()
                        .Callable(1, "Utf8")
                            .Atom(0, "")
                        .Seal()
                    .Seal()
                .Seal()
                .Callable(1, "NamedApply")
                    .Callable(0, "Udf")
                        .Atom(0, "Re2.Options")
                    .Seal()
                    .List(1)
                    .Seal()
                    .Callable(2, "AsStruct")
                        .List(0)
                            .Atom(0, "CaseSensitive")
                            .Callable(1, "Bool")
                                .Atom(0, insensitive ? "false" : "true")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    return ctx.Builder(node->Pos())
        .Callable("ToPg")
            .Callable(0, "If")
                .Callable(0, "And")
                    .Callable(0, "Exists")
                        .Add(0, node->ChildPtr(0))
                    .Seal()
                    .Callable(1, "Exists")
                        .Add(0, node->ChildPtr(1))
                    .Seal()
                .Seal()
                .Callable(1, "Apply")
                    .Add(0, matcher)
                    .Callable(1, "FromPg")
                        .Add(0, node->ChildPtr(0))
                    .Seal()
                .Seal()
                .Callable(2, "Null")
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr ExpandPgBetween(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    Y_UNUSED(optCtx);
    const bool isSym = node->IsCallable("PgBetweenSym");
    auto input = node->ChildPtr(0);
    auto begin = node->ChildPtr(1);
    auto end = node->ChildPtr(2);
    if (isSym) {
        auto swap = ctx.Builder(node->Pos())
            .Callable("PgOp")
                .Atom(0, "<")
                .Add(1, end)
                .Add(2, begin)
            .Seal()
            .Build();

        auto swapper = [&](auto x, auto y) {
            return ctx.Builder(node->Pos())
                .Callable("IfPresent")
                    .Callable(0, "FromPg")
                        .Add(0, swap)
                    .Seal()
                    .Lambda(1)
                        .Param("unwrapped")
                        .Callable("If")
                            .Arg(0, "unwrapped")
                            .Add(1, y)
                            .Add(2, x)
                        .Seal()
                    .Seal()
                    .Callable(2, "Null")
                    .Seal()
                .Seal()
                .Build();
        };

        // swap: null->null, false->begin, true->end
        auto newBegin = swapper(begin, end);
        // swap: null->null, false->end, true->begin
        auto newEnd = swapper(end, begin);
        begin = newBegin;
        end = newEnd;
    }

    return ctx.Builder(node->Pos())
        .Callable("PgAnd")
            .Callable(0, "PgOp")
                .Atom(0, ">=")
                .Add(1, input)
                .Add(2, begin)
            .Seal()
            .Callable(1, "PgOp")
                .Atom(0, "<=")
                .Add(1, input)
                .Add(2, end)
            .Seal()
        .Seal()
        .Build();
}

} // namespace NYql
