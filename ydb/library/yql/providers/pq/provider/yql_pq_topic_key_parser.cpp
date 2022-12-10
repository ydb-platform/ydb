#include "yql_pq_topic_key_parser.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {

namespace {
std::pair<TExprNode::TPtr, TExprNode::TPtr> GetSchema(const TExprNode& settings) {
    for (auto i = 0U; i < settings.ChildrenSize(); ++i) {
        if (settings.Child(i)->Head().IsAtom("userschema")) {
            return { settings.Child(i)->ChildPtr(1), settings.Child(i)->ChildrenSize() > 2 ? settings.Child(i)->TailPtr() : TExprNode::TPtr() };
        }
    }

    return {};
}
}

TTopicKeyParser::TTopicKeyParser(const TExprNode& expr, TExprNode::TPtr readSettings, TExprContext& ctx) {
    YQL_ENSURE(Parse(expr, readSettings, ctx), "Failed to parse topic info");
}

bool TTopicKeyParser::Parse(const TExprNode& expr, TExprNode::TPtr readSettings, TExprContext& ctx) {
    if (readSettings && expr.IsCallable("MrObject")) { // todo: remove MrObject support
        return TryParseObject(expr, readSettings);
    }

    if (!expr.IsCallable("MrTableConcat") && !expr.IsCallable(NNodes::TCoKey::CallableName())) {
        ctx.AddError(TIssue(ctx.GetPosition(expr.Pos()), "Expected MrTableConcat or Key"));
        return false;
    }

    if (expr.IsCallable(NNodes::TCoKey::CallableName())) {
        return TryParseKey(expr, ctx);
    }

    if (readSettings) {
        for (auto i = 0U; i < readSettings->ChildrenSize(); ++i) {
            if (readSettings->Child(i)->Head().IsAtom("userschema")) {
                UserSchema = readSettings->Child(i)->ChildPtr(1);
                if (readSettings->Child(i)->ChildrenSize() > 2) {
                    ColumnOrder = readSettings->Child(i)->TailPtr();
                }
                continue;
            }
            if (readSettings->Child(i)->Head().IsAtom("format")) {
                Format = readSettings->Child(i)->Child(1)->Content();
                continue;
            }
            if (readSettings->Child(i)->Head().IsAtom("compression")) {
                Compression = readSettings->Child(i)->Child(1)->Content();
                continue;
            }
            if (readSettings->Child(i)->Head().IsAtom("data.datetime.formatname")) {
                DateTimeFormatName = readSettings->Child(i);
                continue;
            }
            if (readSettings->Child(i)->Head().IsAtom("data.datetime.format")) {
                DateTimeFormat = readSettings->Child(i);
                continue;
            }
            if (readSettings->Child(i)->Head().IsAtom("data.timestamp.formatname")) {
                TimestampFormatName = readSettings->Child(i);
                continue;
            }
            if (readSettings->Child(i)->Head().IsAtom("data.timestamp.format")) {
                TimestampFormat = readSettings->Child(i);
                continue;
            }
        }
    }

    return TryParseKey(expr.Head(), ctx);
}

bool TTopicKeyParser::TryParseKey(const TExprNode& expr, TExprContext& ctx) {
    const auto maybeKey = NNodes::TExprBase(&expr).Maybe<NNodes::TCoKey>();
    if (!maybeKey) {
        ctx.AddError(TIssue(ctx.GetPosition(expr.Pos()), "Expected Key"));
        return false;
    }

    const auto& keyArg = maybeKey.Cast().Ref().Head();
    if (!keyArg.IsList() || keyArg.ChildrenSize() != 2 ||
        !keyArg.Head().IsAtom("table") || !keyArg.Child(1)->IsCallable(NNodes::TCoString::CallableName())) {
        ctx.AddError(TIssue(ctx.GetPosition(keyArg.Pos()), "Expected single table name"));
        return false;
    }

    TopicPath = TString(keyArg.Child(1)->Child(0)->Content());
    return true;
}

bool TTopicKeyParser::TryParseObject(const TExprNode& expr, TExprNode::TPtr readSettings) {
    std::tie(UserSchema, ColumnOrder) = GetSchema(*readSettings);
    TopicPath = TString(expr.Child(0)->Content());
    Format = TString(expr.Child(1)->Content());
    Compression = TString(expr.Child(2)->Content());
    return true;
}
} // namespace NYql
