#include "list_builtin.h"

using namespace NYql;

namespace NSQLTranslationV1 {

TAstNode* TListBuiltin::Translate(TContext& ctx) const {
    Y_DEBUG_ABORT_UNLESS(Node);
    return Node->Translate(ctx);
}

TNodePtr TListBuiltin::GetIdentityLambda() {
    return BuildLambda(Pos, Y("arg"), Y(), "arg");
}

bool TListSortBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args.size() < 1 || Args.size() > 2) {
        ctx.Error(Pos) << OpName << " requires one or two parameters.";
        return false;
    }
    if (!Args[0]->Init(ctx, src)) {
        return false;
    }
    if (Args.size() == 2) {
        if (!Args[1]->Init(ctx, src)) {
            return false;
        }
    } else {
        Args.push_back(GetIdentityLambda());
    }
    Node = Y(OpName, Args[0], Y("Bool", Q(Asc ? "true" : "false")), Args[1]);
    return true;
}

bool TListExtractBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args.size() != 2) {
        ctx.Error(Pos) << OpName << " requires exactly two parameters.";
        return false;
    }

    for (const auto& arg : Args) {
        if (!arg->Init(ctx, src)) {
            return false;
        }
    }

    Args[1] = MakeAtomFromExpression(Pos, ctx, Args[1]).Build();
    Node = Y(OpName, Args[0], Args[1]);
    return true;
}

bool TListProcessBuiltin::CheckArgs(TContext& ctx, ISource* src) {
    if (Args.size() != 2 ) {
        ctx.Error(Pos) << OpName << " requires exactly two parameters";
        return false;
    }

    for (const auto& arg : Args) {
        if (!arg->Init(ctx, src)) {
            return false;
        }
    }

    return true;
}

bool TListMapBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (!CheckArgs(ctx, src)) {
        return false;
    };
    Node = Y(OpName, Args[0], Args[1]);

    return true;
}

bool TListFilterBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (!CheckArgs(ctx, src)) {
        return false;
    };
    Node = Y(OpName, Args[0], GetFilterLambda());
    return true;
}

TNodePtr TListFilterBuiltin::GetFilterLambda() {
    return BuildLambda(Pos, Y("item"), Y("Coalesce", Y("Apply", Args[1], "item"), Y("Bool", Q("false"))));
}

bool TListCreateBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args.size() != 1) {
        ctx.Error(Pos) << OpName << " requires only one parameter";
        return false;
    }
    if (!Args[0]->Init(ctx, src)) {
        return false;
    }
    Node = Y("List", Y("ListType", Args[0]));
    return true;
}

void TListCreateBuiltin::DoUpdateState() const {
    State.Set(ENodeState::Const);
}

bool TDictCreateBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args.size() != 2) {
        ctx.Error(Pos) << OpName << " requires two parameters";
        return false;
    }

    for (ui32 i = 0; i < 2; ++i) {
        if (!Args[i]->Init(ctx, src)) {
            return false;
        }
    }

    Node = Y("Dict", Y("DictType", Args[0], Args[1]));
    return true;
}

void TDictCreateBuiltin::DoUpdateState() const {
    State.Set(ENodeState::Const);
}

bool TSetCreateBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args.size() != 1) {
        ctx.Error(Pos) << OpName << " requires one parameter";
        return false;
    }

    if (!Args[0]->Init(ctx, src)) {
        return false;
    }

    Node = Y("Dict", Y("DictType", Args[0], Y("VoidType")));
    return true;
}

void TSetCreateBuiltin::DoUpdateState() const {
    State.Set(ENodeState::Const);
}

} // namespace NSQLTranslationV1
