#include "list_builtin.h"

using namespace NYql;

namespace NSQLTranslationV1 {

TAstNode* TListBuiltin::Translate(TContext& ctx) const {
    Y_DEBUG_ABORT_UNLESS(Node_);
    return Node_->Translate(ctx);
}

TNodePtr TListBuiltin::GetIdentityLambda() {
    return BuildLambda(Pos_, Y("arg"), Y(), "arg");
}

bool TListSortBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args_.size() < 1 || Args_.size() > 2) {
        ctx.Error(Pos_) << OpName_ << " requires one or two parameters.";
        return false;
    }
    if (!Args_[0]->Init(ctx, src)) {
        return false;
    }
    if (Args_.size() == 2) {
        if (!Args_[1]->Init(ctx, src)) {
            return false;
        }
    } else {
        Args_.push_back(GetIdentityLambda());
    }
    Node_ = Y(OpName_, Args_[0], Y("Bool", Q(Asc_ ? "true" : "false")), Args_[1]);
    return true;
}

bool TListExtractBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args_.size() != 2) {
        ctx.Error(Pos_) << OpName_ << " requires exactly two parameters.";
        return false;
    }

    for (const auto& arg : Args_) {
        if (!arg->Init(ctx, src)) {
            return false;
        }
    }

    Args_[1] = MakeAtomFromExpression(Pos_, ctx, Args_[1]).Build();
    Node_ = Y(OpName_, Args_[0], Args_[1]);
    return true;
}

bool TListProcessBuiltin::CheckArgs(TContext& ctx, ISource* src) {
    if (Args_.size() != 2 ) {
        ctx.Error(Pos_) << OpName_ << " requires exactly two parameters";
        return false;
    }

    for (const auto& arg : Args_) {
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
    Node_ = Y(OpName_, Args_[0], Args_[1]);

    return true;
}

bool TListFilterBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (!CheckArgs(ctx, src)) {
        return false;
    };
    Node_ = Y(OpName_, Args_[0], GetFilterLambda());
    return true;
}

TNodePtr TListFilterBuiltin::GetFilterLambda() {
    return BuildLambda(Pos_, Y("item"), Y("Coalesce", Y("Apply", Args_[1], "item"), Y("Bool", Q("false"))));
}

bool TListCreateBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args_.size() != 1) {
        ctx.Error(Pos_) << OpName_ << " requires only one parameter";
        return false;
    }
    if (!Args_[0]->Init(ctx, src)) {
        return false;
    }
    Node_ = Y("List", Y("ListType", Args_[0]));
    return true;
}

void TListCreateBuiltin::DoUpdateState() const {
    State_.Set(ENodeState::Const);
}

bool TDictCreateBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args_.size() != 2) {
        ctx.Error(Pos_) << OpName_ << " requires two parameters";
        return false;
    }

    for (ui32 i = 0; i < 2; ++i) {
        if (!Args_[i]->Init(ctx, src)) {
            return false;
        }
    }

    Node_ = Y("Dict", Y("DictType", Args_[0], Args_[1]));
    return true;
}

void TDictCreateBuiltin::DoUpdateState() const {
    State_.Set(ENodeState::Const);
}

bool TSetCreateBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args_.size() != 1) {
        ctx.Error(Pos_) << OpName_ << " requires one parameter";
        return false;
    }

    if (!Args_[0]->Init(ctx, src)) {
        return false;
    }

    Node_ = Y("Dict", Y("DictType", Args_[0], Y("VoidType")));
    return true;
}

void TSetCreateBuiltin::DoUpdateState() const {
    State_.Set(ENodeState::Const);
}

} // namespace NSQLTranslationV1
