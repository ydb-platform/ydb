#include "list_builtin.h"

using namespace NYql;

namespace NSQLTranslationV0 {

TAstNode* TListBuiltin::Translate(TContext& ctx) const {
    Y_UNUSED(ctx);
    Y_DEBUG_ABORT_UNLESS(Node_);
    return Node_->Translate(ctx);
}

TNodePtr TListBuiltin::GetIdentityLambda() {
    return BuildLambda(Pos_, Y("arg"), Y(), "arg");
}

TNodePtr TListBuiltin::SkipEmpty(TNodePtr arg) {
    auto sameArgLambda = BuildLambda(Pos_, Y(), AstNode("item"));
    auto handleNotSkippableType = BuildLambda(Pos_, Y(), Y("Just", "item"));
    auto checkOptional = Y("MatchType", "item", Q("Optional"),
                           sameArgLambda, handleNotSkippableType);
    auto checkOptionalLambda = BuildLambda(Pos_, Y(), checkOptional);
    auto checkList = Y("MatchType", "item", Q("List"),
        sameArgLambda, checkOptionalLambda);
    return Y("OrderedFlatMap", arg, BuildLambda(Pos_, Y("item"), checkList));
}

bool TListSortBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args_.size() < 1 || Args_.size() > 2) {
        ctx.Error(Pos_) << "List" << OpName_
                       << " requires one or two parameters";
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
    Node_ = Y(OpName_, SkipEmpty(Args_[0]), Y("Bool", Q(Asc_ ? "true" : "false")), Args_[1]);
    return true;
}

bool TListExtractBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args_.size() != 2) {
        ctx.Error(Pos_) << "List" << OpName_
                       << " requires exactly two parameters";
        return false;
    }
    if (!Args_[0]->Init(ctx, src)) {
        return false;
    }

    if (!Args_[1]->Init(ctx, src)) {
        return false;
    }

    Args_[1] = MakeAtomFromExpression(ctx, Args_[1]).Build();
    Node_ = Y(OpName_, SkipEmpty(Args_[0]), Args_[1]);
    return true;
}

bool TListProcessBuiltin::CheckArgs(TContext& ctx, ISource* src) {
    if (Args_.size() < 2 ) {
        ctx.Error(Pos_) << "List" << OpName_
                       << " requires at least two parameters";
        return false;
    }

    for (const auto& arg : Args_) {
        if (!arg->Init(ctx, src)) {
            return false;
        }
    }

    OpLiteral_ = Args_[1]->GetLiteral("String");

    return true;
}

TNodePtr TListProcessBuiltin::PrepareResult() {
    TNodePtr result;
    if (OpLiteral_) {
        size_t modulePos = OpLiteral_->find("::");
        if (modulePos != TString::npos) {
            const TString& module = OpLiteral_->substr(0, modulePos);
            const TString& function = OpLiteral_->substr(modulePos + 2);
            auto udf = Y("Udf", Q(module + "." + function));
            result = Y("Apply", udf, "item");
        } else {
            result = Y(*OpLiteral_, "item");
        }
    } else {
        result = Y("Apply", Args_[1], "item");
    }

    for (size_t i = 0; i < Args_.size(); ++i) {
        if (i > 1) {
            result->Add(Args_[i]);
        }
    }

    return result;
}


bool TListMapBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (!CheckArgs(ctx, src)) {
        return false;
    };
    auto prepare = PrepareResult();
    auto just = BuildLambda(Pos_, Y(), Y("Just", prepare));
    auto sameArgLambda = BuildLambda(Pos_, Y(), prepare);
    auto match = Y("MatchType", prepare, Q("Data"), just, sameArgLambda);
    auto lambda = Flat_ ? BuildLambda(Pos_, Y("item"), match) : GetMapLambda();
    Node_ = Y(OpName_,
             Flat_ ? SkipEmpty(Args_[0]) : Args_[0],
             lambda
           );

    return true;
}

TNodePtr TListMapBuiltin::GetMapLambda() {
    return BuildLambda(Pos_, Y("item"), PrepareResult());
}


bool TListFilterBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (!CheckArgs(ctx, src)) {
        return false;
    };
    Node_ = Y("OrderedFlatMap",
             SkipEmpty(Args_[0]),
             GetFilterLambda()
           );
    return true;
}

TNodePtr TListFilterBuiltin::GetFilterLambda() {
    return BuildLambda(Pos_, Y("item"), Y("OptionalIf", Y("Coalesce", PrepareResult(), Y("Bool", Q("false"))), "item"));
}


bool TListFoldBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args_.size() != ArgCount_) {
        ctx.Error(Pos_) << "Folding list with " << OpName_ << "requires exactly " << ArgCount_ << " parameter";
        return false;
    }
    for (const auto& arg : Args_) {
        if (!arg->Init(ctx, src)) {
            return false;
        }
    }
    Node_ = Y("Fold",
             SkipEmpty(Args_[0]),
             GetInitialState(),
             GetUpdateLambda()
           );
    return true;
}


TNodePtr TListFoldBuiltin::GetInitialState() {
    return Y(StateType_, Q(StateValue_));
}


TNodePtr TListFoldBuiltin::GetUpdateLambda() {
    return BuildLambda(Pos_, Y("item", "state"), Y(OpName_, "item", "state"));
}


TNodePtr TListCountBuiltin::GetUpdateLambda() {
    return BuildLambda(Pos_, Y("item", "state"), Y(OpName_, "state"));
}


bool TListAvgBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (TListFoldBuiltin::DoInit(ctx, src)) {
        auto foldResult = Node_;
        Node_ = Y("Div", Y("Nth", foldResult, Q("1")), Y("Nth", foldResult, Q("0")));
        return true;
    } else {
        return false;
    }
}

TNodePtr TListAvgBuiltin::GetInitialState() {
    return Q(Y(Y("Uint64", Q("0")), Y("Double", Q("0"))));
}

TNodePtr TListAvgBuiltin::GetUpdateLambda() {
    auto count = Y("Inc", Y("Nth", "state", Q("0")));
    auto sum = Y("Add", "item", Y("Nth", "state", Q("1")));
    return BuildLambda(Pos_, Y("item", "state"), Q(Y(count, sum)));
}

TNodePtr TListHasBuiltin::GetUpdateLambda() {
    return BuildLambda(Pos_, Y("item", "state"), Y("Or", "state",
                            Y("Coalesce", Y(OpName_, "item", Args_[1]), Y("Bool", Q("false")))));
}

bool TListFold1Builtin::DoInit(TContext& ctx, ISource* src) {
    if (Args_.size() != 1) {
        ctx.Error(Pos_) << "Folding list with " << OpName_ << " requires only one parameter";
        return false;
    }
    if (!Args_[0]->Init(ctx, src)) {
        return false;
    }
    Node_ = Y("Fold1",
             SkipEmpty(Args_[0]),
             GetInitLambda(),
             GetUpdateLambda()
           );
    return true;
}
TNodePtr TListFold1Builtin::GetInitLambda() {
    return GetIdentityLambda();
}

TNodePtr TListFold1Builtin::GetUpdateLambda() {
    return BuildLambda(Pos_, Y("item", "state"), Y(OpName_, "state", "item"));
}

bool TListUniqBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args_.size() != 1) {
        ctx.Error(Pos_) << OpName_ << " requires only one parameter";
        return false;
    }
    if (!Args_[0]->Init(ctx, src)) {
        return false;
    }
    Node_ = Y("DictKeys",
             Y("ToDict", Args_[0], GetIdentityLambda(), BuildLambda(Pos_, Y("item"), Y("Void")), Q(Y(Q("Hashed"), Q("One"))))
           );
    return true;
}

bool TListCreateBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args_.size() != 1) {
        ctx.Error(Pos_) << OpName_ << " requires only one parameter";
        return false;
    }
    if (!Args_[0]->Init(ctx, src)) {
        return false;
    }
    auto literal = Args_[0]->GetLiteral("String");
    if (literal) {
        Node_ = Y("List",
                 Y("ListType",
                   Y("ParseType", Q(*literal))));
    } else {
        Node_ = Y("List",
                 Y("ListType", Args_[0]));
    }
    return true;
}

bool TDictCreateBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args_.size() != 2) {
        ctx.Error(Pos_) << OpName_ << " requires two parameters";
        return false;
    }

    TNodePtr types[2]; // NOLINT(modernize-avoid-c-arrays)
    for (ui32 i = 0; i < 2; ++i) {
        if (!Args_[i]->Init(ctx, src)) {
            return false;
        }

        auto literal = Args_[i]->GetLiteral("String");
        if (literal) {
            types[i] = Y("ParseType", Q(*literal));
        } else {
            types[i] = Args_[i];
        }
    }

    Node_ = Y("Dict",
             Y("DictType", types[0], types[1]));

    return true;
}

} // namespace NSQLTranslationV0
