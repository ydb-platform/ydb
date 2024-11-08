#include "list_builtin.h"

using namespace NYql;

namespace NSQLTranslationV0 {

TAstNode* TListBuiltin::Translate(TContext& ctx) const {
    Y_UNUSED(ctx);
    Y_DEBUG_ABORT_UNLESS(Node);
    return Node->Translate(ctx);
}

TNodePtr TListBuiltin::GetIdentityLambda() {
    return BuildLambda(Pos, Y("arg"), Y(), "arg");
}

TNodePtr TListBuiltin::SkipEmpty(TNodePtr arg) {
    auto sameArgLambda = BuildLambda(Pos, Y(), AstNode("item"));
    auto handleNotSkippableType = BuildLambda(Pos, Y(), Y("Just", "item"));
    auto checkOptional = Y("MatchType", "item", Q("Optional"),
                           sameArgLambda, handleNotSkippableType);
    auto checkOptionalLambda = BuildLambda(Pos, Y(), checkOptional);
    auto checkList = Y("MatchType", "item", Q("List"),
        sameArgLambda, checkOptionalLambda);
    return Y("OrderedFlatMap", arg, BuildLambda(Pos, Y("item"), checkList));
}

bool TListSortBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args.size() < 1 || Args.size() > 2) {
        ctx.Error(Pos) << "List" << OpName
                       << " requires one or two parameters";
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
    Node = Y(OpName, SkipEmpty(Args[0]), Y("Bool", Q(Asc ? "true" : "false")), Args[1]);
    return true;
}

bool TListExtractBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args.size() != 2) {
        ctx.Error(Pos) << "List" << OpName
                       << " requires exactly two parameters";
        return false;
    }
    if (!Args[0]->Init(ctx, src)) {
        return false;
    }

    if (!Args[1]->Init(ctx, src)) {
        return false;
    }

    Args[1] = MakeAtomFromExpression(ctx, Args[1]).Build();
    Node = Y(OpName, SkipEmpty(Args[0]), Args[1]);
    return true;
}

bool TListProcessBuiltin::CheckArgs(TContext& ctx, ISource* src) {
    if (Args.size() < 2 ) {
        ctx.Error(Pos) << "List" << OpName
                       << " requires at least two parameters";
        return false;
    }

    for (const auto& arg : Args) {
        if (!arg->Init(ctx, src)) {
            return false;
        }
    }

    OpLiteral = Args[1]->GetLiteral("String");

    return true;
}

TNodePtr TListProcessBuiltin::PrepareResult() {
    TNodePtr result;
    if (OpLiteral) {
        size_t modulePos = OpLiteral->find("::");
        if (modulePos != TString::npos) {
            const TString& module = OpLiteral->substr(0, modulePos);
            const TString& function = OpLiteral->substr(modulePos + 2);
            auto udf = Y("Udf", Q(module + "." + function));
            result = Y("Apply", udf, "item");
        } else {
            result = Y(*OpLiteral, "item");
        }
    } else {
        result = Y("Apply", Args[1], "item");
    }

    for (size_t i = 0; i < Args.size(); ++i) {
        if (i > 1) {
            result->Add(Args[i]);
        }
    }

    return result;
}


bool TListMapBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (!CheckArgs(ctx, src)) {
        return false;
    };
    auto prepare = PrepareResult();
    auto just = BuildLambda(Pos, Y(), Y("Just", prepare));
    auto sameArgLambda = BuildLambda(Pos, Y(), prepare);
    auto match = Y("MatchType", prepare, Q("Data"), just, sameArgLambda);
    auto lambda = Flat ? BuildLambda(Pos, Y("item"), match) : GetMapLambda();
    Node = Y(OpName,
             Flat ? SkipEmpty(Args[0]) : Args[0],
             lambda
           );

    return true;
}

TNodePtr TListMapBuiltin::GetMapLambda() {
    return BuildLambda(Pos, Y("item"), PrepareResult());
}


bool TListFilterBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (!CheckArgs(ctx, src)) {
        return false;
    };
    Node = Y("OrderedFlatMap",
             SkipEmpty(Args[0]),
             GetFilterLambda()
           );
    return true;
}

TNodePtr TListFilterBuiltin::GetFilterLambda() {
    return BuildLambda(Pos, Y("item"), Y("OptionalIf", Y("Coalesce", PrepareResult(), Y("Bool", Q("false"))), "item"));
}


bool TListFoldBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args.size() != ArgCount) {
        ctx.Error(Pos) << "Folding list with " << OpName << "requires exactly " << ArgCount << " parameter";
        return false;
    }
    for (const auto& arg : Args) {
        if (!arg->Init(ctx, src)) {
            return false;
        }
    }
    Node = Y("Fold",
             SkipEmpty(Args[0]),
             GetInitialState(),
             GetUpdateLambda()
           );
    return true;
}


TNodePtr TListFoldBuiltin::GetInitialState() {
    return Y(StateType, Q(StateValue));
}


TNodePtr TListFoldBuiltin::GetUpdateLambda() {
    return BuildLambda(Pos, Y("item", "state"), Y(OpName, "item", "state"));
}


TNodePtr TListCountBuiltin::GetUpdateLambda() {
    return BuildLambda(Pos, Y("item", "state"), Y(OpName, "state"));
}


bool TListAvgBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (TListFoldBuiltin::DoInit(ctx, src)) {
        auto foldResult = Node;
        Node = Y("Div", Y("Nth", foldResult, Q("1")), Y("Nth", foldResult, Q("0")));
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
    return BuildLambda(Pos, Y("item", "state"), Q(Y(count, sum)));
}

TNodePtr TListHasBuiltin::GetUpdateLambda() {
    return BuildLambda(Pos, Y("item", "state"), Y("Or", "state",
                            Y("Coalesce", Y(OpName, "item", Args[1]), Y("Bool", Q("false")))));
}

bool TListFold1Builtin::DoInit(TContext& ctx, ISource* src) {
    if (Args.size() != 1) {
        ctx.Error(Pos) << "Folding list with " << OpName << " requires only one parameter";
        return false;
    }
    if (!Args[0]->Init(ctx, src)) {
        return false;
    }
    Node = Y("Fold1",
             SkipEmpty(Args[0]),
             GetInitLambda(),
             GetUpdateLambda()
           );
    return true;
}
TNodePtr TListFold1Builtin::GetInitLambda() {
    return GetIdentityLambda();
}

TNodePtr TListFold1Builtin::GetUpdateLambda() {
    return BuildLambda(Pos, Y("item", "state"), Y(OpName, "state", "item"));
}

bool TListUniqBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args.size() != 1) {
        ctx.Error(Pos) << OpName << " requires only one parameter";
        return false;
    }
    if (!Args[0]->Init(ctx, src)) {
        return false;
    }
    Node = Y("DictKeys",
             Y("ToDict", Args[0], GetIdentityLambda(), BuildLambda(Pos, Y("item"), Y("Void")), Q(Y(Q("Hashed"), Q("One"))))
           );
    return true;
}

bool TListCreateBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args.size() != 1) {
        ctx.Error(Pos) << OpName << " requires only one parameter";
        return false;
    }
    if (!Args[0]->Init(ctx, src)) {
        return false;
    }
    auto literal = Args[0]->GetLiteral("String");
    if (literal) {
        Node = Y("List",
                 Y("ListType",
                   Y("ParseType", Q(*literal))));
    } else {
        Node = Y("List",
                 Y("ListType", Args[0]));
    }
    return true;
}

bool TDictCreateBuiltin::DoInit(TContext& ctx, ISource* src) {
    if (Args.size() != 2) {
        ctx.Error(Pos) << OpName << " requires two parameters";
        return false;
    }

    TNodePtr types[2];
    for (ui32 i = 0; i < 2; ++i) {
        if (!Args[i]->Init(ctx, src)) {
            return false;
        }

        auto literal = Args[i]->GetLiteral("String");
        if (literal) {
            types[i] = Y("ParseType", Q(*literal));
        } else {
            types[i] = Args[i];
        }
    }

    Node = Y("Dict",
             Y("DictType", types[0], types[1]));

    return true;
}

} // namespace NSQLTranslationV0
