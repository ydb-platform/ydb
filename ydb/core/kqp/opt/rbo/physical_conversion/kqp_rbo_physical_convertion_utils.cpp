#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace NKikimr::NKqp::NPhysicalConvertionUtils {
TString GetFullName(const TString& name) {
    return name;
}

TString GetFullName(const TInfoUnit& name) {
    return name.GetFullName();
}

TVector<TInfoUnit> GetLiveOutputIUs(IOperator& op) {
    const auto outputIUs = op.GetOutputIUs();
    const auto& liveOut = GetLiveOut(&op);
    TVector<TInfoUnit> liveOutputIUs;
    liveOutputIUs.reserve(outputIUs.size());
    for (const auto& output : outputIUs) {
        if (liveOut.contains(output)) {
            liveOutputIUs.push_back(output);
        }
    }
    return liveOutputIUs;
}

TVector<TInfoUnit> GetLiveInputIUs(IOperator& op, ui32 childIndex) {
    Y_ENSURE(childIndex < op.Children.size());
    const auto outputIUs = op.Children[childIndex]->GetOutputIUs();
    const auto& liveIn = GetLiveIn(&op, childIndex);

    TVector<TInfoUnit> liveInputIUs;
    liveInputIUs.reserve(outputIUs.size());
    for (const auto& output : outputIUs) {
        if (liveIn.contains(output)) {
            liveInputIUs.push_back(output);
        }
    }
    return liveInputIUs;
}

TCoAtomList BuildAtomList(TStringBuf value, TPositionHandle pos, TExprContext& ctx) {
    // clang-format off
    return Build<TCoAtomList>(ctx, pos)
        .Add<TCoAtom>()
            .Value(value)
            .Build()
    .Done();
    // clang-format on
}

TExprNode::TPtr BuildMultiConsumerHandler(TExprNode::TPtr input, const ui32 numConsumers, TExprContext& ctx, TPositionHandle pos) {
    TVector<TExprBase> branches;
    auto inputIndex = BuildAtomList("0", pos, ctx);
    for (ui32 i = 0; i < numConsumers; ++i) {
        branches.emplace_back(inputIndex);
        // Just an empty lambda.
        // clang-format off
        auto lambda = Build<TCoLambda>(ctx, pos)
            .Args({"arg"})
            .Body("arg")
        .Done();
        // clang-format on
        branches.push_back(lambda);
    }

    // clang-format off
    return Build<TCoSwitch>(ctx, pos)
        .Input(input)
        .BufferBytes()
            .Value(ToString(128_MB))
        .Build()
        .FreeArgs()
            .Add(branches)
        .Build()
     .Done().Ptr();
     // clang-format on
}

TExprNode::TPtr ReplaceArg(TExprNode::TPtr input, TExprNode::TPtr arg, TExprContext &ctx, bool removeAliases) {
    // FIXME: This is not always correct, for example:
    // lambda($arg) { $val = expr($arg); return member($val `name)}
    // will replace only member arg but leave expr with free arg.
    if (input->IsCallable("Member")) {
        auto member = TCoMember(input);
        auto memberName = member.Name();
        if (removeAliases) {
            auto strippedName = memberName.StringValue();
            if (auto idx = strippedName.find_last_of('.'); idx != TString::npos) {
                strippedName = strippedName.substr(idx + 1);
            }
            // clang-format off
            memberName = Build<TCoAtom>(ctx, input->Pos()).Value(strippedName).Done();
            // clang-format on
        }
        // clang-format off
        return Build<TCoMember>(ctx, input->Pos())
            .Struct(arg)
            .Name(memberName)
        .Done().Ptr();
        // clang-format on
    } else if (input->IsCallable()) {
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->Children()) {
            newChildren.push_back(ReplaceArg(c, arg, ctx, removeAliases));
        }
        // clang-format off
        return ctx.Builder(input->Pos())
            .Callable(input->Content())
                .Add(std::move(newChildren))
                .Seal()
            .Build();
        // clang-format on
    } else if (input->IsList()) {
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->Children()) {
            newChildren.push_back(ReplaceArg(c, arg, ctx, removeAliases));
        }
        // clang-format off
        return ctx.Builder(input->Pos())
            .List()
                .Add(std::move(newChildren))
                .Seal()
            .Build();
        // clang-format on
    } else {
        return input;
    }
}

TExprNode::TPtr ExtractMembers(TExprNode::TPtr input, TExprContext &ctx, TVector<TInfoUnit> members) {
    auto arg = ctx.NewArgument(input->Pos(), "extract_members_arg");
    TExprNode::TListType fields;
    fields.reserve(members.size());
    for (const auto& iu : members) {
        fields.emplace_back(ctx.Builder(input->Pos())
            .List()
                .Atom(0, iu.GetFullName())
                .Callable(1, "Member")
                    .Add(0, arg)
                    .Atom(1, iu.GetFullName())
                .Seal()
            .Seal()
            .Build());
    }

    auto body = ctx.NewCallable(input->Pos(), "AsStruct", std::move(fields));
    auto lambda = ctx.NewLambda(input->Pos(), ctx.NewArguments(input->Pos(), {std::move(arg)}), std::move(body));
    // OrderedMap is conservative when constraints have not been computed for the newly built input yet.
    return ctx.NewCallable(input->Pos(), "OrderedMap", {std::move(input), std::move(lambda)});
}

TExprNode::TPtr BuildRenameMap(TExprNode::TPtr input, const TVector<std::pair<TString, TString>>& renames, TExprContext& ctx) {
    const auto arg = Build<TCoArgument>(ctx, input->Pos()).Name("map_arg").Done().Ptr();
    TVector<TExprBase> items;
    for (const auto& rename : renames) {
        // clang-format off
        auto tuple = Build<TCoNameValueTuple>(ctx, input->Pos())
            .Name().Build(rename.second)
            .Value<TCoMember>()
                .Struct(arg)
                .Name().Build(rename.first)
            .Build()
        .Done();
        // clang-format on
        items.push_back(tuple);
    }

    // clang-format off
    return Build<TCoMap>(ctx, input->Pos())
        .Input(input)
        .Lambda<TCoLambda>()
            .Args({arg})
            .Body<TCoAsStruct>()
                .Add(items)
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr ConvertToWideJoinFilter(TExprNode::TPtr input, const TVector<TInfoUnit>& inputs, const TVector<bool>& unwrapOptionalInputs, TExprContext& ctx) {
    Y_ENSURE(input->IsLambda());

    TVector<TExprNode::TPtr> lambdaArgs;
    lambdaArgs.reserve(inputs.size());
    for (ui32 i = 0; i < inputs.size(); ++i) {
        lambdaArgs.push_back(ctx.NewArgument(input->Pos(), "param" + ToString(i)));
    }

    TVector<TExprBase> items;
    for (ui32 i = 0; i < inputs.size(); ++i) {
        TExprNode::TPtr value = lambdaArgs[i];
        if (unwrapOptionalInputs[i]) {
            value = Build<TCoUnwrap>(ctx, input->Pos())
                .Optional(value)
            .Done().Ptr();
        }

        // clang-format off
        auto tuple = Build<TCoNameValueTuple>(ctx, input->Pos())
            .Name().Build(inputs[i].GetFullName())
            .Value(value)
        .Done();
        // clang-format on
        items.push_back(tuple);
    }

    // clang-format off
    auto asStruct = Build<TCoAsStruct>(ctx, input->Pos())
        .Add(items)
    .Done().Ptr();
    // clang-format on

    auto lambda = TCoLambda(input);
    auto body = lambda.Body().Ptr();
    auto arg = lambda.Args().Arg(0);
    auto newBody = ctx.ReplaceNode(std::move(body), arg.Ref(), asStruct);

    if (!TMaybeNode<TCoVoid>(newBody)) {
        // Wrap with coalsesce in case of null input.
        // clang-format off
        newBody = Build<TCoCoalesce>(ctx, input->Pos())
            .Predicate(newBody)
            .Value<TCoBool>()
                .Literal().Value("false").Build()
            .Build()
        .Done().Ptr();
        // clang-format on
    }

    return ctx.NewLambda(input->Pos(), ctx.NewArguments(input->Pos(), std::move(lambdaArgs)), std::move(newBody));
}

TExprNode::TPtr BuildVoidLambda(TExprContext& ctx, TPositionHandle pos) {
    // clang-format off
    return Build<TCoLambda>(ctx, pos)
        .Args({"arg"})
        .Body<TCoVoid>().Build()
    .Done().Ptr();
    // clang-format on
}

} // namespace NKikimr::NKqp::NPhysicalConvertionUtils
