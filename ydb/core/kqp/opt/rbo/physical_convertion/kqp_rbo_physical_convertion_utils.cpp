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

bool IsMultiConsumerHandlerNeeded(const TIntrusivePtr<IOperator>& op) {
    return op->Props.NumOfConsumers.has_value() && op->Props.NumOfConsumers.value() > 1;
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
    TVector<TExprBase> items;
    // clang-format off
    auto arg = Build<TCoArgument>(ctx, input->Pos())
        .Name("arg")
    .Done().Ptr();
    // clang-format on

    for (const auto& iu : members) {
        auto name = iu.GetFullName();
        // clang-format off
            auto tuple = Build<TCoNameValueTuple>(ctx, input->Pos())
                .Name().Build(name)
                .Value<TCoMember>()
                    .Struct(arg)
                    .Name().Build(name)
                .Build()
            .Done();
        // clang-format on
        items.push_back(tuple);
    }

    // clang-format off
    return Build<TCoFlatMap>(ctx, input->Pos())
        .Input(input)
        .Lambda<TCoLambda>()
            .Args({arg})
            .Body<TCoJust>()
                .Input<TCoAsStruct>()
                    .Add(items)
                .Build()
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on
}

} // namespace NKikimr::NKqp::NPhysicalConvertionUtils
