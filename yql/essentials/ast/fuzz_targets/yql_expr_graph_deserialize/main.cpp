#include <yql/essentials/ast/serialize/yql_expr_serialize.h>
#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/yql_expr.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace {

constexpr ui32 MaxDepth = 5;
constexpr ui32 MaxChildren = 5;
constexpr size_t MaxAtomSize = 96;
constexpr size_t MaxSavedNodes = 96;

TString SmallName(FuzzedDataProvider& provider, TStringBuf prefix) {
    TString out(prefix.data(), prefix.size());
    const size_t size = provider.ConsumeIntegralInRange<size_t>(0, 16);
    for (size_t i = 0; i < size; ++i) {
        const ui8 ch = provider.ConsumeIntegralInRange<ui8>(0, 37);
        if (ch < 10) {
            out += char('0' + ch);
        } else if (ch < 36) {
            out += char('a' + ch - 10);
        } else {
            out += '_';
        }
    }
    return out;
}

TString AtomContent(FuzzedDataProvider& provider) {
    TString value = provider.ConsumeRandomLengthString(
        provider.ConsumeIntegralInRange<size_t>(0, MaxAtomSize));
    for (char& c : value) {
        if (static_cast<unsigned char>(c) < 0x20) {
            c = char('a' + (static_cast<unsigned char>(c) % 26));
        }
    }
    return value;
}

NYql::TPosition MakePosition(FuzzedDataProvider& provider) {
    if (!provider.ConsumeBool()) {
        return {};
    }

    return NYql::TPosition(
        provider.ConsumeIntegralInRange<ui32>(1, 100000),
        provider.ConsumeIntegralInRange<ui32>(1, 100000),
        SmallName(provider, "f"));
}

NYql::TExprNode::TPtr MakeNode(
    NYql::TExprContext& ctx,
    FuzzedDataProvider& provider,
    ui32 depth,
    NYql::TExprNode::TListType& saved);

NYql::TExprNode::TListType MakeChildren(
    NYql::TExprContext& ctx,
    FuzzedDataProvider& provider,
    ui32 depth,
    NYql::TExprNode::TListType& saved,
    ui32 minChildren = 0) {
    const ui32 count = provider.ConsumeIntegralInRange<ui32>(minChildren, MaxChildren);
    NYql::TExprNode::TListType children;
    children.reserve(count);
    for (ui32 i = 0; i < count; ++i) {
        children.push_back(MakeNode(ctx, provider, depth + 1, saved));
    }
    return children;
}

NYql::TExprNode::TPtr MakeLeaf(
    NYql::TExprContext& ctx,
    FuzzedDataProvider& provider,
    NYql::TPosition pos) {
    switch (provider.ConsumeIntegralInRange<ui8>(0, 2)) {
        case 0: {
            ui32 flags = NYql::TNodeFlags::ArbitraryContent;
            if (provider.ConsumeBool()) {
                flags = provider.ConsumeBool() ? NYql::TNodeFlags::Default : NYql::TNodeFlags::BinaryContent;
            }
            return ctx.NewAtom(pos, AtomContent(provider), flags);
        }
        case 1:
            return ctx.NewArgument(pos, SmallName(provider, "arg"));
        default:
            return ctx.NewWorld(pos);
    }
}

NYql::TExprNode::TPtr MakeNode(
    NYql::TExprContext& ctx,
    FuzzedDataProvider& provider,
    ui32 depth,
    NYql::TExprNode::TListType& saved) {
    if (!saved.empty() && provider.ConsumeIntegralInRange<ui8>(0, 9) == 0) {
        return saved[provider.ConsumeIntegralInRange<size_t>(0, saved.size() - 1)];
    }

    const auto pos = MakePosition(provider);
    NYql::TExprNode::TPtr node;
    if (depth >= MaxDepth) {
        node = MakeLeaf(ctx, provider, pos);
    } else {
        switch (provider.ConsumeIntegralInRange<ui8>(0, 5)) {
            case 0:
                node = MakeLeaf(ctx, provider, pos);
                break;
            case 1:
                node = ctx.NewList(pos, MakeChildren(ctx, provider, depth, saved));
                break;
            case 2: {
                const TStringBuf names[] = {"Add", "Member", "List", "Optional", "Coalesce", "If", "AsStruct", "Just"};
                node = ctx.NewCallable(
                    pos,
                    names[provider.ConsumeIntegralInRange<size_t>(0, sizeof(names) / sizeof(names[0]) - 1)],
                    MakeChildren(ctx, provider, depth, saved));
                break;
            }
            case 3: {
                NYql::TExprNode::TListType args;
                const ui32 argCount = provider.ConsumeIntegralInRange<ui32>(0, 4);
                args.reserve(argCount);
                for (ui32 i = 0; i < argCount; ++i) {
                    args.push_back(ctx.NewArgument(pos, SmallName(provider, "arg")));
                }
                auto argNode = ctx.NewArguments(pos, std::move(args));
                node = ctx.NewLambda(pos, std::move(argNode), MakeNode(ctx, provider, depth + 1, saved));
                break;
            }
            case 4:
                node = ctx.NewArguments(pos, MakeChildren(ctx, provider, depth, saved));
                break;
            default:
                node = ctx.NewWorld(pos);
                break;
        }
    }

    if (saved.size() < MaxSavedNodes) {
        saved.push_back(node);
    }
    return node;
}

void ExerciseNode(const NYql::TExprNode& node, NYql::TExprContext& ctx) {
    NYql::CheckArguments(node);
    NYql::CheckCounts(node);

    auto ast = NYql::ConvertToAst(node, ctx, 0, true);
    if (ast.Root) {
        const TString printed = ast.Root->ToString(
            NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote | NYql::TAstPrintFlags::AdaptArbitraryContent);
        auto reparsed = NYql::ParseAst(printed);
        if (reparsed.Root) {
            (void)reparsed.Root->ToString();
        }
    }

    const TString serialized = NYql::SerializeGraph(
        node,
        ctx,
        NYql::TSerializedExprGraphComponents::Graph | NYql::TSerializedExprGraphComponents::Positions);
    NYql::TExprContext roundTripCtx;
    auto deserialized = NYql::DeserializeGraph(NYql::TPosition{}, serialized, roundTripCtx);
    if (deserialized) {
        (void)NYql::SerializeGraph(*deserialized, roundTripCtx);
    }
}

void ExerciseProductionSerializedGraph(const NYql::TExprNode& node, NYql::TExprContext& ctx) {
    // EvaluateCode receives bytes from SerializeCode, which uses this exact graph envelope.
    const TString serialized = NYql::SerializeGraph(
        node,
        ctx,
        NYql::TSerializedExprGraphComponents::Graph | NYql::TSerializedExprGraphComponents::Positions);

    NYql::TExprContext deserializedCtx;
    auto deserialized = NYql::DeserializeGraph(NYql::TPosition{}, serialized, deserializedCtx);
    if (deserialized) {
        ExerciseNode(*deserialized, deserializedCtx);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    try {
        NYql::TExprContext ctx;
        FuzzedDataProvider provider(data, size);
        NYql::TExprNode::TListType saved;
        auto node = MakeNode(ctx, provider, 0, saved);
        ExerciseProductionSerializedGraph(*node, ctx);
    } catch (...) {
    }

    return 0;
}
