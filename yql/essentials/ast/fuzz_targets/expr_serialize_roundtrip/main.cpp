#include <yql/essentials/ast/serialize/yql_expr_serialize.h>
#include <yql/essentials/ast/yql_expr.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/yassert.h>

#include <iterator>

namespace {

using namespace NYql;

constexpr ui32 MaxDepth = 4;
constexpr ui32 MaxChildren = 4;
constexpr size_t MaxAtomSize = 48;
constexpr size_t MaxSavedNodes = 64;

TString SmallName(FuzzedDataProvider& provider, TStringBuf prefix) {
    TStringBuilder out;
    out << prefix;
    const size_t size = provider.ConsumeIntegralInRange<size_t>(0, 12);
    for (size_t i = 0; i < size; ++i) {
        const ui8 ch = provider.ConsumeIntegralInRange<ui8>(0, 37);
        if (ch < 10) {
            out << char('0' + ch);
        } else if (ch < 36) {
            out << char('a' + ch - 10);
        } else {
            out << '_';
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

TPosition Pos(FuzzedDataProvider& provider) {
    if (!provider.ConsumeBool()) {
        return {};
    }
    return TPosition(
        provider.ConsumeIntegralInRange<ui32>(1, 200),
        provider.ConsumeIntegralInRange<ui32>(1, 200),
        SmallName(provider, "f"));
}

TExprNode::TPtr MakeNode(
    TExprContext& ctx,
    FuzzedDataProvider& provider,
    ui32 depth,
    TExprNode::TListType& saved);

TExprNode::TListType MakeChildren(
    TExprContext& ctx,
    FuzzedDataProvider& provider,
    ui32 depth,
    ui32 minChildren = 0) {
    const ui32 count = provider.ConsumeIntegralInRange<ui32>(minChildren, MaxChildren);
    TExprNode::TListType children;
    children.reserve(count);
    for (ui32 i = 0; i < count; ++i) {
        children.push_back(MakeNode(ctx, provider, depth + 1, children));
    }
    return children;
}

TExprNode::TPtr MakeNode(
    TExprContext& ctx,
    FuzzedDataProvider& provider,
    ui32 depth,
    TExprNode::TListType& saved) {
    if (!saved.empty() && provider.ConsumeIntegralInRange<ui8>(0, 7) == 0) {
        return saved[provider.ConsumeIntegralInRange<size_t>(0, saved.size() - 1)];
    }

    const auto pos = Pos(provider);
    TExprNode::TPtr node;
    const ui8 kind = depth >= MaxDepth
        ? provider.ConsumeIntegralInRange<ui8>(0, 1)
        : provider.ConsumeIntegralInRange<ui8>(0, 5);

    switch (kind) {
        case 0: {
            ui32 flags = TNodeFlags::ArbitraryContent;
            if (provider.ConsumeBool()) {
                flags = provider.ConsumeBool() ? TNodeFlags::Default : TNodeFlags::BinaryContent;
            }
            node = ctx.NewAtom(pos, AtomContent(provider), flags);
            break;
        }
        case 1:
            node = ctx.NewWorld(pos);
            break;
        case 2:
            node = ctx.NewList(pos, MakeChildren(ctx, provider, depth));
            break;
        case 3: {
            const TString names[] = {"Add", "Member", "List", "Optional", "Coalesce", "If"};
            node = ctx.NewCallable(
                pos,
                names[provider.ConsumeIntegralInRange<size_t>(0, std::size(names) - 1)],
                MakeChildren(ctx, provider, depth));
            break;
        }
        case 4: {
            TExprNode::TListType args;
            const ui32 argCount = provider.ConsumeIntegralInRange<ui32>(0, 3);
            args.reserve(argCount);
            for (ui32 i = 0; i < argCount; ++i) {
                args.push_back(ctx.NewArgument(pos, SmallName(provider, "arg")));
            }
            auto argNode = ctx.NewArguments(pos, std::move(args));
            node = ctx.NewLambda(pos, std::move(argNode), MakeChildren(ctx, provider, depth, 1));
            break;
        }
        default:
            node = ctx.NewArgument(pos, SmallName(provider, "free"));
            break;
    }

    if (saved.size() < MaxSavedNodes) {
        saved.push_back(node);
    }
    return node;
}

void CheckSerializeRoundtrip(const TExprNode& root, TExprContext& ctx, ui16 components) {
    const TString serialized = SerializeGraph(root, ctx, components);

    TExprContext parsedCtx;
    auto parsed = DeserializeGraph(TPosition(1, 1, "fuzz"), serialized, parsedCtx);
    Y_ABORT_UNLESS(parsed);
    const TString serializedAgain = SerializeGraph(*parsed, parsedCtx, components);

    TExprContext parsedCtx2;
    auto parsedAgain = DeserializeGraph(TPosition(1, 1, "fuzz"), serializedAgain, parsedCtx2);
    Y_ABORT_UNLESS(parsedAgain);
    const TString serializedThird = SerializeGraph(*parsedAgain, parsedCtx2, components);

    Y_ABORT_UNLESS(serializedAgain == serializedThird);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    TExprContext ctx;
    TExprNode::TListType saved;
    auto root = MakeNode(ctx, provider, 0, saved);

    const ui16 components = provider.ConsumeBool()
        ? TSerializedExprGraphComponents::Graph
        : TSerializedExprGraphComponents::Graph | TSerializedExprGraphComponents::Positions;
    CheckSerializeRoundtrip(*root, ctx, components);

    return 0;
}
