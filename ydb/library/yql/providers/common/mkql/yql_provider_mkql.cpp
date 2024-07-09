#include "yql_provider_mkql.h"
#include "yql_type_mkql.h"

#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_match_recognize.h>

#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <util/stream/null.h>

#include <array>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

namespace NYql {
namespace NCommon {

TRuntimeNode WideTopImpl(const TExprNode& node, TMkqlBuildContext& ctx,
    TRuntimeNode(TProgramBuilder::*func)(TRuntimeNode, TRuntimeNode, const std::vector<std::pair<ui32, TRuntimeNode>>&)) {
    const auto flow = MkqlBuildExpr(node.Head(), ctx);
    const auto count = MkqlBuildExpr(*node.Child(1U), ctx);

    std::vector<std::pair<ui32, TRuntimeNode>> directions;
    directions.reserve(node.Tail().ChildrenSize());
    node.Tail().ForEachChild([&](const TExprNode& dir) {
        directions.emplace_back(std::make_pair(::FromString<ui32>(dir.Head().Content()), MkqlBuildExpr(dir.Tail(), ctx)));
    });

    return (ctx.ProgramBuilder.*func)(flow, count, directions);
}

TRuntimeNode WideSortImpl(const TExprNode& node, TMkqlBuildContext& ctx,
    TRuntimeNode(TProgramBuilder::*func)(TRuntimeNode, const std::vector<std::pair<ui32, TRuntimeNode>>&)) {
    const auto flow = MkqlBuildExpr(node.Head(), ctx);

    std::vector<std::pair<ui32, TRuntimeNode>> directions;
    directions.reserve(node.Tail().ChildrenSize());
    node.Tail().ForEachChild([&](const TExprNode& dir) {
        directions.emplace_back(std::make_pair(::FromString<ui32>(dir.Head().Content()), MkqlBuildExpr(dir.Tail(), ctx)));
    });

    return (ctx.ProgramBuilder.*func)(flow, directions);
}

TRuntimeNode CombineByKeyImpl(const TExprNode& node, TMkqlBuildContext& ctx) {
    NNodes::TCoCombineByKey combine(&node);
    const bool isStreamOrFlow = combine.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream ||
        combine.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow;

    YQL_ENSURE(!isStreamOrFlow);

    const auto input = MkqlBuildExpr(combine.Input().Ref(), ctx);

    TRuntimeNode preMapList = ctx.ProgramBuilder.FlatMap(input, [&](TRuntimeNode item) {
        return MkqlBuildLambda(combine.PreMapLambda().Ref(), ctx, {item});
    });

    const auto dict = ctx.ProgramBuilder.ToHashedDict(preMapList, true, [&](TRuntimeNode item) {
        return MkqlBuildLambda(combine.KeySelectorLambda().Ref(), ctx, {item});
    }, [&](TRuntimeNode item) {
        return item;
    });

    const auto values = ctx.ProgramBuilder.DictItems(dict);
    return ctx.ProgramBuilder.FlatMap(values, [&](TRuntimeNode item) {
        auto key = ctx.ProgramBuilder.Nth(item, 0);
        auto payloadList = ctx.ProgramBuilder.Nth(item, 1);
        auto fold1 = ctx.ProgramBuilder.Fold1(payloadList, [&](TRuntimeNode item2) {
            return MkqlBuildLambda(combine.InitHandlerLambda().Ref(), ctx, {key, item2});
        }, [&](TRuntimeNode item2, TRuntimeNode state) {
            return MkqlBuildLambda(combine.UpdateHandlerLambda().Ref(), ctx, {key, item2, state});
        });
        auto res = ctx.ProgramBuilder.FlatMap(fold1, [&](TRuntimeNode state) {
            return MkqlBuildLambda(combine.FinishHandlerLambda().Ref(), ctx, {key, state});
        });
        return res;
    });
}

namespace {

std::array<TRuntimeNode, 2U> MkqlBuildSplitLambda(const TExprNode& lambda, TMkqlBuildContext& ctx, const std::initializer_list<TRuntimeNode>& args) {
    TMkqlBuildContext::TArgumentsMap innerArguments;
    innerArguments.reserve(args.size());
    auto it = args.begin();
    lambda.Head().ForEachChild([&](const TExprNode& child){ innerArguments.emplace(&child, *it++); });
    TMkqlBuildContext innerCtx(ctx, std::move(innerArguments), lambda.UniqueId());
    const auto& body = lambda.Tail();
    MKQL_ENSURE(body.IsList() && body.ChildrenSize() == 2U, "Expected pair of nodes.");
    return {{MkqlBuildExpr(body.Head(), innerCtx), MkqlBuildExpr(body.Tail(), innerCtx)}};
}

TMkqlBuildContext* GetNodeContext(const TExprNode& node, TMkqlBuildContext& ctx) {
    for (auto currCtx = &ctx; currCtx; currCtx = currCtx->ParentCtx) {
        const auto knownNode = currCtx->Memoization.find(&node);
        if (currCtx->Memoization.cend() != knownNode) {
            return currCtx;
        }
    }
    return nullptr;
}

TMkqlBuildContext* GetNodeContextByLambda(const TExprNode& node, TMkqlBuildContext& ctx) {
    for (auto currCtx = &ctx; currCtx; currCtx = currCtx->ParentCtx) {
        if (currCtx->LambdaId == node.UniqueId()) {
            return currCtx;
        }
    }
    return nullptr;
}

TMkqlBuildContext* GetContextForMemoizeInUnknowScope(const TExprNode& node, TMkqlBuildContext& ctx) {
    TMkqlBuildContext* result = nullptr;
    for (const auto& c : node.Children()) {
        const auto& child = c->IsLambda() ? c->Tail() : *c;
        if (!child.IsAtom()) {
            auto nodeCtx = GetNodeContext(child, ctx);
            if (!nodeCtx) {
                nodeCtx = GetContextForMemoizeInUnknowScope(child, ctx);
            }

            if (!result || result->Level < nodeCtx->Level) {
                result = nodeCtx;
                if (result == &ctx) {
                    break;
                }
            }
        }
    }

    if (!result) {
        for (result = &ctx; result->ParentCtx; result = result->ParentCtx)
            continue;
    }

    return result;
}

TMkqlBuildContext* GetContextForMemoize(const TExprNode& node, TMkqlBuildContext& ctx) {
    if (const auto scope = node.GetDependencyScope()) {
        if (const auto lambda = scope->second) {
            return GetNodeContextByLambda(*lambda, ctx);
        }
    } else {
        return GetContextForMemoizeInUnknowScope(node, ctx);
    }

    auto result = &ctx;
    while (result->ParentCtx) {
        result = result->ParentCtx;
    }

    return result;
}

const TRuntimeNode& CheckTypeAndMemoize(const TExprNode& node, TMkqlBuildContext& ctx, const TRuntimeNode& runtime) {
    if (node.GetTypeAnn()) {
        TNullOutput null;
        if (const auto type = BuildType(*node.GetTypeAnn(), ctx.ProgramBuilder, null)) {
            if (!type->IsSameType(*runtime.GetStaticType())) {
                ythrow TNodeException(node) << "Expected: " << *type << " type, but got: "  << *runtime.GetStaticType() << ".";
            }
        }
    }

    return GetContextForMemoize(node, ctx)->Memoization.emplace(&node, runtime).first->second;
}

std::vector<TRuntimeNode> GetAllArguments(const TExprNode& node, TMkqlBuildContext& ctx) {
    std::vector<TRuntimeNode> args;
    args.reserve(node.ChildrenSize());
    node.ForEachChild([&](const TExprNode& child){ args.emplace_back(MkqlBuildExpr(child, ctx)); });
    return args;
}

template <size_t From>
std::vector<TRuntimeNode> GetArgumentsFrom(const TExprNode& node, TMkqlBuildContext& ctx) {
    std::vector<TRuntimeNode> args;
    args.reserve(node.ChildrenSize() - From);
    for (auto i = From; i < node.ChildrenSize(); ++i) {
        args.emplace_back(MkqlBuildExpr(*node.Child(i), ctx));
    }
    return args;
}

NUdf::TDataTypeId ParseDataType(const TExprNode& owner, const std::string_view& type) {
    if (const auto slot = NUdf::FindDataSlot(type)) {
        return NUdf::GetDataTypeInfo(*slot).TypeId;
    }

    ythrow TNodeException(owner) << "Unsupported data type: " << type;
}

EJoinKind GetJoinKind(const TExprNode& owner, const std::string_view& content) {
    if (content == "Inner") {
        return EJoinKind::Inner;
    }
    else if (content == "Left") {
        return EJoinKind::Left;
    }
    else if (content == "Right") {
        return EJoinKind::Right;
    }
    else if (content == "Full") {
        return EJoinKind::Full;
    }
    else if (content == "LeftOnly") {
        return EJoinKind::LeftOnly;
    }
    else if (content == "RightOnly") {
        return EJoinKind::RightOnly;
    }
    else if (content == "Exclusion") {
        return EJoinKind::Exclusion;
    }
    else if (content == "LeftSemi") {
        return EJoinKind::LeftSemi;
    }
    else if (content == "RightSemi") {
        return EJoinKind::RightSemi;
    }
    else if (content == "Cross") {
        return EJoinKind::Cross;
    }
    else {
        ythrow TNodeException(owner) << "Unexpected join kind: " << content;
    }
}

template<typename TLayout>
std::pair<TLayout, ui16> CutTimezone(const std::string_view& atom) {
    const auto pos = atom.find(',');
    MKQL_ENSURE(std::string_view::npos != pos, "Expected two components.");
    return std::make_pair(::FromString<TLayout>(atom.substr(0, pos)), GetTimezoneId(atom.substr(pos + 1)));
}

} // namespace

bool TMkqlCallableCompilerBase::HasCallable(const std::string_view& name) const {
    return Callables.contains(name);
}

void TMkqlCallableCompilerBase::AddCallable(const std::string_view& name, TCompiler compiler) {
    const auto result = Callables.emplace(TString(name), compiler);
    YQL_ENSURE(result.second, "Callable already exists: " << name);
}

void TMkqlCallableCompilerBase::AddCallable(const std::initializer_list<std::string_view>& names, TCompiler compiler) {
    for (const auto& name : names) {
        AddCallable(name, compiler);
    }
}

void TMkqlCallableCompilerBase::ChainCallable(const std::string_view& name, TCompiler compiler) {
    auto prevCompiler = GetCallable(name);
    auto chainedCompiler = [compiler = std::move(compiler), prevCompiler = std::move(prevCompiler)](const TExprNode& node, TMkqlBuildContext& ctx) -> NKikimr::NMiniKQL::TRuntimeNode {
        if (auto res = compiler(node, ctx)) {
            return res;
        }
        return prevCompiler(node, ctx);
    };
    OverrideCallable(name, chainedCompiler);
}

void TMkqlCallableCompilerBase::ChainCallable(const std::initializer_list<std::string_view>& names, TCompiler compiler) {
    for (const auto& name : names) {
        ChainCallable(name, compiler);
    }
}

void TMkqlCallableCompilerBase::AddSimpleCallables(const std::initializer_list<std::pair<std::string_view, TProgramBuilder::UnaryFunctionMethod>>& callables) {
    for (const auto& callable : callables) {
        AddCallable(callable.first,
            [method=callable.second](const TExprNode& node, TMkqlBuildContext& ctx) {
                const auto arg = MkqlBuildExpr(node.Head(), ctx);
                return (ctx.ProgramBuilder.*method)(arg);
            }
        );
    }
}

void TMkqlCallableCompilerBase::AddSimpleCallables(const std::initializer_list<std::pair<std::string_view, TProgramBuilder::BinaryFunctionMethod>>& callables) {
    for (const auto& callable : callables) {
        AddCallable(callable.first,
            [method=callable.second](const TExprNode& node, TMkqlBuildContext& ctx) {
                const auto one = MkqlBuildExpr(node.Head(), ctx);
                const auto two = MkqlBuildExpr(node.Tail(), ctx);
                return (ctx.ProgramBuilder.*method)(one, two);
            }
        );
    }
}

void TMkqlCallableCompilerBase::AddSimpleCallables(const std::initializer_list<std::pair<std::string_view, TProgramBuilder::TernaryFunctionMethod>>& callables) {
    for (const auto& callable : callables) {
        AddCallable(callable.first,
            [method=callable.second](const TExprNode& node, TMkqlBuildContext& ctx) {
                const auto arg1 = MkqlBuildExpr(node.Head(), ctx);
                const auto arg2 = MkqlBuildExpr(*node.Child(1U), ctx);
                const auto arg3 = MkqlBuildExpr(node.Tail(), ctx);
                return (ctx.ProgramBuilder.*method)(arg1, arg2, arg3);
            }
        );
    }
}

void TMkqlCallableCompilerBase::AddSimpleCallables(const std::initializer_list<std::pair<std::string_view, TProgramBuilder::ArrayFunctionMethod>>& callables) {
    for (const auto& callable : callables) {
        AddCallable(callable.first,
            [method=callable.second](const TExprNode& node, TMkqlBuildContext& ctx) {
                const auto& args = GetAllArguments(node, ctx);
                return (ctx.ProgramBuilder.*method)(args);
            }
        );
    }
}

void TMkqlCallableCompilerBase::AddSimpleCallables(const std::initializer_list<std::pair<std::string_view, TProgramBuilder::ProcessFunctionMethod>>& callables) {
    for (const auto& callable : callables) {
        AddCallable(callable.first,
            [method=callable.second](const TExprNode& node, TMkqlBuildContext& ctx) {
                const auto arg = MkqlBuildExpr(node.Head(), ctx);
                const auto lambda = [&](TRuntimeNode item) { return MkqlBuildLambda(node.Tail(), ctx, {item}); };
                return (ctx.ProgramBuilder.*method)(arg, lambda);
            }
        );
    }
}

void TMkqlCallableCompilerBase::AddSimpleCallables(const std::initializer_list<std::pair<std::string_view, TProgramBuilder::NarrowFunctionMethod>>& callables) {
    for (const auto& callable : callables) {
        AddCallable(callable.first,
            [method=callable.second](const TExprNode& node, TMkqlBuildContext& ctx) {
                const auto arg = MkqlBuildExpr(node.Head(), ctx);
                const auto lambda = [&](TRuntimeNode::TList items) { return MkqlBuildLambda(node.Tail(), ctx, items); };
                return (ctx.ProgramBuilder.*method)(arg, lambda);
            }
        );
    }
}

void TMkqlCallableCompilerBase::OverrideCallable(const std::string_view& name, TCompiler compiler) {
    const auto prevCompiler = Callables.find(name);
    YQL_ENSURE(Callables.cend() != prevCompiler, "Missed callable: " << name);
    prevCompiler->second = compiler;
    Callables[name] = compiler;
}

IMkqlCallableCompiler::TCompiler TMkqlCallableCompilerBase::GetCallable(const std::string_view& name) const {
    const auto compiler = Callables.find(name);
    YQL_ENSURE(Callables.cend() != compiler, "Missed callable: " << name);
    return compiler->second;
}

IMkqlCallableCompiler::TCompiler TMkqlCallableCompilerBase::FindCallable(const std::string_view& name) const {
    const auto compiler = Callables.find(name);
    return Callables.cend() != compiler ? compiler->second : IMkqlCallableCompiler::TCompiler();
}

bool TMkqlCommonCallableCompiler::HasCallable(const std::string_view& name) const {
    if (TMkqlCallableCompilerBase::HasCallable(name)) {
        return true;
    }

    return GetShared().HasCallable(name);
}

IMkqlCallableCompiler::TCompiler TMkqlCommonCallableCompiler::FindCallable(const std::string_view& name) const {
    if (const auto func = TMkqlCallableCompilerBase::FindCallable(name)) {
        return func;
    }

    return GetShared().FindCallable(name);
}

IMkqlCallableCompiler::TCompiler TMkqlCommonCallableCompiler::GetCallable(const std::string_view& name) const {
    if (const auto func = TMkqlCallableCompilerBase::FindCallable(name)) {
        return func;
    }

    return GetShared().GetCallable(name);
}

void TMkqlCommonCallableCompiler::OverrideCallable(const std::string_view& name, TCompiler compiler) {
    if (TMkqlCallableCompilerBase::HasCallable(name)) {
        TMkqlCallableCompilerBase::OverrideCallable(name, compiler);
    } else {
        YQL_ENSURE(GetShared().HasCallable(name));
        TMkqlCallableCompilerBase::AddCallable(name, compiler);
    }
}

void TMkqlCommonCallableCompiler::AddCallable(const std::string_view& name, TCompiler compiler) {
    YQL_ENSURE(!GetShared().HasCallable(name), "Compiler already set for callable: " << name);
    TMkqlCallableCompilerBase::AddCallable(name, compiler);
}

void TMkqlCommonCallableCompiler::AddCallable(const std::initializer_list<std::string_view>& names, TCompiler compiler) {
    for (const auto& name : names) {
        AddCallable(name, compiler);
    }
}

TMkqlCommonCallableCompiler::TShared::TShared() {
    AddSimpleCallables({
        {"Abs", &TProgramBuilder::Abs},
        {"Plus", &TProgramBuilder::Plus},
        {"Minus", &TProgramBuilder::Minus},

        {"Inc", &TProgramBuilder::Increment},
        {"Dec", &TProgramBuilder::Decrement},
        {"Not", &TProgramBuilder::Not},
        {"BlockNot", &TProgramBuilder::BlockNot},
        {"BlockJust", &TProgramBuilder::BlockJust},

        {"BitNot", &TProgramBuilder::BitNot},

        {"Size", &TProgramBuilder::Size},

        {"Way", &TProgramBuilder::Way},
        {"VariantItem", &TProgramBuilder::VariantItem},

        {"CountBits", &TProgramBuilder::CountBits},

        {"Ascending", &TProgramBuilder::Ascending},
        {"Descending", &TProgramBuilder::Descending},

        {"ToOptional", &TProgramBuilder::Head},
        {"Head", &TProgramBuilder::Head},
        {"Last", &TProgramBuilder::Last},

        {"ToList", &TProgramBuilder::ToList},
        {"ToFlow", &TProgramBuilder::ToFlow},
        {"FromFlow", &TProgramBuilder::FromFlow},

        {"WideToBlocks", &TProgramBuilder::WideToBlocks},
        {"WideFromBlocks", &TProgramBuilder::WideFromBlocks},
        {"AsScalar", &TProgramBuilder::AsScalar},

        {"Just", &TProgramBuilder::NewOptional},
        {"Exists", &TProgramBuilder::Exists},
        {"BlockExists", &TProgramBuilder::BlockExists},

        {"Pickle", &TProgramBuilder::Pickle},
        {"StablePickle", &TProgramBuilder::StablePickle},

        {"Collect", &TProgramBuilder::Collect},
        {"Discard", &TProgramBuilder::Discard},
        {"LazyList", &TProgramBuilder::LazyList},
        {"ForwardList", &TProgramBuilder::ForwardList},

        {"Length", &TProgramBuilder::Length},
        {"HasItems", &TProgramBuilder::HasItems},

        {"Reverse", &TProgramBuilder::Reverse},
        {"ToIndexDict", &TProgramBuilder::ToIndexDict},

        {"ToString", &TProgramBuilder::ToString},
        {"ToBytes", &TProgramBuilder::ToBytes},

        {"AggrCountInit", &TProgramBuilder::AggrCountInit},

        {"NewMTRand", &TProgramBuilder::NewMTRand},
        {"NextMTRand", &TProgramBuilder::NextMTRand},

        {"TimezoneId", &TProgramBuilder::TimezoneId},
        {"TimezoneName", &TProgramBuilder::TimezoneName},
        {"RemoveTimezone", &TProgramBuilder::RemoveTimezone},

        {"DictItems", &TProgramBuilder::DictItems},
        {"DictKeys", &TProgramBuilder::DictKeys},
        {"DictPayloads", &TProgramBuilder::DictPayloads},

        {"QueuePop", &TProgramBuilder::QueuePop}
    });

    AddSimpleCallables({
        {"+", &TProgramBuilder::Add},
        {"-", &TProgramBuilder::Sub},
        {"*", &TProgramBuilder::Mul},
        {"/", &TProgramBuilder::Div},
        {"%", &TProgramBuilder::Mod},

        {"Add", &TProgramBuilder::Add},
        {"Sub", &TProgramBuilder::Sub},
        {"Mul", &TProgramBuilder::Mul},
        {"Div", &TProgramBuilder::Div},
        {"Mod", &TProgramBuilder::Mod},

        {"DecimalMul", &TProgramBuilder::DecimalMul},
        {"DecimalDiv", &TProgramBuilder::DecimalDiv},
        {"DecimalMod", &TProgramBuilder::DecimalMod},

        {"==", &TProgramBuilder::Equals},
        {"!=", &TProgramBuilder::NotEquals},
        {"<", &TProgramBuilder::Less},
        {"<=", &TProgramBuilder::LessOrEqual},
        {">", &TProgramBuilder::Greater},
        {">=", &TProgramBuilder::GreaterOrEqual},

        {"Equals", &TProgramBuilder::Equals},
        {"NotEquals", &TProgramBuilder::NotEquals},
        {"Less", &TProgramBuilder::Less},
        {"LessOrEqual", &TProgramBuilder::LessOrEqual},
        {"Greater", &TProgramBuilder::Greater},
        {"GreaterOrEqual", &TProgramBuilder::GreaterOrEqual},

        {"AggrEquals", &TProgramBuilder::AggrEquals},
        {"AggrNotEquals", &TProgramBuilder::AggrNotEquals},
        {"AggrLess", &TProgramBuilder::AggrLess},
        {"AggrLessOrEqual", &TProgramBuilder::AggrLessOrEqual},
        {"AggrGreater", &TProgramBuilder::AggrGreater},
        {"AggrGreaterOrEqual", &TProgramBuilder::AggrGreaterOrEqual},

        {"AggrMin", &TProgramBuilder::AggrMin},
        {"AggrMax", &TProgramBuilder::AggrMax},
        {"AggrAdd", &TProgramBuilder::AggrAdd},

        {"AggrCountUpdate", &TProgramBuilder::AggrCountUpdate},

        {"BitOr", &TProgramBuilder::BitOr},
        {"BitAnd", &TProgramBuilder::BitAnd},
        {"BitXor", &TProgramBuilder::BitXor},

        {"ShiftLeft", &TProgramBuilder::ShiftLeft},
        {"ShiftRight", &TProgramBuilder::ShiftRight},
        {"RotLeft", &TProgramBuilder::RotLeft},
        {"RotRight", &TProgramBuilder::RotRight},

        {"ListIf", &TProgramBuilder::ListIf},

        {"Concat", &TProgramBuilder::Concat},
        {"AggrConcat", &TProgramBuilder::AggrConcat},
        {"ByteAt", &TProgramBuilder::ByteAt},
        {"Nanvl", &TProgramBuilder::Nanvl},

        {"Skip", &TProgramBuilder::Skip},
        {"Take", &TProgramBuilder::Take},
        {"Limit", &TProgramBuilder::Take},

        {"WideTakeBlocks", &TProgramBuilder::WideTakeBlocks},
        {"WideSkipBlocks", &TProgramBuilder::WideSkipBlocks},
        {"BlockCoalesce", &TProgramBuilder::BlockCoalesce},
        {"ReplicateScalar", &TProgramBuilder::ReplicateScalar},

        {"BlockAnd", &TProgramBuilder::BlockAnd},
        {"BlockOr", &TProgramBuilder::BlockOr},
        {"BlockXor", &TProgramBuilder::BlockXor},

        {"Append", &TProgramBuilder::Append},
        {"Insert", &TProgramBuilder::Append},
        {"Prepend", &TProgramBuilder::Prepend},

        {"Lookup", &TProgramBuilder::Lookup},
        {"Contains", &TProgramBuilder::Contains},

        {"AddTimezone", &TProgramBuilder::AddTimezone},

        {"StartsWith", &TProgramBuilder::StartsWith},
        {"EndsWith", &TProgramBuilder::EndsWith},
        {"StringContains", &TProgramBuilder::StringContains},

        {"SqueezeToList", &TProgramBuilder::SqueezeToList},

        {"QueuePush", &TProgramBuilder::QueuePush}
    });

    AddSimpleCallables({
        {"Substring", &TProgramBuilder::Substring},
        {"Find", &TProgramBuilder::Find},
        {"RFind", &TProgramBuilder::RFind},

        {"ListFromRange", &TProgramBuilder::ListFromRange},

        {"PreserveStream", &TProgramBuilder::PreserveStream},

        {"BlockIf", &TProgramBuilder::BlockIf},
    });

    AddSimpleCallables({
        {"If", &TProgramBuilder::If},

        {"Or", &TProgramBuilder::Or},
        {"And", &TProgramBuilder::And},
        {"Xor", &TProgramBuilder::Xor},

        {"Min", &TProgramBuilder::Min},
        {"Max", &TProgramBuilder::Max},

        {"AsList", &TProgramBuilder::AsList},

        {"Extend", &TProgramBuilder::Extend},
        {"OrderedExtend", &TProgramBuilder::OrderedExtend},

        {"Zip", &TProgramBuilder::Zip},
        {"ZipAll", &TProgramBuilder::ZipAll},

        {"Random", &TProgramBuilder::Random},
        {"RandomNumber", &TProgramBuilder::RandomNumber},
        {"RandomUuid", &TProgramBuilder::RandomUuid},

        {"Now", &TProgramBuilder::Now},
        {"CurrentUtcDate", &TProgramBuilder::CurrentUtcDate},
        {"CurrentUtcDatetime", &TProgramBuilder::CurrentUtcDatetime},
        {"CurrentUtcTimestamp", &TProgramBuilder::CurrentUtcTimestamp},
    });

    AddSimpleCallables({
        {"Map", &TProgramBuilder::Map},
        {"OrderedMap", &TProgramBuilder::OrderedMap},

        {"FlatMap", &TProgramBuilder::FlatMap},
        {"OrderedFlatMap", &TProgramBuilder::OrderedFlatMap},

        {"SkipWhile", &TProgramBuilder::SkipWhile},
        {"TakeWhile", &TProgramBuilder::TakeWhile},

        {"SkipWhileInclusive", &TProgramBuilder::SkipWhileInclusive},
        {"TakeWhileInclusive", &TProgramBuilder::TakeWhileInclusive},
    });

    AddSimpleCallables({
        {"NarrowMap", &TProgramBuilder::NarrowMap},
        {"NarrowFlatMap", &TProgramBuilder::NarrowFlatMap},

        {"WideSkipWhile", &TProgramBuilder::WideSkipWhile},
        {"WideTakeWhile", &TProgramBuilder::WideTakeWhile},

        {"WideSkipWhileInclusive", &TProgramBuilder::WideSkipWhileInclusive},
        {"WideTakeWhileInclusive", &TProgramBuilder::WideTakeWhileInclusive},
    });

    AddSimpleCallables({
        {"RangeUnion", &TProgramBuilder::RangeUnion},
        {"RangeIntersect", &TProgramBuilder::RangeIntersect},
        {"RangeMultiply", &TProgramBuilder::RangeMultiply},
    });

    AddSimpleCallables({
        {"RangeCreate", &TProgramBuilder::RangeCreate},
        {"RangeFinalize", &TProgramBuilder::RangeFinalize},
    });

    AddCallable({"RoundUp", "RoundDown"}, [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto dstType = BuildType(node.Tail(), *node.Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.Round(node.Content(), arg, dstType);
    });

    AddSimpleCallables({
        {"NextValue", &TProgramBuilder::NextValue},
    });

    AddCallable({"MultiMap", "OrderedMultiMap"}, [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto lambda = [&](TRuntimeNode item) { return MkqlBuildWideLambda(node.Tail(), ctx, {item}); };
        return ctx.ProgramBuilder.MultiMap(arg, lambda);
    });

    AddCallable("ExpandMap", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto lambda = [&](TRuntimeNode item) { return MkqlBuildWideLambda(node.Tail(), ctx, {item}); };
        return ctx.ProgramBuilder.ExpandMap(arg, lambda);
    });

    AddCallable("WideMap", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto lambda = [&](TRuntimeNode::TList items) { return MkqlBuildWideLambda(node.Tail(), ctx, items); };
        TRuntimeNode result = ctx.ProgramBuilder.WideMap(arg, lambda);
        if (IsWideBlockType(*node.GetTypeAnn()->Cast<TFlowExprType>()->GetItemType())) {
            result = ctx.ProgramBuilder.BlockExpandChunked(result);
        }
        return result;
    });

    AddCallable("WideChain1Map", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto flow = MkqlBuildExpr(node.Head(), ctx);
        return ctx.ProgramBuilder.WideChain1Map(flow,
        [&](TRuntimeNode::TList items) {
            return MkqlBuildWideLambda(*node.Child(1), ctx, items);
        },
        [&](TRuntimeNode::TList items, TRuntimeNode::TList state) {
            items.insert(items.cend(), state.cbegin(), state.cend());
            return MkqlBuildWideLambda(node.Tail(), ctx, items);
        });
    });

    AddCallable("NarrowMultiMap", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto lambda = [&](TRuntimeNode::TList items) { return MkqlBuildWideLambda(node.Tail(), ctx, items); };
        return ctx.ProgramBuilder.NarrowMultiMap(arg, lambda);
    });

    AddCallable("WideFilter", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto lambda = [&](TRuntimeNode::TList items) { return MkqlBuildLambda(*node.Child(1), ctx, items); };
        if (node.ChildrenSize() > 2U) {
            const auto limit = MkqlBuildExpr(node.Tail(), ctx);
            return ctx.ProgramBuilder.WideFilter(arg, limit, lambda);
        } else {
            return ctx.ProgramBuilder.WideFilter(arg, lambda);
        }
    });

    AddCallable("WideCondense1", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto flow = MkqlBuildExpr(node.Head(), ctx);
        return ctx.ProgramBuilder.WideCondense1(flow,
        [&](TRuntimeNode::TList items) {
            return MkqlBuildWideLambda(*node.Child(1), ctx, items);
        },
        [&](TRuntimeNode::TList items, TRuntimeNode::TList state) {
            items.insert(items.cend(), state.cbegin(), state.cend());
            return MkqlBuildLambda(*node.Child(2), ctx, items);
        },
        [&](TRuntimeNode::TList items, TRuntimeNode::TList state) {
            items.insert(items.cend(), state.cbegin(), state.cend());
            return MkqlBuildWideLambda(*node.Child(3), ctx, items);
        },
        HasContextFuncs(*node.Child(1)) || HasContextFuncs(*node.Child(3)));
    });

    AddCallable("WideCombiner", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto flow = MkqlBuildExpr(node.Head(), ctx);
        i64 memLimit = 0LL;
        const bool withLimit = TryFromString<i64>(node.Child(1U)->Content(), memLimit);

        const auto keyExtractor = [&](TRuntimeNode::TList items) {
            return MkqlBuildWideLambda(*node.Child(2U), ctx, items);
        };
        const auto init = [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) {
            keys.insert(keys.cend(), items.cbegin(), items.cend());
            return MkqlBuildWideLambda(*node.Child(3U), ctx, keys);
        };
        const auto update = [&](TRuntimeNode::TList keys, TRuntimeNode::TList items, TRuntimeNode::TList state) {
            keys.insert(keys.cend(), items.cbegin(), items.cend());
            keys.insert(keys.cend(), state.cbegin(), state.cend());
            return MkqlBuildWideLambda(*node.Child(4U), ctx, keys);
        };
        const auto finish = [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) {
            keys.insert(keys.cend(), state.cbegin(), state.cend());
            return MkqlBuildWideLambda(node.Tail(), ctx, keys);
        };

        bool isStatePersistable = true;
        // Traverse through childs skipping input and limit children
        for (size_t i = 2U; i < node.ChildrenSize(); ++i) {
            isStatePersistable = isStatePersistable && node.Child(i)->GetTypeAnn()->IsPersistable();
        }

        if (withLimit) {
            return ctx.ProgramBuilder.WideCombiner(flow, memLimit, keyExtractor, init, update, finish);
        }

        if (isStatePersistable && RuntimeVersion >= 49U) {
            return ctx.ProgramBuilder.WideLastCombinerWithSpilling(flow, keyExtractor, init, update, finish);
        }
        return ctx.ProgramBuilder.WideLastCombiner(flow, keyExtractor, init, update, finish);
    });

    AddCallable("WideChopper", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto flow = MkqlBuildExpr(node.Head(), ctx);

        const auto keyExtractor = [&](TRuntimeNode::TList items) {
            return MkqlBuildWideLambda(*node.Child(1U), ctx, items);
        };

        const auto groupSwitch = [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) {
            keys.insert(keys.cend(), items.cbegin(), items.cend());
            return MkqlBuildLambda(*node.Child(2U), ctx, keys);
        };

        const auto handler = [&](TRuntimeNode::TList keys, TRuntimeNode flow) {
            keys.emplace_back(flow);
            return MkqlBuildLambda(node.Tail(), ctx, keys);
        };

        return ctx.ProgramBuilder.WideChopper(flow, keyExtractor, groupSwitch, handler);
    });

    AddCallable("WideTop", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return WideTopImpl(node, ctx, &TProgramBuilder::WideTop);
    });

    AddCallable("WideTopSort", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return WideTopImpl(node, ctx, &TProgramBuilder::WideTopSort);
    });

    AddCallable("WideSort", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return WideSortImpl(node, ctx, &TProgramBuilder::WideSort);
    });

    AddCallable("WideTopBlocks", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return WideTopImpl(node, ctx, &TProgramBuilder::WideTopBlocks);
    });

    AddCallable("WideTopSortBlocks", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return WideTopImpl(node, ctx, &TProgramBuilder::WideTopSortBlocks);
    });

    AddCallable("WideSortBlocks", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return WideSortImpl(node, ctx, &TProgramBuilder::WideSortBlocks);
    });

    AddCallable("Iterable", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto lambda = [&]() { return MkqlBuildLambda(node.Head(), ctx, {}); };
        return ctx.ProgramBuilder.Iterable(lambda);
    });

    AddCallable("Filter", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto lambda = [&](TRuntimeNode item) { return MkqlBuildLambda(*node.Child(1), ctx, {item}); };
        if (node.ChildrenSize() > 2U) {
            const auto limit = MkqlBuildExpr(node.Tail(), ctx);
            return ctx.ProgramBuilder.Filter(arg, limit, lambda);
        } else {
            return ctx.ProgramBuilder.Filter(arg, lambda);
        }
    });

    AddCallable("OrderedFilter", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto lambda = [&](TRuntimeNode item) { return MkqlBuildLambda(*node.Child(1), ctx, {item}); };
        if (node.ChildrenSize() > 2U) {
            const auto limit = MkqlBuildExpr(node.Tail(), ctx);
            return ctx.ProgramBuilder.OrderedFilter(arg, limit, lambda);
        } else {
            return ctx.ProgramBuilder.OrderedFilter(arg, lambda);
        }
    });

    AddCallable("Member", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto structObj = MkqlBuildExpr(node.Head(), ctx);
        const auto name = node.Tail().Content();
        return ctx.ProgramBuilder.Member(structObj, name);
    });

    AddCallable("RemoveMember", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto structObj = MkqlBuildExpr(node.Head(), ctx);
        const auto name = node.Tail().Content();
        return ctx.ProgramBuilder.RemoveMember(structObj, name, false);
    });

    AddCallable("ForceRemoveMember", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto structObj = MkqlBuildExpr(node.Head(), ctx);
        const auto name = node.Tail().Content();
        return ctx.ProgramBuilder.RemoveMember(structObj, name, true);
    });

    AddCallable("Nth", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto tupleObj = MkqlBuildExpr(node.Head(), ctx);
        const auto index = FromString<ui32>(node.Tail().Content());
        return ctx.ProgramBuilder.Nth(tupleObj, index);
    });

    AddCallable("MatchRecognizeCore", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto& inputStream = node.Child(0);
        const auto& partitionKeySelector = node.Child(1);
        const auto& partitionColumns = node.Child(2);
        const auto& params = node.Child(3);
        const auto& settings = node.Child(4);

        //explore params
        const auto& measures = params->ChildRef(0);
        const auto& pattern = params->ChildRef(3);
        const auto& defines = params->ChildRef(4);

        //explore measures
        const auto measureNames = measures->ChildRef(2);
        constexpr size_t FirstMeasureLambdaIndex = 3;

        //explore defines
        const auto defineNames = defines->ChildRef(2);
        const size_t FirstDefineLambdaIndex = 3;

        TVector<TStringBuf> partitionColumnNames;
        for (const auto& n: partitionColumns->Children()) {
            partitionColumnNames.push_back(n->Content());
        }

        TProgramBuilder::TUnaryLambda getPartitionKeySelector = [partitionKeySelector, &ctx](TRuntimeNode inputRowArg){
            return MkqlBuildLambda(*partitionKeySelector, ctx, {inputRowArg});
        };

        TVector<std::pair<TStringBuf, TProgramBuilder::TTernaryLambda>> getDefines(defineNames->ChildrenSize());
        for (size_t i = 0; i != defineNames->ChildrenSize(); ++i) {
            getDefines[i] = std::pair{
                    defineNames->ChildRef(i)->Content(),
                    [i, defines, &ctx](TRuntimeNode data, TRuntimeNode matchedVars, TRuntimeNode rowIndex) {
                        return MkqlBuildLambda(*defines->ChildRef(FirstDefineLambdaIndex + i), ctx,
                                               {data, matchedVars, rowIndex});
                    }
            };
        }

        TVector<std::pair<TStringBuf, TProgramBuilder::TBinaryLambda>> getMeasures(measureNames->ChildrenSize());
        for (size_t i = 0; i != measureNames->ChildrenSize(); ++i) {
            getMeasures[i] = std::pair{
                    measureNames->ChildRef(i)->Content(),
                    [i, measures, &ctx](TRuntimeNode data, TRuntimeNode matchedVars) {
                        return MkqlBuildLambda(*measures->ChildRef(FirstMeasureLambdaIndex + i), ctx,
                                               {data, matchedVars});
                    }
            };
        }

        const auto streamingMode = FromString<bool>(settings->Child(0)->Child(1)->Content());

        return ctx.ProgramBuilder.MatchRecognizeCore(
                MkqlBuildExpr(*inputStream, ctx),
                getPartitionKeySelector,
                partitionColumnNames,
                getMeasures,
                NYql::NMatchRecognize::ConvertPattern(pattern, ctx.ExprCtx),
                getDefines,
                streamingMode
                );
    });

    AddCallable("TimeOrderRecover", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto inputStream = node.Child(0);
        const auto timeExtractor = node.Child(1);
        const auto delay = node.Child(2);
        const auto ahead = node.Child(3);
        const auto rowLimit = node.Child(4);
        return ctx.ProgramBuilder.TimeOrderRecover(
            MkqlBuildExpr(*inputStream, ctx),
            [timeExtractor, &ctx](TRuntimeNode row) {
                return MkqlBuildLambda(*timeExtractor, ctx, {row});
            },
            MkqlBuildExpr(*delay, ctx),
            MkqlBuildExpr(*ahead, ctx),
            MkqlBuildExpr(*rowLimit, ctx)
        );
    });

    AddCallable("Guess", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto variantObj = MkqlBuildExpr(node.Head(), ctx);
        auto type = node.Head().GetTypeAnn();
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }

        auto varType = type->Cast<TVariantExprType>();
        if (varType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
            auto index = FromString<ui32>(node.Child(1)->Content());
            return ctx.ProgramBuilder.Guess(variantObj, index);
        } else {
            return ctx.ProgramBuilder.Guess(variantObj, node.Child(1)->Content());
        }
    });

    AddCallable("Visit", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto variantObj = MkqlBuildExpr(node.Head(), ctx);
        const auto type = node.Head().GetTypeAnn()->Cast<TVariantExprType>();
        const TTupleExprType* tupleType = nullptr;
        const TStructExprType* structType = nullptr;
        std::vector<TExprNode*> lambdas;
        TRuntimeNode defaultValue;
        if (type->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
            tupleType = type->GetUnderlyingType()->Cast<TTupleExprType>();
            lambdas.resize(tupleType->GetSize());
        } else {
            structType = type->GetUnderlyingType()->Cast<TStructExprType>();
            lambdas.resize(structType->GetSize());
        }

        for (ui32 index = 1; index < node.ChildrenSize(); ++index) {
            const auto child = node.Child(index);
            if (!child->IsAtom()) {
                defaultValue = MkqlBuildExpr(*child, ctx);
                continue;
            }

            ui32 itemIndex;
            if (tupleType) {
                itemIndex = FromString<ui32>(child->Content());
            } else {
                itemIndex = *structType->FindItem(child->Content());
            }

            YQL_ENSURE(itemIndex < lambdas.size());
            ++index;
            lambdas[itemIndex] = node.Child(index);
        }

        const auto handler = [&](ui32 index, TRuntimeNode item) {
            if (const auto lambda = lambdas[index]) {
                return MkqlBuildLambda(*lambda, ctx, {item});
            }
            return defaultValue;
        };

        return ctx.ProgramBuilder.VisitAll(variantObj, handler);
    });

    AddCallable("CurrentActorId", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto retType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), node.Content(), retType);
        return TRuntimeNode(call.Build(), false);
    });

    AddCallable("Uint8", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral(FromString<ui8>(node.Head(), NUdf::EDataSlot::Uint8));
    });

    AddCallable("Int8", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral(FromString<i8>(node.Head(), NUdf::EDataSlot::Int8));
    });

    AddCallable("Uint16", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral(FromString<ui16>(node.Head(), NUdf::EDataSlot::Uint16));
    });

    AddCallable("Int16", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral(FromString<i16>(node.Head(), NUdf::EDataSlot::Int16));
    });

    AddCallable("Int32", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral(FromString<i32>(node.Head(), NUdf::EDataSlot::Int32));
    });

    AddCallable("Uint32", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral(FromString<ui32>(node.Head(), NUdf::EDataSlot::Uint32));
    });

    AddCallable("Int64", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral(FromString<i64>(node.Head(), NUdf::EDataSlot::Int64));
    });

    AddCallable("Uint64", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral(FromString<ui64>(node.Head(), NUdf::EDataSlot::Uint64));
    });

    AddCallable("String", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(node.Head().Content());
    });

    AddCallable("Utf8", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Utf8>(node.Head().Content());
    });

    AddCallable("Yson", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Yson>(node.Head().Content());
    });

    AddCallable("Json", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Json>(node.Head().Content());
    });

    AddCallable("JsonDocument", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        // NOTE: ValueFromString returns TUnboxedValuePod. This type does not free string inside it during destruction.
        // To get smart pointer-like behaviour we convert TUnboxedValuePod to TUnboxedValue. Without this conversion there
        // will be a memory leak.
        NUdf::TUnboxedValue jsonDocument = ValueFromString(NUdf::EDataSlot::JsonDocument, node.Head().Content());
        MKQL_ENSURE(bool(jsonDocument), "Invalid JsonDocument literal");
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::JsonDocument>(jsonDocument.AsStringRef());
    });

    AddCallable("Uuid", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Uuid>(node.Head().Content());
    });

    AddCallable("Decimal", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto precision = FromString<ui8>(node.Child(1)->Content());
        const auto scale = FromString<ui8>(node.Child(2)->Content());
        MKQL_ENSURE(precision > 0, "Precision must be positive.");
        MKQL_ENSURE(scale <= precision, "Scale too large.");
        const auto data = NDecimal::FromString(node.Head().Content(), precision, scale);
        MKQL_ENSURE(!NDecimal::IsError(data), "Bad decimal.");
        return ctx.ProgramBuilder.NewDecimalLiteral(data, precision, scale);
    });

    AddCallable("Bool", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral(FromString<bool>(node.Head(), NUdf::EDataSlot::Bool));
    });

    AddCallable("Float", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral(FromString<float>(node.Head(), NUdf::EDataSlot::Float));
    });

    AddCallable("Double", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral(FromString<double>(node.Head(), NUdf::EDataSlot::Double));
    });

    AddCallable("DyNumber", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const NUdf::TUnboxedValue val = ValueFromString(NUdf::EDataSlot::DyNumber, node.Head().Content());
        MKQL_ENSURE(val, "Bad DyNumber: " << TString(node.Head().Content()).Quote());
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::DyNumber>(val.AsStringRef());
    });

    AddCallable("Date", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto value = FromString<ui16>(node.Head(), NUdf::EDataSlot::Date);
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Date>(
            NUdf::TStringRef((const char*)&value, sizeof(value)));
    });

    AddCallable("Datetime", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto value = FromString<ui32>(node.Head(), NUdf::EDataSlot::Datetime);
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Datetime>(
            NUdf::TStringRef((const char*)&value, sizeof(value)));
    });

    AddCallable("Timestamp", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto value = FromString<ui64>(node.Head(), NUdf::EDataSlot::Timestamp);
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Timestamp>(
            NUdf::TStringRef((const char*)&value, sizeof(value)));
    });

    AddCallable("Interval", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto value = FromString<i64>(node.Head(), NUdf::EDataSlot::Interval);
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(
            NUdf::TStringRef((const char*)&value, sizeof(value)));
    });

    AddCallable("TzDate", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto& parts = CutTimezone<ui16>(node.Head().Content());
        return ctx.ProgramBuilder.NewTzDataLiteral<NUdf::TTzDate>(parts.first, parts.second);
    });

    AddCallable("TzDatetime", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto& parts = CutTimezone<ui32>(node.Head().Content());
        return ctx.ProgramBuilder.NewTzDataLiteral<NUdf::TTzDatetime>(parts.first, parts.second);
    });

    AddCallable("TzTimestamp", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto& parts = CutTimezone<ui64>(node.Head().Content());
        return ctx.ProgramBuilder.NewTzDataLiteral<NUdf::TTzTimestamp>(parts.first, parts.second);
    });

    AddCallable("Date32", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto value = FromString<i32>(node.Head(), NUdf::EDataSlot::Date32);
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Date32>(
            NUdf::TStringRef((const char*)&value, sizeof(value)));
    });

    AddCallable("Datetime64", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto value = FromString<i64>(node.Head(), NUdf::EDataSlot::Datetime64);
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Datetime64>(
            NUdf::TStringRef((const char*)&value, sizeof(value)));
    });

    AddCallable("Timestamp64", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto value = FromString<i64>(node.Head(), NUdf::EDataSlot::Timestamp64);
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Timestamp64>(
            NUdf::TStringRef((const char*)&value, sizeof(value)));
    });

    AddCallable("Interval64", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto value = FromString<i64>(node.Head(), NUdf::EDataSlot::Interval64);
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Interval64>(
            NUdf::TStringRef((const char*)&value, sizeof(value)));
    });

    AddCallable("TzDate32", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto& parts = CutTimezone<i32>(node.Head().Content());
        return ctx.ProgramBuilder.NewTzDataLiteral<NUdf::TTzDate32>(parts.first, parts.second);
    });

    AddCallable("TzDatetime64", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto& parts = CutTimezone<i64>(node.Head().Content());
        return ctx.ProgramBuilder.NewTzDataLiteral<NUdf::TTzDatetime64>(parts.first, parts.second);
    });

    AddCallable("TzTimestamp64", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto& parts = CutTimezone<i64>(node.Head().Content());
        return ctx.ProgramBuilder.NewTzDataLiteral<NUdf::TTzTimestamp64>(parts.first, parts.second);
    });

    AddCallable("FoldMap", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        const auto state = MkqlBuildExpr(*node.Child(1), ctx);
        return ctx.ProgramBuilder.ChainMap(list, state, [&](TRuntimeNode item, TRuntimeNode state) {
            return MkqlBuildSplitLambda(*node.Child(2), ctx, {item, state});
        });
     });

    AddCallable("Fold1Map", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        return ctx.ProgramBuilder.Chain1Map(list, [&](TRuntimeNode item) {
            return MkqlBuildSplitLambda(*node.Child(1), ctx, {item});
        }, [&](TRuntimeNode item, TRuntimeNode state) {
            return MkqlBuildSplitLambda(*node.Child(2), ctx, {item, state});
        });
    });

    AddCallable("Chain1Map", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        return ctx.ProgramBuilder.Chain1Map(list,
            [&](TRuntimeNode item) -> std::array<TRuntimeNode, 2U> {
            const auto out = MkqlBuildLambda(*node.Child(1), ctx, {item});
            return {{out, out}};
        }, [&](TRuntimeNode item, TRuntimeNode state) -> std::array<TRuntimeNode, 2U> {
            const auto out = MkqlBuildLambda(*node.Child(2), ctx, {item, state});
            return {{out, out}};
        });
    });

    AddCallable("Extract", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        const auto name = node.Tail().Content();
        return ctx.ProgramBuilder.Extract(list, name);
    });

    AddCallable("OrderedExtract", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        const auto name = node.Child(1)->Content();
        return ctx.ProgramBuilder.OrderedExtract(list, name);
    });

    AddCallable("Fold", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        const auto state = MkqlBuildExpr(*node.Child(1), ctx);
        return ctx.ProgramBuilder.Fold(list, state, [&](TRuntimeNode item, TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(2), ctx, {item, state});
        });
    });

    AddCallable("MapNext", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        return ctx.ProgramBuilder.MapNext(list, [&](TRuntimeNode item, TRuntimeNode nextItem) {
            return MkqlBuildLambda(node.Tail(), ctx, {item, nextItem});
        });
    });

    AddCallable("Fold1", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        return ctx.ProgramBuilder.Fold1(list, [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(1), ctx, {item});
        }, [&](TRuntimeNode item, TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(2), ctx, {item, state});
        });
    });

    AddCallable("Condense", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto stream = MkqlBuildExpr(node.Head(), ctx);
        const auto state = MkqlBuildExpr(*node.Child(1), ctx);
        return ctx.ProgramBuilder.Condense(stream, state,
        [&](TRuntimeNode item, TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(2), ctx, {item, state});
        },
        [&](TRuntimeNode item, TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(3), ctx, {item, state});
        }, HasContextFuncs(*node.Child(3)));
    });

    AddCallable("Condense1", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto stream = MkqlBuildExpr(node.Head(), ctx);
        return ctx.ProgramBuilder.Condense1(stream,
        [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(1), ctx, {item});
        },
        [&](TRuntimeNode item, TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(2), ctx, {item, state});
        },
        [&](TRuntimeNode item, TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(3), ctx, {item, state});
        }, HasContextFuncs(*node.Child(1)) || HasContextFuncs(*node.Child(3)));
    });

    AddCallable("Squeeze", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto stream = MkqlBuildExpr(node.Head(), ctx);
        const auto state = MkqlBuildExpr(*node.Child(1), ctx);
        return ctx.ProgramBuilder.Squeeze(stream, state, [&](TRuntimeNode item, TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(2), ctx, {item, state});
        }, node.Child(3)->IsCallable("Void") ? std::function<TRuntimeNode(TRuntimeNode)>() : [&](TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(3), ctx, {state});
        }, node.Child(4)->IsCallable("Void") ? std::function<TRuntimeNode(TRuntimeNode)>() : [&](TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(4), ctx, {state});
        });
    });

    AddCallable("Squeeze1", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto stream = MkqlBuildExpr(node.Head(), ctx);
        return ctx.ProgramBuilder.Squeeze1(stream, [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(1), ctx, {item});
        }, [&](TRuntimeNode item, TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(2), ctx, {item, state});
        }, node.Child(3)->IsCallable("Void") ? std::function<TRuntimeNode(TRuntimeNode)>() : [&](TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(3), ctx, {state});
        }, node.Child(4)->IsCallable("Void") ? std::function<TRuntimeNode(TRuntimeNode)>() : [&](TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(4), ctx, {state});
        });
    });

    AddCallable("Sort", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        const auto ascending = MkqlBuildExpr(*node.Child(1), ctx);
        return ctx.ProgramBuilder.Sort(list, ascending, [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(2), ctx, {item});
        });
    });

    AddCallable({"Top", "TopSort"}, [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        const auto count = MkqlBuildExpr(*node.Child(1), ctx);
        const auto ascending = MkqlBuildExpr(*node.Child(2), ctx);
        return (ctx.ProgramBuilder.*(node.Content() == "Top" ? &TProgramBuilder::Top : &TProgramBuilder::TopSort))
            (list, count, ascending, [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(3), ctx, {item});
        });
    });

    AddCallable("KeepTop", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto count = MkqlBuildExpr(node.Head(), ctx);
        const auto list = MkqlBuildExpr(*node.Child(1), ctx);
        const auto item = MkqlBuildExpr(*node.Child(2), ctx);
        const auto ascending = MkqlBuildExpr(*node.Child(3), ctx);
        return ctx.ProgramBuilder.KeepTop(count, list, item, ascending, [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(4), ctx, {item});
        });
    });

    AddCallable("Struct", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto structType = BuildType(node.Head(), *node.Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
        const auto verifiedStructType = AS_TYPE(TStructType, structType);
        std::vector<std::pair<std::string_view, TRuntimeNode>> members;
        members.reserve(verifiedStructType->GetMembersCount());
        node.ForEachChild([&](const TExprNode& child) {
            members.emplace_back(child.Head().Content(), MkqlBuildExpr(child.Tail(), ctx));
        });

        return ctx.ProgramBuilder.NewStruct(verifiedStructType, members);
    });

    AddCallable("AddMember", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto structObj = MkqlBuildExpr(node.Head(), ctx);
        const auto memberName = node.Child(1)->Content();
        const auto value = MkqlBuildExpr(node.Tail(), ctx);
        return ctx.ProgramBuilder.AddMember(structObj, memberName, value);
    });

    AddCallable("List", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto listType = BuildType(node.Head(), *node.Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
        const auto itemType = AS_TYPE(TListType, listType)->GetItemType();
        const auto& items = GetArgumentsFrom<1U>(node, ctx);
        return ctx.ProgramBuilder.NewList(itemType, items);
    });

    AddCallable("FromString", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto type = BuildType(node.Head(), *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.FromString(arg, type);
    });

    AddCallable("StrictFromString", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto type = BuildType(node.Head(), *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.StrictFromString(arg, type);
    });

    AddCallable("FromBytes", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto schemeType = ParseDataType(node, node.Tail().Content());
        return ctx.ProgramBuilder.FromBytes(arg, schemeType);
    });

    AddCallable("Convert", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto type = BuildType(node.Head(), *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.Convert(arg, type);
    });

    AddCallable("ToIntegral", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto type = BuildType(node.Head(), *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.ToIntegral(arg, type);
    });

    AddCallable("UnsafeTimestampCast", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto type = BuildType(node.Head(), *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.Convert(arg, type);
    });

    AddCallable("SafeCast", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto type = BuildType(node.Head(), *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.Cast(arg, type);
    });

    AddCallable("Default", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto type = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.Default(type);
    });

    AddCallable("Coalesce", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto ret = MkqlBuildExpr(node.Head(), ctx);
        for (ui32 i = 1; i < node.ChildrenSize(); ++i) {
            auto value = MkqlBuildExpr(*node.Child(i), ctx);
            ret = ctx.ProgramBuilder.Coalesce(ret, value);
        }

        return ret;
    });

    AddCallable("Unwrap", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto opt = MkqlBuildExpr(node.Head(), ctx);
        const auto message = node.ChildrenSize() > 1 ? MkqlBuildExpr(node.Tail(), ctx) : ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>("");
        const auto pos = ctx.ExprCtx.GetPosition(node.Pos());
        return ctx.ProgramBuilder.Unwrap(opt, message, pos.File, pos.Row, pos.Column);
    });

    AddCallable("EmptyFrom", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto type = BuildType(node.Head(), *node.GetTypeAnn(), ctx.ProgramBuilder);
        switch (node.GetTypeAnn()->GetKind()) {
            case ETypeAnnotationKind::Flow:
            case ETypeAnnotationKind::Stream:
                return ctx.ProgramBuilder.EmptyIterator(type);
            case ETypeAnnotationKind::Optional:
                return ctx.ProgramBuilder.NewEmptyOptional(type);
            case ETypeAnnotationKind::List:
                return ctx.ProgramBuilder.NewEmptyList(AS_TYPE(TListType, type)->GetItemType());
            case ETypeAnnotationKind::Dict:
                return ctx.ProgramBuilder.NewDict(type, {});
            default:
                ythrow TNodeException(node) << "Empty from " << *node.GetTypeAnn() << " isn't supported.";
        }
    });

    AddCallable("Nothing", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto optType = BuildType(node.Head(), *node.Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.NewEmptyOptional(optType);
    });

    AddCallable("Unpickle", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto type = BuildType(node.Head(), *node.Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
        const auto serialized = MkqlBuildExpr(node.Tail(), ctx);
        return ctx.ProgramBuilder.Unpickle(type, serialized);
    });

    AddCallable("Optional", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto optType = BuildType(node.Head(), *node.Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
        const auto arg = MkqlBuildExpr(node.Tail(), ctx);
        return ctx.ProgramBuilder.NewOptional(optType, arg);
    });

    AddCallable("Iterator", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto& args = GetArgumentsFrom<1U>(node, ctx);
        return ctx.ProgramBuilder.Iterator(arg, args);
    });

    AddCallable("EmptyIterator", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto streamType = BuildType(node.Head(), *node.Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.EmptyIterator(streamType);
    });

    AddCallable("Switch", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto stream = MkqlBuildExpr(node.Head(), ctx);
        std::vector<TSwitchInput> inputs;
        ui64 memoryLimitBytes = FromString<ui64>(node.Child(1)->Content());
        ui32 offset = 0;
        for (ui32 i = 2; i < node.ChildrenSize(); i += 2) {
            TSwitchInput input;
            for (auto& child : node.Child(i)->Children()) {
                input.Indicies.push_back(FromString<ui32>(child->Content()));
            }

            const auto& lambda = *node.Child(i + 1);
            const auto& lambdaArg = lambda.Head().Head();
            auto outputStreams = 1;
            const auto& streamItemType = GetSeqItemType(*lambda.Tail().GetTypeAnn());
            if (streamItemType.GetKind() == ETypeAnnotationKind::Variant) {
                outputStreams = streamItemType.Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>()->GetSize();
            }

            if (node.ChildrenSize() > 4 || outputStreams != 1) {
                input.ResultVariantOffset = offset;
            }

            offset += outputStreams;
            input.InputType = BuildType(lambdaArg, *lambdaArg.GetTypeAnn(), ctx.ProgramBuilder);
            inputs.emplace_back(input);
        }

        const auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.Switch(stream, inputs, [&](ui32 index, TRuntimeNode item) -> TRuntimeNode {
            return MkqlBuildLambda(*node.Child(2 + 2 * index + 1), ctx, {item});
        }, memoryLimitBytes, returnType);
    });

    AddCallable("ToStream", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        const auto& args = GetArgumentsFrom<1U>(node, ctx);
        return ctx.ProgramBuilder.Iterator(ctx.ProgramBuilder.ToList(arg), args);
    });

    AddCallable(LeftName, [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        return ctx.ProgramBuilder.Nth(arg, 0);
    });

    AddCallable(RightName, [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        return ctx.ProgramBuilder.Nth(arg, 1);
    });

    AddCallable("FilterNullMembers", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        if (node.ChildrenSize() < 2U) {
            return ctx.ProgramBuilder.FilterNullMembers(list);
        } else {
            std::vector<std::string_view> members;
            members.reserve(node.Tail().ChildrenSize());
            node.Tail().ForEachChild([&](const TExprNode& child){ members.emplace_back(child.Content()); });
            return ctx.ProgramBuilder.FilterNullMembers(list, members);
        }
    });

    AddCallable("SkipNullMembers", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        if (node.ChildrenSize() < 2U) {
            return ctx.ProgramBuilder.SkipNullMembers(list);
        } else {
            std::vector<std::string_view> members;
            members.reserve(node.Tail().ChildrenSize());
            node.Tail().ForEachChild([&](const TExprNode& child){ members.emplace_back(child.Content()); });
            return ctx.ProgramBuilder.SkipNullMembers(list, members);
        }
    });

    AddCallable("FilterNullElements", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        if (node.ChildrenSize() < 2U) {
            return ctx.ProgramBuilder.FilterNullElements(list);
        } else {
            std::vector<ui32> members;
            members.reserve(node.Tail().ChildrenSize());
            node.Tail().ForEachChild([&](const TExprNode& child){ members.emplace_back(FromString<ui32>(child.Content())); });
            return ctx.ProgramBuilder.FilterNullElements(list, members);
        }
    });

    AddCallable("SkipNullElements", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        if (node.ChildrenSize() < 2U) {
            return ctx.ProgramBuilder.SkipNullElements(list);
        } else {
            std::vector<ui32> members;
            members.reserve(node.Tail().ChildrenSize());
            node.Tail().ForEachChild([&](const TExprNode& child){ members.emplace_back(FromString<ui32>(child.Content())); });
            return ctx.ProgramBuilder.SkipNullElements(list, members);
        }
    });

    AddCallable("MapJoinCore", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        const auto dict = MkqlBuildExpr(*node.Child(1), ctx);
        const auto joinKind = GetJoinKind(node, node.Child(2)->Content());

        const auto& outputItemType = GetSeqItemType(*node.GetTypeAnn());
        auto rightItemType = node.Child(1U)->GetTypeAnn()->Cast<TDictExprType>()->GetPayloadType();
        if (ETypeAnnotationKind::List == rightItemType->GetKind()) {
            rightItemType = rightItemType->Cast<TListExprType>()->GetItemType();
        }

        std::vector<ui32> leftKeyColumns, leftRenames, rightRenames;
        switch (const auto& inputItemType = GetSeqItemType(*node.Head().GetTypeAnn()); inputItemType.GetKind()) {
            case ETypeAnnotationKind::Struct: {
                const auto inputStructType = inputItemType.Cast<TStructExprType>();
                const auto outputStructType = outputItemType.Cast<TStructExprType>();

                node.Child(3)->ForEachChild([&](const TExprNode& child){ leftKeyColumns.emplace_back(*GetFieldPosition(*inputStructType, child.Content())); });
                bool s = false;
                node.Child(5)->ForEachChild([&](const TExprNode& child){ leftRenames.emplace_back(*GetFieldPosition((s = !s) ?  *inputStructType : *outputStructType, child.Content())); });

                switch (rightItemType->GetKind()) {
                    case ETypeAnnotationKind::Struct: {
                        const auto rightStructType = rightItemType->Cast<TStructExprType>();
                        node.Child(6)->ForEachChild([&](const TExprNode& child){
                            rightRenames.emplace_back(*GetFieldPosition((s = !s) ? *rightStructType : *outputStructType, child.Content()));    });
                        }
                        break;
                    case ETypeAnnotationKind::Tuple: {
                        const auto rightTupleType = rightItemType->Cast<TTupleExprType>();
                        node.Child(6)->ForEachChild([&](const TExprNode& child){
                            rightRenames.emplace_back((s = !s) ? *GetFieldPosition(*rightTupleType, child.Content()) : *GetFieldPosition(*outputStructType, child.Content())); });
                        }
                        break;
                    default:
                        MKQL_ENSURE(!node.Child(6)->ChildrenSize(), "Expected empty right output columns.");
                }
                break;
            }
            case ETypeAnnotationKind::Tuple: {
                const auto inputTupleType = inputItemType.Cast<TTupleExprType>();
                const auto outputTupleType = outputItemType.Cast<TTupleExprType>();

                node.Child(3)->ForEachChild([&](const TExprNode& child){ leftKeyColumns.emplace_back(*GetFieldPosition(*inputTupleType, child.Content())); });
                bool s = false;
                node.Child(5)->ForEachChild([&](const TExprNode& child){ leftRenames.emplace_back(*GetFieldPosition((s = !s) ?  *inputTupleType : *outputTupleType, child.Content())); });

                switch (rightItemType->GetKind()) {
                    case ETypeAnnotationKind::Tuple: {
                        const auto rightTupleType = rightItemType->Cast<TTupleExprType>();
                        node.Child(6)->ForEachChild([&](const TExprNode& child){
                            rightRenames.emplace_back(*GetFieldPosition((s = !s) ? *rightTupleType : *outputTupleType, child.Content()));    });
                        }
                        break;
                    case ETypeAnnotationKind::Struct: {
                        const auto rightStructType = rightItemType->Cast<TStructExprType>();
                        node.Child(6)->ForEachChild([&](const TExprNode& child){
                            rightRenames.emplace_back((s = !s) ? *GetFieldPosition(*rightStructType, child.Content()) : *GetFieldPosition(*outputTupleType, child.Content())); });
                        }
                        break;
                    default:
                        MKQL_ENSURE(!node.Child(6)->ChildrenSize(), "Expected empty right output columns.");
                }
                break;
            }
            case ETypeAnnotationKind::Multi: {
                const auto inputMultiType = inputItemType.Cast<TMultiExprType>();
                const auto outputMultiType = outputItemType.Cast<TMultiExprType>();

                node.Child(3)->ForEachChild([&](const TExprNode& child){ leftKeyColumns.emplace_back(*GetFieldPosition(*inputMultiType, child.Content())); });
                bool s = false;
                node.Child(5)->ForEachChild([&](const TExprNode& child){ leftRenames.emplace_back(*GetFieldPosition((s = !s) ?  *inputMultiType : *outputMultiType, child.Content())); });

                switch (rightItemType->GetKind()) {
                    case ETypeAnnotationKind::Tuple: {
                        const auto rightTupleType = rightItemType->Cast<TTupleExprType>();
                        node.Child(6)->ForEachChild([&](const TExprNode& child){
                            rightRenames.emplace_back((s = !s) ? *GetFieldPosition(*rightTupleType, child.Content()) : *GetFieldPosition(*outputMultiType, child.Content())); });
                        }
                        break;
                    case ETypeAnnotationKind::Struct: {
                        const auto rightStructType = rightItemType->Cast<TStructExprType>();
                        node.Child(6)->ForEachChild([&](const TExprNode& child){
                            rightRenames.emplace_back((s = !s) ? *GetFieldPosition(*rightStructType, child.Content()) : *GetFieldPosition(*outputMultiType, child.Content())); });
                        }
                        break;
                    default:
                        MKQL_ENSURE(!node.Child(6)->ChildrenSize(), "Expected empty right output columns.");
                }
                break;
            }
            default:
                ythrow TNodeException(node) << "Wrong MapJoinCore input item type: " << inputItemType;
        }

        const auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.MapJoinCore(list, dict, joinKind, leftKeyColumns, leftRenames, rightRenames, returnType);
    });

    AddCallable({"GraceJoinCore", "GraceSelfJoinCore"}, [](const TExprNode& node, TMkqlBuildContext& ctx) {
        bool selfJoin = node.Content() == "GraceSelfJoinCore";
        int shift = selfJoin ? 0 : 1;
        const auto flowLeft = MkqlBuildExpr(*node.Child(0), ctx);
        const auto flowRight = MkqlBuildExpr(*node.Child(shift), ctx);
        const auto joinKind = GetJoinKind(node, node.Child(shift + 1)->Content());

        const auto& outputItemType = GetSeqItemType(*node.GetTypeAnn());

        std::vector<ui32> leftKeyColumns, rightKeyColumns, leftRenames, rightRenames;
        const auto& leftItemType = GetSeqItemType(*node.Child(0)->GetTypeAnn());
        const auto& rightItemType = GetSeqItemType(*node.Child(shift)->GetTypeAnn());

        if (leftItemType.GetKind() != ETypeAnnotationKind::Multi ||
            rightItemType.GetKind() != ETypeAnnotationKind::Multi ) {
            ythrow TNodeException(node) << "Wrong GraceJoinCore input item type: " << leftItemType << " " << rightItemType;

        }

        if (outputItemType.GetKind() != ETypeAnnotationKind::Multi ) {
            ythrow TNodeException(node) << "Wrong GraceJoinCore output item type: " << outputItemType;
        }

        const auto leftTupleType = leftItemType.Cast<TMultiExprType>();
        const auto rightTupleType = rightItemType.Cast<TMultiExprType>();
        const auto outputTupleType = outputItemType.Cast<TMultiExprType>();

        node.Child(shift + 2)->ForEachChild([&](TExprNode& child){
            leftKeyColumns.emplace_back(*GetFieldPosition(*leftTupleType, child.Content()));
            });
        node.Child(shift + 3)->ForEachChild([&](TExprNode& child){
            rightKeyColumns.emplace_back(*GetFieldPosition(*rightTupleType, child.Content())); });
        bool s = false;
        node.Child(shift + 4)->ForEachChild([&](TExprNode& child){
            leftRenames.emplace_back(*GetFieldPosition((s = !s) ?  *leftTupleType : *outputTupleType, child.Content())); });
        s = false;
        node.Child(shift + 5)->ForEachChild([&](TExprNode& child){
            rightRenames.emplace_back(*GetFieldPosition((s = !s) ?  *rightTupleType : *outputTupleType, child.Content())); });

        auto anyJoinSettings = EAnyJoinSettings::None;
        node.Tail().ForEachChild([&](const TExprNode& flag) {
            if (flag.IsAtom("LeftAny"))
               anyJoinSettings = EAnyJoinSettings::Right == anyJoinSettings ? EAnyJoinSettings::Both : EAnyJoinSettings::Left;
            else if (flag.IsAtom("RightAny"))
               anyJoinSettings = EAnyJoinSettings::Left == anyJoinSettings ? EAnyJoinSettings::Both : EAnyJoinSettings::Right;
        });

        const auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);

        return selfJoin
            ? ctx.ProgramBuilder.GraceSelfJoin(flowLeft, joinKind, leftKeyColumns, rightKeyColumns, leftRenames, rightRenames, returnType, anyJoinSettings)
            : ctx.ProgramBuilder.GraceJoin(flowLeft, flowRight, joinKind, leftKeyColumns, rightKeyColumns, leftRenames, rightRenames, returnType, anyJoinSettings);
    });

    AddCallable("CommonJoinCore", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        const auto joinKind = GetJoinKind(node, node.Child(1)->Content());

        std::vector<ui32> leftColumns, rightColumns, requiredColumns, keyColumns;
        ui32 tableIndexFieldPos;
        switch (const auto& inputItemType = GetSeqItemType(*node.Head().GetTypeAnn()); inputItemType.GetKind()) {
            case ETypeAnnotationKind::Struct: {
                const auto inputStructType = inputItemType.Cast<TStructExprType>();
                const auto outputStructType = GetSeqItemType(*node.GetTypeAnn()).Cast<TStructExprType>();
                node.Child(2)->ForEachChild([&](const TExprNode& child){
                    leftColumns.emplace_back(*GetFieldPosition(*inputStructType, child.Content()));
                    leftColumns.emplace_back(*GetFieldPosition(*outputStructType, child.Content()));
                });
                node.Child(3)->ForEachChild([&](const TExprNode& child){
                    rightColumns.emplace_back(*GetFieldPosition(*inputStructType, child.Content()));
                    rightColumns.emplace_back(*GetFieldPosition(*outputStructType, child.Content()));
                });
                node.Child(4)->ForEachChild([&](const TExprNode& child){ requiredColumns.emplace_back(*GetFieldPosition(*inputStructType, child.Content())); });
                node.Child(5)->ForEachChild([&](const TExprNode& child){ keyColumns.emplace_back(*GetFieldPosition(*inputStructType, child.Content())); });
                tableIndexFieldPos = *GetFieldPosition(*inputStructType, node.Tail().Content());
                break;
            }
            case ETypeAnnotationKind::Tuple: {
                const auto inputTupleType = inputItemType.Cast<TTupleExprType>();
                ui32 i = 0U;
                node.Child(2)->ForEachChild([&](const TExprNode& child){
                    leftColumns.emplace_back(*GetFieldPosition(*inputTupleType, child.Content()));
                    leftColumns.emplace_back(i++);
                });
                node.Child(3)->ForEachChild([&](const TExprNode& child){
                    rightColumns.emplace_back(*GetFieldPosition(*inputTupleType, child.Content()));
                    rightColumns.emplace_back(i++);
                });
                node.Child(4)->ForEachChild([&](const TExprNode& child){ requiredColumns.emplace_back(*GetFieldPosition(*inputTupleType, child.Content())); });
                node.Child(5)->ForEachChild([&](const TExprNode& child){ keyColumns.emplace_back(*GetFieldPosition(*inputTupleType, child.Content())); });
                tableIndexFieldPos = *GetFieldPosition(*inputTupleType, node.Tail().Content());
                break;
            }
            case ETypeAnnotationKind::Multi: {
                const auto inputMultiType = inputItemType.Cast<TMultiExprType>();
                ui32 i = 0U;
                node.Child(2)->ForEachChild([&](const TExprNode& child){
                    leftColumns.emplace_back(*GetFieldPosition(*inputMultiType, child.Content()));
                    leftColumns.emplace_back(i++);
                });
                node.Child(3)->ForEachChild([&](const TExprNode& child){
                    rightColumns.emplace_back(*GetFieldPosition(*inputMultiType, child.Content()));
                    rightColumns.emplace_back(i++);
                });
                node.Child(4)->ForEachChild([&](const TExprNode& child){ requiredColumns.emplace_back(*GetFieldPosition(*inputMultiType, child.Content())); });
                node.Child(5)->ForEachChild([&](const TExprNode& child){ keyColumns.emplace_back(*GetFieldPosition(*inputMultiType, child.Content())); });
                tableIndexFieldPos = *GetFieldPosition(*inputMultiType, node.Tail().Content());
                break;
            }
            default:
                ythrow TNodeException(node) << "Wrong CommonJoinCore input item type: " << inputItemType;
        }

        ui64 memLimit = 0U;
        if (const auto memLimitSetting = GetSetting(*node.Child(6), "memLimit")) {
            memLimit = FromString<ui64>(memLimitSetting->Child(1)->Content());
        }

        std::optional<ui32> sortedTableOrder;
        if (const auto sortSetting = GetSetting(*node.Child(6), "sorted")) {
            sortedTableOrder = sortSetting->Child(1)->Content() == "left" ? 0 : 1;
        }


        EAnyJoinSettings anyJoinSettings = EAnyJoinSettings::None;
        if (const auto anyNode = GetSetting(*node.Child(6), "any")) {
            for (auto sideNode : anyNode->Child(1)->Children()) {
                YQL_ENSURE(sideNode->IsAtom());
                AddAnyJoinSide(anyJoinSettings, sideNode->Content() == "left" ? EAnyJoinSettings::Left : EAnyJoinSettings::Right);
            }
        }

        const auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.CommonJoinCore(list, joinKind, leftColumns, rightColumns,
            requiredColumns, keyColumns, memLimit, sortedTableOrder, anyJoinSettings, tableIndexFieldPos, returnType);
    });

    AddCallable("CombineCore", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        NNodes::TCoCombineCore core(&node);

        const auto stream = MkqlBuildExpr(core.Input().Ref(), ctx);
        const auto memLimit = FromString<ui64>(core.MemLimit().Cast().Value());

        const auto keyExtractor = [&](TRuntimeNode item) {
            return MkqlBuildLambda(core.KeyExtractor().Ref(), ctx, {item});
        };
        const auto init = [&](TRuntimeNode key, TRuntimeNode item) {
            return MkqlBuildLambda(core.InitHandler().Ref(), ctx, {key, item});
        };
        const auto update = [&](TRuntimeNode key, TRuntimeNode item, TRuntimeNode state) {
            return MkqlBuildLambda(core.UpdateHandler().Ref(), ctx, {key, item, state});
        };
        const auto finish = [&](TRuntimeNode key, TRuntimeNode state) {
            return MkqlBuildLambda(core.FinishHandler().Ref(), ctx, {key, state});
        };

        return ctx.ProgramBuilder.CombineCore(stream, keyExtractor, init, update, finish, memLimit);
    });

    AddCallable("GroupingCore", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        NNodes::TCoGroupingCore core(&node);

        const auto stream = MkqlBuildExpr(core.Input().Ref(), ctx);

        const auto groupSwitch = [&](TRuntimeNode key, TRuntimeNode item) {
            return MkqlBuildLambda(core.GroupSwitch().Ref(), ctx, {key, item});
        };
        const auto keyExtractor = [&](TRuntimeNode item) {
            return MkqlBuildLambda(core.KeyExtractor().Ref(), ctx, {item});
        };
        TProgramBuilder::TUnaryLambda handler;
        if (auto lambda = core.ConvertHandler()) {
            handler = [&](TRuntimeNode item) {
                return MkqlBuildLambda(core.ConvertHandler().Ref(), ctx, {item});
            };
        }

        return ctx.ProgramBuilder.GroupingCore(stream, groupSwitch, keyExtractor, handler);
    });

    AddCallable("Chopper", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto stream = MkqlBuildExpr(node.Head(), ctx);

        const auto keyExtractor = [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(1U), ctx, {item});
        };

        const auto groupSwitch = [&](TRuntimeNode key, TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(2U), ctx, {key, item});
        };

        const auto handler = [&](TRuntimeNode key, TRuntimeNode flow) {
            return MkqlBuildLambda(node.Tail(), ctx, {key, flow});
        };

        return ctx.ProgramBuilder.Chopper(stream, keyExtractor, groupSwitch, handler);
    });

    AddCallable("HoppingCore", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto stream = MkqlBuildExpr(node.Head(), ctx);

        const auto timeExtractor = [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(1), ctx, {item});
        };
        const auto hop = MkqlBuildExpr(*node.Child(2), ctx);
        const auto interval = MkqlBuildExpr(*node.Child(3), ctx);
        const auto delay = MkqlBuildExpr(*node.Child(4), ctx);

        const auto init = [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(5), ctx, {item});
        };
        const auto update = [&](TRuntimeNode item, TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(6), ctx, {item, state});
        };
        const auto save = node.Child(3)->IsCallable("Void") ? std::function<TRuntimeNode(TRuntimeNode)>() : [&](TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(7), ctx, {state});
        };
        const auto load = node.Child(4)->IsCallable("Void") ? std::function<TRuntimeNode(TRuntimeNode)>() : [&](TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(8), ctx, {state});
        };
        const auto merge = [&](TRuntimeNode state1, TRuntimeNode state2) {
            return MkqlBuildLambda(*node.Child(9), ctx, {state1, state2});
        };
        const auto finish = [&](TRuntimeNode state, TRuntimeNode time) {
            return MkqlBuildLambda(*node.Child(10), ctx, {state, time});
        };

        return ctx.ProgramBuilder.HoppingCore(
            stream, timeExtractor, init, update, save, load, merge, finish, hop, interval, delay);
    });

    AddCallable("MultiHoppingCore", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto stream = MkqlBuildExpr(node.Head(), ctx);

        const auto keyExtractor = [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(1), ctx, {item});
        };

        const auto timeExtractor = [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(2), ctx, {item});
        };
        const auto hop = MkqlBuildExpr(*node.Child(3), ctx);
        const auto interval = MkqlBuildExpr(*node.Child(4), ctx);
        const auto delay = MkqlBuildExpr(*node.Child(5), ctx);
        const auto dataWatermarks = ctx.ProgramBuilder.NewDataLiteral(FromString<bool>(*node.Child(6), NUdf::EDataSlot::Bool));

        const auto init = [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(7), ctx, {item});
        };
        const auto update = [&](TRuntimeNode item, TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(8), ctx, {item, state});
        };
        const auto save = node.Child(3)->IsCallable("Void") ? std::function<TRuntimeNode(TRuntimeNode)>() : [&](TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(9), ctx, {state});
        };
        const auto load = node.Child(4)->IsCallable("Void") ? std::function<TRuntimeNode(TRuntimeNode)>() : [&](TRuntimeNode state) {
            return MkqlBuildLambda(*node.Child(10), ctx, {state});
        };
        const auto merge = [&](TRuntimeNode state1, TRuntimeNode state2) {
            return MkqlBuildLambda(*node.Child(11), ctx, {state1, state2});
        };
        const auto finish = [&](TRuntimeNode key, TRuntimeNode state, TRuntimeNode time) {
            return MkqlBuildLambda(*node.Child(12), ctx, {key, state, time});
        };

        const auto watermarksMode = ctx.ProgramBuilder.NewDataLiteral(FromString<bool>(*node.Child(13), NUdf::EDataSlot::Bool));

        return ctx.ProgramBuilder.MultiHoppingCore(
            stream, keyExtractor, timeExtractor, init, update, save, load, merge, finish,
            hop, interval, delay, dataWatermarks, watermarksMode);
    });

    AddCallable("ToDict", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        TMaybe<bool> isMany;
        TMaybe<EDictType> type;
        TMaybe<ui64> itemsCount;
        bool isCompact;
        if (const auto error = ParseToDictSettings(node, ctx.ExprCtx, type, isMany, itemsCount, isCompact)) {
            ythrow TNodeException(node) << error->GetMessage();
        }

        *type = SelectDictType(*type, node.Child(1)->GetTypeAnn());
        const auto factory = *type == EDictType::Hashed ? &TProgramBuilder::ToHashedDict : &TProgramBuilder::ToSortedDict;
        return (ctx.ProgramBuilder.*factory)(list, *isMany, [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(1), ctx, {item});
        }, [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(2), ctx, {item});
        }, isCompact, itemsCount.GetOrElse(0));
    });

    AddCallable("SqueezeToDict", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto stream = MkqlBuildExpr(node.Head(), ctx);
        TMaybe<bool> isMany;
        TMaybe<EDictType> type;
        TMaybe<ui64> itemsCount;
        bool isCompact;
        if (const auto error = ParseToDictSettings(node, ctx.ExprCtx, type, isMany, itemsCount, isCompact)) {
            ythrow TNodeException(node) << error->GetMessage();
        }

        *type = SelectDictType(*type, node.Child(1)->GetTypeAnn());
        const auto factory = *type == EDictType::Hashed ? &TProgramBuilder::SqueezeToHashedDict : &TProgramBuilder::SqueezeToSortedDict;
        return (ctx.ProgramBuilder.*factory)(stream, *isMany, [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(1), ctx, {item});
        }, [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(2), ctx, {item});
        }, isCompact, itemsCount.GetOrElse(0));
    });

    AddCallable("NarrowSqueezeToDict", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto stream = MkqlBuildExpr(node.Head(), ctx);
        TMaybe<bool> isMany;
        TMaybe<EDictType> type;
        TMaybe<ui64> itemsCount;
        bool isCompact;
        if (const auto error = ParseToDictSettings(node, ctx.ExprCtx, type, isMany, itemsCount, isCompact)) {
            ythrow TNodeException(node) << error->GetMessage();
        }

        *type = SelectDictType(*type, node.Child(1)->GetTypeAnn());
        const auto factory = *type == EDictType::Hashed ? &TProgramBuilder::NarrowSqueezeToHashedDict : &TProgramBuilder::NarrowSqueezeToSortedDict;
        return (ctx.ProgramBuilder.*factory)(stream, *isMany, [&](TRuntimeNode::TList items) {
            return MkqlBuildLambda(*node.Child(1), ctx, items);
        }, [&](TRuntimeNode::TList items) {
            return MkqlBuildLambda(*node.Child(2), ctx, items);
        }, isCompact, itemsCount.GetOrElse(0));
    });

    AddCallable("GroupByKey", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list = MkqlBuildExpr(node.Head(), ctx);
        const auto dict = ctx.ProgramBuilder.ToHashedDict(list, true, [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(1), ctx, {item});
        }, [&](TRuntimeNode item) {
            return item;
        });

        const auto values = ctx.ProgramBuilder.DictItems(dict);
        return ctx.ProgramBuilder.FlatMap(values, [&](TRuntimeNode item) {
            const auto key = ctx.ProgramBuilder.Nth(item, 0);
            const auto payloadList = ctx.ProgramBuilder.Nth(item, 1);
            return MkqlBuildLambda(*node.Child(2), ctx, {key, payloadList});
        });
    });

    AddCallable("PartitionByKey", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const NNodes::TCoPartitionByKey partition(&node);
        const auto input = MkqlBuildExpr(partition.Input().Ref(), ctx);

        const auto makePartitions = [&](TRuntimeNode list) {
            return ctx.ProgramBuilder.Map(
                ctx.ProgramBuilder.DictItems(ctx.ProgramBuilder.ToHashedDict(list, true,
                    [&](TRuntimeNode item) { return MkqlBuildLambda(partition.KeySelectorLambda().Ref(), ctx, {item}); },
                    [&](TRuntimeNode item) { return item; }
                )),
                [&](TRuntimeNode pair) {
                    const auto payload = partition.SortDirections().Ref().IsCallable("Void") ?
                        ctx.ProgramBuilder.Nth(pair, 1):
                        ctx.ProgramBuilder.Sort(ctx.ProgramBuilder.Nth(pair, 1), MkqlBuildExpr(partition.SortDirections().Ref(), ctx),
                            [&](TRuntimeNode item) {
                                return MkqlBuildLambda(partition.SortKeySelectorLambda().Ref(), ctx, {item});
                            }
                        );
                    return ctx.ProgramBuilder.NewTuple({ctx.ProgramBuilder.Nth(pair, 0), ctx.ProgramBuilder.Iterator(payload, {list})});
                }
            );
        };

        switch (const auto kind = partition.Ref().GetTypeAnn()->GetKind()) {
            case ETypeAnnotationKind::Flow:
            case ETypeAnnotationKind::Stream: {
                const auto sorted = ctx.ProgramBuilder.FlatMap(
                    ctx.ProgramBuilder.Condense1(input,
                        [&](TRuntimeNode item) { return ctx.ProgramBuilder.AsList(item); },
                        [&](TRuntimeNode, TRuntimeNode) { return ctx.ProgramBuilder.NewDataLiteral(false); },
                        [&](TRuntimeNode item, TRuntimeNode state) { return ctx.ProgramBuilder.Append(state, item); }
                    ),
                    makePartitions
                );

                return ETypeAnnotationKind::Stream == kind ?MkqlBuildLambda(partition.ListHandlerLambda().Ref(), ctx, {sorted}):
                    ctx.ProgramBuilder.ToFlow(MkqlBuildLambda(partition.ListHandlerLambda().Ref(), ctx, {ctx.ProgramBuilder.FromFlow(sorted)}));
            }
            case ETypeAnnotationKind::List: {
                const auto sorted = ctx.ProgramBuilder.Iterator(makePartitions(input), {});
                return ctx.ProgramBuilder.Collect(MkqlBuildLambda(partition.ListHandlerLambda().Ref(), ctx, {sorted}));
            }
            default: break;
        }
        Y_ABORT("Wrong case.");
    });

    AddCallable("CombineByKey", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return CombineByKeyImpl(node, ctx);
    });

    AddCallable("Enumerate", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto arg = MkqlBuildExpr(node.Head(), ctx);
        TRuntimeNode start;
        if (node.ChildrenSize() > 1) {
            start = MkqlBuildExpr(*node.Child(1), ctx);
        } else {
            start = ctx.ProgramBuilder.NewDataLiteral<ui64>(0);
        }

        TRuntimeNode step;
        if (node.ChildrenSize() > 2) {
            step = MkqlBuildExpr(node.Tail(), ctx);
        } else {
            step = ctx.ProgramBuilder.NewDataLiteral<ui64>(1);
        }

        return ctx.ProgramBuilder.Enumerate(arg, start, step);
    });

    AddCallable("Dict", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto listType = BuildType(node.Head(), *node.Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
        const auto dictType = AS_TYPE(TDictType, listType);

        std::vector<std::pair<TRuntimeNode, TRuntimeNode>> items;
        for (size_t i = 1; i < node.ChildrenSize(); ++i) {
            const auto key = MkqlBuildExpr(node.Child(i)->Head(), ctx);
            const auto payload = MkqlBuildExpr(node.Child(i)->Tail(), ctx);
            items.emplace_back(key, payload);
        }

        return ctx.ProgramBuilder.NewDict(dictType, items);
    });

    AddCallable("Variant", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto varType = node.Child(2)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TVariantExprType>();
        const auto type = BuildType(*node.Child(2), *varType, ctx.ProgramBuilder);

        const auto item = MkqlBuildExpr(node.Head(), ctx);
        return varType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple ?
                ctx.ProgramBuilder.NewVariant(item, FromString<ui32>(node.Child(1)->Content()), type) :
                ctx.ProgramBuilder.NewVariant(item, node.Child(1)->Content(), type);
    });

    AddCallable("AsStruct", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        std::vector<std::pair<std::string_view, TRuntimeNode>> members;
        members.reserve(node.ChildrenSize());
        node.ForEachChild([&](const TExprNode& child){ members.emplace_back(child.Head().Content(), MkqlBuildExpr(child.Tail(), ctx)); });
        return ctx.ProgramBuilder.NewStruct(members);
    });

    AddCallable("AsDict", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        std::vector<std::pair<TRuntimeNode, TRuntimeNode>> items;
        items.reserve(node.ChildrenSize());
        node.ForEachChild([&](const TExprNode& child){ items.emplace_back(MkqlBuildExpr(*child.Child(0), ctx), MkqlBuildExpr(*child.Child(1), ctx)); });
        const auto dictType = ctx.ProgramBuilder.NewDictType(items[0].first.GetStaticType(), items[0].second.GetStaticType(), false);
        return ctx.ProgramBuilder.NewDict(dictType, items);
    });

    AddCallable("Ensure", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto value = MkqlBuildExpr(node.Head(), ctx);
        const auto predicate = MkqlBuildExpr(*node.Child(1), ctx);
        const auto message = node.ChildrenSize() > 2 ? MkqlBuildExpr(node.Tail(), ctx) : ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>("");
        const auto pos = ctx.ExprCtx.GetPosition(node.Pos());
        return ctx.ProgramBuilder.Ensure(value, predicate, message, pos.File, pos.Row, pos.Column);
    });

    AddCallable("Replicate", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto value = MkqlBuildExpr(node.Head(), ctx);
        const auto count = MkqlBuildExpr(*node.Child(1), ctx);
        const auto pos = ctx.ExprCtx.GetPosition(node.Pos());
        return ctx.ProgramBuilder.Replicate(value, count, pos.File, pos.Row, pos.Column);
    });

    AddCallable("IfPresent", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        TRuntimeNode::TList optionals;
        const auto width = node.ChildrenSize() - 2U;
        optionals.reserve(width);
        auto i = 0U;
        std::generate_n(std::back_inserter(optionals), width, [&](){ return MkqlBuildExpr(*node.Child(i++), ctx); });
        const auto elseBranch = MkqlBuildExpr(node.Tail(), ctx);
        return ctx.ProgramBuilder.IfPresent(optionals, [&](TRuntimeNode::TList items) {
            return MkqlBuildLambda(*node.Child(width), ctx, items);
        }, elseBranch);
    });

    AddCallable({"DataType",
                 "ListType",
                 "OptionalType",
                 "TupleType",
                 "StructType",
                 "DictType",
                 "VoidType",
                 "NullType",
                 "CallableType",
                 "UnitType",
                 "GenericType",
                 "ResourceType",
                 "TaggedType",
                 "VariantType",
                 "StreamType",
                 "FlowType",
                 "EmptyListType",
                 "EmptyDictType"},
        [](const TExprNode& node, TMkqlBuildContext& ctx) {
            const auto type = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
            return TRuntimeNode(type, true);
        });

    AddCallable("ParseType", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto type = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return TRuntimeNode(type, true);
    });

    AddCallable("TypeOf", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto type = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return TRuntimeNode(type, true);
    });

    AddCallable("EmptyList", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        Y_UNUSED(node);
        if (RuntimeVersion < 11) {
            return ctx.ProgramBuilder.NewEmptyList(ctx.ProgramBuilder.NewVoid().GetStaticType());
        } else {
            return TRuntimeNode(ctx.ProgramBuilder.GetTypeEnvironment().GetEmptyListLazy(), true);
        }
    });

    AddCallable("EmptyDict", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        Y_UNUSED(node);
        if (RuntimeVersion < 11) {
           auto voidType = ctx.ProgramBuilder.NewVoid().GetStaticType();
           auto dictType = ctx.ProgramBuilder.NewDictType(voidType, voidType, false);
           return ctx.ProgramBuilder.NewDict(dictType, {});
        } else {
           return TRuntimeNode(ctx.ProgramBuilder.GetTypeEnvironment().GetEmptyDictLazy(), true);
        }
    });

    AddCallable("SourceOf", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto type = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.SourceOf(type);
    });

    AddCallable("TypeHandle", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto type = node.Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        const auto yson = WriteTypeToYson(type);
        const auto retType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), node.Content(), retType);
        call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Yson>(yson));
        return TRuntimeNode(call.Build(), false);
    });

    AddCallable("ReprCode", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto type = node.Head().GetTypeAnn();
        const auto yson = WriteTypeToYson(type);
        const auto& args = GetAllArguments(node, ctx);
        const auto retType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        const auto pos = ctx.ExprCtx.GetPosition(node.Pos());
        TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), node.Content(), retType);
        call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(pos.File));
        call.Add(ctx.ProgramBuilder.NewDataLiteral(pos.Row));
        call.Add(ctx.ProgramBuilder.NewDataLiteral(pos.Column));

        for (auto arg : args) {
            call.Add(arg);
        }

        call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Yson>(yson));
        return TRuntimeNode(call.Build(), false);
    });

    // safe and position unaware
    AddCallable({
        "SerializeTypeHandle",
        "TypeKind",
        "FormatCode",
        "FormatCodeWithPositions",
        "SerializeCode",
        }, [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto& args = GetAllArguments(node, ctx);
        const auto retType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), node.Content(), retType);
        for (auto arg : args) {
            call.Add(arg);
        }

        return TRuntimeNode(call.Build(), false);
    });

    // with position
    AddCallable({
        "ParseTypeHandle",
        "DataTypeComponents",
        "DataTypeHandle",
        "OptionalItemType",
        "OptionalTypeHandle",
        "ListItemType",
        "ListTypeHandle",
        "StreamItemType",
        "StreamTypeHandle",
        "TupleTypeComponents",
        "TupleTypeHandle",
        "StructTypeComponents",
        "StructTypeHandle",
        "DictTypeComponents",
        "DictTypeHandle",
        "ResourceTypeTag",
        "ResourceTypeHandle",
        "TaggedTypeComponents",
        "TaggedTypeHandle",
        "VariantUnderlyingType",
        "VariantTypeHandle",
        "VoidTypeHandle",
        "NullTypeHandle",
        "EmptyListTypeHandle",
        "EmptyDictTypeHandle",
        "CallableTypeComponents",
        "CallableArgument",
        "CallableTypeHandle",
        "PgTypeName",
        "PgTypeHandle",
        "WorldCode",
        "AtomCode",
        "ListCode",
        "FuncCode",
        }, [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto& args = GetAllArguments(node, ctx);
        const auto retType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        const auto pos = ctx.ExprCtx.GetPosition(node.Pos());
        TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), node.Content(), retType);
        call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(pos.File));
        call.Add(ctx.ProgramBuilder.NewDataLiteral(pos.Row));
        call.Add(ctx.ProgramBuilder.NewDataLiteral(pos.Column));

        for (auto arg : args) {
            call.Add(arg);
        }

        return TRuntimeNode(call.Build(), false);
    });

    AddCallable("LambdaCode", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto lambda = node.Child(node.ChildrenSize() - 1);
        const auto retType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        const auto pos = ctx.ExprCtx.GetPosition(node.Pos());
        TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), node.Content(), retType);
        call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(pos.File));
        call.Add(ctx.ProgramBuilder.NewDataLiteral(pos.Row));
        call.Add(ctx.ProgramBuilder.NewDataLiteral(pos.Column));
        if (node.ChildrenSize() == 2) {
            auto count = MkqlBuildExpr(node.Head(), ctx);
            call.Add(count);
        } else {
            call.Add(ctx.ProgramBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id));
        }

        TRuntimeNode body;
        {
            TMkqlBuildContext::TArgumentsMap innerArguments;
            innerArguments.reserve(lambda->Head().ChildrenSize());
            lambda->Head().ForEachChild([&](const TExprNode& argNode) {
                const auto argType = BuildType(argNode, *argNode.GetTypeAnn(), ctx.ProgramBuilder);
                const auto arg = ctx.ProgramBuilder.Arg(argType);
                innerArguments.emplace(&argNode, arg);
                call.Add(arg);
            });

            TMkqlBuildContext innerCtx(ctx, std::move(innerArguments), lambda->UniqueId());
            body = MkqlBuildExpr(*lambda->Child(1), innerCtx);
        }

        call.Add(body);
        return TRuntimeNode(call.Build(), false);
    });


    AddCallable("FormatType", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        TRuntimeNode str;
        if (node.Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Resource) {
            auto handle = MkqlBuildExpr(node.Head(), ctx);
            TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), node.Content(), ctx.ProgramBuilder.NewDataType(NUdf::TDataType<char*>::Id));
            call.Add(handle);
            str = TRuntimeNode(call.Build(), false);
        } else {
            str = ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(FormatType(node.Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType()));
        }
        return str;
    });

    AddCallable("FormatTypeDiff", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        if (node.Child(0)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Resource) { // if we got resource + resource
            YQL_ENSURE(node.Child(1)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Resource);
            TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), node.Content(), ctx.ProgramBuilder.NewDataType(NUdf::TDataType<char*>::Id));
            call.Add(MkqlBuildExpr(*node.Child(0), ctx));
            call.Add(MkqlBuildExpr(*node.Child(1), ctx));
            call.Add(ctx.ProgramBuilder.NewDataLiteral(FromString<bool>(*node.Child(2), NUdf::EDataSlot::Bool)));
            return TRuntimeNode(call.Build(), false);
        } else { // if we got type + type
            bool pretty = FromString<bool>(*node.Child(2), NUdf::EDataSlot::Bool);
            const auto type_left = node.Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            const auto type_right = node.Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            return pretty ? ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(NYql::GetTypePrettyDiff(*type_left, *type_right)) :
                ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(NYql::GetTypeDiff(*type_left, *type_right));
        }
    });

    AddCallable("Void", [](const TExprNode&, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewVoid();
    });

    AddCallable("Null", [](const TExprNode&, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewNull();
    });

    AddCallable({ "AsTagged","Untag" }, [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto input = MkqlBuildExpr(node.Head(), ctx);
        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.Nop(input, returnType);
    });

    AddCallable({"WithWorld"}, [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return MkqlBuildExpr(node.Head(), ctx);
    });

    AddCallable("Error", [](const TExprNode& node, TMkqlBuildContext& ctx)->NKikimr::NMiniKQL::TRuntimeNode {
        const auto err = node.GetTypeAnn()->Cast<TErrorExprType>()->GetError();
        ythrow TNodeException(ctx.ExprCtx.AppendPosition(err.Position)) << err.GetMessage();
    });

    AddCallable("ErrorType", [](const TExprNode& node, TMkqlBuildContext& ctx)->NKikimr::NMiniKQL::TRuntimeNode {
        const auto err = node.GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TErrorExprType>()->GetError();
        ythrow TNodeException(ctx.ExprCtx.AppendPosition(err.Position)) << err.GetMessage();
    });

    AddCallable("Join", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto list1 = MkqlBuildExpr(node.Head(), ctx);
        const auto list2 = MkqlBuildExpr(*node.Child(1), ctx);
        const auto dict1 = ctx.ProgramBuilder.ToHashedDict(list1, true, [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(2), ctx, {item});
        }, [&](TRuntimeNode item) {
            return item;
        });

        const auto dict2 = ctx.ProgramBuilder.ToHashedDict(list2, true, [&](TRuntimeNode item) {
            return MkqlBuildLambda(*node.Child(3), ctx, {item});
        }, [&](TRuntimeNode item) {
            return item;
        });

        const auto joinKind = GetJoinKind(node, node.Child(4)->Content());
        return ctx.ProgramBuilder.JoinDict(dict1, true, dict2, true, joinKind);
    });

    AddCallable("JoinDict", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto dict1 = MkqlBuildExpr(*node.Child(0), ctx);
        const auto dict2 = MkqlBuildExpr(*node.Child(1), ctx);
        const auto joinKind = GetJoinKind(node, node.Child(2)->Content());

        bool multi1 = true, multi2 = true;
        if (node.ChildrenSize() > 3) {
            node.Tail().ForEachChild([&](const TExprNode& flag){
                if (const auto& content = flag.Content(); content == "LeftUnique")
                    multi1 = false;
                else if ( content == "RightUnique")
                    multi2 = false;
            });
        }

        return ctx.ProgramBuilder.JoinDict(dict1, multi1, dict2, multi2, joinKind);
    });

    AddCallable({"FilePath", "FileContent", "FolderPath"}, [](const TExprNode& node, TMkqlBuildContext& ctx) {
        TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), node.Content(), ctx.ProgramBuilder.NewDataType(NUdf::TDataType<char*>::Id));
        call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(node.Head().Content()));
        return TRuntimeNode(call.Build(), false);
    });

    AddCallable("TablePath", [](const TExprNode&, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>("");
    });

    AddCallable("TableRecord", [](const TExprNode&, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral<ui64>(0);
    });

    AddCallable("Udf", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        YQL_ENSURE(node.ChildrenSize() == 8);
        std::string_view function = node.Head().Content();
        const auto runConfig = MkqlBuildExpr(*node.Child(1), ctx);
        const auto userType = BuildType(*node.Child(2), *node.Child(2)->GetTypeAnn(), ctx.ProgramBuilder);
        const auto typeConfig = node.Child(3)->Content();
        const auto callableType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        const auto pos = ctx.ExprCtx.GetPosition(node.Pos());
        return ctx.ProgramBuilder.TypedUdf(function, callableType, runConfig, userType, typeConfig,
            pos.File, pos.Row, pos.Column);
    });

    AddCallable("ScriptUdf", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        EScriptType scriptType = ScriptTypeFromStr(node.Head().Content());
        if (scriptType == EScriptType::Unknown) {
            ythrow TNodeException(node.Head())
                << "Unknown script type '"
                << node.Head().Content() << '\'';
        }

        std::string_view funcName = node.Child(1)->Content();
        const auto typeNode = node.Child(2);
        const auto funcType = BuildType(*typeNode, *typeNode->GetTypeAnn(), ctx.ProgramBuilder);
        const auto script = MkqlBuildExpr(*node.Child(3), ctx);
        const auto pos = ctx.ExprCtx.GetPosition(node.Pos());
        return ctx.ProgramBuilder.ScriptUdf(node.Head().Content(), funcName, funcType, script,
            pos.File, pos.Row, pos.Column);
    });

    AddCallable("Apply", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto pos = ctx.ExprCtx.GetPosition(node.Pos());
        const auto callable = MkqlBuildExpr(node.Head(), ctx);
        const auto& args = GetArgumentsFrom<1U>(node, ctx);
        return ctx.ProgramBuilder.Apply(callable, args, pos.File, pos.Row, pos.Column);
    });

    AddCallable("NamedApply", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto pos = ctx.ExprCtx.GetPosition(node.Pos());
        const auto callable = MkqlBuildExpr(node.Head(), ctx);
        const auto positionalArgs = MkqlBuildExpr(*node.Child(1), ctx);
        const auto namedArgs = MkqlBuildExpr(*node.Child(2), ctx);
        const auto dependentNodes = node.ChildrenSize() - 3;
        const auto callableType = node.Head().GetTypeAnn()->Cast<TCallableExprType>();
        const auto tupleType = node.Child(1)->GetTypeAnn()->Cast<TTupleExprType>();
        const auto structType = node.Child(2)->GetTypeAnn()->Cast<TStructExprType>();
        std::vector<TRuntimeNode> args(callableType->GetArgumentsSize() + dependentNodes);
        for (size_t i = 0; i < tupleType->GetSize(); ++i) {
            args[i] = node.Child(1)->IsList() ?
                MkqlBuildExpr(*node.Child(1)->Child(i), ctx):
                ctx.ProgramBuilder.Nth(positionalArgs, i);
        }

        for (size_t i = 0; i < structType->GetSize(); ++i) {
            auto memberName = structType->GetItems()[i]->GetName();
            auto index = callableType->ArgumentIndexByName(memberName);
            if (!index || *index < tupleType->GetSize()) {
                ythrow TNodeException(node.Child(2)) << "Wrong named argument: " << memberName;
            }

            TRuntimeNode arg;
            if (node.Child(2)->IsCallable("AsStruct")) {
                for (auto& child : node.Child(2)->Children()) {
                    if (child->Head().Content() == memberName) {
                        arg = MkqlBuildExpr(child->Tail(), ctx);
                        break;
                    }
                }

                if (!arg.GetNode()) {
                    ythrow TNodeException(node.Child(2)) << "Missing argument: " << memberName;
                }
            }
            else {
                arg = ctx.ProgramBuilder.Member(namedArgs, memberName);
            }

            args[*index] = arg;
        }

        for (ui32 i = tupleType->GetSize(); i < callableType->GetArgumentsSize(); ++i) {
            auto& arg = args[i];
            if (arg.GetNode()) {
                continue;
            }

            auto mkqlType = BuildType(node, *callableType->GetArguments()[i].Type, ctx.ProgramBuilder);
            arg = ctx.ProgramBuilder.NewEmptyOptional(mkqlType);
        }

        for (ui32 i = 0; i < dependentNodes; ++i) {
            args[callableType->GetArgumentsSize() + i] = MkqlBuildExpr(*node.Child(3 + i), ctx);
        }

        return ctx.ProgramBuilder.Apply(callable, args, pos.File, pos.Row, pos.Column, dependentNodes);
    });

    AddCallable("Callable", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto callableType = BuildType(node.Head(), *node.Head().GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.Callable(callableType, [&](const TArrayRef<const TRuntimeNode>& args) {
            const auto& lambda = node.Tail();
            TMkqlBuildContext::TArgumentsMap innerArguments;
            innerArguments.reserve(lambda.Head().ChildrenSize());
            MKQL_ENSURE(args.size() == lambda.Head().ChildrenSize(), "Mismatch of lambda arguments count");
            auto it = args.cbegin();
            lambda.Head().ForEachChild([&](const TExprNode& arg){ innerArguments.emplace(&arg, *it++); });
            TMkqlBuildContext innerCtx(ctx, std::move(innerArguments), lambda.UniqueId());
            return MkqlBuildExpr(lambda.Tail(), innerCtx);
        });
    });

    AddCallable("PgConst", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto type = AS_TYPE(TPgType, BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder));
        TRuntimeNode typeMod;
        if (node.ChildrenSize() >= 3) {
            typeMod = MkqlBuildExpr(*node.Child(2), ctx);
        }

        auto typeMod1 = typeMod;
        if (node.GetTypeAnn()->Cast<TPgExprType>()->GetName() != "interval" &&
            node.GetTypeAnn()->Cast<TPgExprType>()->GetName() != "_interval") {
            typeMod1 = TRuntimeNode();
        }

        auto ret = ctx.ProgramBuilder.PgConst(type, node.Head().Content(), typeMod1);
        if (node.ChildrenSize() >= 3) {
            return ctx.ProgramBuilder.PgCast(ret, type, typeMod);
        } else {
            return ret;
        }
    });

    AddCallable("PgInternal0", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.PgInternal0(returnType);
    });

    AddCallable({"PgResolvedCall","PgResolvedCallCtx" }, [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto name = node.Head().Content();
        auto id = FromString<ui32>(node.Child(1)->Content());
        std::vector<TRuntimeNode> args;
        args.reserve(node.ChildrenSize() - 3);
        for (ui32 i = 3; i < node.ChildrenSize(); ++i) {
            args.push_back(MkqlBuildExpr(*node.Child(i), ctx));
        }

        bool rangeFunction = false;
        for (const auto& child : node.Child(2)->Children()) {
            if (child->Head().Content() == "range") {
                rangeFunction = true;
            }
        }

        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.PgResolvedCall(node.IsCallable("PgResolvedCallCtx"), name, id, args, returnType, rangeFunction);
    });

    AddCallable("PgResolvedOp", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto operId = FromString<ui32>(node.Child(1)->Content());
        auto procId = NPg::LookupOper(operId).ProcId;
        auto procName = NPg::LookupProc(procId).Name;
        std::vector<TRuntimeNode> args;
        args.reserve(node.ChildrenSize() - 2);
        for (ui32 i = 2; i < node.ChildrenSize(); ++i) {
            args.push_back(MkqlBuildExpr(*node.Child(i), ctx));
        }

        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.PgResolvedCall(false, procName, procId, args, returnType, false);
    });

    AddCallable("BlockPgResolvedCall", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto name = node.Head().Content();
        auto id = FromString<ui32>(node.Child(1)->Content());
        std::vector<TRuntimeNode> args;
        args.reserve(node.ChildrenSize() - 3);
        for (ui32 i = 3; i < node.ChildrenSize(); ++i) {
            args.push_back(MkqlBuildExpr(*node.Child(i), ctx));
        }

        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.BlockPgResolvedCall(name, id, args, returnType);
    });

    AddCallable("BlockPgResolvedOp", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto operId = FromString<ui32>(node.Child(1)->Content());
        auto procId = NPg::LookupOper(operId).ProcId;
        auto procName = NPg::LookupProc(procId).Name;
        std::vector<TRuntimeNode> args;
        args.reserve(node.ChildrenSize() - 2);
        for (ui32 i = 2; i < node.ChildrenSize(); ++i) {
            args.push_back(MkqlBuildExpr(*node.Child(i), ctx));
        }

        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.BlockPgResolvedCall(procName, procId, args, returnType);
    });


    AddCallable("PgCast", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto input = MkqlBuildExpr(*node.Child(0), ctx);
        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        TRuntimeNode typeMod;
        if (node.ChildrenSize() >= 3) {
            typeMod = MkqlBuildExpr(*node.Child(2), ctx);
        }

        auto typeMod1 = typeMod;
        if (node.GetTypeAnn()->Cast<TPgExprType>()->GetName() != "interval" &&
            node.GetTypeAnn()->Cast<TPgExprType>()->GetName() != "_interval") {
            typeMod1 = TRuntimeNode();
        }

        if (node.Head().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Null) {
            auto sourceTypeId = node.Head().GetTypeAnn()->Cast<TPgExprType>()->GetId();
            auto targetTypeId = node.GetTypeAnn()->Cast<TPgExprType>()->GetId();
            const auto& sourceTypeDesc = NPg::LookupType(sourceTypeId);
            const auto& targetTypeDesc = NPg::LookupType(targetTypeId);
            const bool isSourceArray = sourceTypeDesc.TypeId == sourceTypeDesc.ArrayTypeId;
            const bool isTargetArray = targetTypeDesc.TypeId == targetTypeDesc.ArrayTypeId;
            if (isSourceArray == isTargetArray && NPg::HasCast(
                isSourceArray ? sourceTypeDesc.ElementTypeId : sourceTypeId,
                isTargetArray ? targetTypeDesc.ElementTypeId : targetTypeId)) {
                typeMod1 = typeMod;
            }
        }

        auto cast = ctx.ProgramBuilder.PgCast(input, returnType, typeMod1);
        if (node.ChildrenSize() >= 3) {
            return ctx.ProgramBuilder.PgCast(cast, returnType, typeMod);
        } else {
            return cast;
        }
    });

    AddCallable("FromPg", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto input = MkqlBuildExpr(*node.Child(0), ctx);
        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.FromPg(input, returnType);
    });

    AddCallable("ToPg", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto input = MkqlBuildExpr(*node.Child(0), ctx);
        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.ToPg(input, returnType);
    });

    AddCallable("BlockFromPg", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto input = MkqlBuildExpr(*node.Child(0), ctx);
        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.BlockFromPg(input, returnType);
    });

    AddCallable("BlockToPg", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto input = MkqlBuildExpr(*node.Child(0), ctx);
        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.BlockToPg(input, returnType);
    });

    AddCallable("PgClone", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto input = MkqlBuildExpr(*node.Child(0), ctx);
        if (IsNull(node.Head())) {
            return input;
        }

        if (NPg::LookupType(node.GetTypeAnn()->Cast<TPgExprType>()->GetId()).PassByValue) {
            return input;
        }

        TVector<TRuntimeNode> dependentNodes;
        for (ui32 i = 1; i < node.ChildrenSize(); ++i) {
            dependentNodes.push_back(MkqlBuildExpr(*node.Child(i), ctx));
        }

        return ctx.ProgramBuilder.PgClone(input, dependentNodes);
    });

    AddCallable("PgTableContent", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.PgTableContent(
            node.Child(0)->Content(),
            node.Child(1)->Content(),
            returnType);
    });

    AddCallable("PgToRecord", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto input = MkqlBuildExpr(*node.Child(0), ctx);
        TVector<std::pair<std::string_view, std::string_view>> members;
        for (auto child : node.Child(1)->Children()) {
            members.push_back({child->Head().Content(), child->Tail().Content()});
        }

        return ctx.ProgramBuilder.PgToRecord(input, members);
    });

    AddCallable("WithContext", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto input = MkqlBuildExpr(*node.Child(0), ctx);
        return ctx.ProgramBuilder.WithContext(input, node.Child(1)->Content());
    });

    AddCallable("BlockFunc", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        TVector<TRuntimeNode> args;
        for (ui32 i = 2; i < node.ChildrenSize(); ++i) {
            args.push_back(MkqlBuildExpr(*node.Child(i), ctx));
        }

        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.BlockFunc(node.Child(0)->Content(), returnType, args);
    });

    AddCallable("BlockBitCast", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto arg = MkqlBuildExpr(*node.Child(0), ctx);
        auto targetType = BuildType(node, *node.Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.BlockBitCast(arg, targetType);
    });

    AddCallable("BlockMember", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto structObj = MkqlBuildExpr(node.Head(), ctx);
        const auto name = node.Tail().Content();
        return ctx.ProgramBuilder.BlockMember(structObj, name);
    });

    AddCallable("BlockNth", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto tupleObj = MkqlBuildExpr(node.Head(), ctx);
        const auto index = FromString<ui32>(node.Tail().Content());
        return ctx.ProgramBuilder.BlockNth(tupleObj, index);
    });

    AddCallable("BlockAsStruct", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        std::vector<std::pair<std::string_view, TRuntimeNode>> members;
        for (const auto& x : node.Children()) {
            members.emplace_back(x->Head().Content(), MkqlBuildExpr(x->Tail(), ctx));
        }
        return ctx.ProgramBuilder.BlockAsStruct(members);
    });

    AddCallable("BlockAsTuple", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        TVector<TRuntimeNode> args;
        for (const auto& x : node.Children()) {
            args.push_back(MkqlBuildExpr(*x, ctx));
        }

        return ctx.ProgramBuilder.BlockAsTuple(args);
    });

    AddCallable("BlockCombineAll", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto arg = MkqlBuildExpr(*node.Child(0), ctx);
        std::optional<ui32> filterColumn;
        if (!node.Child(1)->IsCallable("Void")) {
            filterColumn = FromString<ui32>(node.Child(1)->Content());
        }

        TVector<TAggInfo> aggs;
        for (const auto& agg : node.Child(2)->Children()) {
            TAggInfo info;
            info.Name = TString(agg->Head().Head().Content());
            for (ui32 i = 1; i < agg->ChildrenSize(); ++i) {
                info.ArgsColumns.push_back(FromString<ui32>(agg->Child(i)->Content()));
            }

            aggs.push_back(info);
        }

        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.BlockCombineAll(arg, filterColumn, aggs, returnType);
    });

    AddCallable("BlockCombineHashed", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto arg = MkqlBuildExpr(*node.Child(0), ctx);
        std::optional<ui32> filterColumn;
        if (!node.Child(1)->IsCallable("Void")) {
            filterColumn = FromString<ui32>(node.Child(1)->Content());
        }

        TVector<ui32> keys;
        for (const auto& key : node.Child(2)->Children()) {
            keys.push_back(FromString<ui32>(key->Content()));
        }

        TVector<TAggInfo> aggs;
        for (const auto& agg : node.Child(3)->Children()) {
            TAggInfo info;
            info.Name = TString(agg->Head().Head().Content());
            for (ui32 i = 1; i < agg->ChildrenSize(); ++i) {
                info.ArgsColumns.push_back(FromString<ui32>(agg->Child(i)->Content()));
            }

            aggs.push_back(info);
        }

        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.BlockCombineHashed(arg, filterColumn, keys, aggs, returnType);
    });

    AddCallable("BlockMergeFinalizeHashed", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto arg = MkqlBuildExpr(*node.Child(0), ctx);
        TVector<ui32> keys;
        for (const auto& key : node.Child(1)->Children()) {
            keys.push_back(FromString<ui32>(key->Content()));
        }

        TVector<TAggInfo> aggs;
        for (const auto& agg : node.Child(2)->Children()) {
            TAggInfo info;
            info.Name = TString(agg->Head().Head().Content());
            for (ui32 i = 1; i < agg->ChildrenSize(); ++i) {
                info.ArgsColumns.push_back(FromString<ui32>(agg->Child(i)->Content()));
            }

            aggs.push_back(info);
        }

        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.BlockMergeFinalizeHashed(arg, keys, aggs, returnType);
    });

    AddCallable("BlockMergeManyFinalizeHashed", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        auto arg = MkqlBuildExpr(*node.Child(0), ctx);
        TVector<ui32> keys;
        for (const auto& key : node.Child(1)->Children()) {
            keys.push_back(FromString<ui32>(key->Content()));
        }

        TVector<TAggInfo> aggs;
        for (const auto& agg : node.Child(2)->Children()) {
            TAggInfo info;
            info.Name = TString(agg->Head().Head().Content());
            for (ui32 i = 1; i < agg->ChildrenSize(); ++i) {
                info.ArgsColumns.push_back(FromString<ui32>(agg->Child(i)->Content()));
            }

            aggs.push_back(info);
        }

        ui32 streamIndex = FromString<ui32>(node.Child(3)->Content());
        TVector<TVector<ui32>> streams;
        for (const auto& child : node.Child(4)->Children()) {
            auto& stream = streams.emplace_back();
            for (const auto& atom : child->Children()) {
                stream.emplace_back(FromString<ui32>(atom->Content()));
            }
        }

        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.BlockMergeManyFinalizeHashed(arg, keys, aggs, streamIndex, streams, returnType);
    });

    AddCallable("BlockCompress", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto flow = MkqlBuildExpr(node.Head(), ctx);
        const auto index = FromString<ui32>(node.Child(1)->Content());
        return ctx.ProgramBuilder.BlockCompress(flow, index);
    });

    AddCallable("BlockExpandChunked", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto flow = MkqlBuildExpr(node.Head(), ctx);
        return ctx.ProgramBuilder.BlockExpandChunked(flow);
    });

    AddCallable("PgArray", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        std::vector<TRuntimeNode> args;
        args.reserve(node.ChildrenSize());
        for (ui32 i = 0; i < node.ChildrenSize(); ++i) {
            args.push_back(MkqlBuildExpr(*node.Child(i), ctx));
        }

        auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.PgArray(args, returnType);
    });

    AddCallable("QueueCreate", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto initCapacity = MkqlBuildExpr(*node.Child(1), ctx);
        const auto initSize = MkqlBuildExpr(*node.Child(2), ctx);
        const auto& args = GetArgumentsFrom<3U>(node, ctx);
        const auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.QueueCreate(initCapacity, initSize, args, returnType);
    });

    AddCallable("QueuePeek", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto resource = MkqlBuildExpr(node.Head(), ctx);
        const auto index = MkqlBuildExpr(*node.Child(1), ctx);
        const auto& args = GetArgumentsFrom<2U>(node, ctx);
        const auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.QueuePeek(resource, index, args, returnType);
    });

    AddCallable("QueueRange", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto resource = MkqlBuildExpr(node.Head(), ctx);
        const auto begin = MkqlBuildExpr(*node.Child(1), ctx);
        const auto end = MkqlBuildExpr(*node.Child(2), ctx);
        const auto& args = GetArgumentsFrom<3U>(node, ctx);
        const auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.QueueRange(resource, begin, end, args, returnType);
    });

    AddCallable("Seq", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto& args = GetArgumentsFrom<0U>(node, ctx);
        const auto returnType = BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        return ctx.ProgramBuilder.Seq(args, returnType);
    });

    AddCallable("FromYsonSimpleType", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto input = MkqlBuildExpr(node.Head(), ctx);
        const auto schemeType = ParseDataType(node, node.Child(1)->Content());
        return ctx.ProgramBuilder.FromYsonSimpleType(input, schemeType);
    });

    AddCallable("TryWeakMemberFromDict", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto other = MkqlBuildExpr(node.Head(), ctx);
        const auto rest = MkqlBuildExpr(*node.Child(1), ctx);
        const auto schemeType = ParseDataType(node, node.Child(2)->Content());
        const auto member = node.Child(3)->Content();
        return ctx.ProgramBuilder.TryWeakMemberFromDict(other, rest, schemeType, member);
    });

    AddCallable("DependsOn", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return MkqlBuildExpr(node.Head(), ctx);
    });

    AddCallable("Parameter", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const NNodes::TCoParameter parameter(&node);
        return ctx.ProgramBuilder.Member(ctx.Parameters, parameter.Name());
    });

    AddCallable("SecureParam", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(node.Head().Content());
    });

    AddCallable(SkippableCallables, [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return MkqlBuildExpr(node.Head(), ctx);
    });

    AddCallable({ "AssumeStrict", "AssumeNonStrict", "Likely" }, [](const TExprNode& node, TMkqlBuildContext& ctx) {
        return MkqlBuildExpr(node.Head(), ctx);
    });

    AddCallable("Merge", [](const TExprNode& node, TMkqlBuildContext& ctx) {
        const auto& args = GetAllArguments(node, ctx);
        auto extend = ctx.ProgramBuilder.Extend(args);

        if (auto sortConstr = node.GetConstraint<TSortedConstraintNode>()) {
            const auto input = MkqlBuildExpr(node.Head(), ctx);
            const auto& content = sortConstr->GetContent();
            std::vector<TRuntimeNode> ascending;
            ascending.reserve(content.size());
            for (const auto& c: content) {
                ascending.push_back(ctx.ProgramBuilder.NewDataLiteral(c.second));
            }
            TProgramBuilder::TUnaryLambda keyExractor = [&](TRuntimeNode item) {
                std::vector<TRuntimeNode> keys;
                keys.reserve(content.size());
                for (const auto& c : content) {
                    if (c.first.front().empty())
                         keys.push_back(item);
                    else {
                        MKQL_ENSURE(c.first.front().size() == 1U, "Just column expected.");
                        keys.push_back(ctx.ProgramBuilder.Member(item, c.first.front().front()));
                    }
                }
                return ctx.ProgramBuilder.NewTuple(keys);
            };
            return ctx.ProgramBuilder.Sort(extend, ctx.ProgramBuilder.NewTuple(ascending), keyExractor);
        }
        else {
            return extend;
        }
    });
}

TRuntimeNode MkqlBuildLambda(const TExprNode& lambda, TMkqlBuildContext& ctx, const TRuntimeNode::TList& args) {
    MKQL_ENSURE(2U == lambda.ChildrenSize(), "Wide lambda isn't supported.");
    TMkqlBuildContext::TArgumentsMap innerArguments;
    innerArguments.reserve(args.size());
    auto it = args.begin();
    lambda.Head().ForEachChild([&](const TExprNode& child){ innerArguments.emplace(&child, *it++); });
    TMkqlBuildContext innerCtx(ctx, std::move(innerArguments), lambda.UniqueId());
    return MkqlBuildExpr(lambda.Tail(), innerCtx);
}

TRuntimeNode::TList MkqlBuildWideLambda(const TExprNode& lambda, TMkqlBuildContext& ctx, const TRuntimeNode::TList& args) {
    MKQL_ENSURE(0U < lambda.ChildrenSize(), "Empty lambda.");
    TMkqlBuildContext::TArgumentsMap innerArguments;
    innerArguments.reserve(args.size());
    auto it = args.begin();
    lambda.Head().ForEachChild([&](const TExprNode& child){ innerArguments.emplace(&child, *it++); });
    TMkqlBuildContext innerCtx(ctx, std::move(innerArguments), lambda.UniqueId());
    TRuntimeNode::TList result;
    result.reserve(lambda.ChildrenSize() - 1U);
    for (ui32 i = 1U; i < lambda.ChildrenSize(); ++i)
        result.emplace_back(MkqlBuildExpr(*lambda.Child(i), innerCtx));
    return result;
}

TRuntimeNode MkqlBuildExpr(const TExprNode& node, TMkqlBuildContext& ctx) {
    for (auto currCtx = &ctx; currCtx; currCtx = currCtx->ParentCtx) {
        const auto knownNode = currCtx->Memoization.find(&node);
        if (currCtx->Memoization.cend() != knownNode) {
            return knownNode->second;
        }
    }

    switch (const auto type = node.Type()) {
    case TExprNode::List:
        return CheckTypeAndMemoize(node, ctx, ctx.ProgramBuilder.NewTuple(GetAllArguments(node, ctx)));
    case TExprNode::Callable:
        return CheckTypeAndMemoize(node, ctx, ctx.MkqlCompiler.GetCallable(node.Content())(node, ctx));
    case TExprNode::Argument:
        ythrow TNodeException(node) << "Unexpected argument: " << node.Content();
    default:
        ythrow TNodeException(node) << "Unexpected node type: " << type;
    }
}

} // namespace NCommon
} // namespace NYql
