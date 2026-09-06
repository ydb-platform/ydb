#include "kqp_rbo_physical_map_builder.h"
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_sqlselect.h>
#include <yql/essentials/core/yql_window_features.h>
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace {

TString ResolveWindowKey(const TExprNode& lambda, const TExpression::TWindowMetadata& metadata) {
    Y_ENSURE(lambda.IsLambda() && lambda.ChildrenSize() == 2 &&
        lambda.Head().IsArguments() && lambda.Head().ChildrenSize() == 1,
        "Physical window key must be a unary lambda");
    const auto& body = lambda.Tail();
    const bool member = body.IsCallable("Member") && body.ChildrenSize() == 2;
    const bool group = body.IsCallable("YqlGroupRef") && body.ChildrenSize() == 4;
    Y_ENSURE((member || group) && body.Child(0) == lambda.Head().Child(0) && body.Tail().IsAtom(),
        "Physical window key must be a direct input column");
    TInfoUnit key(TString(body.Tail().Content()));
    for (const auto& renames : metadata.RenameHistory) {
        if (const auto it = renames.find(key); it != renames.end()) {
            key = it->second;
        }
    }
    return key.GetFullName();
}

bool IsWholePartitionFrame(const TExprNode& frame) {
    if (!frame.IsList()) {
        return false;
    }
    if (!frame.ChildrenSize()) {
        return true; // The unordered SQL default is the complete partition.
    }
    if (frame.ChildrenSize() != 3) {
        return false;
    }
    THashMap<TString, TString> settings;
    for (const auto& setting : frame.Children()) {
        if (!setting->IsList() || setting->ChildrenSize() != 2 ||
            !setting->Head().IsAtom() || !setting->Tail().IsAtom() ||
            !settings.emplace(TString(setting->Head().Content()), TString(setting->Tail().Content())).second)
        {
            return false;
        }
    }
    return settings.contains("type") && settings.at("type") == "rows" &&
        settings.contains("from") && settings.at("from") == "up" &&
        settings.contains("to") && settings.at("to") == "uf";
}

// Materialize only the transported native window families. Their source
// relation is fixed by stage assignment; scalar projection runs afterwards.
// CalcOverWindow delegates peer, NULL, frame, and aggregate arithmetic semantics
// to the same native lowering used by ordinary YqlSelect.
TExprNode::TPtr LowerWindows(TOpMap& map, TExprNode::TPtr input,
    TVector<TInfoUnit>& inputColumns, THashMap<const TExprNode*, TString>& results,
    TExprContext& ctx, TPositionHandle pos, TTypeAnnotationContext& types)
{
    THashSet<TString> names;
    THashSet<TString> available;
    for (const auto& column : inputColumns) {
        names.insert(column.GetFullName());
        available.insert(column.GetFullName());
    }
    for (const auto& column : map.GetOutputIUs()) {
        names.insert(column.GetFullName());
    }
    TExprNode::TPtr squeezedInput;
    TExprNode::TPtr listArgument;
    for (const auto& element : map.MapElements) {
        const auto& expression = element.GetExpression();
        if (!expression.HasWindowSemantics()) {
            continue;
        }
        const auto& metadata = expression.GetWindowMetadata();
        Y_ENSURE(metadata && metadata->Definition, "Physical window requires its tracked source definition");
        const auto& definition = *metadata->Definition;
        const auto calls = FindNodes(expression.GetExpressionBody(), [](const TExprNode::TPtr& node) {
            return node->IsCallable({"YqlWin", "YqlAggWin"});
        });
        Y_ENSURE(calls.size() == 1, "Physical window expression requires one tracked window call");
        const auto call = calls.front();
        Y_ENSURE(definition.IsCallable("YqlWindow") && definition.ChildrenSize() == 5 &&
            definition.Child(0)->IsAtom() && definition.Child(1)->IsAtom("") &&
            definition.Child(2)->IsList() && definition.Child(3)->IsList() &&
            call->ChildrenSize() >= 4 && call->Child(1)->IsAtom(definition.Child(0)->Content()) &&
            call->Child(2)->IsList() && call->Child(2)->ChildrenSize() == 0,
            "Physical window has an unsupported source definition");
        const bool rank = call->IsCallable("YqlWin") && call->ChildrenSize() == 4 &&
            call->Head().IsAtom("rank") && definition.Child(3)->ChildrenSize() > 0;
        const bool aggregate = call->IsCallable("YqlAggWin") && call->ChildrenSize() == 5 &&
            call->Head().IsCallable("YqlWinFactory") && call->Head().ChildrenSize() == 1 &&
            (call->Head().Head().IsAtom("avg") || call->Head().Head().IsAtom("sum")) &&
            definition.Child(3)->ChildrenSize() == 0 && IsWholePartitionFrame(definition.Tail());
        Y_ENSURE(rank || aggregate, "Physical window supports ANSI Rank and unordered whole-partition AVG/SUM");
        if (!squeezedInput) {
            // DQ inputs can yield. SqueezeToList preserves its accumulator
            // across yields and emits one completed list (also for empty input).
            // Native list windows run only inside that completed-list handler.
            squeezedInput = ctx.NewCallable(pos, "SqueezeToList", {
                ctx.NewCallable(pos, "ToFlow", {input}),
            });
            listArgument = ctx.NewArgument(pos, "window_input");
            input = listArgument;
        }
        auto listType = ctx.NewCallable(pos, "TypeOf", {input});
        TExprNode::TListType partitions;
        for (const auto& key : definition.Child(2)->Children()) {
            Y_ENSURE(key->IsCallable("YqlGroup") && key->ChildrenSize() == 2);
            const TString name = ResolveWindowKey(key->Tail(), *metadata);
            Y_ENSURE(available.contains(name), "Physical window partition key is unavailable: " << name);
            partitions.push_back(ctx.NewAtom(pos, name));
        }
        auto row = ctx.NewArgument(pos, "window_row");
        TExprNode::TListType keys, directions;
        for (const auto& sort : definition.Child(3)->Children()) {
            Y_ENSURE(sort->IsCallable("YqlSort") && sort->ChildrenSize() == 4 &&
                (sort->Child(2)->IsAtom("asc") || sort->Child(2)->IsAtom("desc")) &&
                (sort->Child(3)->IsAtom("first") || sort->Child(3)->IsAtom("last")),
                "Physical window order must specify direction and NULL placement");
            const TString name = ResolveWindowKey(*sort->Child(1), *metadata);
            Y_ENSURE(available.contains(name), "Physical window order key is unavailable: " << name);
            auto member = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, name)});
            // YqlSort's NULL flag describes the ascending key; DESC reverses
            // it too (BuildSortTraits). Normalize it before separating the
            // presence and value directions, including for tuple keys.
            const bool nullsFirst = sort->Child(2)->IsAtom("asc") == sort->Child(3)->IsAtom("first");
            keys.push_back(ctx.NewCallable(pos, "Exists", {member}));
            directions.push_back(ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, nullsFirst ? "true" : "false")}));
            keys.push_back(member);
            directions.push_back(ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, sort->Child(2)->IsAtom("asc") ? "true" : "false")}));
        }
        auto keyLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), ctx.NewList(pos, std::move(keys)));
        auto sortTraits = rank
            ? ctx.NewCallable(pos, "SortTraits", {listType, ctx.NewList(pos, std::move(directions)), keyLambda})
            : ctx.NewCallable(pos, "Void", {});
        TExprNode::TPtr traits;
        if (rank) {
            traits = ctx.NewCallable(pos, "Rank", {listType, keyLambda,
                ctx.NewList(pos, {ctx.NewList(pos, {ctx.NewAtom(pos, "ansi")})})});
        } else {
            auto value = ctx.ReplaceNode(call->ChildPtr(4), *expression.Node->Head().Child(0), row);
            auto extractor = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(value));
            traits = ExpandYqlTraitsFactory(call->HeadPtr(), listType, extractor, ctx, types);
        }
        TString scratch;
        for (size_t ordinal = names.size();; ++ordinal) {
            scratch = "__kqp_rbo_window_" + ToString(ordinal);
            if (names.insert(scratch).second) {
                break;
            }
        }
        TExprNode::TListType settings = {
            ctx.NewList(pos, {ctx.NewAtom(pos, "begin"), ctx.NewCallable(pos, "Void", {})}),
            ctx.NewList(pos, {ctx.NewAtom(pos, "end"), ctx.NewCallable(pos, "Void", {})}),
        };
        // The new pipeline consumes ordering from each frame, while the old
        // pipeline consumes CalcOverWindow's sort argument. Match SQL lowering.
        if (IsWindowNewPipelineEnabled(types)) {
            settings.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, "sortSpec"), sortTraits}));
        }
        auto frameSettings = ctx.NewList(pos, std::move(settings));
        auto frame = ctx.NewCallable(pos, "WinOnRows", {frameSettings,
            ctx.NewList(pos, {ctx.NewAtom(pos, scratch), traits})});
        input = ctx.NewCallable(pos, "CalcOverWindow", {input, ctx.NewList(pos, std::move(partitions)),
            sortTraits, ctx.NewList(pos, {frame})});
        inputColumns.emplace_back(scratch);
        results.emplace(call.Get(), scratch);
    }
    if (!squeezedInput) {
        return input;
    }
    return ctx.NewCallable(pos, "FlatMap", {squeezedInput,
        ctx.NewLambda(pos, ctx.NewArguments(pos, {listArgument}),
            ctx.NewCallable(pos, "ToFlow", {input})),
    });
}

} // namespace

TExprNode::TPtr TPhysicalMapBuilder::BuildPhysicalOp(TExprNode::TPtr input) {
    const auto originalColumns = Map->GetInput()->GetOutputIUs();
    auto inputColumns = originalColumns;
    THashMap<const TExprNode*, TString> windowResults;
    input = LowerWindows(*Map, std::move(input), inputColumns, windowResults, Ctx, Pos, Types);

    // clang-format off
    input = Build<TCoToFlow>(Ctx, Pos)
        .Input(input)
    .Done().Ptr();
    // clang-format on

    input = NPhysicalConvertionUtils::BuildExpandMapForNarrowInput(input, inputColumns, Ctx);

    THashMap<TString, ui32> colNamesToIndices;
    TVector<TExprNode::TPtr> lambdaArgs;
    TVector<TExprNode::TPtr> lambdaResults;

    TVector<TString> outputColumns;
    THashSet<TInfoUnit, TInfoUnit::THashFunction> renameSources;

    for (ui32 i = 0; i < inputColumns.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "arg_" + ToString(i)));
        colNamesToIndices.emplace(inputColumns[i].GetFullName(), i);
    }

    for (const auto& mapElement : Map->MapElements) {
        if (mapElement.IsRename()) {
            renameSources.insert(mapElement.GetRename());
        }
    }

    for (const auto& input : originalColumns) {
        if (renameSources.contains(input)) {
            continue;
        }
        const auto& fullName = input.GetFullName();
        auto it = colNamesToIndices.find(fullName);
        Y_ENSURE(it != colNamesToIndices.end());
        lambdaResults.push_back(lambdaArgs[it->second]);
        outputColumns.push_back(fullName);
    }

    for (const auto& mapElement : Map->MapElements) {
        if (mapElement.IsRename()){
            const auto colName = mapElement.GetRename().GetFullName();
            auto it = colNamesToIndices.find(colName);
            Y_ENSURE(it != colNamesToIndices.end(), colName + " column not found.");
            lambdaResults.push_back(lambdaArgs[it->second]);
        }
        else {
            auto lambda = TCoLambda(mapElement.GetExpression().Node);
            auto lambdaBody = lambda.Body().Ptr();

            auto isMember = [&](const TExprNode::TPtr& node) -> bool {
                if (node->IsCallable("Member")) {
                    return true;
                }
                return false;
            };

            // For expressions - we want to find all members and replace them with lambda args.
            TNodeOnNodeOwnedMap replaces;
            for (const auto& [window, name] : windowResults) {
                replaces[window] = lambdaArgs[colNamesToIndices.at(name)];
            }
            lambdaBody = Ctx.ReplaceNodes(std::move(lambdaBody), replaces);
            replaces.clear();
            auto members = FindNodes(lambdaBody, isMember);
            for (const auto& member : members) {
                const auto colName = TString(TCoMember(member).Name().StringValue());
                auto it = colNamesToIndices.find(colName);
                Y_ENSURE(it != colNamesToIndices.end(), colName + " column not found.");
                replaces[member.Get()] = lambdaArgs[it->second];
            }
            lambdaResults.push_back(Ctx.ReplaceNodes(std::move(lambdaBody), replaces));
        }

        const auto outColName = mapElement.GetElementName().GetFullName();
        outputColumns.push_back(outColName);
    }

    // Create a wide lambda.
    auto wideLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaArgs)), std::move(lambdaResults));

    // clang-format off
    input = Build<TCoWideMap>(Ctx, Pos)
        .Input(input)
        .Lambda(std::move(wideLambda))
    .Done().Ptr();
    // clang-format on

    input = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(input, outputColumns, NPhysicalConvertionUtils::BuildNameSet(Map->GetOutputIUs()), Ctx);

    // clang-format off
    input = Build<TCoFromFlow>(Ctx, Pos)
        .Input(input)
    .Done().Ptr();
    // clang-format on

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical map] " << KqpExprToPrettyString(TExprBase(input), Ctx);

    return input;
}
