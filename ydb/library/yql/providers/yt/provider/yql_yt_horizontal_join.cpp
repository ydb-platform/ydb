#include "yql_yt_horizontal_join.h"
#include "yql_yt_helpers.h"
#include "yql_yt_optimize.h"

#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/xrange.h>
#include <util/generic/algorithm.h>
#include <util/string/cast.h>
#include <util/digest/numeric.h>


template <>
struct THash<std::set<ui64>> {
    inline size_t operator()(const std::set<ui64>& s) const {
        size_t hash = s.size();
        for (auto& v: s) {
            hash = CombineHashes(NumericHash(v), hash);
        }
        return hash;
    }
};

namespace NYql {

using namespace NNodes;

namespace {
    THashSet<TStringBuf> UDF_HORIZONTAL_JOIN_WHITE_LIST = {
        "Streaming",
    };
}

bool THorizontalJoinBase::IsGoodForHorizontalJoin(TYtMap map) const {
    // Map already executed or in progress
    if (map.Ref().GetState() == TExprNode::EState::ExecutionComplete
        || map.Ref().GetState() == TExprNode::EState::ExecutionInProgress
        || (map.Ref().HasResult() && map.Ref().GetResult().Type() == TExprNode::World)) {
        return false;
    }

    // Another node depends on this Map by world
    if (HasWorldDeps.find(map.Raw()) != HasWorldDeps.end()) {
        return false;
    }

    // Map has multiple outputs or input sections
    if (map.Input().Size() != 1 || map.Output().Size() != 1) {
        return false;
    }

    // Map has output limit or is sharded MapJoin
    if (NYql::HasAnySetting(map.Settings().Ref(), EYtSettingType::Limit | EYtSettingType::SortLimitBy | EYtSettingType::Sharded | EYtSettingType::JobCount)) {
        return false;
    }

    if (!IsYieldTransparent(map.Mapper().Ptr(), *State_->Types)) {
        return false;
    }

    bool good = true;
    const TExprNode* innerLambdaArg = nullptr;
    if (auto maybeInnerLambda = GetFlatMapOverInputStream(map.Mapper()).Lambda()) {
        innerLambdaArg = maybeInnerLambda.Cast().Args().Arg(0).Raw();
    }

    VisitExpr(map.Mapper().Body().Ref(), [&good, innerLambdaArg](const TExprNode& node) -> bool {
        if (!good) {
            return false;
        }
        if (TYtOutput::Match(&node)) {
            // Stop traversing dependent operations
            return false;
        }
        else if (TYtRowNumber::Match(&node)) {
            good = false;
        }
        else if (auto p = TMaybeNode<TYtTablePath>(&node)) {
            // Support only YtTableProps in FlatMap over input stream. Other YtTableProps cannot be properly integrated into the Switch
            if (p.Cast().DependsOn().Input().Raw() != innerLambdaArg) {
                good = false;
            }
        }
        else if (auto p = TMaybeNode<TYtTableRecord>(&node)) {
            if (p.Cast().DependsOn().Input().Raw() != innerLambdaArg) {
                good = false;
            }
        }
        else if (auto p = TMaybeNode<TYtTableIndex>(&node)) {
            if (p.Cast().DependsOn().Input().Raw() != innerLambdaArg) {
                good = false;
            }
        }
        return good;
    });
    return good;
}

TCoLambda THorizontalJoinBase::CleanupAuxColumns(TCoLambda lambda, TExprContext& ctx) const {
    auto maybeInnerLambda = GetFlatMapOverInputStream(lambda).Lambda();
    if (!maybeInnerLambda) {
        return lambda;
    }

    TExprNode::TPtr innerLambda = maybeInnerLambda.Cast().Ptr();
    TExprNode::TPtr clonedInnerLambda = ctx.DeepCopyLambda(*innerLambda);
    TExprNode::TPtr innerLambdaArg = TCoLambda(clonedInnerLambda).Args().Arg(0).Ptr();

    TExprNode::TPtr extendedArg = ctx.NewArgument(lambda.Pos(), "extendedArg");

    bool hasTablePath = false;
    bool hasTableRecord = false;
    bool hasTableIndex = false;
    TNodeOnNodeOwnedMap replaces;
    VisitExpr(*clonedInnerLambda, [&](const TExprNode& node) {
        if (TYtOutput::Match(&node)) {
            // Stop traversing dependent operations
            return false;
        }
        if (auto p = TMaybeNode<TYtTablePath>(&node)) {
            auto input = p.Cast().DependsOn().Input();
            if (input.Ptr() == innerLambdaArg) {
                hasTablePath = true;
                replaces[&node] = Build<TCoMember>(ctx, node.Pos())
                    .Struct(extendedArg)
                    .Name()
                        .Value("_yql_table_path")
                    .Build()
                    .Done().Ptr();
            }
        }
        else if (auto p = TMaybeNode<TYtTableRecord>(&node)) {
            auto input = p.Cast().DependsOn().Input();
            if (input.Ptr() == innerLambdaArg) {
                hasTableRecord = true;
                replaces[&node] = Build<TCoMember>(ctx, node.Pos())
                    .Struct(extendedArg)
                    .Name()
                        .Value("_yql_table_record")
                    .Build()
                    .Done().Ptr();
            }
        }
        else if (auto p = TMaybeNode<TYtTableIndex>(&node)) {
            auto input = p.Cast().DependsOn().Input();
            if (input.Ptr() == innerLambdaArg) {
                hasTableIndex = true;
                replaces[&node] = Build<TCoMember>(ctx, node.Pos())
                    .Struct(extendedArg)
                    .Name()
                        .Value("_yql_table_index")
                    .Build()
                    .Done().Ptr();
            }
        }
        return true;
    });

    if (replaces.empty()) {
        return lambda;
    }

    TExprNode::TPtr cleanArg = extendedArg;
    if (hasTablePath) {
        cleanArg = Build<TCoForceRemoveMember>(ctx, cleanArg->Pos())
            .Struct(cleanArg)
            .Name()
                .Value("_yql_table_path")
            .Build()
            .Done().Ptr();
    }
    if (hasTableRecord) {
        cleanArg = Build<TCoForceRemoveMember>(ctx, cleanArg->Pos())
            .Struct(cleanArg)
            .Name()
                .Value("_yql_table_record")
            .Build()
            .Done().Ptr();
    }
    if (hasTableIndex) {
        cleanArg = Build<TCoForceRemoveMember>(ctx, cleanArg->Pos())
            .Struct(cleanArg)
            .Name()
                .Value("_yql_table_index")
            .Build()
            .Done().Ptr();
    }

    replaces[innerLambdaArg.Get()] = cleanArg;

    auto body = ctx.ReplaceNodes(clonedInnerLambda->TailPtr(), replaces);

    return Build<TCoLambda>(ctx, lambda.Pos())
        .Args({"stream"})
        .Body<TExprApplier>()
            .Apply(lambda)
            .With(lambda.Args().Arg(0), "stream")
            .With(TExprBase(innerLambda), TExprBase(ctx.NewLambda(clonedInnerLambda->Pos(), ctx.NewArguments(extendedArg->Pos(), { extendedArg }), std::move(body))))
        .Build()
        .Done();
}

void THorizontalJoinBase::ClearJoinGroup() {
    DataSink = {};
    UsesTablePath.Clear();
    UsesTableRecord.Clear();
    UsesTableIndex.Clear();
    OpSettings = {};
    MemUsage.clear();
    JoinedMaps.clear();
    UsedFiles = 1; // jobstate
}

void THorizontalJoinBase::AddToJoinGroup(TYtMap map) {
    if (auto maybeInnerLambda = GetFlatMapOverInputStream(map.Mapper()).Lambda()) {
        TExprNode::TPtr innerLambda = maybeInnerLambda.Cast().Ptr();
        TExprNode::TPtr innerLambdaArg = maybeInnerLambda.Cast().Args().Arg(0).Ptr();

        bool hasTablePath = false;
        bool hasTableRecord = false;
        bool hasTableIndex = false;
        VisitExpr(*innerLambda, [&](const TExprNode& node) {
            if (TYtOutput::Match(&node)) {
                // Stop traversing dependent operations
                return false;
            }
            if (auto p = TMaybeNode<TYtTablePath>(&node)) {
                auto input = p.Cast().DependsOn().Input();
                if (input.Ptr() == innerLambdaArg) {
                    hasTablePath = true;
                }
            }
            else if (auto p = TMaybeNode<TYtTableRecord>(&node)) {
                auto input = p.Cast().DependsOn().Input();
                if (input.Ptr() == innerLambdaArg) {
                    hasTableRecord = true;
                }
            }
            else if (auto p = TMaybeNode<TYtTableIndex>(&node)) {
                auto input = p.Cast().DependsOn().Input();
                if (input.Ptr() == innerLambdaArg) {
                    hasTableIndex = true;
                }
            }
            return true;
        });

        if (hasTablePath) {
            UsesTablePath.Set(JoinedMaps.size());
        }
        if (hasTableRecord) {
            UsesTableRecord.Set(JoinedMaps.size());
        }
        if (hasTableIndex) {
            UsesTableIndex.Set(JoinedMaps.size());
        }
    }

    if (!DataSink) {
        DataSink = map.DataSink();
    }

    if (NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Ordered)) {
        OpSettings |= EYtSettingType::Ordered;
    }
    if (NYql::HasSetting(map.Settings().Ref(), EYtSettingType::WeakFields)) {
        OpSettings |= EYtSettingType::WeakFields;
    }
    if (NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Flow)) {
        OpSettings |= EYtSettingType::Flow;
    }

    JoinedMaps.push_back(map);
}

TCoLambda THorizontalJoinBase::BuildMapperWithAuxColumnsForSingleInput(TPositionHandle pos, bool ordered, TExprContext& ctx) const {
    auto mapLambda = Build<TCoLambda>(ctx, pos)
        .Args({"row"})
        .Body("row")
        .Done();

    if (!UsesTablePath.Empty()) {
        mapLambda = Build<TCoLambda>(ctx, pos)
            .Args({"row"})
            .Body<TCoAddMember>()
                .Struct<TExprApplier>()
                    .Apply(mapLambda)
                    .With(0, "row")
                .Build()
                .Name()
                    .Value("_yql_table_path")
                .Build()
                .Item<TYtTablePath>()
                    .DependsOn()
                        .Input("row")
                    .Build()
                .Build()
            .Build()
            .Done();
    }
    if (!UsesTableRecord.Empty()) {
        mapLambda = Build<TCoLambda>(ctx, pos)
            .Args({"row"})
            .Body<TCoAddMember>()
                .Struct<TExprApplier>()
                    .Apply(mapLambda)
                    .With(0, "row")
                .Build()
                .Name()
                    .Value("_yql_table_record")
                .Build()
                .Item<TYtTableRecord>()
                    .DependsOn()
                        .Input("row")
                    .Build()
                .Build()
            .Build()
            .Done();
    }
    if (!UsesTableIndex.Empty()) {
        mapLambda = Build<TCoLambda>(ctx, pos)
            .Args({"row"})
            .Body<TCoAddMember>()
                .Struct<TExprApplier>()
                    .Apply(mapLambda)
                    .With(0, "row")
                .Build()
                .Name()
                    .Value("_yql_table_index")
                .Build()
                .Item<TYtTableIndex>()
                    .DependsOn()
                        .Input("row")
                    .Build()
                .Build()
            .Build()
            .Done();
    }
    return Build<TCoLambda>(ctx, pos)
        .Args({"stream"})
        .Body<TCoMapBase>()
            .CallableName(ordered ? TCoOrderedMap::CallableName() : TCoMap::CallableName())
            .Input("stream")
            .Lambda(mapLambda)
        .Build()
        .Done();
}

TCoLambda THorizontalJoinBase::BuildMapperWithAuxColumnsForMultiInput(TPositionHandle pos, bool ordered, TExprContext& ctx) const {
    TVector<TExprBase> tupleTypes;
    for (size_t i: xrange(JoinedMaps.size())) {
        auto itemType = JoinedMaps[i].Input().Item(0).Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        TVector<const TItemExprType*> extraItems;
        if (UsesTablePath.Get(i)) {
            extraItems.push_back(ctx.MakeType<TItemExprType>("_yql_table_path",
                ctx.MakeType<TDataExprType>(EDataSlot::String)));
        }
        if (UsesTableRecord.Get(i)) {
            extraItems.push_back(ctx.MakeType<TItemExprType>("_yql_table_record",
                ctx.MakeType<TDataExprType>(EDataSlot::Uint64)));
        }
        if (UsesTableIndex.Get(i)) {
            extraItems.push_back(ctx.MakeType<TItemExprType>("_yql_table_index",
                ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));
        }
        if (!extraItems.empty()) {
            auto items = itemType->Cast<TStructExprType>()->GetItems();
            items.insert(items.end(), extraItems.begin(), extraItems.end());
            itemType = ctx.MakeType<TStructExprType>(items);
        }

        tupleTypes.push_back(TExprBase(ExpandType(pos, *itemType, ctx)));
    }
    TExprBase varType = Build<TCoVariantType>(ctx, pos)
        .UnderlyingType<TCoTupleType>()
            .Add(tupleTypes)
        .Build()
        .Done();

    TVector<TExprBase> visitArgs;
    for (size_t i: xrange(JoinedMaps.size())) {
        visitArgs.push_back(Build<TCoAtom>(ctx, pos).Value(ToString(i)).Done());
        auto visitLambda = Build<TCoLambda>(ctx, pos)
            .Args({"row"})
            .Body("row")
            .Done();

        if (UsesTablePath.Get(i)) {
            visitLambda = Build<TCoLambda>(ctx, pos)
                .Args({"row"})
                .Body<TCoAddMember>()
                    .Struct<TExprApplier>()
                        .Apply(visitLambda)
                        .With(0, "row")
                    .Build()
                    .Name()
                        .Value("_yql_table_path")
                    .Build()
                    .Item<TYtTablePath>()
                        .DependsOn()
                            .Input("row")
                        .Build()
                    .Build()
                .Build()
                .Done();
        }

        if (UsesTableRecord.Get(i)) {
            visitLambda = Build<TCoLambda>(ctx, pos)
                .Args({"row"})
                .Body<TCoAddMember>()
                    .Struct<TExprApplier>()
                        .Apply(visitLambda)
                        .With(0, "row")
                    .Build()
                    .Name()
                        .Value("_yql_table_record")
                    .Build()
                    .Item<TYtTableRecord>()
                        .DependsOn()
                            .Input("row")
                        .Build()
                    .Build()
                .Build()
                .Done();
        }

        if (UsesTableIndex.Get(i)) {
            visitLambda = Build<TCoLambda>(ctx, pos)
                .Args({"row"})
                .Body<TCoAddMember>()
                    .Struct<TExprApplier>()
                        .Apply(visitLambda)
                        .With(0, "row")
                    .Build()
                    .Name()
                        .Value("_yql_table_index")
                    .Build()
                    .Item<TYtTableIndex>()
                        .DependsOn()
                            .Input("row")
                        .Build()
                    .Build()
                .Build()
                .Done();
        }

        visitArgs.push_back(Build<TCoLambda>(ctx, pos)
            .Args({"row"})
            .Body<TCoVariant>()
                .Item<TExprApplier>()
                    .Apply(visitLambda)
                    .With(0, "row")
                .Build()
                .Index()
                    .Value(ToString(i))
                .Build()
                .VarType(varType)
            .Build()
            .Done());
    }

    return Build<TCoLambda>(ctx, pos)
        .Args({"stream"})
        .Body<TCoMapBase>()
            .CallableName(ordered ? TCoOrderedMap::CallableName() : TCoMap::CallableName())
            .Input("stream")
            .Lambda()
                .Args({"var"})
                .Body<TCoVisit>()
                    .Input("var")
                    .FreeArgs()
                        .Add(visitArgs)
                    .Build()
                .Build()
            .Build()
        .Build()
        .Done();
}

TCoLambda THorizontalJoinBase::MakeSwitchLambda(size_t mapIndex, size_t fieldsCount, bool singleInput, TExprContext& ctx) const {
    auto map = JoinedMaps[mapIndex];
    auto lambda = map.Mapper();

    if (UsesTablePath.Get(mapIndex) || UsesTableRecord.Get(mapIndex) || UsesTableIndex.Get(mapIndex)) {
        lambda = CleanupAuxColumns(lambda, ctx);
    }

    if (singleInput) {
        auto inputItemType = map.Input().Item(0).Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        if (inputItemType->GetSize() != fieldsCount || !UsesTablePath.Empty() || !UsesTableRecord.Empty() || !UsesTableIndex.Empty()) {
            TVector<TStringBuf> fields;
            for (auto itemType: inputItemType->GetItems()) {
                fields.push_back(itemType->GetName());
            }
            if (UsesTablePath.Get(mapIndex)) {
                fields.push_back("_yql_table_path");
            }
            if (UsesTableRecord.Get(mapIndex)) {
                fields.push_back("_yql_table_record");
            }
            if (UsesTableIndex.Get(mapIndex)) {
                fields.push_back("_yql_table_index");
            }

            auto [placeHolder, lambdaWithPlaceholder] = ReplaceDependsOn(lambda.Ptr(), ctx, State_->Types);
            YQL_ENSURE(placeHolder);

            lambda = Build<TCoLambda>(ctx, lambda.Pos())
                .Args({"stream"})
                .Body<TExprApplier>()
                    .Apply(TCoLambda(lambdaWithPlaceholder))
                    .With<TCoExtractMembers>(0)
                        .Input("stream")
                        .Members(ToAtomList(fields, lambda.Pos(), ctx))
                    .Build()
                    .With(TExprBase(placeHolder), "stream")
                .Build()
                .Done();
        }
    }

    return lambda;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

IGraphTransformer::TStatus THorizontalJoinOptimizer::Optimize(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    TOptimizeExprSettings settings(State_->Types);
    settings.ProcessedNodes = ProcessedNodes;
    return OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        if (auto maybeRead = TMaybeNode<TCoRight>(node).Input().Maybe<TYtReadTable>()) {
            auto read = maybeRead.Cast();
            auto readInput = read.Input().Ptr();
            auto newInput = HandleList(readInput, true, ctx);
            if (newInput != readInput) {
                if (newInput) {
                    YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-HorizontalJoin";
                    return ctx.ChangeChild(*node, TCoRight::idx_Input,
                        ctx.ChangeChild(read.Ref(), TYtReadTable::idx_Input, std::move(newInput))
                    );
                }
                return {};
            }
        }
        if (auto maybeOp = TMaybeNode<TYtTransientOpBase>(node)) {
            if (!node->HasResult() || node->GetResult().Type() != TExprNode::World) {
                auto op = maybeOp.Cast();
                auto opInput = op.Input().Ptr();
                auto newInput = HandleList(opInput, true, ctx);
                if (newInput != opInput) {
                    if (newInput) {
                        YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-HorizontalJoin";
                        return ctx.ChangeChild(op.Ref(), TYtTransientOpBase::idx_Input, std::move(newInput));
                    }
                    return {};
                }
            }
        }
        if (auto maybePublish = TMaybeNode<TYtPublish>(node)) {
            if (TExprNode::EState::ExecutionComplete != node->GetState()
                && TExprNode::EState::ExecutionInProgress != node->GetState()) {
                auto publish = maybePublish.Cast();
                auto pubInput = publish.Input().Ptr();
                if (pubInput->ChildrenSize() > 1) {
                    auto newInput = HandleList(pubInput, false, ctx);
                    if (newInput != pubInput) {
                        if (newInput) {
                            YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-HorizontalJoin";
                            return ctx.ChangeChild(publish.Ref(), TYtPublish::idx_Input, std::move(newInput));
                        }
                        return {};
                    }
                }
            }
        }
        return node;
    }, ctx, settings);
}

TExprNode::TPtr THorizontalJoinOptimizer::HandleList(const TExprNode::TPtr& node, bool sectionList, TExprContext& ctx) {
    auto groups = CollectGroups(node, sectionList);
    if (groups.empty()) {
        return node;
    }

    for (auto& group: groups) {
        if (group.second.size() < 2) {
            continue;
        }

        ClearJoinGroup();

        for (auto& mapGrp: group.second) {
            TYtMap map = std::get<0>(mapGrp);
            const size_t sectionNum = std::get<1>(mapGrp);
            const size_t pathNum = std::get<2>(mapGrp);

            size_t outNdx = JoinedMaps.size();
            const TExprNode* columns = nullptr;
            const TExprNode* ranges = nullptr;
            if (sectionList) {
                auto path = TYtSection(node->Child(sectionNum)).Paths().Item(pathNum);
                columns = path.Columns().Raw();
                ranges = path.Ranges().Raw();
            }

            auto uniqIt = UniqMaps.find(map.Raw());
            if (uniqIt != UniqMaps.end()) {
                // Map output is used multiple times
                // Move all {section,path} pairs to ExclusiveOuts
                ExclusiveOuts[uniqIt->second].emplace_back(sectionNum, pathNum);

                auto it = GroupedOuts.find(std::make_tuple(sectionNum, columns, ranges));
                if (it != GroupedOuts.end()) {
                    auto itOut = it->second.find(uniqIt->second);
                    if (itOut != it->second.end()) {
                        ExclusiveOuts[uniqIt->second].emplace_back(sectionNum, itOut->second);
                        it->second.erase(itOut);
                    }
                    if (it->second.empty()) {
                        GroupedOuts.erase(it);
                    }
                }
                continue;
            }

            const size_t mapInputCount = map.Input().Item(0).Paths().Size();
            if (const auto nextCount = InputCount + mapInputCount; nextCount > MaxTables) {
                if (MakeJoinedMap(node->Pos(), ctx)) {
                    YQL_CLOG(INFO, ProviderYt) << "HorizontalJoin: split by max input tables: " << nextCount;
                }
                // Reinit outNdx because MakeJoinedMap() clears JoinedMaps
                outNdx = JoinedMaps.size();
            }

            size_t outputCountIncrement = 0;
            auto outRowSpec = TYtTableBaseInfo::GetRowSpec(map.Output().Item(0));
            const bool sortedOut = outRowSpec && outRowSpec->IsSorted();

            if (sortedOut) {
                if (ExclusiveOuts.find(outNdx) == ExclusiveOuts.end()) {
                    // Sorted output cannot be joined with others
                    outputCountIncrement = 1;
                }
            }
            else if (GroupedOuts.find(std::make_tuple(sectionNum, columns, ranges)) == GroupedOuts.end()) {
                outputCountIncrement = 1;
            }

            if (ExclusiveOuts.size() + GroupedOuts.size() + outputCountIncrement > MaxOutTables) {
                // MakeJoinedMap() clears ExclusiveOuts and GroupedOuts
                const auto outCount = ExclusiveOuts.size() + GroupedOuts.size();
                if (MakeJoinedMap(node->Pos(), ctx)) {
                    YQL_CLOG(INFO, ProviderYt) << "HorizontalJoin: split by max output tables: " << outCount;
                }
                outputCountIncrement = 1;
                // Reinit outNdx because MakeJoinedMap() clears JoinedMaps
                outNdx = JoinedMaps.size();
            }

            TExprNode::TPtr updatedLambda = map.Mapper().Ptr();
            if (MaxJobMemoryLimit) {
                auto status = UpdateTableContentMemoryUsage(map.Mapper().Ptr(), updatedLambda, State_, ctx);
                if (status.Level != IGraphTransformer::TStatus::Ok) {
                    return {};
                }
            }

            ScanResourceUsage(*updatedLambda, *State_->Configuration, State_->Types, &MemUsage, nullptr, &UsedFiles);
            auto currMemory = Accumulate(MemUsage.begin(), MemUsage.end(), SwitchMemoryLimit,
                [](ui64 sum, const std::pair<const TStringBuf, ui64>& val) { return sum + val.second; });

            // Take into account codec input/output buffers (one for all inputs and one per output)
            currMemory += YQL_JOB_CODEC_MEM * (ExclusiveOuts.size() + GroupedOuts.size() + outputCountIncrement + 1);

            if ((MaxJobMemoryLimit && currMemory > *MaxJobMemoryLimit) || UsedFiles > MaxOperationFiles) {
                const auto usedFiles = UsedFiles; // Save value, because MakeJoinedMap will clear it
                if (MakeJoinedMap(node->Pos(), ctx)) {
                    YQL_CLOG(INFO, ProviderYt) << "HorizontalJoin: split by limits. Memory: " << currMemory << ", files: " << usedFiles;
                }
                ScanResourceUsage(*updatedLambda, *State_->Configuration, State_->Types, &MemUsage, nullptr, &UsedFiles);
                // Reinit outNdx because MakeJoinedMap() clears joinedMapOuts
                outNdx = JoinedMaps.size();
            }

            InputCount += mapInputCount;

            if (map.World().Ref().Type() != TExprNode::World) {
                Worlds.emplace(map.World().Ptr(), Worlds.size());
            }

            if (sortedOut) {
                ExclusiveOuts[outNdx].emplace_back(sectionNum, pathNum);
            } else {
                GroupedOuts[std::make_tuple(sectionNum, columns, ranges)][outNdx] = pathNum;
            }
            UniqMaps.emplace(map.Raw(), outNdx);

            auto section = map.Input().Item(0);
            if (section.Paths().Size() == 1) {
                auto path = section.Paths().Item(0);
                GroupedInputs.emplace(path.Table().Raw(), path.Ranges().Raw(), section.Settings().Raw(), NYql::HasSetting(map.Settings().Ref(), EYtSettingType::WeakFields));
            } else {
                GroupedInputs.emplace(section.Raw(), nullptr, nullptr, NYql::HasSetting(map.Settings().Ref(), EYtSettingType::WeakFields));
            }

            AddToJoinGroup(map);
        }
        MakeJoinedMap(node->Pos(), ctx);
    }

    if (InputSubsts.empty()) {
        return node;
    }

    return RebuildList(node, sectionList, ctx);
}

void THorizontalJoinOptimizer::AddToGroups(TYtMap map, size_t s, size_t p, const TExprNode* section,
    THashMap<TGroupKey, TVector<std::tuple<TYtMap, size_t, size_t>>>& groups,
    TNodeMap<TMaybe<TGroupKey>>& processedMaps) const
{
    auto processedIt = processedMaps.find(map.Raw());
    if (processedIt != processedMaps.end()) {
        if (processedIt->second) {
            groups[*processedIt->second].emplace_back(map, s, p);
        }
        return;
    }

    bool good = IsGoodForHorizontalJoin(map);

    // Map output has multiple readers
    if (good) {
        auto readersIt = OpDeps.find(map.Raw());
        if (readersIt == OpDeps.end()) {
            good = false;
        }
        else if (readersIt->second.size() != 1) {
            // Allow multiple readers from the same section
            for (auto& r: readersIt->second) {
                if (std::get<1>(r) != section) {
                    good = false;
                    break;
                }
            }
        }
    }

    // Gather not yet completed dependencies (input data and world). Not more than one is allowed
    const TExprNode* dep = nullptr;
    bool yamr = false;
    bool qb2 = false;
    if (good) {
        TNodeSet deps;
        for (auto path: map.Input().Item(0).Paths()) {
            yamr = path.Table().Maybe<TYtTable>().RowSpec().Maybe<TCoVoid>().IsValid();
            if (auto setting = path.Table().Maybe<TYtTable>().Settings()) {
                qb2 = qb2 || NYql::HasSetting(setting.Ref(), EYtSettingType::WithQB);
            }
            if (auto op = path.Table().Maybe<TYtOutput>().Operation()) {
                const TExprNode* opNode = op.Cast().Raw();
                if (opNode->GetState() != TExprNode::EState::ExecutionComplete
                    || !opNode->HasResult()
                    || opNode->GetResult().Type() != TExprNode::World) {
                    deps.insert(opNode);
                }
            }
        }
        if (map.World().Ref().Type() != TExprNode::World
            && map.World().Ref().GetState() != TExprNode::EState::ExecutionComplete) {
            deps.insert(map.World().Raw());
        }

        if (deps.size() > 1) {
            good = false;
        } else if (deps.size() == 1) {
            dep = *deps.begin();
        }
    }

    if (good) {
        ui32 flags = 0;
        if (yamr) {
            flags |= EFeatureFlags::YAMR;
        }
        if (qb2) {
            flags |= EFeatureFlags::QB2;
        }
        if (NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Flow)) {
            flags |= EFeatureFlags::Flow;
        }
        auto key = TGroupKey {
            TString{map.DataSink().Cluster().Value()},
            NYql::GetSampleParams(map.Input().Item(0).Settings().Ref()),
            dep,
            flags
        };
        groups[key].emplace_back(map, s, p);
        processedMaps.emplace_hint(processedIt, map.Raw(), key);
    } else {
        processedMaps.emplace_hint(processedIt, map.Raw(), Nothing());
    }
}

THashMap<THorizontalJoinOptimizer::TGroupKey, TVector<std::tuple<TYtMap, size_t, size_t>>> THorizontalJoinOptimizer::CollectGroups(const TExprNode::TPtr& node, bool sectionList) const {

    THashMap<TGroupKey, TVector<std::tuple<TYtMap, size_t, size_t>>> groups; // group key -> Vector{Map, section_num, path_num}
    TNodeMap<TMaybe<TGroupKey>> processedMaps;

    if (sectionList) {
        auto sections = TYtSectionList(node);
        for (size_t s = 0; s < sections.Size(); ++s) {
            auto section = sections.Item(s);
            for (size_t p = 0, pathCount = section.Paths().Size(); p < pathCount; ++p) {
                auto path = section.Paths().Item(p);
                if (auto maybeMap = path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtMap>()) {
                    AddToGroups(maybeMap.Cast(), s, p, section.Raw(), groups, processedMaps);
                }
            }
        }
    } else {
        auto list = TYtOutputList(node);
        for (size_t s = 0; s < list.Size(); ++s) {
            auto out = list.Item(s);
            if (auto maybeMap = out.Operation().Maybe<TYtMap>()) {
                // Treat each output as a separate section to prevent output concat
                AddToGroups(maybeMap.Cast(), s, 0, nullptr, groups, processedMaps);
            }
        }
    }

    return groups;
}

void THorizontalJoinOptimizer::ClearJoinGroup() {
    THorizontalJoinBase::ClearJoinGroup();
    Worlds.clear();
    UniqMaps.clear();
    GroupedOuts.clear();
    ExclusiveOuts.clear();
    InputCount = 0;
    GroupedInputs.clear();
}

bool THorizontalJoinOptimizer::MakeJoinedMap(TPositionHandle pos, TExprContext& ctx) {
    bool res = false;
    if (JoinedMaps.size() > 1) {

        const bool singleInput = GroupedInputs.size() == 1;
        const bool ordered = OpSettings.HasFlags(EYtSettingType::Ordered);

        TCoLambda mapper = (!UsesTablePath.Empty() || !UsesTableRecord.Empty() || !UsesTableIndex.Empty())
            ? (singleInput ? BuildMapperWithAuxColumnsForSingleInput(pos, ordered, ctx) : BuildMapperWithAuxColumnsForMultiInput(pos, ordered, ctx))
            : Build<TCoLambda>(ctx, pos)
                .Args({"stream"})
                .Body("stream")
                .Done();

        TSet<TStringBuf> usedFields;
        TSet<TStringBuf> weakFields;
        if (singleInput) {
            for (auto map: JoinedMaps) {
                for (auto itemType: map.Input().Item(0).Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->GetItems()) {
                    usedFields.insert(itemType->GetName());
                }
                if (std::get<3>(*GroupedInputs.begin())) {
                    for (auto path: map.Input().Item(0).Paths()) {
                        if (auto columns = path.Columns().Maybe<TExprList>()) {
                            for (auto child: columns.Cast()) {
                                if (auto maybeTuple = child.Maybe<TCoAtomList>()) {
                                    auto tuple = maybeTuple.Cast();
                                    if (tuple.Item(1).Value() == "weak") {
                                        weakFields.insert(tuple.Item(0).Value());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        TVector<TExprBase> switchArgs;
        for (size_t i: xrange(JoinedMaps.size())) {
            auto lambda = MakeSwitchLambda(i, usedFields.size(), singleInput, ctx);

            switchArgs.push_back(
                Build<TCoAtomList>(ctx, lambda.Pos())
                    .Add()
                        .Value(singleInput ? TString("0") : ToString(i))
                    .Build()
                .Done()
                );
            switchArgs.push_back(lambda);
        }

        auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, pos);
        if (OpSettings.HasFlags(EYtSettingType::Ordered)) {
            settingsBuilder.Add()
                .Name().Value(ToString(EYtSettingType::Ordered)).Build()
            .Build();
        }
        if (OpSettings.HasFlags(EYtSettingType::WeakFields)) {
            settingsBuilder.Add()
                .Name().Value(ToString(EYtSettingType::WeakFields)).Build()
            .Build();
        }
        if (OpSettings.HasFlags(EYtSettingType::Flow)) {
            settingsBuilder.Add()
                .Name().Value(ToString(EYtSettingType::Flow)).Build()
            .Build();
        }

        mapper = Build<TCoLambda>(ctx, pos)
            .Args({"stream"})
            .Body<TCoSwitch>()
                .Input<TExprApplier>()
                    .Apply(mapper)
                    .With(0, "stream")
                .Build()
                .BufferBytes()
                    .Value(ToString(SwitchMemoryLimit))
                .Build()
                .FreeArgs()
                    .Add(switchArgs)
                .Build()
            .Build()
            .Done();

        // outRemap[oldOutIndex] -> {newOutIndex, drop_flag, TVector{section_num, path_num}}
        TVector<std::tuple<size_t, bool, TVector<std::pair<size_t, size_t>>>> outRemap;
        outRemap.resize(JoinedMaps.size());
        const bool lessOuts = ExclusiveOuts.size() + GroupedOuts.size() < JoinedMaps.size();
        size_t nextNewOutIndex = 0;
        TVector<TYtOutTable> joinedMapOuts;
        for (auto& out: ExclusiveOuts) {
            auto old_out_num = out.first;
            auto& inputs = out.second;
            if (lessOuts) {
                outRemap[old_out_num] = std::make_tuple(nextNewOutIndex++, false, inputs);
                joinedMapOuts.push_back(JoinedMaps[old_out_num].Output().Item(0));
            } else {
                outRemap[old_out_num] = std::make_tuple(old_out_num, false, inputs);
            }
        }
        for (auto& out: GroupedOuts) {
            auto& grp = out.first;
            auto& inputs = out.second;
            TMaybe<size_t> newIndex;
            size_t baseOldIndex = 0;
            TVector<std::pair<size_t, size_t>> pairsSecPath;
            for (auto in: inputs) {
                auto old_out_num = in.first;
                auto path_num = in.second;
                if (!newIndex) {
                    newIndex = nextNewOutIndex++;
                    baseOldIndex = old_out_num;
                }
                std::get<0>(outRemap[old_out_num]) = *newIndex;
                pairsSecPath.emplace_back(std::get<0>(grp), path_num);
            }
            if (lessOuts) {
                outRemap[baseOldIndex] = std::make_tuple(*newIndex, true, std::move(pairsSecPath));
                joinedMapOuts.push_back(JoinedMaps[baseOldIndex].Output().Item(0));
            } else {
                YQL_ENSURE(pairsSecPath.size() == 1);
                outRemap[baseOldIndex] = std::make_tuple(baseOldIndex, true, std::move(pairsSecPath));
            }
        }

        if (lessOuts) {
            YQL_ENSURE(ExclusiveOuts.size() + GroupedOuts.size() == nextNewOutIndex, "Output table count mismatch: " << (ExclusiveOuts.size() + GroupedOuts.size()) << " != " << nextNewOutIndex);
            if (nextNewOutIndex == 1) {
                mapper = Build<TCoLambda>(ctx, pos)
                    .Args({"stream"})
                    .Body<TCoFlatMapBase>()
                        .CallableName(ordered ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                        .Input<TExprApplier>()
                            .Apply(mapper)
                            .With(0, "stream")
                        .Build()
                        .Lambda()
                            .Args({"item"})
                            .Body<TCoJust>()
                                .Input<TCoVariantItem>()
                                    .Variant("item")
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done();
            }
            else if (nextNewOutIndex < outRemap.size()) {
                TVector<TExprBase> tupleTypes;
                for (auto out: joinedMapOuts) {
                    auto itemType = out.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
                    tupleTypes.push_back(TExprBase(ExpandType(pos, *itemType, ctx)));
                }
                TExprBase varType = Build<TCoVariantType>(ctx, pos)
                    .UnderlyingType<TCoTupleType>()
                        .Add(tupleTypes)
                    .Build()
                    .Done();

                TVector<TExprBase> visitArgs;
                for (size_t i: xrange(outRemap.size())) {
                    visitArgs.push_back(Build<TCoAtom>(ctx, pos).Value(ToString(i)).Done());
                    visitArgs.push_back(Build<TCoLambda>(ctx, pos)
                        .Args({"row"})
                        .Body<TCoVariant>()
                            .Item("row")
                            .Index()
                                .Value(ToString(std::get<0>(outRemap[i])))
                            .Build()
                            .VarType(varType)
                        .Build()
                        .Done());
                }

                mapper = Build<TCoLambda>(ctx, pos)
                    .Args({"stream"})
                    .Body<TCoFlatMapBase>()
                        .CallableName(ordered ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                        .Input<TExprApplier>()
                            .Apply(mapper)
                            .With(0, "stream")
                        .Build()
                        .Lambda()
                            .Args({"item"})
                            .Body<TCoJust>()
                                .Input<TCoVisit>()
                                    .Input("item")
                                    .FreeArgs()
                                        .Add(visitArgs)
                                    .Build()
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done();
            }
        } else {
            for (auto map: JoinedMaps) {
                joinedMapOuts.push_back(map.Output().Item(0));
            }
        }

        TVector<TYtSection> joinedMapSections;
        if (singleInput) {
            auto section = JoinedMaps.front().Input().Item(0);
            auto itemType = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

            TMaybeNode<TExprBase> columns;
            if (!weakFields.empty()) {
                auto columnsBuilder = Build<TExprList>(ctx, pos);
                for (auto& field: usedFields) {
                    if (weakFields.contains(field)) {
                        columnsBuilder
                            .Add<TCoAtomList>()
                                .Add()
                                    .Value(field)
                                .Build()
                                .Add()
                                    .Value("weak")
                                .Build()
                            .Build();
                    }
                    else {
                        columnsBuilder
                            .Add<TCoAtom>()
                                .Value(field)
                            .Build();
                    }
                }
                columns = columnsBuilder.Done();
            }
            else if (usedFields.size() != itemType->GetSize()) {
                columns = TExprBase(ToAtomList(usedFields, pos, ctx));
            }

            if (columns) {
                TVector<TYtPath> paths;
                for (const auto& path : section.Paths()) {
                    paths.push_back(Build<TYtPath>(ctx, path.Pos())
                        .InitFrom(path)
                        .Columns(columns.Cast())
                        .Stat<TCoVoid>().Build()
                        .Done());
                }

                section = Build<TYtSection>(ctx, section.Pos())
                    .InitFrom(section)
                    .Paths()
                        .Add(paths)
                    .Build()
                    .Done();
            }
            joinedMapSections.push_back(section);
        }
        else {
            for (auto map: JoinedMaps) {
                joinedMapSections.push_back(map.Input().Item(0));
            }
        }

        auto joinedMap = Build<TYtMap>(ctx, pos)
            .World(NYql::ApplySyncListToWorld(ctx.NewWorld(pos), Worlds, ctx))
            .DataSink(DataSink.Cast())
            .Output()
                .Add(joinedMapOuts)
            .Build()
            .Input()
                .Add(joinedMapSections)
            .Build()
            .Settings(settingsBuilder.Done())
            .Mapper(mapper)
            .Done();

        // {section_num, path_num} -> {joined_map, out_num}
        for (auto& r: outRemap) {
            auto outIndex = std::get<0>(r);
            auto dropFlag = std::get<1>(r);
            auto& inputs = std::get<2>(r);
            if (!inputs.empty()) {
                InputSubsts.emplace(inputs.front(), std::make_pair(joinedMap, outIndex));
                if (dropFlag) {
                    for (size_t i = 1; i < inputs.size(); ++i) {
                        InputSubsts.emplace(inputs[i], Nothing());
                    }
                }
                else {
                    for (size_t i = 1; i < inputs.size(); ++i) {
                        InputSubsts.emplace(inputs[i], std::make_pair(joinedMap, outIndex));
                    }
                }
            }
        }
        res = true;
    }

    ClearJoinGroup();
    return res;
}

TExprNode::TPtr THorizontalJoinOptimizer::RebuildList(const TExprNode::TPtr& node, bool sectionList, TExprContext& ctx) {
    TExprNode::TPtr res;
    if (sectionList) {
        TVector<TYtSection> updatedSections;
        for (size_t s = 0; s < node->ChildrenSize(); ++s) {
            auto section = TYtSection(node->ChildPtr(s));
            updatedSections.push_back(section);

            TVector<TYtPath> updatedPaths;
            bool hasUpdatedPaths = false;
            for (size_t p = 0, pathCount = section.Paths().Size(); p < pathCount; ++p) {
                auto path = section.Paths().Item(p);
                auto it = InputSubsts.find(std::make_pair(s, p));
                if (it != InputSubsts.end()) {
                    if (it->second.Defined()) {
                        updatedPaths.push_back(Build<TYtPath>(ctx, path.Pos())
                            .InitFrom(path)
                            .Table<TYtOutput>()
                                .Operation(it->second->first)
                                .OutIndex()
                                    .Value(ToString(it->second->second))
                                .Build()
                                .Mode(path.Table().Maybe<TYtOutput>().Mode())
                            .Build()
                            .Done());
                    }
                    hasUpdatedPaths = true;
                }
                else {
                    updatedPaths.push_back(path);
                }
            }

            if (hasUpdatedPaths) {
                updatedSections.back() = Build<TYtSection>(ctx, section.Pos())
                    .InitFrom(section)
                    .Paths()
                        .Add(updatedPaths)
                    .Build()
                    .Done();
            }
        }
        res = Build<TYtSectionList>(ctx, node->Pos()).Add(updatedSections).Done().Ptr();
    }
    else {
        TVector<TYtOutput> updatedOuts;
        for (size_t s = 0; s < node->ChildrenSize(); ++s) {
            auto out = TYtOutput(node->ChildPtr(s));

            auto it = InputSubsts.find(std::make_pair(s, 0));
            if (it != InputSubsts.end()) {
                if (it->second.Defined()) {
                    updatedOuts.push_back(Build<TYtOutput>(ctx, out.Pos())
                        .Operation(it->second->first)
                        .OutIndex()
                            .Value(ToString(it->second->second))
                        .Build()
                        .Mode(out.Mode())
                        .Done());
                }
            }
            else {
                updatedOuts.push_back(out);
            }
        }
        res = Build<TYtOutputList>(ctx, node->Pos()).Add(updatedOuts).Done().Ptr();
    }
    InputSubsts.clear();
    return res;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

IGraphTransformer::TStatus TMultiHorizontalJoinOptimizer::Optimize(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    THashMap<TGroupKey, TVector<TYtMap>> mapGroups;

    for (auto writer: OpDepsOrder) {
        if (auto maybeMap = TMaybeNode<TYtMap>(writer)) {
            auto map = maybeMap.Cast();
            if (IsGoodForHorizontalJoin(map)) {
                bool good = true;
                const TExprNode* dep = nullptr;
                bool yamr = false;
                bool qb2 = false;
                TNodeSet deps;
                for (auto path: map.Input().Item(0).Paths()) {
                    yamr = path.Table().Maybe<TYtTable>().RowSpec().Maybe<TCoVoid>().IsValid();
                    if (auto setting = path.Table().Maybe<TYtTable>().Settings()) {
                        qb2 = qb2 || NYql::HasSetting(setting.Ref(), EYtSettingType::WithQB);
                    }
                    if (auto op = path.Table().Maybe<TYtOutput>().Operation()) {
                        const TExprNode* opNode = op.Cast().Raw();
                        if (opNode->GetState() != TExprNode::EState::ExecutionComplete
                            || !opNode->HasResult()
                            || opNode->GetResult().Type() != TExprNode::World) {
                            deps.insert(opNode);
                        }
                    }
                }
                if (map.World().Ref().Type() != TExprNode::World
                    && map.World().Ref().GetState() != TExprNode::EState::ExecutionComplete) {
                    deps.insert(map.World().Raw());
                }

                if (deps.size() > 1) {
                    good = false;
                } else if (deps.size() == 1) {
                    dep = *deps.begin();
                }

                if (good) {
                    std::set<ui64> readerIds;
                    for (auto& reader: OpDeps.at(writer)) {
                        readerIds.insert(std::get<0>(reader)->UniqueId());
                    }
                    ui32 flags = 0;
                    if (yamr) {
                        flags |= EFeatureFlags::YAMR;
                    }
                    if (qb2) {
                        flags |= EFeatureFlags::QB2;
                    }
                    if (NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Flow)) {
                        flags |= EFeatureFlags::Flow;
                    }
                    auto key = TGroupKey {
                        TString{map.DataSink().Cluster().Value()},
                        readerIds,
                        NYql::GetSampleParams(map.Input().Item(0).Settings().Ref()),
                        dep,
                        flags
                    };
                    mapGroups[key].push_back(map);
                }
            }
        }
    }
    if (mapGroups.empty()) {
        return IGraphTransformer::TStatus::Ok;
    }

    for (auto& grp: mapGroups) {
        if (grp.second.size() > 1) {
            ::Sort(grp.second.begin(), grp.second.end(),
                [](const TYtMap& m1, const TYtMap& m2) { return m1.Ref().UniqueId() < m2.Ref().UniqueId(); });

            if (!HandleGroup(grp.second, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!OutputSubsts.empty()) {
                break;
            }
        }
    }

    if (!OutputSubsts.empty()) {
        TNodeOnNodeOwnedMap toOptimize;
        VisitExpr(input, [&](const TExprNode::TPtr& node) {
            if (auto maybeOut = TMaybeNode<TYtOutput>(node)) {
                auto out = maybeOut.Cast();
                auto it = OutputSubsts.find(out.Operation().Raw());
                if (it != OutputSubsts.end()) {
                    YQL_ENSURE(out.OutIndex().Value() == "0");
                    toOptimize[node.Get()] = Build<TYtOutput>(ctx, out.Pos())
                        .Operation(it->second.first)
                        .OutIndex()
                            .Value(ToString(it->second.second))
                        .Build()
                        .Mode(out.Mode())
                        .Done().Ptr();
                }
            }
            return true;
        });

        YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-MultiHorizontalJoin";
        return RemapExpr(input, output, toOptimize, ctx, TOptimizeExprSettings(State_->Types));
    }
    return IGraphTransformer::TStatus::Ok;
}

bool TMultiHorizontalJoinOptimizer::HandleGroup(const TVector<TYtMap>& maps, TExprContext& ctx) {
    for (TYtMap map: maps) {
        const size_t mapInputCount = map.Input().Item(0).Paths().Size();
        if (const auto nextCount = InputCount + mapInputCount; nextCount > MaxTables) {
            if (MakeJoinedMap(ctx)) {
                YQL_CLOG(INFO, ProviderYt) << "MultiHorizontalJoin: split by max input tables: " << nextCount;
            }
        }

        if (const auto nextMapCount = JoinedMaps.size() + 1; nextMapCount > MaxOutTables) {
            if (MakeJoinedMap(ctx)) {
                YQL_CLOG(INFO, ProviderYt) << "MultiHorizontalJoin: split by max output tables: " << nextMapCount;
            }
        }

        TExprNode::TPtr updatedLambda = map.Mapper().Ptr();
        if (MaxJobMemoryLimit) {
            if (UpdateTableContentMemoryUsage(map.Mapper().Ptr(), updatedLambda, State_, ctx).Level != IGraphTransformer::TStatus::Ok) {
                return false;
            }
        }

        ScanResourceUsage(*updatedLambda, *State_->Configuration, State_->Types, &MemUsage, nullptr, &UsedFiles);
        auto currMemory = Accumulate(MemUsage.begin(), MemUsage.end(), SwitchMemoryLimit,
            [](ui64 sum, const std::pair<const TStringBuf, ui64>& val) { return sum + val.second; });

        // Take into account codec input/output buffers (one for all inputs and one per output)
        currMemory += YQL_JOB_CODEC_MEM * (JoinedMaps.size() + 2);

        if ((MaxJobMemoryLimit && currMemory > *MaxJobMemoryLimit) || UsedFiles > MaxOperationFiles) {
            const auto usedFiles = UsedFiles; // Save value, because MakeJoinedMap will clear it
            if (MakeJoinedMap(ctx)) {
                YQL_CLOG(INFO, ProviderYt) << "MultiHorizontalJoin: split by limits. Memory: " << currMemory << ", files: " << usedFiles;
            }
            ScanResourceUsage(*updatedLambda, *State_->Configuration, State_->Types, &MemUsage, nullptr, &UsedFiles);
        }

        InputCount += mapInputCount;

        if (map.World().Ref().Type() != TExprNode::World) {
            Worlds.emplace(map.World().Ptr(), Worlds.size());
        }

        auto section = map.Input().Item(0);
        if (section.Paths().Size() == 1) {
            auto path = section.Paths().Item(0);
            GroupedInputs.emplace(path.Table().Raw(), path.Ranges().Raw(), section.Settings().Raw(), NYql::HasSetting(map.Settings().Ref(), EYtSettingType::WeakFields));
        } else {
            GroupedInputs.emplace(section.Raw(), nullptr, nullptr, NYql::HasSetting(map.Settings().Ref(), EYtSettingType::WeakFields));
        }

        AddToJoinGroup(map);
    }

    MakeJoinedMap(ctx);
    return true;
};

void TMultiHorizontalJoinOptimizer::ClearJoinGroup() {
    THorizontalJoinBase::ClearJoinGroup();
    Worlds.clear();
    InputCount = 0;
    GroupedInputs.clear();
}

bool TMultiHorizontalJoinOptimizer::MakeJoinedMap(TExprContext& ctx) {
    bool res = false;
    if (JoinedMaps.size() > 1) {
        TPositionHandle pos = JoinedMaps.front().Ref().Pos();

        const bool singleInput = GroupedInputs.size() == 1;
        const bool ordered = OpSettings.HasFlags(EYtSettingType::Ordered);

        TCoLambda mapper = (!UsesTablePath.Empty() || !UsesTableRecord.Empty() || !UsesTableIndex.Empty())
            ? (singleInput ? BuildMapperWithAuxColumnsForSingleInput(pos, ordered, ctx) : BuildMapperWithAuxColumnsForMultiInput(pos, ordered, ctx))
            : Build<TCoLambda>(ctx, pos)
                .Args({"stream"})
                .Body("stream")
                .Done();

        TSet<TStringBuf> usedFields;
        TSet<TStringBuf> weakFields;
        if (singleInput) {
            for (auto map: JoinedMaps) {
                for (auto itemType: map.Input().Item(0).Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->GetItems()) {
                    usedFields.insert(itemType->GetName());
                }
                if (std::get<3>(*GroupedInputs.begin())) {
                    for (auto path: map.Input().Item(0).Paths()) {
                        if (auto columns = path.Columns().Maybe<TExprList>()) {
                            for (auto child: columns.Cast()) {
                                if (auto maybeTuple = child.Maybe<TCoAtomList>()) {
                                    auto tuple = maybeTuple.Cast();
                                    if (tuple.Item(1).Value() == "weak") {
                                        weakFields.insert(tuple.Item(0).Value());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        TVector<TExprBase> switchArgs;
        for (size_t i: xrange(JoinedMaps.size())) {
            auto lambda = MakeSwitchLambda(i, usedFields.size(), singleInput, ctx);

            switchArgs.push_back(
                Build<TCoAtomList>(ctx, lambda.Pos())
                    .Add()
                        .Value(singleInput ? TString("0") : ToString(i))
                    .Build()
                .Done()
                );
            switchArgs.push_back(lambda);
        }

        auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, pos);
        if (OpSettings.HasFlags(EYtSettingType::Ordered)) {
            settingsBuilder.Add()
                .Name().Value(ToString(EYtSettingType::Ordered)).Build()
            .Build();
        }
        if (OpSettings.HasFlags(EYtSettingType::WeakFields)) {
            settingsBuilder.Add()
                .Name().Value(ToString(EYtSettingType::WeakFields)).Build()
            .Build();
        }
        if (OpSettings.HasFlags(EYtSettingType::Flow)) {
            settingsBuilder.Add()
                .Name().Value(ToString(EYtSettingType::Flow)).Build()
            .Build();
        }

        mapper = Build<TCoLambda>(ctx, pos)
            .Args({"stream"})
            .Body<TCoSwitch>()
                .Input<TExprApplier>()
                    .Apply(mapper)
                    .With(0, "stream")
                .Build()
                .BufferBytes()
                    .Value(ToString(SwitchMemoryLimit))
                .Build()
                .FreeArgs()
                    .Add(switchArgs)
                .Build()
            .Build()
            .Done();

        TVector<TYtOutTable> joinedMapOuts;
        for (auto map: JoinedMaps) {
            joinedMapOuts.push_back(map.Output().Item(0));
        }

        TVector<TYtSection> joinedMapSections;
        if (singleInput) {
            auto section = JoinedMaps.front().Input().Item(0);
            auto itemType = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

            TMaybeNode<TExprBase> columns;
            if (!weakFields.empty()) {
                auto columnsBuilder = Build<TExprList>(ctx, pos);
                for (auto& field: usedFields) {
                    if (weakFields.contains(field)) {
                        columnsBuilder
                            .Add<TCoAtomList>()
                                .Add()
                                    .Value(field)
                                .Build()
                                .Add()
                                    .Value("weak")
                                .Build()
                            .Build();
                    }
                    else {
                        columnsBuilder
                            .Add<TCoAtom>()
                                .Value(field)
                            .Build();
                    }
                }
                columns = columnsBuilder.Done();
            }
            else if (usedFields.size() != itemType->GetSize()) {
                columns = TExprBase(ToAtomList(usedFields, pos, ctx));
            }

            if (columns) {
                TVector<TYtPath> paths;
                for (const auto& path : section.Paths()) {
                    paths.push_back(Build<TYtPath>(ctx, path.Pos())
                        .InitFrom(path)
                        .Columns(columns.Cast())
                        .Stat<TCoVoid>().Build()
                        .Done());
                }

                section = Build<TYtSection>(ctx, section.Pos())
                    .InitFrom(section)
                    .Paths()
                        .Add(paths)
                    .Build()
                    .Done();
            }
            joinedMapSections.push_back(section);
        }
        else {
            for (auto map: JoinedMaps) {
                joinedMapSections.push_back(map.Input().Item(0));
            }
        }

        auto joinedMap = Build<TYtMap>(ctx, pos)
            .World(NYql::ApplySyncListToWorld(ctx.NewWorld(pos), Worlds, ctx))
            .DataSink(DataSink.Cast())
            .Output()
                .Add(joinedMapOuts)
            .Build()
            .Input()
                .Add(joinedMapSections)
            .Build()
            .Settings(settingsBuilder.Done())
            .Mapper(mapper)
            .Done();

        for (size_t i: xrange(JoinedMaps.size())) {
            auto map = JoinedMaps[i];
            OutputSubsts.emplace(map.Raw(), std::make_pair(joinedMap, i));
        }

        res = true;
    }

    ClearJoinGroup();
    return res;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TOutHorizontalJoinOptimizer::ClearJoinGroup() {
    THorizontalJoinBase::ClearJoinGroup();
    UsedFields.clear();
    WeakFields.clear();
    UsedSysFields.clear();
}

bool TOutHorizontalJoinOptimizer::MakeJoinedMap(TPositionHandle pos, const TGroupKey& key, const TStructExprType* itemType, TExprContext& ctx) {
    bool res = false;
    if (JoinedMaps.size() > 1) {
        const bool ordered = OpSettings.HasFlags(EYtSettingType::Ordered);
        auto mapper = (!UsesTablePath.Empty() || !UsesTableRecord.Empty() || !UsesTableIndex.Empty())
            ? BuildMapperWithAuxColumnsForSingleInput(pos, ordered, ctx)
            : Build<TCoLambda>(ctx, pos)
                .Args({"stream"})
                .Body("stream")
                .Done();

        TVector<TExprBase> switchArgs;
        for (size_t i: xrange(JoinedMaps.size())) {

            auto lambda = MakeSwitchLambda(i, UsedFields.size(), true, ctx);

            switchArgs.push_back(
                Build<TCoAtomList>(ctx, lambda.Pos())
                    .Add()
                        .Value("0")
                    .Build()
                .Done()
                );
            switchArgs.push_back(lambda);
        }

        auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, pos);
        if (OpSettings.HasFlags(EYtSettingType::Ordered)) {
            settingsBuilder.Add()
                .Name().Value(ToString(EYtSettingType::Ordered)).Build()
            .Build();
        }
        if (OpSettings.HasFlags(EYtSettingType::WeakFields)) {
            settingsBuilder.Add()
                .Name().Value(ToString(EYtSettingType::WeakFields)).Build()
            .Build();
        }
        if (OpSettings.HasFlags(EYtSettingType::Flow)) {
            settingsBuilder.Add()
                .Name().Value(ToString(EYtSettingType::Flow)).Build()
            .Build();
        }

        for (auto sys: UsedSysFields) {
            UsedFields.erase(TString(YqlSysColumnPrefix).append(sys));
        }

        TExprBase columns = Build<TCoVoid>(ctx, pos).Done();
        if (!WeakFields.empty()) {
            auto columnsBuilder = Build<TExprList>(ctx, pos);
            for (auto& field: UsedFields) {
                if (WeakFields.contains(field)) {
                    columnsBuilder
                        .Add<TCoAtomList>()
                            .Add()
                                .Value(field)
                            .Build()
                            .Add()
                                .Value("weak")
                            .Build()
                        .Build();
                }
                else {
                    columnsBuilder
                        .Add<TCoAtom>()
                            .Value(field)
                        .Build();
                }
            }
            columns = columnsBuilder.Done();
        }
        else if (UsedFields.size() != itemType->GetSize()) {
            columns = TExprBase(ToAtomList(UsedFields, pos, ctx));
        }

        TVector<TYtOutTable> joinedMapOuts;
        for (auto& map: JoinedMaps) {
            joinedMapOuts.push_back(map.Output().Item(0));
        }

        auto sectionSettingsBuilder = Build<TCoNameValueTupleList>(ctx, pos);
        if (std::get<4>(key)) {
            sectionSettingsBuilder.Add(ctx.ShallowCopy(*std::get<4>(key)));
        }
        if (!UsedSysFields.empty()) {
            sectionSettingsBuilder
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::SysColumns))
                    .Build()
                    .Value(ToAtomList(UsedSysFields, pos, ctx))
                .Build()
                ;
        }

        auto joinedMap = Build<TYtMap>(ctx, pos)
            .World(JoinedMaps.front().World())
            .DataSink(DataSink.Cast())
            .Output()
                .Add(joinedMapOuts)
            .Build()
            .Input()
                .Add()
                    .Paths()
                        .Add()
                            .Table(ctx.ShallowCopy(*std::get<2>(key)))
                            .Columns(columns)
                            .Ranges(ctx.ShallowCopy(*std::get<3>(key)))
                            .Stat<TCoVoid>().Build()
                        .Build()
                    .Build()
                    .Settings(sectionSettingsBuilder.Done())
                .Build()
            .Build()
            .Settings(settingsBuilder.Done())
            .Mapper()
                .Args({"stream"})
                .Body<TCoSwitch>()
                    .Input<TExprApplier>()
                        .Apply(mapper)
                        .With(0, "stream")
                    .Build()
                    .BufferBytes()
                        .Value(ToString(SwitchMemoryLimit))
                    .Build()
                    .FreeArgs()
                        .Add(switchArgs)
                    .Build()
                .Build()
            .Build()
            .Done();

        for (size_t i: xrange(JoinedMaps.size())) {
            auto map = JoinedMaps[i];
            OutputSubsts.emplace(map.Raw(), std::make_pair(joinedMap, i));
        }

        res = true;
    }

    ClearJoinGroup();

    return res;
}

bool TOutHorizontalJoinOptimizer::HandleGroup(TPositionHandle pos, const TGroupKey& key, const TVector<TYtMap>& maps, TExprContext& ctx) {
    auto itemType = std::get<2>(key)->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    for (TYtMap map: maps) {
        if (const auto nextMapCount = JoinedMaps.size() + 1; nextMapCount > MaxOutTables) {
            if (MakeJoinedMap(pos, key, itemType, ctx)) {
                YQL_CLOG(INFO, ProviderYt) << "OutHorizontalJoin: split by max output tables: " << nextMapCount;
            }
        }

        TExprNode::TPtr updatedLambda = map.Mapper().Ptr();
        if (MaxJobMemoryLimit) {
            if (UpdateTableContentMemoryUsage(map.Mapper().Ptr(), updatedLambda, State_, ctx).Level != IGraphTransformer::TStatus::Ok) {
                return false;
            }
        }

        ScanResourceUsage(*updatedLambda, *State_->Configuration, State_->Types, &MemUsage, nullptr, &UsedFiles);
        auto currMemory = Accumulate(MemUsage.begin(), MemUsage.end(), SwitchMemoryLimit,
            [](ui64 sum, const std::pair<const TStringBuf, ui64>& val) { return sum + val.second; });

        // Take into account codec input/output buffers (one for all inputs and one per output)
        currMemory += YQL_JOB_CODEC_MEM * (JoinedMaps.size() + 2);

        if ((MaxJobMemoryLimit && currMemory > *MaxJobMemoryLimit) || UsedFiles > MaxOperationFiles) {
            const auto usedFiles = UsedFiles; // Save value, because MakeJoinedMap will clear it
            if (MakeJoinedMap(pos, key, itemType, ctx)) {
                YQL_CLOG(INFO, ProviderYt) << "OutHorizontalJoin: split by limits. Memory: " << currMemory << ", files: " << usedFiles;
            }
            ScanResourceUsage(*updatedLambda, *State_->Configuration, State_->Types, &MemUsage, nullptr, &UsedFiles);
        }

        AddToJoinGroup(map);

        for (auto itemType: map.Input().Item(0).Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->GetItems()) {
            UsedFields.insert(itemType->GetName());
        }

        auto sysColumns = NYql::GetSettingAsColumnList(map.Input().Item(0).Settings().Ref(), EYtSettingType::SysColumns);
        UsedSysFields.insert(sysColumns.begin(), sysColumns.end());

        if (auto columns = map.Input().Item(0).Paths().Item(0).Columns().Maybe<TExprList>()) {
            for (auto child: columns.Cast()) {
                if (auto maybeTuple = child.Maybe<TCoAtomList>()) {
                    auto tuple = maybeTuple.Cast();
                    if (tuple.Item(1).Value() == "weak") {
                        WeakFields.insert(tuple.Item(0).Value());
                    }
                }
            }
        }
    }

    MakeJoinedMap(pos, key, itemType, ctx);
    return true;
};

bool TOutHorizontalJoinOptimizer::IsGoodForOutHorizontalJoin(const TExprNode* op) {
    auto it = ProcessedOps.find(op);
    if (it != ProcessedOps.end()) {
        return it->second;
    }

    bool res = false;
    if (auto maybeMap = TMaybeNode<TYtMap>(op)) {
        auto map = maybeMap.Cast();

        res = IsGoodForHorizontalJoin(map)
            && map.Input().Item(0).Paths().Size() == 1
            && !NYql::HasAnySetting(map.Input().Item(0).Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip)
            && !HasNonEmptyKeyFilter(map.Input().Item(0));
    }
    ProcessedOps.emplace(op, res);
    return res;
};

IGraphTransformer::TStatus TOutHorizontalJoinOptimizer::Optimize(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {

    THashMap<TGroupKey, TVector<TYtMap>> opGroups;
    THashMap<TGroupKey, TVector<TYtMap>> tableGroups;

    for (auto writer: OpDepsOrder) {
        if (IsGoodForOutHorizontalJoin(writer)) {
            auto map = TYtMap(writer);
            auto section = map.Input().Item(0);
            auto path = section.Paths().Item(0);

            // Only input table. Temp outputs are processed above
            if (auto table = path.Table().Maybe<TYtTable>()) {
                ui32 flags = 0;
                if (NYql::HasSetting(table.Settings().Ref(), EYtSettingType::WithQB)) {
                    flags |= EFeatureFlags::QB2;
                }
                if (NYql::HasSetting(map.Settings().Ref(), EYtSettingType::WeakFields)) {
                    flags |= EFeatureFlags::WeakField;
                }
                if (NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Flow)) {
                    flags |= EFeatureFlags::Flow;
                }

                auto key = TGroupKey{
                    TString{map.DataSink().Cluster().Value()},
                    map.World().Raw(),
                    table.Cast().Raw(),
                    path.Ranges().Raw(),
                    NYql::GetSetting(section.Settings().Ref(), EYtSettingType::Sample).Get(),
                    flags
                };
                tableGroups[key].push_back(map);
            }
        }

        opGroups.clear();
        for (auto& reader: OpDeps.at(writer)) {
            if (IsGoodForOutHorizontalJoin(std::get<0>(reader))) {
                auto map = TYtMap(std::get<0>(reader));
                auto section = map.Input().Item(0);

                ui32 flags = 0;
                if (NYql::HasSetting(map.Settings().Ref(), EYtSettingType::WeakFields)) {
                    flags |= EFeatureFlags::WeakField;
                }
                if (NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Flow)) {
                    flags |= EFeatureFlags::Flow;
                }

                auto key = TGroupKey{
                    TString{map.DataSink().Cluster().Value()},
                    map.World().Raw(),
                    std::get<2>(reader),
                    section.Paths().Item(0).Ranges().Raw(),
                    NYql::GetSetting(section.Settings().Ref(), EYtSettingType::Sample).Get(),
                    flags
                };
                opGroups[key].push_back(map);
            }
        }
        if (opGroups.empty()) {
            continue;
        }

        for (auto& group: opGroups) {
            if (group.second.size() < 2) {
                continue;
            }

            if (!HandleGroup(writer->Pos(), group.first, group.second, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!OutputSubsts.empty()) {
                break;
            }
        }

        if (!OutputSubsts.empty()) {
            break;
        }
    }

    if (OutputSubsts.empty() && !tableGroups.empty()) {
        for (auto& group: tableGroups) {
            if (group.second.size() < 2) {
                continue;
            }

            if (!HandleGroup(std::get<2>(group.first)->Pos(), group.first, group.second, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!OutputSubsts.empty()) {
                break;
            }
        }
    }

    if (!OutputSubsts.empty()) {
        TNodeOnNodeOwnedMap toOptimize;
        VisitExpr(input, [&](const TExprNode::TPtr& node) {
            if (auto maybeOut = TMaybeNode<TYtOutput>(node)) {
                auto out = maybeOut.Cast();
                auto it = OutputSubsts.find(out.Operation().Raw());
                if (it != OutputSubsts.end()) {
                    YQL_ENSURE(out.OutIndex().Value() == "0");
                    toOptimize[node.Get()] = Build<TYtOutput>(ctx, out.Pos())
                        .Operation(it->second.first)
                        .OutIndex()
                            .Value(ToString(it->second.second))
                        .Build()
                        .Mode(out.Mode())
                        .Done().Ptr();
                }
            }
            return true;
        });

        YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-OutHorizontalJoin";
        return RemapExpr(input, output, toOptimize, ctx, TOptimizeExprSettings(State_->Types));
    }
    return IGraphTransformer::TStatus::Ok;
}

} // NYql
