#include "yql_yt_file_mkql_compiler.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_table.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_mkql_compiler.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/defs.h>

#include <yt/cpp/mapreduce/interface/node.h>
#include <yt/cpp/mapreduce/interface/serialize.h>
#include <yt/cpp/mapreduce/common/helpers.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/generic/xrange.h>
#include <util/string/cast.h>
#include <util/stream/str.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NNodes;

namespace {

bool IsValidKeyNode(TExprBase node) {
    if (node.Maybe<TCoNull>() || node.Maybe<TCoNothing>()) {
        return true;
    }

    if (auto maybeJust = node.Maybe<TCoJust>()) {
        node = maybeJust.Cast().Input();
    }

    return TCoDataCtor::Match(node.Raw());
}

bool HasRanges(const TExprNode& input, bool withKeyUsage) {
    if (input.IsCallable(TYtOutTable::CallableName())) {
        return false;
    }
    auto sectionList = TYtSectionList(&input);
    return AnyOf(sectionList, [&](const TYtSection& section) {
        return AnyOf(section.Paths(), [&](const TYtPath& path) {
            TYtPathInfo pathInfo(path);
            auto ranges = pathInfo.Ranges;
            return ranges && (!withKeyUsage || ranges->GetUsedKeyPrefixLength());
        });
    });
}

bool HasRangesWithKeyColumns(const TExprNode& input) {
    return HasRanges(input, /*withKeyUsage*/true);
}

TRuntimeNode ApplyPathRanges(TRuntimeNode inputList, const TExprNode& input, NCommon::TMkqlBuildContext& ctx) {
    if (!HasRanges(input, false)) {
        return inputList;
    }

    ui32 tableIndex = 0;
    auto makeSectionFilter = [&](TYtSection section, TRuntimeNode item) {
        ui64 currentOffset = 0;
        TRuntimeNode filter = ctx.ProgramBuilder.NewDataLiteral(true);

        TRuntimeNode rowNumber = ctx.ProgramBuilder.Member(item, YqlSysColumnNum);
        rowNumber = ctx.ProgramBuilder.Sub(
            ctx.ProgramBuilder.Max(
                rowNumber,
                ctx.ProgramBuilder.NewDataLiteral(ui64(1))
            ),
            ctx.ProgramBuilder.NewDataLiteral(ui64(1))
        );

        TRuntimeNode tableIndexCall = ctx.ProgramBuilder.Member(item, YqlSysColumnIndex);

        TVector<std::pair<TRuntimeNode, TRuntimeNode>> tableIndexDictItems;

        for (auto p: section.Paths()) {
            TYtPathInfo pathInfo(p);
            if (pathInfo.Ranges) {
                TRuntimeNode condition = ctx.ProgramBuilder.NewDataLiteral(false);
                for (auto& range: pathInfo.Ranges->GetRanges()) {
                    TRuntimeNode data;
                    // We can't use switch by index there since msvc deny to compile it.
                    // Anyway, visit works faster because we're making only one check instead of two.
                    std::visit([&](const auto& val) {
                        using TValue = std::decay_t<decltype(val)>;
                        if constexpr (std::is_same_v<TValue, TYtRangesInfo::TRowSingle>) {
                            data = ctx.ProgramBuilder.AggrEquals(rowNumber, ctx.ProgramBuilder.NewDataLiteral(currentOffset + val.Offset));
                        } else if constexpr (std::is_same_v<TValue, TYtRangesInfo::TRowRange>) {
                            if (val.Lower) {
                                data = ctx.ProgramBuilder.AggrGreaterOrEqual(rowNumber, ctx.ProgramBuilder.NewDataLiteral(currentOffset + *val.Lower));
                            }
                            if (val.Upper) {
                                auto upper = ctx.ProgramBuilder.AggrLess(rowNumber, ctx.ProgramBuilder.NewDataLiteral(currentOffset + *val.Upper));
                                data = data ? ctx.ProgramBuilder.And({data, upper}) : upper;
                            }
                        } else if constexpr (std::is_same_v<TValue, TYtRangesInfo::TKeySingle>) {
                            YQL_ENSURE(pathInfo.Table->RowSpec);
                            auto sortedBy = pathInfo.Table->RowSpec->SortedBy;
                            YQL_ENSURE(val.Key.size() <= sortedBy.size());
                            size_t sortedByIndex = 0;
                            for (auto& node : val.Key) {
                                TRuntimeNode keyPart;
                                YQL_ENSURE(IsValidKeyNode(node), "Range should be calculated");
                                if (TCoNull::Match(node.Raw())) {
                                    keyPart = ctx.ProgramBuilder.Not(ctx.ProgramBuilder.Exists(
                                        ctx.ProgramBuilder.Member(item, sortedBy[sortedByIndex++])));
                                } else if (TCoJust::Match(node.Raw()) || TCoNothing::Match(node.Raw())) {
                                    keyPart = ctx.ProgramBuilder.AggrEquals(
                                        ctx.ProgramBuilder.Member(item, sortedBy[sortedByIndex++]),
                                        NCommon::MkqlBuildExpr(node.Ref(), ctx));
                                } else {
                                    keyPart = ctx.ProgramBuilder.Equals(
                                        ctx.ProgramBuilder.Member(item, sortedBy[sortedByIndex++]),
                                        NCommon::MkqlBuildExpr(node.Ref(), ctx));
                                }
                                data = data ? ctx.ProgramBuilder.And({data, keyPart}) : keyPart;
                            }
                        } else if constexpr (std::is_same_v<TValue, TYtRangesInfo::TKeyRange>) {
                            YQL_ENSURE(pathInfo.Table->RowSpec);
                            auto sortedBy = pathInfo.Table->RowSpec->SortedBy;
                            if (!val.Lower.empty()) {
                                YQL_ENSURE(val.Lower.size() <= sortedBy.size());
                                for (size_t i: xrange(val.Lower.size() - 1)) {
                                    YQL_ENSURE(IsValidKeyNode(val.Lower[i]), "Lower range should be calculated");
                                    TRuntimeNode keyPart;
                                    TRuntimeNode member = ctx.ProgramBuilder.Member(item, sortedBy[i]);
                                    TRuntimeNode expr = NCommon::MkqlBuildExpr(val.Lower[i].Ref(), ctx);

                                    if (TCoNull::Match(val.Lower[i].Raw())) {
                                        keyPart = ctx.ProgramBuilder.Not(ctx.ProgramBuilder.Exists(member));
                                    } else if (TCoJust::Match(val.Lower[i].Raw()) || TCoNothing::Match(val.Lower[i].Raw())) {
                                        keyPart = ctx.ProgramBuilder.AggrGreaterOrEqual(member, expr);
                                    } else {
                                        keyPart = ctx.ProgramBuilder.GreaterOrEqual(member, expr);
                                    }
                                    data = data ? ctx.ProgramBuilder.And({data, keyPart}) : keyPart;
                                }
                                TRuntimeNode keyPart;
                                TRuntimeNode member = ctx.ProgramBuilder.Member(item, sortedBy[val.Lower.size() - 1]);
                                TRuntimeNode expr = NCommon::MkqlBuildExpr(val.Lower.back().Ref(), ctx);
                                if (TCoJust::Match(val.Lower.back().Raw()) || TCoNothing::Match(val.Lower.back().Raw())) {
                                    keyPart = val.LowerInclude
                                        ? ctx.ProgramBuilder.AggrGreaterOrEqual(member, expr)
                                        : ctx.ProgramBuilder.AggrGreater(member, expr);
                                } else {
                                    keyPart = val.LowerInclude
                                        ? ctx.ProgramBuilder.GreaterOrEqual(member, expr)
                                        : ctx.ProgramBuilder.Greater(member, expr);
                                }
                                data = data ? ctx.ProgramBuilder.And({data, keyPart}) : keyPart;
                            }
                            if (!val.Upper.empty()) {
                                YQL_ENSURE(val.Upper.size() <= sortedBy.size());
                                for (size_t i: xrange(val.Upper.size() - 1)) {
                                    YQL_ENSURE(IsValidKeyNode(val.Upper[i]), "Upper range should be calculated");
                                    TRuntimeNode keyPart;
                                    TRuntimeNode member = ctx.ProgramBuilder.Member(item, sortedBy[i]);
                                    TRuntimeNode expr = NCommon::MkqlBuildExpr(val.Upper[i].Ref(), ctx);
                                    if (TCoNull::Match(val.Upper[i].Raw())) {
                                        keyPart = ctx.ProgramBuilder.Not(ctx.ProgramBuilder.Exists(member));
                                    } else if (TCoJust::Match(val.Upper[i].Raw()) || TCoNothing::Match(val.Upper[i].Raw())) {
                                        keyPart = ctx.ProgramBuilder.AggrLessOrEqual(member, expr);
                                    } else {
                                        keyPart = ctx.ProgramBuilder.LessOrEqual(member, expr);
                                    }
                                    data = data ? ctx.ProgramBuilder.And({data, keyPart}) : keyPart;
                                }
                                TRuntimeNode keyPart;
                                TRuntimeNode member = ctx.ProgramBuilder.Member(item, sortedBy[val.Upper.size() - 1]);
                                TRuntimeNode expr = NCommon::MkqlBuildExpr(val.Upper.back().Ref(), ctx);
                                if (TCoJust::Match(val.Upper.back().Raw()) || TCoNothing::Match(val.Upper.back().Raw())) {
                                    keyPart = val.UpperInclude
                                        ? ctx.ProgramBuilder.AggrLessOrEqual(member, expr)
                                        : ctx.ProgramBuilder.AggrLess(member, expr);
                                } else {
                                    keyPart = val.UpperInclude
                                        ? ctx.ProgramBuilder.LessOrEqual(member, expr)
                                        : ctx.ProgramBuilder.Less(member, expr);
                                }
                                data = data ? ctx.ProgramBuilder.And({data, keyPart}) : keyPart;
                            }
                        }
                    }, range);
                    condition = ctx.ProgramBuilder.Or({condition, data});
                }
                if (condition.GetStaticType()->IsOptional() != filter.GetStaticType()->IsOptional()) {
                    if (condition.GetStaticType()->IsOptional()) {
                        filter = ctx.ProgramBuilder.NewOptional(filter);
                    } else {
                        condition = ctx.ProgramBuilder.NewOptional(condition);
                    }
                }

                filter = ctx.ProgramBuilder.If(
                    ctx.ProgramBuilder.AggrEquals(tableIndexCall, ctx.ProgramBuilder.NewDataLiteral(tableIndex)),
                    condition,
                    filter);

                tableIndexDictItems.emplace_back(ctx.ProgramBuilder.NewDataLiteral<ui32>(tableIndex), ctx.ProgramBuilder.NewVoid());
            }
            ++tableIndex;
            currentOffset += pathInfo.Table->Stat->RecordsCount;
        }

        if (!tableIndexDictItems.empty()) {
            auto dictType = ctx.ProgramBuilder.NewDictType(
                ctx.ProgramBuilder.NewDataType(NUdf::TDataType<ui32>::Id),
                ctx.ProgramBuilder.GetTypeEnvironment().GetTypeOfVoidLazy(),
                false
            );
            auto dict = ctx.ProgramBuilder.NewDict(dictType, tableIndexDictItems);
            filter = ctx.ProgramBuilder.Or({filter, ctx.ProgramBuilder.Not(ctx.ProgramBuilder.Contains(dict, tableIndexCall))});
        }

        if (filter.GetStaticType()->IsOptional()) {
            filter = ctx.ProgramBuilder.Coalesce(filter, ctx.ProgramBuilder.NewDataLiteral(false));
        }

        return filter;
    };

    auto sectionList = TYtSectionList(&input);
    if (sectionList.Size() > 1) {
        inputList = ctx.ProgramBuilder.OrderedFilter(inputList, [&](TRuntimeNode varItem) -> TRuntimeNode {
            YQL_ENSURE(AS_TYPE(TVariantType, varItem)->GetAlternativesCount() == sectionList.Size());
            return ctx.ProgramBuilder.VisitAll(varItem, [&](ui32 index, TRuntimeNode item) -> TRuntimeNode {
                return makeSectionFilter(sectionList.Item(index), item);
            });
        });
    } else {
        inputList = ctx.ProgramBuilder.OrderedFilter(inputList, [&](TRuntimeNode item) -> TRuntimeNode {
            return makeSectionFilter(sectionList.Item(0), item);
        });
    }

    return inputList;
}

TRuntimeNode ApplySampling(TRuntimeNode list, const TExprNode& input, NCommon::TMkqlBuildContext& ctx)
{
    if (input.IsCallable(TYtOutTable::CallableName())) {
        return list;
    }

    auto getSamplingPercent = [](TYtSection section) -> TMaybe<double> {
        if (auto setting = NYql::GetSetting(section.Settings().Ref(), EYtSettingType::Sample)) {
            return FromString<double>(setting->Child(1)->Child(1)->Content());
        }
        return Nothing();
    };

    // At least one table has ranges
    auto sectonList = TYtSectionList(&input);
    TMaybe<double> percent = getSamplingPercent(sectonList.Item(0));

    if (!percent) {
        return list;
    }

    auto oneHundredNode = ctx.ProgramBuilder.NewDataLiteral<double>(100.);
    auto percentNode = ctx.ProgramBuilder.NewDataLiteral<double>(*percent);
    auto trueNode = ctx.ProgramBuilder.NewDataLiteral<bool>(true);
    auto falseNode = ctx.ProgramBuilder.NewDataLiteral<bool>(false);
    list = ctx.ProgramBuilder.ChainMap(list, oneHundredNode, [&] (TRuntimeNode item, TRuntimeNode state) -> TRuntimeNodePair {
        auto tuple = ctx.ProgramBuilder.If(ctx.ProgramBuilder.AggrGreaterOrEqual(state, oneHundredNode),
            ctx.ProgramBuilder.NewTuple({trueNode, ctx.ProgramBuilder.Add(ctx.ProgramBuilder.Sub(state, oneHundredNode), percentNode)}),
            ctx.ProgramBuilder.NewTuple({falseNode, ctx.ProgramBuilder.Add(state, percentNode)})
        );
        return {
            ctx.ProgramBuilder.NewTuple({item, ctx.ProgramBuilder.Nth(tuple, 0)}),
            ctx.ProgramBuilder.Nth(tuple, 1)
        };
    });
    list = ctx.ProgramBuilder.Filter(list, [&] (TRuntimeNode item) {
        return ctx.ProgramBuilder.Nth(item, 1);
    });
    list = ctx.ProgramBuilder.Map(list, [&] (TRuntimeNode item) {
        return ctx.ProgramBuilder.Nth(item, 0);
    });
    return list;
}

TRuntimeNode ApplyPathRangesAndSampling(TRuntimeNode inputList, TType* itemType, const TExprNode& input, NCommon::TMkqlBuildContext& ctx) {
    inputList = ApplyPathRanges(inputList, input, ctx);
    TType* actualItemType = inputList.GetStaticType();
    if (actualItemType->IsStream()) {
        actualItemType = AS_TYPE(TStreamType, actualItemType)->GetItemType();
    } else if (actualItemType->IsFlow()) {
        actualItemType = AS_TYPE(TFlowType, actualItemType)->GetItemType();
    } else {
        YQL_ENSURE(actualItemType->IsList());
        actualItemType = AS_TYPE(TListType, actualItemType)->GetItemType();
    }

    auto dropExtraMembers = [&](TRuntimeNode item, TStructType* structType, TStructType* actualStructType) {
        TVector<TStringBuf> toDrop;
        for (ui32 i = 0; i < actualStructType->GetMembersCount(); ++i) {
            auto member = actualStructType->GetMemberName(i);
            if (!structType->FindMemberIndex(member)) {
                toDrop.push_back(member);
            }
        }

        for (auto& member : toDrop) {
            item = ctx.ProgramBuilder.RemoveMember(item, member, true);
        }

        return item;
    };

    if (itemType->IsStruct()) {
        YQL_ENSURE(actualItemType->IsStruct());
        const auto structType = AS_TYPE(TStructType, itemType);
        const auto actualStructType = AS_TYPE(TStructType, actualItemType);
        YQL_ENSURE(actualStructType->GetMembersCount() >= structType->GetMembersCount());

        if (actualStructType->GetMembersCount() > structType->GetMembersCount()) {
            inputList = ctx.ProgramBuilder.Map(inputList, [&](TRuntimeNode item) {
                return dropExtraMembers(item, structType, actualStructType);
            });
        }
    } else {
        const auto varType = AS_TYPE(TVariantType, itemType);
        const auto actualVarType = AS_TYPE(TVariantType, actualItemType);

        const auto tupleType = AS_TYPE(TTupleType, varType->GetUnderlyingType());
        const auto actualTupleType = AS_TYPE(TTupleType, actualVarType->GetUnderlyingType());

        inputList = ctx.ProgramBuilder.Map(inputList, [&](TRuntimeNode varItem) {
            return ctx.ProgramBuilder.VisitAll(varItem, [&](ui32 index, TRuntimeNode item) {
                const auto structType = AS_TYPE(TStructType, tupleType->GetElementType(index));
                const auto actualStructType = AS_TYPE(TStructType, actualTupleType->GetElementType(index));
                YQL_ENSURE(actualStructType->GetMembersCount() >= structType->GetMembersCount());

                item = dropExtraMembers(item, structType, actualStructType);
                return ctx.ProgramBuilder.NewVariant(item, index, varType);
            });
        });
    }
    inputList = ApplySampling(inputList, input, ctx);
    return inputList;
}

TRuntimeNode ToList(TRuntimeNode list, NCommon::TMkqlBuildContext& ctx) {
    const auto listType = list.GetStaticType();
    if (listType->IsOptional()) {
        return ctx.ProgramBuilder.ToList(list);
    } else if (listType->IsList()) {
        return list;
    } else if (listType->IsFlow() || listType->IsStream()) {
        return ctx.ProgramBuilder.ForwardList(list);
    } else {
        YQL_ENSURE(false, "Expected list, stream or optional");
    }
}

TType* BuildInputType(TYtSectionList input, NCommon::TMkqlBuildContext& ctx) {
    TVector<TType*> items;
    for (auto section: input) {
        items.push_back(NCommon::BuildType(input.Ref(), *section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType(), ctx.ProgramBuilder));
    }
    return items.size() == 1
        ? items.front()
        : ctx.ProgramBuilder.NewVariantType(ctx.ProgramBuilder.NewTupleType(items));
}

TType* BuildOutputType(TYtOutSection output, NCommon::TMkqlBuildContext& ctx) {
    TVector<TType*> items;
    for (auto table: output) {
        items.push_back(NCommon::BuildType(output.Ref(), *table.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType(), ctx.ProgramBuilder));
    }
    return items.size() == 1
        ? items.front()
        : ctx.ProgramBuilder.NewVariantType(ctx.ProgramBuilder.NewTupleType(items));
}

TRuntimeNode ExpandFlow(TRuntimeNode flow, NCommon::TMkqlBuildContext& ctx) {
    const auto structType = AS_TYPE(TStructType, AS_TYPE(TFlowType, flow.GetStaticType())->GetItemType());
    return ctx.ProgramBuilder.ExpandMap(flow,
        [&](TRuntimeNode item) {
            TRuntimeNode::TList fields;
            fields.reserve(structType->GetMembersCount());
            auto i = 0U;
            std::generate_n(std::back_inserter(fields), structType->GetMembersCount(), [&](){ return ctx.ProgramBuilder.Member(item, structType->GetMemberName(i++)); });
            return fields;
    });
}

TRuntimeNode NarrowFlow(TRuntimeNode flow, const TStructType& structType, NCommon::TMkqlBuildContext& ctx) {
    return ctx.ProgramBuilder.NarrowMap(flow,
        [&](TRuntimeNode::TList items) {
            TSmallVec<const std::pair<std::string_view, TRuntimeNode>> fields;
            fields.reserve(structType.GetMembersCount());
            auto i = 0U;
            std::transform(items.cbegin(), items.cend(), std::back_inserter(fields), [&](TRuntimeNode item) {
                return std::make_pair(structType.GetMemberName(i++), item);
            });
            return ctx.ProgramBuilder.NewStruct(fields);
    });
}

TRuntimeNode NarrowFlowOutput(TPositionHandle pos, TRuntimeNode flow, const TStructExprType* type, NCommon::TMkqlBuildContext& ctx) {
    if (const auto width = GetWideComponentsCount(AS_TYPE(TFlowType, flow.GetStaticType())); type->GetSize() < width) {
        auto items = type->GetItems();
        auto i = 0U;
        do items.emplace_back(ctx.ExprCtx.MakeType<TItemExprType>(TString("_yql_column_") += ToString(i++), ctx.ExprCtx.MakeType<TDataExprType>(EDataSlot::String)));
        while (items.size() < width);
        type = ctx.ExprCtx.MakeType<TStructExprType>(items);
    }

    return NarrowFlow(flow, *AS_TYPE(TStructType, NCommon::BuildType(pos, *type, ctx.ProgramBuilder)), ctx);
}

TRuntimeNode NarrowFlow(TRuntimeNode flow, TYtOutputOpBase op, NCommon::TMkqlBuildContext& ctx) {
    auto type = op.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    return NarrowFlowOutput(op.Pos(), flow, type, ctx);
}

TRuntimeNode BuildTableInput(TType* outItemType, TStringBuf clusterName, const TExprNode& input, NCommon::TMkqlBuildContext& ctx,
    const THashSet<TString>& extraSysColumns, bool forceKeyColumns)
{
    return BuildTableContentCall("YtTableInput", outItemType, clusterName, input, Nothing(), ctx, false, extraSysColumns, forceKeyColumns);
}

} // unnamed

TRuntimeNode ToStream(TRuntimeNode list, NCommon::TMkqlBuildContext& ctx) {
    const auto listType = list.GetStaticType();
    if (listType->IsFlow()) {
        return ctx.ProgramBuilder.FromFlow(list);
    } else if (listType->IsOptional()) {
        return ctx.ProgramBuilder.Iterator(ctx.ProgramBuilder.ToList(list), {});
    } else if (listType->IsList()) {
        return ctx.ProgramBuilder.Iterator(list, {});
    } else if (listType->IsStream()) {
        return list;
    } else {
        YQL_ENSURE(false, "Expected list, stream or optional");
    }
}

TRuntimeNode SortListBy(TRuntimeNode list, const TVector<std::pair<TString, bool>>& sortBy, NCommon::TMkqlBuildContext& ctx) {
    TRuntimeNode sortDirections;
    std::function<TRuntimeNode(TRuntimeNode item)> keySelector;
    if (sortBy.size() == 1) {
        sortDirections = ctx.ProgramBuilder.NewDataLiteral(sortBy.front().second);
        keySelector = [&ctx, &sortBy](TRuntimeNode item) {
            return ctx.ProgramBuilder.Member(item, sortBy.front().first);
        };
    } else {
        TVector<TRuntimeNode> tupleItems{sortBy.size()};
        std::transform(sortBy.cbegin(), sortBy.cend(), tupleItems.begin(), [&ctx](const auto& it) { return ctx.ProgramBuilder.NewDataLiteral(it.second); });
        sortDirections = ctx.ProgramBuilder.NewTuple(tupleItems);
        keySelector = [&ctx, &sortBy](TRuntimeNode item) {
            TVector<TRuntimeNode> members;
            for (auto& it: sortBy) {
                members.push_back(ctx.ProgramBuilder.Member(item, it.first));
            }
            return ctx.ProgramBuilder.NewTuple(members);
        };
    }

    return ctx.ProgramBuilder.Sort(list, sortDirections, keySelector);
}

TRuntimeNode BuildTableOutput(TRuntimeNode list, NCommon::TMkqlBuildContext& ctx) {
    list = ToStream(list, ctx);

    TCallableBuilder fileWriteCall(ctx.ProgramBuilder.GetTypeEnvironment(), "YtWriteFile", ctx.ProgramBuilder.GetTypeEnvironment().GetTypeOfVoidLazy());
    fileWriteCall.Add(list);

    return ctx.ProgramBuilder.AsList(TRuntimeNode(fileWriteCall.Build(), false));
}

TRuntimeNode BuildDqWrite(TRuntimeNode item, TStringBuf path, NCommon::TMkqlBuildContext& ctx) {
    TCallableBuilder fileWriteCall(ctx.ProgramBuilder.GetTypeEnvironment(), "DqWriteFile", ctx.ProgramBuilder.GetTypeEnvironment().GetTypeOfVoidLazy());
    fileWriteCall.Add(item);
    fileWriteCall.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(path));

    return TRuntimeNode(fileWriteCall.Build(), false);
}

TRuntimeNode BuildRuntimeTableInput(TStringBuf callName,
    TType* outItemType,
    TStringBuf clusterName,
    TStringBuf tableName,
    TStringBuf spec,
    bool isTemp,
    NCommon::TMkqlBuildContext& ctx)
{
    auto outListType = ctx.ProgramBuilder.NewListType(outItemType);
    TType* const strType = ctx.ProgramBuilder.NewDataType(NUdf::TDataType<char*>::Id);
    TType* const boolType = ctx.ProgramBuilder.NewDataType(NUdf::TDataType<bool>::Id);
    TType* const ui64Type = ctx.ProgramBuilder.NewDataType(NUdf::TDataType<ui64>::Id);
    TType* const ui32Type = ctx.ProgramBuilder.NewDataType(NUdf::TDataType<ui32>::Id);
    TType* const tupleTypeTables = ctx.ProgramBuilder.NewTupleType({strType, boolType, strType, ui64Type, ui64Type, boolType, ui32Type});
    TType* const listTypeGroup = ctx.ProgramBuilder.NewListType(tupleTypeTables);

    TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), callName, outListType);
    call.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(clusterName)); // cluster name

    TVector<TRuntimeNode> groups;
    groups.push_back(
        ctx.ProgramBuilder.NewList(tupleTypeTables, {ctx.ProgramBuilder.NewTuple(tupleTypeTables, {
            ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(NYT::NodeToYsonString(NYT::PathToNode(NYT::TRichYPath(TString{tableName})))),
            ctx.ProgramBuilder.NewDataLiteral(isTemp),
            ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(spec),
            ctx.ProgramBuilder.NewDataLiteral(ui64(1)),
            ctx.ProgramBuilder.NewDataLiteral(ui64(1)),
            ctx.ProgramBuilder.NewDataLiteral(false),
            ctx.ProgramBuilder.NewDataLiteral(ui32(0)),
        })})
    );

    call.Add(ctx.ProgramBuilder.NewList(listTypeGroup, groups));
    call.Add(ctx.ProgramBuilder.NewEmptyTuple()); // Sampling
    call.Add(ctx.ProgramBuilder.NewEmptyTuple()); // length

    return TRuntimeNode(call.Build(), false);
}

void RegisterYtFileMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler) {
    compiler.OverrideCallable(TYtTableContent::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            TYtTableContent tableContent(&node);
            TMaybe<ui64> itemsCount;
            if (auto setting = NYql::GetSetting(tableContent.Settings().Ref(), EYtSettingType::ItemsCount)) {
                itemsCount = FromString<ui64>(setting->Child(1)->Content());
            }
            const auto itemType = NCommon::BuildType(node, *node.GetTypeAnn()->Cast<TListExprType>()->GetItemType(), ctx.ProgramBuilder);
            TRuntimeNode values;
            if (auto maybeRead = tableContent.Input().Maybe<TYtReadTable>()) {
                auto read = maybeRead.Cast();

                const bool hasRangesOrSampling = AnyOf(read.Input(), [](const TYtSection& s) {
                    return NYql::HasSetting(s.Settings().Ref(), EYtSettingType::Sample)
                        || AnyOf(s.Paths(), [](const TYtPath& p) { return !p.Ranges().Maybe<TCoVoid>(); });
                });
                if (hasRangesOrSampling) {
                    itemsCount.Clear();
                }

                const bool forceKeyColumns = HasRangesWithKeyColumns(read.Input().Ref());
                values = BuildTableContentCall(
                    TYtTableContent::CallableName(),
                    itemType,
                    read.DataSource().Cluster().Value(), read.Input().Ref(), itemsCount, ctx, true, THashSet<TString>{"num", "index"}, forceKeyColumns);
                values = ApplyPathRangesAndSampling(values, itemType, read.Input().Ref(), ctx);
            } else {
                auto output = tableContent.Input().Cast<TYtOutput>();
                values = BuildTableContentCall(
                    TYtTableContent::CallableName(),
                    itemType,
                    GetOutputOp(output).DataSink().Cluster().Value(), output.Ref(), itemsCount, ctx, true);
            }

            return values;
        });

    compiler.AddCallable({TYtSort::CallableName(), TYtCopy::CallableName(), TYtMerge::CallableName()},
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {

            TYtTransientOpBase ytOp(&node);
            TYtOutTableInfo outTableInfo(ytOp.Output().Item(0));

            TMaybe<ui64> limit;
            if (ytOp.Maybe<TYtSort>()) {
                limit = GetLimit(ytOp.Settings().Ref());
            }

            const TStructExprType* inputType = ytOp.Input().Item(0).Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            auto mkqlInputType = NCommon::BuildType(ytOp.Input().Ref(), *inputType, ctx.ProgramBuilder);
            for (size_t i: xrange(outTableInfo.RowSpec->SortedBy.size())) {
                if (!inputType->FindItem(outTableInfo.RowSpec->SortedBy[i])) {
                    mkqlInputType = ctx.ProgramBuilder.NewStructType(mkqlInputType, outTableInfo.RowSpec->SortedBy[i],
                        NCommon::BuildType(ytOp.Input().Ref(), *outTableInfo.RowSpec->SortedByTypes[i], ctx.ProgramBuilder));
                }
            }

            const bool forceKeyColumns = HasRangesWithKeyColumns(ytOp.Input().Ref());
            TRuntimeNode values = BuildTableInput(mkqlInputType,
                ytOp.DataSink().Cluster().Value(), ytOp.Input().Ref(), ctx, THashSet<TString>{"num", "index"}, forceKeyColumns);

            values = ApplyPathRangesAndSampling(values, mkqlInputType, ytOp.Input().Ref(), ctx);

            if ((ytOp.Maybe<TYtMerge>() && outTableInfo.RowSpec->IsSorted() && ytOp.Input().Item(0).Paths().Size() > 1)
                || ytOp.Maybe<TYtSort>())
            {
                values = SortListBy(values, outTableInfo.RowSpec->GetForeignSort(), ctx);
            }
            if (limit) {
                values = ctx.ProgramBuilder.Take(values, ctx.ProgramBuilder.NewDataLiteral(*limit));
            }
            auto res = BuildTableOutput(values, ctx);
            return res;
        });

    compiler.AddCallable(TYtMap::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {

            TYtMap ytMap(&node);

            const auto itemType = BuildInputType(ytMap.Input(), ctx);
            const bool forceKeyColumns = HasRangesWithKeyColumns(ytMap.Input().Ref());
            TRuntimeNode values = BuildTableInput(
                itemType,
                ytMap.DataSink().Cluster().Value(), ytMap.Input().Ref(), ctx,
                THashSet<TString>{"num", "index"}, forceKeyColumns);

            const auto arg = ytMap.Mapper().Args().Arg(0).Raw();
            values = arg->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow ?
                ctx.ProgramBuilder.ToFlow(values) : ctx.ProgramBuilder.Iterator(values, {});
            values = ApplyPathRangesAndSampling(values, itemType, ytMap.Input().Ref(), ctx);

            if (ETypeAnnotationKind::Multi == GetSeqItemType(*ytMap.Mapper().Args().Arg(0).Ref().GetTypeAnn()).GetKind())
                values = ExpandFlow(values, ctx);

            NCommon::TMkqlBuildContext innerCtx(ctx, {{arg, values}}, ytMap.Mapper().Ref().UniqueId());
            values = NCommon::MkqlBuildExpr(ytMap.Mapper().Body().Ref(), innerCtx);

            if (ETypeAnnotationKind::Multi == GetSeqItemType(*ytMap.Mapper().Body().Ref().GetTypeAnn()).GetKind())
                values = NarrowFlow(values, ytMap, ctx);

            auto res = BuildTableOutput(values, ctx);
            return res;
        });

    compiler.AddCallable(TYtReduce::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {

            TYtReduce ytReduce(&node);

            TType* itemType = BuildInputType(ytReduce.Input(), ctx);
            const bool multiSection = itemType->GetKind() == TType::EKind::Variant;

            const bool forceKeyColumns = HasRangesWithKeyColumns(ytReduce.Input().Ref());
            TRuntimeNode values = BuildTableInput(itemType,
                ytReduce.DataSink().Cluster().Value(), ytReduce.Input().Ref(), ctx,
                THashSet<TString>{"num", "index"}, forceKeyColumns);

            values = ApplyPathRangesAndSampling(values, itemType, ytReduce.Input().Ref(), ctx);

            TVector<TString> reduceBy = NYql::GetSettingAsColumnList(ytReduce.Settings().Ref(), EYtSettingType::ReduceBy);
            TVector<std::pair<TString, bool>> sortBy = NYql::GetSettingAsColumnPairList(ytReduce.Settings().Ref(), EYtSettingType::SortBy);
            TVector<bool> opt;
            if (multiSection) {
                opt.resize(Max(reduceBy.size(), sortBy.size()), false);
                auto varType = AS_TYPE(TVariantType, AS_TYPE(TListType, values)->GetItemType());
                for (ui32 i = 0; i < varType->GetAlternativesCount(); ++i) {
                    TStructType* structType = AS_TYPE(TStructType, varType->GetAlternativeType(i));
                    for (size_t c: xrange(reduceBy.size())) {
                        opt[c] = opt[c] || structType->GetMemberType(structType->GetMemberIndex(reduceBy[c]))->IsOptional();
                    }
                    if (!sortBy.empty() && sortBy.size() > reduceBy.size()) {
                        for (size_t c: xrange(reduceBy.size(), sortBy.size())) {
                            opt[c] = opt[c] || structType->GetMemberType(structType->GetMemberIndex(sortBy[c].first))->IsOptional();
                        }
                    }
                }
            }

            auto dict = ctx.ProgramBuilder.ToHashedDict(values, true, [&](TRuntimeNode item) {
                if (multiSection) {
                    return ctx.ProgramBuilder.VisitAll(item, [&](ui32, TRuntimeNode varItem) {
                        TVector<TRuntimeNode> keyItems;
                        for (size_t c: xrange(reduceBy.size())) {
                            auto key = ctx.ProgramBuilder.Member(varItem, reduceBy[c]);
                            if (opt[c] && !key.GetStaticType()->IsOptional()) {
                                key = ctx.ProgramBuilder.NewOptional(key);
                            }
                            keyItems.push_back(key);
                        }
                        return keyItems.size() == 1
                            ? keyItems.front()
                            : ctx.ProgramBuilder.NewTuple(keyItems);
                    });
                }
                TVector<TRuntimeNode> keyItems;
                for (auto& column: reduceBy) {
                    keyItems.push_back(ctx.ProgramBuilder.Member(item, column));
                }
                return keyItems.size() == 1
                    ? keyItems.front()
                    : ctx.ProgramBuilder.NewTuple(keyItems);
            }, [&](TRuntimeNode item) {
                return item;
            });

            values = ctx.ProgramBuilder.DictPayloads(dict);
            if (!sortBy.empty() && sortBy.size() > reduceBy.size()) {
                sortBy.erase(sortBy.begin(), sortBy.begin() + reduceBy.size());
            }

            // sort partial lists
            if (!sortBy.empty() || multiSection) {
                size_t keySize = sortBy.size() + multiSection;

                TRuntimeNode sortDirections;
                if (keySize > 1) {
                    TVector<TRuntimeNode> tupleItems(keySize, ctx.ProgramBuilder.NewDataLiteral(true));
                    std::transform(sortBy.cbegin(), sortBy.cend(), tupleItems.begin(), [&ctx](const auto& it) { return ctx.ProgramBuilder.NewDataLiteral(it.second); });
                    sortDirections = ctx.ProgramBuilder.NewTuple(tupleItems);
                } else {
                    sortDirections = ctx.ProgramBuilder.NewDataLiteral(sortBy.empty() || sortBy.front().second);
                }

                values = ctx.ProgramBuilder.Map(values, [&](TRuntimeNode list) {
                    list = ctx.ProgramBuilder.Sort(list, sortDirections, [&](TRuntimeNode item) {
                        if (multiSection) {
                            return ctx.ProgramBuilder.VisitAll(item, [&](ui32 ndx, TRuntimeNode varItem) {
                                TVector<TRuntimeNode> keyItems;
                                for (size_t c: xrange(sortBy.size())) {
                                    auto key = ctx.ProgramBuilder.Member(varItem, sortBy[c].first);
                                    if (opt[c + reduceBy.size()] && !key.GetStaticType()->IsOptional()) {
                                        key = ctx.ProgramBuilder.NewOptional(key);
                                    }
                                    keyItems.push_back(key);
                                }
                                keyItems.push_back(ctx.ProgramBuilder.NewDataLiteral(ndx));
                                return keyItems.size() == 1
                                    ? keyItems.front()
                                    : ctx.ProgramBuilder.NewTuple(keyItems);
                            });
                        }
                        TVector<TRuntimeNode> keyItems;
                        for (auto& column: sortBy) {
                            keyItems.push_back(ctx.ProgramBuilder.Member(item, column.first));
                        }
                        return keyItems.size() == 1
                            ? keyItems.front()
                            : ctx.ProgramBuilder.NewTuple(keyItems);
                    });

                    return list;
                });
            }

            const auto arg = ytReduce.Reducer().Args().Arg(0).Raw();
            if (NYql::HasSetting(ytReduce.Settings().Ref(), EYtSettingType::KeySwitch)) {
                itemType = multiSection ?
                    NCommon::BuildType(ytReduce.Reducer().Ref(), GetSeqItemType(*arg->GetTypeAnn()), ctx.ProgramBuilder):
                    ctx.ProgramBuilder.NewStructType(itemType, YqlSysColumnKeySwitch, ctx.ProgramBuilder.NewDataType(EDataSlot::Bool));
                values = ctx.ProgramBuilder.Map(values, [&](TRuntimeNode list) {
                    list = ctx.ProgramBuilder.Enumerate(list);
                    list = ctx.ProgramBuilder.Map(list, [&](TRuntimeNode item) {
                        auto grpSwitch = ctx.ProgramBuilder.Equals(ctx.ProgramBuilder.Nth(item, 0), ctx.ProgramBuilder.NewDataLiteral(ui64(0)));
                        if (multiSection) {
                            return ctx.ProgramBuilder.VisitAll(ctx.ProgramBuilder.Nth(item, 1), [&](ui32 ndx, TRuntimeNode varItem) {
                                return ctx.ProgramBuilder.NewVariant(ctx.ProgramBuilder.AddMember(varItem, YqlSysColumnKeySwitch, grpSwitch), ndx, itemType);
                            });
                        }
                        return ctx.ProgramBuilder.AddMember(ctx.ProgramBuilder.Nth(item, 1), YqlSysColumnKeySwitch, grpSwitch);
                    });
                    return list;
                });
            }

            TCallableBuilder callableBuilder(ctx.ProgramBuilder.GetTypeEnvironment(), "YtUngroupingList",
                ctx.ProgramBuilder.NewListType(itemType));
            callableBuilder.Add(values);
            values = TRuntimeNode(callableBuilder.Build(), false);

            values = arg->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow ?
                ctx.ProgramBuilder.ToFlow(values) : ctx.ProgramBuilder.Iterator(values, {});

            if (ETypeAnnotationKind::Multi == GetSeqItemType(*ytReduce.Reducer().Args().Arg(0).Ref().GetTypeAnn()).GetKind())
                values = ExpandFlow(values, ctx);

            NCommon::TMkqlBuildContext innerCtx(ctx, {{arg, values}}, ytReduce.Reducer().Ref().UniqueId());

            values = NCommon::MkqlBuildExpr(ytReduce.Reducer().Body().Ref(), innerCtx);

            if (ETypeAnnotationKind::Multi == GetSeqItemType(*ytReduce.Reducer().Body().Ref().GetTypeAnn()).GetKind())
                values = NarrowFlow(values, ytReduce, ctx);

            // TODO: preserve sorting in reduce processing instead of sorting according to output spec
            TYtOutTableInfo outTableInfo(ytReduce.Output().Item(0));
            if (outTableInfo.RowSpec->IsSorted()) {
                values = SortListBy(values, outTableInfo.RowSpec->GetForeignSort(), ctx);
            }

            auto res = BuildTableOutput(values, ctx);
            return res;
        });

    compiler.AddCallable(TYtMapReduce::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {

            TYtMapReduce ytMapReduce(&node);

            bool hasMap = !ytMapReduce.Mapper().Maybe<TCoVoid>();
            const auto itemType = BuildInputType(ytMapReduce.Input(), ctx);
            const bool forceKeyColumns = HasRangesWithKeyColumns(ytMapReduce.Input().Ref());
            TRuntimeNode values = BuildTableInput(itemType,
                ytMapReduce.DataSink().Cluster().Value(), ytMapReduce.Input().Ref(), ctx,
                THashSet<TString>{"num", "index"}, forceKeyColumns);

            values = ApplyPathRangesAndSampling(values, itemType, ytMapReduce.Input().Ref(), ctx);

            const auto outputItemType = BuildOutputType(ytMapReduce.Output(), ctx);
            const size_t outputsCount = ytMapReduce.Output().Ref().ChildrenSize();

            size_t mapDirectOutputsCount = 0;
            TRuntimeNode mapDirectOutputs;
            if (hasMap) {
                const auto& mapper = ytMapReduce.Mapper().Cast<TCoLambda>();
                if (const auto arg = mapper.Args().Arg(0).Raw(); arg != mapper.Body().Raw()) {
                    values = arg->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow ?
                        ctx.ProgramBuilder.ToFlow(values) : ctx.ProgramBuilder.Iterator(values, {});

                    if (ETypeAnnotationKind::Multi == GetSeqItemType(*ytMapReduce.Mapper().Cast<TCoLambda>().Args().Arg(0).Ref().GetTypeAnn()).GetKind())
                        values = ExpandFlow(values, ctx);

                    NCommon::TMkqlBuildContext innerCtx(ctx, {{arg, values}}, ytMapReduce.Mapper().Ref().UniqueId());

                    const auto& body = ytMapReduce.Mapper().Cast<TCoLambda>().Body().Ref();
                    values = NCommon::MkqlBuildExpr(body, innerCtx);

                    const auto& mapOutItemType = GetSeqItemType(*body.GetTypeAnn());
                    if (const auto mapOutputTypeSetting = NYql::GetSetting(ytMapReduce.Settings().Ref(), EYtSettingType::MapOutputType)) {
                        if (ETypeAnnotationKind::Multi == mapOutItemType.GetKind()) {
                            values = NarrowFlow(values, *AS_TYPE(TStructType, NCommon::BuildType(body, *mapOutputTypeSetting->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder)), ctx);
                        }
                    }

                    values = ToList(values, ctx);

                    if (mapOutItemType.GetKind() == ETypeAnnotationKind::Variant) {
                        auto tupleType = mapOutItemType.Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>();
                        YQL_ENSURE(tupleType->GetSize() > 0);
                        mapDirectOutputsCount = tupleType->GetSize() - 1;
                        YQL_ENSURE(mapDirectOutputsCount < outputsCount);

                        values = ctx.ProgramBuilder.Collect(values);

                        mapDirectOutputs = ctx.ProgramBuilder.OrderedFlatMap(values, [&](TRuntimeNode mapOut) {
                            return ctx.ProgramBuilder.VisitAll(mapOut, [&](ui32 index, TRuntimeNode varitem) {
                                if (index == 0) {
                                    return ctx.ProgramBuilder.NewEmptyOptional(ctx.ProgramBuilder.NewOptionalType(outputItemType));
                                }
                                return ctx.ProgramBuilder.NewOptional(ctx.ProgramBuilder.NewVariant(varitem, index - 1, outputItemType));
                            });
                        });

                        auto toReduceType = NCommon::BuildType(body, *tupleType->GetItems().front(), ctx.ProgramBuilder);
                        values = ctx.ProgramBuilder.OrderedFlatMap(values, [&](TRuntimeNode mapOut) {
                            return ctx.ProgramBuilder.VisitAll(mapOut, [&](ui32 index, TRuntimeNode varitem) {
                                if (index == 0) {
                                    return ctx.ProgramBuilder.NewOptional(varitem);
                                }
                                return ctx.ProgramBuilder.NewEmptyOptional(ctx.ProgramBuilder.NewOptionalType(toReduceType));
                            });
                        });
                    }
                }
            }

            TVector<TString> reduceBy = NYql::GetSettingAsColumnList(ytMapReduce.Settings().Ref(), EYtSettingType::ReduceBy);
            auto dict = ctx.ProgramBuilder.ToHashedDict(values, true, [&](TRuntimeNode item) {
                TVector<TRuntimeNode> keyItems;
                for (auto& column: reduceBy) {
                    keyItems.push_back(ctx.ProgramBuilder.Member(item, column));
                }
                return keyItems.size() == 1
                    ? keyItems.front()
                    : ctx.ProgramBuilder.NewTuple(keyItems);
            }, [&](TRuntimeNode item) {
                return item;
            });

            values = ctx.ProgramBuilder.DictPayloads(dict);
            TVector<std::pair<TString, bool>> sortBy = NYql::GetSettingAsColumnPairList(ytMapReduce.Settings().Ref(), EYtSettingType::SortBy);
            TVector<TString> filterBy = NYql::GetSettingAsColumnList(ytMapReduce.Settings().Ref(), EYtSettingType::ReduceFilterBy);
            if (!sortBy.empty() && sortBy.size() > reduceBy.size()) {
                // sort partial lists
                sortBy.erase(sortBy.begin(), sortBy.begin() + reduceBy.size());

                TRuntimeNode sortDirections;
                if (sortBy.size() > 1) {
                    TVector<TRuntimeNode> tupleItems(sortBy.size(), ctx.ProgramBuilder.NewDataLiteral(true));
                    std::transform(sortBy.cbegin(), sortBy.cend(), tupleItems.begin(), [&ctx](const auto& it) { return ctx.ProgramBuilder.NewDataLiteral(it.second); });
                    sortDirections = ctx.ProgramBuilder.NewTuple(tupleItems);
                } else {
                    sortDirections = ctx.ProgramBuilder.NewDataLiteral(sortBy.front().second);
                }

                values = ctx.ProgramBuilder.Map(values, [&](TRuntimeNode list) {
                    list = ctx.ProgramBuilder.Sort(list, sortDirections, [&](TRuntimeNode item) {
                        TVector<TRuntimeNode> keyItems;
                        for (auto& column: sortBy) {
                            keyItems.push_back(ctx.ProgramBuilder.Member(item, column.first));
                        }
                        return keyItems.size() == 1
                            ? keyItems.front()
                            : ctx.ProgramBuilder.NewTuple(keyItems);
                    });

                    if (NYql::HasSetting(ytMapReduce.Settings().Ref(), EYtSettingType::ReduceFilterBy)) {
                        list = ctx.ProgramBuilder.OrderedMap(list, [&filterBy, &ctx] (TRuntimeNode item) {
                            TRuntimeNode res = ctx.ProgramBuilder.NewEmptyStruct();
                            for (auto& column: filterBy) {
                                res = ctx.ProgramBuilder.AddMember(res, column, ctx.ProgramBuilder.Member(item, column));
                            }
                            return res;
                        });
                    }

                    return list;
                });
            }
            else if (!filterBy.empty()) {
                values = ctx.ProgramBuilder.Map(values, [&](TRuntimeNode list) {
                    list = ctx.ProgramBuilder.OrderedMap(list, [&filterBy, &ctx] (TRuntimeNode item) {
                        TRuntimeNode res = ctx.ProgramBuilder.NewEmptyStruct();
                        for (auto& column: filterBy) {
                            res = ctx.ProgramBuilder.AddMember(res, column, ctx.ProgramBuilder.Member(item, column));
                        }
                        return res;
                    });

                    return list;
                });
            }

            const auto arg = ytMapReduce.Reducer().Args().Arg(0).Raw();
            const auto reduceInputTypeSetting = NYql::GetSetting(ytMapReduce.Settings().Ref(), EYtSettingType::ReduceInputType);
            const auto reduceInputType = reduceInputTypeSetting ? reduceInputTypeSetting->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType() : &GetSeqItemType(*arg->GetTypeAnn());
            TType* mkqlItemType = NCommon::BuildType(ytMapReduce.Reducer().Ref(), *reduceInputType, ctx.ProgramBuilder);
            if (NYql::HasSetting(ytMapReduce.Settings().Ref(), EYtSettingType::KeySwitch)) {
                values = ctx.ProgramBuilder.Map(values, [&](TRuntimeNode list) {
                    list = ctx.ProgramBuilder.Enumerate(list);
                    list = ctx.ProgramBuilder.Map(list, [&](TRuntimeNode item) {
                        auto grpSwitch = ctx.ProgramBuilder.Equals(ctx.ProgramBuilder.Nth(item, 0), ctx.ProgramBuilder.NewDataLiteral(ui64(0)));
                        return ctx.ProgramBuilder.AddMember(ctx.ProgramBuilder.Nth(item, 1), YqlSysColumnKeySwitch, grpSwitch);
                    });
                    return list;
                });
            }

            TCallableBuilder callableBuilder(ctx.ProgramBuilder.GetTypeEnvironment(), "YtUngroupingList",
                ctx.ProgramBuilder.NewListType(mkqlItemType));
            callableBuilder.Add(values);
            values = TRuntimeNode(callableBuilder.Build(), false);

            values = arg->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow ?
                ctx.ProgramBuilder.ToFlow(values) : ctx.ProgramBuilder.Iterator(values, {});

            if (ETypeAnnotationKind::Multi == GetSeqItemType(*arg->GetTypeAnn()).GetKind())
                values = ExpandFlow(values, ctx);

            NCommon::TMkqlBuildContext innerCtx(ctx, {{arg, values}}, ytMapReduce.Reducer().Ref().UniqueId());

            values = NCommon::MkqlBuildExpr(ytMapReduce.Reducer().Body().Ref(), innerCtx);

            const auto& reduceOutItemType = GetSeqItemType(*ytMapReduce.Reducer().Body().Ref().GetTypeAnn());

            if (ETypeAnnotationKind::Multi == reduceOutItemType.GetKind()) {
                auto type = ytMapReduce.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back()->Cast<TListExprType>()->GetItemType();
                if (type->GetKind() == ETypeAnnotationKind::Variant) {
                    type = type->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>()->GetItems().back();
                }
                values = NarrowFlowOutput(ytMapReduce.Pos(), values, type->Cast<TStructExprType>(), ctx);
            }

            if (mapDirectOutputsCount > 0) {
                // remap reduce output to new indexes
                values = ctx.ProgramBuilder.OrderedMap(values, [&](TRuntimeNode redueOutItem) {
                    if (reduceOutItemType.GetKind() == ETypeAnnotationKind::Variant) {
                        return ctx.ProgramBuilder.VisitAll(redueOutItem, [&](ui32 idx, TRuntimeNode item) {
                            return ctx.ProgramBuilder.NewVariant(item, idx + mapDirectOutputsCount, outputItemType);
                        });
                    }
                    return ctx.ProgramBuilder.NewVariant(redueOutItem, mapDirectOutputsCount, outputItemType);
                });

                // prepend with map output
                values = ctx.ProgramBuilder.Extend({mapDirectOutputs, ToList(values, ctx)});
            }

            auto res = BuildTableOutput(values, ctx);
            return res;
        });

    compiler.AddCallable(TYtFill::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {

            TYtFill ytFill(&node);

            auto values = NCommon::MkqlBuildExpr(ytFill.Content().Body().Ref(), ctx);

            if (ETypeAnnotationKind::Multi == GetSeqItemType(*ytFill.Content().Body().Ref().GetTypeAnn()).GetKind())
                values = NarrowFlow(values, ytFill, ctx);

            auto res = BuildTableOutput(values, ctx);
            return res;
        });

    compiler.AddCallable("Pull",
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            TPull pull(&node);
            auto clusterName = GetClusterName(pull.Input());
            const auto itemType = NCommon::BuildType(pull.Input().Ref(), *pull.Input().Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType(), ctx.ProgramBuilder);
            if (auto out = pull.Input().Maybe<TYtOutput>()) {

                return BuildTableInput(
                    itemType,
                    clusterName, pull.Input().Ref(), ctx, THashSet<TString>{}, false);

            } else {
                auto read = pull.Input().Maybe<TCoRight>().Input().Maybe<TYtReadTable>();
                YQL_ENSURE(read, "Unknown operation input");

                const bool forceKeyColumns = HasRangesWithKeyColumns(read.Cast().Input().Ref());
                TRuntimeNode values = BuildTableInput(
                    itemType,
                    clusterName, read.Cast().Input().Ref(), ctx,
                    THashSet<TString>{"num", "index"}, forceKeyColumns);

                values = ApplyPathRangesAndSampling(values, itemType, read.Cast().Input().Ref(), ctx);

                return values;
            }
        });
} // RegisterYtFileMkqlCompilers


void RegisterDqYtFileMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler) {
    compiler.OverrideCallable(TDqReadWideWrap::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            const auto wrapper = TDqReadWrapBase(&node);
            if (wrapper.Input().Maybe<TYtReadTable>().IsValid()) {
                auto ytRead = wrapper.Input().Cast<TYtReadTable>();
                auto cluster = TString{ytRead.DataSource().Cluster().Value()};
                const auto outputType = NCommon::BuildType(wrapper.Ref(),
                    *ytRead.Input().Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[0]->Cast<TListExprType>()->GetItemType(), ctx.ProgramBuilder);

                const bool forceKeyColumns = HasRangesWithKeyColumns(ytRead.Input().Ref());
                auto values = BuildTableContentCall("YtTableInputFile", outputType, cluster,
                    ytRead.Input().Ref(), Nothing(), ctx, false, THashSet<TString>{"num", "index"}, forceKeyColumns);
                values = ApplyPathRangesAndSampling(values, outputType, ytRead.Input().Ref(), ctx);

                return ExpandFlow(ctx.ProgramBuilder.ToFlow(values), ctx);
            }

            return TRuntimeNode();
        });

    compiler.OverrideCallable(TDqReadBlockWideWrap::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            const auto wrapper = TDqReadWrapBase(&node);
            if (wrapper.Input().Maybe<TYtReadTable>().IsValid()) {
                auto ytRead = wrapper.Input().Cast<TYtReadTable>();
                auto cluster = TString{ytRead.DataSource().Cluster().Value()};
                const auto outputType = NCommon::BuildType(wrapper.Ref(),
                    *ytRead.Input().Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[0]->Cast<TListExprType>()->GetItemType(), ctx.ProgramBuilder);

                const bool forceKeyColumns = HasRangesWithKeyColumns(ytRead.Input().Ref());
                auto values = BuildTableContentCall("YtTableInputFile", outputType, cluster,
                    ytRead.Input().Ref(), Nothing(), ctx, false, THashSet<TString>{"num", "index"}, forceKeyColumns);
                values = ApplyPathRangesAndSampling(values, outputType, ytRead.Input().Ref(), ctx);
                return ctx.ProgramBuilder.FromFlow(ctx.ProgramBuilder.WideToBlocks(ExpandFlow(ctx.ProgramBuilder.ToFlow(values), ctx)));
            }

            return TRuntimeNode();
        });

    compiler.OverrideCallable(TYtDqWideWrite::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            const auto wideWrite = TYtDqWideWrite(&node);

            auto values = NCommon::MkqlBuildExpr(wideWrite.Input().Ref(), ctx);

            auto tableName = GetSetting(wideWrite.Settings().Ref(), "tableName")->Child(1)->Content();
            auto tableType = GetSetting(wideWrite.Settings().Ref(), "tableType")->Child(1)->Content();

            TStringStream err;
            auto inputItemType = NCommon::ParseTypeFromYson(tableType, ctx.ProgramBuilder, err);
            YQL_ENSURE(inputItemType, "Parse type error: " << err.Str());

            auto structType = AS_TYPE(TStructType, inputItemType);
            values = NarrowFlow(values, *structType, ctx);
            values = ctx.ProgramBuilder.Map(values, [&](TRuntimeNode item) {
                return BuildDqWrite(item, tableName, ctx);
            });
            return values;
        });
}

} // NYql
