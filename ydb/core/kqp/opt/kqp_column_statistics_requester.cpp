#include "kqp_column_statistics_requester.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <yql/essentials/core/yql_statistics.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <yql/essentials/utils/log/log.h>

namespace NKikimr::NKqp {

using namespace NThreading;
using namespace NYql;

void TKqpColumnStatisticsRequester::PropagateTableToLambdaArgument(const TExprNode::TPtr& input) {
    if (input->ChildrenSize() < 2) {
        return;
    }

    auto callableInput = input->ChildRef(0);


    for (size_t i = 1; i < input->ChildrenSize(); ++i) {
        auto maybeLambda = TExprBase(input->ChildRef(i));
        if (!maybeLambda.Maybe<TCoLambda>()) {
            continue;
        }

        auto lambda = maybeLambda.Cast<TCoLambda>();
        if (!lambda.Args().Size()){
            continue;
        }

        if (callableInput->IsList()){
            for (size_t j = 0; j < callableInput->ChildrenSize(); ++j){
                KqpTableByExprNode[lambda.Args().Arg(j).Ptr()] = KqpTableByExprNode[callableInput->Child(j)];
            }
        } else {
            KqpTableByExprNode[lambda.Args().Arg(0).Ptr()] = KqpTableByExprNode[callableInput.Get()];
        }
    }
}

IGraphTransformer::TStatus TKqpColumnStatisticsRequester::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    Y_UNUSED(ctx);

    output = input;
    auto optLvl = Config->CostBasedOptimizationLevel.Get().GetOrElse(TDqSettings::TDefault::CostBasedOptimizationLevel);
    auto enableColumnStats = Config->FeatureFlags.GetEnableColumnStatistics();
    if (!(optLvl > 0 && enableColumnStats)) {
        return IGraphTransformer::TStatus::Ok;
    }

    VisitExprLambdasLast(
        input,
        [&](const TExprNode::TPtr& input) {
            BeforeLambdas(input) || BeforeLambdasUnmatched(input);

            if (input->IsCallable()) {
                PropagateTableToLambdaArgument(input);
            }

            return true;
        },
        [&](const TExprNode::TPtr& input) {
            return AfterLambdas(input) || AfterLambdasUnmatched(input);
        }
    );

    std::vector<TFuture<TColumnStatisticsResponse>> futures;

    auto addStatRequest = [&](
            NStat::EStatType type,
            const THashMap<TString, THashSet<TString>>& columnsByTableName,
            auto alreadyHasStatistics) {
        struct TTableMeta {
            TString TableName;
            THashMap<ui32, TString> ColumnNameByTag;
        };

        THashMap<TPathId, TTableMeta> tableMetaByPathId;
        std::vector<NKikimr::NStat::TRequest> statRequests;
        for (const auto& [table, columns]: columnsByTableName) {
            auto tableMeta = Tables.GetTable(Cluster, table).Metadata;
            auto& columnsMeta = tableMeta->Columns;

            auto pathId = TPathId(tableMeta->PathId.OwnerId(), tableMeta->PathId.TableId());

            auto statsTableIt = TypesCtx.ColumnStatisticsByTableName.find(table);
            for (const auto& column: columns) {
                if (statsTableIt != TypesCtx.ColumnStatisticsByTableName.end()) {
                    auto statsColumnIt = statsTableIt->second->Data.find(column);
                    if (statsColumnIt != statsTableIt->second->Data.end()) {
                        if (alreadyHasStatistics(statsColumnIt->second)) {
                            continue;
                        }
                    }
                }

                if (!columnsMeta.contains(column)) {
                    YQL_CLOG(DEBUG, ProviderKikimr) << "Table: " + table + " doesn't contain " + column + " to request for column statistics";
                    continue;
                }

                YQL_CLOG(TRACE, DqCore) << "Requesting statistics for table: " << table << ", column: " << column;

                NKikimr::NStat::TRequest req;
                req.ColumnTag = columnsMeta[column].Id;
                req.PathId = pathId;
                statRequests.push_back(req);

                tableMetaByPathId[pathId].TableName = table;
                tableMetaByPathId[pathId].ColumnNameByTag[req.ColumnTag.value()] = column;
            }
        }

        if (statRequests.empty()) {
            return;
        }

        auto request = MakeHolder<NStat::TEvStatistics::TEvGetStatistics>();
        request->Database = Database;
        request->StatType = type;
        request->StatRequests = std::move(statRequests);

        auto callback = [tableMetaByPathId = std::move(tableMetaByPathId)]
    (TPromise<TColumnStatisticsResponse> promise, NStat::TEvStatistics::TEvGetStatisticsResult&& response) mutable {
            if (!response.Success) {
                promise.SetValue(NYql::NCommon::ResultFromError<TColumnStatisticsResponse>("can't get column statistics!"));
                return;
            }

            THashMap<TString, TOptimizerStatistics::TColumnStatMap> columnStatisticsByTableName;

            for (auto&& stat: response.StatResponses) {
                auto meta = tableMetaByPathId[stat.Req.PathId];
                auto columnName = meta.ColumnNameByTag[stat.Req.ColumnTag.value()];
                auto& columnStatistics = columnStatisticsByTableName[meta.TableName].Data[columnName];
                if (stat.CountMinSketch.CountMin) {
                    columnStatistics.CountMinSketch = std::move(stat.CountMinSketch.CountMin);
                }
                if (stat.EqWidthHistogram.Data) {
                    columnStatistics.EqWidthHistogramEstimator = std::make_shared<NKikimr::TEqWidthHistogramEstimator>(stat.EqWidthHistogram.Data);
                }
            }

            promise.SetValue(TColumnStatisticsResponse{.ColumnStatisticsByTableName = std::move(columnStatisticsByTableName)});
        };

        using TRequest = NStat::TEvStatistics::TEvGetStatistics;
        using TResponse = NStat::TEvStatistics::TEvGetStatisticsResult;

        auto promise = NewPromise<TColumnStatisticsResponse>();

        auto statServiceId = NStat::MakeStatServiceID(ActorSystem->NodeId);
        IActor* requestHandler = new TActorRequestHandler<TRequest, TResponse, TColumnStatisticsResponse>(statServiceId, request.Release(), promise, callback);
        ActorSystem->Register(requestHandler, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);

        futures.push_back(promise.GetFuture());
    };

    addStatRequest(
        NStat::EStatType::COUNT_MIN_SKETCH, CMColumnsByTableName,
        [](const TColumnStatistics& stats) { return !!stats.CountMinSketch; });
    addStatRequest(
        NStat::EStatType::EQ_WIDTH_HISTOGRAM, HistColumnsByTableName,
        [](const TColumnStatistics& stats) { return !!stats.EqWidthHistogramEstimator; });

    if (futures.empty()) {
        return IGraphTransformer::TStatus::Ok;
    }

    AsyncReadiness = NThreading::WaitAll(futures).Apply(
            [this, futures=std::move(futures)](const TFuture<void>&) mutable {
        for (auto& fut : futures) {
            if (fut.HasException()) {
                fut.TryRethrow();
            }

            auto newStats = fut.ExtractValue();
            if (!ColumnStatisticsResponse) {
                ColumnStatisticsResponse = std::move(newStats);
            } else {
                // merge statistics
                for (const auto& [table, column2Stat] : newStats.ColumnStatisticsByTableName) {
                    auto& oldColumn2Stat = ColumnStatisticsResponse->ColumnStatisticsByTableName[table];
                    for (const auto& [column, newStat] : column2Stat.Data) {
                        auto& oldStat = oldColumn2Stat.Data[column];
                        if (newStat.CountMinSketch) {
                            oldStat.CountMinSketch = newStat.CountMinSketch;
                        }
                        if (newStat.EqWidthHistogramEstimator) {
                            oldStat.EqWidthHistogramEstimator = newStat.EqWidthHistogramEstimator;
                        }
                    }
                }
            }
        }
    });

    return TStatus::Async;
}

IGraphTransformer::TStatus TKqpColumnStatisticsRequester::DoApplyAsyncChanges(TExprNode::TPtr, TExprNode::TPtr&, TExprContext&) {
    Y_ENSURE(AsyncReadiness.IsReady() && ColumnStatisticsResponse.has_value());

    if (!ColumnStatisticsResponse->Issues().Empty()) {
        TStringStream ss; ColumnStatisticsResponse->Issues().PrintTo(ss);
        YQL_CLOG(TRACE, ProviderKikimr) << "Can't load columns statistics for request: " << ss.Str();
        return IGraphTransformer::TStatus::Ok;
    }

    for (auto&& [tableName, columnStatistics]:  ColumnStatisticsResponse->ColumnStatisticsByTableName) {
        TypesCtx.ColumnStatisticsByTableName.insert(
            {std::move(tableName), new TOptimizerStatistics::TColumnStatMap(std::move(columnStatistics))}
        );
    }

    return TStatus::Ok;
}

TFuture<void> TKqpColumnStatisticsRequester::DoGetAsyncFuture(const TExprNode&) {
    return AsyncReadiness;
}

bool TKqpColumnStatisticsRequester::BeforeLambdas(const TExprNode::TPtr& input) {
    bool matched = true;

    if (TKqpTable::Match(input.Get())) {
        KqpTableByExprNode[input.Get()] = input.Get();
    } else if (auto maybeStreamLookup = TExprBase(input).Maybe<TKqpCnStreamLookup>()) {
        KqpTableByExprNode[input.Get()] = maybeStreamLookup.Cast().Table().Ptr();
    } else {
        matched = false;
    }

    return matched;
}

bool TKqpColumnStatisticsRequester::BeforeLambdasUnmatched(const TExprNode::TPtr& input) {
    for (const auto& node: input->Children()) {
        if (KqpTableByExprNode.contains(node)) {
            KqpTableByExprNode[input.Get()] = KqpTableByExprNode[node];
            return true;
        }
    }

    return true;
}

TMaybe<std::pair<TString, TString>> TKqpColumnStatisticsRequester::GetTableAndColumnNames(const TCoMember& member) {
    auto exprNode = TExprBase(member).Ptr();
    if (!KqpTableByExprNode.contains(exprNode) || KqpTableByExprNode[exprNode] == nullptr) {
        return {};
    }

    auto table = TExprBase(KqpTableByExprNode[exprNode]).Cast<TKqpTable>().Path().StringValue();
    auto column = member.Name().StringValue();
    size_t pointPos = column.find('.'); // table.column
    if (pointPos != TString::npos) {
        column = column.substr(pointPos + 1);
    }

    return std::pair{std::move(table), std::move(column)};
}


TVector<std::pair<TString, TString>> TKqpColumnStatisticsRequester::GetEquiJoinConditions(const TCoEquiJoin& equiJoin) {
    if (equiJoin.ArgCount() < 3) {
        return {};
    }

    THashMap<TString, TExprNode::TPtr> joinArgMap;

    for (size_t i = 0; i < equiJoin.ArgCount() - 2; ++i) {
        auto input = equiJoin.Arg(i).Cast<TCoEquiJoinInput>();
        auto joinArg = input.List().Ptr()->ChildPtr(0);

        auto scope = input.Scope();
        if (!scope.Maybe<TCoAtom>()){
            return {};
        }

        joinArgMap.insert({scope.Cast<TCoAtom>().StringValue(), joinArg});
    }

    TVector<std::pair<TString, TString>> result;
    auto joinTuple = equiJoin.Arg(equiJoin.ArgCount() - 2).Cast<TCoEquiJoinTuple>();
    GetEquiJoinConditions(joinTuple, joinArgMap, result);

    return result;
}

void TKqpColumnStatisticsRequester::GetEquiJoinConditions(const TCoEquiJoinTuple& joinTuple, 
        THashMap<TString, TExprNode::TPtr> & joinArgMap,
        TVector<std::pair<TString, TString>> & result) {

    if (auto left = joinTuple.LeftScope().Maybe<TCoEquiJoinTuple>()) {
        GetEquiJoinConditions(left.Cast(), joinArgMap, result);
    }
    if (auto right = joinTuple.RightScope().Maybe<TCoEquiJoinTuple>()) {
        GetEquiJoinConditions(right.Cast(), joinArgMap, result);  
    }

    size_t joinKeysCount = joinTuple.LeftKeys().Size() / 2;

    for (size_t i = 0; i < joinKeysCount; ++i) {
        size_t keyIndex = i * 2;

        auto leftScope = joinTuple.LeftKeys().Item(keyIndex).StringValue();
        auto leftColumn = joinTuple.LeftKeys().Item(keyIndex + 1).StringValue();

        auto rightScope = joinTuple.RightKeys().Item(keyIndex).StringValue();
        auto rightColumn = joinTuple.RightKeys().Item(keyIndex + 1).StringValue();

        auto leftArg = joinArgMap.at(leftScope);
        auto rightArg = joinArgMap.at(rightScope);

        YQL_CLOG(TRACE, CoreDq) << "Trying to add join stats";


        if (!KqpTableByExprNode.contains(leftArg.Get()) || KqpTableByExprNode.at(leftArg.Get()) == nullptr) {
            continue;
        }
        if (!KqpTableByExprNode.contains(rightArg.Get()) || KqpTableByExprNode.at(rightArg.Get()) == nullptr) {
            continue;
        }

        auto leftTable = TExprBase(KqpTableByExprNode.at(leftArg.Get())).Cast<TKqpTable>().Path().StringValue();
        auto rightTable = TExprBase(KqpTableByExprNode.at(rightArg.Get())).Cast<TKqpTable>().Path().StringValue();

        result.push_back(std::make_pair(leftTable, leftColumn));
        result.push_back(std::make_pair(rightTable, rightColumn));
    }
}

bool TKqpColumnStatisticsRequester::AfterLambdas(const TExprNode::TPtr& input) {
    bool matched = true;

    if (
        TCoFilterBase::Match(input.Get()) ||
        (TCoFlatMapBase::Match(input.Get()) && IsPredicateFlatMap(TExprBase(input).Cast<TCoFlatMapBase>().Lambda().Body().Ref())) ||
        TCoEquiJoin::Match(input.Get())
    ) {
        std::shared_ptr<TOptimizerStatistics> dummyStats = nullptr;
        auto computer = NDq::TPredicateSelectivityComputer(dummyStats, true);

        if (TCoFilterBase::Match(input.Get())) {
            computer.Compute(TExprBase(input).Cast<TCoFilterBase>().Lambda().Body());
        } else if (TCoFlatMapBase::Match(input.Get())) {
            computer.Compute(TExprBase(input).Cast<TCoFlatMapBase>().Lambda().Body());
        } else if (TCoEquiJoin::Match(input.Get())) {
            YQL_CLOG(TRACE, CoreDq) << "Fetching join stats";

            auto joinColumns = GetEquiJoinConditions(TCoEquiJoin(input));
            for (auto & [table, column] : joinColumns) {
                YQL_CLOG(TRACE, CoreDq) << "Adding join stats for table: " << table << ", column: " << column;
                HistColumnsByTableName[table].insert(std::move(column));
            }
        } else {
            Y_ENSURE(false);
        }

        auto columnStatsUsedMembers = computer.GetColumnStatsUsedMembers();
        for (const auto& item: columnStatsUsedMembers.Data) {
            if (auto maybeTableAndColumn = GetTableAndColumnNames(item.Member)) {
                const auto& [table, column] = *maybeTableAndColumn;
                using TColumnStatisticsUsedMember = NDq::TPredicateSelectivityComputer::TColumnStatisticsUsedMembers::TColumnStatisticsUsedMember;
                switch (item.PredicateType) {
                case TColumnStatisticsUsedMember::EEquality:
                    CMColumnsByTableName[table].insert(std::move(column));
                    break;
                case TColumnStatisticsUsedMember::EInequality:
                    HistColumnsByTableName[table].insert(std::move(column));
                    break;
                }
            }
        }

        auto memberEqualities = computer.GetMemberEqualities();
        for (const auto& [lhs, rhs]: memberEqualities) {
            auto maybeLhsTableAndColumn = GetTableAndColumnNames(lhs);
            if (!maybeLhsTableAndColumn) {
                continue;
            }

            auto maybeRhsTableAndColumn = GetTableAndColumnNames(rhs);
            if (!maybeRhsTableAndColumn) {
                continue;
            }

            // const auto& [lhsTable, lhsColumn] = *maybeLhsTableAndColumn;
            // const auto& [rhsTable, rhsColumn] = *maybeRhsTableAndColumn;
        }
    } else {
        matched = false;
    }

    return matched;
}

bool TKqpColumnStatisticsRequester::AfterLambdasUnmatched(const TExprNode::TPtr& input) {
    if (KqpTableByExprNode.contains(input.Get())) {
        return true;
    }

    for (const auto& node: input->Children()) {
        if (KqpTableByExprNode.contains(node)) {
            KqpTableByExprNode[input.Get()] = KqpTableByExprNode[node];
            return true;
        }
    }

    return true;
}

TAutoPtr<IGraphTransformer> CreateKqpColumnStatisticsRequester(
    const TKikimrConfiguration::TPtr& config,
    TTypeAnnotationContext& typesCtx,
    TKikimrTablesData& tables,
    const TString& cluster,
    const TString& database,
    TActorSystem* actorSystem
) {
    return THolder<IGraphTransformer>(new TKqpColumnStatisticsRequester(config, typesCtx, tables, cluster, database, actorSystem));
}

} // end of NKikimr::NKqp
