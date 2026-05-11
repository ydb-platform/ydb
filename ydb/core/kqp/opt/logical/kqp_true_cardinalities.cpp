#include "kqp_true_cardinalities.h"

#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_cbo.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/library/query_actor/query_actor.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <yql/essentials/core/yql_statistics.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <yql/essentials/utils/log/log.h>
#include <ydb/library/yql/dq/opt/dq_opt_join_cost_based.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>

#include <library/cpp/disjoint_sets/disjoint_sets.h>

namespace NKikimr::NKqp::NOpt {

    struct TTrueCardinalitiesEvents {
        enum ETrueCardinalitiesEvents {
            EvSubQueryResponse
        };
    };

    struct TTrueCardinalitiesResponseEvent: public TEventLocal<TTrueCardinalitiesResponseEvent, TTrueCardinalitiesEvents::EvSubQueryResponse> {
        TOptimizerTrueCardinalitiesHints::TTrueCardsView View;
    };

    class TTrueCardinalitiesWorker: public TActorBootstrapped<TTrueCardinalitiesWorker> {
        static constexpr size_t MaxRetries = 3;
        static constexpr size_t BackOffMs = 30 * 1000;

    public:
        TTrueCardinalitiesWorker(TActorId admin, TString queryId, TString database, TString ast, TVector<TString> labels, std::shared_ptr<TOptimizerHints> hints, uint64_t timeoutSecs)
            : AdminActorId(admin)
            , QueryId(std::move(queryId))
            , Database(std::move(database))
            , Ast(std::move(ast))
            , Labels(std::move(labels))
            , Hints(hints)
            , TimeoutSecs(timeoutSecs)
        {
        }

        void SendRequest() {
            Rows.clear();

            auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
            ev->Record.MutableRequest()->SetQuery(Ast);
            ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_AST_SCAN);
            ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            ev->Record.MutableRequest()->SetDatabase(Database);
            ev->Record.MutableRequest()->SetApplicationName(QueryId);
            ev->Record.MutableRequest()->SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE);
            ev->Record.MutableRequest()->SetTimeoutMs(TimeoutSecs * 1000);
            ev->Record.MutableRequest()->SetCancelAfterMs(TimeoutSecs * 1000);

            TUserRequestContext ctx;
            ctx.Hints = Hints;
            ctx.CostBasedOptimizationLevelOverride = 3;
            ev->SetUserRequestContext(MakeIntrusive<TUserRequestContext>(ctx));
            ActorIdToProto(SelfId(), ev->Record.MutableRequestActorId());

            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release());
        }

        void Bootstrap() {
            SendRequest();
            Become(&TThis::State);
        }

        STATEFN(State) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
                hFunc(NKqp::TEvKqpExecuter::TEvStreamData, HandleStream);
                cFunc(TEvents::TEvPoison::EventType, PassAway);
                cFunc(TEvents::TEvWakeup::EventType, SendRequest);
            }
        }

        void FillSubQueryStats(const NKqpProto::TKqpStatsQuery& queryStats, TOptimizerTrueCardinalitiesHints::TSubQueryStats& subQueryStats) {
            subQueryStats.TotalTimeUs = queryStats.GetDurationUs();
            uint64_t cpuTimeUs = 0;
            for (const auto& execution : queryStats.GetExecutions()) {
                cpuTimeUs += execution.GetCpuTimeUs();
            }
            subQueryStats.CpuTimeUs = cpuTimeUs;
        }

        uint64_t CalculateBytesUsed(const NKqpProto::TKqpStatsQuery& queryStats) {
            uint64_t bytesUsed = 0;
            for (const auto& execution : queryStats.GetExecutions()) {
                for (const auto& table : execution.GetTables()) {
                    bytesUsed += table.GetReadBytes();
                }
                for (const auto& stage : execution.GetStages()) {
                    for (const auto& actor : stage.GetComputeActors()) {
                        for (const auto& task : actor.GetTasks()) {
                            bytesUsed += task.GetOutputBytes();
                        }
                    }
                }
            }
            return bytesUsed;
        }

        void FillResponse(TOptimizerTrueCardinalitiesHints::TTrueCardsView& view, const ::NKqpProto::TKqpStatsQuery& queryStats) {
            YQL_ENSURE(Rows.size() == 1, "Expected 1 row, got " << Rows.size());
            view.TrueCardinalityHint = Rows.at(0).items().Get(0).uint64_value();
            view.TrueBytesHint = CalculateBytesUsed(queryStats);
            FillSubQueryStats(queryStats, view.SubQueryStats);
        }

        static bool IsRetriable(Ydb::StatusIds::StatusCode status) {
            switch (status) {
                case Ydb::StatusIds::TIMEOUT:
                case Ydb::StatusIds::UNAVAILABLE:
                    return true;
                default:
                    return false;
            }
        }

        void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& response) {
            const auto& rec = response->Get()->Record;
            const auto& effectivePool = rec.GetResponse().GetEffectivePoolId();
            Cout << QueryId << " EffectivePoolId=" << (effectivePool.empty() ? "<empty>" : effectivePool) << Endl;
            auto resp = MakeHolder<TTrueCardinalitiesResponseEvent>();
            bool ok = rec.GetYdbStatus() == Ydb::StatusIds::SUCCESS && Rows.size() == 1;
            if (!ok) {
                Cout << QueryId << " Failed with status: " << ToString(rec.GetYdbStatus()) << Endl;
                if (auto issues = rec.GetResponse().GetQueryIssues(); !issues.empty()) {
                    Cout << "got " << issues.size() << " issues" << Endl;
                    for (const auto& issue : issues) {
                        Cout << issue.Getmessage() << Endl;
                    }
                }
                if (RetryCount < MaxRetries && IsRetriable(rec.GetYdbStatus())) {
                    Cout << "retrying in " << BackOffMs << "ms" << Endl;
                    auto backoff = TDuration::MilliSeconds(BackOffMs << RetryCount);
                    ++RetryCount;
                    Schedule(backoff, new TEvents::TEvWakeup());
                    return;
                }
            } else {
                const auto& queryStats = rec.GetResponse().GetQueryStats();
                FillResponse(resp.GetRef().View, queryStats);
            }
            // Cout << rec.GetResponse().AsJSON() << '\n';
            Send(AdminActorId, resp.Release());
            PassAway();
        }

        void HandleStream(NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev) {
            auto& rec = ev->Get()->Record;
            for (auto& row : *rec.MutableResultSet()->mutable_rows()) {
                Rows.emplace_back(std::move(row));
            }
            auto ack = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(
                rec.GetSeqNo(), rec.GetChannelId());
            ack->Record.SetFreeSpace(std::numeric_limits<i64>::max());
            Send(ev->Sender, ack.Release());
        }

    private:
        const TActorId AdminActorId;
        const TString QueryId;
        const TString Database;
        const TString Ast;
        TVector<TString> Labels;

        std::shared_ptr<TOptimizerHints> Hints;
        uint64_t TimeoutSecs;

        TVector<Ydb::Value> Rows;
        size_t RetryCount = 0;
    };

    class TTrueCardinalitiesAdmin: public TActorBootstrapped<TTrueCardinalitiesAdmin> {
    public:
        static constexpr size_t MaxConcurrentSubQueriesDefaultValue = 2;

        TTrueCardinalitiesAdmin(NThreading::TPromise<void> IsFinished, TString database, TVector<TString> asts, std::shared_ptr<TOptimizerTrueCardinalitiesHints> hints, uint64_t timeoutSecs, std::shared_ptr<TOptimizerHints> preCalcedHints, size_t maxConcurrent)
            : IsFinishedPromise(std::move(IsFinished))
            , Database(database)
            , Asts(std::move(asts))
            , Hints(hints)
            , TimeoutSecs(timeoutSecs)
            , PreCalcedHints(preCalcedHints)
            , MaxConcurrent(Max<size_t>(maxConcurrent, 1))
            , Timer(TMonotonic::Now())
        {
            RunningSubQueries.resize(Asts.size());
        }

        void SendSingleRequest(size_t idx) {
            if (Finished) {
                return;
            }
            YQL_ENSURE(RunningSubQueries[idx] == TActorId(), "Running " << idx << " second time");
            auto timeout = TimeoutSecs - (TMonotonic::Now() - Timer).Seconds();
            auto view = Hints->GetView(idx);
            Cout << "Scheduling task " << idx << " with timeout " << timeout << " labels: " << view.Labels << Endl;
            IActor* requestHandler = new TTrueCardinalitiesWorker(SelfId(), JoinSeq(',', view.Labels), Database, Asts[idx], view.Labels, CollectFinishedHints(), timeout);
            RunningSubQueries[idx] = Register(requestHandler, TMailboxType::HTSwap, ActorContext().ActorSystem()->AppData<TAppData>()->UserPoolId);
        }

        void SendFirstBatch() {
            auto batchSize = Min(MaxConcurrent, Asts.size());
            IdxOfNextQueryToRun = batchSize;
            Cout << "TrueCardinalities concurrency=" << MaxConcurrent << ", total subqueries=" << Asts.size() << Endl;
            for (size_t i = 0; i < batchSize; ++i) {
                SendSingleRequest(i);
            }
        }

        std::shared_ptr<TOptimizerHints> CollectFinishedHints() {
            auto hints = std::make_shared<TOptimizerHints>();
            if (PreCalcedHints) {
                *hints = *PreCalcedHints;
            }
            for (size_t i = 0; i < Asts.size(); ++i) {
                auto view = Hints->GetView(i);
                if (view.TrueCards.TrueCardinalityHint != 0) {
                    hints->CardinalityHints->PushBack(view.Labels, TCardinalityHints::Replace, view.TrueCards.TrueCardinalityHint, "");
                }
            }
            return hints;
        }

        void Bootstrap() {
            auto timeout = TDuration::Seconds(TimeoutSecs);
            Schedule(timeout, new TEvents::TEvWakeup());
            SendFirstBatch();
            Become(&TThis::State);
        }

        void Handle(TTrueCardinalitiesResponseEvent::TPtr& ev) {
            ++FinishedTasks;
            auto view = ev->Get()->View;
            size_t idx = FindIndex(RunningSubQueries, ev->Sender);
            YQL_ENSURE(idx != NPOS);
            Cout << "Finished: " << idx << " in " << view.SubQueryStats.TotalTimeUs / 1000'000 << " seconds" << Endl;
            RunningSubQueries[idx] = TActorId();
            Hints->MergeTrueCardsView(idx, view);

            if (FinishedTasks == Asts.size()) {
                Finish();
                return;
            }
            if (IdxOfNextQueryToRun < Asts.size()) {
                SendSingleRequest(IdxOfNextQueryToRun++);
            }
        }

        void Finish() {
            Cout << "Finish Admin with " << FinishedTasks << " tasks" << Endl;
            Finished = true;
            for (auto& id : RunningSubQueries) {
                if (id) {
                    Send(id, new TEvents::TEvPoison());
                }
            }
            IsFinishedPromise.SetValue();
            PassAway();
        }

        STATEFN(State) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TTrueCardinalitiesResponseEvent, Handle);
                cFunc(TEvents::TEvWakeup::EventType, Finish);
                cFunc(TEvents::TEvPoison::EventType, Finish);
            }
        }

    private:
        NThreading::TPromise<void> IsFinishedPromise;
        const TString Database;
        TVector<TString> Asts;
        std::shared_ptr<TOptimizerTrueCardinalitiesHints> Hints;
        const uint64_t TimeoutSecs;
        std::shared_ptr<TOptimizerHints> PreCalcedHints;
        const size_t MaxConcurrent;

        TMonotonic Timer;
        size_t IdxOfNextQueryToRun{0};
        size_t FinishedTasks{0};
        bool Finished{false};

        TVector<TActorId> RunningSubQueries;
    };

    class TKqpTrueCardinalities: public TGraphTransformerBase {
        static constexpr size_t TimeoutSecondsDefault = 55 * 60;

    public:
        TKqpTrueCardinalities(
            TIntrusivePtr<NOpt::TKqpOptimizeContext>& kqpCtx,
            TTypeAnnotationContext& typesCtx,
            const TKikimrConfiguration::TPtr& config,
            const TString& cluster,
            const TString& database,
            TActorSystem* actorSystem)
            : KqpCtx(*kqpCtx)
            , Config(config)
            , TypesCtx(typesCtx)
            , Cluster(cluster)
            , ActorSystem(actorSystem)
            , Database(database)
        {
        }

        // Main method of the transformer
        IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

        NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final;

        IGraphTransformer::TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

        void Rewind() override {
        }

        ~TKqpTrueCardinalities() override {
            if (AdminActorId) {
                ActorSystem->Send(AdminActorId, new TEvents::TEvPoison());
            }
        }

    private:
        TMaybeNode<TExprBase> OptimizeEquiJoinWithCosts(TExprBase node, TExprContext& ctx, TOptimizerTrueCardinalitiesHints* costBasedHints);

        void FillCostBasedHints(TExprNode::TPtr& input, TExprContext& ctx);

    private:
        NOpt::TKqpOptimizeContext& KqpCtx;
        const TKikimrConfiguration::TPtr& Config;
        TTypeAnnotationContext& TypesCtx;
        const TString Cluster;
        TActorSystem* const ActorSystem;
        const TString Database;

        bool HintsCollected = false;
        std::shared_ptr<TOptimizerTrueCardinalitiesHints> CostBasedHints = std::make_shared<TOptimizerTrueCardinalitiesHints>();

        TActorId AdminActorId;

        NThreading::TFuture<void> AsyncReadiness;
    };

    using namespace NThreading;
    using namespace NYql;

    TMaybeNode<TExprBase> TKqpTrueCardinalities::OptimizeEquiJoinWithCosts(TExprBase node, TExprContext& ctx, TOptimizerTrueCardinalitiesHints* costBasedHints) {
        TCBOSettings settings{
            .MaxDPhypDPTableSize = Config->MaxDPHypDPTableSize.Get().GetOrElse(TDqSettings::TDefault::MaxDPHypDPTableSize),
            .ShuffleEliminationJoinNumCutoff = Config->ShuffleEliminationJoinNumCutoff.Get().GetOrElse(TDqSettings::TDefault::ShuffleEliminationJoinNumCutoff)};

        auto optLevel = Config->CostBasedOptimizationLevel.Get().GetOrElse(Config->GetDefaultCostBasedOptimizationLevel());
        bool enableShuffleElimination = false;
        auto providerCtx = TKqpProviderContext(KqpCtx, optLevel);
        auto stats = TypesCtx.GetStats(node.Raw());
        NYql::NDq::TTableAliasMap* tableAliases = stats ? stats->TableAliases.Get() : nullptr;
        auto opt = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(providerCtx, settings, ctx, enableShuffleElimination, TypesCtx.OrderingsFSM, tableAliases));

        TExprBase output = NYql::NDq::DqOptimizeEquiJoinWithCosts(node, ctx, TypesCtx, optLevel,
                                                                  *opt, [](auto& rels, auto label, auto node, auto stat) {
                                                                      rels.emplace_back(std::make_shared<TKqpRelOptimizerNode>(TString(label), *stat, node));
                                                                  },
                                                                  KqpCtx.EquiJoinsCount,
                                                                  KqpCtx.GetOptimizerHints(),
                                                                  enableShuffleElimination,
                                                                  &KqpCtx.ShufflingOrderingsByJoinLabels,
                                                                  costBasedHints);
        return output;
    }

    class TransitiveRelsManager {
    public:
        using TElement = std::pair<TStringBuf, TStringBuf>;

        TransitiveRelsManager()
            : Dsu(0)
        {
        }

        void mergeSets(const TElement& primary, const TElement& secondary) {
            auto secondaryId = RelToId.at(secondary);
            IdToRel[secondaryId] = primary;
            Dsu.UnionSets(RelToId.at(primary), secondaryId);
        }

        void registerRel(const TElement& rel) {
            RelToId[rel] = IdToRel.size();
            IdToRel.push_back(rel);
            Dsu.Expand(IdToRel.size());
        }

        TElement getTransitiveRel(const TElement& rel) {
            if (!RelToId.contains(rel)) {
                registerRel(rel);
            }
            return IdToRel[Dsu.CanonicSetElement(RelToId.at(rel))];
        }

    private:
        THashMap<TElement, size_t> RelToId;
        TDisjointSets Dsu;
        TVector<TElement> IdToRel;
    };

    TMaybe<TExprBase> PruneTreeRec(const TCoEquiJoin& equiJoin, const TCoEquiJoinTuple& tuple, TExprContext& ctx, const THashSet<TString>& labels, TransitiveRelsManager& transitiveRels) {
        const auto& leftScope = tuple.LeftScope();
        const auto& rightScope = tuple.RightScope();
        TMaybe<TExprBase> leftExpr, rightExpr;
        if (leftScope.Maybe<TCoEquiJoinTuple>()) {
            leftExpr = PruneTreeRec(equiJoin, leftScope.Cast<TCoEquiJoinTuple>(), ctx, labels, transitiveRels);
        } else {
            auto scope = leftScope.Cast<TCoAtom>().StringValue();
            if (labels.contains(scope)) {
                leftExpr = leftScope;
            }
        }

        if (rightScope.Maybe<TCoEquiJoinTuple>()) {
            rightExpr = PruneTreeRec(equiJoin, rightScope.Cast<TCoEquiJoinTuple>(), ctx, labels, transitiveRels);
        } else {
            auto scope = rightScope.Cast<TCoAtom>().StringValue();
            if (labels.contains(scope)) {
                rightExpr = rightScope;
            }
        }
        auto label = JoinSeq(',', labels);
        TVector<TCoAtom> newLeftKeys, newRightKeys;
        YQL_ENSURE(tuple.LeftKeys().Size() == tuple.RightKeys().Size());
        YQL_ENSURE(tuple.LeftKeys().Size() % 2 == 0);

        auto buildAtom = [&ctx](TStringBuf value, TPositionHandle pos) {
            return Build<TCoAtom>(ctx, pos).Value(value).Done();
        };
        for (size_t i = 0; i < tuple.LeftKeys().Size(); i += 2) {
            auto leftTableKeyPair = transitiveRels.getTransitiveRel(std::make_pair(tuple.LeftKeys().Item(i), tuple.LeftKeys().Item(i + 1)));
            auto rightTableKeyPair = transitiveRels.getTransitiveRel(std::make_pair(tuple.RightKeys().Item(i), tuple.RightKeys().Item(i + 1)));

            bool leftInLabels = labels.contains(leftTableKeyPair.first);
            bool rightInLabels = labels.contains(rightTableKeyPair.first);
            if (leftInLabels && rightInLabels) {
                newLeftKeys.push_back(buildAtom(leftTableKeyPair.first, tuple.LeftKeys().Item(i).Pos()));
                newLeftKeys.push_back(buildAtom(leftTableKeyPair.second, tuple.LeftKeys().Item(i + 1).Pos()));
                newRightKeys.push_back(buildAtom(rightTableKeyPair.first, tuple.RightKeys().Item(i).Pos()));
                newRightKeys.push_back(buildAtom(rightTableKeyPair.second, tuple.RightKeys().Item(i + 1).Pos()));
            }

            if (leftInLabels) {
                transitiveRels.mergeSets(leftTableKeyPair, rightTableKeyPair);
            } else {
                transitiveRels.mergeSets(rightTableKeyPair, leftTableKeyPair);
            }
        }

        if (!rightExpr) {
            return leftExpr;
        }
        if (!leftExpr) {
            return rightExpr;
        }

        YQL_ENSURE(!newLeftKeys.empty() && !newRightKeys.empty(), "Has empty keys for " << "labels: " << label << " and tuple: " << KqpExprToPrettyString(*tuple.Ptr(), ctx));

        auto leftKeys = Build<TCoAtomList>(ctx, tuple.LeftKeys().Pos())
                            .Add(newLeftKeys)
                            .Done();
        auto rightKeys = Build<TCoAtomList>(ctx, tuple.RightKeys().Pos())
                             .Add(newRightKeys)
                             .Done();

        return Build<TCoEquiJoinTuple>(ctx, equiJoin.Pos())
            .Type(tuple.Type())
            .LeftScope(*leftExpr)
            .RightScope(*rightExpr)
            .LeftKeys(leftKeys)
            .RightKeys(rightKeys)
            .Options(tuple.Options())
            .Done();
    }

    TExprBase PruneJoinColumns(const TExprBase& joinSettings, TExprContext& ctx, const THashSet<TString>& labels)
    {
        TVector<TExprBase> newInputs;
        for (const auto& option : joinSettings.Ref().Children()) {
            if (option->Head().IsAtom("rename")) {
                TCoAtom fromName{option->Child(1)};
                YQL_ENSURE(!fromName.Value().empty());
                TCoAtom toName{option->Child(2)};
                if (labels.contains(fromName.Value().Before('.')) || labels.contains(toName.Value().Before('.'))) {
                    newInputs.push_back(TExprBase(option));
                }
            } else {
                newInputs.push_back(TExprBase(option));
            }
        }
        return Build<TExprList>(ctx, joinSettings.Pos())
            .Add(newInputs)
            .Done();
    }

    TExprBase PruneTreeByLabels(TExprNode::TPtr node, TExprContext& ctx, const THashSet<TString>& labels) {
        auto equiJoinPtr = FindNode(node, [](const TExprNode::TPtr& subnode) {
            return subnode->IsCallable("EquiJoin");
        });
        YQL_ENSURE(equiJoinPtr, "No EquiJoin found");
        auto equiJoin = TExprBase(equiJoinPtr).Cast<TCoEquiJoin>();

        auto joinTuple = equiJoin.Arg(equiJoin.ArgCount() - 2).Cast<TCoEquiJoinTuple>();
        TransitiveRelsManager transitiveRelsManager;
        auto pruned = PruneTreeRec(equiJoin, joinTuple, ctx, labels, transitiveRelsManager);
        YQL_ENSURE(pruned, "Failed to prune the tree");

        TVector<TExprBase> joinArgs;
        for (size_t i = 0; i < equiJoin.ArgCount() - 2; i++) {
            auto arg = equiJoin.Arg(i).Cast<TCoEquiJoinInput>();
            const auto& label = arg.Scope().Cast<TCoAtom>().StringValue();
            if (labels.contains(label)) {
                joinArgs.push_back(arg);
            }
        }
        YQL_ENSURE(labels.size() == joinArgs.size());

        if (labels.size() == 1) {
            return joinArgs.front().Cast<TCoEquiJoinInput>().List();
        }

        joinArgs.push_back(*pruned);

        // CollectJoinColumns;
        auto prunedSettings = PruneJoinColumns(equiJoin.Arg(equiJoin.ArgCount() - 1), ctx, labels);
        joinArgs.push_back(prunedSettings);

        return Build<TCoEquiJoin>(ctx, equiJoin.Pos())
            .Add(joinArgs)
            .Done();
    }

    TExprBase ConvertKqpAstToYqlAst(const TExprBase& kqpAst, TExprContext& ctx, const TString& cluster) {
        TOptimizeExprSettings settings(nullptr);
        TExprNode::TPtr output;
        auto status = OptimizeExpr(kqpAst.Ptr(), output, [&cluster](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            if (!node->IsCallable("KqlReadTableRanges")) {
                return node;
            }
            auto tableRanges = TExprBase(node).Cast<TKqlReadTableRanges>();
            auto table = tableRanges.Table();
            const TString tablePath(table.Path().Value());
            const auto pos = node->Pos();
            auto tableTuple = ctx.NewList(pos, {ctx.NewAtom(pos, "table"),
                                                ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, tablePath)})});

            auto key = ctx.NewCallable(pos, "Key", {tableTuple});
            // (DataSource 'kikimr '<cluster>)
            auto dataSource = Build<TKiDataSource>(ctx, pos)
                                  .Category()
                                  .Build(KikimrProviderName)
                                  .Cluster()
                                  .Build(cluster)
                                  .Done();

            auto world = Build<TCoWorld>(ctx, pos)
                             .Done();
            // (KiReadTable! world ds key columns '())
            auto kiRead = Build<TKiReadTable>(ctx, pos)
                              .World(world)
                              .DataSource(dataSource)
                              .TableKey(TExprBase(key))
                              .Select(tableRanges.Columns())
                              .Settings()
                              .Build()
                              .Done();

            // (Right! ...) — unwraps (world, list) → list
            return Build<TCoRight>(ctx, pos)
                .Input(kiRead)
                .Done()
                .Ptr();
        }, ctx, settings);
        YQL_ENSURE(status != IGraphTransformer::TStatus::Error);
        auto value = Build<TCoAsList>(ctx, kqpAst.Pos())
                         .Add<TCoAsStruct>()
                         .Add<TCoNameValueTuple>()
                         .Name()
                         .Build("column0")
                         .Value<TCoLength>()
                         .List(output)
                         .Build()
                         .Build()
                         .Build()
                         .Done();
        auto colAtom = Build<TCoAtom>(ctx, kqpAst.Pos()).Value("column0").Done();
        auto colList = Build<TCoAtomList>(ctx, kqpAst.Pos()).Add(colAtom).Done();
        auto kiResult = Build<TKiResult>(ctx, kqpAst.Pos())
                            .Value(value)
                            .Columns(colList)
                            .RowsLimit<TCoAtom>()
                            .Build("1")
                            .Discard<TCoAtom>()
                            .Build("false")
                            .Done();
        TKiDataQueryBlockSettings blockSettings;

        auto block = Build<TKiDataQueryBlock>(ctx, kqpAst.Pos())
                         .Results()
                         .Add(kiResult)
                         .Build()
                         .Effects()
                         .Build()
                         .Operations()
                         .Build()
                         .Settings(blockSettings.BuildNode(ctx, kqpAst.Pos()))
                         .Done();

        auto blocks = Build<TKiDataQueryBlocks>(ctx, kqpAst.Pos())
                          .Add(block)
                          .Done();
        return blocks;
    }

    void TKqpTrueCardinalities::FillCostBasedHints(TExprNode::TPtr& input, TExprContext& ctx) {
        TOptimizeExprSettings settings(&TypesCtx);
        auto status = OptimizeExpr(input, input, [this](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            if (!node->IsCallable("EquiJoin")) {
                return node;
            }
            auto ret = OptimizeEquiJoinWithCosts(TExprBase(node), ctx, CostBasedHints.get());
            if (!ret) {
                Cout << "error when calling OptimizeEquiJoinWithCosts" << Endl;
                return {};
            }
            TExprBase retNode = ret.Cast();
            if (retNode.Ptr() != node) {
                return retNode.Ptr();
            }
            return node;
        }, ctx, settings);
        YQL_ENSURE(status != IGraphTransformer::TStatus::Error);

        // already calculated hints, do not calculate them again, but use in subqueries
        if (KqpCtx.Hints) {
            TSet<TVector<TString>> labelsToDelete;
            for (size_t j = 0; j < KqpCtx.Hints->CardinalityHints->Hints.size(); ++j) {
                labelsToDelete.insert(KqpCtx.Hints->CardinalityHints->Hints[j].JoinLabels);
            }
            auto i = 0;
            while (i < CostBasedHints->Size()) {
                const auto& labels = CostBasedHints->Labels->at(i);
                if (labelsToDelete.contains(labels)) {
                    CostBasedHints->Swap(i, CostBasedHints->Size() - 1);
                    CostBasedHints->PopBack();
                    continue;
                }
                ++i;
            }
        }
    }

    template <typename T>
    void Permute(TVector<uint64_t>& permutation, TVector<T>& arr) {
        TVector<T> temp(arr.size());
        for (size_t i = 0; i < arr.size(); ++i) {
            temp[i] = std::move(arr[permutation[i]]);
        }
        arr = std::move(temp);
    }

    void SortLabelsSize(TOptimizerTrueCardinalitiesHints& hints) {
        TVector<uint64_t> permutation(hints.Size());
        Iota(permutation.begin(), permutation.end(), 0);
        Sort(permutation, [&hints](size_t l, size_t r) {
            auto lview = hints.GetView(l);
            auto rview = hints.GetView(r);
            if (lview.Labels.size() != rview.Labels.size()) {
                return lview.Labels.size() < rview.Labels.size();
            }
            return lview.Labels < rview.Labels;
        });
        auto newCostBased = TOptimizerTrueCardinalitiesHints();
        for (ssize_t i = 0; i < hints.Size(); ++i) {
            newCostBased.PushBack(hints.GetView(permutation[i]));
        }
        hints = std::move(newCostBased);
    }

    IGraphTransformer::TStatus TKqpTrueCardinalities::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
        output = input;
        if (!Config->OptTrueCardinalities.Get()) {
            return IGraphTransformer::TStatus::Ok;
        }
        if (HintsCollected) {
            return IGraphTransformer::TStatus::Ok;
        }
        HintsCollected = true;

        FillCostBasedHints(output, ctx);
        Cout << "collected hints of size: " << CostBasedHints->EstimatedCardinalityHints->size() << Endl;
        if (CostBasedHints->Size() == 0) {
            return TStatus::Ok;
        }
        size_t hintsSize = CostBasedHints->Labels->size();
        SortLabelsSize(*CostBasedHints);
        Cout << "input tree: " << KqpExprToPrettyString(TExprBase(output), ctx) << Endl;
        TVector<TString> asts(Reserve(hintsSize));
        for (const auto& labels : *CostBasedHints->Labels) {
            THashSet<TString> labelsSet(labels.begin(), labels.end());
            TExprBase pruned = PruneTreeByLabels(output, ctx, labelsSet);
            // Cout << "pruned " << JoinSeq(',', labels) << ": " << KqpExprToPrettyString(pruned, ctx) << Endl;
            auto yqlAst = ConvertKqpAstToYqlAst(pruned, ctx, KqpCtx.Cluster);
            asts.push_back(NYql::NCommon::SerializeExpr(ctx, *yqlAst.Ptr()));
        }

        NThreading::TPromise<void> promise = NThreading::NewPromise<void>();
        AsyncReadiness = promise.GetFuture();
        auto timeout = Config->TrueCardsTimeoutSec.Get().GetOrElse(TimeoutSecondsDefault);
        auto maxConcurrent = Config->TrueCardinalitiesConcurrency.Get().GetOrElse(TTrueCardinalitiesAdmin::MaxConcurrentSubQueriesDefaultValue);
        IActor* requestHandler = new TTrueCardinalitiesAdmin(std::move(promise), Database, std::move(asts), CostBasedHints, timeout, KqpCtx.Hints, maxConcurrent);
        AdminActorId = ActorSystem->Register(requestHandler, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
        return TStatus::Async;
    }

    TFuture<void> TKqpTrueCardinalities::DoGetAsyncFuture(const TExprNode&) {
        return AsyncReadiness;
    }

    IGraphTransformer::TStatus TKqpTrueCardinalities::DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) {
        output = input;
        YQL_ENSURE(AsyncReadiness.IsReady());
        Cout << "labels\toldCardinality\tnewCardinality" << Endl;
        for (size_t i = 0; i < CostBasedHints->Labels->size(); ++i) {
            auto view = CostBasedHints->GetView(i);
            Cout << view.Labels << "\t" << view.EstimatedCardinalityHint << "\t" << view.TrueCards.TrueCardinalityHint << Endl;
        }
        KqpCtx.TrueCardinalityHints = CostBasedHints;
        return TStatus::Ok;
    }

    TAutoPtr<IGraphTransformer> CreateKqpTrueCardinalities(
        TIntrusivePtr<NOpt::TKqpOptimizeContext>& kqpCtx,
        TTypeAnnotationContext& typesCtx,
        const TKikimrConfiguration::TPtr& config,
        const TString& cluster,
        const TString& database,
        TActorSystem* actorSystem) {
        return THolder<IGraphTransformer>(new TKqpTrueCardinalities(kqpCtx, typesCtx, config, cluster, database, actorSystem));
    }

} // namespace NKikimr::NKqp::NOpt
