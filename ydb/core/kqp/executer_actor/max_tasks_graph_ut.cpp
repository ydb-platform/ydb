#include <library/cpp/testing/unittest/registar.h>

#include "max_tasks_graph.h"

using namespace NKikimr::NKqp;

namespace {

NYql::NDq::TStageId MakeStageId(ui32 txId, ui32 stageId) {
    return NYql::NDq::TStageId(txId, stageId);
}

TMaxTasksGraph InitGraph(size_t maxChannelsCount, size_t snapshotSize) {
    TMaxTasksGraph graph(maxChannelsCount);
    TVector<NKikimrKqp::TKqpNodeResources> snapshot(snapshotSize);
    for (size_t i = 0; i < snapshot.size(); ++i) {
        snapshot.at(i).SetNodeId(i);
    }
    if (snapshotSize) {
        graph.AddNodes(snapshot);
    }
    return graph;
}

} // namespace

Y_UNIT_TEST_SUITE(TMaxTasksGraphTest) {

    Y_UNIT_TEST(SingleStageSingleNodeNoShrink) {
        auto graph = InitGraph(1000, 1);

        auto stage = MakeStageId(0, 0);
        graph.AddStage(stage, TMaxTasksGraph::ANY, {});
        graph.AddTasks(stage, 0, 10);

        graph.Shrink();

        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stage, 0), 10);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stage), 10);
    }

    Y_UNIT_TEST(TwoStagesFullMeshChannels) {
        // 1 узел, 2 стадии: A -> B
        // A: 10 задач на узле 0, B: 10 задач на узле 0
        // Каналы на узле 0:
        //   Стадия A: 10 задач, output B (не copy) => channelsPerTask = total_tasks(B) = 10 => 10*10 = 100
        //   Стадия B: 10 задач, input A (не copy) => channelsPerTask = total_tasks(A) = 10 => 10*10 = 100
        //   Итого: 200

        auto graph = InitGraph(200, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 10);
        graph.AddTasks(stageB, 0, 10);

        graph.Shrink();

        // Лимит ровно хватает — не должно шринкать
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageA, 0), 10);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageB, 0), 10);
    }

    Y_UNIT_TEST(TwoStagesFullMeshChannelsShrinks) {
        // 1 узел, 2 стадии: A -> B, по 10 задач
        // Каналы = 200, но лимит = 100 => нужно шринкать

        auto graph = InitGraph(100, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 10);
        graph.AddTasks(stageB, 0, 10);

        graph.Shrink();

        auto tasksA = graph.GetStageTasksCount(stageA, 0);
        auto tasksB = graph.GetStageTasksCount(stageB, 0);

        // После шринка задач должно стать меньше
        UNIT_ASSERT_LE(tasksA, 10);
        UNIT_ASSERT_LE(tasksB, 10);
        UNIT_ASSERT_GE(tasksA, 1);
        UNIT_ASSERT_GE(tasksB, 1);

        // Проверяем, что каналы теперь в лимите
        // channels = tasksA * tasksB + tasksB * tasksA = 2 * tasksA * tasksB
        UNIT_ASSERT_LE(2 * tasksA * tasksB, 100u);
    }

    Y_UNIT_TEST(FixedStageNotScaled) {
        auto graph = InitGraph(50, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::FIXED, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 10);
        graph.AddTasks(stageB, 0, 10);

        graph.Shrink();

        // FIXED стадия должна остаться с 10 задачами
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageA, 0), 10);

        // B могла уменьшиться
        auto tasksB = graph.GetStageTasksCount(stageB, 0);
        UNIT_ASSERT_LE(tasksB, 10);
        UNIT_ASSERT_GE(tasksB, 1);
    }

    Y_UNIT_TEST(CopyStageScalesWithSource) {
        auto graph = InitGraph(1000, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1); // COPY of A
        auto stageC = MakeStageId(0, 2); // зависит от B, создаёт нагрузку

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::COPY, {stageA}, stageA);
        graph.AddStage(stageC, TMaxTasksGraph::ANY, {stageB});

        graph.AddTasks(stageA, 0, 20);
        graph.AddTasks(stageB, 0, 20);
        graph.AddTasks(stageC, 0, 20);

        graph.Shrink();

        // A и B должны иметь одинаковое число задач (COPY группа)
        auto tasksA = graph.GetStageTasksCount(stageA, 0);
        auto tasksB = graph.GetStageTasksCount(stageB, 0);
        UNIT_ASSERT_VALUES_EQUAL(tasksA, tasksB);
    }

    Y_UNIT_TEST(CopyOfFixedBecomesFixed) {
        // Если source — FIXED, то COPY тоже становится FIXED
        auto graph = InitGraph(50, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);
        auto stageC = MakeStageId(0, 2);

        graph.AddStage(stageA, TMaxTasksGraph::FIXED, {});
        graph.AddStage(stageB, TMaxTasksGraph::COPY, {stageA}, stageA);
        graph.AddStage(stageC, TMaxTasksGraph::ANY, {stageB});

        graph.AddTasks(stageA, 0, 5);
        graph.AddTasks(stageB, 0, 5);
        graph.AddTasks(stageC, 0, 10);

        graph.Shrink();

        // Обе стадии A и B фиксированы
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageA, 0), 5);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageB, 0), 5);
    }

    Y_UNIT_TEST(MultipleNodesDistribution) {
        auto graph = InitGraph(500, 3);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 5);
        graph.AddTasks(stageA, 1, 5);
        graph.AddTasks(stageA, 2, 5);
        graph.AddTasks(stageB, 0, 5);
        graph.AddTasks(stageB, 1, 5);
        graph.AddTasks(stageB, 2, 5);

        graph.Shrink();

        size_t totalA = graph.GetStageTasksCount(stageA);
        size_t totalB = graph.GetStageTasksCount(stageB);

        UNIT_ASSERT_GE(totalA, 1);
        UNIT_ASSERT_GE(totalB, 1);
        UNIT_ASSERT_LE(totalA, 15);
        UNIT_ASSERT_LE(totalB, 15);
    }

    Y_UNIT_TEST(AddTasksRoundRobinDistribution) {
        auto graph = InitGraph(1000, 3);

        auto stage = MakeStageId(0, 0);
        graph.AddStage(stage, TMaxTasksGraph::ANY, {});
        graph.AddTasks(stage, 7); // 7 задач на 3 узла round-robin: 3,2,2 или 3,3,1?

        // Round robin: 0,1,2,0,1,2,0 => node0=3, node1=2, node2=2
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stage, 0), 3);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stage, 1), 2);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stage, 2), 2);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stage), 7);
    }

    Y_UNIT_TEST(EmptyGraphShrinkDoesNotCrash) {
        auto graph = InitGraph(100, 0);
        graph.Shrink();
    }

    Y_UNIT_TEST(NoStagesShrinkDoesNotCrash) {
        auto graph = InitGraph(100, 3);
        graph.Shrink();
    }

    Y_UNIT_TEST(InfeasibleEvenAtMinimumDoesNotCrash) {
        // Даже 1 задача в каждой стадии даёт слишком много каналов
        // Нужно много стадий в цепочке или очень маленький лимит
        auto graph = InitGraph(0, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 5);
        graph.AddTasks(stageB, 0, 5);

        // Не крашится, просто оставляет как есть (или ничего не делает)
        graph.Shrink();
    }

    Y_UNIT_TEST(AlreadyFeasibleNoChange) {
        auto graph = InitGraph(10000, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 3);
        graph.AddTasks(stageB, 0, 4);

        graph.Shrink();

        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageA, 0), 3);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageB, 0), 4);
    }

    Y_UNIT_TEST(CopyConnectionUsesOneChannelPerTask) {
        // A -> B (copy), B -> C (full mesh)
        // На узле 0: A=10, B=10, C=10
        // Каналы стадии A: output B (copy) => 1 per task => 10*1 = 10
        // Каналы стадии B: input A (copy) => 1 per task, output C (full mesh) => 10 => 10*(1+10) = 110
        // Каналы стадии C: input B (full mesh) => 10 => 10*10 = 100
        // Итого: 10 + 110 + 100 = 220

        auto graph = InitGraph(220, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);
        auto stageC = MakeStageId(0, 2);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::COPY, {stageA}, stageA);
        graph.AddStage(stageC, TMaxTasksGraph::ANY, {stageB});

        graph.AddTasks(stageA, 0, 10);
        graph.AddTasks(stageB, 0, 10);
        graph.AddTasks(stageC, 0, 10);

        // Должно быть ровно feasible
        graph.Shrink();

        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageA, 0), 10);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageB, 0), 10);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageC, 0), 10);
    }

    Y_UNIT_TEST(CopyConnectionShrinks) {
        // Тот же граф, но лимит меньше => должно шринкнуть
        auto graph = InitGraph(100, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);
        auto stageC = MakeStageId(0, 2);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::COPY, {stageA}, stageA);
        graph.AddStage(stageC, TMaxTasksGraph::ANY, {stageB});

        graph.AddTasks(stageA, 0, 10);
        graph.AddTasks(stageB, 0, 10);
        graph.AddTasks(stageC, 0, 10);

        graph.Shrink();

        auto a = graph.GetStageTasksCount(stageA, 0);
        auto b = graph.GetStageTasksCount(stageB, 0);
        auto c = graph.GetStageTasksCount(stageC, 0);

        UNIT_ASSERT_LE(a, 10);
        UNIT_ASSERT_LE(b, 10);
        UNIT_ASSERT_LE(c, 10);
        UNIT_ASSERT_GE(a, 1);
        UNIT_ASSERT_GE(b, 1);
        UNIT_ASSERT_GE(c, 1);

        // A и B — COPY группа, должны быть равны
        UNIT_ASSERT_VALUES_EQUAL(a, b);
    }

    // ==================== Множественные входы ====================

    Y_UNIT_TEST(MultipleInputsChannelCount) {
        // A -> C, B -> C (оба full mesh)
        // На узле 0: A=5, B=5, C=5
        // Каналы C: inputs A(5) + B(5) => 5*(5+5) = 50
        // Каналы A: output C(5) => 5*5 = 25
        // Каналы B: output C(5) => 5*5 = 25
        // Итого: 100

        auto graph = InitGraph(100, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);
        auto stageC = MakeStageId(0, 2);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageC, TMaxTasksGraph::ANY, {stageA, stageB});

        graph.AddTasks(stageA, 0, 5);
        graph.AddTasks(stageB, 0, 5);
        graph.AddTasks(stageC, 0, 5);

        graph.Shrink();

        // Ровно хватает
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageA, 0), 5);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageB, 0), 5);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageC, 0), 5);
    }

    // ==================== Цепочка из трёх стадий ====================

    Y_UNIT_TEST(ThreeStageChainShrink) {
        // A -> B -> C, по 10 задач, 1 узел
        // Каналы A: out B(10) => 10*10 = 100
        // Каналы B: in A(10) + out C(10) => 10*20 = 200
        // Каналы C: in B(10) => 10*10 = 100
        // Итого: 400

        auto graph = InitGraph(200, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);
        auto stageC = MakeStageId(0, 2);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});
        graph.AddStage(stageC, TMaxTasksGraph::ANY, {stageB});

        graph.AddTasks(stageA, 0, 10);
        graph.AddTasks(stageB, 0, 10);
        graph.AddTasks(stageC, 0, 10);

        graph.Shrink();

        auto a = graph.GetStageTasksCount(stageA, 0);
        auto b = graph.GetStageTasksCount(stageB, 0);
        auto c = graph.GetStageTasksCount(stageC, 0);

        UNIT_ASSERT_LE(a, 10);
        UNIT_ASSERT_LE(b, 10);
        UNIT_ASSERT_LE(c, 10);
        UNIT_ASSERT_GE(a, 1);
        UNIT_ASSERT_GE(b, 1);
        UNIT_ASSERT_GE(c, 1);
    }

    // ==================== ScaleTasks: пропорциональное распределение ====================

    Y_UNIT_TEST(MultiNodeProportionalScaling) {
        // 2 узла, A -> B
        // A: node0=8, node1=2, total=10
        // B: node0=6, node1=4, total=10
        // Каналы node0: A: out B(10) => 8*10=80, B: in A(10) => 6*10=60 => 140
        // Каналы node1: A: out B(10) => 2*10=20, B: in A(10) => 4*10=40 => 60
        // Лимит 140 => feasible, не шринкает

        auto graph = InitGraph(140, 2);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 8);
        graph.AddTasks(stageA, 1, 2);
        graph.AddTasks(stageB, 0, 6);
        graph.AddTasks(stageB, 1, 4);

        graph.Shrink();

        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageA, 0), 8);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageA, 1), 2);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageB, 0), 6);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageB, 1), 4);
    }

    Y_UNIT_TEST(MultiNodeShrinkReducesTasks) {
        // То же, но лимит 50 => нужно шринкнуть
        auto graph = InitGraph(50, 2);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 8);
        graph.AddTasks(stageA, 1, 2);
        graph.AddTasks(stageB, 0, 6);
        graph.AddTasks(stageB, 1, 4);

        graph.Shrink();

        auto totalA = graph.GetStageTasksCount(stageA);
        auto totalB = graph.GetStageTasksCount(stageB);

        UNIT_ASSERT_LE(totalA, 10);
        UNIT_ASSERT_LE(totalB, 10);
        UNIT_ASSERT_GE(totalA, 1);
        UNIT_ASSERT_GE(totalB, 1);

        // Проверяем, что пропорции примерно сохранились
        // A: node0 должно быть >= node1
        UNIT_ASSERT_GE(graph.GetStageTasksCount(stageA, 0), graph.GetStageTasksCount(stageA, 1));
    }

    // ==================== Одиночная задача — не может быть меньше 1 ====================

    Y_UNIT_TEST(MinimumOneTaskPerStage) {
        auto graph = InitGraph(1, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 100);
        graph.AddTasks(stageB, 0, 100);

        graph.Shrink();

        // Даже при максимальном шринке должно быть >= 1
        UNIT_ASSERT_GE(graph.GetStageTasksCount(stageA, 0), 1);
        UNIT_ASSERT_GE(graph.GetStageTasksCount(stageB, 0), 1);
    }

    // ==================== Diamond graph (A -> B, A -> C, B -> D, C -> D) ====================

    Y_UNIT_TEST(DiamondGraphShrink) {
        auto graph = InitGraph(100, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);
        auto stageC = MakeStageId(0, 2);
        auto stageD = MakeStageId(0, 3);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});
        graph.AddStage(stageC, TMaxTasksGraph::ANY, {stageA});
        graph.AddStage(stageD, TMaxTasksGraph::ANY, {stageB, stageC});

        graph.AddTasks(stageA, 0, 10);
        graph.AddTasks(stageB, 0, 10);
        graph.AddTasks(stageC, 0, 10);
        graph.AddTasks(stageD, 0, 10);

        graph.Shrink();

        for (auto* stage : {&stageA, &stageB, &stageC, &stageD}) {
            UNIT_ASSERT_GE(graph.GetStageTasksCount(*stage, 0), 1);
            UNIT_ASSERT_LE(graph.GetStageTasksCount(*stage, 0), 10);
        }
    }

    // ==================== GetStageTasksCount без указания узла ====================

    Y_UNIT_TEST(GetStageTasksCountTotal) {
        auto graph = InitGraph(10000, 3);

        auto stage = MakeStageId(0, 0);
        graph.AddStage(stage, TMaxTasksGraph::ANY, {});

        graph.AddTasks(stage, 0, 3);
        graph.AddTasks(stage, 1, 5);
        graph.AddTasks(stage, 2, 7);

        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stage), 15);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stage, 0), 3);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stage, 1), 5);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stage, 2), 7);
    }

    Y_UNIT_TEST(IsolatedStageZeroChannels) {
        auto graph = InitGraph(0, 1);

        auto stage = MakeStageId(0, 0);
        graph.AddStage(stage, TMaxTasksGraph::ANY, {});
        graph.AddTasks(stage, 0, 100);

        // 0 каналов (нет входов/выходов) => feasible даже при лимите 0
        graph.Shrink();

        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stage, 0), 100);
    }

    Y_UNIT_TEST(TransitiveCopyChainShrinks) {
        // A -> B(copy A) -> C(copy B => resolves to A) -> D
        // Все три в одной COPY-группе, должны масштабироваться вместе
        auto graph = InitGraph(50, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);
        auto stageC = MakeStageId(0, 2);
        auto stageD = MakeStageId(0, 3);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::COPY, {stageA}, stageA);
        graph.AddStage(stageC, TMaxTasksGraph::COPY, {stageB}, stageB);
        graph.AddStage(stageD, TMaxTasksGraph::ANY, {stageC});

        graph.AddTasks(stageA, 0, 20);
        graph.AddTasks(stageB, 0, 20);
        graph.AddTasks(stageC, 0, 20);
        graph.AddTasks(stageD, 0, 20);

        graph.Shrink();

        auto a = graph.GetStageTasksCount(stageA, 0);
        auto b = graph.GetStageTasksCount(stageB, 0);
        auto c = graph.GetStageTasksCount(stageC, 0);

        // Все три в COPY-группе с root=A => одинаковый total
        UNIT_ASSERT_VALUES_EQUAL(a, b);
        UNIT_ASSERT_VALUES_EQUAL(b, c);
        UNIT_ASSERT_GE(a, 1);
        UNIT_ASSERT_LE(a, 20);
    }

    Y_UNIT_TEST(LargeGraphStressTest) {
        const size_t numNodes = 10;
        const size_t numStages = 20;
        const size_t tasksPerStage = 50;

        auto graph = InitGraph(500, numNodes);

        std::vector<NYql::NDq::TStageId> stages;
        for (size_t i = 0; i < numStages; ++i) {
            auto stage = MakeStageId(0, i);
            stages.push_back(stage);

            std::list<NYql::NDq::TStageId> inputs;
            if (i > 0) {
                inputs.push_back(stages[i - 1]);
            }

            graph.AddStage(stage, TMaxTasksGraph::ANY, inputs);
            graph.AddTasks(stage, tasksPerStage); // round-robin
        }

        graph.Shrink();

        for (const auto& stage : stages) {
            UNIT_ASSERT_GE(graph.GetStageTasksCount(stage), 1);
            UNIT_ASSERT_LE(graph.GetStageTasksCount(stage), tasksPerStage);
        }
    }

    Y_UNIT_TEST(BottleneckNodeDeterminesShrink) {
        // 2 узла, A -> B
        // node0: A=9, B=9 => channels = 2 * 9 * total(18) = 324 (для node0: 9 * 18 + 9 * 18 = 324)
        // Нет, каналы = A_on_node * totalB + B_on_node * totalA
        // node0: 9*18 + 9*18 = 324
        // node1: 9*18 + 9*18 = 324
        // Равномерно, оба одинаковые. Сделаем неравномерно:
        // node0: A=15, B=15. node1: A=1, B=1. total=16
        // node0: 15*16 + 15*16 = 480
        // node1: 1*16 + 1*16 = 32
        // Лимит 400 — node0 является бутылочным горлышком

        auto graph = InitGraph(400, 2);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 15);
        graph.AddTasks(stageA, 1, 1);
        graph.AddTasks(stageB, 0, 15);
        graph.AddTasks(stageB, 1, 1);

        graph.Shrink();

        auto totalA = graph.GetStageTasksCount(stageA);
        auto totalB = graph.GetStageTasksCount(stageB);

        // Должно уменьшиться
        UNIT_ASSERT_LE(totalA, 16);
        UNIT_ASSERT_LE(totalB, 16);

        // Пропорции сохраняются: node0 >> node1
        UNIT_ASSERT_GE(graph.GetStageTasksCount(stageA, 0), graph.GetStageTasksCount(stageA, 1));
        UNIT_ASSERT_GE(graph.GetStageTasksCount(stageB, 0), graph.GetStageTasksCount(stageB, 1));

        // Проверяем, что лимит на обоих узлах соблюдён
        for (ui32 nodeId = 0; nodeId < 2; ++nodeId) {
            auto a = graph.GetStageTasksCount(stageA, nodeId);
            auto b = graph.GetStageTasksCount(stageB, nodeId);
            // Пересчитываем total после шринка
            auto newTotalA = graph.GetStageTasksCount(stageA);
            auto newTotalB = graph.GetStageTasksCount(stageB);
            auto realChannels = a * newTotalB + b * newTotalA;
            UNIT_ASSERT_LE(realChannels, 400u);
        }
    }

    Y_UNIT_TEST(FixedStageMakesInfeasible) {
        // FIXED стадия с 100 задачами, ANY стадия зависит от неё
        // Даже если ANY уменьшить до 1 задачи:
        // channels = 100*1 + 1*100 = 200
        // Лимит 100 — infeasible при любом alpha

        auto graph = InitGraph(100, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::FIXED, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 100);
        graph.AddTasks(stageB, 0, 100);

        // Shrink не крашится, но может не найти feasible решение
        graph.Shrink();

        // FIXED остаётся 100
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageA, 0), 100);

        // B уменьшилась максимально (infeasible => IsFeasible(lo=0) == false => ничего не меняется)
        // Значит B тоже осталась 100 (shrink сдался)
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageB, 0), 100);
    }

    Y_UNIT_TEST(TwoIndependentCopyGroups) {
        // Группа 1: A -> B (copy)
        // Группа 2: C -> D (copy)
        // E зависит от B и D (full mesh)
        auto graph = InitGraph(1000, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);
        auto stageC = MakeStageId(0, 2);
        auto stageD = MakeStageId(0, 3);
        auto stageE = MakeStageId(0, 4);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::COPY, {stageA}, stageA);
        graph.AddStage(stageC, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageD, TMaxTasksGraph::COPY, {stageC}, stageC);
        graph.AddStage(stageE, TMaxTasksGraph::ANY, {stageB, stageD});

        graph.AddTasks(stageA, 0, 10);
        graph.AddTasks(stageB, 0, 10);
        graph.AddTasks(stageC, 0, 20);
        graph.AddTasks(stageD, 0, 20);
        graph.AddTasks(stageE, 0, 5);

        graph.Shrink();

        // Группа 1: A и B одинаковые
        UNIT_ASSERT_VALUES_EQUAL(
            graph.GetStageTasksCount(stageA, 0),
            graph.GetStageTasksCount(stageB, 0));
        // Группа 2: C и D одинаковые
        UNIT_ASSERT_VALUES_EQUAL(
            graph.GetStageTasksCount(stageC, 0),
            graph.GetStageTasksCount(stageD, 0));
    }

    Y_UNIT_TEST(ExactFitAfterShrink) {
        // A -> B, 1 узел
        // Каналы = 2*a*b. Лимит = 50.
        // Максимальное a*b при a=b: a=b=5 даёт 2*25=50. Ровно.
        // Начинаем с a=b=10 (каналы=200), шринк должен найти alpha ~= 0.5
        auto graph = InitGraph(50, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 10);
        graph.AddTasks(stageB, 0, 10);

        graph.Shrink();

        auto a = graph.GetStageTasksCount(stageA, 0);
        auto b = graph.GetStageTasksCount(stageB, 0);

        UNIT_ASSERT_LE(2 * a * b, 50u);
        // Бинарный поиск должен найти оптимум близкий к a=b=5
        UNIT_ASSERT_GE(a, 4); // допускаем погрешность округления
        UNIT_ASSERT_GE(b, 4);
    }

    Y_UNIT_TEST(AllFixedNoShrink) {
        auto graph = InitGraph(10, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::FIXED, {});
        graph.AddStage(stageB, TMaxTasksGraph::FIXED, {stageA});

        graph.AddTasks(stageA, 0, 50);
        graph.AddTasks(stageB, 0, 50);

        // Каналы = 2*50*50 = 5000 >> 10, но обе FIXED => alpha=0 даёт те же задачи
        // IsFeasible(0) тоже false => shrink сдаётся, ничего не меняет
        graph.Shrink();

        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageA, 0), 50);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageB, 0), 50);
    }

    Y_UNIT_TEST(RoundRobinThenShrink) {
        auto graph = InitGraph(30, 3);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 12); // round-robin: 4,4,4
        graph.AddTasks(stageB, 12); // round-robin: 4,4,4

        // Каналы per node: 4*12 + 4*12 = 96 (total = 12 для обоих)
        // Лимит 30 => нужно шринкать

        graph.Shrink();

        auto totalA = graph.GetStageTasksCount(stageA);
        auto totalB = graph.GetStageTasksCount(stageB);

        UNIT_ASSERT_GE(totalA, 1);
        UNIT_ASSERT_GE(totalB, 1);
        UNIT_ASSERT_LE(totalA, 12);
        UNIT_ASSERT_LE(totalB, 12);

        // Распределение должно остаться равномерным (все узлы имели одинаковое количество)
        // Из-за округления допускаем разницу в 1
        for (ui32 n = 0; n < 3; ++n) {
            for (ui32 m = n + 1; m < 3; ++m) {
                auto diff = std::abs(
                    (long)graph.GetStageTasksCount(stageA, n) -
                    (long)graph.GetStageTasksCount(stageA, m));
                UNIT_ASSERT_LE(diff, 1);
            }
        }
    }

    Y_UNIT_TEST(FanOutShrinks) {
        // Тот же fan-out, но лимит меньше
        auto graph = InitGraph(200, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);
        auto stageC = MakeStageId(0, 2);
        auto stageD = MakeStageId(0, 3);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});
        graph.AddStage(stageC, TMaxTasksGraph::ANY, {stageA});
        graph.AddStage(stageD, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 10);
        graph.AddTasks(stageB, 0, 10);
        graph.AddTasks(stageC, 0, 10);
        graph.AddTasks(stageD, 0, 10);

        graph.Shrink();

        auto a = graph.GetStageTasksCount(stageA, 0);
        auto b = graph.GetStageTasksCount(stageB, 0);
        auto c = graph.GetStageTasksCount(stageC, 0);
        auto d = graph.GetStageTasksCount(stageD, 0);

        // Проверяем реальные каналы
        size_t channels = a * (b + c + d) + b * a + c * a + d * a;
        // = a*(b+c+d) + a*(b+c+d) = 2*a*(b+c+d)
        UNIT_ASSERT_LE(channels, 200u);

        // Все уменьшились
        UNIT_ASSERT_LE(a, 10);
        UNIT_ASSERT_GE(a, 1);
    }

    Y_UNIT_TEST(ParallelStagesNoChannels) {
        // A и B не связаны — 0 каналов между ними
        auto graph = InitGraph(0, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {});

        graph.AddTasks(stageA, 0, 100);
        graph.AddTasks(stageB, 0, 100);

        // Лимит 0, но каналов тоже 0 => feasible
        graph.Shrink();

        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageA, 0), 100);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageB, 0), 100);
    }

    Y_UNIT_TEST(ScalePreservesTotal) {
        // 4 узла, неравномерное распределение
        // После шринка сумма по узлам должна быть одинаковой для стадий в одной COPY-группе
        auto graph = InitGraph(100, 4);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 10);
        graph.AddTasks(stageA, 1, 5);
        graph.AddTasks(stageA, 2, 3);
        graph.AddTasks(stageA, 3, 2);
        graph.AddTasks(stageB, 0, 8);
        graph.AddTasks(stageB, 1, 6);
        graph.AddTasks(stageB, 2, 4);
        graph.AddTasks(stageB, 3, 2);

        graph.Shrink();

        auto totalA = graph.GetStageTasksCount(stageA);
        auto totalB = graph.GetStageTasksCount(stageB);

        // Все стадии в одной группе (нет COPY, обе — корневые), масштабируются одинаково
        // => totalA и totalB должны быть масштабированы с одним alpha
        // totalA_orig = 20, totalB_orig = 20, => totalA_new == totalB_new
        UNIT_ASSERT_VALUES_EQUAL(totalA, totalB);

        // Сумма per-node совпадает с total
        size_t sumA = 0, sumB = 0;
        for (ui32 n = 0; n < 4; ++n) {
            sumA += graph.GetStageTasksCount(stageA, n);
            sumB += graph.GetStageTasksCount(stageB, n);
        }
        UNIT_ASSERT_VALUES_EQUAL(sumA, totalA);
        UNIT_ASSERT_VALUES_EQUAL(sumB, totalB);
    }

    Y_UNIT_TEST(ZeroTasksOnSomeNodes) {
        // 3 узла, стадия A имеет задачи только на node0 и node1
        // При шринке node2 должен остаться с 0 задачами
        auto graph = InitGraph(50, 3);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 10);
        graph.AddTasks(stageA, 1, 10);
        // node2: 0 задач для stageA
        graph.AddTasks(stageB, 0, 10);
        graph.AddTasks(stageB, 1, 10);

        graph.Shrink();

        // node2 для stageA должен остаться 0
        UNIT_ASSERT_VALUES_EQUAL(graph.GetStageTasksCount(stageA, 2), 0);
    }

    Y_UNIT_TEST(SingleTaskStageRemains) {
        auto graph = InitGraph(50, 1);

        auto stageA = MakeStageId(0, 0);
        auto stageB = MakeStageId(0, 1);

        graph.AddStage(stageA, TMaxTasksGraph::ANY, {});
        graph.AddStage(stageB, TMaxTasksGraph::ANY, {stageA});

        graph.AddTasks(stageA, 0, 1);
        graph.AddTasks(stageB, 0, 100);

        graph.Shrink();

        // A имела 1 задачу — не может уменьшиться
        UNIT_ASSERT_GE(graph.GetStageTasksCount(stageA, 0), 1);
        UNIT_ASSERT_GE(graph.GetStageTasksCount(stageB, 0), 1);
    }
}
