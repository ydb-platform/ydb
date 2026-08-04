#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "scheduler_basic.h"

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

using namespace NActors;

Y_UNIT_TEST_SUITE(TSubSystemTest) {

    class TRootSubSystem : public ISubSystem {
    public:
        explicit TRootSubSystem(TVector<int>* lifecycle = nullptr)
            : Lifecycle(lifecycle)
        {
        }

        void OnAfterStart(TActorSystem&) override {
            if (Lifecycle) {
                Lifecycle->push_back(1);
            }
        }

        void OnBeforeStop(TActorSystem&) override {
            if (Lifecycle) {
                Lifecycle->push_back(-1);
            }
        }

    private:
        TVector<int>* Lifecycle;
    };

    class TMiddleSubSystem : public ISubSystem {
    public:
        explicit TMiddleSubSystem(TVector<int>* lifecycle = nullptr)
            : Lifecycle(lifecycle)
        {
        }

        TSubSystemDependencies GetDependencies() const override {
            return DependsOn<TRootSubSystem>();
        }

        void OnAfterStart(TActorSystem&) override {
            if (Lifecycle) {
                Lifecycle->push_back(2);
            }
        }

        void OnBeforeStop(TActorSystem&) override {
            if (Lifecycle) {
                Lifecycle->push_back(-2);
            }
        }

    private:
        TVector<int>* Lifecycle;
    };

    class TLeafSubSystem : public ISubSystem {
    public:
        explicit TLeafSubSystem(TVector<int>* lifecycle = nullptr)
            : Lifecycle(lifecycle)
        {
        }

        TSubSystemDependencies GetDependencies() const override {
            return DependsOn<TMiddleSubSystem>() && DependsOn<TRootSubSystem>();
        }

        void OnAfterStart(TActorSystem&) override {
            if (Lifecycle) {
                Lifecycle->push_back(3);
            }
        }

        void OnBeforeStop(TActorSystem&) override {
            if (Lifecycle) {
                Lifecycle->push_back(-3);
            }
        }

    private:
        TVector<int>* Lifecycle;
    };

    class TMissingSubSystem : public ISubSystem {
    };

    class TDependsOnMissingSubSystem : public ISubSystem {
    public:
        TSubSystemDependencies GetDependencies() const override {
            return DependsOn<TMissingSubSystem>();
        }
    };

    class TDependsTransitivelySubSystem : public ISubSystem {
    public:
        TSubSystemDependencies GetDependencies() const override {
            return DependsOn<TDependsOnMissingSubSystem>();
        }
    };

    class TAlternativeSubSystem : public ISubSystem {
    public:
        TSubSystemDependencies GetDependencies() const override {
            return DependsOn<TMissingSubSystem>() || DependsOn<TRootSubSystem>();
        }
    };

    class TCompositeSubSystem : public ISubSystem {
    public:
        explicit TCompositeSubSystem(TVector<TSubSystemTypeId>* resolved = nullptr)
            : Resolved(resolved)
        {
        }

        TSubSystemDependencies GetDependencies() const override {
            return DependsOn<TRootSubSystem>() &&
                (DependsOn<TMissingSubSystem>() || DependsOn<TMiddleSubSystem>());
        }

        void OnDependenciesResolved(const TResolvedSubSystemDependencies& dependencies) override {
            if (Resolved) {
                for (const auto& dependency : dependencies) {
                    Resolved->push_back(dependency.Type);
                }
            }
        }

    private:
        TVector<TSubSystemTypeId>* Resolved;
    };

    class TCycleBSubSystem;

    class TCycleASubSystem : public ISubSystem {
    public:
        TSubSystemDependencies GetDependencies() const override;
    };

    class TCycleBSubSystem : public ISubSystem {
    public:
        TSubSystemDependencies GetDependencies() const override {
            return DependsOn<TCycleASubSystem>();
        }
    };

    TSubSystemDependencies TCycleASubSystem::GetDependencies() const {
        return DependsOn<TCycleBSubSystem>();
    }

    class TAlternativeCycleBSubSystem;

    class TAlternativeCycleASubSystem : public ISubSystem {
    public:
        explicit TAlternativeCycleASubSystem(TVector<TSubSystemTypeId>* resolved = nullptr)
            : Resolved(resolved)
        {
        }

        TSubSystemDependencies GetDependencies() const override;

        void OnDependenciesResolved(const TResolvedSubSystemDependencies& dependencies) override {
            if (Resolved) {
                for (const auto& dependency : dependencies) {
                    Resolved->push_back(dependency.Type);
                }
            }
        }

    private:
        TVector<TSubSystemTypeId>* Resolved;
    };

    class TAlternativeCycleBSubSystem : public ISubSystem {
    public:
        TSubSystemDependencies GetDependencies() const override {
            return DependsOn<TAlternativeCycleASubSystem>();
        }
    };

    TSubSystemDependencies TAlternativeCycleASubSystem::GetDependencies() const {
        return DependsOn<TAlternativeCycleBSubSystem>() || DependsOn<TRootSubSystem>();
    }

    Y_UNIT_TEST(ResolvesSubSystemsInTopologicalOrder) {
        TSubSystems subSystems;
        RegisterSubSystem(subSystems, std::make_unique<TLeafSubSystem>());
        RegisterSubSystem(subSystems, std::make_unique<TMiddleSubSystem>());
        RegisterSubSystem(subSystems, std::make_unique<TRootSubSystem>());

        auto order = ResolveSubSystemDependencies(subSystems);
        UNIT_ASSERT(order);
        UNIT_ASSERT_VALUES_EQUAL(order->size(), 3);
        UNIT_ASSERT_VALUES_EQUAL((*order)[0], TSubSystemRegistry::TItem<TRootSubSystem>::Index());
        UNIT_ASSERT_VALUES_EQUAL((*order)[1], TSubSystemRegistry::TItem<TMiddleSubSystem>::Index());
        UNIT_ASSERT_VALUES_EQUAL((*order)[2], TSubSystemRegistry::TItem<TLeafSubSystem>::Index());
    }

    Y_UNIT_TEST(AllowsEmptySubSystemRegistry) {
        TSubSystems subSystems;

        auto order = ResolveSubSystemDependencies(subSystems);
        UNIT_ASSERT(order.has_value());
        UNIT_ASSERT(order->empty());
    }

    Y_UNIT_TEST(StoresDependencyExpressionsInDnf) {
        TSubSystemDependencies dependencies =
            DependsOn<TRootSubSystem>() &&
            (DependsOn<TMissingSubSystem>() || DependsOn<TMiddleSubSystem>());

        const auto root = TSubSystemRegistry::TItem<TRootSubSystem>::Index();
        const auto missing = TSubSystemRegistry::TItem<TMissingSubSystem>::Index();
        const auto middle = TSubSystemRegistry::TItem<TMiddleSubSystem>::Index();

        UNIT_ASSERT_VALUES_EQUAL(dependencies.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(dependencies[0].AllOf.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(dependencies[0].AllOf[0], root);
        UNIT_ASSERT_VALUES_EQUAL(dependencies[0].AllOf[1], missing);
        UNIT_ASSERT_VALUES_EQUAL(dependencies[1].AllOf.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(dependencies[1].AllOf[0], root);
        UNIT_ASSERT_VALUES_EQUAL(dependencies[1].AllOf[1], middle);
    }

    Y_UNIT_TEST(RemovesTransitivelyUnavailableSubSystems) {
        TSubSystems subSystems;
        TVector<TSubSystemTypeId> resolvedCompositeDependencies;
        RegisterSubSystem(subSystems, std::make_unique<TDependsTransitivelySubSystem>());
        RegisterSubSystem(subSystems, std::make_unique<TDependsOnMissingSubSystem>());
        RegisterSubSystem(subSystems, std::make_unique<TAlternativeSubSystem>());
        RegisterSubSystem(subSystems,
            std::make_unique<TCompositeSubSystem>(&resolvedCompositeDependencies));
        RegisterSubSystem(subSystems, std::make_unique<TMiddleSubSystem>());
        RegisterSubSystem(subSystems, std::make_unique<TRootSubSystem>());

        auto order = ResolveSubSystemDependencies(subSystems);
        UNIT_ASSERT(order);
        UNIT_ASSERT(!GetSubSystem<TDependsOnMissingSubSystem>(subSystems));
        UNIT_ASSERT(!GetSubSystem<TDependsTransitivelySubSystem>(subSystems));
        UNIT_ASSERT(GetSubSystem<TAlternativeSubSystem>(subSystems));
        UNIT_ASSERT(GetSubSystem<TCompositeSubSystem>(subSystems));
        UNIT_ASSERT(GetSubSystem<TMiddleSubSystem>(subSystems));
        UNIT_ASSERT(GetSubSystem<TRootSubSystem>(subSystems));

        const auto root = TSubSystemRegistry::TItem<TRootSubSystem>::Index();
        const auto alternative = TSubSystemRegistry::TItem<TAlternativeSubSystem>::Index();
        const auto rootPosition = std::find(order->begin(), order->end(), root);
        const auto alternativePosition = std::find(order->begin(), order->end(), alternative);
        UNIT_ASSERT(rootPosition != order->end());
        UNIT_ASSERT(alternativePosition != order->end());
        UNIT_ASSERT(rootPosition < alternativePosition);

        const auto middle = TSubSystemRegistry::TItem<TMiddleSubSystem>::Index();
        const auto composite = TSubSystemRegistry::TItem<TCompositeSubSystem>::Index();
        const auto middlePosition = std::find(order->begin(), order->end(), middle);
        const auto compositePosition = std::find(order->begin(), order->end(), composite);
        UNIT_ASSERT(middlePosition != order->end());
        UNIT_ASSERT(compositePosition != order->end());
        UNIT_ASSERT(rootPosition < compositePosition);
        UNIT_ASSERT(middlePosition < compositePosition);
        UNIT_ASSERT_VALUES_EQUAL(resolvedCompositeDependencies.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resolvedCompositeDependencies[0], root);
        UNIT_ASSERT_VALUES_EQUAL(resolvedCompositeDependencies[1], middle);
    }

    Y_UNIT_TEST(DetectsCyclicSubSystemDependencies) {
        TSubSystems subSystems;
        RegisterSubSystem(subSystems, std::make_unique<TCycleASubSystem>());
        RegisterSubSystem(subSystems, std::make_unique<TCycleBSubSystem>());

        UNIT_ASSERT(!ResolveSubSystemDependencies(subSystems));
    }

    Y_UNIT_TEST(SelectsAcyclicDependencyAlternative) {
        TSubSystems subSystems;
        TVector<TSubSystemTypeId> resolvedDependencies;
        RegisterSubSystem(subSystems,
            std::make_unique<TAlternativeCycleASubSystem>(&resolvedDependencies));
        RegisterSubSystem(subSystems, std::make_unique<TAlternativeCycleBSubSystem>());
        RegisterSubSystem(subSystems, std::make_unique<TRootSubSystem>());

        auto order = ResolveSubSystemDependencies(subSystems);
        UNIT_ASSERT(order);

        const auto root = TSubSystemRegistry::TItem<TRootSubSystem>::Index();
        const auto cycleA = TSubSystemRegistry::TItem<TAlternativeCycleASubSystem>::Index();
        const auto cycleB = TSubSystemRegistry::TItem<TAlternativeCycleBSubSystem>::Index();
        const auto rootPosition = std::find(order->begin(), order->end(), root);
        const auto cycleAPosition = std::find(order->begin(), order->end(), cycleA);
        const auto cycleBPosition = std::find(order->begin(), order->end(), cycleB);
        UNIT_ASSERT(rootPosition != order->end());
        UNIT_ASSERT(cycleAPosition != order->end());
        UNIT_ASSERT(cycleBPosition != order->end());
        UNIT_ASSERT(rootPosition < cycleAPosition);
        UNIT_ASSERT(cycleAPosition < cycleBPosition);
        UNIT_ASSERT_VALUES_EQUAL(resolvedDependencies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resolvedDependencies[0], root);
    }

    Y_UNIT_TEST(RunsSubSystemLifecycleInDependencyOrder) {
        TVector<int> lifecycle;

        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;
        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[setup->ExecutorsCount]);
        setup->Executors[0] = new TBasicExecutorPool(0, 1, 10, "system");
        setup->Scheduler = new TBasicSchedulerThread;
        setup->RegisterSubSystem(std::make_unique<TLeafSubSystem>(&lifecycle));
        setup->RegisterSubSystem(std::make_unique<TMiddleSubSystem>(&lifecycle));
        setup->RegisterSubSystem(std::make_unique<TRootSubSystem>(&lifecycle));

        TActorSystem actorSystem(setup);
        actorSystem.Start();
        UNIT_ASSERT_VALUES_EQUAL(lifecycle.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(lifecycle[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(lifecycle[1], 2);
        UNIT_ASSERT_VALUES_EQUAL(lifecycle[2], 3);

        actorSystem.Stop();
        UNIT_ASSERT_VALUES_EQUAL(lifecycle.size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(lifecycle[3], -3);
        UNIT_ASSERT_VALUES_EQUAL(lifecycle[4], -2);
        UNIT_ASSERT_VALUES_EQUAL(lifecycle[5], -1);
    }
}
