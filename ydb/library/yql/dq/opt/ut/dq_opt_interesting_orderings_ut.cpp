#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/core/cbo/cbo_interesting_orderings.h>

// using namespace NYql;
// using namespace NNodes;
using namespace NYql::NDq;

Y_UNIT_TEST_SUITE(InterestingOrderingsShuffle) {
    TFunctionalDependency EquivFD(std::size_t l, std::size_t r) {
        return TFunctionalDependency({l}, r, TFunctionalDependency::EEquivalence);
    }

    template<typename... Args>
    TOrdering Shuffle(Args... args) {
        std::vector<std::size_t> ordering = {static_cast<std::size_t>(args)...};
        return TOrdering(std::move(ordering), TOrdering::EShuffle);
    }

    Y_UNIT_TEST(TwoOneItemEquivOnly) {
        std::vector<TFunctionalDependency> fds = {EquivFD(0, 1), EquivFD(1, 0) };
        std::vector<TOrdering> interesting = { Shuffle(0), Shuffle(1) };
        TOrderingsStateMachine fsm(fds, interesting);

        auto orderings = fsm.CreateState();
        orderings.SetOrdering(0);
        UNIT_ASSERT(orderings.ContainsShuffle(0));
        UNIT_ASSERT(!orderings.ContainsShuffle(1));
        orderings.InduceNewOrderings(fsm.GetFDSet(0));
        UNIT_ASSERT(orderings.ContainsShuffle(0));
        UNIT_ASSERT(orderings.ContainsShuffle(1));

        orderings.SetOrdering(1);
        UNIT_ASSERT(!orderings.ContainsShuffle(0));
        UNIT_ASSERT(orderings.ContainsShuffle(1));
        orderings.InduceNewOrderings(fsm.GetFDSet(1));
        UNIT_ASSERT(orderings.ContainsShuffle(0));
        UNIT_ASSERT(orderings.ContainsShuffle(1));
    }

    Y_UNIT_TEST(ManyOneItemEquivOnly) {
        std::vector<TFunctionalDependency> fds = {EquivFD(0, 1), EquivFD(2, 3), EquivFD(0, 3)};
        std::vector<TOrdering> interesting = { Shuffle(0), Shuffle(1), Shuffle(2), Shuffle(3) };
        TOrderingsStateMachine fsm(fds, interesting);

        auto orderings = fsm.CreateState();
        orderings.SetOrdering(3);
        UNIT_ASSERT(!orderings.ContainsShuffle(0));
        UNIT_ASSERT(!orderings.ContainsShuffle(1));
        UNIT_ASSERT(!orderings.ContainsShuffle(2));
        UNIT_ASSERT(orderings.ContainsShuffle(3));

        orderings.InduceNewOrderings(fsm.GetFDSet(1));
        UNIT_ASSERT(!orderings.ContainsShuffle(0));
        UNIT_ASSERT(!orderings.ContainsShuffle(1));
        UNIT_ASSERT(orderings.ContainsShuffle(2));
        UNIT_ASSERT(orderings.ContainsShuffle(3));
    }

    Y_UNIT_TEST(ConsideringOldFDs) {
        std::vector<TFunctionalDependency> fds = {EquivFD(0, 1), EquivFD(1, 2) };
        std::vector<TOrdering> interesting = { Shuffle(0), Shuffle(1), Shuffle(2) };

        TOrderingsStateMachine fsm(fds, interesting);

        auto orderings = fsm.CreateState();
        orderings.SetOrdering(0);
        UNIT_ASSERT(orderings.ContainsShuffle(0));
        UNIT_ASSERT(!orderings.ContainsShuffle(1));
        UNIT_ASSERT(!orderings.ContainsShuffle(2));
    
        orderings.InduceNewOrderings(fsm.GetFDSet(1));
        orderings.InduceNewOrderings(fsm.GetFDSet(0));
        UNIT_ASSERT(orderings.ContainsShuffle(0));
        UNIT_ASSERT(orderings.ContainsShuffle(1));
        UNIT_ASSERT(orderings.ContainsShuffle(2));
    }

    Y_UNIT_TEST(Join64ChainImitation) {
        std::vector<TOrdering> interesting = {Shuffle(0)};
        std::vector<TFunctionalDependency> fds;
        for (std::size_t i = 1; i < 64; ++i) {
            interesting.push_back(Shuffle(i));
            fds.push_back(EquivFD(i, i - 1));
        }

        TOrderingsStateMachine fsm(fds, interesting);   
        auto orderings = fsm.CreateState();
        orderings.SetOrdering(0);

        for (std::int64_t i = 62; i >= 0; --i) {
            orderings.InduceNewOrderings(fsm.GetFDSet(i));
        }

        for (std::int64_t i = 0; i < 64; ++i) {
            UNIT_ASSERT(orderings.ContainsShuffle(i));
        }
    }

    Y_UNIT_TEST(ManyItems) {
        std::vector<TFunctionalDependency> fds = { EquivFD(0, 1), EquivFD(0, 2) };
        std::vector<TOrdering> interesting = { Shuffle(0), Shuffle(0, 1), Shuffle(2, 3) };

        TOrderingsStateMachine fsm(fds, interesting);

        auto orderings = fsm.CreateState();
        orderings.SetOrdering(0);
        UNIT_ASSERT(orderings.ContainsShuffle(0));
        UNIT_ASSERT(orderings.ContainsShuffle(1));
        UNIT_ASSERT(!orderings.ContainsShuffle(2));       
        orderings.InduceNewOrderings(fsm.GetFDSet({0, 1}));
        UNIT_ASSERT(orderings.ContainsShuffle(0));
        UNIT_ASSERT(orderings.ContainsShuffle(1));
        UNIT_ASSERT(orderings.ContainsShuffle(2));
    }

    Y_UNIT_TEST(PruningFDs) {
        std::vector<TFunctionalDependency> fds = {EquivFD(2, 4), EquivFD(2, 3), EquivFD(13, 37), EquivFD(0, 1) };
        std::vector<TOrdering> interesting = { Shuffle(0), Shuffle(1) };

        TOrderingsStateMachine fsm(fds, interesting);

        auto orderings = fsm.CreateState();
        orderings.SetOrdering(0);
        orderings.InduceNewOrderings(fsm.GetFDSet(3));
        UNIT_ASSERT(orderings.ContainsShuffle(0));
        UNIT_ASSERT(orderings.ContainsShuffle(1));
    }

}
