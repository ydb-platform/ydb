#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/core/cbo/cbo_interesting_orderings.h>
#include <yql/essentials/utils/log/log.h>

// using namespace NYql;
// using namespace NNodes;
using namespace NYql::NDq;


template<typename... Args>
TOrdering Shuffling(Args... args) {
    std::vector<std::size_t> ordering = {static_cast<std::size_t>(args)...};
    return TOrdering(std::move(ordering), {}, TOrdering::EShuffle);
}

constexpr auto Asc = TOrdering::TItem::EDirection::EAscending;
constexpr auto Desc = TOrdering::TItem::EDirection::EDescending;

#define DESC(x) OrderingItem(x, Desc)
#define ASC(x) OrderingItem(x, Asc)

struct OrderingItem {
    std::size_t index;
    TOrdering::TItem::EDirection direction;

    OrderingItem(
        std::size_t idx,
        TOrdering::TItem::EDirection dir = TOrdering::TItem::EDirection::EAscending
    )
        : index(idx)
        , direction(dir)
    {}

    OrderingItem(int idx)
        : index(static_cast<std::size_t>(idx))
        , direction(TOrdering::TItem::EDirection::EAscending)
    {}
};

template<typename... Args>
TOrdering Sorting(const Args&... args) {
    std::vector<std::size_t> ordering;
    std::vector<TOrdering::TItem::EDirection> directions;

    auto processArg = [&](const auto& arg) {
        using ArgType = std::decay_t<decltype(arg)>;

        if constexpr (std::is_integral_v<ArgType>) {
            ordering.push_back(static_cast<std::size_t>(arg));
            directions.push_back(Asc);
        }
        else if constexpr (std::is_same_v<ArgType, OrderingItem>) {
            ordering.push_back(arg.index);
            directions.push_back(arg.direction);
        }
        else {
            static_assert(false, "Unsupported argument type");
        }
    };

    (processArg(args), ...);

    return TOrdering(std::move(ordering), std::move(directions), TOrdering::ESorting);
}

TFunctionalDependency EquivFD(std::size_t l, std::size_t r) {
    return TFunctionalDependency({l}, r, TFunctionalDependency::EEquivalence);
}

TFunctionalDependency FD(std::size_t l, std::size_t r) {
    return TFunctionalDependency({l}, r, TFunctionalDependency::EImplication);
}

TFunctionalDependency Constant(std::size_t orderingIdx) {
    return TFunctionalDependency({}, orderingIdx, TFunctionalDependency::EImplication);
}

TOrderingsStateMachine MakeFSM(
    const std::vector<TFunctionalDependency>& fds,
    const std::vector<TOrdering>& interestingOrderings,
    TOrdering::EType machineType
) {
    TFDStorage fdStorage;
    fdStorage.FDs = fds;
    fdStorage.InterestingOrderings = interestingOrderings;

    auto start = TInstant::Now();
    TOrderingsStateMachine fsm(fdStorage, machineType);
    Cerr << "Time of fsm construction: " << TInstant::Now() - start << Endl;
    Cout << fsm.ToString() << Endl;
    return fsm;
}


Y_UNIT_TEST_SUITE(InterestingOrderingsShuffle) {
    Y_UNIT_TEST(TwoOneItemEquivOnly) {
        std::vector<TFunctionalDependency> fds = {EquivFD(0, 1), EquivFD(1, 0) };
        std::vector<TOrdering> interesting = { Shuffling(0), Shuffling(1) };
        auto fsm = MakeFSM(fds, interesting, TOrdering::EShuffle);

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
        std::vector<TOrdering> interesting = { Shuffling(0), Shuffling(1), Shuffling(2), Shuffling(3) };
        auto fsm = MakeFSM(fds, interesting, TOrdering::EShuffle);

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
        std::vector<TOrdering> interesting = { Shuffling(0), Shuffling(1), Shuffling(2) };
        auto fsm = MakeFSM(fds, interesting, TOrdering::EShuffle);

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
        std::vector<TOrdering> interesting = {Shuffling(0)};
        std::vector<TFunctionalDependency> fds;
        for (std::size_t i = 1; i < 64; ++i) {
            interesting.push_back(Shuffling(i));
            fds.push_back(EquivFD(i, i - 1));
        }

        auto fsm = MakeFSM(fds, interesting, TOrdering::EShuffle);
        auto orderings = fsm.CreateState();
        orderings.SetOrdering(0);

        for (std::int64_t i = 62; i >= 0; --i) {
            orderings.InduceNewOrderings(fsm.GetFDSet(i));
        }

        std::size_t containsShuffle = 0;
        for (std::int64_t i = 0; i < 64; ++i) {
            containsShuffle += orderings.ContainsShuffle(i);
        }
        Cerr << "ContainsShuffle count: " << containsShuffle << Endl;
    }

    Y_UNIT_TEST(ManyItems) {
        std::vector<TFunctionalDependency> fds = { EquivFD(0, 1), EquivFD(0, 2) };
        std::vector<TOrdering> interesting = { Shuffling(0), Shuffling(0, 1), Shuffling(2, 3) };

        auto fsm = MakeFSM(fds, interesting, TOrdering::EShuffle);

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
        std::vector<TOrdering> interesting = { Shuffling(0), Shuffling(1) };

        auto fsm = MakeFSM(fds, interesting, TOrdering::EShuffle);

        auto orderings = fsm.CreateState();
        orderings.SetOrdering(0);
        orderings.InduceNewOrderings(fsm.GetFDSet(3));
        UNIT_ASSERT(orderings.ContainsShuffle(0));
        UNIT_ASSERT(orderings.ContainsShuffle(1));
    }

}

Y_UNIT_TEST_SUITE(InterestingOrderingsSorting) {
    Y_UNIT_TEST(PrefixClosure) { /* checks, that (0, 1) implies (0) */
        auto fsm = MakeFSM({}, { Sorting(0), Sorting(0, 1) }, TOrdering::ESorting);

        auto orderings = fsm.CreateState();
        orderings.SetOrdering(0);
        UNIT_ASSERT(orderings.ContainsSorting(0));
        UNIT_ASSERT(!orderings.ContainsSorting(1));

        orderings.SetOrdering(1);
        UNIT_ASSERT(orderings.ContainsSorting(0));
        UNIT_ASSERT(orderings.ContainsSorting(1));
    }


    Y_UNIT_TEST(SimpleImplicationFD) {
        std::vector<TFunctionalDependency> fds = {FD(0, 1)};
        std::vector<TOrdering> interesting = { Sorting(0), Sorting(0, 1), Sorting(2) };
        auto fsm = MakeFSM(fds, interesting, TOrdering::ESorting);

        {
            auto orderings = fsm.CreateState();
            orderings.SetOrdering(0);
            orderings.InduceNewOrderings(fsm.GetFDSet(0));
            UNIT_ASSERT(orderings.ContainsSorting(0));
            UNIT_ASSERT(orderings.ContainsSorting(1));
            UNIT_ASSERT(!orderings.ContainsSorting(2));
        }

        {
            auto orderings = fsm.CreateState();
            orderings.SetOrdering(1);
            orderings.InduceNewOrderings(fsm.GetFDSet(0));
            UNIT_ASSERT(orderings.ContainsSorting(0));
            UNIT_ASSERT(orderings.ContainsSorting(1));
            UNIT_ASSERT(!orderings.ContainsSorting(2));
        }
    }

    Y_UNIT_TEST(EquivWithImplicationFDs) {
        std::vector<TFunctionalDependency> fds = {EquivFD(0, 1), FD(0, 2)};
        std::vector<TOrdering> interesting = {Sorting(0), Sorting(0, 1, 2), Sorting(1, 0, 2)};
        auto fsm = MakeFSM(fds, interesting, TOrdering::ESorting);

        {
            auto orderings = fsm.CreateState();
            orderings.SetOrdering(0);
            orderings.InduceNewOrderings(fsm.GetFDSet({0, 1}));
            UNIT_ASSERT(orderings.ContainsSorting(0));
            UNIT_ASSERT(orderings.ContainsSorting(1));
            UNIT_ASSERT(orderings.ContainsSorting(2));
        }
    }

    Y_UNIT_TEST(EquivReplaceElements) {
        std::vector<TFunctionalDependency> fds = {EquivFD(0, 1)};
        std::vector<TOrdering> interesting = {Sorting(1, 0), Sorting(0, 1)};

        auto fsm = MakeFSM(fds, interesting, TOrdering::ESorting);

        {
            auto orderings = fsm.CreateState();
            orderings.SetOrdering(0);
            orderings.InduceNewOrderings(fsm.GetFDSet(0));
            UNIT_ASSERT(orderings.ContainsSorting(0));
            UNIT_ASSERT(orderings.ContainsSorting(1));
        }

        {
            auto orderings = fsm.CreateState();
            orderings.SetOrdering(1);
            orderings.InduceNewOrderings(fsm.GetFDSet(0));
            UNIT_ASSERT(orderings.ContainsSorting(0));
            UNIT_ASSERT(orderings.ContainsSorting(1));
        }
    }

    Y_UNIT_TEST(ComplexPrefixClosure) {
        std::vector<TFunctionalDependency> fds = {
            EquivFD(0, 1),
            FD(0, 2),
            FD(1, 5)
        };

        std::vector<TOrdering> interesting = {
            Sorting(0),
            Sorting(1),
            Sorting(0, 1, 2),
            Sorting(1, 0, 2),
            Sorting(1, 0, 3, 2, 6)
        };

        auto fsm = MakeFSM(fds, interesting, TOrdering::ESorting);

        {
            auto orderings = fsm.CreateState();
            orderings.SetOrdering(4);
            orderings.InduceNewOrderings(fsm.GetFDSet({0, 1}));
            UNIT_ASSERT(orderings.ContainsSorting(0));
            UNIT_ASSERT(orderings.ContainsSorting(1));
            UNIT_ASSERT(orderings.ContainsSorting(2));
            UNIT_ASSERT(orderings.ContainsSorting(3));
            UNIT_ASSERT(orderings.ContainsSorting(4));
        }
    }

    Y_UNIT_TEST(ConstantFD) {
        const std::size_t a = 0;
        const std::size_t b = 1;
        const std::size_t c = 2;
        const std::size_t d = 3;

        std::vector<TOrdering> interesting = {
            Sorting(c, a, b),
            Sorting(d),
            Sorting(a, b, c),
            Sorting(a, d, b)
        };
        std::vector<TFunctionalDependency> fds = {
            Constant(a),
            Constant(b),
            EquivFD(c, d)
        };
        auto fsm = MakeFSM(fds, interesting, TOrdering::ESorting);

        {
            auto orderings = fsm.CreateState();
            orderings.SetOrdering(1);
            orderings.InduceNewOrderings(fsm.GetFDSet({0, 1, 2}));
            UNIT_ASSERT(orderings.ContainsSorting(0));
            UNIT_ASSERT(orderings.ContainsSorting(1));
            UNIT_ASSERT(orderings.ContainsSorting(2));
            UNIT_ASSERT(orderings.ContainsSorting(3));
        }
    }

    Y_UNIT_TEST(ComplexOrderingsWithMultipleImplicationsAndEquivalences) {
        std::vector<TFunctionalDependency> fds = {
            EquivFD(0, 3),
            EquivFD(1, 4),
            FD(0, 2),
            FD(1, 5),
            FD(0, 6),
            FD(3, 7)
        };

        std::vector<TOrdering> interesting = {
            Sorting(0),
            Sorting(1),
            Sorting(0, 1, 2),
            Sorting(1, 0, 5),
            Sorting(3, 4, 7),
            Sorting(0, 1, 6),
            Sorting(3, 1, 2, 5)
        };

        auto fsm = MakeFSM(fds, interesting, TOrdering::ESorting);
    }

    Y_UNIT_TEST(TPCH8) {
        const size_t o_year = 0;
        const size_t o_partkey = 1;
        const size_t p_partkey = 2;
        const size_t p_type = 3;
        const size_t l_partkey = 4;
        const size_t l_suppkey = 5;
        const size_t l_orderkey = 6;
        const size_t o_orderkey = 7;
        const size_t o_custkey = 8;
        const size_t c_custkey = 9;
        const size_t c_nationkey = 10;
        const size_t n1_nationkey = 11;
        const size_t n2_nationkey = 12;
        const size_t n_regionkey = 13;
        const size_t r_name = 14;
        const size_t r_regionkey = 15;
        const size_t s_suppkey = 16;
        const size_t s_nationkey = 17;

        std::vector<TFunctionalDependency> fds = {
            EquivFD(p_partkey, l_partkey),
            Constant(p_type),
            EquivFD(o_custkey, c_custkey),
            Constant(r_name),
            EquivFD(c_nationkey, n1_nationkey),
            EquivFD(s_nationkey, n2_nationkey),
            EquivFD(l_orderkey, o_orderkey),
            EquivFD(s_suppkey, l_suppkey),
            EquivFD(n1_nationkey, r_regionkey)
        };

        std::vector<TOrdering> interesting = {
            Sorting(o_year),
            Sorting(o_partkey),
            Sorting(p_partkey),
            Sorting(l_partkey),
            Sorting(l_suppkey),
            Sorting(l_orderkey),
            Sorting(o_orderkey),
            Sorting(o_custkey),
            Sorting(c_custkey),
            Sorting(c_nationkey),
            Sorting(n1_nationkey),
            Sorting(n2_nationkey),
            Sorting(n_regionkey),
            Sorting(r_regionkey),
            Sorting(s_suppkey),
            Sorting(s_nationkey)
        };

        auto fsm = MakeFSM(fds, interesting, TOrdering::ESorting);
    }

    Y_UNIT_TEST(DifferentDirections) {
        std::vector<TFunctionalDependency> fds = {
            EquivFD(0, 1)
        };

        std::vector<TOrdering> interesting = {
            Sorting(0),
            Sorting(0, DESC(1)),
            Sorting(0, 1)
        };

        auto fsm = MakeFSM(fds, interesting, TOrdering::ESorting);

        {
            auto orderings = fsm.CreateState();
            orderings.SetOrdering(0);

            UNIT_ASSERT(orderings.ContainsSorting(0));
            UNIT_ASSERT(!orderings.ContainsSorting(1));
            UNIT_ASSERT(!orderings.ContainsSorting(2));

            orderings.InduceNewOrderings(fsm.GetFDSet(0));

            UNIT_ASSERT(orderings.ContainsSorting(0));
            UNIT_ASSERT(orderings.ContainsSorting(1));
            UNIT_ASSERT(orderings.ContainsSorting(2));
        }

        {
            auto orderings = fsm.CreateState();
            orderings.SetOrdering(1);

            UNIT_ASSERT(orderings.ContainsSorting(0));
            UNIT_ASSERT(orderings.ContainsSorting(1));
            UNIT_ASSERT(!orderings.ContainsSorting(2));

            orderings.InduceNewOrderings(fsm.GetFDSet(0));

            UNIT_ASSERT(orderings.ContainsSorting(0));
            UNIT_ASSERT(orderings.ContainsSorting(1));
            UNIT_ASSERT(orderings.ContainsSorting(2));
        }

        {
            auto orderings = fsm.CreateState();
            orderings.SetOrdering(2);

            UNIT_ASSERT(orderings.ContainsSorting(0));
            UNIT_ASSERT(!orderings.ContainsSorting(1));
            UNIT_ASSERT(orderings.ContainsSorting(2));

            orderings.InduceNewOrderings(fsm.GetFDSet(0));

            UNIT_ASSERT(orderings.ContainsSorting(0));
            UNIT_ASSERT(orderings.ContainsSorting(1));
            UNIT_ASSERT(orderings.ContainsSorting(2));
        }
    }

    Y_UNIT_TEST(DifferentDirectionsEquivWithImplication) {
        std::vector<TFunctionalDependency> fds = {
            EquivFD(1, 2),
            FD(0, 1)
        };

        std::vector<TOrdering> interesting = {
            Sorting(0),
            Sorting(0, DESC(2))
        };

        auto fsm = MakeFSM(fds, interesting, TOrdering::ESorting);
    }

    Y_UNIT_TEST(IncompatibleDirsSortingsPrefixClosure) {
        std::vector<TOrdering> interesting = {
            Sorting(ASC(1)),
            Sorting(DESC(1), DESC(0))
        };

        auto fsm = MakeFSM({}, interesting, TOrdering::ESorting);

        {
            auto orderings = fsm.CreateState();
            orderings.SetOrdering(0);

            UNIT_ASSERT(orderings.ContainsSorting(0));
            UNIT_ASSERT(!orderings.ContainsSorting(1));
        }


        {
            auto orderings = fsm.CreateState();
            orderings.SetOrdering(1);

            UNIT_ASSERT(!orderings.ContainsSorting(0));
            UNIT_ASSERT(orderings.ContainsSorting(1));
        }
    }
}
