#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/yql/utils/sort.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLHeapTest) {
    Y_UNIT_TEST_LLVM(TestMakeHeap) {
        const std::array<float, 10U> xxx = {{0.f, 13.f, -3.14f, 1212.f, -7898.8f, 21E4f, HUGE_VALF, -HUGE_VALF, 3673.f, -32764.f}};

        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        std::array<TRuntimeNode, 10U> data;
        std::transform(xxx.cbegin(), xxx.cend(), data.begin(), [&pb](float f) { return pb.NewDataLiteral(f); } );

        const auto type = pb.NewDataType(NUdf::TDataType<float>::Id);
        const auto list = pb.NewList(type, data);

        const auto pgmReturn = pb.MakeHeap(list,
            [&](TRuntimeNode l, TRuntimeNode r) {
                return pb.AggrLess(pb.Abs(l), pb.Abs(r));
            });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto& result = graph->GetValue();

        UNIT_ASSERT_VALUES_EQUAL(result.GetListLength(), xxx.size());

        auto copy = xxx;
        std::make_heap(copy.begin(), copy.end(), [](float l, float r){ return std::abs(l) < std::abs(r); });

        for (auto i = 0U; i < copy.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(copy[i], result.GetElement(i).template Get<float>());
        }
    }

    Y_UNIT_TEST_LLVM(TestPopHeap) {
        const std::array<double, 10U> xxx = {{0.0, 13.0, -3.140, 1212.0, -7898.8, 210000.0, 17E13, -HUGE_VAL, 3673.0, -32764.0}};

        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        std::array<TRuntimeNode, 10U> data;
        std::transform(xxx.cbegin(), xxx.cend(), data.begin(), [&pb](double f) { return pb.NewDataLiteral(f); } );

        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto list = pb.NewList(type, data);

        const auto comparer = [&](TRuntimeNode l, TRuntimeNode r) {
            return pb.AggrGreater(pb.Abs(l), pb.Abs(r));
        };

        const auto pgmReturn = pb.PopHeap(pb.MakeHeap(list,comparer), comparer);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto& result = graph->GetValue();

        UNIT_ASSERT_VALUES_EQUAL(result.GetListLength(), xxx.size());

        auto copy = xxx;
        const auto c = [](double l, double r){ return std::abs(l) > std::abs(r); };
        std::make_heap(copy.begin(), copy.end(), c);
        std::pop_heap(copy.begin(), copy.end(), c);

        for (auto i = 0U; i < copy.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(copy[i], result.GetElement(i).template Get<double>());
        }
    }

    Y_UNIT_TEST_LLVM(TestSortHeap) {
        const std::array<float, 10U> xxx = {{9E9f, -HUGE_VALF, 0.003f, 137.4f, -3.1415f, 1212.f, -7898.8f, 21E4f, 3673.f, -32764.f}};

        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        std::array<TRuntimeNode, 10U> data;
        std::transform(xxx.cbegin(), xxx.cend(), data.begin(), [&pb](float f) { return pb.NewDataLiteral(f); } );

        const auto type = pb.NewDataType(NUdf::TDataType<float>::Id);
        const auto list = pb.NewList(type, data);

        const auto pgmReturn = pb.SortHeap(
            pb.MakeHeap(list,
                [&](TRuntimeNode l, TRuntimeNode r) {
                    return pb.AggrGreater(l, r);
                }),
            [&](TRuntimeNode l, TRuntimeNode r) {
                return pb.AggrGreater(l, r);
            });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto& result = graph->GetValue();

        UNIT_ASSERT_VALUES_EQUAL(result.GetListLength(), xxx.size());

        auto copy = xxx;
        std::make_heap(copy.begin(), copy.end(), std::greater<float>());
        std::sort_heap(copy.begin(), copy.end(), std::greater<float>());

        for (auto i = 0U; i < copy.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(copy[i], result.GetElement(i).template Get<float>());
        }
    }

    Y_UNIT_TEST_LLVM(TestStableSort) {
        const std::array<double, 10U> xxx = {{9E9f, -HUGE_VALF, 0.003f, HUGE_VALF, +3.1415f, -0.003f, -7898.8f, -3.1415f, 3673.f, 0.003f}};

        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        std::array<TRuntimeNode, 10U> data;
        std::transform(xxx.cbegin(), xxx.cend(), data.begin(), [&pb](double f) { return pb.NewDataLiteral(f); } );

        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto list = pb.NewList(type, data);

        const auto pgmReturn = pb.StableSort(list,
            [&](TRuntimeNode l, TRuntimeNode r) {
                return pb.AggrGreater(pb.Abs(l), pb.Abs(r));
            });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto& result = graph->GetValue();

        UNIT_ASSERT_VALUES_EQUAL(result.GetListLength(), xxx.size());

        auto copy = xxx;
        std::stable_sort(copy.begin(), copy.end(), [](double l, double r){ return std::abs(l) > std::abs(r); });

        for (auto i = 0U; i < copy.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(copy[i], result.GetElement(i).template Get<double>());
        }
    }

    Y_UNIT_TEST_LLVM(TestNthElement) {
        const std::array<float, 10U> xxx = {{0.f, 13.f, -3.14f, 1212.f, -7898.8f, 21E4f, HUGE_VALF, -HUGE_VALF, 3673.f, -32764.f}};

        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        std::array<TRuntimeNode, 10U> data;
        std::transform(xxx.cbegin(), xxx.cend(), data.begin(), [&pb](float f) { return pb.NewDataLiteral(f); } );

        const auto type = pb.NewDataType(NUdf::TDataType<float>::Id);
        const auto list = pb.NewList(type, data);
        const auto n = pb.NewDataLiteral<ui64>(4U);

        const auto pgmReturn = pb.NthElement(list, n,
            [&](TRuntimeNode l, TRuntimeNode r) {
                return pb.AggrGreater(pb.Abs(l), pb.Abs(r));
            });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto& result = graph->GetValue();

        UNIT_ASSERT_VALUES_EQUAL(result.GetListLength(), xxx.size());

        auto copy = xxx;
        NYql::FastNthElement(copy.begin(), copy.begin() + 4U, copy.end(), [](float l, float r){ return std::abs(l) > std::abs(r); });

        for (auto i = 0U; i < copy.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(copy[i], result.GetElement(i).template Get<float>());
        }
    }

    Y_UNIT_TEST_LLVM(TestPartialSort) {
        const std::array<double, 10U> xxx = {{0.0, 13.0, -3.14, 1212.0, -7898.8, 21.0E4, HUGE_VAL, -HUGE_VAL, 3673.0, -32764.0}};

        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        std::array<TRuntimeNode, 10U> data;
        std::transform(xxx.cbegin(), xxx.cend(), data.begin(), [&pb](double f) { return pb.NewDataLiteral(f); } );

        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto list = pb.NewList(type, data);
        const auto n = pb.NewDataLiteral<ui64>(6U);

        const auto pgmReturn = pb.PartialSort(list, n,
            [&](TRuntimeNode l, TRuntimeNode r) {
                return pb.AggrLess(pb.Abs(l), pb.Abs(r));
            });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto& result = graph->GetValue();

        UNIT_ASSERT_VALUES_EQUAL(result.GetListLength(), xxx.size());

        auto copy = xxx;
        NYql::FastPartialSort(copy.begin(), copy.begin() + 6U, copy.end(), [](double l, double r){ return std::abs(l) < std::abs(r); });

        for (auto i = 0U; i < copy.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(copy[i], result.GetElement(i).template Get<double>());
        }
    }

    Y_UNIT_TEST_LLVM(TestTopN) {
        const std::array<double, 10U> xxx = {{0.0, 13.0, -3.140, -7898.8, 210000.0, 17E13, 1212.0, -HUGE_VAL, 3673.0, -32764.0}};

        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        std::array<TRuntimeNode, 10U> data;
        std::transform(xxx.cbegin(), xxx.cend(), data.begin(), [&pb](double f) { return pb.NewDataLiteral(f); } );

        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto list = pb.NewList(type, data);

        const auto comparator = [&](TRuntimeNode l, TRuntimeNode r) { return pb.AggrGreater(pb.Abs(l), pb.Abs(r)); };

        const auto n = 5ULL;

        const auto limit = pb.NewDataLiteral<ui64>(n);
        const auto last = pb.Decrement(limit);

        const auto pgmReturn = pb.Take(pb.NthElement(pb.Fold(list, pb.NewEmptyList(type),
            [&](TRuntimeNode item, TRuntimeNode state) {
                const auto size = pb.Length(state);

                return pb.If(pb.AggrLess(size, limit),
                    pb.If(pb.AggrLess(size, last),
                        pb.Append(state, item), pb.MakeHeap(pb.Append(state, item), comparator)),
                    pb.If(comparator(item, pb.Unwrap(pb.ToOptional(state), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0)),
                        pb.PushHeap(pb.Append(pb.Take(pb.PopHeap(state, comparator), pb.Decrement(size)), item), comparator),
                        state
                    )
                );
            }
        ), last, comparator), limit);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto& result = graph->GetValue();

        UNIT_ASSERT_VALUES_EQUAL(result.GetListLength(), n);

        auto copy = xxx;

        const auto comp = [](double l, double r){ return std::abs(l) > std::abs(r); };
        NYql::FastNthElement(copy.begin(), copy.begin() + n - 1U, copy.end(), comp);
        const auto mm = std::minmax_element(copy.begin(), copy.begin() + n, comp);

        double min = result.GetElement(0).template Get<double>(), max = min;
        for (auto i = 1U; i < n; ++i) {
            const auto v = result.GetElement(i).template Get<double>();
            min = std::min(min, v, comp);
            max = std::max(max, v, comp);
        }

        UNIT_ASSERT_VALUES_EQUAL(*mm.first, min);
        UNIT_ASSERT_VALUES_EQUAL(*mm.second, max);
    }

    Y_UNIT_TEST_LLVM(TestTopByNthElement) {
        const std::array<double, 10U> xxx = {{0.0, 13.0, -3.140, -7898.8, 210000.0, 17E13, 1212.0, -HUGE_VAL, 3673.0, -32764.0}};

        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        std::array<TRuntimeNode, 10U> data;
        std::transform(xxx.cbegin(), xxx.cend(), data.begin(), [&pb](double f) { return pb.NewDataLiteral(f); } );

        const auto type = pb.NewDataType(NUdf::TDataType<double>::Id);
        const auto list = pb.NewList(type, data);

        const auto comparator = [&](TRuntimeNode l, TRuntimeNode r) { return pb.AggrLess(pb.Abs(l), pb.Abs(r)); };

        const auto n = 5ULL;

        const auto limit = pb.NewDataLiteral<ui64>(n);
        const auto reserve = pb.ShiftLeft(limit, pb.NewDataLiteral<ui8>(1U));
        const auto last = pb.Decrement(limit);

        const auto pgmReturn = pb.Take(pb.NthElement(pb.Fold(list, pb.NewEmptyList(type),
            [&](TRuntimeNode item, TRuntimeNode state) {
                const auto size = pb.Length(state);

                return pb.If(pb.AggrLess(size, limit),
                    pb.If(pb.AggrLess(size, last),
                        pb.Append(state, item), pb.MakeHeap(pb.Append(state, item), comparator)),
                    pb.If(comparator(item, pb.Unwrap(pb.ToOptional(state), pb.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0)),
                        pb.If(pb.AggrLess(size, reserve),
                            pb.Append(state, item),
                            pb.Take(pb.NthElement(pb.Prepend(item, pb.Skip(state, pb.NewDataLiteral<ui64>(1U))), last, comparator), limit)
                        ),
                        state
                    )
                );
            }
        ), last, comparator), limit);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto& result = graph->GetValue();

        UNIT_ASSERT_VALUES_EQUAL(result.GetListLength(), n);

        auto copy = xxx;

        const auto comp = [](double l, double r){ return std::abs(l) < std::abs(r); };
        NYql::FastNthElement(copy.begin(), copy.begin() + n - 1U, copy.end(), comp);
        const auto mm = std::minmax_element(copy.begin(), copy.begin() + n, comp);

        double min = result.GetElement(0).template Get<double>(), max = min;
        for (auto i = 1U; i < n; ++i) {
            const auto v = result.GetElement(i).template Get<double>();
            min = std::min(min, v, comp);
            max = std::max(max, v, comp);
        }

        UNIT_ASSERT_VALUES_EQUAL(*mm.first, min);
        UNIT_ASSERT_VALUES_EQUAL(*mm.second, max);
    }
}
}
}
