#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>
#include <yql/essentials/parser/pg_wrapper/interface/comp_factory.h>

namespace NKikimr::NMiniKQL {

#if MKQL_RUNTIME_VERSION >= 78U

namespace {

using TColumnsVec = TVector<std::pair<const ui32, const ui32>>;

// Builds a ListJoinCore that zips left.a with right.b for the given input rows.
// All=false: Zip  — returns a stream of (i32, i32), length = min(|left|, |right|).
// All=true:  ZipAll — returns a stream of (Maybe<i32>, Maybe<i32>), length = max(|left|, |right|).
using TDefaultInRow = NTest::TStructType<NTest::TStructMember<"left.a", TMaybe<i32>>,
                                         NTest::TStructMember<"right.b", TMaybe<i32>>,
                                         NTest::TStructMember<"key", TMaybe<i32>>,
                                         NTest::TStructMember<"_yql_table_index", i32>>;
template <bool Optional>
using TDefaultOutRow = std::conditional_t<Optional, std::tuple<TMaybe<i32>, TMaybe<i32>>, std::tuple<i32, i32>>;

std::tuple<TStructType*, TStructType*, TStructType*> BuildDefaultStructTypes(TProgramBuilder& pb, const TRuntimeNode& input, TType* payloadType) {
    const auto inputRowType = AS_TYPE(TStructType, AS_TYPE(TListType, input.GetStaticType())->GetItemType());
    const auto leftArgType = AS_TYPE(TStructType, pb.NewStructType({{"a", payloadType}}));
    const auto rightArgType = AS_TYPE(TStructType, pb.NewStructType({{"b", payloadType}}));
    return {inputRowType, leftArgType, rightArgType};
}

std::tuple<TColumnsVec, TColumnsVec, TColumnsVec> BuildDefaultColumnMaps(const TStructType* inputRowType, const TStructType* leftArgType, const TStructType* rightArgType) {
    const TColumnsVec keyColumns = {{inputRowType->GetMemberIndex("key"), 0U}};
    const TColumnsVec leftColumns = {{inputRowType->GetMemberIndex("left.a"), leftArgType->GetMemberIndex("a")}};
    const TColumnsVec rightColumns = {{inputRowType->GetMemberIndex("right.b"), rightArgType->GetMemberIndex("b")}};
    return {keyColumns, leftColumns, rightColumns};
}

template <bool All>
TRuntimeNode BuildZipJoin(TProgramBuilder& pb, TVector<TDefaultInRow>&& rows) {
    const auto list = ConvertValueToLiteralNode(pb, rows);

    const auto int32Type = pb.NewDataType(NUdf::TDataType<i32>::Id);
    const auto elemType = All ? pb.NewOptionalType(int32Type) : int32Type;
    const auto outputRowType = pb.NewTupleType({elemType, elemType});
    const auto [inputRowType, leftArgType, rightArgType] = BuildDefaultStructTypes(pb, list, int32Type);
    const auto [keyColumns, leftColumns, rightColumns] = BuildDefaultColumnMaps(inputRowType, leftArgType, rightArgType);
    const auto listJoinType = pb.NewStreamType(outputRowType);

    return pb.ListJoinCore(pb.Iterator(list, {}),
                           int32Type, keyColumns,
                           leftColumns, rightColumns,
                           leftArgType, [&pb](const TRuntimeNode leftArg) { return pb.Member(leftArg, "a"); },
                           rightArgType, [&pb](const TRuntimeNode rightArg) { return pb.Member(rightArg, "b"); },
                           listJoinType,
                           [&pb](const TRuntimeNode, const TRuntimeNode leftList, const TRuntimeNode rightList) {
                               TRuntimeNode root;
                               if constexpr (All) {
                                   root = pb.ZipAll({leftList, rightList});
                               } else {
                                   root = pb.Zip({leftList, rightList});
                               }
                               return pb.Iterator(root, {}); });
}

class TUnreachableWrapper: public TMutableComputationNode<TUnreachableWrapper> {
    using TBaseComputation = TMutableComputationNode<TUnreachableWrapper>;

public:
    explicit TUnreachableWrapper(TComputationMutables& mutables)
        : TBaseComputation(mutables)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&) const {
        THROW yexception() << "Unreachable";
    }

private:
    void RegisterDependencies() const final {
    }
};

IComputationNode* WrapUnreachable(TCallable&, const TComputationNodeFactoryContext& ctx) {
    return new TUnreachableWrapper(ctx.Mutables);
}

TComputationNodeFactory GetUnreachableFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "Unreachable") {
            return WrapUnreachable(callable, ctx);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

TRuntimeNode Unreachable(TProgramBuilder& pb) {
    TCallableBuilder callableBuilder(pb.GetTypeEnvironment(), "Unreachable", pb.NewVoidType());
    return TRuntimeNode(callableBuilder.Build(), false);
}

TComputationNodeFactory GetFactoryWithPg() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (auto* pgCallable = NYql::GetPgFactory()(callable, ctx)) {
            return pgCallable;
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLListJoinCoreTest) {

Y_UNIT_TEST_QUAD(EmptyLeftList, LLVM, All) {
    // Only right-side rows. Zip: empty output. ZipAll: left slots become Nothing.
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listJoin = BuildZipJoin<All>(pb, TVector<TDefaultInRow>{
                                                    {{{TMaybe<i32>{}}, {TMaybe<i32>{3}}, {TMaybe<i32>{9}}, {1}}},
                                                    {{{TMaybe<i32>{}}, {TMaybe<i32>{4}}, {TMaybe<i32>{9}}, {1}}},
                                                });

    const auto graph = setup.BuildGraph(listJoin);

    using TOutRow = TDefaultOutRow<All>;
    if constexpr (All) {
        const TVector<TOutRow> expected{
            {{TMaybe<i32>{}}, {TMaybe<i32>{3}}},
            {{TMaybe<i32>{}}, {TMaybe<i32>{4}}},
        };
        AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
    } else {
        const TVector<TOutRow> expected{};
        AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
    }
}

Y_UNIT_TEST_QUAD(EmptyRightList, LLVM, All) {
    // Only left-side rows. Zip: empty output. ZipAll: right slots become Nothing.
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listJoin = BuildZipJoin<All>(pb, TVector<TDefaultInRow>{
                                                    {{{TMaybe<i32>{1}}, {TMaybe<i32>{}}, {TMaybe<i32>{9}}, {0}}},
                                                    {{{TMaybe<i32>{2}}, {TMaybe<i32>{}}, {TMaybe<i32>{9}}, {0}}},
                                                });

    const auto graph = setup.BuildGraph(listJoin);

    using TOutRow = TDefaultOutRow<All>;
    if constexpr (All) {
        const TVector<TOutRow> expected{
            {{TMaybe<i32>{1}}, {TMaybe<i32>{}}},
            {{TMaybe<i32>{2}}, {TMaybe<i32>{}}},
        };
        AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
    } else {
        const TVector<TOutRow> expected{};
        AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
    }
}

Y_UNIT_TEST_QUAD(EmptyStream, LLVM, All) {
    // No rows at all. Both Zip and ZipAll of two empty lists produce no output.
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listJoin = BuildZipJoin<All>(pb, TVector<TDefaultInRow>{});

    const auto graph = setup.BuildGraph(listJoin);

    using TOutRow = TDefaultOutRow<All>;
    const TVector<TOutRow> expected{};
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
}

Y_UNIT_TEST_LLVM(InterleavedRows) {
    // Left and right rows appear alternating in the stream. Accumulation must be
    // order-independent: left=[1,2], right=[3,4] regardless of arrival order.
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listJoin = BuildZipJoin<false>(pb, TVector<TDefaultInRow>{
                                                      {{{TMaybe<i32>{1}}, {TMaybe<i32>{}}, {TMaybe<i32>{9}}, {0}}}, // left
                                                      {{{TMaybe<i32>{}}, {TMaybe<i32>{3}}, {TMaybe<i32>{9}}, {1}}}, // right
                                                      {{{TMaybe<i32>{2}}, {TMaybe<i32>{}}, {TMaybe<i32>{9}}, {0}}}, // left
                                                      {{{TMaybe<i32>{}}, {TMaybe<i32>{4}}, {TMaybe<i32>{9}}, {1}}}, // right
                                                  });

    const auto graph = setup.BuildGraph(listJoin);

    using TOutRow = TDefaultOutRow<false>;
    const TVector<TOutRow> expected{{1, 3}, {2, 4}};
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
}

Y_UNIT_TEST(InvalidTableIndex) {
    // A row with _yql_table_index=2 must throw during Fetch.
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listJoin = BuildZipJoin<false>(pb, TVector<TDefaultInRow>{
                                                      {{{TMaybe<i32>{1}}, {TMaybe<i32>{}}, {TMaybe<i32>{9}}, {2}}},
                                                  });

    const auto graph = setup.BuildGraph(listJoin);
    auto stream = graph->GetValue();
    NUdf::TUnboxedValue result;
    UNIT_ASSERT_EXCEPTION_CONTAINS(stream.Fetch(result), yexception, "Bad table index: 2");
}

Y_UNIT_TEST(InvalidColumnsMapping) {
    // Mismap inputRowType members types with leftArgType and rightArgType
    // member types to check EnrichNeedUnwrap sentinel branch.
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = ConvertValueToLiteralNode(pb, TVector<TDefaultInRow>{
                                                        {{{TMaybe<i32>{23}}, {TMaybe<i32>{}}, {9}, {0}}},
                                                    });

    const auto int32Type = pb.NewDataType(NUdf::TDataType<i32>::Id);
    const auto uint32Type = pb.NewDataType(NUdf::TDataType<ui32>::Id);
    const auto [inputRowType, leftArgType, rightArgType] = BuildDefaultStructTypes(pb, list, uint32Type);
    const auto [keyColumns, leftColumns, rightColumns] = BuildDefaultColumnMaps(inputRowType, leftArgType, rightArgType);
    const auto listJoinType = pb.NewStreamType(pb.NewTupleType({uint32Type, uint32Type}));

    const auto listJoin = pb.ListJoinCore(pb.Iterator(list, {}),
                                          int32Type, keyColumns,
                                          leftColumns, rightColumns,
                                          leftArgType, [&pb](const TRuntimeNode arg) { return pb.Member(arg, "a"); },
                                          rightArgType, [&pb](const TRuntimeNode arg) { return pb.Member(arg, "b"); },
                                          listJoinType,
                                          [&pb](const TRuntimeNode, const TRuntimeNode leftList, const TRuntimeNode rightList) {
                                              const auto root = pb.Zip({leftList, rightList});
                                              return pb.Iterator(root, {}); });

    UNIT_ASSERT_EXCEPTION_CONTAINS(setup.BuildGraph(listJoin), yexception, "Bad column mapping:");
}

Y_UNIT_TEST_LLVM(ColumnReordering) {
    // Both left and right output columns are mapped in reverse order relative to input.
    // Left:  input[a=9,  b=24] -> output {x=b=24, y=a=9} (a->y, b->x).
    // Right: input[c=42, d=73] -> output {p=d=73, q=c=42} (c->q, d->p).
    // Verifies MakeListItem uses itemsPtr[outColumn], not itemsPtr[inColumn].
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TInRow = NTest::TStructType<NTest::TStructMember<"left.a", TMaybe<i32>>,
                                      NTest::TStructMember<"left.b", TMaybe<i32>>,
                                      NTest::TStructMember<"right.c", TMaybe<i32>>,
                                      NTest::TStructMember<"right.d", TMaybe<i32>>,
                                      NTest::TStructMember<"key", i32>,
                                      NTest::TStructMember<"_yql_table_index", i32>>;
    const auto list = ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                        {{{TMaybe<i32>{9}}, {TMaybe<i32>{24}}, {TMaybe<i32>{}}, {TMaybe<i32>{}}, {0}, {0}}},  // left:  a=9,  b=24
                                                        {{{TMaybe<i32>{}}, {TMaybe<i32>{}}, {TMaybe<i32>{42}}, {TMaybe<i32>{73}}, {0}, {1}}}, // right: c=42, d=73
                                                    });

    const auto int32Type = pb.NewDataType(NUdf::TDataType<i32>::Id);
    // Left output struct: {x (idx=0), y (idx=1)}; a->y (outCol=1), b->x (outCol=0).
    const auto leftItemType = AS_TYPE(TStructType, pb.NewStructType({{"x", int32Type}, {"y", int32Type}}));
    // Right output struct: {p (idx=0), q (idx=1)}; c->q (outCol=1), d->p (outCol=0).
    const auto rightItemType = AS_TYPE(TStructType, pb.NewStructType({{"p", int32Type}, {"q", int32Type}}));
    const auto inputRowType = AS_TYPE(TStructType, AS_TYPE(TListType, list.GetStaticType())->GetItemType());

    const TColumnsVec keyColumns = {{inputRowType->GetMemberIndex("key"), 0U}};
    const TColumnsVec leftColumns = {
        {inputRowType->GetMemberIndex("left.a"), leftItemType->GetMemberIndex("y")},
        {inputRowType->GetMemberIndex("left.b"), leftItemType->GetMemberIndex("x")},
    };
    const TColumnsVec rightColumns = {
        {inputRowType->GetMemberIndex("right.c"), rightItemType->GetMemberIndex("q")},
        {inputRowType->GetMemberIndex("right.d"), rightItemType->GetMemberIndex("p")},
    };
    // Output: (left.x, left.y, right.p, right.q) = (24, 9, 73, 42).
    const auto listJoinType = pb.NewStreamType(pb.NewTupleType({int32Type, int32Type, int32Type, int32Type}));

    const auto listJoin = pb.ListJoinCore(pb.Iterator(list, {}),
                                          int32Type, keyColumns,
                                          leftColumns, rightColumns,
                                          leftItemType, [&pb](const TRuntimeNode arg) { return pb.NewTuple({pb.Member(arg, "x"), pb.Member(arg, "y")}); },
                                          rightItemType, [&pb](const TRuntimeNode arg) { return pb.NewTuple({pb.Member(arg, "p"), pb.Member(arg, "q")}); },
                                          listJoinType,
                                          [&pb](const TRuntimeNode, const TRuntimeNode leftList, const TRuntimeNode rightList) {
                                              const auto root = pb.Map(pb.Zip({leftList, rightList}), [&pb](const auto pair) {
                                                 const auto l = pb.Nth(pair, 0);
                                                 const auto r = pb.Nth(pair, 1);
                                                 return pb.NewTuple({pb.Nth(l, 0), pb.Nth(l, 1), pb.Nth(r, 0), pb.Nth(r, 1)});
                                              });
                                              return pb.Iterator(root , {}); });

    const auto graph = setup.BuildGraph(listJoin);

    using TOutRow = std::tuple<i32, i32, i32, i32>;
    const TVector<TOutRow> expected{{24, 9, 73, 42}};
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
}

Y_UNIT_TEST_LLVM(MultipleColumnsPerSide) {
    // Left side has columns {a, c}, right side has columns {a, c} (same field name, same type).
    // Verifies that all columns from both sides are correctly extracted and placed.
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TInRow = NTest::TStructType<NTest::TStructMember<"left.a", TMaybe<i32>>,
                                      NTest::TStructMember<"left.c", TMaybe<i32>>,
                                      NTest::TStructMember<"right.a", TMaybe<i32>>,
                                      NTest::TStructMember<"right.c", TMaybe<i32>>,
                                      NTest::TStructMember<"key", i32>,
                                      NTest::TStructMember<"_yql_table_index", i32>>;
    const auto list = ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                        {{{1}, {3}, {TMaybe<i32>{}}, {TMaybe<i32>{}}, {0}, {0}}}, // left:  a=1, c=3
                                                        {{{TMaybe<i32>{}}, {TMaybe<i32>{}}, {2}, {4}, {0}, {1}}}, // right: a=2, c=4
                                                    });

    const auto int32Type = pb.NewDataType(NUdf::TDataType<i32>::Id);
    const auto leftArgType = AS_TYPE(TStructType, pb.NewStructType({{"la", int32Type}, {"lc", int32Type}}));
    const auto rightArgType = AS_TYPE(TStructType, pb.NewStructType({{"ra", int32Type}, {"rc", int32Type}}));
    const auto inputRowType = AS_TYPE(TStructType, AS_TYPE(TListType, list.GetStaticType())->GetItemType());

    const TColumnsVec keyColumns = {{inputRowType->GetMemberIndex("key"), 0U}};
    const TColumnsVec leftColumns = {
        {inputRowType->GetMemberIndex("left.a"), leftArgType->GetMemberIndex("la")},
        {inputRowType->GetMemberIndex("left.c"), leftArgType->GetMemberIndex("lc")},
    };
    const TColumnsVec rightColumns = {
        {inputRowType->GetMemberIndex("right.a"), rightArgType->GetMemberIndex("ra")},
        {inputRowType->GetMemberIndex("right.c"), rightArgType->GetMemberIndex("rc")},
    };
    const auto listJoinType = pb.NewStreamType(pb.NewTupleType({int32Type, int32Type}));

    const auto listJoin = pb.ListJoinCore(pb.Iterator(list, {}),
                                          int32Type, keyColumns,
                                          leftColumns, rightColumns,
                                          leftArgType, [&pb](const TRuntimeNode arg) { return pb.NewTuple({pb.Member(arg, "la"), pb.Member(arg, "lc")}); },
                                          rightArgType, [&pb](const TRuntimeNode arg) { return pb.NewTuple({pb.Member(arg, "ra"), pb.Member(arg, "rc")}); },
                                          listJoinType,
                                          [&pb](const TRuntimeNode, const TRuntimeNode leftList, const TRuntimeNode rightList) {
                                              const auto root = pb.Extend({leftList, rightList});
                                              return pb.Iterator(root, {}); });

    const auto graph = setup.BuildGraph(listJoin);

    using TOutRow = TDefaultOutRow<false>;
    const TVector<TOutRow> expected{{1, 3}, {2, 4}};
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
}

Y_UNIT_TEST_LLVM(KeyUsedInLambda) {
    // The key value is read inside the lambda and emitted in every output row.
    // Verifies that KeyArg_ receives the correct value from InitializeKey.
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = ConvertValueToLiteralNode(pb, TVector<TDefaultInRow>{
                                                        {{{TMaybe<i32>{1}}, {TMaybe<i32>{}}, {9}, {0}}},
                                                        {{{TMaybe<i32>{2}}, {TMaybe<i32>{}}, {9}, {0}}},
                                                        {{{TMaybe<i32>{}}, {TMaybe<i32>{3}}, {9}, {1}}},
                                                        {{{TMaybe<i32>{}}, {TMaybe<i32>{4}}, {9}, {1}}},
                                                    });

    const auto int32Type = pb.NewDataType(NUdf::TDataType<i32>::Id);
    const auto [inputRowType, leftArgType, rightArgType] = BuildDefaultStructTypes(pb, list, int32Type);
    const auto [keyColumns, leftColumns, rightColumns] = BuildDefaultColumnMaps(inputRowType, leftArgType, rightArgType);
    // Output: (key, a, b).
    const auto listJoinType = pb.NewStreamType(pb.NewTupleType({int32Type, int32Type, int32Type}));

    const auto listJoin = pb.ListJoinCore(pb.Iterator(list, {}),
                                          int32Type, keyColumns,
                                          leftColumns, rightColumns,
                                          leftArgType, [&pb](const TRuntimeNode arg) { return pb.Member(arg, "a"); },
                                          rightArgType, [&pb](const TRuntimeNode arg) { return pb.Member(arg, "b"); },
                                          listJoinType,
                                          [&pb](const TRuntimeNode key, const TRuntimeNode leftList, const TRuntimeNode rightList) {
                                              const auto root = pb.Map(pb.Zip({leftList, rightList}), [&pb, key](const auto pair) {
                                                  return pb.NewTuple({key, pb.Nth(pair, 0), pb.Nth(pair, 1)});
                                              });
                                              return pb.Iterator(root, {}); });

    const auto graph = setup.BuildGraph(listJoin);

    using TOutRow = std::tuple<i32, i32, i32>;
    const TVector<TOutRow> expected{{9, 1, 3}, {9, 2, 4}};
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
}

Y_UNIT_TEST_LLVM(MultiColumnKey) {
    // Key is composed of two columns (key1, key2). Exercises the InitializeKey
    // branch that builds a tuple array instead of returning a scalar directly.
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TInRow = NTest::TStructType<NTest::TStructMember<"left.a", TMaybe<i32>>,
                                      NTest::TStructMember<"right.b", TMaybe<i32>>,
                                      NTest::TStructMember<"key1", i32>,
                                      NTest::TStructMember<"key2", i32>,
                                      NTest::TStructMember<"_yql_table_index", i32>>;
    const auto list = ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                        {{{TMaybe<i32>{1}}, {TMaybe<i32>{}}, {5}, {7}, {0}}},
                                                        {{{TMaybe<i32>{2}}, {TMaybe<i32>{}}, {5}, {7}, {0}}},
                                                        {{{TMaybe<i32>{}}, {TMaybe<i32>{3}}, {5}, {7}, {1}}},
                                                        {{{TMaybe<i32>{}}, {TMaybe<i32>{4}}, {5}, {7}, {1}}},
                                                    });

    const auto int32Type = pb.NewDataType(NUdf::TDataType<i32>::Id);
    const auto [inputRowType, leftArgType, rightArgType] = BuildDefaultStructTypes(pb, list, int32Type);
    // Two-column key maps to a tuple (key1, key2).
    const auto keyType = AS_TYPE(TStructType, pb.NewStructType({{"key1", int32Type}, {"key2", int32Type}}));
    const TColumnsVec keyColumns = {
        {inputRowType->GetMemberIndex("key1"), keyType->GetMemberIndex("key1")},
        {inputRowType->GetMemberIndex("key2"), keyType->GetMemberIndex("key2")},
    };
    const TColumnsVec leftColumns = {{inputRowType->GetMemberIndex("left.a"), leftArgType->GetMemberIndex("a")}};
    const TColumnsVec rightColumns = {{inputRowType->GetMemberIndex("right.b"), rightArgType->GetMemberIndex("b")}};
    // Output: (key1, key2, a, b).
    const auto listJoinType = pb.NewStreamType(pb.NewTupleType({int32Type, int32Type, int32Type, int32Type}));

    const auto listJoin = pb.ListJoinCore(pb.Iterator(list, {}),
                                          keyType, keyColumns,
                                          leftColumns, rightColumns,
                                          leftArgType, [&pb](const TRuntimeNode arg) { return pb.Member(arg, "a"); },
                                          rightArgType, [&pb](const TRuntimeNode arg) { return pb.Member(arg, "b"); },
                                          listJoinType,
                                          [&pb](const TRuntimeNode key, const TRuntimeNode leftList, const TRuntimeNode rightList) {
                                              const auto root = pb.Map(pb.Zip({leftList, rightList}), [&pb, key](const auto pair) {
                                                  return pb.NewTuple({pb.Member(key, "key1"), pb.Member(key, "key2"), pb.Nth(pair, 0), pb.Nth(pair, 1)});
                                              });
                                              return pb.Iterator(root, {}); });

    const auto graph = setup.BuildGraph(listJoin);

    using TOutRow = std::tuple<i32, i32, i32, i32>;
    const TVector<TOutRow> expected{{5, 7, 1, 3}, {5, 7, 2, 4}};
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
}

Y_UNIT_TEST_LLVM(CrossProductJoin) {
    // Lambda uses FlatMap to produce the full cross product of left x right rows.
    // 2 left rows x 3 right rows = 6 output rows.
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = ConvertValueToLiteralNode(pb, TVector<TDefaultInRow>{
                                                        {{{TMaybe<i32>{1}}, {TMaybe<i32>{}}, {0}, {0}}},
                                                        {{{TMaybe<i32>{2}}, {TMaybe<i32>{}}, {0}, {0}}},
                                                        {{{TMaybe<i32>{}}, {TMaybe<i32>{3}}, {0}, {1}}},
                                                        {{{TMaybe<i32>{}}, {TMaybe<i32>{4}}, {0}, {1}}},
                                                        {{{TMaybe<i32>{}}, {TMaybe<i32>{5}}, {0}, {1}}},
                                                    });

    const auto int32Type = pb.NewDataType(NUdf::TDataType<i32>::Id);
    const auto [inputRowType, leftArgType, rightArgType] = BuildDefaultStructTypes(pb, list, int32Type);
    const auto [keyColumns, leftColumns, rightColumns] = BuildDefaultColumnMaps(inputRowType, leftArgType, rightArgType);
    const auto listJoinType = pb.NewStreamType(pb.NewTupleType({int32Type, int32Type}));

    const auto listJoin = pb.ListJoinCore(pb.Iterator(list, {}),
                                          int32Type, keyColumns,
                                          leftColumns, rightColumns,
                                          leftArgType, [&pb](const TRuntimeNode arg) { return pb.Member(arg, "a"); },
                                          rightArgType, [&pb](const TRuntimeNode arg) { return pb.Member(arg, "b"); },
                                          listJoinType,
                                          [&pb](const TRuntimeNode, const TRuntimeNode leftList, const TRuntimeNode rightList) {
                                              const auto root = pb.FlatMap(leftList, [&pb, rightList](const TRuntimeNode a) {
                                                  return pb.Map(rightList, [&pb, a](const TRuntimeNode b) {
                                                      return pb.NewTuple({a, b});
                                                  });
                                              });
                                              return pb.Iterator(root, {}); });

    const auto graph = setup.BuildGraph(listJoin);

    using TOutRow = TDefaultOutRow<false>;
    const TVector<TOutRow> expected{{1, 3}, {1, 4}, {1, 5}, {2, 3}, {2, 4}, {2, 5}};
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
}

Y_UNIT_TEST_LLVM(ArgmapTransform) {
    // Argmap lambdas apply arithmetic transforms: left increments (a+1), right doubles (b*2).
    // Verifies MakeListItem stores the premap output, not the raw premap arg.
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = ConvertValueToLiteralNode(pb, TVector<TDefaultInRow>{
                                                        {{{42}, {TMaybe<i32>{}}, {9}, {0}}}, // left:  a=42 -> premap: 42+1=43
                                                        {{{TMaybe<i32>{}}, {73}, {9}, {1}}}, // right: a=73 -> premap: 73*2=146
                                                    });

    const auto int32Type = pb.NewDataType(NUdf::TDataType<i32>::Id);
    const auto [inputRowType, leftArgType, rightArgType] = BuildDefaultStructTypes(pb, list, int32Type);
    const auto [keyColumns, leftColumns, rightColumns] = BuildDefaultColumnMaps(inputRowType, leftArgType, rightArgType);
    const auto listJoinType = pb.NewStreamType(pb.NewTupleType({int32Type, int32Type}));

    const auto listJoin = pb.ListJoinCore(pb.Iterator(list, {}),
                                          int32Type, keyColumns,
                                          leftColumns, rightColumns,
                                          leftArgType, [&pb](const TRuntimeNode arg) { return pb.Increment(pb.Member(arg, "a")); },
                                          rightArgType, [&pb](const TRuntimeNode arg) { return pb.Mul(pb.Member(arg, "b"), pb.NewDataLiteral<i32>(2)); },
                                          listJoinType,
                                          [&pb](const TRuntimeNode, const TRuntimeNode leftList, const TRuntimeNode rightList) {
                                              const auto root = pb.Zip({leftList, rightList});
                                              return pb.Iterator(root, {}); });

    const auto graph = setup.BuildGraph(listJoin);

    using TOutRow = TDefaultOutRow<false>;
    const TVector<TOutRow> expected{{43, 146}};
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
}

Y_UNIT_TEST_LLVM(SpecialArgmapLambdas) {
    // Left uses identity premap: list items are the premap arg structs themselves.
    // Right uses constant premap: list items are always 42, ignoring the arg content.
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = ConvertValueToLiteralNode(pb, TVector<TDefaultInRow>{
                                                        {{{TMaybe<i32>{23}}, {TMaybe<i32>{}}, {9}, {0}}}, // left:  a=23
                                                        {{{TMaybe<i32>{24}}, {TMaybe<i32>{}}, {9}, {0}}}, // left:  a=24
                                                        {{{TMaybe<i32>{}}, {TMaybe<i32>{42}}, {9}, {1}}}, // right: b=42 (ignored by premap)
                                                        {{{TMaybe<i32>{}}, {TMaybe<i32>{42}}, {9}, {1}}}, // right: b=42 (ignored by premap)
                                                    });

    const auto int32Type = pb.NewDataType(NUdf::TDataType<i32>::Id);
    const auto [inputRowType, leftArgType, rightArgType] = BuildDefaultStructTypes(pb, list, int32Type);
    const auto [keyColumns, leftColumns, rightColumns] = BuildDefaultColumnMaps(inputRowType, leftArgType, rightArgType);
    const auto listJoinType = pb.NewStreamType(pb.NewTupleType({int32Type, int32Type}));

    const auto listJoin = pb.ListJoinCore(pb.Iterator(list, {}),
                                          int32Type, keyColumns,
                                          leftColumns, rightColumns,
                                          leftArgType, [](const TRuntimeNode arg) { return arg; },
                                          rightArgType, [&pb](const TRuntimeNode) { return pb.NewDataLiteral<i32>(73); },
                                          listJoinType,
                                          [&pb](const TRuntimeNode, const TRuntimeNode leftList, const TRuntimeNode rightList) {
                                              const auto root = pb.Zip({rightList, pb.Map(leftList, [&pb](const auto item) { return pb.Member(item, "a"); })});
                                              return pb.Iterator(root, {}); });

    const auto graph = setup.BuildGraph(listJoin);

    using TOutRow = TDefaultOutRow<false>;
    const TVector<TOutRow> expected{{73, 23}, {73, 24}};
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
}

Y_UNIT_TEST_LLVM(FinishImmediate) {
    // Stream returns Finish before any row is produced (KeyValue_ stays Invalid).
    // The new early-return path fires: Finish is propagated directly without
    // invoking the join lambda, even though the lambda would return non-empty output.
    TSetup<LLVM> setup(GetUnreachableFactory());
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = ConvertValueToLiteralNode(pb, TVector<TDefaultInRow>{});

    const auto int32Type = pb.NewDataType(NUdf::TDataType<i32>::Id);
    const auto [inputRowType, leftArgType, rightArgType] = BuildDefaultStructTypes(pb, list, int32Type);
    const auto [keyColumns, leftColumns, rightColumns] = BuildDefaultColumnMaps(inputRowType, leftArgType, rightArgType);
    const auto listJoinType = pb.NewStreamType(int32Type);
    const auto unreachableArgmapLambda = [&pb](const TRuntimeNode) -> TRuntimeNode { return Unreachable(pb); };
    const auto unreachableJoinLambda = [&pb, &listJoinType](const TRuntimeNode, const TRuntimeNode, const TRuntimeNode) -> TRuntimeNode {
        const auto listItemType = AS_TYPE(TStreamType, listJoinType)->GetItemType();
        return pb.Seq({Unreachable(pb), pb.Iterator(pb.NewEmptyList(listItemType), {})}, listJoinType);
    };

    const auto listJoin = pb.ListJoinCore(pb.Iterator(list, {}),
                                          int32Type, keyColumns,
                                          leftColumns, rightColumns,
                                          leftArgType, unreachableArgmapLambda,
                                          rightArgType, unreachableArgmapLambda,
                                          listJoinType, unreachableJoinLambda);

    const auto graph = setup.BuildGraph(listJoin);

    const TVector<i32> expected{};
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<i32>(expected));
}

Y_UNIT_TEST_LLVM(CheckOptionalWithNoUnwrap) {
    // Test whether nested optionals are handled properly (i.e. NeedUnwrap is false).
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TInnerOpt = TMaybe<i32>;
    using TOuterOpt = TMaybe<TInnerOpt>;
    using TInRow = NTest::TStructType<NTest::TStructMember<"left.a", TOuterOpt>,
                                      NTest::TStructMember<"right.b", TOuterOpt>,
                                      NTest::TStructMember<"key", i32>,
                                      NTest::TStructMember<"_yql_table_index", i32>>;
    const auto list = ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                        {{{TOuterOpt{42}}, {TOuterOpt{}}, {9}, {0}}},          // left: 42??
                                                        {{{TOuterOpt{}}, {TOuterOpt{TInnerOpt{}}}, {9}, {1}}}, // right: Just(Nothing(Int32)
                                                        {{{TOuterOpt{73}}, {TOuterOpt{}}, {9}, {0}}},          // left: 73??
                                                        {{{TOuterOpt{}}, {TOuterOpt{}}, {9}, {1}}},            // right: Nothing(Int32?)
                                                    });

    const auto int32Type = pb.NewDataType(NUdf::TDataType<i32>::Id);
    const auto innerOptType = pb.NewOptionalType(int32Type);
    const auto outerOptType = pb.NewOptionalType(innerOptType);
    const auto leftArgType = AS_TYPE(TStructType, pb.NewStructType({{"a", outerOptType}}));
    const auto rightArgType = AS_TYPE(TStructType, pb.NewStructType({{"b", outerOptType}}));
    const auto inputRowType = AS_TYPE(TStructType, AS_TYPE(TListType, list.GetStaticType())->GetItemType());
    const auto [keyColumns, leftColumns, rightColumns] = BuildDefaultColumnMaps(inputRowType, leftArgType, rightArgType);
    const auto listJoinType = pb.NewStreamType(pb.NewTupleType({outerOptType, outerOptType}));

    const auto listJoin = pb.ListJoinCore(pb.Iterator(list, {}),
                                          int32Type, keyColumns,
                                          leftColumns, rightColumns,
                                          leftArgType, [&pb](const TRuntimeNode arg) { return pb.Member(arg, "a"); },
                                          rightArgType, [&pb](const TRuntimeNode arg) { return pb.Member(arg, "b"); },
                                          listJoinType,
                                          [&pb](const TRuntimeNode, const TRuntimeNode leftList, const TRuntimeNode rightList) {
                                              const auto root = pb.Zip({leftList, rightList});
                                              return pb.Iterator(root, {}); });

    const auto graph = setup.BuildGraph(listJoin);

    using TOutRow = std::tuple<TOuterOpt, TOuterOpt>;
    const TVector<TOutRow> expected{{TOuterOpt{42}, TOuterOpt{TInnerOpt{}}}, {73, TOuterOpt{}}};
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
}

Y_UNIT_TEST_LLVM(CheckPgTypeWithNoUnwrap) {
    // Test whether PGtypes are handled properly (i.e. NeedUnwrap is false).
    TSetup<LLVM> setup(GetFactoryWithPg());
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TInRow = NTest::TStructType<NTest::TStructMember<"left.a", NTest::TPgInt>,
                                      NTest::TStructMember<"right.b", NTest::TPgInt>,
                                      NTest::TStructMember<"key", i32>,
                                      NTest::TStructMember<"_yql_table_index", i32>>;
    const auto list = ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                        {{{NTest::TPgInt(23)}, {NTest::TPgInt()}, {9}, {0}}}, // left: 23
                                                        {{{NTest::TPgInt(24)}, {NTest::TPgInt()}, {9}, {0}}}, // left: 24
                                                        {{{NTest::TPgInt()}, {NTest::TPgInt(42)}, {9}, {1}}}, // right: 42
                                                        {{{NTest::TPgInt()}, {NTest::TPgInt(73)}, {9}, {1}}}, // right: 73
                                                    });

    const auto int32Type = pb.NewDataType(NUdf::TDataType<i32>::Id);
    const auto pgIntType = pb.NewPgType(NYql::NPg::LookupType("int4").TypeId);
    const auto leftArgType = AS_TYPE(TStructType, pb.NewStructType({{"a", pgIntType}}));
    const auto rightArgType = AS_TYPE(TStructType, pb.NewStructType({{"b", pgIntType}}));
    const auto inputRowType = AS_TYPE(TStructType, AS_TYPE(TListType, list.GetStaticType())->GetItemType());
    const auto [keyColumns, leftColumns, rightColumns] = BuildDefaultColumnMaps(inputRowType, leftArgType, rightArgType);
    const auto listJoinType = pb.NewStreamType(pb.NewTupleType({int32Type, int32Type}));

    const auto listJoin = pb.ListJoinCore(pb.Iterator(list, {}),
                                          int32Type, keyColumns,
                                          leftColumns, rightColumns,
                                          leftArgType, [&pb, &int32Type](const TRuntimeNode arg) { return pb.FromPg(pb.Member(arg, "a"), int32Type); },
                                          rightArgType, [&pb, &int32Type](const TRuntimeNode arg) { return pb.FromPg(pb.Member(arg, "b"), int32Type); },
                                          listJoinType,
                                          [&pb](const TRuntimeNode, const TRuntimeNode leftList, const TRuntimeNode rightList) {
                                              const auto root = pb.Zip({leftList, rightList});
                                              return pb.Iterator(root, {}); });

    const auto graph = setup.BuildGraph(listJoin);

    using TOutRow = std::tuple<i32, i32>;
    const TVector<TOutRow> expected{{23, 42}, {24, 73}};
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TOutRow>(expected));
}

} // Y_UNIT_TEST_SUITE(TMiniKQLListJoinCoreTest)

#endif // MKQL_RUNTIME_VERSION >= 78U

} // namespace NKikimr::NMiniKQL
