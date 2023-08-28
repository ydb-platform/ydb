#include "mkql_computation_node_ut.h"
#include <ydb/library/yql/minikql/mkql_runtime_version.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLMapJoinCoreTest) {
    Y_UNIT_TEST_LLVM(TestInnerOnTuple) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto optionalUi64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id, true);
        const auto tupleType = pb.NewTupleType({optionalUi64Type, optionalUi64Type});
        const auto emptyOptionalUi64 = pb.NewEmptyOptional(optionalUi64Type);

        const auto key1 = pb.NewTuple(tupleType, {
            pb.NewOptional(pb.NewDataLiteral<ui64>(1)),
            pb.NewOptional(pb.NewDataLiteral<ui64>(1)),
        });
        const auto key2 = pb.NewTuple(tupleType, {
            pb.NewOptional(pb.NewDataLiteral<ui64>(2)),
            pb.NewOptional(pb.NewDataLiteral<ui64>(2)),
        });
        const auto key3 = pb.NewTuple(tupleType, {
            pb.NewOptional(pb.NewDataLiteral<ui64>(3)),
            emptyOptionalUi64,
        });
        const auto key4 = pb.NewTuple(tupleType, {
            pb.NewOptional(pb.NewDataLiteral<ui64>(4)),
            pb.NewOptional(pb.NewDataLiteral<ui64>(4)),
        });
        const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
        const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
        const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
        const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
        const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
        const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

        const auto structType = pb.NewStructType({
            {"Key", tupleType},
            {"Payload", pb.NewDataType(NUdf::TDataType<char*>::Id)}
        });

        const auto list1 = pb.NewList(structType, {
            pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key1), "Payload", payload1),
            pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload2),
            pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload3)
        });

        const auto list2 = pb.NewList(structType, {
            pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload4),
            pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload5),
            pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key4), "Payload", payload6)
        });
        const auto dict2 = pb.ToSortedDict(list2, false,
            [&](TRuntimeNode item) {
            return pb.Member(item, "Key");
        },
            [&](TRuntimeNode item) {
            return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload"));
        });

        const auto resultType = pb.NewFlowType(pb.NewStructType({
            {"Left", pb.NewDataType(NUdf::TDataType<char*>::Id)},
            {"Right", pb.NewDataType(NUdf::TDataType<char*>::Id)},
        }));

        const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::Inner, {0U}, {1U, 0U}, {0U, 1U}, resultType));
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue tuple;

        UNIT_ASSERT(iterator.Next(tuple));
        UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
        UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
        UNIT_ASSERT(iterator.Next(tuple));
        UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
        UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
        UNIT_ASSERT(!iterator.Next(tuple));
        UNIT_ASSERT(!iterator.Next(tuple));
    }

    Y_UNIT_TEST_LLVM(TestInner) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(3);
            const auto key4 = pb.NewDataLiteral<ui32>(4);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto structType = pb.NewStructType({
                {"Key", pb.NewDataType(NUdf::TDataType<ui32>::Id)},
                {"Payload", pb.NewDataType(NUdf::TDataType<char*>::Id)}
            });

            const auto list1 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key1), "Payload", payload1),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload2),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload3)
            });

            const auto list2 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload4),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload5),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key4), "Payload", payload6)
            });
            const auto dict2 = pb.ToHashedDict(list2, false,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload"));
            });

            const auto resultType = pb.NewFlowType(pb.NewStructType({
                {"Left", pb.NewDataType(NUdf::TDataType<char*>::Id)},
                {"Right", pb.NewDataType(NUdf::TDataType<char*>::Id)},
            }));

            const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::Inner, {0U}, {1U, 0U}, {0U, 1U}, resultType));
            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestInnerMulti) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto structType = pb.NewStructType({
                {"Key", pb.NewDataType(NUdf::TDataType<ui32>::Id)},
                {"Payload", pb.NewDataType(NUdf::TDataType<char*>::Id)}
            });

            const auto list1 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key1), "Payload", payload1),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload2),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload3)
            });

            const auto list2 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload4),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload5),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key4), "Payload", payload6)
            });
            const auto dict2 = pb.ToHashedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload"));
            });

            const auto resultType = pb.NewFlowType(pb.NewStructType({
                {"Left", pb.NewDataType(NUdf::TDataType<char*>::Id)},
                {"Right", pb.NewDataType(NUdf::TDataType<char*>::Id)},
            }));

            const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::Inner, {0U}, {1U, 0U}, {0U, 1U}, resultType));
            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeft) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(3);
            const auto key4 = pb.NewDataLiteral<ui32>(4);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto structType = pb.NewStructType({
                {"Key", pb.NewDataType(NUdf::TDataType<ui32>::Id)},
                {"Payload", pb.NewDataType(NUdf::TDataType<char*>::Id)}
            });
            const auto list1 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key1), "Payload", payload1),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload2),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload3)
            });

            const auto list2 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload4),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload5),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key4), "Payload", payload6)
            });
            const auto dict2 = pb.ToHashedDict(list2, false,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload"));
            });

            const auto resultType = pb.NewFlowType(pb.NewStructType({
                {"Left", pb.NewDataType(NUdf::TDataType<char*>::Id)},
                {"Right", pb.NewDataType(NUdf::TDataType<char*>::Id)},
            }));

            const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::Left, {0U}, {1U, 0U}, {0U, 1U}, resultType));
            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNIT_ASSERT(!tuple.GetElement(1));
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeftMulti) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto structType = pb.NewStructType({
                {"Key", pb.NewDataType(NUdf::TDataType<ui32>::Id)},
                {"Payload", pb.NewDataType(NUdf::TDataType<char*>::Id)}
            });
            const auto list1 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key1), "Payload", payload1),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload2),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload3)
            });

            const auto list2 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload4),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload5),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key4), "Payload", payload6)
            });
            const auto dict2 = pb.ToHashedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload"));
            });

            const auto resultType = pb.NewFlowType(pb.NewStructType({
                {"Left", pb.NewDataType(NUdf::TDataType<char*>::Id)},
                {"Right", pb.NewDataType(NUdf::TDataType<char*>::Id)},
            }));

            const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::Left, {0U}, {1U, 0U}, {0U, 1U}, resultType));
            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNIT_ASSERT(!tuple.GetElement(1));
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeftSemi) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto structType = pb.NewStructType({
                {"Key", pb.NewDataType(NUdf::TDataType<ui32>::Id)},
                {"Payload", pb.NewDataType(NUdf::TDataType<char*>::Id)}
            });
            const auto list1 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key1), "Payload", payload1),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload2),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload3)
            });

            const auto list2 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload4),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload5),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key4), "Payload", payload6)
            });
            const auto dict2 = pb.ToHashedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload"));
            });

            const auto resultType = pb.NewFlowType(pb.NewStructType({
                {"Left", pb.NewDataType(NUdf::TDataType<char*>::Id)},
                {"Key", pb.NewDataType(NUdf::TDataType<ui32>::Id)},
            }));

            const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::LeftSemi, {0U}, {1U, 1U, 0U, 0U}, {}, resultType));
            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(0).Get<ui32>(), 2);
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "B");
            UNIT_ASSERT(iterator.Next(tuple));
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(0).Get<ui32>(), 2);
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "C");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeftOnly) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto structType = pb.NewStructType({
                {"Key", pb.NewDataType(NUdf::TDataType<ui32>::Id)},
                {"Payload", pb.NewDataType(NUdf::TDataType<char*>::Id)}
            });
            const auto list1 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key1), "Payload", payload1),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload2),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload3)
            });

            const auto list2 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload4),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload5),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key4), "Payload", payload6)
            });
            const auto dict2 = pb.ToHashedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload"));
            });

            const auto resultType = pb.NewFlowType(pb.NewStructType({
                {"Left", pb.NewDataType(NUdf::TDataType<char*>::Id)},
                {"Key", pb.NewDataType(NUdf::TDataType<ui32>::Id)},
            }));

            const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::LeftOnly, {0U}, {1U, 1U, 0U, 0U}, {}, resultType));
            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(0).Get<ui32>(), 1);
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "A");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeftSemiWithNullKey) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key0 = pb.NewEmptyOptional(pb.NewDataType(NUdf::TDataType<ui32>::Id, true));
            const auto key1 = pb.NewOptional(pb.NewDataLiteral<ui32>(1));
            const auto key2 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key3 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key4 = pb.NewOptional(pb.NewDataLiteral<ui32>(3));
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto structType = pb.NewStructType({
                {"Key", pb.NewDataType(NUdf::TDataType<ui32>::Id, true)},
                {"Payload", pb.NewDataType(NUdf::TDataType<char*>::Id)}
            });
            const auto list1 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key0), "Payload", payload4),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key1), "Payload", payload1),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload2),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload3)
            });

            const auto list2 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key0), "Payload", payload3),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload4),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload5),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key4), "Payload", payload6)
            });

            const auto dict2 = pb.ToHashedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload"));
            });

            const auto resultType = pb.NewFlowType(pb.NewStructType({
                {"Left", pb.NewDataType(NUdf::TDataType<char*>::Id)},
                {"Key", pb.NewDataType(NUdf::TDataType<ui32>::Id)},
            }));

            const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::LeftSemi, {0U}, {1U, 1U, 0U, 0U}, {}, resultType));
            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(0).Get<ui32>(), 2);
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "B");
            UNIT_ASSERT(iterator.Next(tuple));
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(0).Get<ui32>(), 2);
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "C");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeftOnlyWithNullKey) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key0 = pb.NewEmptyOptional(pb.NewDataType(NUdf::TDataType<ui32>::Id, true));
            const auto key1 = pb.NewOptional(pb.NewDataLiteral<ui32>(1));
            const auto key2 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key3 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key4 = pb.NewOptional(pb.NewDataLiteral<ui32>(3));
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto structType = pb.NewStructType({
                {"Key", pb.NewDataType(NUdf::TDataType<ui32>::Id, true)},
                {"Payload", pb.NewDataType(NUdf::TDataType<char*>::Id)}
            });
            const auto list1 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key0), "Payload", payload4),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key1), "Payload", payload1),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload2),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload3)
            });

            const auto list2 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key0), "Payload", payload3),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload4),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload5),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key4), "Payload", payload6)
            });

            const auto dict2 = pb.ToHashedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload"));
            });

            const auto resultType = pb.NewFlowType(pb.NewStructType({
                {"Left", pb.NewDataType(NUdf::TDataType<char*>::Id)},
                {"Key", pb.NewDataType(NUdf::TDataType<ui32>::Id)},
            }));

            const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::LeftOnly, {0U}, {1U, 1U, 0U, 0U}, {}, resultType));
            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNIT_ASSERT(!tuple.GetElement(0));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(0).Get<ui32>(), 1);
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "A");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }
}
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 18u
Y_UNIT_TEST_SUITE(TMiniKQLWideMapJoinCoreTest) {
    Y_UNIT_TEST_LLVM(TestInner) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(4);
            const auto key4 = pb.NewDataLiteral<ui32>(4);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto dict2 = pb.ToHashedDict(list2, false,
                [&](TRuntimeNode item) {
                return pb.Nth(item, 0U);
            },
                [&](TRuntimeNode item) {
                return pb.NewTuple({pb.Nth(item, 1U)});
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                dict2, EJoinKind::Inner, {0U}, {1U, 0U}, {0U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestInnerMulti) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto dict2 = pb.ToHashedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Nth(item, 0U);
            },
                [&](TRuntimeNode item) {
                return pb.NewTuple({pb.Nth(item, 1U)});
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                dict2, EJoinKind::Inner, {0U}, {1U, 0U}, {0U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeft) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(3);
            const auto key4 = pb.NewDataLiteral<ui32>(4);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto dict2 = pb.ToHashedDict(list2, false,
                [&](TRuntimeNode item) {
                return pb.Nth(item, 0U);
            },
                [&](TRuntimeNode item) {
                return pb.NewTuple({pb.Nth(item, 1U)});
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                dict2, EJoinKind::Left, {0U}, {1U, 0U}, {0U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNIT_ASSERT(!tuple.GetElement(1));
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeftMulti) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto dict2 = pb.ToHashedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Nth(item, 0U);
            },
                [&](TRuntimeNode item) {
                return pb.NewTuple({pb.Nth(item, 1U)});
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                dict2, EJoinKind::Left, {0U}, {1U, 0U}, {0U, 1U}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNIT_ASSERT(!tuple.GetElement(1));
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeftSemi) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto dict2 = pb.ToHashedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Nth(item, 0U);
            },
                [&](TRuntimeNode item) {
                return pb.NewTuple({pb.Nth(item, 1U)});
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<ui32>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                dict2, EJoinKind::LeftSemi, {0U}, {1U, 0U, 0U, 1U}, {}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 2);
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 2);
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeftOnly) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key1 = pb.NewDataLiteral<ui32>(1);
            const auto key2 = pb.NewDataLiteral<ui32>(2);
            const auto key3 = pb.NewDataLiteral<ui32>(2);
            const auto key4 = pb.NewDataLiteral<ui32>(3);
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto dict2 = pb.ToHashedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Nth(item, 0U);
            },
                [&](TRuntimeNode item) {
                return pb.NewTuple({pb.Nth(item, 1U)});
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<ui32>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                dict2, EJoinKind::LeftOnly, {0U}, {1U, 0U, 0U, 1U}, {}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 1);
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeftSemiWithNullKey) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key0 = pb.NewEmptyOptional(pb.NewDataType(NUdf::TDataType<ui32>::Id, true));
            const auto key1 = pb.NewOptional(pb.NewDataLiteral<ui32>(1));
            const auto key2 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key3 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key4 = pb.NewOptional(pb.NewDataLiteral<ui32>(3));
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id, true),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key0, payload4}),
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key0, payload3}),
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto dict2 = pb.ToHashedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Nth(item, 0U);
            },
                [&](TRuntimeNode item) {
                return pb.NewTuple({pb.Nth(item, 1U)});
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<ui32>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                dict2, EJoinKind::LeftSemi, {0U}, {1U, 0U, 0U, 1U}, {}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 2);
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 2);
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestLeftOnlyWithNullKey) {
        for (ui32 pass = 0; pass < 1; ++pass) {
            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            const auto key0 = pb.NewEmptyOptional(pb.NewDataType(NUdf::TDataType<ui32>::Id, true));
            const auto key1 = pb.NewOptional(pb.NewDataLiteral<ui32>(1));
            const auto key2 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key3 = pb.NewOptional(pb.NewDataLiteral<ui32>(2));
            const auto key4 = pb.NewOptional(pb.NewDataLiteral<ui32>(3));
            const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
            const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
            const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
            const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("X");
            const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Y");
            const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Z");

            const auto tupleType = pb.NewTupleType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id, true),
                pb.NewDataType(NUdf::TDataType<char*>::Id)
            });

            const auto list1 = pb.NewList(tupleType, {
                pb.NewTuple({key0, payload4}),
                pb.NewTuple({key1, payload1}),
                pb.NewTuple({key2, payload2}),
                pb.NewTuple({key3, payload3})
            });

            const auto list2 = pb.NewList(tupleType, {
                pb.NewTuple({key0, payload3}),
                pb.NewTuple({key2, payload4}),
                pb.NewTuple({key3, payload5}),
                pb.NewTuple({key4, payload6})
            });

            const auto dict2 = pb.ToHashedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Nth(item, 0U);
            },
                [&](TRuntimeNode item) {
                return pb.NewTuple({pb.Nth(item, 1U)});
            });

            const auto resultType = pb.NewFlowType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<char*>::Id),
                pb.NewDataType(NUdf::TDataType<ui32>::Id)
            }));

            const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                dict2, EJoinKind::LeftOnly, {0U}, {1U, 0U, 0U, 1U}, {}, resultType),
                [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); })
            );

            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "X");
            UNIT_ASSERT(!tuple.GetElement(1));
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNIT_ASSERT_VALUES_EQUAL(tuple.GetElement(1).Get<ui32>(), 1);
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }
}
#endif
}
}

