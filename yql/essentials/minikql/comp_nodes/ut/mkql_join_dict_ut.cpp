#include "mkql_computation_node_ut.h"

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLJoinDictNodeTest) {
    Y_UNIT_TEST_LLVM(TestInner) {
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
            const auto dict1 = pb.ToSortedDict(list1, true,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.Member(item, "Payload");
            });

            const auto list2 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload4),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload5),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key4), "Payload", payload6)
            });
            const auto dict2 = pb.ToSortedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.Member(item, "Payload");
            });

            const auto pgmReturn = pb.JoinDict(dict1, true, dict2, true, EJoinKind::Inner);
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
            const auto dict1 = pb.ToSortedDict(list1, true,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.Member(item, "Payload");
            });

            const auto list2 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload4),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload5),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key4), "Payload", payload6)
            });
            const auto dict2 = pb.ToSortedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.Member(item, "Payload");
            });

            const auto pgmReturn = pb.JoinDict(dict1, true, dict2, true, EJoinKind::Left);
            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
            UNIT_ASSERT(!tuple.GetElement(1));
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNIT_ASSERT(tuple.GetElement(1));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNIT_ASSERT(tuple.GetElement(1));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNIT_ASSERT(tuple.GetElement(1));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNIT_ASSERT(tuple.GetElement(1));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestRight) {
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
            const auto dict1 = pb.ToSortedDict(list1, true,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.Member(item, "Payload");
            });

            const auto list2 = pb.NewList(structType, {
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload4),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload5),
                pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key4), "Payload", payload6)
            });
            const auto dict2 = pb.ToSortedDict(list2, true,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.Member(item, "Payload");
            });

            const auto pgmReturn = pb.JoinDict(dict1, true, dict2, true, EJoinKind::Right);
            const auto graph = setup.BuildGraph(pgmReturn);
            const auto iterator = graph->GetValue().GetListIterator();
            NUdf::TUnboxedValue tuple;

            UNIT_ASSERT(iterator.Next(tuple));
            UNIT_ASSERT(tuple.GetElement(0));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNIT_ASSERT(tuple.GetElement(0));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNIT_ASSERT(tuple.GetElement(0));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
            UNIT_ASSERT(iterator.Next(tuple));
            UNIT_ASSERT(tuple.GetElement(0));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
            UNIT_ASSERT(iterator.Next(tuple));
            UNIT_ASSERT(!tuple.GetElement(0));
            UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Z");
            UNIT_ASSERT(!iterator.Next(tuple));
            UNIT_ASSERT(!iterator.Next(tuple));
        }
    }

    Y_UNIT_TEST_LLVM(TestFull) {
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
        const auto dict1 = pb.ToSortedDict(list1, true,
            [&](TRuntimeNode item) {
            return pb.Member(item, "Key");
        },
            [&](TRuntimeNode item) {
            return pb.Member(item, "Payload");
        });

        const auto list2 = pb.NewList(structType, {
            pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key2), "Payload", payload4),
            pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key3), "Payload", payload5),
            pb.AddMember(pb.AddMember(pb.NewEmptyStruct(), "Key", key4), "Payload", payload6)
        });
        const auto dict2 = pb.ToSortedDict(list2, true,
            [&](TRuntimeNode item) {
            return pb.Member(item, "Key");
        },
            [&](TRuntimeNode item) {
            return pb.Member(item, "Payload");
        });

        const auto pgmReturn = pb.JoinDict(dict1, true, dict2, true, EJoinKind::Full);
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue tuple;

        UNIT_ASSERT(iterator.Next(tuple));
        UNIT_ASSERT(tuple.GetElement(0));
        UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "A");
        UNIT_ASSERT(!tuple.GetElement(1));
        UNIT_ASSERT(iterator.Next(tuple));
        UNIT_ASSERT(tuple.GetElement(0));
        UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
        UNIT_ASSERT(tuple.GetElement(1));
        UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
        UNIT_ASSERT(iterator.Next(tuple));
        UNIT_ASSERT(tuple.GetElement(0));
        UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "B");
        UNIT_ASSERT(tuple.GetElement(1));
        UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
        UNIT_ASSERT(iterator.Next(tuple));
        UNIT_ASSERT(tuple.GetElement(0));
        UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
        UNIT_ASSERT(tuple.GetElement(1));
        UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "X");
        UNIT_ASSERT(iterator.Next(tuple));
        UNIT_ASSERT(tuple.GetElement(0));
        UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(0), "C");
        UNIT_ASSERT(tuple.GetElement(1));
        UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Y");
        UNIT_ASSERT(iterator.Next(tuple));
        UNIT_ASSERT(!tuple.GetElement(0));
        UNIT_ASSERT(tuple.GetElement(1));
        UNBOXED_VALUE_STR_EQUAL(tuple.GetElement(1), "Z");
        UNIT_ASSERT(!iterator.Next(tuple));
        UNIT_ASSERT(!iterator.Next(tuple));
    }

    Y_UNIT_TEST_LLVM(TestInnerFlat) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto key1 = pb.NewDataLiteral<ui32>(1U);
        const auto key2 = pb.NewDataLiteral<ui32>(2U);
        const auto key3 = pb.NewDataLiteral<ui32>(3U);
        const auto key4 = pb.NewDataLiteral<ui32>(4U);
        const auto key5 = pb.NewDataLiteral<ui32>(5U);
        const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("A");
        const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("B");
        const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("C");
        const auto payload4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("D");
        const auto payload5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("E");
        const auto payload6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("F");
        const auto payload7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("G");
        const auto payload8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("H");

        const auto structType = pb.NewStructType({
            {"Key", pb.NewDataType(NUdf::TDataType<ui32>::Id)},
            {"Payload", pb.NewDataType(NUdf::TDataType<char*>::Id)}
        });

        const auto list1 = pb.NewList(structType, {
            pb.NewStruct(structType, {{"Key", key1}, {"Payload", payload1}}),
            pb.NewStruct(structType, {{"Key", key2}, {"Payload", payload2}}),
            pb.NewStruct(structType, {{"Key", key3}, {"Payload", payload3}}),
            pb.NewStruct(structType, {{"Key", key4}, {"Payload", payload4}})
        });

        const auto list2 = pb.NewList(structType, {
            pb.NewStruct(structType, {{"Key", key2}, {"Payload", payload8}}),
            pb.NewStruct(structType, {{"Key", key3}, {"Payload", payload7}}),
            pb.NewStruct(structType, {{"Key", key4}, {"Payload", payload6}}),
            pb.NewStruct(structType, {{"Key", key5}, {"Payload", payload5}})
        });

        const auto listList = pb.NewList(pb.NewListType(structType), {list1, list2});

        const auto pgmReturn = pb.FlatMap(listList,
            [&](TRuntimeNode left) {
            const auto dict1 = pb.ToSortedDict(left, false,
                [&](TRuntimeNode item) {
                return pb.Member(item, "Key");
            },
                [&](TRuntimeNode item) {
                return pb.Member(item, "Payload");
            }, false, 0);
            return pb.FlatMap(listList,
                [&](TRuntimeNode right) {
                const auto dict2 = pb.ToSortedDict(right, false,
                    [&](TRuntimeNode item) {
                    return pb.Member(item, "Key");
                },
                    [&](TRuntimeNode item) {
                    return pb.Member(item, "Payload");
                }, false, 0);
                return pb.JoinDict(dict1, false, dict2, false, EJoinKind::Inner);
            });
        });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto list = graph->GetValue();
        UNIT_ASSERT_VALUES_EQUAL(list.GetListLength(), 14U);

        const auto iterator = list.GetListIterator();
        NUdf::TUnboxedValue item;

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "A");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "A");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "B");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "B");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "C");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "C");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "D");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "D");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "B");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "H");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "C");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "G");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "D");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "F");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "H");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "B");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "G");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "C");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "F");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "D");

        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "H");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "H");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "G");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "G");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "F");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "F");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), "E");
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "E");

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
}

}
}
