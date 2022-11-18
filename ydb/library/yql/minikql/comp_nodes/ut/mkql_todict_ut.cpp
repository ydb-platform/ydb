#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <util/random/shuffle.h>
#include <map>
#include <optional>

namespace NKikimr::NMiniKQL {

static const TStringBuf data[] = {
    "13d49d4db08e57d645fe4d44bbed4738f386af6e9e742cf186961063feb9919b",
    "14d285e88582d87c41d3e6d2e9352686d0363ea74a297fe02f901f18c19978a3",
    "1795ad46329c4fc6b3355dc22d252c5fe390a971ddf009b54fdeceb93d3b8930",
    "18042e88fb4cf6b09cb8e6c5588ae525fc7a37bd2248a857d83ac1d1dcdf0a64",
    "1b30b154ac814f7e4ed7e7488e037d781b78fbc336cac027f4c301ad9368514e",
    "1a0b94cebdc038bb293575af4f6954e9dbf19801e581ad03be90b4aef36347d7",
    "1c9ac5b87de7d68efae1bdf1ad47e58d28a6a70e966f19899798c61b2a65b6e2",
    "1618c1e3d9dbc3edaccb7934eca55d2d96cb59d2633655f57401dba99deec3ef",
    "1bd7a6ff86a1940283e202b142ecba685fea86f93f2aafad8cd37d80582aca95",
    "0fba3f2f741b0579a3eec906f5341e0556fbd74088fcdfbe776bd6122fa81681",
    "19768b3228cef7a82e0f1c367d78c42596fa511c735bd85d7cafca0965045562",
    "1a9c0a14272795d7ad39a4725754c3f1d013a761c41fba16e06ae247833fd42b",
    "1562ce72ff7229866723e81e35db30d08c8b8dc7b7d076cff787f157d70763e6",
    "0faf214bafe219413618fdf186bb9290e6a610755d90947cd686b72899e78445",
    "14f3fe97da837197b98f6303ac5aa1b6f34bffe9841fe65f084a27f4bd4ced8a",
    "198c0706af7107ababebf1500875ba64508519b21aa534d0f55e8a32e394799d",
    "1bb66a4593b77b1650a4a530bae30e454c2815477769d67fe6c6b337ae4acafe",
    "0f67ef1ca6ef6b2d699dfac0360e8f24dc8960428cff058fe285d63ab55ef6d3",
    "1097009fe853793802120394fbb6404df82725240d410c69375e5a53ec5395b8",
    "1b1262275eae8a055732253e8d717c19ebde386b25e51dd546413e7ee997c5e1",
    "1c4a73588541a0c869b4ee27c32cc4218f3c8db13713c61cedc387336a2720c9",
    "1c73442f0ac53d8b38f779231680fab806a6cb9c86d25d9db5fa67c0ebf8e803",
    "19152f0c06baf7962ca287a303b85437f321d725985f1586ac8358bdb6a0df63",
    "13436f337815f5929559e6f621b850ed60b36f23ce9d8d06db981b70d40ad3db",
    "298268d866eea5d6fcae470fdbb6d7787d73ab1e50b8126d6452d81264fbdafd",
    "1a67b4e4c213baa140c5a00352cdbc9256b4e2fe81482c380b08ebe2e6b76e1b",
    "19824d2008be54e35a0e2a9d2df9746e96f73367518b111695e1c3857966c241",
    "2997c49ed21482d30b8ef89bd26bfdb6384dda6825032145fe0a3ad9d2f2a7e3",
    "137ccc1d4ab00210bd9af5ee875cb799bd818f4803470abca68a9655ea73be01",
    "12d4cf2eb41c90ede84bece72f76e97d7d0144c45341a0176f656b295cb838c3",
    "11d02da4f449e6aeee4f213409baed6eaab35496688d743991870ba093676c44",
    "163fb1ef04a1453a44fb897014287f7ceefe0b553d99718f986ada32cec6ca29",
    "16f579a7eda4d7f5cde29724bf35e1b36e95fbeb72914ba2ba8f19192b92dab7",
    "0f60c1387bf29d8d81174bd43c136e75f0f33b8b4d2712de0cc3a23f04fac76e",
    "0f83662d3b4cc9aaa0f76c8801d2d32909c050286d514acc108b6c3d9964679b",
    "1a30b7c4bf1c4eaaa92802cb90a27b5073d4a5ec095271490df8443b8f5df88f",
    "105af591b517f126c985f54e833d61907ff49945ab123a606caa6d9bda0e3d66",
    "1a5196fdfc1b81974905a66e6f1ff31403fc84b4d84effde521e848153f42e10",
    "17d6cb5ba9489d8397cb1e1d054e45cff6c7225aeeba5c9e76cacd9da6c9a0c1",
    "127ab4e2169329082bdd546e94c4fb6643999b14a26e08eaa719689789767014",
    "143883410f000b5f3ff4c6434b0654516e9502d0a50a2b3ecdc20c8d3e198915",
    "16ccd345646dd3d39e6bd157b51513c1b319bd1f441199003052a74b2eddb53d",
    "11e9f02dc56d575fac5a306a6e83f597ffda1bd81a01f13fdda059ab24d90892",
    "13f75a9e662faea5fc93f0f83d363c42083345cdcc42f1b0d320d11409ef3052",
    "18cca97e8c6ede52e0b7d8c53c85c0fac68f6d1b7c6622a4cebc21433e6d8eea",
    "160d6b818fab5ad00a1e81e46888c5ff3e5f2c175c013ce17d88c31df4475aba",
    "1c4d09dff19175af7fc0d8e8fd23e9288fc2839dedfc067dcf9f5a3e3a9d92aa",
    "16e25b2a6eef4cde6879c20c94c4360604b9099c29e1abaf9fc079fe67cfcaac",
    "2a577ab7e2541e2cc2cc20e6a76c4ea9b77501808db9c4045be82c680cf227d5",
    "11b4753fd9cc33656dbd59769b3202b7f68bd067bf7f64bd54676f6f60366ef1",
    "1932a0aecc4a569d7d3fbcdd329b92c0b4dbd870d6be48ec4f18285ab3183676",
    "2a2e6b62a4383cb48ffbb69b2f356ceb0410593f5b5500142498692dec7c125f",
};


Y_UNIT_TEST_SUITE(TMiniKQLToDictTest) {
    Y_UNIT_TEST_LLVM(TestCompactUtf8Set) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        TVector<TRuntimeNode> items;
        for (auto s: data) {
            items.push_back(pb.NewDataLiteral<NUdf::EDataSlot::Utf8>(s));
        }
        Shuffle(items.begin(), items.end());
        auto dataType = pb.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id);
        auto list = pb.NewList(dataType, items);
        auto dict = pb.ToHashedDict(list, false, [](TRuntimeNode n) { return n; }, [&pb](TRuntimeNode /*n*/) { return pb.NewVoid(); }, true);
        auto pgmReturn = pb.Contains(dict, items.front());

        auto graph = setup.BuildGraph(pgmReturn);
        auto res = graph->GetValue().template Get<bool>();
        UNIT_ASSERT_VALUES_EQUAL(res, true);
    }

    Y_UNIT_TEST_LLVM(TestUtf8Set) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        TVector<TRuntimeNode> items;
        for (auto s: data) {
            items.push_back(pb.NewDataLiteral<NUdf::EDataSlot::Utf8>(s));
        }
        Shuffle(items.begin(), items.end());
        auto dataType = pb.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id);
        auto list = pb.NewList(dataType, items);
        auto dict = pb.ToHashedDict(list, false, [](TRuntimeNode n) { return n; }, [&pb](TRuntimeNode /*n*/) { return pb.NewVoid(); }, false);
        auto pgmReturn = pb.Contains(dict, items.front());

        auto graph = setup.BuildGraph(pgmReturn);
        auto res = graph->GetValue().template Get<bool>();
        UNIT_ASSERT_VALUES_EQUAL(res, true);
    }

    Y_UNIT_TEST_LLVM(TestSqueezeToDict) {
        auto test = [](bool stream, bool hashed, bool multi, bool compact, bool withPayload) {
            Cerr << "TestSqueezeToDict [on: " << (stream ? "stream" : "flow")
                 << "type: " << (hashed ? "hashed" : "sorted") << ", multi: " << multi
                 << ", compact: " << compact << ", payload: " << withPayload << "]" << Endl;

            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            TVector<TRuntimeNode> items;
            for (auto s : data) {
                items.push_back(pb.NewDataLiteral<NUdf::EDataSlot::Utf8>(s));
            }
            Shuffle(items.begin(), items.end());

            auto dataType = pb.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id);
            auto list = pb.NewList(dataType, items);
            auto input = stream ? pb.Iterator(list, items) : pb.ToFlow(list);
            auto pgmReturn = hashed
                ? pb.SqueezeToHashedDict(input, multi, [](TRuntimeNode n) { return n; },
                    [&pb, withPayload](TRuntimeNode n) { return withPayload ? n : pb.NewVoid(); }, compact)
                : pb.SqueezeToSortedDict(input, multi, [](TRuntimeNode n) { return n; },
                    [&pb, withPayload](TRuntimeNode n) { return withPayload ? n : pb.NewVoid(); }, compact);
            if (!stream) {
                pgmReturn = pb.FromFlow(pgmReturn);
            }

            auto graph = setup.BuildGraph(pgmReturn);
            NUdf::TUnboxedValue res = graph->GetValue();
            UNIT_ASSERT(!res.IsSpecial());

            NUdf::TUnboxedValue v;
            auto status = res.Fetch(v);
            UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, status);

            for (auto s : data) {
                UNIT_ASSERT_C(v.Contains(NUdf::TUnboxedValue(MakeString(s))), s);
            }
            UNIT_ASSERT(!v.Contains(NUdf::TUnboxedValue(MakeString("green cucumber"))));

            status = res.Fetch(v);
            UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, status);
        };

        for (auto stream : {true, false}) {
            for (auto hashed : {true, false}) {
                for (auto multi : {true, false}) {
                    for (auto compact : {true, false}) {
                        for (auto withPayload : {true, false}) {
                            test(stream, hashed, multi, compact, withPayload);
                        }
                    }
                }
            }
        }
    }
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 23u
    Y_UNIT_TEST_LLVM(TestNarrowSqueezeToDict) {
        auto test = [](bool hashed, bool multi, bool compact, bool withPayload) {
            Cerr << "TestNarrowSqueezeToDict [type: " << (hashed ? "hashed" : "sorted") << ", multi: " << multi
                 << ", compact: " << compact << ", payload: " << withPayload << "]" << Endl;

            TSetup<LLVM> setup;
            TProgramBuilder& pb = *setup.PgmBuilder;

            TVector<TRuntimeNode> items;
            for (auto s : data) {
                items.push_back(pb.NewDataLiteral<NUdf::EDataSlot::Utf8>(s));
            }
            Shuffle(items.begin(), items.end());

            auto dataType = pb.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id);
            auto list = pb.NewList(dataType, items);
            auto input = pb.ExpandMap(pb.ToFlow(list), [](TRuntimeNode n) ->TRuntimeNode::TList { return {n}; });
            auto pgmReturn = hashed
                ? pb.NarrowSqueezeToHashedDict(input, multi, [](TRuntimeNode::TList n) { return n.front(); },
                    [&pb, withPayload](TRuntimeNode::TList n) { return withPayload ? n.back() : pb.NewVoid(); }, compact)
                : pb.NarrowSqueezeToSortedDict(input, multi, [](TRuntimeNode::TList n) { return n.front(); },
                    [&pb, withPayload](TRuntimeNode::TList n) { return withPayload ? n.back() : pb.NewVoid(); }, compact);
            pgmReturn = pb.FromFlow(pgmReturn);

            auto graph = setup.BuildGraph(pgmReturn);
            NUdf::TUnboxedValue res = graph->GetValue();
            UNIT_ASSERT(!res.IsSpecial());

            NUdf::TUnboxedValue v;
            auto status = res.Fetch(v);
            UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, status);

            for (auto s : data) {
                UNIT_ASSERT_C(v.Contains(NUdf::TUnboxedValue(MakeString(s))), s);
            }
            UNIT_ASSERT(!v.Contains(NUdf::TUnboxedValue(MakeString("green cucumber"))));

            status = res.Fetch(v);
            UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, status);
        };

        for (auto hashed : {true, false}) {
            for (auto multi : {true, false}) {
                for (auto compact : {true, false}) {
                    for (auto withPayload : {true, false}) {
                        test(hashed, multi, compact, withPayload);
                    }
                }
            }
        }
    }
#endif
    template <bool LLVM>
    static void TestDictWithDataKeyImpl(bool optionalKey, bool multi, bool compact, bool withNull, bool withData) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        TType* keyType = pb.NewDataType(NUdf::EDataSlot::Int32, optionalKey);
        TType* valueType = pb.NewDataType(NUdf::EDataSlot::Int32, false);
        TType* tupleType = pb.NewTupleType({keyType, valueType});
        TVector<TRuntimeNode> items;
        TVector<TRuntimeNode> keys;
        if (withNull) {
            UNIT_ASSERT(optionalKey);
            keys.push_back(pb.NewEmptyOptional(keyType));
            for (size_t k = 0; k < 1 + multi; ++k) {
                items.push_back(pb.NewTuple(tupleType, {keys.back(), pb.NewDataLiteral((i32)items.size())}));
            }
        }
        if (withData) {
            for (i32 i = 0; i < 2; ++i) {
                auto key = pb.NewDataLiteral(i);
                if (optionalKey) {
                    key = pb.NewOptional(key);
                }
                keys.push_back(key);
                for (size_t k = 0; k < 1 + multi; ++k) {
                    items.push_back(pb.NewTuple(tupleType, {key, pb.NewDataLiteral((i32)items.size())}));
                }
            }
        }
        auto list = pb.NewList(tupleType, items);
        auto keyList = pb.NewList(keyType, keys);
        auto dict = pb.ToHashedDict(list, multi, [&](TRuntimeNode tuple) { return pb.Nth(tuple, 0); }, [&pb](TRuntimeNode tuple) { return pb.Nth(tuple, 1); }, compact);

        auto compareLists = [&](bool itemIsTuple, TRuntimeNode list1, TRuntimeNode list2) {
            return pb.And({
                pb.Equals(
                    pb.Length(list1),
                    pb.Length(list2)
                ),
                pb.Not(
                    pb.Exists(
                        pb.Head(
                            pb.SkipWhile(
                                pb.Zip({list1, list2}),
                                [&](TRuntimeNode pair) {
                                    if (itemIsTuple) {
                                        return pb.And({
                                            pb.AggrEquals(pb.Nth(pb.Nth(pair, 0), 0), pb.Nth(pb.Nth(pair, 1), 0)),
                                            pb.AggrEquals(pb.Nth(pb.Nth(pair, 0), 1), pb.Nth(pb.Nth(pair, 1), 1)),
                                        });
                                    } else {
                                        return pb.AggrEquals(pb.Nth(pair, 0), pb.Nth(pair, 1));
                                    }
                                }
                            )
                        )
                    )
                )
            });
        };

        TVector<TRuntimeNode> results;

        // Check Dict has items
        results.push_back(pb.AggrEquals(
            pb.HasItems(dict),
            pb.NewDataLiteral(withNull || withData)
        ));

        // Check Dict length
        results.push_back(pb.AggrEquals(
            pb.Length(dict),
            pb.NewDataLiteral((ui64)keys.size())
        ));

        // Check Dict Contains
        results.push_back(pb.AllOf(
            pb.Map(list, [&](TRuntimeNode tuple) {
                return pb.Contains(dict, pb.Nth(tuple, 0));
            }),
            [&](TRuntimeNode item) { return item; }
        ));

        // Check Dict Lookup
        results.push_back(compareLists(false,
            pb.Sort(
                pb.FlatMap(
                    pb.Map(
                        keyList,
                        [&](TRuntimeNode key) {
                            return pb.Unwrap(pb.Lookup(dict, key), pb.NewDataLiteral<NUdf::EDataSlot::String>("Lookup failed"), "", 0, 0);
                        }
                    ),
                    [&](TRuntimeNode item) {
                        return multi ? item : pb.NewOptional(item);
                    }
                ),
                pb.NewDataLiteral(true),
                [&](TRuntimeNode item) { return item; }
            ),
            pb.Sort(
                pb.Map(list, [&](TRuntimeNode tuple) {
                    return pb.Nth(tuple, 1);
                }),
                pb.NewDataLiteral(true),
                [&](TRuntimeNode item) { return item; }
            )
        ));

        // Check Dict items iterator
        results.push_back(compareLists(true,
            pb.Sort(
                pb.FlatMap(
                    pb.DictItems(dict),
                    [&](TRuntimeNode pair) {
                        if (multi) {
                            return pb.Map(
                                pb.Nth(pair, 1),
                                [&](TRuntimeNode p) {
                                    return pb.NewTuple({pb.Nth(pair, 0), p});
                                }
                            );
                        } else {
                            return pb.NewOptional(pair);
                        }
                    }
                ),
                pb.NewTuple({pb.NewDataLiteral(true), pb.NewDataLiteral(true)}),
                [&](TRuntimeNode item) { return item; }
            ),
            list
        ));

        // Check Dict payloads iterator
        results.push_back(compareLists(false,
            pb.Sort(
                pb.FlatMap(
                    pb.DictPayloads(dict),
                    [&](TRuntimeNode item) {
                        return multi ? item : pb.NewOptional(item);
                    }
                ),
                pb.NewDataLiteral(true),
                [&](TRuntimeNode item) { return item; }
            ),
            pb.Map(
                list,
                [&](TRuntimeNode item) {
                    return pb.Nth(item, 1);
                }
            )
        ));

        auto graph = setup.BuildGraph(pb.NewTuple(results));
        NUdf::TUnboxedValue res = graph->GetValue();

        UNIT_ASSERT_C(res.GetElement(0).Get<bool>(), "Dict HasItems fail");
        UNIT_ASSERT_C(res.GetElement(1).Get<bool>(), "Dict Length fail");
        UNIT_ASSERT_C(res.GetElement(2).Get<bool>(), "Dict Contains fail");
        UNIT_ASSERT_C(res.GetElement(3).Get<bool>(), "Dict Lookup fail");
        UNIT_ASSERT_C(res.GetElement(4).Get<bool>(), "DictItems fail");
        UNIT_ASSERT_C(res.GetElement(5).Get<bool>(), "DictPayloads fail");
    }

    Y_UNIT_TEST_LLVM(TestDictWithDataKey) {
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/false, /*multi*/false, /*compact*/false, /*withNull*/false, /*withData*/true);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/false, /*multi*/false, /*compact*/false, /*withNull*/false, /*withData*/false); // empty dict
    }

    Y_UNIT_TEST_LLVM(TestDictCompactWithDataKey) {
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/false, /*multi*/false, /*compact*/true, /*withNull*/false, /*withData*/true);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/false, /*multi*/false, /*compact*/true, /*withNull*/false, /*withData*/false); // empty dict
    }

    Y_UNIT_TEST_LLVM(TestDictMultiWithDataKey) {
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/false, /*multi*/true, /*compact*/false, /*withNull*/false, /*withData*/true);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/false, /*multi*/true, /*compact*/false, /*withNull*/false, /*withData*/false); // empty dict
    }

    Y_UNIT_TEST_LLVM(TestDictCompactMultiWithDataKey) {
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/false, /*multi*/true, /*compact*/true, /*withNull*/false, /*withData*/true);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/false, /*multi*/true, /*compact*/true, /*withNull*/false, /*withData*/false); // empty dict
    }

    Y_UNIT_TEST_LLVM(TestDictWithOptionalDataKey) {
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/false, /*compact*/false, /*withNull*/false, /*withData*/true);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/false, /*compact*/false, /*withNull*/true, /*withData*/false);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/false, /*compact*/false, /*withNull*/true, /*withData*/true);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/false, /*compact*/false, /*withNull*/false, /*withData*/false); // empty dict
    }

    Y_UNIT_TEST_LLVM(TestDictCompactWithOptionalDataKey) {
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/false, /*compact*/true, /*withNull*/false, /*withData*/true);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/false, /*compact*/true, /*withNull*/true, /*withData*/false);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/false, /*compact*/true, /*withNull*/true, /*withData*/true);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/false, /*compact*/true, /*withNull*/false, /*withData*/false); // empty dict
    }

    Y_UNIT_TEST_LLVM(TestDictMultiWithOptionalDataKey) {
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/true, /*compact*/false, /*withNull*/false, /*withData*/true);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/true, /*compact*/false, /*withNull*/true, /*withData*/false);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/true, /*compact*/false, /*withNull*/true, /*withData*/true);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/true, /*compact*/false, /*withNull*/false, /*withData*/false); // empty dict
    }

    Y_UNIT_TEST_LLVM(TestDictCompactMultiWithOptionalDataKey) {
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/true, /*compact*/true, /*withNull*/false, /*withData*/true);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/true, /*compact*/true, /*withNull*/true, /*withData*/false);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/true, /*compact*/true, /*withNull*/true, /*withData*/true);
        TestDictWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*multi*/true, /*compact*/true, /*withNull*/false, /*withData*/false); // empty dict
    }

    template <bool LLVM>
    static void TestSetWithDataKeyImpl(bool optionalKey, bool compact, bool withNull, bool withData) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        TType* keyType = pb.NewDataType(NUdf::EDataSlot::Int32, optionalKey);
        TVector<TRuntimeNode> keys;
        if (withNull) {
            UNIT_ASSERT(optionalKey);
            keys.push_back(pb.NewEmptyOptional(keyType));
        }
        if (withData) {
            for (i32 i = 0; i < 2; ++i) {
                auto key = pb.NewDataLiteral(i);
                if (optionalKey) {
                    key = pb.NewOptional(key);
                }
                keys.push_back(key);
            }
        }
        auto keyList = pb.NewList(keyType, keys);
        auto set = pb.ToHashedDict(keyList, false, [&](TRuntimeNode key) { return key; }, [&pb](TRuntimeNode) { return pb.NewVoid(); }, compact);

        auto compareLists = [&](TRuntimeNode list1, TRuntimeNode list2) {
            return pb.And({
                pb.Equals(
                    pb.Length(list1),
                    pb.Length(list2)
                ),
                pb.Not(
                    pb.Exists(
                        pb.Head(
                            pb.SkipWhile(
                                pb.Zip({list1, list2}),
                                [&](TRuntimeNode pair) {
                                    return pb.AggrEquals(pb.Nth(pair, 0), pb.Nth(pair, 1));
                                }
                            )
                        )
                    )
                )
            });
        };

        TVector<TRuntimeNode> results;

        // Check Set has items
        results.push_back(pb.AggrEquals(
            pb.HasItems(set),
            pb.NewDataLiteral(withNull || withData)
        ));

        // Check Set length
        results.push_back(pb.AggrEquals(
            pb.Length(set),
            pb.NewDataLiteral((ui64)keys.size())
        ));

        // Check Set Contains
        results.push_back(pb.AllOf(
            pb.Map(keyList, [&](TRuntimeNode key) {
                return pb.Contains(set, key);
            }),
            [&](TRuntimeNode item) { return item; }
        ));

        // Check Set Lookup
        results.push_back(pb.AllOf(
            pb.Map(keyList, [&](TRuntimeNode key) {
                return pb.Exists(pb.Lookup(set, key));
            }),
            [&](TRuntimeNode item) { return item; }
        ));

        // Check Set items iterator
        results.push_back(compareLists(
            pb.Sort(
                pb.DictKeys(set),
                pb.NewDataLiteral(true),
                [&](TRuntimeNode item) { return item; }
            ),
            keyList
        ));

        auto graph = setup.BuildGraph(pb.NewTuple(results));
        NUdf::TUnboxedValue res = graph->GetValue();

        UNIT_ASSERT_C(res.GetElement(0).Get<bool>(), "Set HasItems fail");
        UNIT_ASSERT_C(res.GetElement(1).Get<bool>(), "Set Length fail");
        UNIT_ASSERT_C(res.GetElement(2).Get<bool>(), "Set Contains fail");
        UNIT_ASSERT_C(res.GetElement(3).Get<bool>(), "Set Lookup fail");
        UNIT_ASSERT_C(res.GetElement(4).Get<bool>(), "Set DictKeys fail");
    }

    Y_UNIT_TEST_LLVM(TestSetWithDataKey) {
        TestSetWithDataKeyImpl<LLVM>(/*optionalKey*/false, /*compact*/false, /*withNull*/false, /*withData*/true);
        TestSetWithDataKeyImpl<LLVM>(/*optionalKey*/false, /*compact*/false, /*withNull*/false, /*withData*/false); // empty set
    }

    Y_UNIT_TEST_LLVM(TestSetCompactWithDataKey) {
        TestSetWithDataKeyImpl<LLVM>(/*optionalKey*/false, /*compact*/true, /*withNull*/false, /*withData*/true);
        TestSetWithDataKeyImpl<LLVM>(/*optionalKey*/false, /*compact*/true, /*withNull*/false, /*withData*/false); // empty set
    }

    Y_UNIT_TEST_LLVM(TestSetWithOptionalDataKey) {
        TestSetWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*compact*/false, /*withNull*/false, /*withData*/true);
        TestSetWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*compact*/false, /*withNull*/true, /*withData*/false);
        TestSetWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*compact*/false, /*withNull*/true, /*withData*/true);
        TestSetWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*compact*/false, /*withNull*/false, /*withData*/false); // empty set
    }

    Y_UNIT_TEST_LLVM(TestSetCompactWithOptionalDataKey) {
        TestSetWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*compact*/true, /*withNull*/false, /*withData*/true);
        TestSetWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*compact*/true, /*withNull*/true, /*withData*/false);
        TestSetWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*compact*/true, /*withNull*/true, /*withData*/true);
        TestSetWithDataKeyImpl<LLVM>(/*optionalKey*/true, /*compact*/true, /*withNull*/false, /*withData*/false); // empty set
    }
}


} // namespace
