#include "ut3/py_test_engine.h"

#include <ydb/library/yql/public/udf/udf_ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NPython;

Y_UNIT_TEST_SUITE(TPyDictTest) {
    Y_UNIT_TEST(FromPyEmptyDict) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDict<ui32, char*>>(
                "def Test(): return {}",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT(!value.HasDictItems());
                    UNIT_ASSERT_EQUAL(value.GetDictLength(), 0);
                });
    }

    Y_UNIT_TEST(FromPyDict_Length) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDict<ui32, char*>>(
                "def Test(): return {1: 'one', 3: 'three', 2: 'two'}",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT(value.HasDictItems());
                    UNIT_ASSERT(!value.IsSortedDict());
                    UNIT_ASSERT_EQUAL(value.GetDictLength(), 3);
                });
    }

    Y_UNIT_TEST(FromPyDict_Lookup) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDict<ui32, char*>>(
                "def Test(): return {1: 'one', 3: 'three', 2: 'two'}",
                [](const NUdf::TUnboxedValuePod& value) {
                    const auto v1 = value.Lookup(NUdf::TUnboxedValuePod(ui32(1)));
                    UNIT_ASSERT_EQUAL(v1.AsStringRef(), "one");
                    const auto v2 = value.Lookup(NUdf::TUnboxedValuePod(ui32(2)));
                    UNIT_ASSERT_EQUAL(v2.AsStringRef(), "two");
                    const auto v3 = value.Lookup(NUdf::TUnboxedValuePod(ui32(3)));
                    UNIT_ASSERT_EQUAL(v3.AsStringRef(), "three");

                    UNIT_ASSERT(!value.Lookup(NUdf::TUnboxedValuePod(ui32(0))));
                    UNIT_ASSERT(!value.Lookup(NUdf::TUnboxedValuePod(ui32(4))));
                });
    }

    Y_UNIT_TEST(FromPyDict_Contains) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDict<ui32, char*>>(
                "def Test(): return {1: 'one', 3: 'three', 2: 'two'}",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(!value.Contains(NUdf::TUnboxedValuePod(ui32(0))));
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod(ui32(1))));
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod(ui32(2))));
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod(ui32(3))));
                    UNIT_ASSERT(!value.Contains(NUdf::TUnboxedValuePod(ui32(4))));
                });
    }

    Y_UNIT_TEST(FromPyDict_Items) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDict<ui32, char*>>(
                "def Test(): return {1: 'one', 3: 'three', 2: 'two'}",
                [](const NUdf::TUnboxedValuePod& value) {
                    std::map<ui32, TString> items;
                    const auto it = value.GetDictIterator();
                    for (NUdf::TUnboxedValue key, payload; it.NextPair(key, payload);) {
                        items.emplace(key.Get<ui32>(), payload.AsStringRef());
                    }

                    UNIT_ASSERT_EQUAL(items.size(), 3);
                    UNIT_ASSERT_EQUAL(items[1], "one");
                    UNIT_ASSERT_EQUAL(items[2], "two");
                    UNIT_ASSERT_EQUAL(items[3], "three");
                });
    }

    Y_UNIT_TEST(FromPyDict_Keys) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDict<ui32, char*>>(
                "def Test(): return {1: 'one', 3: 'three', 2: 'two'}",
                [](const NUdf::TUnboxedValuePod& value) {
                    std::vector<ui32> items;
                    const auto it = value.GetKeysIterator();
                    for (NUdf::TUnboxedValue key; it.Next(key);) {
                        items.emplace_back(key.Get<ui32>());
                    }

                    UNIT_ASSERT_EQUAL(items.size(), 3);

                    std::sort(items.begin(), items.end());
                    UNIT_ASSERT_EQUAL(items[0], 1U);
                    UNIT_ASSERT_EQUAL(items[1], 2U);
                    UNIT_ASSERT_EQUAL(items[2], 3U);
                });
    }

    Y_UNIT_TEST(FromPyDict_Values) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDict<ui32, char*>>(
                "def Test(): return {1: 'one', 3: 'three', 2: 'two'}",
                [](const NUdf::TUnboxedValuePod& value) {
                    std::vector<TString> items;
                    const auto it = value.GetPayloadsIterator();
                    for (NUdf::TUnboxedValue payload; it.Next(payload);) {
                        items.emplace_back(payload.AsStringRef());
                    }

                    UNIT_ASSERT_EQUAL(items.size(), 3);

                    std::sort(items.begin(), items.end());
                    UNIT_ASSERT_EQUAL(items[0], "one");
                    UNIT_ASSERT_EQUAL(items[1], "three");
                    UNIT_ASSERT_EQUAL(items[2], "two");
                });
    }

    Y_UNIT_TEST(FromPyList_Length) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDict<ui32, char*>>(
                "def Test(): return ['one', 'two', 'three']",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT(value.HasDictItems());
                    UNIT_ASSERT(value.IsSortedDict());
                    UNIT_ASSERT_EQUAL(value.GetDictLength(), 3);
                });
    }

    Y_UNIT_TEST(FromPyTuple_Lookup) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDict<i32, char*>>(
                "def Test(): return ('one', 'two', 'three')",
                [](const NUdf::TUnboxedValuePod& value) {
                    const auto v1 = value.Lookup(NUdf::TUnboxedValuePod(i32(0)));
                    UNIT_ASSERT_EQUAL(v1.AsStringRef(), "one");
                    const auto v2 = value.Lookup(NUdf::TUnboxedValuePod(i32(1)));
                    UNIT_ASSERT_EQUAL(v2.AsStringRef(), "two");
                    const auto v3 = value.Lookup(NUdf::TUnboxedValuePod(i32(2)));
                    UNIT_ASSERT_EQUAL(v3.AsStringRef(), "three");
                    const auto v4 = value.Lookup(NUdf::TUnboxedValuePod(i32(-1)));
                    UNIT_ASSERT_EQUAL(v4.AsStringRef(), "three");
                    const auto v5 = value.Lookup(NUdf::TUnboxedValuePod(i32(-2)));
                    UNIT_ASSERT_EQUAL(v5.AsStringRef(), "two");
                    const auto v6 = value.Lookup(NUdf::TUnboxedValuePod(i32(-3)));
                    UNIT_ASSERT_EQUAL(v6.AsStringRef(), "one");

                    UNIT_ASSERT(!value.Lookup(NUdf::TUnboxedValuePod(i32(3))));
                    UNIT_ASSERT(!value.Lookup(NUdf::TUnboxedValuePod(i32(-4))));
                });
    }

    Y_UNIT_TEST(FromPyList_Contains) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDict<i16, char*>>(
                "def Test(): return ['one', 'two', 'three']",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod(i16(0))));
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod(i16(1))));
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod(i16(2))));
                    UNIT_ASSERT(!value.Contains(NUdf::TUnboxedValuePod(i16(3))));
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod(i16(-1))));
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod(i16(-2))));
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod(i16(-3))));
                    UNIT_ASSERT(!value.Contains(NUdf::TUnboxedValuePod(i16(-4))));
                });
    }

    Y_UNIT_TEST(FromPyTuple_Items) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDict<ui16, char*>>(
                "def Test(): return ('one', 'two', 'three')",
                [](const NUdf::TUnboxedValuePod& value) {
                    std::vector<std::pair<ui16, TString>> items;
                    const auto it = value.GetDictIterator();
                    for (NUdf::TUnboxedValue key, payload; it.NextPair(key, payload);) {
                        items.emplace_back(key.Get<ui16>(), payload.AsStringRef());
                    }

                    UNIT_ASSERT_EQUAL(items.size(), 3U);
                    UNIT_ASSERT_EQUAL(items[0].first, 0);
                    UNIT_ASSERT_EQUAL(items[1].first, 1);
                    UNIT_ASSERT_EQUAL(items[2].first, 2);
                    UNIT_ASSERT_EQUAL(items[0].second, "one");
                    UNIT_ASSERT_EQUAL(items[1].second, "two");
                    UNIT_ASSERT_EQUAL(items[2].second, "three");
                });
    }

    Y_UNIT_TEST(FromPyList_Keys) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDict<i64, char*>>(
                "def Test(): return ['one', 'two', 'three']",
                [](const NUdf::TUnboxedValuePod& value) {
                    std::vector<i64> items;
                    const auto it = value.GetKeysIterator();
                    for (NUdf::TUnboxedValue key; it.Next(key);) {
                        items.emplace_back(key.Get<i64>());
                    }

                    UNIT_ASSERT_EQUAL(items.size(), 3);
                    UNIT_ASSERT_EQUAL(items[0], 0);
                    UNIT_ASSERT_EQUAL(items[1], 1);
                    UNIT_ASSERT_EQUAL(items[2], 2);
                });
    }

    Y_UNIT_TEST(FromPyTuple_Values) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDict<ui64, char*>>(
                "def Test(): return ('one', 'two', 'three')",
                [](const NUdf::TUnboxedValuePod& value) {
                    std::vector<TString> items;
                    const auto it = value.GetPayloadsIterator();
                    for (NUdf::TUnboxedValue payload; it.Next(payload);) {
                        items.emplace_back(payload.AsStringRef());
                    }

                    UNIT_ASSERT_EQUAL(items.size(), 3);
                    UNIT_ASSERT_EQUAL(items[0], "one");
                    UNIT_ASSERT_EQUAL(items[1], "two");
                    UNIT_ASSERT_EQUAL(items[2], "three");
                });
    }

    Y_UNIT_TEST(ToPyEmptyDict) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TDict<ui8, ui32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    return vb.NewDict(type, NUdf::TDictFlags::Hashed)->Build();
                },
                "def Test(value):\n"
                "    assert not value\n"
                "    assert len(value) == 0\n"
        );
    }

    Y_UNIT_TEST(ToPyDict) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TDict<int, double>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    return vb.NewDict(type, NUdf::TDictFlags::Hashed)->
                            Add(NUdf::TUnboxedValuePod((int) 1), NUdf::TUnboxedValuePod((double) 0.1))
                            .Add(NUdf::TUnboxedValuePod((int) 2), NUdf::TUnboxedValuePod((double) 0.2))
                            .Add(NUdf::TUnboxedValuePod((int) 3), NUdf::TUnboxedValuePod((double) 0.3))
                            .Build();
                },
                "def Test(value):\n"
                "    assert value\n"
                "    assert len(value) == 3\n"
                "    assert iter(value) is not None\n"
                "    assert 2 in value\n"
                "    assert 0 not in value\n"
                "    assert set(iter(value)) == set([1, 2, 3])\n"
                "    assert value[2] == 0.2\n"
                "    assert value.get(0, 0.7) == 0.7\n"
                "    assert value.get(3, 0.7) == 0.3\n"
                "    assert sorted(value.keys()) == [1, 2, 3]\n"
                "    assert sorted(value.items()) == [(1, 0.1), (2, 0.2), (3, 0.3)]\n"
                "    assert sorted(value.values()) == [0.1, 0.2, 0.3]\n"
#if PY_MAJOR_VERSION < 3
                "    assert all(isinstance(k, int) for k in value.iterkeys())\n"
                "    assert all(isinstance(v, float) for v in value.itervalues())\n"
                "    assert all(isinstance(k, int) and isinstance(v, float) for k,v in value.iteritems())\n"
#endif
        );
    }

    Y_UNIT_TEST(ToPyDictWrongKey) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TDict<int, double>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    return vb.NewDict(type, NUdf::TDictFlags::Hashed)->
                            Add(NUdf::TUnboxedValuePod((int) 1), NUdf::TUnboxedValuePod((double) 0.1))
                            .Add(NUdf::TUnboxedValuePod((int) 2), NUdf::TUnboxedValuePod((double) 0.2))
                            .Add(NUdf::TUnboxedValuePod((int) 3), NUdf::TUnboxedValuePod((double) 0.3))
                            .Build();
                },
                "def Test(value):\n"
                "    try:\n"
                "        print(value[0])\n"
                "    except KeyError:\n"
                "        pass\n"
                "    else:\n"
                "        assert False\n"
        );
    }

    Y_UNIT_TEST(FromPyEmptySet) {
        TPythonTestEngine engine;

        engine.ToMiniKQL<NUdf::TDict<ui32, void>>(
                "def Test(): return set([])",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT(!value.HasDictItems());
                    UNIT_ASSERT_EQUAL(value.GetDictLength(), 0);
                });

    }

    Y_UNIT_TEST(FromPySet) {
        TPythonTestEngine engine;

        engine.ToMiniKQL<NUdf::TDict<char*, void>>(
                "def Test(): return set(['one', 'two', 'three'])",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT(value.HasDictItems());
                    UNIT_ASSERT(!value.IsSortedDict());
                    UNIT_ASSERT_EQUAL(value.GetDictLength(), 3);

                    std::set<TString> set;
                    const auto it = value.GetKeysIterator();
                    for (NUdf::TUnboxedValue key; it.Next(key);) {
                        set.emplace(key.AsStringRef());
                    }

                    UNIT_ASSERT_EQUAL(set.size(), 3);
                    UNIT_ASSERT(set.count("one"));
                    UNIT_ASSERT(set.count("two"));
                    UNIT_ASSERT(set.count("three"));
                });

    }

    Y_UNIT_TEST(FromPySet_Contains) {
        TPythonTestEngine engine;

        engine.ToMiniKQL<NUdf::TDict<char*, void>>(
                "def Test(): return {b'one', b'two', b'three'}",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod::Embedded("one")));
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod::Embedded("two")));
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod::Embedded("three")));
                    UNIT_ASSERT(!value.Contains(NUdf::TUnboxedValuePod::Embedded("zero")));
                });

    }

    Y_UNIT_TEST(ToPyEmptySet) {
        TPythonTestEngine engine;

        engine.ToPython<NUdf::TDict<ui8, void>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    return vb.NewDict(type, NUdf::TDictFlags::Hashed)->Build();
                },
                "def Test(value):\n"
                "    assert not value\n"
                "    assert len(value) == 0\n"
        );

    }

    Y_UNIT_TEST(ToPySet) {
        TPythonTestEngine engine;

        engine.ToPython<NUdf::TDict<ui8, void>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    return vb.NewDict(type, NUdf::TDictFlags::Hashed)->
                            Add(NUdf::TUnboxedValuePod((ui8) 1), NUdf::TUnboxedValuePod::Void())
                            .Add(NUdf::TUnboxedValuePod((ui8) 2), NUdf::TUnboxedValuePod::Void())
                            .Add(NUdf::TUnboxedValuePod((ui8) 3), NUdf::TUnboxedValuePod::Void())
                            .Build();

                },
                "def Test(value):\n"
                "    assert len(value) == 3\n"
                "    assert all(isinstance(k, int) for k in iter(value))\n"
                "    assert all(i in value for i in [1, 2, 3])\n");
    }

    Y_UNIT_TEST(FromPyMultiDict) {
        TPythonTestEngine engine;

        engine.ToMiniKQL<NUdf::TDict<ui32, NUdf::TListType<char*>>>(
                "def Test(): return {1: ['one', 'two'], 3: ['three']}",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT_EQUAL(value.GetDictLength(), 2);

                    std::unordered_map<ui32, std::vector<TString>> map;
                    const auto dictIt = value.GetDictIterator();
                    for (NUdf::TUnboxedValue key, payload; dictIt.NextPair(key, payload);) {
                        auto& val = map[key.Get<ui32>()];
                        const auto listIt = payload.GetListIterator();
                        for (NUdf::TUnboxedValue listItem; listIt.Next(listItem);) {
                            val.emplace_back(listItem.AsStringRef());
                        }
                    }

                    UNIT_ASSERT_EQUAL(map.size(), 2);
                    auto it = map.find(1);
                    UNIT_ASSERT(it != map.end());
                    UNIT_ASSERT_EQUAL(it->second.size(), 2);
                    UNIT_ASSERT_EQUAL(it->second[0], "one");
                    UNIT_ASSERT_EQUAL(it->second[1], "two");
                    it = map.find(3);
                    UNIT_ASSERT(it != map.end());
                    UNIT_ASSERT_EQUAL(it->second.size(), 1);
                    UNIT_ASSERT_EQUAL(it->second[0], "three");
                });

    }

    Y_UNIT_TEST(ToPyMultiDict) {
        TPythonTestEngine engine;

        engine.ToPython<NUdf::TDict<ui8, NUdf::TListType<NUdf::TUtf8>>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    ui32 flags = NUdf::TDictFlags::Hashed | NUdf::TDictFlags::Multi;
                    return vb.NewDict(type, flags)->
                            Add(NUdf::TUnboxedValuePod((ui8) 1), vb.NewString("one"))
                            .Add(NUdf::TUnboxedValuePod((ui8) 1), vb.NewString("two"))
                            .Add(NUdf::TUnboxedValuePod((ui8) 3), vb.NewString("three"))
                            .Build();

                },
                "def Test(value):\n"
                "    assert len(value) == 2\n"
                "    assert 1 in value\n"
                "    assert 3 in value\n"
                "    assert len(value[1]) == 2\n"
                "    assert 'one' in value[1]\n"
                "    assert 'two' in value[1]\n"
                "    assert list(value[3]) == ['three']\n");
    }

    Y_UNIT_TEST(ToPyAndBackDictAsIs) {
        TPythonTestEngine engine;
        engine.ToPythonAndBack<NUdf::TDict<i32, double>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    return vb.NewDict(type, NUdf::TDictFlags::Sorted)->
                            Add(NUdf::TUnboxedValuePod((i32) 1), NUdf::TUnboxedValuePod((double) 0.1))
                            .Add(NUdf::TUnboxedValuePod((i32) 2), NUdf::TUnboxedValuePod((double) 0.2))
                            .Add(NUdf::TUnboxedValuePod((i32) 3), NUdf::TUnboxedValuePod((double) 0.3))
                            .Build();
                },
                "def Test(value): return value",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value.HasDictItems());
                    UNIT_ASSERT_EQUAL(value.GetDictLength(), 3);
                    UNIT_ASSERT(!value.Contains(NUdf::TUnboxedValuePod((i32) 0)));
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod((i32) 3)));
                    UNIT_ASSERT_EQUAL(value.Lookup(NUdf::TUnboxedValuePod((i32) 2)).Get<double>(), 0.2);
                    UNIT_ASSERT(!value.Lookup(NUdf::TUnboxedValuePod((i32) 4)));

                    std::vector<std::pair<i32, double>> items;
                    const auto it = value.GetDictIterator();
                    for (NUdf::TUnboxedValue key, payload; it.NextPair(key, payload);) {
                        items.emplace_back(key.Get<i32>(), payload.Get<double>());
                    }
                    UNIT_ASSERT_EQUAL(items.size(), 3);
                    UNIT_ASSERT_EQUAL(items[0].first, 1);
                    UNIT_ASSERT_EQUAL(items[1].first, 2);
                    UNIT_ASSERT_EQUAL(items[2].first, 3);
                    UNIT_ASSERT_EQUAL(items[0].second, 0.1);
                    UNIT_ASSERT_EQUAL(items[1].second, 0.2);
                    UNIT_ASSERT_EQUAL(items[2].second, 0.3);

                    std::vector<i32> keys;
                    const auto kit = value.GetKeysIterator();
                    for (NUdf::TUnboxedValue key; kit.Next(key);) {
                        keys.emplace_back(key.Get<i32>());
                    }

                    UNIT_ASSERT_EQUAL(keys.size(), 3);
                    UNIT_ASSERT_EQUAL(keys[0], 1);
                    UNIT_ASSERT_EQUAL(keys[1], 2);
                    UNIT_ASSERT_EQUAL(keys[2], 3);

                    std::vector<double> values;
                    const auto pit = value.GetPayloadsIterator();
                    for (NUdf::TUnboxedValue payload; pit.Next(payload);) {
                        values.emplace_back(payload.Get<double>());
                    }

                    UNIT_ASSERT_EQUAL(values.size(), 3);
                    UNIT_ASSERT_EQUAL(values[0], 0.1);
                    UNIT_ASSERT_EQUAL(values[1], 0.2);
                    UNIT_ASSERT_EQUAL(values[2], 0.3);
                }
        );
    }

    Y_UNIT_TEST(PyInvertDict) {
        TPythonTestEngine engine;
        engine.ToPythonAndBack<NUdf::TDict<i32, double>, NUdf::TDict<double, i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    return vb.NewDict(type, NUdf::TDictFlags::Hashed)->
                            Add(NUdf::TUnboxedValuePod((i32) 1), NUdf::TUnboxedValuePod((double) 0.1))
                            .Add(NUdf::TUnboxedValuePod((i32) 2), NUdf::TUnboxedValuePod((double) 0.2))
                            .Add(NUdf::TUnboxedValuePod((i32) 3), NUdf::TUnboxedValuePod((double) 0.3))
                            .Build();
                },
                "def Test(value): return { v: k for k, v in value.items() }",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value.HasDictItems());
                    UNIT_ASSERT_EQUAL(value.GetDictLength(), 3);
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod((double) 0.1)));
                    UNIT_ASSERT(!value.Contains(NUdf::TUnboxedValuePod((double) 0.0)));
                    UNIT_ASSERT(!value.Lookup(NUdf::TUnboxedValuePod((double) 0.4)));
                    UNIT_ASSERT_EQUAL(value.Lookup(NUdf::TUnboxedValuePod((double) 0.2)).Get<i32>(), 2);

                    std::map<double, i32> items;
                    const auto it = value.GetDictIterator();
                    for (NUdf::TUnboxedValue key, payload; it.NextPair(key, payload);) {
                        items.emplace(key.Get<double>(), payload.Get<i32>());
                    }
                    UNIT_ASSERT_EQUAL(items.size(), 3);
                    UNIT_ASSERT_EQUAL(items[0.1], 1);
                    UNIT_ASSERT_EQUAL(items[0.2], 2);
                    UNIT_ASSERT_EQUAL(items[0.3], 3);
                }
        );
    }

    Y_UNIT_TEST(FromPyOrderedDict) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDict<ui32, char*>>(
                "from collections import OrderedDict\n"
                "def Test(): return OrderedDict([(2, 'two'), (1, 'one'), (3, 'three')])\n",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT(value.HasDictItems());
                    UNIT_ASSERT_EQUAL(value.GetDictLength(), 3);

                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod(ui32(1))));
                    UNIT_ASSERT(!value.Contains(NUdf::TUnboxedValuePod(ui32(0))));
                    const auto v = value.Lookup(NUdf::TUnboxedValuePod(ui32(1)));
                    UNIT_ASSERT_EQUAL(v.AsStringRef(), "one");
                    UNIT_ASSERT(!value.Lookup(NUdf::TUnboxedValuePod((ui32(4)))));

#if PY_MAJOR_VERSION >= 3
                    std::vector<std::pair<ui32, TString>> items;
                    const auto it = value.GetDictIterator();
                    for (NUdf::TUnboxedValue key, payload; it.NextPair(key, payload);) {
                        items.emplace_back(key.Get<ui32>(), payload.AsStringRef());
                    }

                    UNIT_ASSERT_EQUAL(items.size(), 3);
                    UNIT_ASSERT_EQUAL(items[0].first, 2);
                    UNIT_ASSERT_EQUAL(items[1].first, 1);
                    UNIT_ASSERT_EQUAL(items[2].first, 3);
                    UNIT_ASSERT_EQUAL(items[0].second, "two");
                    UNIT_ASSERT_EQUAL(items[1].second, "one");
                    UNIT_ASSERT_EQUAL(items[2].second, "three");

                    std::vector<ui32> keys;
                    const auto kit = value.GetKeysIterator();
                    for (NUdf::TUnboxedValue key; kit.Next(key);) {
                        keys.emplace_back(key.Get<ui32>());
                    }

                    UNIT_ASSERT_EQUAL(keys.size(), 3);
                    UNIT_ASSERT_EQUAL(keys[0], 2);
                    UNIT_ASSERT_EQUAL(keys[1], 1);
                    UNIT_ASSERT_EQUAL(keys[2], 3);

                    std::vector<TString> values;
                    const auto pit = value.GetPayloadsIterator();
                    for (NUdf::TUnboxedValue payload; pit.Next(payload);) {
                        values.emplace_back(payload.AsStringRef());
                    }

                    UNIT_ASSERT_EQUAL(values.size(), 3);
                    UNIT_ASSERT_EQUAL(values[0], "two");
                    UNIT_ASSERT_EQUAL(values[1], "one");
                    UNIT_ASSERT_EQUAL(values[2], "three");
#endif
                });
    }

    Y_UNIT_TEST(ToPyAndBackSetAsIs) {
        TPythonTestEngine engine;
        engine.ToPythonAndBack<NUdf::TDict<float, void>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    return vb.NewDict(type, NUdf::TDictFlags::Sorted)->
                            Add(NUdf::TUnboxedValuePod(0.1f), NUdf::TUnboxedValuePod::Void())
                            .Add(NUdf::TUnboxedValuePod(0.2f), NUdf::TUnboxedValuePod::Void())
                            .Add(NUdf::TUnboxedValuePod(0.3f), NUdf::TUnboxedValuePod::Void())
                            .Build();
                },
                "def Test(value): return value",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value.HasDictItems());
                    UNIT_ASSERT_EQUAL(value.GetDictLength(), 3);
                    UNIT_ASSERT(!value.Contains(NUdf::TUnboxedValuePod(0.0f)));
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod(0.3f)));
                    UNIT_ASSERT(value.Lookup(NUdf::TUnboxedValuePod(0.2f)));
                    UNIT_ASSERT(!value.Lookup(NUdf::TUnboxedValuePod(0.4f)));

                    std::vector<float> keys;
                    const auto kit = value.GetKeysIterator();
                    for (NUdf::TUnboxedValue key; kit.Next(key);) {
                        keys.emplace_back(key.Get<float>());
                    }

                    UNIT_ASSERT_EQUAL(keys.size(), 3);
                    UNIT_ASSERT_EQUAL(keys[0], 0.1f);
                    UNIT_ASSERT_EQUAL(keys[1], 0.2f);
                    UNIT_ASSERT_EQUAL(keys[2], 0.3f);
                }
        );
    }

    Y_UNIT_TEST(ToPyAsThinList_FromPyAsDict) {
        TPythonTestEngine engine;
        engine.ToPythonAndBack<NUdf::TListType<float>, NUdf::TDict<i8, float>>(
                [](const TType*, const NUdf::IValueBuilder& vb) {
                    NUdf::TUnboxedValue *items = nullptr;
                    const auto a = vb.NewArray(9U, items);
                    const float f[] = { 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f };
                    std::transform(f, f + 9U, items, [](float v){ return NUdf::TUnboxedValuePod(v); });
                    return a;
                },
                "def Test(value): return value",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value.HasDictItems());
                    UNIT_ASSERT_EQUAL(value.GetDictLength(), 9U);
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod(i8(0))));
                    UNIT_ASSERT(!value.Contains(NUdf::TUnboxedValuePod(i8(10))));
                    UNIT_ASSERT_EQUAL(value.Lookup(NUdf::TUnboxedValuePod(i8(5))).Get<float>(), 0.6f);
                    UNIT_ASSERT(!value.Lookup(NUdf::TUnboxedValuePod(i8(13))));

                    std::vector<std::pair<i8, float>> items;
                    const auto it = value.GetDictIterator();
                    for (NUdf::TUnboxedValue key, payload; it.NextPair(key, payload);) {
                        items.emplace_back(key.Get<i8>(), payload.Get<float>());
                    }

                    UNIT_ASSERT_EQUAL(items.size(), 9U);
                    UNIT_ASSERT_EQUAL(items.front().first, 0);
                    UNIT_ASSERT_EQUAL(items.back().first, 8);
                    UNIT_ASSERT_EQUAL(items.front().second, 0.1f);
                    UNIT_ASSERT_EQUAL(items.back().second, 0.9f);

                    std::vector<i8> keys;
                    const auto kit = value.GetKeysIterator();
                    for (NUdf::TUnboxedValue key; kit.Next(key);) {
                        keys.emplace_back(key.Get<i8>());
                    }

                    UNIT_ASSERT_EQUAL(keys.size(), 9U);
                    UNIT_ASSERT_EQUAL(keys.front(), 0);
                    UNIT_ASSERT_EQUAL(keys.back(), 8);

                    std::vector<float> values;
                    const auto pit = value.GetPayloadsIterator();
                    for (NUdf::TUnboxedValue payload; pit.Next(payload);) {
                        values.emplace_back(payload.Get<float>());
                    }

                    UNIT_ASSERT_EQUAL(values.size(), 9U);
                    UNIT_ASSERT_EQUAL(values.front(), 0.1f);
                    UNIT_ASSERT_EQUAL(values.back(), 0.9f);
                }
        );
    }

    Y_UNIT_TEST(ToPyAsLazyList_FromPyAsDict) {
        TPythonTestEngine engine;
        engine.ToPythonAndBack<NUdf::TListType<i32>, NUdf::TDict<ui8, i32>>(
                [](const TType*, const NUdf::IValueBuilder&) {
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(1, 10));
                },
                "def Test(value): return value",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value.HasDictItems());
                    UNIT_ASSERT_EQUAL(value.GetDictLength(), 9U);
                    UNIT_ASSERT(value.Contains(NUdf::TUnboxedValuePod(ui8(0))));
                    UNIT_ASSERT(!value.Contains(NUdf::TUnboxedValuePod(ui8(10))));
                    UNIT_ASSERT_EQUAL(value.Lookup(NUdf::TUnboxedValuePod(ui8(5))).Get<i32>(), 6);
                    UNIT_ASSERT(!value.Lookup(NUdf::TUnboxedValuePod(ui8(13))));

                    std::vector<std::pair<ui8, i32>> items;
                    const auto it = value.GetDictIterator();
                    for (NUdf::TUnboxedValue key, payload; it.NextPair(key, payload);) {
                        items.emplace_back(key.Get<ui8>(), payload.Get<i32>());
                    }

                    UNIT_ASSERT_EQUAL(items.size(), 9U);
                    UNIT_ASSERT_EQUAL(items.front().first, 0);
                    UNIT_ASSERT_EQUAL(items.back().first, 8);
                    UNIT_ASSERT_EQUAL(items.front().second, 1);
                    UNIT_ASSERT_EQUAL(items.back().second, 9);

                    std::vector<ui8> keys;
                    const auto kit = value.GetKeysIterator();
                    for (NUdf::TUnboxedValue key; kit.Next(key);) {
                        keys.emplace_back(key.Get<ui8>());
                    }

                    UNIT_ASSERT_EQUAL(keys.size(), 9U);
                    UNIT_ASSERT_EQUAL(keys.front(), 0);
                    UNIT_ASSERT_EQUAL(keys.back(), 8);

                    std::vector<i32> values;
                    const auto pit = value.GetPayloadsIterator();
                    for (NUdf::TUnboxedValue payload; pit.Next(payload);) {
                        values.emplace_back(payload.Get<i32>());
                    }

                    UNIT_ASSERT_EQUAL(values.size(), 9U);
                    UNIT_ASSERT_EQUAL(values.front(), 1);
                    UNIT_ASSERT_EQUAL(values.back(), 9);
                }
        );
    }
}
