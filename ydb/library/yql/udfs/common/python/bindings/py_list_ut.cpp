#include "ut3/py_test_engine.h"

#include <ydb/library/yql/public/udf/udf_ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>


using namespace NPython;

Y_UNIT_TEST_SUITE(TPyListTest) {
    Y_UNIT_TEST(FromPyEmptyList) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TListType<ui32>>(
                "def Test(): return []",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT_EQUAL(value.GetListLength(), 0);
                });
    }

    Y_UNIT_TEST(FromPyList) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TListType<ui32>>(
                "def Test(): return [1, 2, 3, 4]",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT_EQUAL(value.GetListLength(), 4);
                    const auto it = value.GetListIterator();
                    NUdf::TUnboxedValue item;

                    UNIT_ASSERT(it.Next(item));
                    UNIT_ASSERT_EQUAL(item.Get<ui32>(), 1);
                    UNIT_ASSERT(it.Next(item));
                    UNIT_ASSERT_EQUAL(item.Get<ui32>(), 2);
                    UNIT_ASSERT(it.Next(item));
                    UNIT_ASSERT_EQUAL(item.Get<ui32>(), 3);
                    UNIT_ASSERT(it.Next(item));
                    UNIT_ASSERT_EQUAL(item.Get<ui32>(), 4);
                    UNIT_ASSERT(false == it.Next(item));
                });
    }

    Y_UNIT_TEST(ToPyEmptyList) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<char*>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    return vb.NewEmptyList();
                },
                "def Test(value):\n"
                "    assert value.has_fast_len()\n"
                "    assert len(value) == 0\n");
    }

    Y_UNIT_TEST(ToPyList) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<double>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    std::array<NUdf::TUnboxedValue, 3U> list = {{
                        NUdf::TUnboxedValuePod(0.1),
                        NUdf::TUnboxedValuePod(0.2),
                        NUdf::TUnboxedValuePod(0.3)
                    }};
                    return vb.NewList(list.data(), list.size());
                },
                "def Test(value):\n"
                "    assert value.has_fast_len()\n"
                "    assert len(value) == 3\n"
                "    assert all(isinstance(v, float) for v in value)\n"
                "    assert list(value) == [0.1, 0.2, 0.3]\n");
    }

    Y_UNIT_TEST(FromPyTuple) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TListType<ui32>>(
                "def Test(): return (1, 2, 3)",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT_EQUAL(value.GetListLength(), 3);

                    ui32 expected = 1;
                    auto it = value.GetListIterator();
                    for (NUdf::TUnboxedValue item; it.Next(item);) {
                        ui32 actual = item.Get<ui32>();
                        UNIT_ASSERT_EQUAL(actual, expected);
                        expected++;
                    }
                });
    }

    Y_UNIT_TEST(ThinListIteration) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<double>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    std::array<NUdf::TUnboxedValue, 3U> list = {{
                        NUdf::TUnboxedValuePod(0.1),
                        NUdf::TUnboxedValuePod(0.2),
                        NUdf::TUnboxedValuePod(0.3)
                    }};
                    return vb.NewList(list.data(), list.size());
                },
                "def Test(value):\n"
                "   assert '__iter__' in dir(value)\n"
                "   it = iter(value)\n"
                "   assert next(it) == 0.1\n"
                "   assert next(it) == 0.2\n"
                "   assert next(it) == 0.3\n"
                "   try:\n"
                "       next(it)\n"
                "   except StopIteration:\n"
                "       pass\n"
                "   else:\n"
                "       assert False\n"
        );
    }

    Y_UNIT_TEST(ThinListReversed) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    std::array<NUdf::TUnboxedValue, 10U> list = {{
                        NUdf::TUnboxedValuePod(0U),
                        NUdf::TUnboxedValuePod(1U),
                        NUdf::TUnboxedValuePod(2U),
                        NUdf::TUnboxedValuePod(3U),
                        NUdf::TUnboxedValuePod(4U),
                        NUdf::TUnboxedValuePod(5U),
                        NUdf::TUnboxedValuePod(6U),
                        NUdf::TUnboxedValuePod(7U),
                        NUdf::TUnboxedValuePod(8U),
                        NUdf::TUnboxedValuePod(9U)
                    }};
                    return vb.NewList(list.data(), list.size());
                },
                "def Test(v):\n"
                "   e = list(range(0, 10))\n"
                "   assert '__reversed__' in dir(v)\n"
                "   assert all(one == two for one, two in zip(reversed(v), reversed(e)))\n"
        );
    }

    Y_UNIT_TEST(LazyListReversed) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(0, 3));
                },
                "def Test(v):\n"
                "   assert '__reversed__' in dir(v)\n"
                "   it = iter(reversed(v))\n"
                "   assert next(it) == 2\n"
                "   assert next(it) == 1\n"
                "   assert next(it) == 0\n"
                "   try:\n"
                "       next(it)\n"
                "   except StopIteration:\n"
                "       pass\n"
                "   else:\n"
                "       assert False\n"
        );
    }

    Y_UNIT_TEST(LazyListIteration) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(0, 3));
                },
                "def Test(value):\n"
                "   assert '__iter__' in dir(value)\n"
                "   it = iter(value)\n"
                "   assert next(it) == 0\n"
                "   assert next(it) == 1\n"
                "   assert next(it) == 2\n"
                "   try:\n"
                "       next(it)\n"
                "   except StopIteration:\n"
                "       pass\n"
                "   else:\n"
                "       assert False\n"
        );
    }

    Y_UNIT_TEST(LazyListInvalidIndexType) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(0, 3));
                },
                "def Test(v):\n"
                "   try:\n"
                "       print(v[{}])\n"
                "   except TypeError:\n"
                "       pass\n"
                "   else:\n"
                "       assert False\n"
        );
    }

    Y_UNIT_TEST(ThinListInvalidIndexType) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<double>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    std::array<NUdf::TUnboxedValue, 3U> list = {{
                        NUdf::TUnboxedValuePod(0.1),
                        NUdf::TUnboxedValuePod(0.2),
                        NUdf::TUnboxedValuePod(0.3)
                    }};
                    return vb.NewList(list.data(), list.size());
                },
                "def Test(v):\n"
                "   try:\n"
                "       print(v[{}])\n"
                "   except TypeError:\n"
                "       pass\n"
                "   else:\n"
                "       assert False\n"
        );
    }

    Y_UNIT_TEST(LazyListZeroSliceStep) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(0, 3));
                },
                "def Test(v):\n"
                "   try:\n"
                "       print(v[::0])\n"
                "   except ValueError:\n"
                "       pass\n"
                "   else:\n"
                "       assert False\n"
        );
    }

    Y_UNIT_TEST(ThinListZeroSliceStep) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<double>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    std::array<NUdf::TUnboxedValue, 3U> list = {{
                        NUdf::TUnboxedValuePod(0.1),
                        NUdf::TUnboxedValuePod(0.2),
                        NUdf::TUnboxedValuePod(0.3)
                    }};
                    return vb.NewList(list.data(), list.size());
                },
                "def Test(v):\n"
                "   try:\n"
                "       print(v[::0])\n"
                "   except ValueError:\n"
                "       pass\n"
                "   else:\n"
                "       assert False\n"
        );
    }

    Y_UNIT_TEST(ThinListSlice) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    std::array<NUdf::TUnboxedValue, 10U> list = {{
                        NUdf::TUnboxedValuePod(0U),
                        NUdf::TUnboxedValuePod(1U),
                        NUdf::TUnboxedValuePod(2U),
                        NUdf::TUnboxedValuePod(3U),
                        NUdf::TUnboxedValuePod(4U),
                        NUdf::TUnboxedValuePod(5U),
                        NUdf::TUnboxedValuePod(6U),
                        NUdf::TUnboxedValuePod(7U),
                        NUdf::TUnboxedValuePod(8U),
                        NUdf::TUnboxedValuePod(9U)
                    }};
                    return vb.NewList(list.data(), list.size());
                },
                "def Test(v):\n"
                "   e = list(range(0, 10))\n"
                "   assert '__len__' in dir(v)\n"
                "   assert list(v[::1]) == e[::1]\n"
                "   assert list(v[::-1]) == e[::-1]\n"
                "   assert list(v[1::1]) == e[1::1]\n"
                "   assert list(v[2::1]) == e[2::1]\n"
                "   assert list(v[3::1]) == e[3::1]\n"
                "   assert list(v[:-1:1]) == e[:-1:1]\n"
                "   assert list(v[:-2:1]) == e[:-2:1]\n"
                "   assert list(v[:-3:1]) == e[:-3:1]\n"
                "   assert list(v[1::-1]) == e[1::-1]\n"
                "   assert list(v[2::-1]) == e[2::-1]\n"
                "   assert list(v[3::-1]) == e[3::-1]\n"
                "   assert list(v[:-1:-1]) == e[:-1:-1]\n"
                "   assert list(v[:-2:-1]) == e[:-2:-1]\n"
                "   assert list(v[:-3:-1]) == e[:-3:-1]\n"
                "   assert list(v[:-2:-1]) == e[:-2:-1]\n"
                "   assert list(v[-12:-1:1]) == e[-12:-1:1]\n"
                "   assert list(v[-12:-1:-1]) == e[-12:-1:-1]\n"
                "   assert list(v[-5:-3:1]) == e[-5:-3:1]\n"
                "   assert list(v[-7:-2:-1]) == e[-7:-2:-1]\n"
                "   assert list(v[:7:1]) == e[:7:1]\n"
                "   assert list(v[-1:4]) == e[-1:4]\n"
                "   assert list(v[5:11]) == e[5:11]\n"
                "   assert list(v[4:1]) == e[4:1]\n"
                "   assert list(v[5:-2]) == e[5:-2]\n"
        );
    }

    Y_UNIT_TEST(ThinListSliceOverReversed) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    std::array<NUdf::TUnboxedValue, 10U> list = {{
                        NUdf::TUnboxedValuePod(0U),
                        NUdf::TUnboxedValuePod(1U),
                        NUdf::TUnboxedValuePod(2U),
                        NUdf::TUnboxedValuePod(3U),
                        NUdf::TUnboxedValuePod(4U),
                        NUdf::TUnboxedValuePod(5U),
                        NUdf::TUnboxedValuePod(6U),
                        NUdf::TUnboxedValuePod(7U),
                        NUdf::TUnboxedValuePod(8U),
                        NUdf::TUnboxedValuePod(9U)
                    }};
                    return vb.NewList(list.data(), list.size());
                },
                "def Test(x):\n"
                "   e = list(reversed(range(0, 10)))\n"
                "   v = reversed(x)\n"
                "   assert list(v[::1]) == e[::1]\n"
                "   assert list(v[::-1]) == e[::-1]\n"
                "   assert list(v[1::1]) == e[1::1]\n"
                "   assert list(v[2::1]) == e[2::1]\n"
                "   assert list(v[3::1]) == e[3::1]\n"
                "   assert list(v[:-1:1]) == e[:-1:1]\n"
                "   assert list(v[:-2:1]) == e[:-2:1]\n"
                "   assert list(v[:-3:1]) == e[:-3:1]\n"
                "   assert list(v[1::-1]) == e[1::-1]\n"
                "   assert list(v[2::-1]) == e[2::-1]\n"
                "   assert list(v[3::-1]) == e[3::-1]\n"
                "   assert list(v[:-1:-1]) == e[:-1:-1]\n"
                "   assert list(v[:-2:-1]) == e[:-2:-1]\n"
                "   assert list(v[:-3:-1]) == e[:-3:-1]\n"
                "   assert list(v[:-2:-1]) == e[:-2:-1]\n"
                "   assert list(v[-12:-1:1]) == e[-12:-1:1]\n"
                "   assert list(v[-12:-1:-1]) == e[-12:-1:-1]\n"
                "   assert list(v[-5:-3:1]) == e[-5:-3:1]\n"
                "   assert list(v[-7:-2:-1]) == e[-7:-2:-1]\n"
                "   assert list(v[:7:1]) == e[:7:1]\n"
                "   assert list(v[-1:4]) == e[-1:4]\n"
                "   assert list(v[5:11]) == e[5:11]\n"
                "   assert list(v[4:1]) == e[4:1]\n"
                "   assert list(v[5:-2]) == e[5:-2]\n"
        );
    }

    Y_UNIT_TEST(LazyListSlice) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<true>(0, 10));
                },
                "def Test(v):\n"
                "   e = list(range(0, 10))\n"
                "   assert '__len__' in dir(v)\n"
                "   assert len(v) == len(e)\n"
                "   assert list(v[::1]) == e[::1]\n"
                "   assert list(v[::-1]) == e[::-1]\n"
                "   assert list(v[3:]) == e[3:]\n"
                "   assert list(v[-2:]) == e[-2:]\n"
                "   assert list(v[2::-1]) == e[2::-1]\n"
                "   assert list(v[:-2:-1]) == e[:-2:-1]\n"
                "   assert list(v[-12:-1:1]) == e[-12:-1:1]\n"
                "   assert list(v[-12:-1:-1]) == e[-12:-1:-1]\n"
                "   assert list(v[-5:-3:1]) == e[-5:-3:1]\n"
                "   assert list(v[-7:-2:-1]) == e[-7:-2:-1]\n"
                "   assert list(v[:7:1]) == e[:7:1]\n"
                "   assert list(v[-1:4]) == e[-1:4]\n"
                "   assert list(v[5:11]) == e[5:11]\n"
                "   assert list(v[4:1]) == e[4:1]\n"
                "   assert list(v[5:-2]) == e[5:-2]\n"
        );
    }

    Y_UNIT_TEST(ThinListIterateSliceWithStep) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    std::array<NUdf::TUnboxedValue, 20U> list = {{
                        NUdf::TUnboxedValuePod(0U),
                        NUdf::TUnboxedValuePod(1U),
                        NUdf::TUnboxedValuePod(2U),
                        NUdf::TUnboxedValuePod(3U),
                        NUdf::TUnboxedValuePod(4U),
                        NUdf::TUnboxedValuePod(5U),
                        NUdf::TUnboxedValuePod(6U),
                        NUdf::TUnboxedValuePod(7U),
                        NUdf::TUnboxedValuePod(8U),
                        NUdf::TUnboxedValuePod(9U),
                        NUdf::TUnboxedValuePod(10U),
                        NUdf::TUnboxedValuePod(11U),
                        NUdf::TUnboxedValuePod(12U),
                        NUdf::TUnboxedValuePod(13U),
                        NUdf::TUnboxedValuePod(14U),
                        NUdf::TUnboxedValuePod(15U),
                        NUdf::TUnboxedValuePod(16U),
                        NUdf::TUnboxedValuePod(17U),
                        NUdf::TUnboxedValuePod(18U),
                        NUdf::TUnboxedValuePod(19U)
                    }};
                    return vb.NewList(list.data(), list.size());
                },
                "def Test(v):\n"
                "   e = list(range(0, 20))\n"
                "   assert all(one == two for one, two in zip(iter(v[::2]), e[::2]))\n"
                "   assert all(one == two for one, two in zip(iter(v[3:8:2]), e[3:8:2]))\n"
                "   assert all(one == two for one, two in zip(iter(v[::-2]), e[::-2]))\n"
                "   assert all(one == two for one, two in zip(iter(v[::-3]), e[::-3]))\n"
                "   assert all(one == two for one, two in zip(iter(v[:3:-3]), e[:3:-3]))\n"
                "   assert all(one == two for one, two in zip(iter(v[-7::-3]), e[-7::-3]))\n"
                "   assert all(one == two for one, two in zip(iter(v[-6::-3]), e[-6::-3]))\n"
                "   assert all(one == two for one, two in zip(iter(v[-5::-3]), e[-5::-3]))\n"
                "   assert all(one == two for one, two in zip(iter(v[:-2:-2]), e[:-2:-2]))\n"
                "   assert all(one == two for one, two in zip(iter(v[-2:-6:-2]), e[-2:-6:-2]))\n"
                "   assert all(one == two for one, two in zip(iter(v[2:-6:-2][::2]), e[2:-6:-2][::2]))\n"
                "   assert all(one == two for one, two in zip(iter(v[2:6:-2][:-2:-2]), e[2:6:-2][:-2:-2]))\n"
                "   assert all(one == two for one, two in zip(iter(v[:-2:-2][:2:3]), e[:-2:-2][:2:3]))\n"
                "   assert all(one == two for one, two in zip(iter(v[:-2:-2][:2:-3]), e[:-2:-2][:2:-3]))\n"
                "   assert all(one == two for one, two in zip(iter(v[:-2:2][:2:3]), e[:-2:2][:2:3]))\n"
        );
    }

    Y_UNIT_TEST(LazyListIterateSliceWithStep) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<true>(0, 20));
                },
                "def Test(v):\n"
                "   e = list(range(0, 20))\n"
                "   assert all(one == two for one, two in zip(iter(v[::2]), e[::2]))\n"
                "   assert all(one == two for one, two in zip(iter(v[::-3]), e[::-3]))\n"
                "   assert all(one == two for one, two in zip(iter(v[:3:-3]), e[:3:-3]))\n"
                "   assert all(one == two for one, two in zip(iter(v[3:4:2]), e[3:4:2]))\n"
                "   assert all(one == two for one, two in zip(iter(v[-7::-3]), e[-7::-3]))\n"
                "   assert all(one == two for one, two in zip(iter(v[-6::-3]), e[-6::-3]))\n"
                "   assert all(one == two for one, two in zip(iter(v[-5::-3]), e[-5::-3]))\n"
                "   assert all(one == two for one, two in zip(iter(v[:-2:-2]), e[:-2:-2]))\n"
                "   assert all(one == two for one, two in zip(iter(v[-2:-6:-2]), e[-2:-6:-2]))\n"
                "   assert all(one == two for one, two in zip(iter(v[2:-6:-2][::2]), e[2:-6:-2][::2]))\n"
                "   assert all(one == two for one, two in zip(iter(v[2:6:-2][:-2:-2]), e[2:6:-2][:-2:-2]))\n"
                "   assert all(one == two for one, two in zip(iter(v[:-2:2][:2:3]), e[:-2:2][:2:3]))\n"
                "   assert all(one == two for one, two in zip(iter(v[:-2:-2][:2:3]), e[:-2:-2][:2:3]))\n"
                "   assert all(one == two for one, two in zip(iter(v[:-2:-2][:2:-3]), e[:-2:-2][:2:-3]))\n"
        );
    }

    Y_UNIT_TEST(ThinListGetByIndexSliceWithStep) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    std::array<NUdf::TUnboxedValue, 20U> list = {{
                        NUdf::TUnboxedValuePod(0U),
                        NUdf::TUnboxedValuePod(1U),
                        NUdf::TUnboxedValuePod(2U),
                        NUdf::TUnboxedValuePod(3U),
                        NUdf::TUnboxedValuePod(4U),
                        NUdf::TUnboxedValuePod(5U),
                        NUdf::TUnboxedValuePod(6U),
                        NUdf::TUnboxedValuePod(7U),
                        NUdf::TUnboxedValuePod(8U),
                        NUdf::TUnboxedValuePod(9U),
                        NUdf::TUnboxedValuePod(10U),
                        NUdf::TUnboxedValuePod(11U),
                        NUdf::TUnboxedValuePod(12U),
                        NUdf::TUnboxedValuePod(13U),
                        NUdf::TUnboxedValuePod(14U),
                        NUdf::TUnboxedValuePod(15U),
                        NUdf::TUnboxedValuePod(16U),
                        NUdf::TUnboxedValuePod(17U),
                        NUdf::TUnboxedValuePod(18U),
                        NUdf::TUnboxedValuePod(19U)
                    }};
                    return vb.NewList(list.data(), list.size());
                },
                "def Test(v):\n"
                "   e = list(range(0, 20))\n"
                "   assert v[::2][3] == e[::2][3]\n"
                "   assert v[::2][5] == e[::2][5]\n"
                "   assert v[::2][-3] == e[::2][-3]\n"
                "   assert v[::2][-7] == e[::2][-7]\n"
                "   assert v[2::2][4] == e[2::2][4]\n"
                "   assert v[2::2][5] == e[2::2][5]\n"
                "   assert v[2::2][-7] == e[2::2][-7]\n"
                "   assert v[2::2][-2] == e[2::2][-2]\n"
                "   assert v[:-3:2][2] == e[:-3:2][2]\n"
                "   assert v[:-3:2][4] == e[:-3:2][4]\n"
                "   assert v[:-3:2][-1] == e[:-3:2][-1]\n"
                "   assert v[:-3:2][-2] == e[:-3:2][-2]\n"
                "   assert v[:-4:3][2] == e[:-4:3][2]\n"
                "   assert v[:-4:3][4] == e[:-4:3][4]\n"
                "   assert v[:-4:3][-3] == e[:-4:3][-3]\n"
                "   assert v[:-4:3][-2] == e[:-4:3][-2]\n"
                "   assert v[-6::-3][1] == e[-6::-3][1]\n"
                "   assert v[-6::-3][3] == e[-6::-3][3]\n"
                "   assert v[-6::-3][-4] == e[-6::-3][-4]\n"
                "   assert v[-6::-3][-1] == e[-6::-3][-1]\n"
        );
    }

    Y_UNIT_TEST(LazyListGetByIndexSliceWithStep) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<true>(0, 20));
                },
                "def Test(v):\n"
                "   e = list(range(0, 20))\n"
                "   assert v[::2][3] == e[::2][3]\n"
                "   assert v[::2][5] == e[::2][5]\n"
                "   assert v[::2][-3] == e[::2][-3]\n"
                "   assert v[::2][-7] == e[::2][-7]\n"
                "   assert v[2::2][4] == e[2::2][4]\n"
                "   assert v[2::2][5] == e[2::2][5]\n"
                "   assert v[2::2][-7] == e[2::2][-7]\n"
                "   assert v[2::2][-2] == e[2::2][-2]\n"
                "   assert v[:-3:2][2] == e[:-3:2][2]\n"
                "   assert v[:-3:2][4] == e[:-3:2][4]\n"
                "   assert v[:-3:2][-1] == e[:-3:2][-1]\n"
                "   assert v[:-3:2][-2] == e[:-3:2][-2]\n"
                "   assert v[:-4:3][2] == e[:-4:3][2]\n"
                "   assert v[:-4:3][4] == e[:-4:3][4]\n"
                "   assert v[:-4:3][-3] == e[:-4:3][-3]\n"
                "   assert v[:-4:3][-2] == e[:-4:3][-2]\n"
                "   assert v[-6::-3][1] == e[-6::-3][1]\n"
                "   assert v[-6::-3][3] == e[-6::-3][3]\n"
                "   assert v[-6::-3][-4] == e[-6::-3][-4]\n"
                "   assert v[-6::-3][-1] == e[-6::-3][-1]\n"
        );
    }

    Y_UNIT_TEST(ThinListByIndex) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    std::array<NUdf::TUnboxedValue, 10U> list = {{
                        NUdf::TUnboxedValuePod(0U),
                        NUdf::TUnboxedValuePod(1U),
                        NUdf::TUnboxedValuePod(2U),
                        NUdf::TUnboxedValuePod(3U),
                        NUdf::TUnboxedValuePod(4U),
                        NUdf::TUnboxedValuePod(5U),
                        NUdf::TUnboxedValuePod(6U),
                        NUdf::TUnboxedValuePod(7U),
                        NUdf::TUnboxedValuePod(8U),
                        NUdf::TUnboxedValuePod(9U)
                    }};
                    return vb.NewList(list.data(), list.size());
                },
                "def Test(v):\n"
                "   e = list(range(0, 10))\n"
                "   assert '__getitem__' in dir(v)\n"
                "   assert v[0] == e[0]\n"
                "   assert v[3] == e[3]\n"
                "   assert v[5] == e[5]\n"
                "   assert v[9] == e[9]\n"
                "   assert v[-1] == e[-1]\n"
                "   assert v[-4] == e[-4]\n"
                "   assert v[-9] == e[-9]\n"
                "   assert v[-10] == e[-10]\n"
        );
    }

    Y_UNIT_TEST(LazyListByIndex) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(0, 10));
                },
                "def Test(v):\n"
                "   e = list(range(0, 10))\n"
                "   assert '__getitem__' in dir(v)\n"
                "   assert v[0] == e[0]\n"
                "   assert v[3] == e[3]\n"
                "   assert v[5] == e[5]\n"
                "   assert v[9] == e[9]\n"
                "   assert v[-1] == e[-1]\n"
                "   assert v[-4] == e[-4]\n"
                "   assert v[-9] == e[-9]\n"
                "   assert v[-10] == e[-10]\n"
        );
    }

    Y_UNIT_TEST(ThinListIndexOutOfBounds) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    std::array<NUdf::TUnboxedValue, 3U> list = {{
                        NUdf::TUnboxedValuePod(0U),
                        NUdf::TUnboxedValuePod(1U),
                        NUdf::TUnboxedValuePod(2U)
                    }};
                    return vb.NewList(list.data(), list.size());
                },
                "def Test(v):\n"
                "   try:\n"
                "       print(v[3])\n"
                "   except IndexError:\n"
                "       pass\n"
                "   else:\n"
                "       assert False\n"
                "   try:\n"
                "       print(v[-4])\n"
                "   except IndexError:\n"
                "       pass\n"
                "   else:\n"
                "       assert False\n"
        );
    }

    Y_UNIT_TEST(LazyListIndexOutOfBounds) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(0, 3));
                },
                "def Test(v):\n"
                "   try:\n"
                "       print(v[3])\n"
                "   except IndexError:\n"
                "       pass\n"
                "   else:\n"
                "       assert False\n"
                "   try:\n"
                "       print(v[-4])\n"
                "   except IndexError:\n"
                "       pass\n"
                "   else:\n"
                "       assert False\n"
        );
    }

    Y_UNIT_TEST(LazyListWithoutLenghNormalSlice) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(0, 10));
                },
                "def Test(v):\n"
                "   e = range(0, 10)\n"
                "   assert '__len__' in dir(v)\n"
                "   assert all(one == two for one, two in zip(iter(v[::1]), e[::1]))\n"
                "   assert all(one == two for one, two in zip(iter(v[::-1]), e[::-1]))\n"
                "   assert all(one == two for one, two in zip(iter(v[4:]), e[4:]))\n"
                "   assert all(one == two for one, two in zip(iter(v[1::-1]), e[1::-1]))\n"
                "   assert all(one == two for one, two in zip(iter(v[:6:1]), e[:6:1]))\n"
                "   assert all(one == two for one, two in zip(iter(v[1::-1]), e[1::-1]))\n"
                "   assert all(one == two for one, two in zip(iter(v[4:11]), e[4:11]))\n"
                "   assert all(one == two for one, two in zip(iter(v[5:1]), e[5:1]))\n"
        );
    }

    Y_UNIT_TEST(ThinListTakeSkip) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    std::array<NUdf::TUnboxedValue, 10U> list = {{
                        NUdf::TUnboxedValuePod(0U),
                        NUdf::TUnboxedValuePod(1U),
                        NUdf::TUnboxedValuePod(2U),
                        NUdf::TUnboxedValuePod(3U),
                        NUdf::TUnboxedValuePod(4U),
                        NUdf::TUnboxedValuePod(5U),
                        NUdf::TUnboxedValuePod(6U),
                        NUdf::TUnboxedValuePod(7U),
                        NUdf::TUnboxedValuePod(8U),
                        NUdf::TUnboxedValuePod(9U)
                    }};
                    return vb.NewList(list.data(), list.size());
                },
                "def Test(v):\n"
                "   e = list(range(0, 10))\n"
                "   assert len(v) == len(e)\n"
                "   assert list(v.skip(5)) == e[5:]\n"
                "   assert list(v.take(5)) == e[0:5]\n"
                "   assert list(v.skip(4).take(5)) == e[4:][:5]\n"
                "   try:\n"
                "       print(list(v.skip(-1)))\n"
                "   except IndexError:\n"
                "       pass\n"
                "   else:\n"
                "       assert False\n"
            );
    }

    Y_UNIT_TEST(LazyListTakeSkip) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<true>(0, 10));
                },
                "def Test(v):\n"
                "   e = list(range(0, 10))\n"
                "   assert list(v.skip(5)) == e[5:]\n"
                "   assert list(v.take(5)) == e[0:5]\n"
                "   assert list(v.skip(4).take(5)) == e[4:][:5]\n"
                "   try:\n"
                "       print(list(v.skip(-1)))\n"
                "   except IndexError:\n"
                "       pass\n"
                "   else:\n"
                "       assert False\n"
            );
    }

    Y_UNIT_TEST(LazyListToIndexDict) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(3, 6));
                },
                "def Test(value):\n"
                "   d = value.to_index_dict()\n"
                "   assert len(d) == 3\n"
                "   assert d[0] == 3\n"
                "   assert d[1] == 4\n"
                "   assert d[2] == 5\n"
                "   assert 3 not in d");
    }

    Y_UNIT_TEST(LazyListTrue) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    NUdf::TUnboxedValue *items = nullptr;
                    return vb.NewArray(1U, items);
                },
                "def Test(value):\n"
                "    assert value\n"
        );
    }

    Y_UNIT_TEST(LazyListFalse) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(0, 0));
                },
                "def Test(value):\n"
                "    assert not value\n"
        );
    }

    Y_UNIT_TEST(ThinListTrue) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(3, 6));
                },
                "def Test(value):\n"
                "    assert value\n"
        );
    }

    Y_UNIT_TEST(ThinListFalse) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type);
                    return vb.NewEmptyList();
                },
                "def Test(value):\n"
                "    assert not value\n"
        );
    }

    Y_UNIT_TEST(LazyListHasItems) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(3, 6));
                },
                "def Test(value):\n"
                "   b = value.has_items()\n"
                "   assert b\n");
    }

    Y_UNIT_TEST(LazyListEmptyHasItems) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(0, 0));
                },
                "def Test(value):\n"
                "   b = value.has_items()\n"
                "   assert not b\n");
    }

    Y_UNIT_TEST(LazyIndexDictContains) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(3, 6));
                },
                "def Test(value):\n"
                "   d = value.to_index_dict()\n"
                "   assert 0 in d\n"
                "   assert 1 in d\n"
                "   assert 2 in d\n"
                "   assert 3 not in d\n"
                "   assert -1 not in d");
    }

    Y_UNIT_TEST(LazyIndexDictIter) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(3, 6));
                },
                "def Test(value):\n"
                "   d = value.to_index_dict()\n"
                "   i, j = 0, 3\n"
                "   for k, v in d.items():\n"
                "       assert i == k\n"
                "       assert j == v\n"
                "       i, j = i+1, j+1");
    }

    Y_UNIT_TEST(LazyIndexDictGet) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TListType<i32>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new NUdf::TLazyList<false>(3, 5));
                },
                "def Test(value):\n"
                "   d = value.to_index_dict()\n"
                "   assert d.get(1) == 4\n"
                "   assert d.get(5) == None\n"
                "   assert d.get(5, 10) == 10\n");
    }

    Y_UNIT_TEST(FromPyGeneratorFactory) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TListType<ui32>>(
                "def first_10():\n"
                "    num = 0\n"
                "    while num < 10:\n"
                "        yield num\n"
                "        num += 1\n"
                "def Test():\n"
                "    return first_10\n",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT(!value.HasFastListLength());
                    UNIT_ASSERT(value.HasListItems());

                    const auto it = value.GetListIterator();
                    ui32 expected = 0;
                    for (NUdf::TUnboxedValue item; it.Next(item);) {
                        ui32 actual = item.Get<ui32>();
                        UNIT_ASSERT_EQUAL(actual, expected);
                        expected++;
                    }

                    UNIT_ASSERT_EQUAL(value.GetEstimatedListLength(), 10);
                    UNIT_ASSERT_EQUAL(value.GetListLength(), 10);
                });
    }

    Y_UNIT_TEST(FromPyIterable) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TListType<ui32>>(
                "def Test():\n"
#if PY_MAJOR_VERSION >= 3
                "    return range(10)\n",
#else
                "    return xrange(10)\n",
#endif
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT(!value.HasFastListLength());
                    UNIT_ASSERT(value.HasListItems());

                    const auto it = value.GetListIterator();
                    ui32 expected = 0U;
                    for (NUdf::TUnboxedValue item; it.Next(item);) {
                        UNIT_ASSERT_EQUAL(item.Get<ui32>(), expected++);
                    }

                    UNIT_ASSERT_EQUAL(value.GetEstimatedListLength(), 10);
                    UNIT_ASSERT_EQUAL(value.GetListLength(), 10);
                    UNIT_ASSERT(value.HasFastListLength());
                });
    }

    Y_UNIT_TEST(FromPyCustomIterable) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TListType<ui32>>(
                "class T:\n"
                "    def __init__(self, l):\n"
                "        self.l = l\n"
                "    def __len__(self):\n"
                "        return len(self.l)\n"
                "    def __nonzero__(self):\n"
                "        return bool(self.l)\n"
                "    def __iter__(self):\n"
                "        return iter(self.l)\n"
                "\n"
                "def Test():\n"
                "    return T([1, 2])\n",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT(value.HasListItems());
                    UNIT_ASSERT_EQUAL(value.GetListLength(), 2);

                    auto it = value.GetListIterator();
                    {
                        NUdf::TUnboxedValue item;
                        it.Next(item);
                        ui32 actual = item.Get<ui32>();
                        UNIT_ASSERT_EQUAL(actual, 1);
                    }
                    {
                        NUdf::TUnboxedValue item;
                        it.Next(item);
                        ui32 actual = item.Get<ui32>();
                        UNIT_ASSERT_EQUAL(actual, 2);
                    }

                    UNIT_ASSERT(false == it.Skip());
                });
    }

    Y_UNIT_TEST(FromPyIterator) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TListType<ui32>>(
                "def Test():\n"
                "    return iter(range(2))\n",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT(false == value.HasFastListLength());

                    auto it = value.GetListIterator();
                    {
                        NUdf::TUnboxedValue item;
                        it.Next(item);
                        ui32 actual = item.Get<ui32>();
                        UNIT_ASSERT_EQUAL(actual, 0);
                    }
                    {
                        NUdf::TUnboxedValue item;
                        it.Next(item);
                        ui32 actual = item.Get<ui32>();
                        UNIT_ASSERT_EQUAL(actual, 1);
                    }

                    UNIT_ASSERT(false == it.Skip());
                });
    }

    Y_UNIT_TEST(FromPyGenerator) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TListType<ui32>>(
                "def Test():\n"
                "    yield 0\n"
                "    yield 1\n",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT(false == value.HasFastListLength());

                    auto it = value.GetListIterator();
                    {
                        NUdf::TUnboxedValue item;
                        it.Next(item);
                        ui32 actual = item.Get<ui32>();
                        UNIT_ASSERT_EQUAL(actual, 0);
                    }
                    {
                        NUdf::TUnboxedValue item;
                        it.Next(item);
                        ui32 actual = item.Get<ui32>();
                        UNIT_ASSERT_EQUAL(actual, 1);
                    }

                    UNIT_ASSERT(false == it.Skip());
                });
    }
}
