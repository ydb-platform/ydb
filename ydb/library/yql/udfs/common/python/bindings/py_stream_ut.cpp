#include "ut3/py_test_engine.h"

#include <library/cpp/testing/unittest/registar.h>


using namespace NPython;

Y_UNIT_TEST_SUITE(TPyStreamTest) {
    void Ui32StreamValidator(const NUdf::TUnboxedValuePod& value) {
        UNIT_ASSERT(value);
        UNIT_ASSERT(value.IsBoxed());

        NUdf::TUnboxedValue item;
        ui32 expected = 0;
        NUdf::EFetchStatus status;

        while (true) {
            status = value.Fetch(item);
            if (status != NUdf::EFetchStatus::Ok) break;

            ui32 actual = item.Get<ui32>();
            UNIT_ASSERT_EQUAL(actual, expected);
            expected++;
        }

        UNIT_ASSERT_EQUAL(status, NUdf::EFetchStatus::Finish);
        UNIT_ASSERT_EQUAL(expected, 10);
    }

    struct TTestStream final: NUdf::TBoxedValue {
        TTestStream(ui32 maxValue, ui32 yieldOn = Max<ui32>())
            : Current_(0)
            , YieldOn_(yieldOn)
            , MaxValue_(maxValue)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Current_ == YieldOn_) {
                return NUdf::EFetchStatus::Yield;
            } else if (Current_ >= MaxValue_) {
                return NUdf::EFetchStatus::Finish;
            }
            result = NUdf::TUnboxedValuePod(Current_++);
            return NUdf::EFetchStatus::Ok;
        }

        ui32 Current_, YieldOn_, MaxValue_;
    };

    Y_UNIT_TEST(FromGenerator) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TStream<ui32>>(
                "def Test():\n"
                "    num = 0\n"
                "    while num < 10:\n"
                "        yield num\n"
                "        num += 1\n",
                Ui32StreamValidator);
    }

    Y_UNIT_TEST(FromGeneratorFactory) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TStream<ui32>>(
                "def first_10():\n"
                "    num = 0\n"
                "    while num < 10:\n"
                "        yield num\n"
                "        num += 1\n"
                "def Test():\n"
                "    return first_10\n",
                Ui32StreamValidator);
    }

    Y_UNIT_TEST(FromIterator) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TStream<ui32>>(
                "def Test():\n"
                "    return iter(range(10))\n",
                Ui32StreamValidator);
    }

    Y_UNIT_TEST(FromIterable) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TStream<ui32>>(
                "def Test():\n"
#if PY_MAJOR_VERSION >= 3
                "    return range(10)\n",
#else
                "    return xrange(10)\n",
#endif
                Ui32StreamValidator);
    }

    Y_UNIT_TEST(FromCustomIterable) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TStream<ui32>>(
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
                "    return T(list(range(10)))\n",
                Ui32StreamValidator);
    }

    Y_UNIT_TEST(FromList) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TStream<ui32>>(
                "def Test():\n"
                "    return [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]\n",
                Ui32StreamValidator);
    }

    Y_UNIT_TEST(ToPython) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TStream<ui32>>(
                [](const TType* /*type*/, const NUdf::IValueBuilder& /*vb*/) {
                    return NUdf::TUnboxedValuePod(new TTestStream(10));
                },
                "def Test(value):\n"
                "    import yql\n"
                "    assert repr(value) == '<yql.TStream>'\n"
                "    assert type(value).__name__ == 'TStream'\n"
                "    assert list(value) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]\n");
    }

    Y_UNIT_TEST(ToPythonAndBackAsIs) {
        TPythonTestEngine engine;
        engine.ToPythonAndBack<NUdf::TStream<ui32>>(
                [](const TType* /*type*/, const NUdf::IValueBuilder& /*vb*/) {
                    return NUdf::TUnboxedValuePod(new TTestStream(10));
                },
                "def Test(value): return value",
                Ui32StreamValidator
        );
    }

    Y_UNIT_TEST(YieldingStreamFromPython) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TStream<ui32>>(
                "import yql\n"
                "def Test():\n"
                "    yield 0\n"
                "    yield 1\n"
                "    yield yql.TYieldIteration\n"
                "    yield 2\n",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());

                    NUdf::TUnboxedValue item;
                    ui32 expected = 0;
                    NUdf::EFetchStatus status;

                    while ((status = value.Fetch(item)) == NUdf::EFetchStatus::Ok) {
                        ui32 actual = item.Get<ui32>();
                        UNIT_ASSERT_EQUAL(actual, expected);
                        expected++;
                    }

                    UNIT_ASSERT_EQUAL(status, NUdf::EFetchStatus::Yield);
                    UNIT_ASSERT_EQUAL(expected, 2);
                });
    }

    Y_UNIT_TEST(YieldingStreamFromCpp) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TStream<ui32>>(
                [](const TType* /*type*/, const NUdf::IValueBuilder& /*vb*/) {
                    return NUdf::TUnboxedValuePod(new TTestStream(5, 2));
                },
                "import yql\n"
                "def Test(value):\n"
                "    assert repr(value) == '<yql.TStream>'\n"
                "    assert type(value).__name__ == 'TStream'\n"
                "    assert next(value) == 0\n"
                "    assert next(value) == 1\n"
                "    try:\n"
                "        next(value)\n"
                "    except yql.TYieldIteration:\n"
                "       pass\n"
                "    else:\n"
                "        assert False, 'Expected yql.TYieldIteration'\n");
    }

    Y_UNIT_TEST(FromCppListIterator) {
        TPythonTestEngine engine;
        engine.ToPythonAndBack<NUdf::TListType<ui32>, NUdf::TStream<ui32>>(
                [](const TType*, const NUdf::IValueBuilder& vb) {
                    NUdf::TUnboxedValue *items = nullptr;
                    const auto a = vb.NewArray(10U, items);
                    ui32 i = 0U;
                    std::generate_n(items, 10U, [&i](){ return NUdf::TUnboxedValuePod(i++); });
                    return a;
                },
                "def Test(value): return iter(value)",
                Ui32StreamValidator
        );
    }
}
