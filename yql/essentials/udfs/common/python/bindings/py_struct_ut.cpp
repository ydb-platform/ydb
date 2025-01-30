#include "ut3/py_test_engine.h"

#include <library/cpp/testing/unittest/registar.h>


using namespace NPython;

Y_UNIT_TEST_SUITE(TPyStructTest) {
    Y_UNIT_TEST(FromPyObject) {
        TPythonTestEngine engine;

        ui32 ageIdx = 0, nameIdx = 0;
        auto personType = engine.GetTypeBuilder().Struct()->
                AddField<int>("age", &ageIdx)
                .AddField<char*>("name", &nameIdx)
                .Build();

        engine.ToMiniKQL(personType,
                "class Person:\n"
                "    def __init__(self, age, name):\n"
                "        self.age = age\n"
                "        self.name = name\n"
                "\n"
                "def Test():\n"
                "    return Person(99, 'Jamel')\n",
                [ageIdx, nameIdx](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    auto name = value.GetElement(nameIdx);
                    UNIT_ASSERT_STRINGS_EQUAL(name.AsStringRef(), "Jamel");
                    auto age = value.GetElement(ageIdx);
                    UNIT_ASSERT_EQUAL(age.Get<ui32>(), 99);
                });
    }

    Y_UNIT_TEST(FromPyObjectMissingOptionalField) {
        TPythonTestEngine engine;

        ui32 ageIdx = 0, nameIdx = 0;
        auto optionalStringType = engine.GetTypeBuilder().Optional()->Item<char*>().Build();
        auto personType = engine.GetTypeBuilder().Struct()->
                AddField<int>("age", &ageIdx)
                .AddField("name", optionalStringType, &nameIdx)
                .Build();

        engine.ToMiniKQL(personType,
                "class Person:\n"
                "    def __init__(self, age):\n"
                "        self.age = age\n"
                "\n"
                "def Test():\n"
                "    return Person(99)\n",
                [ageIdx, nameIdx](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    auto name = value.GetElement(nameIdx);
                    UNIT_ASSERT(!name);
                    auto age = value.GetElement(ageIdx);
                    UNIT_ASSERT_EQUAL(age.Get<ui32>(), 99);
                });
    }

    Y_UNIT_TEST(FromPyDict) {
        TPythonTestEngine engine;

        ui32 ageIdx = 0, nameIdx = 0;
        auto personType = engine.GetTypeBuilder().Struct()->
                AddField<int>("age", &ageIdx)
                .AddField<char*>("name", &nameIdx)
                .Build();

        engine.ToMiniKQL(personType,
                "def Test():\n"
                "    return { 'name': 'Jamel', 'age': 99 }\n",
                [ageIdx, nameIdx](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    auto name = value.GetElement(nameIdx);
                    UNIT_ASSERT_STRINGS_EQUAL(name.AsStringRef(), "Jamel");
                    auto age = value.GetElement(ageIdx);
                    UNIT_ASSERT_EQUAL(age.Get<ui32>(), 99);
                });
    }

    Y_UNIT_TEST(FromPyDictMissingOptionalField) {
        TPythonTestEngine engine;

        ui32 ageIdx = 0, nameIdx = 0;
        auto optionalStringType = engine.GetTypeBuilder().Optional()->Item<char*>().Build();
        auto personType = engine.GetTypeBuilder().Struct()->
                AddField<int>("age", &ageIdx)
                .AddField("name", optionalStringType, &nameIdx)
                .Build();

        engine.ToMiniKQL(personType,
                "def Test():\n"
                "    return { 'age': 99 }\n",
                [ageIdx, nameIdx](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    auto name = value.GetElement(nameIdx);
                    UNIT_ASSERT(!name);
                    auto age = value.GetElement(ageIdx);
                    UNIT_ASSERT_EQUAL(age.Get<ui32>(), 99);
                });
    }

    Y_UNIT_TEST(FromPyDictBytesKeyWithNullCharacter) {
        TPythonTestEngine engine;

        ui32 ageIdx = 0;
        auto personType = engine.GetTypeBuilder().Struct()->
                AddField<int>("a\0ge", &ageIdx)
                .Build();

        engine.ToMiniKQL(personType,
                "def Test():\n"
                "    return { b'a\\0ge': 99 }\n",
                [ageIdx](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    auto age = value.GetElement(ageIdx);
                    UNIT_ASSERT_EQUAL(age.Get<ui32>(), 99);
                });
    }

    Y_UNIT_TEST(FromPyNamedTuple) {
        TPythonTestEngine engine;

        ui32 ageIdx = 0, nameIdx = 0;
        auto personType = engine.GetTypeBuilder().Struct()->
                AddField<int>("age", &ageIdx)
                .AddField<char*>("name", &nameIdx)
                .Build();

        engine.ToMiniKQL(personType,
                "from collections import namedtuple\n"
                "def Test():\n"
                "    Person = namedtuple('Person', 'name age')\n"
                "    return Person(age=13, name='Tony')\n",
                [ageIdx, nameIdx](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    auto name = value.GetElement(nameIdx);
                    UNIT_ASSERT_STRINGS_EQUAL(name.AsStringRef(), "Tony");
                    auto age = value.GetElement(ageIdx);
                    UNIT_ASSERT_EQUAL(age.Get<ui32>(), 13);
                });
    }

    Y_UNIT_TEST(FromPyNamedTupleNoneOptionalField) {
        TPythonTestEngine engine;

        ui32 ageIdx = 0, nameIdx = 0;
        auto optionalStringType = engine.GetTypeBuilder().Optional()->Item<char*>().Build();
        auto personType = engine.GetTypeBuilder().Struct()->
                AddField<int>("age", &ageIdx)
                .AddField("name", optionalStringType, &nameIdx)
                .Build();

        engine.ToMiniKQL(personType,
                "from collections import namedtuple\n"
                "def Test():\n"
                "    Pers = namedtuple('Person', 'name age')\n"
                "    return Pers(name=None, age=15)\n",
                [ageIdx, nameIdx](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    auto name = value.GetElement(nameIdx);
                    UNIT_ASSERT(!name);
                    auto age = value.GetElement(ageIdx);
                    UNIT_ASSERT_EQUAL(age.Get<ui32>(), 15);
                });
    }

    Y_UNIT_TEST(FromPyEmptyStruct) {
        TPythonTestEngine engine;
        auto emptyStruct = engine.GetTypeBuilder().Struct()->Build();

        engine.ToMiniKQL(emptyStruct,
                "class Empty: pass\n"
                "\n"
                "def Test():\n"
                "    return Empty()\n",
                [](const NUdf::TUnboxedValuePod&) {});
    }

    Y_UNIT_TEST(ToPyObject) {
        TPythonTestEngine engine;

        ui32 ageIdx = 0, nameIdx = 0, addressIdx = 0, cityIdx = 0, streetIdx = 0, buildingIdx = 0;
        auto addressType = engine.GetTypeBuilder().Struct()->
                AddField<NUdf::TUtf8>("city", &cityIdx)
                .AddField<NUdf::TUtf8>("street", &streetIdx)
                .AddField<ui16>("building", &buildingIdx)
                .Build();

        auto personType = engine.GetTypeBuilder().Struct()->
                AddField<ui16>("age", &ageIdx)
                .AddField<NUdf::TUtf8>("name", &nameIdx)
                .AddField("address", addressType, &addressIdx)
                .Build();


        engine.ToPython(personType,
                [=](const TType* type, const NUdf::IValueBuilder& vb) {
                    NUdf::TUnboxedValue* items = nullptr;
                    auto new_struct = vb.NewArray(static_cast<const TStructType*>(type)->GetMembersCount(), items);
                    items[ageIdx] = NUdf::TUnboxedValuePod(ui16(97));
                    items[nameIdx] = vb.NewString("Jamel");
                    NUdf::TUnboxedValue* items2 = nullptr;
                    items[addressIdx] = vb.NewArray(static_cast<const TStructType*>(static_cast<const TStructType*>(type)->GetMemberType(addressIdx))->GetMembersCount(), items2);
                    items2[cityIdx] = vb.NewString("Moscow");;
                    items2[streetIdx] = vb.NewString("L'va Tolstogo");
                    items2[buildingIdx] = NUdf::TUnboxedValuePod(ui16(16));
                    return new_struct;
                },
                "def Test(value):\n"
                "    assert isinstance(value, object)\n"
                "    assert value.name == 'Jamel'\n"
                "    assert value.age == 97\n"
                "    assert value.address.city == 'Moscow'\n"
                "    assert value.address.building == 16\n"
        );
    }

    Y_UNIT_TEST(ToPyObjectKeywordsAsFields) {
        TPythonTestEngine engine;

        ui32 passIdx = 0, whileIdx = 0, ifIdx = 0, notIdx = 0;
        auto structType = engine.GetTypeBuilder().Struct()->
                AddField<NUdf::TUtf8>("pass", &passIdx)
                .AddField<NUdf::TUtf8>("while", &whileIdx)
                .AddField<NUdf::TUtf8>("if", &ifIdx)
                .AddField<NUdf::TUtf8>("not", &notIdx)
                .Build();

        engine.ToPython(structType,
                [=](const TType* type, const NUdf::IValueBuilder& vb) {
                    NUdf::TUnboxedValue* items = nullptr;
                    auto new_struct = vb.NewArray(static_cast<const TStructType*>(type)->GetMembersCount(), items);
                    items[ifIdx] = vb.NewString("You");
                    items[whileIdx] = vb.NewString("Shall");
                    items[notIdx] = vb.NewString("Not");
                    items[passIdx] = vb.NewString("Pass");
                    return new_struct;
                },
                "def Test(value):\n"
                "    assert getattr(value, 'if') == 'You'\n"
                "    assert getattr(value, 'while') == 'Shall'\n"
                "    assert getattr(value, 'not') == 'Not'\n"
                "    assert getattr(value, 'pass') == 'Pass'\n"
        );
    }

#if PY_MAJOR_VERSION >= 3 // TODO: Fix for python 2
    Y_UNIT_TEST(ToPyObjectTryModify) {
        TPythonTestEngine engine;

        ui32 field1Idx = 0, field2Idx = 0;
        auto structType = engine.GetTypeBuilder().Struct()->
                AddField<NUdf::TUtf8>("field1", &field1Idx)
                .AddField<NUdf::TUtf8>("field2", &field2Idx)
                .Build();

        engine.ToPython(structType,
                [=](const TType* type, const NUdf::IValueBuilder& vb) {
                    NUdf::TUnboxedValue* items = nullptr;
                    auto new_struct = vb.NewArray(static_cast<const TStructType*>(type)->GetMembersCount(), items);
                    items[field1Idx] = NUdf::TUnboxedValuePod::Zero();
                    items[field2Idx] = NUdf::TUnboxedValuePod::Embedded("empty");
                    return new_struct;
                },
                "def Test(value):\n"
                "    try:\n"
                "        setattr(value, 'field1', 17)\n"
                "    except AttributeError:\n"
                "        pass\n"
                "    else:\n"
                "        assert False\n"
                "    try:\n"
                "        value.field2 = 18\n"
                "    except AttributeError:\n"
                "        pass\n"
                "    else:\n"
                "        assert False\n"
        );
    }
#endif

    Y_UNIT_TEST(ToPyObjectEmptyStruct) {
        TPythonTestEngine engine;

        auto personType = engine.GetTypeBuilder().Struct()->Build();

        engine.ToPython(personType,
                [](const TType*, const NUdf::IValueBuilder& vb) {
                    return vb.NewEmptyList();
                },
                "def Test(value):\n"
                "    assert isinstance(value, object)\n"
#if PY_MAJOR_VERSION >= 3
                "    assert len(value) == 0\n"
#endif
        );
    }
}
