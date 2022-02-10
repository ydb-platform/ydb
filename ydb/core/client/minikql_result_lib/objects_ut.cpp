#include "objects.h"

#include <ydb/core/scheme/scheme_type_id.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

using namespace NKikimrMiniKQL;
using namespace NResultLib;

Y_UNIT_TEST_SUITE(TMiniKQLResultTest) {

Y_UNIT_TEST(TOptionalTest) {
    { // Optional of Bool.
        TType type;
        type.SetKind(ETypeKind::Optional);
        type.MutableOptional()->MutableItem()->SetKind(ETypeKind::Data);
        type.MutableOptional()->MutableItem()->MutableData()->SetScheme(NScheme::NTypeIds::Bool);
        TValue value;
        value.MutableOptional()->SetBool(true);
        TOptional opt(value, type);
        UNIT_ASSERT_EQUAL(opt.GetItem<bool>(), true);
    }
    { // Optional of Uint64.
        TType type;
        type.SetKind(ETypeKind::Optional);
        type.MutableOptional()->MutableItem()->SetKind(ETypeKind::Data);
        type.MutableOptional()->MutableItem()->MutableData()->SetScheme(NScheme::NTypeIds::Uint64);
        TValue value;
        value.MutableOptional()->SetUint64(200100);
        TOptional opt(value, type);
        UNIT_ASSERT_EQUAL(opt.GetItem<ui64>(), 200100);
    }
}

Y_UNIT_TEST(TListTest) {
    { // List of Int32.
        TType type;
        type.SetKind(ETypeKind::List);
        type.MutableList()->MutableItem()->SetKind(ETypeKind::Data);
        type.MutableList()->MutableItem()->MutableData()->SetScheme(NScheme::NTypeIds::Int32);
        TValue value;
        value.AddList()->SetInt32(-100);
        value.AddList()->SetInt32(0);
        value.AddList()->SetInt32(100);

        TListType l(value, type);

        UNIT_ASSERT_EQUAL(l.GetSize(), 3);
        UNIT_ASSERT_EQUAL(l.GetItem<i32>(0), -100);
        UNIT_ASSERT_EQUAL(l.GetItem<i32>(1), 0);
        UNIT_ASSERT_EQUAL(l.GetItem<i32>(2), 100);

        TVector<i32> v;
        for (const auto& listItem : l.MakeIterable<i32>()) {
            v.push_back(listItem);
        }
        UNIT_ASSERT_EQUAL(v.size(), 3);
        UNIT_ASSERT_EQUAL(v[0], -100);
        UNIT_ASSERT_EQUAL(v[1], 0);
        UNIT_ASSERT_EQUAL(v[2], 100);
    }
    { // List of Optional of Int32.
        TType type;
        type.SetKind(ETypeKind::List);
        type.MutableList()->MutableItem()->SetKind(ETypeKind::Optional);
        type.MutableList()->MutableItem()->MutableOptional()->MutableItem()->SetKind(ETypeKind::Data);
        type.MutableList()->MutableItem()->MutableOptional()->MutableItem()->MutableData()->SetScheme(NScheme::NTypeIds::Int32);
        TValue value;
        value.AddList()->MutableOptional()->SetInt32(-100);
        value.AddList()->MutableOptional()->SetInt32(0);
        value.AddList()->MutableOptional()->SetInt32(100);
        TListType l(value, type);

        TVector<i32> v;
        for (const TOptional& opt : l.MakeIterable<TOptional>()) {
            v.push_back(opt.GetItem<i32>());
        }
        UNIT_ASSERT_EQUAL(v.size(), 3);
        UNIT_ASSERT_EQUAL(v[0], -100);
        UNIT_ASSERT_EQUAL(v[1], 0);
        UNIT_ASSERT_EQUAL(v[2], 100);
    }
}


Y_UNIT_TEST(TTupleTest) {
    { // Tuple of (Optional of Int32, List of Uint64).
        TType type;
        type.SetKind(ETypeKind::Tuple);
        auto el0Type = type.MutableTuple()->AddElement();
        el0Type->SetKind(ETypeKind::Optional);
        el0Type->MutableOptional()->MutableItem()->SetKind(ETypeKind::Data);
        el0Type->MutableOptional()->MutableItem()->MutableData()->SetScheme(NScheme::NTypeIds::Int32);

        auto el1Type = type.MutableTuple()->AddElement();
        el1Type->SetKind(ETypeKind::List);
        el1Type->MutableList()->MutableItem()->SetKind(ETypeKind::Data);
        el1Type->MutableList()->MutableItem()->MutableData()->SetScheme(NScheme::NTypeIds::Uint64);

        TValue value;
        value.AddTuple()->MutableOptional()->SetInt32(911);
        auto el1 = value.AddTuple();
        el1->AddList()->SetUint64(0);
        el1->AddList()->SetUint64(1);
        el1->AddList()->SetUint64(2);

        TTuple t(value, type);

        TOptional opt = t.GetElement<TOptional>(0);
        UNIT_ASSERT_EQUAL(opt.GetItem<i32>(), 911);
        TListType l = t.GetElement<TListType>(1);

        TVector<ui64> v;
        for (ui64 item : l.MakeIterable<ui64>()) {
            v.push_back(item);
        }
        UNIT_ASSERT_EQUAL(v.size(), 3);
        UNIT_ASSERT_EQUAL(v[0], 0);
        UNIT_ASSERT_EQUAL(v[1], 1);
        UNIT_ASSERT_EQUAL(v[2], 2);
    }
}

Y_UNIT_TEST(TStructTest) {
    { // Struct of {"a" : Int64, "b" : Int32}.
        TType type;
        type.SetKind(ETypeKind::Struct);
        auto mem1Type = type.MutableStruct()->AddMember();
        mem1Type->SetName("a");
        mem1Type->MutableType()->SetKind(ETypeKind::Data);
        mem1Type->MutableType()->MutableData()->SetScheme(NScheme::NTypeIds::Int64);
        auto mem2Type = type.MutableStruct()->AddMember();
        mem2Type->SetName("b");
        mem2Type->MutableType()->SetKind(ETypeKind::Data);
        mem2Type->MutableType()->MutableData()->SetScheme(NScheme::NTypeIds::Int32);

        TValue value;
        value.AddStruct()->SetInt64(-1000);
        value.AddStruct()->SetInt32(1000);

        TStruct s(value, type);

        UNIT_ASSERT_EQUAL(s.GetMember<i64>("a"), -1000);
        UNIT_ASSERT_EQUAL(s.GetMember<i32>("b"), 1000);

        auto aIndex = s.GetMemberIndex("a");
        auto bIndex = s.GetMemberIndex("b");
        UNIT_ASSERT_EQUAL(s.GetMember<i64>(aIndex), -1000);
        UNIT_ASSERT_EQUAL(s.GetMember<i32>(bIndex), 1000);
    }
}

}

} // namespace NKikimr
