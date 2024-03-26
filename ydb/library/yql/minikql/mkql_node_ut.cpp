#include "mkql_node.h"
#include "mkql_node_visitor.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>


namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLNodeTest) {
    Y_UNIT_TEST(TestTypeOfType) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        {
            auto type = env.GetTypeOfTypeLazy();
            UNIT_ASSERT(type->IsType());
            UNIT_ASSERT(type->GetType() == type);
            UNIT_ASSERT(type->IsSameType(*type));
        }
    }

    Y_UNIT_TEST(TestTypeOfVoid) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        {
            auto type = env.GetTypeOfVoidLazy();
            UNIT_ASSERT(type->IsVoid());
            UNIT_ASSERT(type->GetType() == env.GetTypeOfTypeLazy());
            UNIT_ASSERT(type->IsSameType(*type));
        }
    }

    Y_UNIT_TEST(TestVoid) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TVoid* voidObj = env.GetVoidLazy();
        UNIT_ASSERT(voidObj->GetType()->IsVoid());
        UNIT_ASSERT(voidObj->GetType() == env.GetTypeOfVoidLazy());
    }

    Y_UNIT_TEST(TestTypeOfData) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TDataType* dtype1 = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
        UNIT_ASSERT_EQUAL(dtype1->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(dtype1->GetSchemeType(), NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(dtype1->IsSameType(*dtype1));

        UNIT_ASSERT_EXCEPTION(TDataType::Create(0, env), yexception);

        TDataType* dtype1Cloned = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
        UNIT_ASSERT(dtype1Cloned->IsSameType(*dtype1));
    }

    Y_UNIT_TEST(TestData) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TDataType* dtype1 = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
        ui32 u = 34567;

        auto d1 = TDataLiteral::Create(NUdf::TUnboxedValuePod(u), dtype1, env);
        UNIT_ASSERT(memcmp(d1->AsValue().AsStringRef().Data(), &u, sizeof(u)) == 0);
        UNIT_ASSERT(d1->GetType() == dtype1);
    }

    Y_UNIT_TEST(TestStructType) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TStructType* sempty = env.GetEmptyStructLazy()->GetType();
        UNIT_ASSERT_EQUAL(sempty->GetKind(), TType::EKind::Struct);
        UNIT_ASSERT_EQUAL(sempty->GetMembersCount(), 0);

        TVector<std::pair<TString, TType*>> s1members;
        s1members.push_back(std::make_pair("aaa", env.GetVoidLazy()->GetGenericType()));
        TStructType* s1 = TStructType::Create(s1members.data(), s1members.size(), env);
        UNIT_ASSERT_EQUAL(s1->GetMembersCount(), 1);
        UNIT_ASSERT_EQUAL(s1->GetMemberName(0), "aaa");
        UNIT_ASSERT_EQUAL(s1->GetMemberType(0)->GetKind(), TType::EKind::Void);

        TVector<std::pair<TString, TType*>> s2members;
        s2members.push_back(std::make_pair("bbb", env.GetEmptyStructLazy()->GetGenericType()));
        s2members.push_back(std::make_pair("ccc", env.GetTypeOfVoidLazy()));
        TStructType* s2 = TStructType::Create(s2members.data(), s2members.size(), env);
        UNIT_ASSERT_EQUAL(s2->GetMembersCount(), 2);
        UNIT_ASSERT_EQUAL(s2->GetMemberName(0), "bbb");
        UNIT_ASSERT_EQUAL(s2->GetMemberType(0)->GetKind(), TType::EKind::Struct);
        UNIT_ASSERT_EQUAL(s2->GetMemberName(1), "ccc");
        UNIT_ASSERT_EQUAL(s2->GetMemberType(1)->GetKind(), TType::EKind::Void);

        TVector<std::pair<TString, TType*>> s3members;
        s3members.push_back(std::make_pair("", env.GetEmptyStructLazy()->GetGenericType()));
        UNIT_ASSERT_EXCEPTION(TStructType::Create(s3members.data(), s3members.size(), env), yexception);

        TStructType* s2cloned = TStructType::Create(s2members.data(), s2members.size(), env);
        UNIT_ASSERT(s2cloned->IsSameType(*s2cloned));
        UNIT_ASSERT(s2cloned->IsSameType(*s2));
        UNIT_ASSERT(!s2cloned->IsSameType(*s1));

        Reverse(s2members.begin(), s2members.end());
        UNIT_ASSERT_EXCEPTION(TStructType::Create(s2members.data(), s2members.size(), env), yexception);

        TVector<std::pair<TString, TType*>> duplicatedMembers;
        duplicatedMembers.push_back(std::make_pair("aaa", env.GetEmptyStructLazy()->GetGenericType()));
        duplicatedMembers.push_back(std::make_pair("aaa", env.GetTypeOfVoidLazy()));
        UNIT_ASSERT_EXCEPTION(TStructType::Create(duplicatedMembers.data(), duplicatedMembers.size(), env), yexception);
    }

    Y_UNIT_TEST(TestStructLiteral) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TStructType* semptyType = env.GetEmptyStructLazy()->GetType();
        TStructLiteral* sempty = TStructLiteral::Create(0, nullptr, semptyType, env);
        UNIT_ASSERT_EQUAL(sempty->GetValuesCount(), 0);

        TVector<std::pair<TString, TType*>> s1members;
        s1members.push_back(std::make_pair("aaa", env.GetVoidLazy()->GetGenericType()));
        TStructType* s1type = TStructType::Create(s1members.data(), s1members.size(), env);

        TVector<TRuntimeNode> s1values;
        s1values.push_back(TRuntimeNode(env.GetVoidLazy(), true));
        TStructLiteral* s1 = TStructLiteral::Create(s1values.size(), s1values.data(), s1type, env);
        UNIT_ASSERT_EQUAL(s1->GetValuesCount(), 1);
        UNIT_ASSERT_EQUAL(s1->GetValue(0).GetNode(), s1values[0].GetNode());

        UNIT_ASSERT_EXCEPTION(TStructLiteral::Create(s1values.size(), s1values.data(), semptyType, env), yexception);

        TVector<std::pair<TString, TType*>> s2members;
        s2members.push_back(std::make_pair("aaa", env.GetEmptyTupleLazy()->GetGenericType()));
        TStructType* s2type = TStructType::Create(s2members.data(), s2members.size(), env);
        UNIT_ASSERT_EXCEPTION(TStructLiteral::Create(s1values.size(), s1values.data(), s2type, env), yexception);

        TStructType* s1typeDyn = TStructType::Create(s1members.data(), s1members.size(), env);
        TVector<TRuntimeNode> s1DynValues;
        TCallableType* ctype = TCallableType::Create("c1", env.GetVoidLazy()->GetGenericType(),
            0, nullptr, nullptr, env);
        s1DynValues.push_back(TRuntimeNode(TCallable::Create(0, nullptr, ctype, env), false));
        UNIT_ASSERT_NO_EXCEPTION(TStructLiteral::Create(s1DynValues.size(), s1DynValues.data(), s1typeDyn, env));
        UNIT_ASSERT_NO_EXCEPTION(TStructLiteral::Create(s1values.size(), s1values.data(), s1typeDyn, env));
    }

    Y_UNIT_TEST(TestListType) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TListType* list1type = TListType::Create(env.GetVoidLazy()->GetGenericType(), env);
        UNIT_ASSERT_EQUAL(list1type->GetKind(), TType::EKind::List);
        TListType* list2type = TListType::Create(env.GetEmptyTupleLazy()->GetGenericType(), env);
        TListType* list1typeCloned = TListType::Create(env.GetVoidLazy()->GetGenericType(), env);
        UNIT_ASSERT(list1type->IsSameType(*list1typeCloned));
        UNIT_ASSERT(!list1type->IsSameType(*list2type));
    }

    Y_UNIT_TEST(TestStreamType) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TStreamType* stream1type = TStreamType::Create(env.GetVoidLazy()->GetGenericType(), env);
        UNIT_ASSERT_EQUAL(stream1type->GetKind(), TType::EKind::Stream);
        TStreamType* stream2type = TStreamType::Create(env.GetEmptyTupleLazy()->GetGenericType(), env);
        TStreamType* stream1typeCloned = TStreamType::Create(env.GetVoidLazy()->GetGenericType(), env);
        UNIT_ASSERT(stream1type->IsSameType(*stream1typeCloned));
        UNIT_ASSERT(!stream1type->IsSameType(*stream2type));
    }

    Y_UNIT_TEST(TestListLiteral) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TDataType* dtype1 = TDataType::Create(NUdf::TDataType<char*>::Id, env);
        TListType* list1type = TListType::Create(env.GetVoidLazy()->GetGenericType(), env);
        TListType* list2type = TListType::Create(dtype1, env);

        UNIT_ASSERT_NO_EXCEPTION(TListLiteral::Create(nullptr, 0, list1type, env));

        TString u = "34567";
        TVector<TRuntimeNode> someItems;
        someItems.push_back(TRuntimeNode(TDataLiteral::Create(
            NUdf::TUnboxedValue::Embedded(u), dtype1, env), true));
        u = "878";
        someItems.push_back(TRuntimeNode(TDataLiteral::Create(
            NUdf::TUnboxedValue::Embedded(u), dtype1, env), true));
        TListLiteral* list2 = TListLiteral::Create(someItems.data(), someItems.size(), list2type, env);
        UNIT_ASSERT_EQUAL(list2->GetItemsCount(), 2);
        UNIT_ASSERT_EQUAL(list2->GetItems()[0].GetNode()->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(static_cast<const TDataLiteral&>(*list2->GetItems()[0].GetNode()).AsValue().AsStringRef(), NUdf::TStringRef::Of("34567"));
        UNIT_ASSERT_EQUAL(list2->GetItems()[1].GetNode()->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(static_cast<const TDataLiteral&>(*list2->GetItems()[1].GetNode()).AsValue().AsStringRef(), NUdf::TStringRef::Of("878"));

        UNIT_ASSERT_EXCEPTION(TListLiteral::Create(someItems.data(), someItems.size(), list1type, env), yexception);

        TListType* list2typeDyn = TListType::Create(dtype1, env);
        UNIT_ASSERT_NO_EXCEPTION(TListLiteral::Create(someItems.data(), someItems.size(), list2typeDyn, env));

        TVector<TRuntimeNode> someDynItems;
        TCallableType* ctype = TCallableType::Create("c1", dtype1, 0, nullptr, nullptr, env);
        someDynItems.push_back(TRuntimeNode(TCallable::Create(0, nullptr, ctype, env), false));

        UNIT_ASSERT_NO_EXCEPTION(TListLiteral::Create(someDynItems.data(), someDynItems.size(), list2typeDyn, env));
    }

    Y_UNIT_TEST(TestOptionalType) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TOptionalType* opt1type = TOptionalType::Create(env.GetVoidLazy()->GetGenericType(), env);
        UNIT_ASSERT_EQUAL(opt1type->GetKind(), TType::EKind::Optional);
        TOptionalType* opt2type = TOptionalType::Create(env.GetEmptyTupleLazy()->GetGenericType(), env);
        TOptionalType* opt1typeCloned = TOptionalType::Create(env.GetVoidLazy()->GetGenericType(), env);
        UNIT_ASSERT(opt1type->IsSameType(*opt1typeCloned));
        UNIT_ASSERT(!opt1type->IsSameType(*opt2type));
    }

    Y_UNIT_TEST(TestOptionalLiteral) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TDataType* dtype1 = TDataType::Create(NUdf::TDataType<char*>::Id, env);
        TOptionalType* opt1type = TOptionalType::Create(env.GetVoidLazy()->GetGenericType(), env);
        TOptionalType* opt2type = TOptionalType::Create(dtype1, env);

        TOptionalLiteral* emptyOpt = TOptionalLiteral::Create(opt1type, env);
        UNIT_ASSERT(!emptyOpt->HasItem());

        TString u = "34567";
        auto item1 = TRuntimeNode(TDataLiteral::Create(
            NUdf::TUnboxedValuePod::Embedded(u), dtype1, env), true);

        TOptionalLiteral* opt1 = TOptionalLiteral::Create(item1, opt2type, env);
        UNIT_ASSERT(opt1->HasItem());
        UNIT_ASSERT_EQUAL(opt1->GetItem().GetNode()->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(static_cast<const TDataLiteral&>(*opt1->GetItem().GetNode()).AsValue().AsStringRef(), NUdf::TStringRef::Of("34567"));

        UNIT_ASSERT_EXCEPTION(TOptionalLiteral::Create(item1, opt1type, env), yexception);

        auto opt2typeDyn = TOptionalType::Create(dtype1, env);
        UNIT_ASSERT_NO_EXCEPTION(TOptionalLiteral::Create(item1, opt2typeDyn, env));

        TCallableType* ctype = TCallableType::Create("c1", dtype1, 0, nullptr, nullptr, env);
        auto dynItem = TRuntimeNode(TCallable::Create(0, nullptr, ctype, env), false);

        UNIT_ASSERT_NO_EXCEPTION(TOptionalLiteral::Create(dynItem, opt2typeDyn, env));
    }

    Y_UNIT_TEST(TestDictType) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TDataType* dtype1 = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
        TDictType* dictType1 = TDictType::Create(dtype1, env.GetVoidLazy()->GetGenericType(), env);
        UNIT_ASSERT_EQUAL(dictType1->GetKind(), TType::EKind::Dict);
        TDictType* dictType2 = TDictType::Create(dtype1, env.GetEmptyStructLazy()->GetGenericType(), env);
        TDictType* dictType1Cloned = TDictType::Create(dtype1, env.GetVoidLazy()->GetGenericType(), env);
        UNIT_ASSERT(dictType1->IsSameType(*dictType1Cloned));
        UNIT_ASSERT(!dictType1->IsSameType(*dictType2));
    }

    Y_UNIT_TEST(TestCallableType) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TCallableType* ctype1 = TCallableType::Create("c1", env.GetTypeOfVoidLazy(),
            0, nullptr, nullptr, env);
        UNIT_ASSERT_EQUAL(ctype1->GetKind(), TType::EKind::Callable);
        UNIT_ASSERT_EQUAL(ctype1->GetName(), "c1");
        UNIT_ASSERT_EQUAL(ctype1->GetArgumentsCount(), 0);
        UNIT_ASSERT_EQUAL(ctype1->GetReturnType()->GetKind(), TType::EKind::Void);

        TVector<TType*> types;
        types.push_back(env.GetVoidLazy()->GetGenericType());
        types.push_back(env.GetEmptyStructLazy()->GetGenericType());
        TCallableType* ctype2 = TCallableType::Create("c2", env.GetVoidLazy()->GetGenericType(), types.size(), types.data(), nullptr, env);
        UNIT_ASSERT_EQUAL(ctype2->GetName(), "c2");
        UNIT_ASSERT_EQUAL(ctype2->GetReturnType()->GetKind(), TType::EKind::Void);
        UNIT_ASSERT_EQUAL(ctype2->GetArgumentsCount(), 2);
        UNIT_ASSERT_EQUAL(ctype2->GetArgumentType(0)->GetKind(), TType::EKind::Void);
        UNIT_ASSERT_EQUAL(ctype2->GetArgumentType(1)->GetKind(), TType::EKind::Struct);

        TCallableType* ctype2Cloned = TCallableType::Create("c2", env.GetVoidLazy()->GetGenericType(), types.size(), types.data(), nullptr, env);
        UNIT_ASSERT(ctype2->IsSameType(*ctype2));
        UNIT_ASSERT(ctype2->IsSameType(*ctype2Cloned));
        UNIT_ASSERT(!ctype2->IsSameType(*ctype1));

        TVector<TType*> types2;
        types2.push_back(env.GetEmptyStructLazy()->GetGenericType());
        types2.push_back(env.GetVoidLazy()->GetGenericType());
        TCallableType* ctype2rev = TCallableType::Create("c2", env.GetVoidLazy()->GetGenericType(), types2.size(), types2.data(), nullptr, env);
        UNIT_ASSERT(!ctype2->IsSameType(*ctype2rev));

        TVector<TType*> types3;
        types3.push_back(env.GetVoidLazy()->GetGenericType());
        types3.push_back(env.GetEmptyStructLazy()->GetGenericType());
        TCallableType* ctype2withPayload1 = TCallableType::Create("c2", env.GetVoidLazy()->GetGenericType(), types.size(), types.data(), env.GetEmptyStructLazy(), env);
        UNIT_ASSERT(!ctype2withPayload1->IsSameType(*ctype2));
        TCallableType* ctype2withPayload2 = TCallableType::Create("c2", env.GetVoidLazy()->GetGenericType(), types.size(), types.data(), env.GetListOfVoidLazy(), env);
        UNIT_ASSERT(!ctype2withPayload2->IsSameType(*ctype2withPayload1));
        TCallableType* ctype2withPayload2clone = TCallableType::Create("c2", env.GetVoidLazy()->GetGenericType(), types.size(), types.data(), env.GetListOfVoidLazy(), env);
        UNIT_ASSERT(ctype2withPayload2clone->IsSameType(*ctype2withPayload2));

        TCallableType* ctype2optArg = TCallableType::Create("c2", env.GetVoidLazy()->GetGenericType(), types.size(), types.data(), nullptr, env);

        ctype2optArg->SetOptionalArgumentsCount(0);
        UNIT_ASSERT(ctype2optArg->IsSameType(*ctype2));
        UNIT_ASSERT(ctype2optArg->IsConvertableTo(*ctype2));
        UNIT_ASSERT(ctype2->IsSameType(*ctype2optArg));
        UNIT_ASSERT(ctype2->IsConvertableTo(*ctype2optArg));

        UNIT_ASSERT_EXCEPTION(ctype2optArg->SetOptionalArgumentsCount(1), yexception);
        UNIT_ASSERT_EXCEPTION(ctype2optArg->SetOptionalArgumentsCount(3), yexception);
    }

    template<typename... T>
    struct TArgs { };

    template<typename... T>
    struct TOpt { };

    template<>
    struct TArgs<> {
        void Build(bool inc, TVector<TType*>& types, int& opts, TTypeEnvironment& env) const {
            Y_UNUSED(inc);
            Y_UNUSED(types);
            Y_UNUSED(opts);
            Y_UNUSED(env);
        }
    };
    
    template<typename... T>
    struct TArgs<TOpt<T...>>
    {
        void Build(bool, TVector<TType*>& types, int& opts, TTypeEnvironment& env) const {
            TArgs<T...>().Build(true, types, opts, env);
        }
    };

    template<typename H, typename... T>
    struct TArgs<H, T...>
    {
        void Build(bool inc, TVector<TType*>& types, int& opts, TTypeEnvironment& env) const {
            if (inc) {
                opts ++;
            }
            auto* type = TOptionalType::Create(env.GetEmptyTupleLazy()->GetGenericType(), env);
            types.push_back(type);
            TArgs<T...>().Build(inc, types, opts, env);
        }
    };

    template<typename... T1, typename... T2>
    void IsConvertableTo(const TArgs<T1...>& f1, const TArgs<T2...>& f2, TTypeEnvironment& env, bool result) {
        TVector<TType*> types1; int opts1 = 0;
        TVector<TType*> types2; int opts2 = 0;
        f1.Build(false, types1, opts1, env);
        f2.Build(false, types2, opts2, env);

        TCallableType* c1 = TCallableType::Create("c1", env.GetVoidLazy()->GetGenericType(), types1.size(), types1.data(), nullptr, env);
        c1->SetOptionalArgumentsCount(opts1);

        TCallableType* c2 = TCallableType::Create("c2", env.GetVoidLazy()->GetGenericType(), types2.size(), types2.data(), nullptr, env);
        c2->SetOptionalArgumentsCount(opts2);

        UNIT_ASSERT(c1->IsConvertableTo(*c2) == result);
    }

    Y_UNIT_TEST(TestCallableTypeWithOptionalArguments) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);

        IsConvertableTo(TArgs<TOpt<int,int,int>>(), TArgs<int,int,int>(), env, true);
        IsConvertableTo(TArgs<int,int>(), TArgs<int,int>(), env, true);
        IsConvertableTo(TArgs<int,int>(), TArgs<int,TOpt<int>>(), env, false);
        IsConvertableTo(TArgs<int,TOpt<int>>(), TArgs<int,int>(), env, true);
        IsConvertableTo(TArgs<TOpt<int,int>>(), TArgs<int,int>(), env, true);
        IsConvertableTo(TArgs<TOpt<int,int,int>>(), TArgs<int,TOpt<int>>(), env, true);
        IsConvertableTo(TArgs<TOpt<int,int>>(), TArgs<int,TOpt<int,int>>(), env, false);
        IsConvertableTo(TArgs<int,int,int>(), TArgs<int,TOpt<int>>(), env, false);
    }

    Y_UNIT_TEST(TestDictLiteral) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TDataType* dtype1 = TDataType::Create(NUdf::TDataType<char*>::Id, env);
        TDataType* dtype2 = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
        TDictType* dict1Type = TDictType::Create(dtype1, env.GetVoidLazy()->GetGenericType(), env);
        TDictType* dict2Type = TDictType::Create(dtype1, dtype2, env);

        TVector<std::pair<TRuntimeNode, TRuntimeNode>> emptyItems;
        UNIT_ASSERT_NO_EXCEPTION(TDictLiteral::Create(emptyItems.size(), emptyItems.data(), dict1Type, env));

        TString u = "34567";
        TVector<std::pair<TRuntimeNode, TRuntimeNode>> someItems;
        someItems.push_back(std::make_pair(TRuntimeNode(TDataLiteral::Create(
            NUdf::TUnboxedValuePod::Embedded(u), dtype1, env), true),
            TRuntimeNode(TDataLiteral::Create(
            NUdf::TUnboxedValuePod((ui32)13), dtype2, env), true)));
        u = "878";
        someItems.push_back(std::make_pair(TRuntimeNode(TDataLiteral::Create(
            NUdf::TUnboxedValuePod::Embedded(u), dtype1, env), true),
            TRuntimeNode(TDataLiteral::Create(NUdf::TUnboxedValuePod((ui32)14), dtype2, env), true)));
        TDictLiteral* dict2 = TDictLiteral::Create(someItems.size(), someItems.data(), dict2Type, env);
        UNIT_ASSERT_EQUAL(dict2->GetItemsCount(), 2);
        UNIT_ASSERT_EQUAL(dict2->GetItem(0).first.GetNode()->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(static_cast<const TDataLiteral&>(*dict2->GetItem(0).first.GetNode()).AsValue().AsStringRef(), NUdf::TStringRef::Of("34567"));
        UNIT_ASSERT_EQUAL(dict2->GetItem(0).second.GetNode()->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(static_cast<const TDataLiteral&>(*dict2->GetItem(0).second.GetNode()).AsValue().Get<ui32>(), 13);
        UNIT_ASSERT_EQUAL(dict2->GetItem(1).first.GetNode()->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(static_cast<const TDataLiteral&>(*dict2->GetItem(1).first.GetNode()).AsValue().AsStringRef(), NUdf::TStringRef::Of("878"));
        UNIT_ASSERT_EQUAL(dict2->GetItem(1).second.GetNode()->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(static_cast<const TDataLiteral&>(*dict2->GetItem(1).second.GetNode()).AsValue().Get<ui32>(), 14);

        UNIT_ASSERT_EXCEPTION(TDictLiteral::Create(someItems.size(), someItems.data(), dict1Type, env), yexception);

        TDictType* dict2TypeDyn = TDictType::Create(dtype1, dtype2, env);
        UNIT_ASSERT_NO_EXCEPTION(TDictLiteral::Create(someItems.size(), someItems.data(), dict2TypeDyn, env));

        TVector<std::pair<TRuntimeNode, TRuntimeNode>> someDynItems;
        TCallableType* ctype = TCallableType::Create("c1", dtype1, 0, nullptr, nullptr, env);
        someDynItems.push_back(std::make_pair(
            TRuntimeNode(TCallable::Create(0, nullptr, ctype, env), false),
            TRuntimeNode(TDataLiteral::Create(NUdf::TUnboxedValuePod((ui32)123), dtype2, env), true)));

        UNIT_ASSERT_NO_EXCEPTION(TDictLiteral::Create(someDynItems.size(), someDynItems.data(), dict2TypeDyn, env));
    }

    Y_UNIT_TEST(TestCallable) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TCallableType* ctype1 = TCallableType::Create("c1", env.GetTypeOfVoidLazy(),
            0, nullptr, nullptr, env);
        TCallable* c1 = TCallable::Create(0, nullptr, ctype1, env);
        UNIT_ASSERT_EQUAL(c1->GetInputsCount(), 0);
        UNIT_ASSERT(!c1->HasResult());
        c1->SetResult(TRuntimeNode(env.GetVoidLazy(), true), env);
        UNIT_ASSERT(c1->HasResult());
        UNIT_ASSERT_EQUAL(c1->GetResult().GetStaticType()->GetKind(), TType::EKind::Void);

        TVector<TType*> ctype2args;
        TDataType* dtype1 = TDataType::Create(NUdf::TDataType<char*>::Id, env);
        ctype2args.push_back(dtype1);
        ctype2args.push_back(dtype1);
        TCallableType* ctype2 = TCallableType::Create("c2", env.GetTypeOfVoidLazy(), ctype2args.size(), ctype2args.data(), nullptr, env);
        TCallableType* ctype2cloned = TCallableType::Create("c2", env.GetTypeOfVoidLazy(), ctype2args.size(), ctype2args.data(), nullptr, env);

        UNIT_ASSERT(ctype2cloned->IsSameType(*ctype2));
        UNIT_ASSERT(!ctype2cloned->IsSameType(*ctype1));

        TCallableType* ctype3 = TCallableType::Create("c1", dtype1, 0, nullptr, nullptr, env);

        TVector<TRuntimeNode> c2args;
        c2args.push_back(TRuntimeNode(TDataLiteral::Create(
            NUdf::TUnboxedValuePod::Embedded("abc"), dtype1, env), true));
        c2args.push_back(TRuntimeNode(TCallable::Create(0, nullptr, ctype3, env), false));
        TCallable* c2 = TCallable::Create(c2args.size(), c2args.data(), ctype2, env);
        UNIT_ASSERT_EQUAL(c2->GetInputsCount(), 2);
        UNIT_ASSERT_EQUAL(c2->GetInput(0).IsImmediate(), true);
        UNIT_ASSERT_EQUAL(c2->GetInput(0).GetNode()->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(c2->GetInput(1).IsImmediate(), false);
        UNIT_ASSERT_EQUAL(c2->GetInput(1).GetNode()->GetType()->GetKind(), TType::EKind::Callable);
        UNIT_ASSERT(!c2->HasResult());
        c2->SetResult(TRuntimeNode(env.GetVoidLazy(), true), env);
        UNIT_ASSERT(c2->HasResult());
        UNIT_ASSERT_EQUAL(c2->GetResult().GetStaticType()->GetKind(), TType::EKind::Void);
    }

    Y_UNIT_TEST(TestThrowingVisitor) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TThrowingNodeVisitor visitor;

        UNIT_ASSERT_EXCEPTION(visitor.Visit(*env.GetTypeOfTypeLazy()), yexception);
    }

    Y_UNIT_TEST(TestEmptyVisitor) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TEmptyNodeVisitor visitor;

        visitor.Visit(*env.GetTypeOfTypeLazy());
    }

    Y_UNIT_TEST(TesAnyType) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        UNIT_ASSERT(env.GetAnyTypeLazy()->IsSameType(*env.GetAnyTypeLazy()));
    }

    Y_UNIT_TEST(TestAny) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TAny* emptyAny = TAny::Create(env);
        UNIT_ASSERT(!emptyAny->HasItem());
        TAny* emptyAny2 = TAny::Create(env);
        UNIT_ASSERT(!emptyAny->TNode::Equals(*emptyAny2));

        TDataType* dtype1 = TDataType::Create(NUdf::TDataType<char*>::Id, env);
        TString u = "34567";
        auto item1 = TRuntimeNode(TDataLiteral::Create(
            NUdf::TUnboxedValuePod::Embedded(u), dtype1, env), true);

        TAny* any1 = TAny::Create(env);
        any1->SetItem(item1);
        UNIT_ASSERT(any1->HasItem());
        UNIT_ASSERT_EQUAL(any1->GetItem().GetNode()->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(static_cast<const TDataLiteral&>(*any1->GetItem().GetNode()).AsValue().AsStringRef(), NUdf::TStringRef::Of("34567"));

        auto item2 = TRuntimeNode(TDataLiteral::Create(
            NUdf::TUnboxedValuePod::Embedded(u), dtype1, env), true);
        TAny* any2 = TAny::Create(env);
        any2->SetItem(item2);

        UNIT_ASSERT(any1->TNode::Equals(*any2));
    }

    Y_UNIT_TEST(TestTupleType) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TTupleType* tempty = env.GetEmptyTupleLazy()->GetType();
        UNIT_ASSERT_EQUAL(tempty->GetKind(), TType::EKind::Tuple);
        UNIT_ASSERT_EQUAL(tempty->GetElementsCount(), 0);

        TVector<TType*> t1elems;
        t1elems.push_back(env.GetVoidLazy()->GetGenericType());
        TTupleType* t1 = TTupleType::Create(t1elems.size(), t1elems.data(), env);
        UNIT_ASSERT_EQUAL(t1->GetElementsCount(), 1);
        UNIT_ASSERT_EQUAL(t1->GetElementType(0)->GetKind(), TType::EKind::Void);

        TVector<TType*> t2elems;
        t2elems.push_back(env.GetEmptyStructLazy()->GetGenericType());
        t2elems.push_back(env.GetTypeOfVoidLazy());
        TTupleType* t2 = TTupleType::Create(t2elems.size(), t2elems.data(), env);
        UNIT_ASSERT_EQUAL(t2->GetElementsCount(), 2);
        UNIT_ASSERT_EQUAL(t2->GetElementType(0)->GetKind(), TType::EKind::Struct);
        UNIT_ASSERT_EQUAL(t2->GetElementType(1)->GetKind(), TType::EKind::Void);

        TTupleType* t2cloned = TTupleType::Create(t2elems.size(), t2elems.data(), env);
        UNIT_ASSERT(t2cloned->IsSameType(*t2cloned));
        UNIT_ASSERT(t2cloned->IsSameType(*t2));
        UNIT_ASSERT(!t2cloned->IsSameType(*t1));
    }

    Y_UNIT_TEST(TestTupleLiteral) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TTupleType* temptyType = env.GetEmptyTupleLazy()->GetType();
        TTupleLiteral* tempty = TTupleLiteral::Create(0, nullptr, temptyType, env);
        UNIT_ASSERT_EQUAL(tempty->GetValuesCount(), 0);

        TVector<TType*> t1elems;
        t1elems.push_back(env.GetVoidLazy()->GetGenericType());
        TTupleType* t1type = TTupleType::Create(t1elems.size(), t1elems.data(), env);

        TVector<TRuntimeNode> t1values;
        t1values.push_back(TRuntimeNode(env.GetVoidLazy(), true));
        TTupleLiteral* t1 = TTupleLiteral::Create(t1values.size(), t1values.data(), t1type, env);
        UNIT_ASSERT_EQUAL(t1->GetValuesCount(), 1);
        UNIT_ASSERT_EQUAL(t1->GetValue(0).GetNode(), t1values[0].GetNode());

        UNIT_ASSERT_EXCEPTION(TTupleLiteral::Create(t1values.size(), t1values.data(), temptyType, env), yexception);

        TVector<TType*> t2elems;
        t2elems.push_back(env.GetEmptyStructLazy()->GetGenericType());
        TTupleType* t2type = TTupleType::Create(t2elems.size(), t2elems.data(), env);
        UNIT_ASSERT_EXCEPTION(TTupleLiteral::Create(t1values.size(), t1values.data(), t2type, env), yexception);

        TTupleType* t1typeDyn = TTupleType::Create(t1elems.size(), t1elems.data(), env);
        TVector<TRuntimeNode> t1DynValues;
        TCallableType* ctype = TCallableType::Create("c1", env.GetVoidLazy()->GetGenericType(),
            0, nullptr, nullptr, env);
        t1DynValues.push_back(TRuntimeNode(TCallable::Create(0, nullptr, ctype, env), false));
        UNIT_ASSERT_NO_EXCEPTION(TTupleLiteral::Create(t1DynValues.size(), t1DynValues.data(), t1typeDyn, env));
        UNIT_ASSERT_NO_EXCEPTION(TTupleLiteral::Create(t1values.size(), t1values.data(), t1typeDyn, env));
    }

    Y_UNIT_TEST(TestVariantType) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto emptyStructType = env.GetEmptyStructLazy()->GetType();
        auto emptyTupleType = env.GetEmptyTupleLazy()->GetType();
        UNIT_ASSERT_EXCEPTION(TVariantType::Create(emptyStructType, env), yexception);
        UNIT_ASSERT_EXCEPTION(TVariantType::Create(emptyTupleType, env), yexception);

        // one element tuple
        TType* elements[2] = {
            TDataType::Create(NUdf::TDataType<i32>::Id, env),
            emptyStructType
        };

        auto tuple1type = TTupleType::Create(1, elements, env);
        auto var1tuple = TVariantType::Create(tuple1type, env);
        UNIT_ASSERT_VALUES_EQUAL(var1tuple->GetAlternativesCount(), 1);
        UNIT_ASSERT(var1tuple->GetUnderlyingType() == tuple1type);
        // two elements tuple
        auto tuple2type = TTupleType::Create(2, elements, env);
        auto var2tuple = TVariantType::Create(tuple2type, env);
        UNIT_ASSERT_VALUES_EQUAL(var2tuple->GetAlternativesCount(), 2);
        UNIT_ASSERT(var2tuple->GetUnderlyingType() == tuple2type);

        // one element struct
        TStructMember members[2] = {
            { "A", TDataType::Create(NUdf::TDataType<i32>::Id, env) },
            { "B", emptyStructType }
        };

        auto struct1type = TStructType::Create(1, members, env);
        auto var1struct = TVariantType::Create(struct1type, env);
        UNIT_ASSERT_VALUES_EQUAL(var1struct->GetAlternativesCount(), 1);
        UNIT_ASSERT(var1struct->GetUnderlyingType() == struct1type);

        // two elements struct
        auto struct2type = TStructType::Create(2, members, env);
        auto var2struct = TVariantType::Create(struct2type, env);
        UNIT_ASSERT_VALUES_EQUAL(var2struct->GetAlternativesCount(), 2);
        UNIT_ASSERT(var2struct->GetUnderlyingType() == struct2type);
    }

    Y_UNIT_TEST(TestVariantLiteral) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto emptyStructType = env.GetEmptyStructLazy()->GetType();

        // one element tuple
        auto dt = TDataType::Create(NUdf::TDataType<i32>::Id, env);
        TType* elements[2] = {
            dt,
            emptyStructType
        };

        auto tuple1type = TTupleType::Create(1, elements, env);
        auto var1tuple = TVariantType::Create(tuple1type, env);
        UNIT_ASSERT_EXCEPTION(TVariantLiteral::Create(TRuntimeNode(env.GetEmptyTupleLazy(), true), 0, var1tuple, env), yexception);
        auto i32value = TRuntimeNode(TDataLiteral::Create(NUdf::TUnboxedValuePod((i32)42), dt, env), true);
        UNIT_ASSERT_EXCEPTION(TVariantLiteral::Create(i32value, 23, var1tuple, env), yexception);
        auto varValue = TVariantLiteral::Create(i32value, 0, var1tuple, env);
        UNIT_ASSERT_VALUES_EQUAL(varValue->GetIndex(), 0);
        UNIT_ASSERT(varValue->GetItem() == i32value);

        // two elements tuple
        auto tuple2type = TTupleType::Create(2, elements, env);
        auto var2tuple = TVariantType::Create(tuple2type, env);
        UNIT_ASSERT_EXCEPTION(TVariantLiteral::Create(TRuntimeNode(env.GetEmptyTupleLazy(), true), 0, var2tuple, env), yexception);
        UNIT_ASSERT_EXCEPTION(TVariantLiteral::Create(i32value, 23, var2tuple, env), yexception);
        UNIT_ASSERT_EXCEPTION(TVariantLiteral::Create(i32value, 1, var2tuple, env), yexception);
        varValue = TVariantLiteral::Create(i32value, 0, var2tuple, env);
        UNIT_ASSERT_VALUES_EQUAL(varValue->GetIndex(), 0);
        UNIT_ASSERT(varValue->GetItem() == i32value);
        varValue = TVariantLiteral::Create(TRuntimeNode(env.GetEmptyStructLazy(), true), 1, var2tuple, env);
        UNIT_ASSERT_VALUES_EQUAL(varValue->GetIndex(), 1);
        UNIT_ASSERT(varValue->GetItem() == TRuntimeNode(env.GetEmptyStructLazy(), true));

        // one element struct
        TStructMember members[2] = {
            { "A", TDataType::Create(NUdf::TDataType<i32>::Id, env) },
            { "B", emptyStructType }
        };

        auto struct1type = TStructType::Create(1, members, env);
        auto var1struct = TVariantType::Create(struct1type, env);
        UNIT_ASSERT_EXCEPTION(TVariantLiteral::Create(TRuntimeNode(env.GetEmptyTupleLazy(), true), 0, var1struct, env), yexception);
        UNIT_ASSERT_EXCEPTION(TVariantLiteral::Create(i32value, 23, var1struct, env), yexception);
        varValue = TVariantLiteral::Create(i32value, 0, var1struct, env);
        UNIT_ASSERT_VALUES_EQUAL(varValue->GetIndex(), 0);
        UNIT_ASSERT(varValue->GetItem() == i32value);

        // two elements struct
        auto struct2type = TStructType::Create(2, members, env);
        auto var2struct = TVariantType::Create(struct2type, env);

        UNIT_ASSERT_EXCEPTION(TVariantLiteral::Create(TRuntimeNode(env.GetEmptyTupleLazy(), true), 0, var2struct, env), yexception);
        UNIT_ASSERT_EXCEPTION(TVariantLiteral::Create(i32value, 23, var2struct, env), yexception);
        UNIT_ASSERT_EXCEPTION(TVariantLiteral::Create(i32value, 1, var2struct, env), yexception);
        varValue = TVariantLiteral::Create(i32value, 0, var2struct, env);
        UNIT_ASSERT_VALUES_EQUAL(varValue->GetIndex(), 0);
        UNIT_ASSERT(varValue->GetItem() == i32value);
        varValue = TVariantLiteral::Create(TRuntimeNode(env.GetEmptyStructLazy(), true), 1, var2struct, env);
        UNIT_ASSERT_VALUES_EQUAL(varValue->GetIndex(), 1);
        UNIT_ASSERT(varValue->GetItem() == TRuntimeNode(env.GetEmptyStructLazy(), true));
    }
}

}
}
