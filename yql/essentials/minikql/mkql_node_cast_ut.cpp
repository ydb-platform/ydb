#include "mkql_node.h"
#include "mkql_node_cast.h"
#include "mkql_node_builder.h"

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {
namespace NMiniKQL {

class TMiniKQLNodeCast: public TTestBase
{
    UNIT_TEST_SUITE(TMiniKQLNodeCast);
        UNIT_TEST(AsTypeTest);
        UNIT_TEST_EXCEPTION(BadAsTypeTest, yexception);
        UNIT_TEST(AsValueTest);
        UNIT_TEST_EXCEPTION(BadAsValueTest, yexception);
    UNIT_TEST_SUITE_END();

    void AsTypeTest() {
        TRuntimeNode node = Uint32AsNode(123);
        TDataType* type = AS_TYPE(TDataType, node);
        UNIT_ASSERT_EQUAL(type, node.GetStaticType());
        UNIT_ASSERT_EQUAL(type, node.GetNode()->GetType());
    }

    void BadAsTypeTest() {
        TRuntimeNode node = Uint32AsNode(123);
        TCallableType* type = AS_TYPE(TCallableType, node);
        Y_UNUSED(type);
    }

    void AsValueTest() {
        TRuntimeNode dataNode = Uint32AsNode(123);

        TCallableType* ctype = TCallableType::Create(
                    "callable", dataNode.GetStaticType(),
                    0, nullptr, nullptr, Env);

        TCallable* callable = TCallable::Create(dataNode, ctype, Env);

        TRuntimeNode node(callable, false);
        node.Freeze();

        TDataLiteral* value = AS_VALUE(TDataLiteral, node);
        UNIT_ASSERT_EQUAL(value, dataNode.GetNode());
    }

    void BadAsValueTest() {
        TRuntimeNode node = Uint32AsNode(123);
        TListLiteral* list = AS_VALUE(TListLiteral, node);
        Y_UNUSED(list);
    }

    TMiniKQLNodeCast()
        : Alloc(__LOCATION__)
        , Env(Alloc)
    {}

private:
    TRuntimeNode Uint32AsNode(ui32 value) {
        return TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod(value), NUdf::EDataSlot::Uint32, Env), true);
    }

private:
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
};

UNIT_TEST_SUITE_REGISTRATION(TMiniKQLNodeCast);

} // namespace NMiniKQL
} // namespace NKikimr
