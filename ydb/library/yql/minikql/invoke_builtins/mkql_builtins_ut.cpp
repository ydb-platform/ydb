#include "mkql_builtins.h"

#include <ydb/library/yql/public/udf/udf_value.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>

namespace NKikimr {
namespace NMiniKQL {

static TFunctionParamMetadata AddUi32Metadata[] = {
    { NUdf::TDataType<ui32>::Id, 0 }, // result
    { NUdf::TDataType<ui32>::Id, 0 }, // first arg
    { NUdf::TDataType<ui32>::Id, 0 }, // second arg
    { 0, 0 }
};

static NUdf::TUnboxedValuePod AddUi32(const NUdf::TUnboxedValuePod* args)
{
    const ui32 first = args[0].Get<ui32>();
    const ui32 second = args[1].Get<ui32>();
    return NUdf::TUnboxedValuePod(first + second);
}

Y_UNIT_TEST_SUITE(TFunctionRegistryTest) {
    Y_UNIT_TEST(TestRegistration) {
        const auto functionRegistry = CreateBuiltinRegistry();
        functionRegistry->Register("MyAdd", TFunctionDescriptor(AddUi32Metadata, &AddUi32));

        const std::array<TArgType, 3U> argTypes ={{{ NUdf::TDataType<ui32>::Id, false }, { NUdf::TDataType<ui32>::Id, false }, { NUdf::TDataType<ui32>::Id, false }}};
        auto op = functionRegistry->GetBuiltin("MyAdd", argTypes.data(), argTypes.size());
        UNIT_ASSERT_EQUAL(op.Function, &AddUi32);
        UNIT_ASSERT_EQUAL(op.ResultAndArgs[0].SchemeType, NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT_EQUAL(op.ResultAndArgs[0].Flags, 0);
        UNIT_ASSERT_EQUAL(op.ResultAndArgs[1].SchemeType, NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT_EQUAL(op.ResultAndArgs[1].Flags, 0);
        UNIT_ASSERT_EQUAL(op.ResultAndArgs[2].SchemeType, NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT_EQUAL(op.ResultAndArgs[2].Flags, 0);
        UNIT_ASSERT_EQUAL(op.ResultAndArgs[3].SchemeType, 0);

        const NUdf::TUnboxedValuePod args[2] = {NUdf::TUnboxedValuePod(ui32(2)), NUdf::TUnboxedValuePod(ui32(3))};

        auto result = op.Function(&args[0]);
        UNIT_ASSERT_EQUAL(result.Get<ui32>(), 5);
    }
}

} // namespace NMiniKQL

} // namespace NKikimr
