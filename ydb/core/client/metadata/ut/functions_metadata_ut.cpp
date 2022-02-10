#include <ydb/core/client/metadata/functions_metadata.h>

#include <ydb/core/scheme_types/scheme_type_registry.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/testing/unittest/registar.h>


using namespace NKikimr;
using namespace NMiniKQL;

static const NScheme::TTypeId Uint32Id = NScheme::NTypeIds::Uint32;

Y_UNIT_TEST_SUITE(TFunctionsMetadataTest)
{
    Y_UNIT_TEST(Serialization) {
        static TFunctionParamMetadata AddUi32Metadata[] = {
            { Uint32Id, 0 }, // result
            { Uint32Id, 0 }, // first arg
            { Uint32Id, 0 }, // second arg
            { 0, 0 }
        };

        const auto functionRegistry = CreateBuiltinRegistry();
        functionRegistry->Register("MyAdd", TFunctionDescriptor(AddUi32Metadata, nullptr));

        TString metadata;
        SerializeMetadata(*functionRegistry, &metadata);
        DeserializeMetadata(metadata, *functionRegistry);

        std::pair<NScheme::TTypeId, bool> argTypes[] = {
            { Uint32Id, false},
            { Uint32Id, false},
            { Uint32Id, false}
        };

        const auto op = functionRegistry->GetBuiltin("MyAdd", argTypes, Y_ARRAY_SIZE(argTypes));
        UNIT_ASSERT_EQUAL(op.Function, nullptr);
        UNIT_ASSERT_EQUAL(op.ResultAndArgs[0].SchemeType, Uint32Id);
        UNIT_ASSERT_EQUAL(op.ResultAndArgs[0].Flags, 0);
        UNIT_ASSERT_EQUAL(op.ResultAndArgs[1].SchemeType, Uint32Id);
        UNIT_ASSERT_EQUAL(op.ResultAndArgs[1].Flags, 0);
        UNIT_ASSERT_EQUAL(op.ResultAndArgs[2].SchemeType, Uint32Id);
        UNIT_ASSERT_EQUAL(op.ResultAndArgs[2].Flags, 0);
        UNIT_ASSERT_EQUAL(op.ResultAndArgs[3].SchemeType, 0);
    }
}
