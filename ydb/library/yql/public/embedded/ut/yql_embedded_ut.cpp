#include <ydb/library/yql/public/embedded/yql_embedded.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {
namespace NEmbedded {

namespace {
    THolder<IOperationFactory> MakeFactory() {
        TOperationFactoryOptions options;
        return MakeOperationFactory(options, "", [](const TFileStorageConfig&) {
            return nullptr;
        });
    }
}

Y_UNIT_TEST_SUITE(TValidateTests) {
    Y_UNIT_TEST(SimpleOk) {
        auto factory = MakeFactory();
        auto res = factory->Run("select 1", {.Mode = EExecuteMode::Validate});
    }

    Y_UNIT_TEST(SimpleFail) {
        auto factory = MakeFactory();
        UNIT_ASSERT_EXCEPTION(factory->Run("select foo", {.Mode = EExecuteMode::Validate}), yexception);
    }
}

} // NEmbedded
} // NYql
