#include "validate_grpc_metadata.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NGrpc;

Y_UNIT_TEST_SUITE(NValidateGrpcMetadata) {
    Y_UNIT_TEST(ValidateHeaderIsLegal) {
        TString error = "error";
        UNIT_ASSERT_C(ValidateHeaderIsLegal("key", "value", error), error);
        // Assert 'error' is cleared
        UNIT_ASSERT_C(error.empty(), error);

        // Valid character values upper and lower bounds
        UNIT_ASSERT_C(ValidateHeaderIsLegal("\x30\x39\x61\x7A_-.", "\x20\x7E", error), error);
        UNIT_ASSERT_C(error.empty(), error);

        TString null = " ";
        null[0] = '\0';
        UNIT_ASSERT(!ValidateHeaderIsLegal("key", null, error));
        UNIT_ASSERT_C(error.Contains("\\0"), error);
        Cerr << "Error is '" << error << "'" << Endl;

        // Simple escape sequences
        UNIT_ASSERT(!ValidateHeaderIsLegal("key", "value\x0A\t", error));
        UNIT_ASSERT_C(error.Contains("\\n\\t"), error);
        Cerr << "Error is '" << error << "'" << Endl;

        UNIT_ASSERT(!ValidateHeaderIsLegal("key", "value\x1F", error));
        UNIT_ASSERT_C(error.Contains("\\x1F"), error);
        Cerr << "Error is '" << error << "'" << Endl;

        UNIT_ASSERT(!ValidateHeaderIsLegal("key", "value\x7F", error));
        UNIT_ASSERT_C(error.Contains("\\x7F"), error);
        Cerr << "Error is '" << error << "'" << Endl;

        // Octal character
        UNIT_ASSERT(!ValidateHeaderIsLegal("key", "value\177", error));
        UNIT_ASSERT_C(error.Contains("\\x7F"), error);
        Cerr << "Error is '" << error << "'" << Endl;

        // Invalid header key
        UNIT_ASSERT(!ValidateHeaderIsLegal("key\n", "value", error));
        UNIT_ASSERT_C(error.Contains("\\n"), error);
        Cerr << "Error is '" << error << "'" << Endl;
    }
}
