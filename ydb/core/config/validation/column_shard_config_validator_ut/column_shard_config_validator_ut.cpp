#include <ydb/core/config/validation/validators.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <vector>

using namespace NKikimr::NConfig;

Y_UNIT_TEST_SUITE(ColumnShardConfigValidation) {
    Y_UNIT_TEST(AcceptDefaultCompression) {
        NKikimrConfig::TColumnShardConfig CSConfig;
        std::vector<TString> error;
        EValidationResult result = ValidateColumnShardConfig(CSConfig, error);
        UNIT_ASSERT_EQUAL(result, EValidationResult::Ok);
        UNIT_ASSERT_C(error.empty(), "Should not be errors");
    }

    Y_UNIT_TEST(NotAcceptDefaultCompression) {
        NKikimrConfig::TColumnShardConfig CSConfig;
        std::vector<TString> error;
        CSConfig.SetDefaultCompressionLevel(2);
        EValidationResult result = ValidateColumnShardConfig(CSConfig, error);
        UNIT_ASSERT_EQUAL(result, EValidationResult::Error);
        UNIT_ASSERT_VALUES_EQUAL(error.size(), 1);
        UNIT_ASSERT_STRINGS_EQUAL(error.front(), "ColumnShardConfig: compression level is set without compression type");
    }

    Y_UNIT_TEST(CorrectPlainCompression) {
        NKikimrConfig::TColumnShardConfig CSConfig;
        std::vector<TString> error;
        CSConfig.SetDefaultCompression(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
        EValidationResult result = ValidateColumnShardConfig(CSConfig, error);
        UNIT_ASSERT_EQUAL(result, EValidationResult::Ok);
        UNIT_ASSERT_C(error.empty(), "Should not be errors");
    }

    Y_UNIT_TEST(NotCorrectPlainCompression) {
        NKikimrConfig::TColumnShardConfig CSConfig;
        std::vector<TString> error;
        CSConfig.SetDefaultCompression(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
        CSConfig.SetDefaultCompressionLevel(1);
        EValidationResult result = ValidateColumnShardConfig(CSConfig, error);
        UNIT_ASSERT_EQUAL(result, EValidationResult::Error);
        UNIT_ASSERT_VALUES_EQUAL(error.size(), 1);
        UNIT_ASSERT_STRINGS_EQUAL(error.front(), "ColumnShardConfig: compression `uncompressed` does not support compression level");
    }

    Y_UNIT_TEST(CorrectLZ4Compression) {
        NKikimrConfig::TColumnShardConfig CSConfig;
        std::vector<TString> error;
        CSConfig.SetDefaultCompression(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);
        EValidationResult result = ValidateColumnShardConfig(CSConfig, error);
        UNIT_ASSERT_EQUAL(result, EValidationResult::Ok);
        UNIT_ASSERT_C(error.empty(), "Should not be errors");
    }

    Y_UNIT_TEST(NotCorrectLZ4Compression) {
        NKikimrConfig::TColumnShardConfig CSConfig;
        std::vector<TString> error;
        CSConfig.SetDefaultCompression(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);
        CSConfig.SetDefaultCompressionLevel(1);
        EValidationResult result = ValidateColumnShardConfig(CSConfig, error);
        UNIT_ASSERT_EQUAL(result, EValidationResult::Error);
        UNIT_ASSERT_VALUES_EQUAL(error.size(), 1);
        UNIT_ASSERT_STRINGS_EQUAL(error.front(), "ColumnShardConfig: compression `lz4` does not support compression level");
    }

    Y_UNIT_TEST(CorrectZSTDCompression) {
        NKikimrConfig::TColumnShardConfig CSConfig;
        std::vector<TString> error;
        CSConfig.SetDefaultCompression(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD);
        EValidationResult result = ValidateColumnShardConfig(CSConfig, error);
        UNIT_ASSERT_EQUAL(result, EValidationResult::Ok);
        UNIT_ASSERT_C(error.empty(), "Should not be errors");
        CSConfig.SetDefaultCompressionLevel(0);
        result = ValidateColumnShardConfig(CSConfig, error);
        UNIT_ASSERT_EQUAL(result, EValidationResult::Ok);
        UNIT_ASSERT_C(error.empty(), "Should not be errors");
        CSConfig.SetDefaultCompressionLevel(-100);
        result = ValidateColumnShardConfig(CSConfig, error);
        UNIT_ASSERT_EQUAL(result, EValidationResult::Ok);
        UNIT_ASSERT_C(error.empty(), "Should not be errors");
    }

    Y_UNIT_TEST(NotCorrectZSTDCompression) {
        NKikimrConfig::TColumnShardConfig CSConfig;
        std::vector<TString> error;
        CSConfig.SetDefaultCompression(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD);
        CSConfig.SetDefaultCompressionLevel(100);
        EValidationResult result = ValidateColumnShardConfig(CSConfig, error);
        UNIT_ASSERT_EQUAL(result, EValidationResult::Error);
        UNIT_ASSERT_VALUES_EQUAL(error.size(), 1);
        UNIT_ASSERT_STRINGS_EQUAL(error.front(), "ColumnShardConfig: compression `zstd` does not support compression level = 100");
    }
}
