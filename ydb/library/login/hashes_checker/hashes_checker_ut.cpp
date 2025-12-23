#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>

#include <ydb/library/login/hashes_checker/hashes_checker.h>


using namespace NLogin;

Y_UNIT_TEST_SUITE(HashesCheckerOldFormat) {

    Y_UNIT_TEST(CorrectRecord) {
        auto hash = R"(
            {
                "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                "type": "argon2id"
            }
        )";

        auto result = THashesChecker().OldFormatCheck(hash);
        UNIT_ASSERT(result.Success);
    }

    Y_UNIT_TEST(WrongSaltLength) {
        auto hash = R"(
            {
                "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                "salt": "wrongSaltLength",
                "type": "argon2id"
            }
        )";

        auto result = THashesChecker().OldFormatCheck(hash);
        UNIT_ASSERT(!result.Success);
        UNIT_ASSERT_STRING_CONTAINS(result.Error, "Length of field \'salt\' is 15, but it must be equal 24");
    }

    Y_UNIT_TEST(WrongHashLength) {
        auto hash = R"(
            {
                "hash": "wrongHashLength",
                "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                "type": "argon2id"
            }
        )";

        auto result = THashesChecker().OldFormatCheck(hash);
        UNIT_ASSERT(!result.Success);
        UNIT_ASSERT_STRING_CONTAINS(result.Error, "Length of field \'hash\' is 15, but it must be equal 44");
    }

    Y_UNIT_TEST(WrongHashType) {
        auto hash = R"(
            {
                "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                "type": "wrongtype"
            }
        )";

        auto result = THashesChecker().OldFormatCheck(hash);
        UNIT_ASSERT(!result.Success);
        UNIT_ASSERT_STRING_CONTAINS(result.Error, "Field 'type' must be equal 'argon2id'");
    }

    Y_UNIT_TEST(NonJsonFormat) {
        auto hash = R"(
            {{{{}}}
                "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                "type": "argon2id"
        )";

        auto result = THashesChecker().OldFormatCheck(hash);
        UNIT_ASSERT(!result.Success);
        UNIT_ASSERT_STRING_CONTAINS(result.Error, "Cannot parse hash value; it should be in JSON-format");
    }

    Y_UNIT_TEST(UnknownField) {
        auto hash = R"(
            {
                "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                "type": "argon2id",
                "some_strange_field": "some_strange_value"
            }
        )";

        auto result = THashesChecker().OldFormatCheck(hash);
        UNIT_ASSERT(!result.Success);
        UNIT_ASSERT_STRING_CONTAINS(result.Error, "There should be strictly three fields here: salt, hash and type");
    }

    Y_UNIT_TEST(WrongHashEncoding) {
        auto hash = R"(
            {
                "hash": "Field not in base64format but with 44 length",
                "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                "type": "argon2id"
            }
        )";

        auto result = THashesChecker().OldFormatCheck(hash);
        UNIT_ASSERT(!result.Success);
        UNIT_ASSERT_STRING_CONTAINS(result.Error, "Field \'hash\' must be in base64 format");
    }

    Y_UNIT_TEST(WrongSaltEncoding) {
        auto hash = R"(
            {
                "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                "salt": "Not in base64 format =) ",
                "type": "argon2id"
            }
        )";

        auto result = THashesChecker().OldFormatCheck(hash);
        UNIT_ASSERT(!result.Success);
        UNIT_ASSERT_STRING_CONTAINS(result.Error, "Field \'salt\' must be in base64 format");
    }

}

Y_UNIT_TEST_SUITE(HashesCheckerNewFormat) {

    Y_UNIT_TEST(CorrectRecord) {
        TString hashes = R"(
            {
                "version": 1,
                "argon2id": "U+tzBtgo06EBQCjlARA6Jg==$p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                "scram-sha-256": "4096:dgnDNb/a9Qc8e/LclrONVw==$26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=:MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
            }
        )";

        auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
        UNIT_ASSERT(result.Success);
    }

    Y_UNIT_TEST(NonBase64EncodedJsonRecord) {
        {
            TString hashes = R"(
                "argon2id": "U+tzBtgo06EBQCjlARA6Jg==$p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
            )";

            auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "Cannot parse hashes value; it should be JSON in base64 encoding");
        }

        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "U+tzBtgo06EBQCjlARA6Jg==$p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "scram-sha-256": "4096:dgnDNb/a9Qc8e/LclrONVw==$26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=:MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
                }
            )";

            auto result = THashesChecker().NewFormatCheck(hashes);
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "Cannot parse hashes value; it should be JSON in base64 encoding");
        }
    }

    Y_UNIT_TEST(AbsentVersionField) {
        TString hashes = R"(
            {
                "argon2id": "U+tzBtgo06EBQCjlARA6Jg==$p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                "scram-sha-256": "4096:dgnDNb/a9Qc8e/LclrONVw==$26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=:MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
            }
        )";

        auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
        UNIT_ASSERT(!result.Success);
        UNIT_ASSERT_STRING_CONTAINS(result.Error, "Field 'version' must be in JSON map and have numeric type");
    }

    Y_UNIT_TEST(UnknownHashName) {
        TString hashes = R"(
            {
                "version": 1,
                "margon": "U+tzBtgo06EBQCjlARA6Jg==$p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw="
            }
        )";

        auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
        UNIT_ASSERT(!result.Success);
        UNIT_ASSERT_STRING_CONTAINS(result.Error, "Unknown field name");
    }

    Y_UNIT_TEST(WrongArgonHashFormat) {
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "U+tzBtgo06EBQCjlARA6Jg==:ZGl2aW5nbGVzc21pbmluZw=="
                }
            )";

            auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "Argon hash has to have '<salt>$<hash>' format");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "saltsaltsalt=)$ZGl2aW5nbGVzc21pbmluZw=="
                }
            )";

            auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "Salt in Argon hash must be in base64 encoding");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "U+tzBtgo06EBQCjlARA6Jg==$hashhashhash=="
                }
            )";

            auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "Hash in Argon hash must be in base64 encoding");
        }
    }

    Y_UNIT_TEST(WrongArgonHashLength) {
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "aGtsZm1rbW1tamh2$p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw="
                }
            )";

            auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "Salt in Argon hash must be 16 bytes long");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "U+tzBtgo06EBQCjlARA6Jg==$ZGl2aW5nbGVzc21pbmluZw=="
                }
            )";

            auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "Hash in Argon hash must be 32 bytes long");
        }
    }

    Y_UNIT_TEST(WrongScramHashFormat) {
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "scram-sha-256": "4096,dgnDNb/a9Qc8e/LclrONVw==,26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=,MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
                }
            )";

            auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "Scram hash has to have '<iterations>:<salt>$<storedkey>:<serverkey>' format");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "scram-sha-256": "3849.23:saltsalt=$26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=:MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
                }
            )";

            auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "Iterations in Scram hash must be equal to 4096");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "scram-sha-256": "4096:saltsalt=$26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=:MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
                }
            )";

            auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "Salt in Scram hash must be in base64 encoding");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "scram-sha-256": "4096:dgnDNb/a9Qc8e/LclrONVw==$26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=:server_key"
                }
            )";

            auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "ServerKey in Scram hash must be in base64 encoding");
        }
    }

    Y_UNIT_TEST(WrongScramHashLength) {
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "scram-sha-256": "4096:c2FsdHNhbHQ=$26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=:MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
                }
            )";

            auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "Salt in Scram hash must be 16 bytes long");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "scram-sha-256": "4096:dgnDNb/a9Qc8e/LclrONVw==$c3RvcmVlZF9rZXk=:MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
                }
            )";

            auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "StoredKey in Scram hash must be 32 bytes long");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "scram-sha-256": "4096:dgnDNb/a9Qc8e/LclrONVw==$26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=:c2VydmVyX2tleQ=="
                }
            )";

            auto result = THashesChecker().NewFormatCheck(Base64Encode(hashes));
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Error, "ServerKey in Scram hash must be 32 bytes long");
        }
    }

}
