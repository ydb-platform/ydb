#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;

Y_UNIT_TEST_SUITE(KqpUserManagement) {

    Y_UNIT_TEST(CreateUserWithPassword) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 ENCRYPTED PASSWORD 'password1';
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD NULL;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateUserWithHash) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetQueryClient();
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user1 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }';
            )";

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user2 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "aGtsZm1rbW1tamh2",
                    "type": "argon2id"
                }';
            )"; // wrong salt length

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Salt in Argon hash must be 16 bytes long");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user3 HASH '{
                    "hash": "ZGl2aW5nbGVzc21pbmluZw==",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }';
            )"; // wrong argon hash length

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Hash in Argon hash must be 32 bytes long");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user4 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "wrongtype"
                }';
            )";

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "WrongRequest");
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user5 HASH '{{{{}}}
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                ';
            )"; // incorrect json

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "WrongRequest");
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user6 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id",
                    "some_strange_field": "some_strange_value"
                }';
            )";

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "There should be strictly three fields here: salt, hash and type");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user7 HASH '{
                    "hash": "Field not in base64 format",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }';
            )";

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Hash in Argon hash must be in base64 encoding");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user8 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "Field not in base64 format",
                    "type": "argon2id"
                }';
            )";

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Salt in Argon hash must be in base64 encoding");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "flbr3YnA9kG67qegwDTaYg==$wsTryyX+vdkLiZ4PfYabvgVwHf8tbxBVVtDluhiz3fo=",
                    "scram-sha-256": "4096:s0QSrrFVkMTh3k2TTk860A==$LmCubRpIYV1zHMLucTtu7XjhB+PgWwH8ABCYGyVF1mo=:eUrie0C98tEFgygSOtom/fwPmgnMxeq53l7YTFfYncc="
                }
            )";
            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
                CREATE USER user9 HASH '%s';
            )", Base64Encode(hashes).c_str());

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "flbr3YnA9kG67qegwDTaYg==$wsTryyX+vdkLiZ4PfYabvgVwHf8tbxBVVtDluhiz3fo=",
                    "scram-sha-256": "4096,dgnDNb/a9Qc8e/LclrONVw==,26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=,MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
                }
            )"; // wrong scram hash format
            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
                CREATE USER user10 HASH '%s';
            )", Base64Encode(hashes).c_str());

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Scram hash has to have '<iterations>:<salt>$<storedkey>:<serverkey>' format");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "flbr3YnA9kG67qegwDTaYg==$wsTryyX+vdkLiZ4PfYabvgVwHf8tbxBVVtDluhiz3fo=",
                    "scram-sha-256": "4096:salt_not_in_base64$26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=:MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
                }
            )";
            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
                CREATE USER user11 HASH '%s';
            )", Base64Encode(hashes).c_str());

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Salt in Scram hash must be in base64 encoding");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "flbr3YnA9kG67qegwDTaYg==$wsTryyX+vdkLiZ4PfYabvgVwHf8tbxBVVtDluhiz3fo=",
                    "scram-sha-256": "4096:dgnDNb/a9Qc8e/LclrONVw==$26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=:ServerKeyNotInBase64="
                }
            )";
            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
                CREATE USER user12 HASH '%s';
            )", Base64Encode(hashes).c_str());

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "ServerKey in Scram hash must be in base64 encoding");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "flbr3YnA9kG67qegwDTaYg==$wsTryyX+vdkLiZ4PfYabvgVwHf8tbxBVVtDluhiz3fo=",
                    "scram-sha-256": "4096:c2FsdHNhbHQ=$26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=:MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
                }
            )"; // wrong scram salt length
            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
                CREATE USER user13 HASH '%s';
            )", Base64Encode(hashes).c_str());

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Salt in Scram hash must be 16 bytes long");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "flbr3YnA9kG67qegwDTaYg==$wsTryyX+vdkLiZ4PfYabvgVwHf8tbxBVVtDluhiz3fo=",
                    "scram-sha-256": "4096:dgnDNb/a9Qc8e/LclrONVw==$c3RvcmVlZF9rZXk=:MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
                }
            )"; // wrong scram stored key length
            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
                CREATE USER user14 HASH '%s';
            )", Base64Encode(hashes).c_str());

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "StoredKey in Scram hash must be 32 bytes long");
        }
    }

    Y_UNIT_TEST(AlterUserWithHash) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetQueryClient();
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user1;
                ALTER USER user1 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }';
            )";

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user2;
                ALTER USER user2 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "aGtsZm1rbW1tamh2",
                    "type": "argon2id"
                }';
            )"; // wrong salt length

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Salt in Argon hash must be 16 bytes long");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user3;
                ALTER USER user3 HASH '{
                    "hash": "ZGl2aW5nbGVzc21pbmluZw==",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }';
            )"; // wrong hash length

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Hash in Argon hash must be 32 bytes long");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user4;
                ALTER USER user4 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "wrongtype"
                }';
            )";

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "WrongRequest");
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user5;
                ALTER USER user5 HASH '{{{{}}}
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                ';
            )"; // incorrect json

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "WrongRequest");
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user6;
                ALTER USER user6 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id",
                    "some_strange_field": "some_strange_value"
                }';
            )";

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "There should be strictly three fields here: salt, hash and type");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user7;
                ALTER USER user7 HASH '{
                    "hash": "Field not in base64 format",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }';
            )";

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Hash in Argon hash must be in base64 encoding");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user8;
                ALTER USER user8 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "Field not in base64 format",
                    "type": "argon2id"
                }';
            )";

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Salt in Argon hash must be in base64 encoding");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "flbr3YnA9kG67qegwDTaYg==$wsTryyX+vdkLiZ4PfYabvgVwHf8tbxBVVtDluhiz3fo=",
                    "scram-sha-256": "4096:s0QSrrFVkMTh3k2TTk860A==$LmCubRpIYV1zHMLucTtu7XjhB+PgWwH8ABCYGyVF1mo=:eUrie0C98tEFgygSOtom/fwPmgnMxeq53l7YTFfYncc="
                }
            )";
            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
                CREATE USER user9;
                ALTER USER user9 HASH '%s';
            )", Base64Encode(hashes).c_str());

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "flbr3YnA9kG67qegwDTaYg==$wsTryyX+vdkLiZ4PfYabvgVwHf8tbxBVVtDluhiz3fo=",
                    "scram-sha-256": "4096,dgnDNb/a9Qc8e/LclrONVw==,26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=,MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
                }
            )"; // wrong scram hash format
            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
                CREATE USER user10;
                ALTER USER user10 HASH '%s';
            )", Base64Encode(hashes).c_str());

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Scram hash has to have '<iterations>:<salt>$<storedkey>:<serverkey>' format");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "flbr3YnA9kG67qegwDTaYg==$wsTryyX+vdkLiZ4PfYabvgVwHf8tbxBVVtDluhiz3fo=",
                    "scram-sha-256": "4096:salt_not_in_base64$26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=:MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
                }
            )";
            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
                CREATE USER user11;
                ALTER USER user11 HASH '%s';
            )", Base64Encode(hashes).c_str());

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Salt in Scram hash must be in base64 encoding");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "flbr3YnA9kG67qegwDTaYg==$wsTryyX+vdkLiZ4PfYabvgVwHf8tbxBVVtDluhiz3fo=",
                    "scram-sha-256": "4096:dgnDNb/a9Qc8e/LclrONVw==$26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=:ServerKeyNotInBase64="
                }
            )";
            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
                CREATE USER user12;
                ALTER USER user12 HASH '%s';
            )", Base64Encode(hashes).c_str());

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "ServerKey in Scram hash must be in base64 encoding");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "flbr3YnA9kG67qegwDTaYg==$wsTryyX+vdkLiZ4PfYabvgVwHf8tbxBVVtDluhiz3fo=",
                    "scram-sha-256": "4096:c2FsdHNhbHQ=$26pg7R/Q4k3mT2a9P1Sm1mDnq1X7tDhXlS3BRu/9oUc=:MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
                }
            )"; // wrong scram salt length
            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
                CREATE USER user13;
                ALTER USER user13 HASH '%s';
            )", Base64Encode(hashes).c_str());

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Salt in Scram hash must be 16 bytes long");
        }
        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "flbr3YnA9kG67qegwDTaYg==$wsTryyX+vdkLiZ4PfYabvgVwHf8tbxBVVtDluhiz3fo=",
                    "scram-sha-256": "4096:dgnDNb/a9Qc8e/LclrONVw==$c3RvcmVlZF9rZXk=:MLFyR60CNFATMLnxoI2b7IcQUA/SGAIEF2cHUrM/Jj8="
                }
            )"; // wrong scram stored key length
            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
                CREATE USER user14;
                ALTER USER user14 HASH '%s';
            )", Base64Encode(hashes).c_str());

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "StoredKey in Scram hash must be 32 bytes long");
        }
    }

    Y_UNIT_TEST(CreateAlterUserLoginNoLogin) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE USER user1 ENCRYPTED PASSWORD '123' LOGIN;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE USER user2 ENCRYPTED PASSWORD '123' NOLOGIN;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE USER user3 ENCRYPTED PASSWORD '123';
                ALTER USER user3 NOLOGIN;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE USER user4 ENCRYPTED PASSWORD '123' NOLOGIN;
                ALTER USER user4 LOGIN;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE USER user5 someNonExistentOption;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "extraneous input \'someNonExistentOption\'");
        }

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE USER user6;
                ALTER USER user6 someNonExistentOption;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "mismatched input \'someNonExistentOption\'");
        }
    }

     Y_UNIT_TEST(CreateUserWithoutPassword) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_TWIN(CreateAndDropUser, StrictAclCheck) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableStrictAclCheck(StrictAclCheck);
        auto db = kikimr.GetTableClient();
        {
            // Drop non-existing user force
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP USER IF EXISTS user1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD 'password1';
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            // Drop existing user
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP USER user1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD NULL;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            // Drop existing user force
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP USER IF EXISTS user1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD NULL;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            // Drop existing user
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP USER user1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            // Drop user with ACL
            auto session = db.CreateSession().GetValueSync().GetSession();

            TString query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user2 PASSWORD NULL;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            query = TStringBuilder() << R"(
            --!syntax_v1
            GRANT ALL ON `/Root` TO user2;
            )";
            result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            query = TStringBuilder() << R"(
            --!syntax_v1
            DROP USER user2;
            )";
            result = session.ExecuteSchemeQuery(query).GetValueSync();
            if (!StrictAclCheck) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Error: User user2 has an ACL record on /Root and can't be removed");
            }
        }
    }

    Y_UNIT_TEST(AlterUser) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD 'password1';
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER USER user1 WITH PASSWORD 'password2';
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER USER user1 WITH ENCRYPTED PASSWORD 'password3';
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER USER user1 WITH PASSWORD NULL;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterUserImplicitTX) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetQueryClient();
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD 'password1';
            )";
            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER USER user1 WITH PASSWORD 'password2';
            )";
            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER USER user1 WITH ENCRYPTED PASSWORD 'password3';
            )";
            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER USER user1 WITH PASSWORD NULL;
            )";
            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateAndDropGroup) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        {
            // Drop non-existing group force
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP GROUP IF EXISTS group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE GROUP group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            // Drop existing group
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP GROUP group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE GROUP group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            // Drop existing group force
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP GROUP IF EXISTS group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            // Drop existing group
            auto query1 = TStringBuilder() << R"(
            --!syntax_v1
            CREATE GROUP group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query1).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto query2 = TStringBuilder() << R"(
            --!syntax_v1
            DROP GROUP group1;
            )";
            result = session.ExecuteSchemeQuery(query2).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterGroup) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD 'password1';
            CREATE USER user2 PASSWORD 'password2';
            CREATE USER user3;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE GROUP group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER GROUP group1 ADD USER user1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER GROUP group1 DROP USER user1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER GROUP group1 ADD USER user1, user2;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER GROUP group1 DROP USER user1, user2;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
