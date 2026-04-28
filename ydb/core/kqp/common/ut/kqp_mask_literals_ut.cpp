#include <ydb/core/kqp/common/kqp_mask_literals.h>
#include <ydb/core/kqp/session_actor/kqp_log_query.h>
#include <ydb/core/protos/kqp_physical.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(MaskSensitiveLiteralsTest) {

    Y_UNIT_TEST(SelectNumber) {
        auto result = MaskSensitiveLiterals("select 1;");
        UNIT_ASSERT_STRING_CONTAINS(result, "1");
        UNIT_ASSERT_STRING_CONTAINS(result, ";");
    }

    Y_UNIT_TEST(SelectTrue) {
        auto result = MaskSensitiveLiterals("select true;");
        UNIT_ASSERT_STRING_CONTAINS(result, "true");
    }

    Y_UNIT_TEST(SelectStringLiteral) {
        auto result = MaskSensitiveLiterals("select 'foo';");
        UNIT_ASSERT_STRING_CONTAINS(result, "'***removed***'");
        UNIT_ASSERT(!result.Contains("foo"));
    }

    Y_UNIT_TEST(SelectDoubleQuotedString) {
        auto result = MaskSensitiveLiterals("select \"foo\";");
        UNIT_ASSERT_STRING_CONTAINS(result, "'***removed***'");
        UNIT_ASSERT(!result.Contains("foo"));
    }

    Y_UNIT_TEST(SelectMultilineString) {
        auto result = MaskSensitiveLiterals("select @@multi\nline@@;");
        UNIT_ASSERT_STRING_CONTAINS(result, "'***removed***'");
        UNIT_ASSERT(!result.Contains("multi"));
        UNIT_ASSERT(!result.Contains("line"));
    }

    Y_UNIT_TEST(SelectFloat) {
        auto result = MaskSensitiveLiterals("select 3.0;");
        UNIT_ASSERT_STRING_CONTAINS(result, "3.");
    }

    Y_UNIT_TEST(SelectColumn) {
        auto result = MaskSensitiveLiterals("select col;");
        UNIT_ASSERT_STRING_CONTAINS(result, "col");
    }

    Y_UNIT_TEST(WhereWithStringLiteral) {
        auto result = MaskSensitiveLiterals("select * from `logs/of/bob` where pwd='foo';");
        UNIT_ASSERT_STRING_CONTAINS(result, "'***removed***'");
        UNIT_ASSERT_STRING_CONTAINS(result, "`logs/of/bob`");
        UNIT_ASSERT(!result.Contains("foo"));
    }

    Y_UNIT_TEST(CreateUserPassword) {
        auto result = MaskSensitiveLiterals("CREATE USER foo PASSWORD 'secret123';");
        UNIT_ASSERT_STRING_CONTAINS(result, "'***removed***'");
        UNIT_ASSERT(!result.Contains("secret123"));
    }

    Y_UNIT_TEST(AlterUserPassword) {
        auto result = MaskSensitiveLiterals("ALTER USER foo WITH PASSWORD 'newsecret';");
        UNIT_ASSERT_STRING_CONTAINS(result, "'***removed***'");
        UNIT_ASSERT(!result.Contains("newsecret"));
    }

    Y_UNIT_TEST(CreateUserEncryptedPassword) {
        auto result = MaskSensitiveLiterals("CREATE USER foo ENCRYPTED PASSWORD 'secret';");
        UNIT_ASSERT_STRING_CONTAINS(result, "'***removed***'");
        UNIT_ASSERT(!result.Contains("'secret'"));
    }

    Y_UNIT_TEST(CreateUserPasswordDoubleQuoted) {
        auto result = MaskSensitiveLiterals("CREATE USER foo PASSWORD \"secret\";");
        UNIT_ASSERT_STRING_CONTAINS(result, "'***removed***'");
        UNIT_ASSERT(!result.Contains("secret"));
    }

    Y_UNIT_TEST(CreateUserPasswordMultiline) {
        auto result = MaskSensitiveLiterals("CREATE USER foo PASSWORD @@secret@@;");
        UNIT_ASSERT_STRING_CONTAINS(result, "'***removed***'");
        UNIT_ASSERT(!result.Contains("secret"));
    }

    Y_UNIT_TEST(CreateUserHash) {
        auto result = MaskSensitiveLiterals("CREATE USER foo HASH '{\"hash\":\"x\"}';");
        UNIT_ASSERT_STRING_CONTAINS(result, "'***removed***'");
        UNIT_ASSERT(!result.Contains("hash"));
    }

    Y_UNIT_TEST(PragmaNumber) {
        auto result = MaskSensitiveLiterals("pragma a=1");
        UNIT_ASSERT_STRING_CONTAINS(result, "1");
    }

    Y_UNIT_TEST(PragmaString) {
        auto result = MaskSensitiveLiterals("pragma a='foo';");
        UNIT_ASSERT_STRING_CONTAINS(result, "'***removed***'");
        UNIT_ASSERT(!result.Contains("foo"));
    }

    Y_UNIT_TEST(PragmaTrue) {
        auto result = MaskSensitiveLiterals("pragma a=true;");
        UNIT_ASSERT_STRING_CONTAINS(result, "true");
    }

    Y_UNIT_TEST(InvalidSqlReturnsOriginal) {
        TString query = "CREATE SCRET my_sct WITH (value = \"va\")";
        auto result = MaskSensitiveLiterals(query);
        // Parser fails on invalid SQL — returns original unchanged
        UNIT_ASSERT_VALUES_EQUAL(result, query);
    }
}

namespace {

NKqpProto::TKqpPhyQuery MakeQueryWithSchemeOp(
    std::function<void(NKqpProto::TKqpSchemeOperation&)> setupOp,
    const TString& objectType = {})
{
    NKqpProto::TKqpPhyQuery phyQuery;
    auto* tx = phyQuery.AddTransactions();
    tx->SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
    auto* op = tx->MutableSchemeOperation();
    if (objectType) {
        op->SetObjectType(objectType);
    }
    setupOp(*op);
    return phyQuery;
}

} // namespace

Y_UNIT_TEST_SUITE(HasSensitiveSchemeOperationTest) {

    Y_UNIT_TEST(EmptyQuery) {
        NKqpProto::TKqpPhyQuery phyQuery;
        UNIT_ASSERT(!HasSensitiveSchemeOperation(phyQuery));
    }

    Y_UNIT_TEST(NoSchemeOperation) {
        NKqpProto::TKqpPhyQuery phyQuery;
        auto* tx = phyQuery.AddTransactions();
        tx->SetType(NKqpProto::TKqpPhyTx::TYPE_COMPUTE);
        UNIT_ASSERT(!HasSensitiveSchemeOperation(phyQuery));
    }

    Y_UNIT_TEST(CreateTableIsNotSensitive) {
        auto query = MakeQueryWithSchemeOp([](auto& op) {
            op.MutableCreateTable();
        });
        UNIT_ASSERT(!HasSensitiveSchemeOperation(query));
    }

    Y_UNIT_TEST(CreateUserIsSensitive) {
        auto query = MakeQueryWithSchemeOp([](auto& op) {
            op.MutableCreateUser();
        });
        UNIT_ASSERT(HasSensitiveSchemeOperation(query));
    }

    Y_UNIT_TEST(AlterUserIsSensitive) {
        auto query = MakeQueryWithSchemeOp([](auto& op) {
            op.MutableAlterUser();
        });
        UNIT_ASSERT(HasSensitiveSchemeOperation(query));
    }

    Y_UNIT_TEST(CreateSecretIsSensitive) {
        auto query = MakeQueryWithSchemeOp([](auto& op) {
            op.MutableCreateSecret();
        });
        UNIT_ASSERT(HasSensitiveSchemeOperation(query));
    }

    Y_UNIT_TEST(AlterSecretIsSensitive) {
        auto query = MakeQueryWithSchemeOp([](auto& op) {
            op.MutableAlterSecret();
        });
        UNIT_ASSERT(HasSensitiveSchemeOperation(query));
    }

    Y_UNIT_TEST(CreateObjectSecretIsSensitive) {
        auto query = MakeQueryWithSchemeOp([](auto& op) {
            op.MutableCreateObject();
        }, "SECRET");
        UNIT_ASSERT(HasSensitiveSchemeOperation(query));
    }

    Y_UNIT_TEST(UpsertObjectSecretIsSensitive) {
        auto query = MakeQueryWithSchemeOp([](auto& op) {
            op.MutableUpsertObject();
        }, "SECRET");
        UNIT_ASSERT(HasSensitiveSchemeOperation(query));
    }

    Y_UNIT_TEST(AlterObjectSecretIsSensitive) {
        auto query = MakeQueryWithSchemeOp([](auto& op) {
            op.MutableAlterObject();
        }, "SECRET");
        UNIT_ASSERT(HasSensitiveSchemeOperation(query));
    }

    Y_UNIT_TEST(CreateObjectNonSecretIsNotSensitive) {
        auto query = MakeQueryWithSchemeOp([](auto& op) {
            op.MutableCreateObject();
        }, "EXTERNAL_DATA_SOURCE");
        UNIT_ASSERT(!HasSensitiveSchemeOperation(query));
    }

    Y_UNIT_TEST(DropSecretIsNotSensitive) {
        auto query = MakeQueryWithSchemeOp([](auto& op) {
            op.MutableDropSecret();
        });
        UNIT_ASSERT(!HasSensitiveSchemeOperation(query));
    }
}

} // namespace NKikimr::NKqp
