#include <ydb/core/kqp/common/kqp_mask_literals.h>
#include <ydb/core/kqp/session_actor/kqp_log_query.h>
#include <ydb/core/protos/kqp_physical.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(MaskSecretValueLiteralsTest) {

    Y_UNIT_TEST(SelectNumber) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("select 1;"),
            "select 1 ;");
    }

    Y_UNIT_TEST(SelectTrue) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("select true;"),
            "select true ;");
    }

    Y_UNIT_TEST(SelectStringLiteral) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("select 'foo';"),
            "select '***removed***' ;");
    }

    Y_UNIT_TEST(SelectDoubleQuotedString) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("select \"foo\";"),
            "select '***removed***' ;");
    }

    Y_UNIT_TEST(SelectMultilineString) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("select @@multi\nline@@;"),
            "select '***removed***' ;");
    }

    Y_UNIT_TEST(SelectFloat) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("select 3.0;"),
            "select 3.0 ;");
    }

    Y_UNIT_TEST(SelectColumn) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("select col;"),
            "select col ;");
    }

    Y_UNIT_TEST(WhereWithStringLiteral) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("select * from `logs/of/bob` where pwd='foo';"),
            "select * from `logs/of/bob` where pwd = '***removed***' ;");
    }

    Y_UNIT_TEST(CreateUserPassword) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("CREATE USER foo PASSWORD 'secret123';"),
            "CREATE USER foo PASSWORD '***removed***' ;");
    }

    Y_UNIT_TEST(AlterUserPassword) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("ALTER USER foo WITH PASSWORD 'newsecret';"),
            "ALTER USER foo WITH PASSWORD '***removed***' ;");
    }

    Y_UNIT_TEST(CreateUserEncryptedPassword) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("CREATE USER foo ENCRYPTED PASSWORD 'secret';"),
            "CREATE USER foo ENCRYPTED PASSWORD '***removed***' ;");
    }

    Y_UNIT_TEST(CreateUserPasswordDoubleQuoted) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("CREATE USER foo PASSWORD \"secret\";"),
            "CREATE USER foo PASSWORD '***removed***' ;");
    }

    Y_UNIT_TEST(CreateUserPasswordMultiline) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("CREATE USER foo PASSWORD @@secret@@;"),
            "CREATE USER foo PASSWORD '***removed***' ;");
    }

    Y_UNIT_TEST(CreateUserHash) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("CREATE USER foo HASH '{\"hash\":\"x\"}';"),
            "CREATE USER foo HASH '***removed***' ;");
    }

    Y_UNIT_TEST(PragmaNumber) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("pragma a=1"),
            "pragma a = 1");
    }

    Y_UNIT_TEST(PragmaString) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("pragma a='foo';"),
            "pragma a = '***removed***' ;");
    }

    Y_UNIT_TEST(PragmaTrue) {
        UNIT_ASSERT_VALUES_EQUAL(
            *MaskSecretValueLiterals("pragma a=true;"),
            "pragma a = true ;");
    }

    Y_UNIT_TEST(InvalidSqlReturnsNothing) {
        // Parser fails on invalid SQL — returns Nothing() so the caller is
        // forced to suppress the query instead of silently logging the
        // unmasked text.
        TString query = "CREATE SCRET my_sct WITH (value = \"va\")";
        UNIT_ASSERT(!MaskSecretValueLiterals(query).Defined());
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
