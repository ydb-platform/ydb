#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>
#include "ydbc_query_helper.h"

Y_UNIT_TEST_SUITE(Mvp) {
    Y_UNIT_TEST(QueryProcessParameterSimple) {
        TString body = R"__(
        {
            "$pnumber": 8
        }
        )__";
        NJson::TJsonValue json;
        NJson::ReadJsonTree(body, &json);
        auto result = NMVP::ProcessQueryParameters(json, true);
        TMaybe<NYdb::TValue> value = result->Params->Build().GetValue("$pnumber");
        UNIT_ASSERT(value.Defined());
        UNIT_ASSERT(value->GetType().GetProto().type_id() == Ydb::Type::INT64);
        UNIT_ASSERT_VALUES_EQUAL(value->GetProto().int64_value(), 8);
        UNIT_ASSERT_VALUES_EQUAL(result->InjectData, "DECLARE $pnumber AS INT64;\n");
    }

    Y_UNIT_TEST(QueryProcessParameterNull) {
        TString body = R"__(
        {
            "$pnumber": null
        }
        )__";
        NJson::TJsonValue json;
        NJson::ReadJsonTree(body, &json);
        auto result = NMVP::ProcessQueryParameters(json, true);
        UNIT_ASSERT(result->HasError);
    }

    Y_UNIT_TEST(QueryProcessParameterDecimal) {
        TString body = R"__(
        {
            "$pnumber": {
                "type": "Decimal(22,9)",
                "value": "23.12"
            }
        }
        )__";
        NJson::TJsonValue json;
        NJson::ReadJsonTree(body, &json);
        auto result = NMVP::ProcessQueryParameters(json, true);
        TMaybe<NYdb::TValue> value = result->Params->Build().GetValue("$pnumber");
        UNIT_ASSERT(value.Defined());
        UNIT_ASSERT_VALUES_EQUAL(value->GetType().GetProto().decimal_type().precision(), 22);
        UNIT_ASSERT_VALUES_EQUAL(value->GetType().GetProto().decimal_type().scale(), 9);
        UNIT_ASSERT_VALUES_EQUAL(result->InjectData, "DECLARE $pnumber AS DECIMAL(22,9);\n");
    }
}
