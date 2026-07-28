#include "plan_check.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NQueryReplay;

namespace {

NJson::TJsonValue PlanWithReadType(const TString& readType) {
    NJson::TJsonValue plan(NJson::JSON_MAP);
    NJson::TJsonValue tables(NJson::JSON_ARRAY);
    NJson::TJsonValue table(NJson::JSON_MAP);
    table["name"] = "t";
    NJson::TJsonValue reads(NJson::JSON_ARRAY);
    NJson::TJsonValue read(NJson::JSON_MAP);
    read["type"] = readType;
    reads.AppendValue(read);
    table["reads"] = reads;
    tables.AppendValue(table);
    plan["tables"] = tables;
    return plan;
}

NJson::TJsonValue PlanWithWrite(const TString& writeType, const TVector<TString>& columns) {
    NJson::TJsonValue plan(NJson::JSON_MAP);
    NJson::TJsonValue tables(NJson::JSON_ARRAY);
    NJson::TJsonValue table(NJson::JSON_MAP);
    table["name"] = "t";
    NJson::TJsonValue writes(NJson::JSON_ARRAY);
    NJson::TJsonValue write(NJson::JSON_MAP);
    write["type"] = writeType;
    if (!columns.empty()) {
        NJson::TJsonValue cols(NJson::JSON_ARRAY);
        for (const auto& column : columns) {
            cols.AppendValue(column);
        }
        write["columns"] = cols;
    }
    writes.AppendValue(write);
    table["writes"] = writes;
    tables.AppendValue(table);
    plan["tables"] = tables;
    return plan;
}

TString WritePlanJson(const NJson::TJsonValue& plan) {
    return NJson::WriteJson(plan, false);
}

const TTableMetadataLookup NoMetadataLookup = [](const TString&) {
    return nullptr;
};

} // namespace

Y_UNIT_TEST_SUITE(TQueryReplayPlanCheck) {
    Y_UNIT_TEST(ReadTypesMismatch) {
        const auto oldPlan = PlanWithReadType("Scan");
        const auto newPlan = PlanWithReadType("Lookup");
        const auto [status, message] = CheckQueryPlans(
            WritePlanJson(oldPlan),
            newPlan,
            NoMetadataLookup);
        UNIT_ASSERT_EQUAL(status, TQueryReplayEvents::ReadTypesMismatch);
        UNIT_ASSERT_STRINGS_EQUAL(
            message,
            "Read types mismatch, old engine: Scan, new engine: Lookup");
        UNIT_ASSERT_STRINGS_EQUAL(StatusToFailReason(status), "read_types_mismatch");
    }

    Y_UNIT_TEST(WriteTypesMismatch) {
        const auto oldPlan = PlanWithWrite("MultiUpsert", {});
        const auto newPlan = PlanWithWrite("MultiReplace", {});
        const auto [status, message] = CheckQueryPlans(
            WritePlanJson(oldPlan),
            newPlan,
            NoMetadataLookup);
        UNIT_ASSERT_EQUAL(status, TQueryReplayEvents::WriteTypesMismatch);
        UNIT_ASSERT_STRINGS_EQUAL(
            message,
            "Write types mismatch, old engine: MultiUpsert, new engine: MultiReplace");
        UNIT_ASSERT_STRINGS_EQUAL(StatusToFailReason(status), "write_types_mismatch");
    }

    Y_UNIT_TEST(WriteColumnsMismatch) {
        const auto oldPlan = PlanWithWrite("Upsert", {"new"});
        const auto newPlan = PlanWithWrite("Upsert", {"old"});
        const auto [status, message] = CheckQueryPlans(
            WritePlanJson(oldPlan),
            newPlan,
            NoMetadataLookup);
        UNIT_ASSERT_EQUAL(status, TQueryReplayEvents::WriteColumnsMismatch);
        UNIT_ASSERT_STRINGS_EQUAL(message, "Write columns mismatch");
    }

    Y_UNIT_TEST(MatchingPlansSuccess) {
        const auto plan = PlanWithReadType("Lookup");
        const auto [status, message] = CheckQueryPlans(
            WritePlanJson(plan),
            plan,
            NoMetadataLookup);
        UNIT_ASSERT_EQUAL(status, TQueryReplayEvents::Success);
        UNIT_ASSERT_VALUES_EQUAL(message, "");
    }
}
