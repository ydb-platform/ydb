#include "check_counter.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/testlib/test_client.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/split.h>

namespace NKikimr::NKqp {

static TIntrusivePtr<NMonitoring::TDynamicCounters> ResolveCounterPath(
    TIntrusivePtr<NMonitoring::TDynamicCounters> root,
    const TString& path
) {
    TVector<TString> parts;
    Split(path, "/", parts);
    if (parts.empty()) {
        return nullptr;
    }
    const TString service = parts[0];
    auto current = GetServiceCounters(root, service);
    if (!current) {
        return nullptr;
    }
    for (size_t i = 1; i + 1 < parts.size(); i += 2) {
        const TString& key = parts[i];
        const TString& value = parts[i + 1];
        current = current->FindSubgroup(key, value);
        if (!current) {
            return nullptr;
        }
    }
    return current;
}

TConclusionStatus TCheckCounterCommand::DoExecute(TKikimrRunner& kikimr) {
    auto* runtime = kikimr.GetTestServer().GetRuntime();
    UNIT_ASSERT(runtime != nullptr);
    auto root = runtime->GetAppData().Counters;
    UNIT_ASSERT(root != nullptr);

    auto group = ResolveCounterPath(root, Path);
    UNIT_ASSERT_C(group != nullptr, "Counter path not found: " << Path);

    auto counter = group->FindCounter(CounterName);
    UNIT_ASSERT_C(counter != nullptr, "Counter not found: " << Path << " / " << CounterName);

    const i64 actual = counter->Val();
    if (AtLeast) {
        UNIT_ASSERT_C(actual >= ExpectedValue,
            "Counter " << Path << "/" << CounterName << " actual=" << actual << " expected at least " << ExpectedValue);
    } else {
        UNIT_ASSERT_VALUES_EQUAL_C(actual, ExpectedValue,
            "Counter " << Path << "/" << CounterName << " actual=" << actual << " expected=" << ExpectedValue);
    }

    return TConclusionStatus::Success();
}

TConclusionStatus TCheckCounterCommand::DoDeserializeProperties(const TPropertiesCollection& props) {
    auto pathOpt = props.GetOptional("PATH");
    auto expectedOpt = props.GetOptional("EXPECTED");
    auto atLeastOpt = props.GetOptional("AT_LEAST");

    if (props.GetFreeArgumentsCount() != 1) {
        return TConclusionStatus::Fail("CHECK_COUNTER requires exactly one free argument (counter name)");
    }
    CounterName = props.GetFreeArgumentVerified(0);

    if (!pathOpt || pathOpt->empty()) {
        return TConclusionStatus::Fail("CHECK_COUNTER requires PATH");
    }
    Path = *pathOpt;

    const bool hasExpected = expectedOpt && !expectedOpt->empty();
    const bool hasAtLeast = atLeastOpt && !atLeastOpt->empty();
    if (hasExpected && hasAtLeast) {
        return TConclusionStatus::Fail("CHECK_COUNTER: use only one of EXPECTED or AT_LEAST, not both");
    }
    if (!hasExpected && !hasAtLeast) {
        return TConclusionStatus::Fail("CHECK_COUNTER requires exactly one of EXPECTED or AT_LEAST");
    }

    if (hasExpected) {
        if (!TryFromString<i64>(*expectedOpt, ExpectedValue)) {
            return TConclusionStatus::Fail("CHECK_COUNTER EXPECTED must be integer: " + *expectedOpt);
        }
        AtLeast = false;
    } else {
        if (!TryFromString<i64>(*atLeastOpt, ExpectedValue)) {
            return TConclusionStatus::Fail("CHECK_COUNTER AT_LEAST must be integer: " + *atLeastOpt);
        }
        AtLeast = true;
    }
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NKqp
