#include "yaml_config.h"

#include <ydb/library/yaml_config/reserved_fields_ut.pb.h>

#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NYamlConfig;
using NKikimr::NYamlConfig::NTesting::TReservedFieldsTestConfig;

namespace {

TMap<TString, std::pair<TString, TString>> CollectUnknownKeys(const TString& json) {
    TReservedFieldsTestConfig proto;
    TSimpleSharedPtr<TBasicUnknownFieldsCollector> collector = new TBasicUnknownFieldsCollector();

    NProtobufJson::TJson2ProtoConfig cfg;
    cfg.SetUnknownFieldsCollector(collector).SetAllowUnknownFields(true);

    NProtobufJson::Json2Proto(TStringBuf(json), proto, cfg);
    return collector->GetUnknownKeys();
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(YamlConfigReservedFields) {

    Y_UNIT_TEST(KnownFieldIsAccepted) {
        const auto unknownKeys = CollectUnknownKeys(R"({"KeptField": 42})");
        UNIT_ASSERT_C(unknownKeys.empty(), "defined fields shouldnt be unknown");
    }

    Y_UNIT_TEST(ReservedFieldIsIgnored) {

        const auto unknownKeys = CollectUnknownKeys(
            R"({"KeptField": 42, "RemovedField": 1, "AnotherRemovedField": 2})");
        UNIT_ASSERT_C(unknownKeys.empty(), "reserved fields shouldnt be unknown");
    }

    Y_UNIT_TEST(UnknownFieldIsReported) {
        const auto unknownKeys = CollectUnknownKeys(
            R"({"KeptField": 42, "RemovedField": 1, "TrulyUnknownField": 7})");
        UNIT_ASSERT_VALUES_EQUAL(unknownKeys.size(), 1);
        UNIT_ASSERT(unknownKeys.contains("/TrulyUnknownField"));
        UNIT_ASSERT(!unknownKeys.contains("/RemovedField"));
    }
}
