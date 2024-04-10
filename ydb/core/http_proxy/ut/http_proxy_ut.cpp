#include <ydb/core/http_proxy/http_req.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/library/testlib/service_mocks/access_service_mock.h>
#include <ydb/library/testlib/service_mocks/iam_token_service_mock.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <nlohmann/json.hpp>


using namespace NKikimr::NHttpProxy;
using namespace NKikimr::Tests;
using namespace NActors;


#include "datastreams_fixture.h"


void openMonitoringsInBrowser(const auto& kikimrServer, ui16 monPort) {
    auto cmd = TStringBuilder() <<
        "browse http://127.0.0.1:" << monPort <<
        " && browse http://127.0.0.1:" << kikimrServer->Server_->GetRuntime()->GetMonPort();
        std::system(cmd.c_str());
        Sleep(TDuration::Seconds(300));
}

void PatchRateBins(NJson::TJsonValue& json)
{
    struct TValue {
        i64 Sum = 0; // sum of /value
        TString Bin; // min of /labels/bin
    };

    THashMap<TString, TValue> values; // aggregates for /labels/name

    for (auto& sensor : json["sensors"].GetArraySafe()) {
        if ((GetByPath<TString>(sensor, "kind") != "RATE") || !GetByPath<TJMap>(sensor, "labels").contains("bin")) {
            continue;
        }

        auto& v = values[sensor["labels"]["name"].GetStringSafe()];

        v.Sum += GetByPath<i64>(sensor, "value");
        sensor["value"].SetValue(0);

        auto bin = sensor["labels"]["bin"].GetStringSafe();
        if (v.Bin.empty() || (v.Bin > bin)) {
            v.Bin = bin;
        }
    }

    for (auto& sensor : json["sensors"].GetArraySafe()) {
        if ((GetByPath<TString>(sensor, "kind") != "RATE") || !GetByPath<TJMap>(sensor, "labels").contains("bin")) {
            continue;
        }

        auto& v = values[sensor["labels"]["name"].GetStringSafe()];

        if (sensor["labels"]["bin"].GetStringSafe() == v.Bin) {
            sensor["value"].SetValue(v.Sum);
        }
    }
}

void PatchHistRateBuckets(NJson::TJsonValue& json)
{
    // Run time of Create method differs from run to run, so we only check here
    // the fact of its presence (sum == 1) and zero it as reference json contains
    // buckets with 0 values
    for (auto &sensor : json["sensors"].GetArraySafe()) {
        if (GetByPath<TString>(sensor, "kind") == "HIST_RATE" &&
            GetByPath<TJMap>(sensor, "labels").contains("method") &&
            GetByPath<TJMap>(sensor, "hist").contains("buckets")) {
            ui32 sum{0};
            for (auto &bucket : sensor["hist"]["buckets"].GetArraySafe()) {
                sum += bucket.GetIntegerSafe();
                bucket.SetValue(0);
            }
            sensor["hist"]["buckets"].GetArraySafe().begin()->SetValue(sum);
        }
    }
}

void compareJsons(ui16 port, const TString& query, const TString& referenceFile) {
    TString referenceJsonStr = NResource::Find(TStringBuf(referenceFile));
    NJson::TJsonValue referenceJson;
    UNIT_ASSERT(NJson::ReadJsonTree(TStringBuf(referenceJsonStr), &referenceJson));

    NJson::TJsonValue inputJson = NKikimr::NPersQueueTests::SendQuery(port, query);

    PatchHistRateBuckets(inputJson);
    PatchRateBins(inputJson);

    TVector<TString> input;
    for (auto &sensor : inputJson["sensors"].GetArraySafe()) {
        auto str = NJson::WriteJson(sensor, /*formatOutput*/ true, /*sortkeys*/ true, /*validateUtf8*/ true);
        input.push_back(str);
    }
    std::sort(input.begin(), input.end());

    Cerr << "RESULT:\n" <<  NJson::WriteJson(inputJson, /*formatOutput*/ true, /*sortkeys*/ true, /*validateUtf8*/ true) << "\nRESULT\n";

    TVector<TString> reference;
    for (auto &sensor : referenceJson["sensors"].GetArraySafe()) {
        auto str = NJson::WriteJson(sensor, /*formatOutput*/ true, /*sortkeys*/ true, /*validateUtf8*/ true);
        reference.push_back(str);
    }
    std::sort(reference.begin(), reference.end());

    Cerr <<  "reference file: " << referenceFile << Endl;

    TStringBuilder inputStr, referenceStr;
    for (ui32 i = 0; i < input.size(); ++i) {
        inputStr << input[i] << "\n";
    }
    for (ui32 i = 0; i < reference.size(); ++i) {
        referenceStr << reference[i] << "\n";
    }

    Cerr <<  "ref: " << referenceStr << Endl;
    Cerr <<  "input: " << inputStr << Endl;

    UNIT_ASSERT_VALUES_EQUAL(referenceStr, inputStr);
    UNIT_ASSERT_VALUES_EQUAL(input.size(), reference.size());

}

#include "http_proxy_ut.h"
