#include "ut_helpers.h"

#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/retry/retry.h>

#include <util/system/guard.h>


namespace NYql::NDq {

using namespace NKikimr::NMiniKQL;

namespace {

void FillDqSolomonScheme(NSo::NProto::TDqSolomonShardScheme& scheme) {
    scheme.MutableTimestamp()->SetKey("ts");
    scheme.MutableTimestamp()->SetIndex(0);
    scheme.MutableTimestamp()->SetDataTypeId(NUdf::TDataType<NUdf::TTimestamp>::Id);

    NSo::NProto::TDqSolomonSchemeItem label;
    label.SetKey("label1");
    label.SetIndex(1);
    label.SetDataTypeId(NUdf::TDataType<ui32>::Id);

    NSo::NProto::TDqSolomonSchemeItem sensor;
    sensor.SetKey("sensor1");
    sensor.SetIndex(2);
    sensor.SetDataTypeId(NUdf::TDataType<ui32>::Id);

    scheme.MutableLabels()->Add(std::move(label));
    scheme.MutableSensors()->Add(std::move(sensor));
}

}

void InitAsyncOutput(
    TFakeCASetup& caSetup,
    NSo::NProto::TDqSolomonShard&& settings,
    i64 freeSpace)
{
    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    const THashMap<TString, TString> secureParams;

    caSetup.Execute([&](TFakeActor& actor) {
        auto [dqAsyncOutput, dqAsyncOutputAsActor] = CreateDqSolomonWriteActor(
            std::move(settings),
            0,
            NYql::NDq::TCollectStatsLevel::None,
            "TxId-42",
            secureParams,
            &actor.GetAsyncOutputCallbacks(),
            counters,
            nullptr,
            freeSpace);

        actor.InitAsyncOutput(dqAsyncOutput, dqAsyncOutputAsActor);
    });
}

void CleanupSolomon(TString cloudId, TString folderId, TString service, bool isCloud) {
    const auto solomonPort = TString(getenv("SOLOMON_PORT"));
    TSimpleHttpClient httpClient("localhost", std::stoi(solomonPort));
    TStringStream str;
    TStringBuilder builder;
    builder << "/cleanup";
    if (isCloud) {
        builder << "?folderId=" << folderId << "&service=" << service;
    } else {
        builder << "?project=" << cloudId << "&cluster=" << folderId << "&service=" << service;
    }

    DoWithRetry(
        [&]{ httpClient.DoPost(builder, "", &str); },
        TRetryOptions(3, TDuration::Seconds(1)),
        true);
}

TString GetSolomonMetrics(TString folderId, TString service) {
    const auto solomonPort = TString(getenv("SOLOMON_PORT"));
    TSimpleHttpClient httpClient("localhost", std::stoi(solomonPort));
    TStringStream str;
    httpClient.DoGet("/metrics?folderId=" + folderId + "&service=" + service, &str);
    return TString(str.Str());
}

NSo::NProto::TDqSolomonShard BuildSolomonShardSettings(bool isCloud) {
    NSo::NProto::TDqSolomonShard settings;
    settings.SetEndpoint(TStringBuilder() << getenv("SOLOMON_HOST") << ":" << TString(getenv("SOLOMON_PORT")));
    if (isCloud) {
        settings.SetProject("folderId1");
        settings.SetCluster("folderId1");
        settings.SetService("custom");
    } else {
        settings.SetProject("cloudId1");
        settings.SetCluster("folderId1");
        settings.SetService("custom");
    }

    settings.SetClusterType(isCloud ? NSo::NProto::ESolomonClusterType::CT_MONITORING : NSo::NProto::ESolomonClusterType::CT_SOLOMON);
    settings.SetUseSsl(false);

    FillDqSolomonScheme(*settings.MutableScheme());

    return settings;
}

NUdf::TUnboxedValue CreateStruct(
    NKikimr::NMiniKQL::THolderFactory& holderFactory,
    std::initializer_list<NUdf::TUnboxedValuePod> fields)
{
    NUdf::TUnboxedValue* itemsPtr = nullptr;
    auto structValues = holderFactory.CreateDirectArrayHolder(fields.size(), itemsPtr);
    for (auto&& field : fields) {
        *(itemsPtr++) = std::move(field);
    }
    return structValues;
}

int GetMetricsCount(TString metrics) {
    NJson::TJsonValue json;
    NJson::ReadJsonTree(metrics, &json, true);
    return json.GetArray().size();
}

} // namespace NYql::NDq
