#pragma once

#include <ydb/mvp/core/mvp_test_runtime.h>

#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>

class TTestActorRuntime : public NActors::TTestActorRuntimeBase {
public:
    TTestActorRuntime() {
        Initialize();
    }
};

inline NHttp::THttpIncomingRequestPtr BuildHttpRequest(TStringBuf url, TStringBuf method = "GET") {
    NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
    EatWholeString(request, TStringBuilder() << method << " " << url << " HTTP/1.1\r\nHost: localhost\r\n\r\n");
    UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
    return request;
}

inline NJson::TJsonValue ParseJson(TStringBuf body) {
    NJson::TJsonReaderConfig jsonReaderConfig;
    NJson::TJsonValue json;
    UNIT_ASSERT(NJson::ReadJsonTree(TString(body), &jsonReaderConfig, &json));
    return json;
}

inline void AssertJsonEquals(TStringBuf actualBody, TStringBuf expectedBody) {
    const NJson::TJsonValue actualJson = ParseJson(actualBody);
    const NJson::TJsonValue expectedJson = ParseJson(expectedBody);
    UNIT_ASSERT_VALUES_EQUAL(NJson::WriteJson(actualJson, false, true), NJson::WriteJson(expectedJson, false, true));
}
