#include "yql_mock.h"

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/fq/libs/actors/proxy.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/protobuf/json/json2proto.h>

namespace NFq {

using namespace NActors;
using namespace NThreading;

struct TMockLocation {
    TString Endpoint = "unconfigured_endpoint";
    TString Database = "unconfigured_database";
};

// actor used in tests for databaseId -> endpoint resolution
class TYqlMockActor : public NActors::TActor<TYqlMockActor> {
public:
    using TBase = NActors::TActor<TYqlMockActor>;

    TYqlMockActor(int grpcPort)
        : TBase(&TYqlMockActor::Handler)
    {
        Location.Database = "Root";
        Location.Endpoint = TStringBuilder() << "localhost:" << grpcPort;
    }

    static constexpr char ActorName[] = "YQL_MOCK";

private:
    STRICT_STFUNC(Handler, {
        hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
    });

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
    {
        auto request = event->Get()->Request;
        auto parameters = NHttp::TUrlParameters(request->URL);
        TString databaseId = parameters["databaseId"];

        NJson::TJsonValue json;
        bool ok = true;

        if (databaseId.Contains("FakeDatabaseId")) {
            json["endpoint"] = Location.Endpoint + "/?database=" + Location.Database;
        } else {
            ok = false;
            json["message"] = "Database not found";
        }

        TStringStream stream;
        NJson::WriteJson(&stream, &json);
        NHttp::THttpOutgoingResponsePtr response = (ok)
            ? request->CreateResponseOK(stream.Str(), "application/json; charset=utf-8")
            : request->CreateResponseNotFound(stream.Str(), "application/json; charset=utf-8");
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    TMockLocation Location;
};

void InitTest(NActors::TTestActorRuntime* runtime, int httpPort, int grpcPort, const IYqSharedResources::TPtr& yqSharedResources)
{
    yqSharedResources->Init(runtime->GetAnyNodeActorSystem());

    auto httpProxyId = NFq::MakeYqlAnalyticsHttpProxyId();

    TActorId mockActorId = runtime->Register(CreateYqlMockActor(grpcPort));

    runtime->Send(new NActors::IEventHandle(
                      httpProxyId, TActorId(),
                      new NHttp::TEvHttpProxy::TEvAddListeningPort(httpPort)), 0, true);

    runtime->Send(new NActors::IEventHandle(
                      httpProxyId, TActorId(),
                      new NHttp::TEvHttpProxy::TEvRegisterHandler("/yql-mock/abc/database", mockActorId)),
                  0, true);
}

NActors::IActor* CreateYqlMockActor(int grpcPort) {
    return new TYqlMockActor(grpcPort);
}

} // namespace NFq
