#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/protos/actors.pb.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <util/stream/str.h>

using namespace NActors;
using namespace NMon;

Y_UNIT_TEST_SUITE(ActorSystemMon) {
    Y_UNIT_TEST(SerializeEv) {
        NActorsProto::TRemoteHttpInfo info;
        info.SetPath("hello");

        auto ev = std::make_unique<TEvRemoteHttpInfo>(info);
        UNIT_ASSERT(ev->ExtendedQuery);
        UNIT_ASSERT_VALUES_EQUAL(ev->ExtendedQuery->GetPath(), info.GetPath());
        UNIT_ASSERT_VALUES_EQUAL(ev->PathInfo(), info.GetPath());

        TAllocChunkSerializer ser;
        const bool success = ev->SerializeToArcadiaStream(&ser);
        Y_ABORT_UNLESS(success);
        auto buffer = ser.Release(ev->CreateSerializationInfo(false));
        std::unique_ptr<TEvRemoteHttpInfo> restored(TEvRemoteHttpInfo::Load(buffer.Get()));
        UNIT_ASSERT(restored->Query == ev->Query);
        UNIT_ASSERT(restored->Query.size());
        UNIT_ASSERT(restored->Query[0] == '\0');
        UNIT_ASSERT(restored->ExtendedQuery);
        UNIT_ASSERT_VALUES_EQUAL(restored->ExtendedQuery->GetPath(), ev->ExtendedQuery->GetPath());
        UNIT_ASSERT_VALUES_EQUAL(restored->PathInfo(), ev->PathInfo());
    }
}

namespace {
    template <class TEvent>
    std::unique_ptr<TEvent> RoundTrip(const TEvent& event) {
        TAllocChunkSerializer serializer;
        UNIT_ASSERT(event.IsSerializable());
        UNIT_ASSERT(event.SerializeToArcadiaStream(&serializer));
        auto buffer = serializer.Release(event.CreateSerializationInfo(false));
        UNIT_ASSERT_VALUES_EQUAL(buffer->GetSize(), event.CalculateSerializedSize());
        return std::unique_ptr<TEvent>(TEvent::Load(buffer.Get()));
    }
}

Y_UNIT_TEST_SUITE(ActorSystemMonContracts) {
    Y_UNIT_TEST(LegacyRequest) {
        TEvRemoteHttpInfo empty;
        UNIT_ASSERT(empty.PathInfo().empty());
        UNIT_ASSERT(empty.Cgi().empty());
        UNIT_ASSERT_VALUES_EQUAL(empty.GetMethod(), HTTP_METHOD_UNDEFINED);
        for (const TString& query : {TString(), TString("/actors"), TString("/actors?"),
                TString("/actors?a=one&a=two&encoded=x%26y+z&empty=")}) {
            TEvRemoteHttpInfo event(query, HTTP_METHOD_POST);
            UNIT_ASSERT(!event.ExtendedQuery);
            UNIT_ASSERT(event.GetUserToken().empty());
            UNIT_ASSERT(event.GetHeader("Cookie").empty());
            UNIT_ASSERT(event.GetCookie("session").empty());
            UNIT_ASSERT_VALUES_EQUAL(event.GetMethod(), HTTP_METHOD_POST);
            UNIT_ASSERT_VALUES_EQUAL(event.PathInfo(), query.Contains('?') ? "/actors" : "");
            auto restored = RoundTrip(event);
            UNIT_ASSERT_VALUES_EQUAL(restored->Query, query);
            // The legacy wire format contains only the query, not Method.
            UNIT_ASSERT_VALUES_EQUAL(restored->GetMethod(), HTTP_METHOD_UNDEFINED);
            UNIT_ASSERT_VALUES_EQUAL(restored->PathInfo(), event.PathInfo());
            if (query.Contains("a=")) {
                const auto params = restored->Cgi();
                UNIT_ASSERT_VALUES_EQUAL(params.NumOfValues("a"), 2);
                UNIT_ASSERT_VALUES_EQUAL(params.Get("a", 0), "one");
                UNIT_ASSERT_VALUES_EQUAL(params.Get("a", 1), "two");
                UNIT_ASSERT_VALUES_EQUAL(params.Get("encoded"), "x&y z");
                UNIT_ASSERT(params.Has("empty"));
            }
        }
    }

    Y_UNIT_TEST(ExtendedRequestMetadata) {
        NActorsProto::TRemoteHttpInfo info;
        info.SetPath("/actors/detail");
        info.SetMethod(HTTP_METHOD_GET);
        info.SetUserToken("serialized-token");
        for (const auto& value : {"one", "two"}) {
            auto* param = info.AddQueryParams();
            param->SetKey("key");
            param->SetValue(value);
        }
        auto* literal = info.AddQueryParams();
        literal->SetKey("literal");
        literal->SetValue("%26+ "); // protobuf parameters are already decoded
        auto* header = info.AddHeaders();
        header->SetName("X-Custom");
        header->SetValue("first");
        header = info.AddHeaders();
        header->SetName("x-custom");
        header->SetValue("second");
        header = info.AddHeaders();
        header->SetName("cOoKiE");
        header->SetValue("first=1;   session=a=b; empty=; bare; Session=upper");
        TEvRemoteHttpInfo event(info);
        auto restored = RoundTrip(event);
        for (const auto* request : {&event, restored.get()}) {
            UNIT_ASSERT_VALUES_EQUAL(request->PathInfo(), "/actors/detail");
            UNIT_ASSERT_VALUES_EQUAL(request->GetMethod(), HTTP_METHOD_GET);
            UNIT_ASSERT_VALUES_EQUAL(request->GetUserToken(), "serialized-token");
            UNIT_ASSERT_VALUES_EQUAL(request->GetHeader("X-CUSTOM"), "first");
            UNIT_ASSERT(request->GetHeader("Missing").empty());
            UNIT_ASSERT_VALUES_EQUAL(request->GetCookie("session"), "a=b");
            UNIT_ASSERT_VALUES_EQUAL(request->GetCookie("Session"), "upper");
            UNIT_ASSERT(request->GetCookie("empty").empty());
            UNIT_ASSERT(request->GetCookie("missing").empty());
            const auto params = request->Cgi();
            UNIT_ASSERT_VALUES_EQUAL(params.NumOfValues("key"), 2);
            UNIT_ASSERT_VALUES_EQUAL(params.Get("key", 0), "one");
            UNIT_ASSERT_VALUES_EQUAL(params.Get("key", 1), "two");
            UNIT_ASSERT_VALUES_EQUAL(params.Get("literal"), "%26+ ");
            UNIT_ASSERT_VALUES_EQUAL(request->ToStringHeader(), "TEvRemoteHttpInfo");
        }
    }

    Y_UNIT_TEST(HttpResponseRoundTrip) {
        for (const TString& html : {TString(), TString("<html>body</html>"), TString("binary\0tail", 11)}) {
            for (const TString& nonce : {TString(), TString("nonce"), TString("n\0n", 3)}) {
                TEvRemoteHttpInfoRes event(html);
                event.Nonce = nonce;
                auto restored = RoundTrip(event);
                UNIT_ASSERT_VALUES_EQUAL(restored->Html, html);
                UNIT_ASSERT_VALUES_EQUAL(restored->Nonce, nonce);
                UNIT_ASSERT_VALUES_EQUAL(restored->ToStringHeader(), "TEvRemoteHttpInfoRes");
            }
        }
        TEvRemoteHttpInfoRes empty;
        UNIT_ASSERT(empty.Html.empty());
    }

    Y_UNIT_TEST(TruncatedExtendedResponseFallsBackToOriginalBytes) {
        for (size_t size = 1; size <= sizeof(ui32); ++size) {
            const TString raw(size, '\0');
            TEventSerializedData data(raw, {});
            std::unique_ptr<TEvRemoteHttpInfoRes> event(TEvRemoteHttpInfoRes::Load(&data));
            UNIT_ASSERT_VALUES_EQUAL(event->Html, raw);
            UNIT_ASSERT(event->Nonce.empty());
        }
        const ui32 length = 4;
        TString raw(1, '\0');
        raw.append(reinterpret_cast<const char*>(&length), sizeof(length));
        raw.append("abc");
        TEventSerializedData data(raw, {});
        std::unique_ptr<TEvRemoteHttpInfoRes> event(TEvRemoteHttpInfoRes::Load(&data));
        UNIT_ASSERT_VALUES_EQUAL(event->Html, raw);
        UNIT_ASSERT(event->Nonce.empty());
    }

    Y_UNIT_TEST(JsonAndBinaryResponsesRoundTrip) {
        TEvRemoteJsonInfoRes json("{\"value\":42}");
        UNIT_ASSERT_VALUES_EQUAL(RoundTrip(json)->Json, json.Json);
        UNIT_ASSERT_VALUES_EQUAL(json.ToStringHeader(), "TEvRemoteJsonInfoRes");
        TEvRemoteBinaryInfoRes binary(TString("\0\xff\0data", 7));
        UNIT_ASSERT_VALUES_EQUAL(RoundTrip(binary)->Blob, binary.Blob);
        UNIT_ASSERT_VALUES_EQUAL(binary.ToStringHeader(), "TEvRemoteBinaryInfoRes");
        TEvRemoteJsonInfoRes emptyJson;
        TEvRemoteBinaryInfoRes emptyBinary;
        UNIT_ASSERT(RoundTrip(emptyJson)->Json.empty());
        UNIT_ASSERT(RoundTrip(emptyBinary)->Blob.empty());
    }

    Y_UNIT_TEST(LocalResponseOutputAndNonce) {
        NMonitoring::TMonService2HttpRequest request(nullptr, nullptr, nullptr, nullptr, "/actors", nullptr);
        TEvHttpInfo part(request, 7);
        TEvHttpInfo authenticated(request, TString("token"));
        TEvHttpInfo database(request, "token", "/Root/db");
        UNIT_ASSERT_VALUES_EQUAL(&part.Request, &request);
        UNIT_ASSERT_VALUES_EQUAL(part.SubRequestId, 7);
        UNIT_ASSERT(part.UserToken.empty());
        UNIT_ASSERT_VALUES_EQUAL(authenticated.UserToken, "token");
        UNIT_ASSERT_VALUES_EQUAL(authenticated.SubRequestId, 0);
        UNIT_ASSERT(authenticated.Database.empty());
        UNIT_ASSERT_VALUES_EQUAL(database.Database, "/Root/db");
        for (auto contentType : {IEvHttpInfoRes::Html, IEvHttpInfoRes::Custom}) {
            TEvHttpInfoRes response(TString("body\0tail", 9), 7, contentType);
            response.Nonce = "nonce";
            const IEvHttpInfoRes& polymorphic = response;
            TStringStream out;
            polymorphic.Output(out);
            UNIT_ASSERT_VALUES_EQUAL(out.Str(), response.Answer);
            UNIT_ASSERT(polymorphic.GetContentType() == contentType);
            UNIT_ASSERT_VALUES_EQUAL(polymorphic.GetNonce(), "nonce");
            UNIT_ASSERT_VALUES_EQUAL(response.SubRequestId, 7);
        }
        TEvHttpInfoRes defaults("body");
        UNIT_ASSERT(defaults.GetContentType() == IEvHttpInfoRes::Html);
        UNIT_ASSERT(defaults.GetNonce().empty());
        UNIT_ASSERT_VALUES_EQUAL(defaults.SubRequestId, 0);
    }

    Y_UNIT_TEST(ActorsLinkMergesParameters) {
        TCgiParameters current("keep=1&replace=old&replace=older&remove=x");
        const TString before = current.Print();
        const auto link = BuildActorsLink("/actors", current,
            {{"replace", "new&value"}, {"remove", ""}, {"added", "hello world"}});
        UNIT_ASSERT(link.StartsWith("/actors?"));
        TCgiParameters params(link.substr(TString("/actors?").size()));
        UNIT_ASSERT_VALUES_EQUAL(params.Get("keep"), "1");
        UNIT_ASSERT_VALUES_EQUAL(params.NumOfValues("replace"), 1);
        UNIT_ASSERT_VALUES_EQUAL(params.Get("replace"), "new&value");
        UNIT_ASSERT_VALUES_EQUAL(params.Get("added"), "hello world");
        UNIT_ASSERT(!params.Has("remove"));
        UNIT_ASSERT_VALUES_EQUAL(current.Print(), before);
        UNIT_ASSERT_VALUES_EQUAL(BuildActorsLink("/actors", TCgiParameters("a=1"), {{"a", ""}}), "/actors");
        UNIT_ASSERT_VALUES_EQUAL(BuildActorsLink("/actors", {}, {}), "/actors");
    }

    Y_UNIT_TEST(CspNonceHasGuidSizeInBase64) {
        const auto nonce = GenerateCspNonce();
        UNIT_ASSERT(!nonce.empty());
        UNIT_ASSERT_VALUES_EQUAL(Base64Decode(nonce).size(), 16);
        UNIT_ASSERT_VALUES_EQUAL(Base64Encode(Base64Decode(nonce)), nonce);
    }
}
