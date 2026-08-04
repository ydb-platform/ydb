#include <ydb/core/cms/console/console.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>

#include <library/cpp/monlib/service/mon_service_http_request.h>
#include <library/cpp/protobuf/json/json2proto.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace {

class TFuzzHttpRequest : public NMonitoring::IHttpRequest {
public:
    HTTP_METHOD Method = HTTP_METHOD_GET;
    TString Uri;
    TString Path;
    TString PostContent;
    TString RemoteAddr;
    TCgiParameters Params;
    TCgiParameters PostParams;
    THttpHeaders Headers;

    const char* GetURI() const override {
        return Uri.c_str();
    }

    const char* GetPath() const override {
        return Path.c_str();
    }

    const TCgiParameters& GetParams() const override {
        return Params;
    }

    const TCgiParameters& GetPostParams() const override {
        return PostParams;
    }

    TStringBuf GetPostContent() const override {
        return PostContent;
    }

    HTTP_METHOD GetMethod() const override {
        return Method;
    }

    const THttpHeaders& GetHeaders() const override {
        return Headers;
    }

    TString GetRemoteAddr() const override {
        return RemoteAddr;
    }
};

bool IsInterestingField(const NProtoBuf::FieldDescriptor* field) {
    if (field->cpp_type() != NProtoBuf::FieldDescriptor::CPPTYPE_STRING) {
        return false;
    }

    const TString name = field->name();
    return name.Contains("yaml") || name.Contains("config");
}

void MaybeParseYaml(TStringBuf data) {
    if (data.empty() || data.size() > 16 * 1024) {
        return;
    }

    try {
        NKikimr::NYaml::Parse(TString(data));
    } catch (...) {
    }
}

void ExerciseYamlFields(const NProtoBuf::Message& message, ui32 depth = 0) {
    if (depth > 3) {
        return;
    }

    const auto* descriptor = message.GetDescriptor();
    const auto* reflection = message.GetReflection();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const auto* field = descriptor->field(i);
        if (field->cpp_type() == NProtoBuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            if (field->is_repeated()) {
                const int count = reflection->FieldSize(message, field);
                for (int idx = 0; idx < count && idx < 8; ++idx) {
                    ExerciseYamlFields(reflection->GetRepeatedMessage(message, field, idx), depth + 1);
                }
            } else if (reflection->HasField(message, field)) {
                ExerciseYamlFields(reflection->GetMessage(message, field), depth + 1);
            }
            continue;
        }

        if (!IsInterestingField(field)) {
            continue;
        }

        if (field->is_repeated()) {
            const int count = reflection->FieldSize(message, field);
            for (int idx = 0; idx < count && idx < 8; ++idx) {
                MaybeParseYaml(reflection->GetRepeatedString(message, field, idx));
            }
        } else if (reflection->HasField(message, field)) {
            MaybeParseYaml(reflection->GetString(message, field));
        }
    }
}

template <typename TEvent>
void ParseConsoleJson(TStringBuf json) {
    typename TEvent::ProtoRecordType proto;
    try {
        NProtobufJson::Json2Proto(TString(json), proto);
    } catch (...) {
        return;
    }

    ExerciseYamlFields(proto);
    (void)proto.ByteSizeLong();
}

void ExerciseAdminRequest(TStringBuf path, TStringBuf body) {
    if (path == "/api/console/yamlconfig") {
        ParseConsoleJson<NKikimr::NConsole::TEvConsole::TEvGetAllConfigsRequest>(body);
    } else if (path == "/api/console/readonly") {
        ParseConsoleJson<NKikimr::NConsole::TEvConsole::TEvIsYamlReadOnlyRequest>(body);
    } else if (path == "/api/console/removevolatileyamlconfig") {
        ParseConsoleJson<NKikimr::NConsole::TEvConsole::TEvRemoveVolatileConfigRequest>(body);
    } else if (path == "/api/console/configureyamlconfig") {
        ParseConsoleJson<NKikimr::NConsole::TEvConsole::TEvReplaceYamlConfigRequest>(body);
    } else if (path == "/api/console/configurevolatileyamlconfig") {
        ParseConsoleJson<NKikimr::NConsole::TEvConsole::TEvAddVolatileConfigRequest>(body);
    } else if (path == "/api/console/resolveyamlconfig") {
        ParseConsoleJson<NKikimr::NConsole::TEvConsole::TEvResolveConfigRequest>(body);
    } else if (path == "/api/console/resolveallyamlconfig") {
        ParseConsoleJson<NKikimr::NConsole::TEvConsole::TEvResolveAllConfigRequest>(body);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);

    static constexpr TStringBuf Paths[] = {
        "/api/console/yamlconfig",
        "/api/console/readonly",
        "/api/console/removevolatileyamlconfig",
        "/api/console/configureyamlconfig",
        "/api/console/configurevolatileyamlconfig",
        "/api/console/resolveyamlconfig",
        "/api/console/resolveallyamlconfig",
        "/yaml-config-enabled",
    };

    TFuzzHttpRequest request;
    request.Method = fdp.ConsumeBool() ? HTTP_METHOD_POST : HTTP_METHOD_GET;
    request.Path = TString(Paths[fdp.ConsumeIntegralInRange<size_t>(0, Y_ARRAY_SIZE(Paths) - 1)]);
    const TString query = fdp.ConsumeRandomLengthString(128);
    request.Uri = query.empty() ? request.Path : TStringBuilder() << request.Path << "?" << query;
    request.PostContent = fdp.ConsumeRandomLengthString(16 * 1024);
    request.RemoteAddr = fdp.ConsumeBool() ? "127.0.0.1" : "::1";
    request.Headers.AddHeader("Content-Type", "application/json");
    request.Headers.AddHeader("Accept", fdp.ConsumeBool() ? "application/json" : "text/plain");
    request.Headers.AddHeader("Authorization", TStringBuilder() << "Bearer " << fdp.ConsumeRandomLengthString(64));

    NMonitoring::TMonService2HttpRequest monRequest(nullptr, &request, nullptr, nullptr, request.Path, nullptr);
    const TString userToken = fdp.ConsumeRandomLengthString(128);
    NActors::NMon::TEvHttpInfo event(monRequest, userToken);

    (void)event.Request.GetMethod();
    (void)event.Request.GetUri();
    (void)event.Request.GetPathInfo();
    (void)event.Request.GetHeaders();
    (void)event.Request.GetPostContent();
    (void)event.UserToken;

    try {
        ExerciseAdminRequest(event.Request.GetPathInfo(), event.Request.GetPostContent());
    } catch (...) {
    }

    return 0;
}
