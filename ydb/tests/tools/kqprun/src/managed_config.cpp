#include "managed_config.h"

#include <ydb/core/base/tabletid.h>
#include <ydb/core/cms/console/events/console.h>

#include <ydb/library/yaml_config/util.h>

#include <library/cpp/json/json_writer.h>

namespace NKqpRun {

void InitializeManagedConfig(NActors::TTestActorRuntime& runtime, const NKikimrConfig::TAppConfig& startup, TDuration timeout) {
    namespace TEvConsole = NKikimr::NConsole::TEvConsole;
    const auto edge = runtime.AllocateEdgeActor();
    runtime.SendToPipe(NKikimr::MakeConsoleID(), edge, new TEvConsole::TEvGetAllConfigsRequest());
    TAutoPtr<NActors::IEventHandle> handle;
    const auto [fetched, fetchError] =
        runtime.GrabEdgeEventsRethrow<TEvConsole::TEvGetAllConfigsResponse, TEvConsole::TEvGenericError>(handle, timeout);
    Y_ENSURE(fetched && !fetchError, "Cannot fetch managed configuration from Console");
    const auto& response = fetched->Record.GetResponse();
    Y_ENSURE(response.config_size() == response.identity_size(), "Invalid managed configuration response");

    for (int i = 0; i < response.identity_size(); ++i) {
        const auto& identity = response.identity(i);
        if (identity.has_database()) {
            continue;
        }

        if (!response.config(i).empty()) {
            return;
        }

        NJson::TJsonValue document(NJson::JSON_MAP);
        document["metadata"]["kind"] = "MainConfig";
        document["metadata"]["cluster"] = identity.cluster();
        document["metadata"]["version"] = identity.version();
        NProtobufJson::Proto2Json(startup, document["config"], NKikimr::NYamlConfig::GetProto2JsonConfig());
        document["config"]["yaml_config_enabled"] = true;

        for (const auto* label : { "host", "node_id", "tenant" }) {
            document["allowed_labels"][label]["type"] = "string";
        }

        document["selector_config"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        auto request = MakeHolder<TEvConsole::TEvReplaceYamlConfigRequest>();
        request->Record.MutableRequest()->set_config(NJson::WriteJson(document, /* formatOutput */ true));
        runtime.SendToPipe(NKikimr::MakeConsoleID(), edge, request.Release());
        handle.Reset();
        const auto [replaced, replaceError] =
            runtime.GrabEdgeEventsRethrow<TEvConsole::TEvReplaceYamlConfigResponse, TEvConsole::TEvGenericError>(handle, timeout);
        Y_ENSURE(replaced && !replaceError, "Cannot initialize managed YAML from kqprun startup settings");
        return;
    }

    ythrow yexception() << "Console returned no main configuration identity";
}

}   // namespace NKqpRun
