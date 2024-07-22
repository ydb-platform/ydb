#include <unordered_map>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/json/json_writer.h>
#include "viewer.h"
#include "json_handlers.h"
#include "json_pipe_req.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TViewerCapabilities : public TViewerPipeClient {
public:
    using TThis = TViewerCapabilities;
    using TBase = TViewerPipeClient;

    TViewerCapabilities(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {}

    void Bootstrap() override {
        ReplyAndPassAway();
    }

    void ReplyAndPassAway() override {
        NJson::TJsonValue json;
        json["Capabilities"] = Viewer->GetCapabilities();
        TBase::ReplyAndPassAway(GetHTTPOKJSON(NJson::WriteJson(json, false)));
    }
};

template <>
YAML::Node TJsonRequestSwagger<TViewerCapabilities>::GetSwagger() {
    YAML::Node node = YAML::Load(R"___(
        post:
          tags:
          - viewer
          summary: Viewer capabilities
          description: Viewer capabilities
          responses:
            200:
              description: OK
              content:
                application/json:
                  schema:
                    type: object
                    description: format depends on schema parameter
            400:
              description: Bad Request
            403:
              description: Forbidden
            504:
              description: Gateway Timeout
            )___");
    return node;
}


void InitViewerCapabilitiesJsonHandler(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/capabilities", new TJsonHandler<TViewerCapabilities>());
}

} // namespace NViewer
} // namespace NKikimr
