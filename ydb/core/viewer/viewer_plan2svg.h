#pragma once
#include "json_pipe_req.h"
#include "viewer.h"
#include <ydb/library/yaml_config/yaml_config.h>

#include <ydb/public/lib/ydb_cli/common/plan2svg.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonPlanToSvg : public TActorBootstrapped<TJsonPlanToSvg> {

    IViewer* Viewer = nullptr;
    NMon::TEvHttpInfo::TPtr Event;

public:
    TJsonPlanToSvg(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() {
        TString plan(Event->Get()->Request.GetPostContent());
        TString result;

        TRequestState state(Event->Get());
        try {
            TPlanVisualizer planViz;
            planViz.LoadPlans(plan);
            result = Viewer->GetHTTPOK(state, "image/svg+xml", planViz.PrintSvg());
        } catch (std::exception& e) {
            result = Viewer->GetHTTPBADREQUEST(state, "text/plain", TStringBuilder() << "Conversion error: " << e.what());
        }

        Send(Event->Sender, new NMon::TEvHttpInfoRes(result, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "post",
            .Tag = "viewer",
            .Summary = "Plan2svg converter",
            .Description = "Renders plan json to svg image"
        });
        return yaml;
    }
};

}
