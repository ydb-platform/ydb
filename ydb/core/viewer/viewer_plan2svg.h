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

        try {
            TPlanVisualizer planViz;
            planViz.LoadPlans(plan);
            result = planViz.PrintSvg();
        } catch (std::exception& e) {
            TStringBuilder builder;
            builder
                << "<svg width='600' height='200' xmlns='http://www.w3.org/2000/svg'>" << Endl
                << "  <text font-size='16px' x='20' y='40'>Conversion error:</text>" << Endl
                << "  <text font-size='16px' x='20' y='80'>" << e.what() << "</text>" << Endl
                << "</svg>" << Endl;
            result = builder;
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
