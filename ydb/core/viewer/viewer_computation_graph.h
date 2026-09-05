#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include <ydb/core/kqp/common/kqp.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonComputationGraph : public TViewerPipeClient {
    using TThis = TJsonComputationGraph;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;

    bool Json = false;

public:
    TJsonComputationGraph(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() override;
    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev);
    void ReplyAndPassAway() override;

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            default:
                return TBase::StateWork(ev);
        }
    }

    static YAML::Node GetSwagger();
};

} // namespace NKikimr::NViewer
