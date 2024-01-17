#include "html.h"
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // THtmlLightSignalRenderer
    ////////////////////////////////////////////////////////////////////////////
    const std::pair<TStringBuf, TStringBuf> THtmlLightSignalRenderer::Lights[NKikimrWhiteboard::EFlag_ARRAYSIZE] = {
        {"label", "background-color:grey"},
        {"label label-success", {}},
        {"label label-warning", {}},
        {"label", "background-color:orange"},
        {"label label-danger", {}},
    };

    void THtmlLightSignalRenderer::Output(IOutputStream &str) const {
        HTML(str) {
            SPAN_CLASS_STYLE (Lights[Light].first, Lights[Light].second) {
                str << Value;
            }
        }
    }

} // NKikimr
