#pragma once

#include "defs.h"
#include <ydb/core/protos/node_whiteboard.pb.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // THtmlLightSignalRenderer is good routine for generating light signals
    // while rendering you html page
    // Usage:
    // 1. THtmlLightSignalRenderer(THtmlLightSignalRenderer::Green, "Ok").Output(stream)
    // 2. THtmlLightSignalRenderer(THtmlLightSignalRenderer::Red, "Error").Output(stream)
    ////////////////////////////////////////////////////////////////////////////
    class THtmlLightSignalRenderer {
    public:
        THtmlLightSignalRenderer(NKikimrWhiteboard::EFlag light, const TString &value)
            : Light(light)
            , Value(value)
        {}

        void Output(IOutputStream &str) const;

    private:
        NKikimrWhiteboard::EFlag Light;
        const TString Value;
        static const std::pair<TStringBuf, TStringBuf> Lights[NKikimrWhiteboard::EFlag_ARRAYSIZE]; // {class, style}
    };

} // NKikimr
