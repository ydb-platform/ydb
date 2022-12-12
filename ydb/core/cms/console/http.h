#pragma once
#include "defs.h"

#include <ydb/core/protos/config.pb.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/output.h>

#define COLLAPSED_REF_CONTENT(target, text) \
    WITH_SCOPED(tmp, NKikimr::NConsole::NHttp::TCollapsedRef(__stream, target, text))

namespace NKikimr::NConsole::NHttp {

struct TCollapsedRef {
    TCollapsedRef(IOutputStream& str, const TString& target, const TString& text)
        : Str(str)
    {
        Str << "<a class='collapse-ref' data-toggle='collapse' data-target='#" << target << "'>"
            << text << "</a>";
        Str << "<div id='" << target << "' class='collapse'>";
    }

    ~TCollapsedRef()
    {
        try {
            Str << "</div>";
        } catch (...) {
        }
    }

    explicit inline operator bool() const noexcept
    {
        return true; // just to work with WITH_SCOPED
    }

    IOutputStream &Str;
};

void OutputStaticPart(IOutputStream &os);
void OutputStyles(IOutputStream &os);
void OutputConfigHTML(IOutputStream &os, const NKikimrConfig::TAppConfig &config);

} // namespace NKikimr::NConsole::NHttp
