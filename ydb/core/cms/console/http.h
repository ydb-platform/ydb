#pragma once
#include "defs.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/config/init/init.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/output.h>

#define COLLAPSED_REF_CONTENT(target, text, ...) \
    WITH_SCOPED(tmp, NKikimr::NConsole::NHttp::TCollapsedRef(__stream, target, text __VA_OPT__(,) __VA_ARGS__))

namespace NKikimr::NConsole::NHttp {

using namespace NConfig;

struct TCollapsedRef {
    template <class... TClasses>
    TCollapsedRef(IOutputStream& str, const TString& target, const TString& text, TClasses... classes)
        : Str(str)
    {
        Str << "<a class='collapse-ref";
        void *dummy[sizeof...(TClasses)] = {(void*)&(Str << " " << classes)...};
        Y_UNUSED(dummy);
        Str << "' data-toggle='collapse' data-target='#" << target << "'>"
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
void OutputRichConfigHTML(
    IOutputStream &os,
    const NKikimrConfig::TAppConfig &initialConfig,
    const NKikimrConfig::TAppConfig &yamlConfig,
    const NKikimrConfig::TAppConfig &protoConfig,
    const THashSet<ui32> &dynamicKinds,
    const THashSet<ui32> &nonYamlKinds,
    bool yamlEnabled);
void OutputConfigDebugInfoHTML(
    IOutputStream &os,
    const NKikimrConfig::TAppConfig &initialConfig,
    const NKikimrConfig::TAppConfig &yamlConfig,
    const NKikimrConfig::TAppConfig &protoConfig,
    const THashMap<ui32, TConfigItemInfo>& configInitInfo,
    const THashSet<ui32> &dynamicKinds,
    const THashSet<ui32> &nonYamlKinds,
    bool yamlEnabled);


} // namespace NKikimr::NConsole::NHttp
