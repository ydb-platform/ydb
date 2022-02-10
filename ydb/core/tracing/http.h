#pragma once
#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/core/base/tracing.h>

#define COLLAPSED_REF_CONTENT(target, text) \
    WITH_SCOPED(tmp, NKikimr::NTracing::NHttp::TCollapsedRef(__stream, target, text))

#define COLLAPSED_REF_CONTENT_AJAX(target, tsInfo, text) \
    WITH_SCOPED(tmp, NKikimr::NTracing::NHttp::TCollapsedRefAjax(__stream, target, tsInfo, text))

namespace NKikimr {
namespace NTracing {
namespace NHttp {

struct TCollapsedRef {
    TCollapsedRef(IOutputStream& str, const TString& target, const TString& text);
    ~TCollapsedRef();

    explicit inline operator bool() const noexcept {
        return true; // just to work with WITH_SCOPED
    }

    IOutputStream& Str;
};

struct TCollapsedRefAjax {
    TCollapsedRefAjax(IOutputStream& str, const TString& target, const TTimestampInfo& tsInfo, const TString& text);
    ~TCollapsedRefAjax();

    explicit inline operator bool() const noexcept {
        return true; // just to work with WITH_SCOPED
    }

    IOutputStream& Str;
};

void OutputStyles(IOutputStream& os);
void OutputScripts(IOutputStream& str);
void OutputStaticPart(IOutputStream& str);
void OutputTimeDropdown(IOutputStream& str, const TTraceInfo& traceInfo);

}
}
}
