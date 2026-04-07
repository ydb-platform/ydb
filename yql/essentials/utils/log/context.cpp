#include "context.h"
#include "log.h"

#include <util/thread/singleton.h>

namespace NYql::NLog {
namespace {

struct TThrowedLogContext {
    TString LocationWithLogContext; // separated with ': '
};

} // namespace

TStringBuf ToStringBuf(EContextKey key) {
    switch (key) {
        case EContextKey::DateTime:
            return "datetime";
        case EContextKey::Level:
            return "level";
        case EContextKey::ProcessName:
            return "procname";
        case EContextKey::ProcessID:
            return "pid";
        case EContextKey::ThreadID:
            return "tid";
        case EContextKey::Component:
            return "component";
        case EContextKey::FileName:
            return "filename";
        case EContextKey::Line:
            return "line";
        case EContextKey::Path:
            return "path";
    }
}

void OutputLogCtx(IOutputStream* out, bool withBraces, bool skipSessionId) {
    const NImpl::TLogContextListItem* ctxList = NImpl::GetLogContextList();

    if (ctxList->HasNext()) {
        if (withBraces) {
            (*out) << '{';
        }

        // skip header stub element
        NImpl::TLogContextListItem* ctxItem = ctxList->Next;

        bool isFirst = true;
        while (ctxItem != ctxList) {
            for (const TString& name : *ctxItem) {
                if (!skipSessionId && !name.empty()) {
                    if (!isFirst) {
                        (*out) << '/';
                    }
                    (*out) << name;
                    isFirst = false;
                }
                skipSessionId = false;
            }
            ctxItem = ctxItem->Next;
        }

        if (withBraces) {
            (*out) << TStringBuf("} ");
        }
    }
}

NImpl::TLogContextListItem* NImpl::GetLogContextList() {
    return FastTlsSingleton<NImpl::TLogContextListItem>();
}

std::pair<TString, TString> CurrentLogContextPath() {
    TString sessionId;
    const NImpl::TLogContextListItem* possibleRootLogCtx = NImpl::GetLogContextList()->Next;
    if (auto rootLogCtx = dynamic_cast<const NImpl::TLogContextSessionItem*>(possibleRootLogCtx)) {
        if (rootLogCtx->HasSessionId()) {
            sessionId = (*rootLogCtx->begin());
        }
    }

    TStringStream ss;
    OutputLogCtx(&ss, false, !sessionId.empty());
    return std::make_pair(sessionId, ss.Str());
}

TString ThrowedLogContextPath() {
    TThrowedLogContext* tlc = FastTlsSingleton<TThrowedLogContext>();
    return std::move(tlc->LocationWithLogContext);
}

TAutoPtr<TLogElement> TContextPreprocessor::Preprocess(TAutoPtr<TLogElement> element)
{
    TStringStream out;
    OutputLogCtx(&out, false);

    if (!out.Empty()) {
        element->With(ToStringBuf(EContextKey::Path), std::move(out.Str()));
    }

    return element;
}

void TYqlLogContextLocation::SetThrowedLogContextPath() const {
    TStringStream ss;
    ss << Location_ << TStringBuf(": ");
    OutputLogCtx(&ss, true);
    TThrowedLogContext* tlc = FastTlsSingleton<TThrowedLogContext>();
    tlc->LocationWithLogContext = ss.Str();
}

} // namespace NYql::NLog
