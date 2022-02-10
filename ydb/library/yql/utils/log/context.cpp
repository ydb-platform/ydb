#include "context.h"
#include "log.h"

#include <util/thread/singleton.h>


namespace NYql {
namespace NLog {
namespace {

struct TThrowedLogContext {
    TString LocationWithLogContext; // separated with ': ' 
};

} // namspace

void OutputLogCtx(IOutputStream* out, bool withBraces) {
    const NImpl::TLogContextListItem* ctxList = NImpl::GetLogContextList();

    if (ctxList->HasNext()) {
        if (withBraces) {
            (*out) << '{';
        }

        // skip header stub element
        NImpl::TLogContextListItem* ctxItem = ctxList->Next;

        { // special case for outputing the first element
            const TString* it = ctxItem->begin();
            const TString* end = ctxItem->end();

            (*out) << *it++;
            for (; it != end; ++it) {
                (*out) << '/' << *it;
            }

            ctxItem = ctxItem->Next;
        }

        // output remaining elements
        while (ctxItem != ctxList) {
            for (const TString& name: *ctxItem) {
                (*out) << '/' << name;
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

TString CurrentLogContextPath() { 
    TStringStream ss;
    OutputLogCtx(&ss, false);
    return ss.Str();
}

TString ThrowedLogContextPath() { 
    TThrowedLogContext* tlc = FastTlsSingleton<TThrowedLogContext>();
    return std::move(tlc->LocationWithLogContext);
}

TAutoPtr<TLogElement> TContextPreprocessor::Preprocess(
        TAutoPtr<TLogElement> element)
{
    OutputLogCtx(element.Get(), true);
    return element;
}

void TYqlLogContextLocation::SetThrowedLogContextPath() const {
    TStringStream ss;
    ss << Location_ << TStringBuf(": ");
    OutputLogCtx(&ss, true);
    TThrowedLogContext* tlc = FastTlsSingleton<TThrowedLogContext>();
    tlc->LocationWithLogContext = ss.Str();
}

} // namespace NLog
} // namespace NYql
