#include "helpers.h"

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT component

namespace NYdb::NBS {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString EventInfo(const IEventHandle& ev)
{
    return ev.GetTypeName();
}

[[maybe_unused]] void LogUnexpectedEvent(
    const IEventHandle& ev,
    int component,
    const TString& location)
{
    YDB_LOG_ERROR_CTX(*TlsActivationContext, "Unexpected event: (0x%08X)",
        {"#_ev.GetTypeRewrite", ev.GetTypeRewrite()},
        {"#_EventInfo(ev).c_str", EventInfo(ev)},
        {"location", location});
}

void HandleUnexpectedEvent(
    const IEventHandle& ev,
    int component,
    const TString& location)
{
#if defined(NDEBUG)
    LogUnexpectedEvent(ev, component, location);
#else
    Y_ABORT(
        "[%s] Unexpected event: (0x%08X) %s, %s",
        TlsActivationContext->LoggerSettings()->ComponentName(component),
        ev.GetTypeRewrite(),
        EventInfo(ev).c_str(),
        location.c_str());
#endif
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void HandleUnexpectedEvent(
    const TAutoPtr<IEventHandle>& ev,
    int component,
    const TString& location)
{
    HandleUnexpectedEvent(*ev, component, location);
}

void HandleUnexpectedEvent(
    const NActors::IEventHandlePtr& ev,
    int component,
    const TString& location)
{
    HandleUnexpectedEvent(*ev, component, location);
}

void LogUnexpectedEvent(
    const TAutoPtr<IEventHandle>& ev,
    int component,
    const TString& location)
{
    LogUnexpectedEvent(*ev, component, location);
}

}   // namespace NYdb::NBS
