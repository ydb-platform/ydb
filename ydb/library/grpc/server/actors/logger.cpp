#include "logger.h"

namespace NYdbGrpc {
namespace {

static_assert(
        ui16(TLOG_EMERG) == ui16(NActors::NLog::PRI_EMERG) &&
        ui16(TLOG_DEBUG) == ui16(NActors::NLog::PRI_DEBUG),
        "log levels in the library/log and library/cpp/actors don't match");

class TActorSystemLogger final: public TLogger {
public:
    TActorSystemLogger(NActors::TActorSystem& as, NActors::NLog::EComponent component) noexcept
        : ActorSystem_{as}
        , Component_{component}
    {
    }

protected:
    bool DoIsEnabled(ELogPriority p) const noexcept override {
        const auto* settings = static_cast<::NActors::NLog::TSettings*>(ActorSystem_.LoggerSettings());
        const auto priority = static_cast<::NActors::NLog::EPriority>(p);

        return settings && settings->Satisfies(priority, Component_, 0);
    }

    void DoWrite(ELogPriority p, const char* format, va_list args) noexcept override {
        Y_DEBUG_ABORT_UNLESS(DoIsEnabled(p));

        const auto priority = static_cast<::NActors::NLog::EPriority>(p);
        ::NActors::MemLogAdapter(
            ActorSystem_,
            priority,
            Component_,
            __FILE_NAME__,
            __LINE__,
            format,
            args);
    }

private:
    NActors::TActorSystem& ActorSystem_;
    NActors::NLog::EComponent Component_;
};

} // namespace

TLoggerPtr CreateActorSystemLogger(NActors::TActorSystem& as, NActors::NLog::EComponent component) {
    return MakeIntrusive<TActorSystemLogger>(as, component);
}

} // namespace NYdbGrpc
