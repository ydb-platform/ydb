#pragma once
#include "defs.h"

#include <util/generic/string.h>

// VDisk prefix for log records
#define VDISK(...) ::NKikimr::AppendVDiskLogPrefix((Db->VCtx), __VA_ARGS__)
#define VDISKP(prefix, ...) ::NKikimr::AppendVDiskLogPrefix((prefix), __VA_ARGS__)


namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // Log Prefix
    ////////////////////////////////////////////////////////////////////////////
    class TVDiskContext;
    Y_PRINTF_FORMAT(2, 3)
    TString AppendVDiskLogPrefix(const TIntrusivePtr<TVDiskContext> &vctx, const char *c, ...);
    Y_PRINTF_FORMAT(2, 3)
    TString AppendVDiskLogPrefix(const TString &prefix, const char *c, ...);

    struct TVDiskID;
    TString GenerateVDiskLogPrefix(const TVDiskID &vdisk, bool donorMode);


    // logger
    using TLogger = std::function<void (NLog::EPriority priority,
            NLog::EComponent component,
            const TString &)>;

    ////////////////////////////////////////////////////////////////////////////
    // ILoggerCtx -- interface for logger context. Objects of this type can
    // be passed to LOG_* macro
    ////////////////////////////////////////////////////////////////////////////
    class ILoggerCtx {
    public:
        virtual void DeliverLogMessage(NLog::EPriority mPriority, NLog::EComponent mComponent, TString &&str, bool json) = 0;
        virtual NActors::NLog::TSettings* LoggerSettings() = 0;
        virtual ~ILoggerCtx() = default;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TActorSystemLoggerCtx -- implementation of ILoggerCtx via ActorSystem
    ////////////////////////////////////////////////////////////////////////////
    class TActorSystemLoggerCtx : public ILoggerCtx {
    public:
        TActorSystemLoggerCtx(TActorSystem *as)
            : ActorSystem(as)
        {}

        void DeliverLogMessage(NLog::EPriority mPriority, NLog::EComponent mComponent, TString &&str, bool json) override {
            ::NActors::DeliverLogMessage(*ActorSystem, mPriority, mComponent, std::move(str), json);
        }

        virtual NActors::NLog::TSettings* LoggerSettings() override {
            return ActorSystem->LoggerSettings();
        }
    private:
        TActorSystem *ActorSystem;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TFakeLoggerCtx -- fake implementation of ILoggerCtx
    ////////////////////////////////////////////////////////////////////////////
    class TFakeLoggerCtx : public ILoggerCtx {
    public:
        TFakeLoggerCtx();

        void DeliverLogMessage(NLog::EPriority mPriority, NLog::EComponent mComponent, TString &&str, bool /*json*/) override {
            Y_UNUSED(mPriority);
            Y_UNUSED(mComponent);
            Y_UNUSED(str);
        }

        virtual NActors::NLog::TSettings* LoggerSettings() override {
            return &Settings;
        }

    private:
        NActors::NLog::TSettings Settings;
    };

} // NKikimr

namespace NActors {

    template <>
    inline void DeliverLogMessage<NKikimr::ILoggerCtx>(
            NKikimr::ILoggerCtx& ctx,
            NLog::EPriority mPriority,
            NLog::EComponent mComponent,
            TString &&str,
            bool json)
    {
        ctx.DeliverLogMessage(mPriority, mComponent, std::move(str), json);
    }

} // NActors
