#pragma once

#include <library/c/tvmauth/high_lvl_client.h>

#include <library/cpp/tvmauth/client/logger.h>

#include <util/generic/string.h>

namespace NTvmAuthC {
    class TLoggerC: public NTvmAuth::ILogger {
    public:
        TLoggerC(TA_TLoggerFunc f)
            : F_(f)
        {
        }

    private:
        void Log(int lvl, const TString& msg) override {
            F_(lvl, msg.c_str());
        }

    private:
        TA_TLoggerFunc F_;
    };
}
