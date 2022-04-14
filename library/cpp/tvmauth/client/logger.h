#pragma once

#include <util/generic/ptr.h>

namespace NTvmAuth {
    class ILogger: public TAtomicRefCount<ILogger> {
    public:
        virtual ~ILogger() = default;

        void Debug(const TString& msg) {
            Log(7, msg);
        }

        void Info(const TString& msg) {
            Log(6, msg);
        }

        void Warning(const TString& msg) {
            Log(4, msg);
        }

        void Error(const TString& msg) {
            Log(3, msg);
        }

    protected:
        /*!
         * Log event
         * @param lvl is syslog level: 0(Emergency) ... 7(Debug)
         * @param msg
         */
        virtual void Log(int lvl, const TString& msg) = 0;
    };

    class TCerrLogger: public ILogger {
    public:
        TCerrLogger(int level)
            : Level_(level)
        {
        }

        void Log(int lvl, const TString& msg) override;

    private:
        const int Level_;
    };

    using TLoggerPtr = TIntrusivePtr<ILogger>;

    class TDevNullLogger: public ILogger {
    public:
        static TLoggerPtr IAmBrave() {
            return MakeIntrusive<TDevNullLogger>();
        }

        void Log(int, const TString&) override {
        }
    };
}
