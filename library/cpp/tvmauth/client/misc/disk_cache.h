#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NTvmAuth {
    class ILogger;

    class TDiskReader {
    public:
        TDiskReader(const TString& filename, ILogger* logger = nullptr);

        bool Read();

        const TString& Data() const {
            return Data_;
        }

        TInstant Time() const {
            return Time_;
        }

    public: // for tests
        bool ParseData(TStringBuf buf);

        static TString GetHash(TStringBuf data);
        static time_t GetTimestamp(TStringBuf data);

    private:
        TString Filename_;
        ILogger* Logger_;
        TInstant Time_;
        TString Data_;
    };

    class TDiskWriter {
    public:
        TDiskWriter(const TString& filename, ILogger* logger = nullptr);

        bool Write(TStringBuf data, TInstant now = TInstant::Now());

    public: // for tests
        static TString PrepareData(TInstant time, TStringBuf data);
        static TString WriteTimestamp(time_t time);

    private:
        TString Filename_;
        ILogger* Logger_;
    };
}
