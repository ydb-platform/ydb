#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/logger/log.h>

namespace NYdbWorkload {
namespace NTPCC {

class TErrorException : public yexception {
public:
    TErrorException(NYdb::TStatus&& status, TString&& errorPrefix = "")
        : Status(std::move(status))
        , ErrorPrefix(std::move(errorPrefix))
    {}

    friend IOutputStream& operator<<(IOutputStream& out, const TErrorException& e) {
        out << e.ErrorPrefix << ": " << "Status: " << e.Status.GetStatus() << Endl;
        if (e.Status.GetIssues()) {
            out << "Issues: " << Endl;
            e.Status.GetIssues().PrintTo(out);
        }
        return out;
    }

    void AddPrefix(const TString& errorPrefix) {
        ErrorPrefix = errorPrefix + (errorPrefix.Empty() ? "" : ": ") + ErrorPrefix;
    }

    NYdb::EStatus GetStatus() const {
        return Status.GetStatus();
    }

private:
    NYdb::TStatus Status;
    TString ErrorPrefix;
};

template<typename T, typename = std::enable_if_t<std::is_base_of_v<NYdb::TStatus, T>>>
inline void ThrowOnError(T&& status, TString&& errorPrefix, TLog& log) {
    if (!status.IsSuccess()) {
        throw TErrorException(std::move(status), std::move(errorPrefix));
    } else if (status.GetIssues()) {
        log.Write(TLOG_WARNING, status.GetIssues().ToString());
    }
}

template<typename T>
inline void ThrowOnError(T result, TLog& log, TString errorPrefix = "") {
    ThrowOnError(std::move(result), std::move(errorPrefix), log);
}
    
}
}
