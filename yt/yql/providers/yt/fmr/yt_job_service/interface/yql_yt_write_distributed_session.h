#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

class IWriteDistributedSession : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IWriteDistributedSession>;
    virtual ~IWriteDistributedSession() = default;

    virtual TString GetId() const = 0;

    virtual std::vector<TString> GetCookies() const = 0;

    virtual void Finish(
        const std::vector<TString>& fragmentResultsYson) = 0;
};

} // namespace NYql::NFmr

