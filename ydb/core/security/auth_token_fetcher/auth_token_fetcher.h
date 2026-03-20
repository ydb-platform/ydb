#pragma once

#include <memory>

#include <util/generic/string.h>

namespace NHttp {

////////////////////////////////////////////////////////////////////////////////

struct THeaders;

}

namespace NKikimr::NSecurity {

////////////////////////////////////////////////////////////////////////////////

class TAuthTokenFetcher {
public:
    virtual ~TAuthTokenFetcher() = default;

    virtual TString GetAuthToken(const NHttp::THeaders& headers) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

using TAuthTokenFetcherPtr = std::shared_ptr<TAuthTokenFetcher>;

TAuthTokenFetcherPtr CreateAuthTokenFetcherStub();
TAuthTokenFetcherPtr CreateAuthTokenFetcherDefault();

} // namespace NKikimr::NSecurity