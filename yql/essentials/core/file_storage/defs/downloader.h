#pragma once

#include "provider.h"

#include <library/cpp/uri/http_url.h>

#include <util/generic/string.h>
#include <util/generic/ptr.h>

#include <tuple>

namespace NYql::NFS {

struct IDownloader : public TThrRefBase {
    virtual bool Accept(const THttpURL& url) = 0;
    virtual std::tuple<TDataProvider, TString, TString> Download(const THttpURL& url, const TString& token, const TString& etag, const TString& lastModified) = 0;
};
using IDownloaderPtr = TIntrusivePtr<IDownloader>;

} // NYql
