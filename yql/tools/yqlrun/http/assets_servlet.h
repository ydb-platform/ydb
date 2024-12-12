#pragma once

#include "servlet.h"


namespace NYql {
namespace NHttp {

///////////////////////////////////////////////////////////////////////////////
// TAssetsServlet
///////////////////////////////////////////////////////////////////////////////
class TAssetsServlet: public IServlet
{
    struct TAsset {
        TBlob Data;
        time_t LastModified;
        const char* ContentType;
    };

public:
    TAssetsServlet(
            const TString& baseUrl,
            const TString& baseDir,
            const TString& indexFile);

    void DoGet(const TRequest& req, TResponse& resp) const override final;

private:
    TString SafeFilePath(const TString& basePath, TStringBuf reqPath) const;
    bool IsCachedOnClient(const TRequest& req, const TAsset& asset) const;
    void LoadAsset(const TRequest& req, TAsset* asset) const;

private:
    TString BaseUrl_;
    TString BaseDir_;
    TString IndexFile_;
};


} // namspace NNttp
} // namspace NYql
