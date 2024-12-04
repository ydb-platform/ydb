#include "assets_servlet.h"

#include <library/cpp/http/misc/httpdate.h>
#include <library/cpp/mime/types/mime.h>
#include <library/cpp/uri/uri.h>

#include <util/system/fstat.h>
#include <util/system/file.h>


namespace NYql {
namespace NHttp {

///////////////////////////////////////////////////////////////////////////////
// TAssetsServlet
///////////////////////////////////////////////////////////////////////////////
TAssetsServlet::TAssetsServlet(
        const TString& baseUrl,
        const TString& baseDir,
        const TString& indexFile)
    : BaseUrl_(baseUrl)
    , BaseDir_(baseDir)
    , IndexFile_(indexFile)
{
}

void TAssetsServlet::DoGet(const TRequest& req, TResponse& resp) const
{
    TAsset asset;
    LoadAsset(req, &asset);

    TString lastModified = FormatHttpDate(asset.LastModified);
    resp.Headers.AddHeader("Last-Modified", lastModified);
    if (asset.ContentType) {
        resp.ContentType = asset.ContentType;
    }
    resp.Body = asset.Data;
}

TString TAssetsServlet::SafeFilePath(const TString& basePath, TStringBuf reqPath) const
{
    Y_ENSURE_EX(reqPath.Head(BaseUrl_.size()) == BaseUrl_,
              THttpError(HTTP_BAD_REQUEST) << "invalid url prefix");

    size_t shift = BaseUrl_ == TStringBuf("/") ? 0 : BaseUrl_.size();
    TStringBuf skipped = reqPath.Skip(shift);
    const char* p = skipped.data();
    size_t len = skipped.size();

    if (p[0] != '/' || (len > 2 && p[1] == '.' && p[2] == '.'))
        ythrow THttpError(HTTP_BAD_REQUEST);

    TString buf(skipped.data(), len);
    char* b = buf.begin();
    char* e = b + len;
    ::NUri::TUri::PathOperation(b, e, 1);

    return basePath + TStringBuf(b, e);
}

bool TAssetsServlet::IsCachedOnClient(const TRequest& req, const TAsset& asset) const
{
    const TString* value = req.RD.HeaderIn("If-Modified-Since");
    if (value) {
        time_t mt = parse_http_date(*value);
        return (mt >= asset.LastModified);
    }

    return false;
}

void TAssetsServlet::LoadAsset(const TRequest& req, TAsset* asset) const
{
    TString filePath = SafeFilePath(BaseDir_, req.RD.ScriptName());

    TFileStat stat(filePath);
    if (stat.IsDir()) {
        filePath += "/" + IndexFile_;
        stat = TFileStat(filePath);
    }

    if (!stat.IsFile()) {
        ythrow THttpError(HTTP_NOT_FOUND)
                << "File '" << filePath << "' not found";
    }

    asset->LastModified = stat.MTime;
    asset->ContentType = mimetypeByExt(filePath.data());

    if (IsCachedOnClient(req, *asset)) {
        ythrow THttpError(HTTP_NOT_MODIFIED);
    }

    TFile file(filePath, EOpenModeFlag::RdOnly);
    asset->Data = TBlob::FromFile(file);
}

} // namspace NNttp
} // namspace NYql
