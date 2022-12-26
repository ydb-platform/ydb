#include "http_download.h"

#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>
#include <ydb/library/yql/core/file_storage/http_download/proto/http_download.pb.h>
#include <ydb/library/yql/core/file_storage/download/download_stream.h>
#include <ydb/library/yql/core/file_storage/download/download_config.h>
#include <ydb/library/yql/core/file_storage/defs/downloader.h>
#include <ydb/library/yql/utils/fetch/fetch.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/log/context.h>
#include <ydb/library/yql/utils/md5_stream.h>
#include <ydb/library/yql/utils/retry.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/http/misc/httpcodes.h>

#include <util/generic/guid.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/system/file.h>
#include <util/system/env.h>


namespace NYql {

class THttpDownloader: public TDownloadConfig<THttpDownloader, THttpDownloaderConfig>, public NYql::NFS::IDownloader {
public:
    THttpDownloader(const TFileStorageConfig& config)
        : UseFakeChecksums(GetEnv("YQL_LOCAL") == "1")
    {
        Configure(config, "http");

    }
    ~THttpDownloader() = default;

    void DoConfigure(const THttpDownloaderConfig& cfg) {
        SocketTimeoutMs = cfg.GetSocketTimeoutMs();
    }

    bool Accept(const THttpURL& url) final {
        switch (url.GetScheme()) {
        case NUri::TScheme::SchemeHTTP:
        case NUri::TScheme::SchemeHTTPS:
            return true;
        default:
            break;
        }
        return false;
    }

    std::tuple<NYql::NFS::TDataProvider, TString, TString> Download(const THttpURL& url, const TString& token, const TString& oldEtag, const TString& oldLastModified) final {
        TFetchResultPtr fr1 = FetchWithETagAndLastModified(url, token, oldEtag, oldLastModified, SocketTimeoutMs);
        switch (fr1->GetRetCode()) {
        case HTTP_NOT_MODIFIED:
            return std::make_tuple(NYql::NFS::TDataProvider{}, TString{}, TString{});
        case HTTP_OK:
            break;
        default:
            ythrow yexception() << "Url " << url.PrintS() << " cannot be accessed, code: " << fr1->GetRetCode();
        }

        auto pair = ExtractETagAndLastModified(*fr1);

        auto puller = [urlStr = url.PrintS(), fr1, useFakeChecksums = UseFakeChecksums](const TFsPath& dstPath) -> std::pair<ui64, TString> {
            return CopyToFile(urlStr, *fr1, dstPath, useFakeChecksums);
        };

        return std::make_tuple(puller, pair.first, pair.second);
    }

private:
    static TFetchResultPtr FetchWithETagAndLastModified(const THttpURL& url, const TString& token, const TString& oldEtag, const TString& oldLastModified, ui32 socketTimeoutMs) {
        // more details about ETag and ModifiedSince: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.26
        THttpHeaders headers;
        if (!token.empty()) {
            headers.AddHeader(THttpInputHeader("Authorization", "OAuth " + token));
        }

        // ETag has priority over modification time
        if (!oldEtag.empty()) {
            headers.AddHeader(THttpInputHeader("If-None-Match", oldEtag));
        } else if (!oldLastModified.empty()) {
            headers.AddHeader(THttpInputHeader("If-Modified-Since", oldLastModified));
        }

        try {
            return Fetch(url, headers, TDuration::MilliSeconds(socketTimeoutMs));
        } catch (const std::exception& e) {
            // remap exception type to leverage retry logic
            throw TDownloadError() << e.what();
        }
    }

    static std::pair<ui64, TString> CopyToFile(const TString& url, IFetchResult& src, const TString& dstFile, bool useFakeChecksums) {
        TFile outFile(dstFile, CreateAlways | ARW | AX);
        THttpInput& httpStream = src.GetStream();
        TDownloadStream input(httpStream);
        ui64 size = 0;
        TString md5;
        if (useFakeChecksums) {
            TFileOutput out(outFile);
            size = TransferData(&input, &out);
            out.Finish();
        } else {
            TUnbufferedFileOutput out(outFile);
            TMd5OutputStream md5Out(out);
            size = TransferData(&input, &md5Out);
            md5 = md5Out.Finalize();
            out.Finish();
        }
        outFile.Close();

        ui64 contentLength = 0;
        // additional check for not compressed data
        if (!httpStream.ContentEncoded() && httpStream.GetContentLength(contentLength) && contentLength != size) {
            // let's retry this error
            ythrow TDownloadError() << "Size mismatch while downloading url " << url << ", downloaded size: " << size << ", ContentLength: " << contentLength;
        }

        i64 dstFileLen = GetFileLength(dstFile.c_str());
        if (dstFileLen == -1) {
            ythrow TSystemError() << "cannot get file length: " << dstFile;
        }

        YQL_ENSURE(static_cast<ui64>(dstFileLen) == size);
        return std::make_pair(size, md5);
    }

    static TString WeakETag2Strong(const TString& etag) {
        // drop W/ at the beginning if any
        return etag.StartsWith("W/") ? etag.substr(2) : etag;
    }

    static std::pair<TString, TString> ExtractETagAndLastModified(IFetchResult& result) {
        const auto& headers = result.GetStream().Headers();
        TString etag;
        TString lastModified;
        // linear scan
        for (auto it = headers.Begin(); it != headers.End(); ++it) {
            if (TCIEqualTo<TString>()(it->Name(), TString(TStringBuf("ETag")))) {
                etag = WeakETag2Strong(it->Value());
            }

            if (TCIEqualTo<TString>()(it->Name(), TString(TStringBuf("Last-Modified")))) {
                lastModified = it->Value();
            }
        }

        return std::make_pair(etag, lastModified);
    }

private:
    const bool UseFakeChecksums = false;
    ui32 SocketTimeoutMs = 300000;
};

NYql::NFS::IDownloaderPtr MakeHttpDownloader(const TFileStorageConfig& config) {
    return MakeIntrusive<THttpDownloader>(config);
}

} // NYql
