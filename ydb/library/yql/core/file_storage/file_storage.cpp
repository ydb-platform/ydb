#include "file_storage.h"
#include "file_exporter.h"
#include "download_stream.h"
#include "pattern_group.h"
#include "storage.h"
#include "url_mapper.h"
#include "url_meta.h"

#include <ydb/library/yql/utils/fetch/fetch.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/yql/utils/multi_resource_lock.h>
#include <ydb/library/yql/utils/md5_stream.h>
#include <ydb/library/yql/utils/retry.h>

#include <library/cpp/cache/cache.h>
#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/cgiparam/cgiparam.h>

#include <util/generic/guid.h>
#include <util/folder/dirut.h>
#include <util/stream/file.h>
#include <util/stream/null.h>
#include <util/string/strip.h>
#include <util/system/fs.h>
#include <util/system/fstat.h>
#include <util/system/guard.h>
#include <util/system/shellcommand.h>
#include <util/system/sysstat.h>
#include <util/system/utime.h>

#include <tuple>

namespace NYql {
class TFileStorageImpl: public IFileStorage {
public:
    explicit TFileStorageImpl(const TFileStorageConfig& params)
        : Storage(params.GetMaxFiles(), ui64(params.GetMaxSizeMb()) << 20ull, params.GetPath())
        , Config(params)
        , QueueStarted(0)
        , SupportArcLinks(true)
        , FileExporter(CreateFileExporter())
    {
        try {
            for (const auto& sc : params.GetCustomSchemes()) {
                Mapper.AddMapping(sc.GetPattern(), sc.GetTargetUrl());
            }

            if (params.GetExternalAllowedUrlPatterns().empty()) {
                InitUrlPatterns(false);
            }
        } catch (const yexception& e) {
            ythrow yexception() << "FileStorage: " << e.what();
        }

        auto numThreads = params.GetThreads();
        if (1 == numThreads) {
            MtpQueue.Reset(new TFakeThreadPool());
        } else {
            MtpQueue.Reset(new TSimpleThreadPool(TThreadPoolParams{"FileStorage"}));
        }

        // do not call MtpQueue->Start here as we have to do it _after_ fork
    }

    ~TFileStorageImpl() {
        MtpQueue->Stop();
    }

    void AddAllowedUrlPattern(const TString& pattern) override {
        AllowedUrls.Add(pattern);
    }

    void SetExternalUser(bool isExternal) override {
        try {
            SupportArcLinks = !isExternal;
            InitUrlPatterns(isExternal);
        } catch (const yexception& e) {
            ythrow yexception() << "FileStorage: " << e.what();
        }
    }

    void InitUrlPatterns(bool isExternalUser) {
        for (const auto& p : isExternalUser ? Config.GetExternalAllowedUrlPatterns() : Config.GetAllowedUrlPatterns()) {
            AllowedUrls.Add(p);
        }
    }

    TFileLinkPtr PutFile(const TString& file, const TString& outFileName = {}) override {
        YQL_LOG(INFO) << "PutFile to cache: " << file;
        const auto md5 = MD5::File(file);
        const TString storageFileName = md5 + ".file";
        auto lock = MultiResourceLock.Acquire(storageFileName);
        return Storage.Put(storageFileName, outFileName, md5, [&file, &md5](const TFsPath& dstFile) {
            try {
                NFs::HardLinkOrCopy(file, dstFile);
                SetCacheFilePermissionsNoThrow(dstFile);
                i64 length = GetFileLength(dstFile.c_str());
                if (length == -1) {
                    ythrow TSystemError() << "cannot get file length: " << dstFile;
                }
                i64 srcLength = GetFileLength(file.c_str());
                if (srcLength == -1) {
                    ythrow TSystemError() << "cannot get file length: " << file;
                }

                YQL_ENSURE(srcLength == length);
                return std::make_pair(static_cast<ui64>(length), md5);
            } catch (...) {
                YQL_LOG(ERROR) << CurrentExceptionMessage();
                NFs::Remove(dstFile);
                throw;
            }
        });
    }

    TFileLinkPtr PutFileStripped(const TString& file, const TString& originalMd5 = {}) override {
        YQL_LOG(INFO) << "PutFileStripped to cache: " << file;
        if (originalMd5.empty()) {
            YQL_LOG(WARN) << "Empty md5 for: " << file;
        }
        const auto md5 = originalMd5.empty() ? MD5::File(file) : originalMd5;
        const auto strippedMetaFile = md5 + ".stripped_meta";
        auto lock = MultiResourceLock.Acquire(strippedMetaFile);

        TUrlMeta strippedMeta;
        strippedMeta.TryReadFrom(GetRoot() / strippedMetaFile);
        if (strippedMeta.ContentFile) {
            if (auto result = Storage.HardlinkFromStorage(strippedMeta.ContentFile, strippedMeta.Md5, "")) {
                return result;
            }
        }

        strippedMeta = TUrlMeta();
        const TString storageFileName = md5 + ".file.stripped";
        TFileLinkPtr result = Storage.Put(storageFileName, "", "", [&file](const TFsPath& dstPath) {
            try {
                ui64 size;
                TString md5;
                TShellCommand cmd("strip", {file, "-o", dstPath.GetPath()});
                cmd.Run().Wait();
                if (*cmd.GetExitCode() != 0) {
                    ythrow yexception() << cmd.GetError();
                }
                md5 = MD5::File(dstPath.GetPath());
                size = TFile(dstPath.GetPath(), OpenExisting | RdOnly).GetLength();
                YQL_LOG(DEBUG) << "Strip " << file << " to " << dstPath.GetPath();
                return std::make_pair(size, md5);
            } catch (...) {
                YQL_LOG(ERROR) << CurrentExceptionMessage();
                NFs::Remove(dstPath);
                throw;
            }
        });

        strippedMeta.ContentFile = result->GetStorageFileName();
        strippedMeta.Md5 = result->GetMd5();
        auto metaTmpFile = Storage.GetTemp() / Storage.GetTempName();
        strippedMeta.SaveTo(metaTmpFile);
        SetCacheFilePermissions(metaTmpFile);
        Storage.MoveToStorage(metaTmpFile, strippedMetaFile);
        return result;
    }

    TFileLinkPtr PutInline(const TString& data) override {
        const auto md5 = MD5::Calc(data);
        const TString storageFileName = md5 + ".file";
        YQL_LOG(INFO) << "PutInline to cache. md5=" << md5;
        auto lock = MultiResourceLock.Acquire(storageFileName);
        return Storage.Put(storageFileName, TString(), md5, [&data, &md5](const TFsPath& dstFile) {
            try {
                TStringInput in(data);
                TFile outFile(dstFile, CreateAlways | ARW | AX);
                TUnbufferedFileOutput out(outFile);
                auto result = std::make_pair(TransferData(&in, &out), md5);
                outFile.Close();
                SetCacheFilePermissions(dstFile);

                i64 length = GetFileLength(dstFile.c_str());
                if (length == -1) {
                    ythrow TSystemError() << "cannot get file length: " << dstFile;
                }

                YQL_ENSURE(data.size() == static_cast<ui64>(length));
                return result;
            } catch (...) {
                YQL_LOG(ERROR) << CurrentExceptionMessage();
                NFs::Remove(dstFile);
                throw;
            }
        });
    }

    TFileLinkPtr PutUrl(const TString& urlStr, const TString& oauthToken) override {
        try {
            TString convertedUrl;
            if (!Mapper.MapUrl(urlStr, convertedUrl)) {
                convertedUrl = urlStr;
            }

            YQL_LOG(INFO) << "PutUrl to cache: " << convertedUrl;
            THttpURL url = ParseURL(convertedUrl);
            auto rawScheme = url.GetField(NUri::TField::FieldScheme);
            if (NUri::EqualNoCase(rawScheme, "arc")) {
                return PutUrl(convertedUrl, url, oauthToken, true);
            } else {
                switch (url.GetSchemeInfo().Kind) {
                case NUri::TScheme::SchemeHTTP:
                case NUri::TScheme::SchemeHTTPS:
                    return PutUrl(convertedUrl, url, oauthToken, false);
                default:
                    ythrow yexception() << "Unsupported url scheme: " << convertedUrl;
                }
            }
        } catch (const std::exception& e) {
            YQL_LOG(ERROR) << "Failed to download file by URL \"" << urlStr << "\", details: " << e.what();
            YQL_LOG_CTX_THROW yexception() << "FileStorage: Failed to download file by URL \"" << urlStr << "\", details: " << e.what();
        }
    }

    NThreading::TFuture<TFileLinkPtr> PutFileAsync(const TString& file, const TString& outFileName = {}) override  {
        StartQueueOnce();
        return NThreading::Async([=]() {
            return this->PutFile(file, outFileName);
        }, *MtpQueue);
    }

    NThreading::TFuture<TFileLinkPtr> PutInlineAsync(const TString& data) override {
        StartQueueOnce();
        return NThreading::Async([=]() {
            return this->PutInline(data);
        }, *MtpQueue);
    }

    NThreading::TFuture<TFileLinkPtr> PutUrlAsync(const TString& url, const TString& oauthToken) override  {
        StartQueueOnce();
        return NThreading::Async([=]() {
            return this->PutUrl(url, oauthToken);
        }, *MtpQueue);
    }

    TFsPath GetRoot() const override {
        return Storage.GetRoot();
    }

    TFsPath GetTemp() const override {
        return Storage.GetTemp();
    }

private:
    void StartQueueOnce() {
        // we shall call Start only once
        if (AtomicTryLock(&QueueStarted)) {
            MtpQueue->Start(Config.GetThreads());
        }
    }

    TFileLinkPtr PutUrl(const TString& convertedUrl, const THttpURL& url, const TString& oauthToken, bool isArc) {
        if (isArc && !SupportArcLinks) {
            YQL_LOG(WARN) << "FileStorage: Arcadia support is disabled, reject downloading " << convertedUrl;
            throw yexception() << "It is not allowed to download Arcadia url " << convertedUrl;
        }

        if (!isArc && !AllowedUrls.IsEmpty() && !AllowedUrls.Match(convertedUrl)) {
            YQL_LOG(WARN) << "FileStorage: url " << convertedUrl << " is not in whitelist, reject downloading";
            throw yexception() << "It is not allowed to download url " << convertedUrl;
        }

        return WithRetry<TDownloadError>(Config.GetRetryCount(), [&, this]() {
            return this->DoPutUrl(convertedUrl, url, oauthToken, isArc);
        }, [&](const auto& e, int attempt, int attemptCount) {
            YQL_LOG(WARN) << "Error while downloading url " << convertedUrl << ", attempt " << attempt << "/" << attemptCount << ", details: " << e.what();
            Sleep(TDuration::MilliSeconds(Config.GetRetryDelayMs()));
        });
    }

    TFileLinkPtr DoPutUrl(const TString& convertedUrl, const THttpURL& url, const TString& oauthToken, bool isArc) {
        const auto urlMetaFile = BuildUrlMetaFileName(url);
        auto lock = MultiResourceLock.Acquire(urlMetaFile); // let's use meta file as lock name

        TUrlMeta urlMeta;
        urlMeta.TryReadFrom(GetRoot() / urlMetaFile);

        TFileLinkPtr oldContentLink = nullptr;
        if (urlMeta.ContentFile) {
            oldContentLink = Storage.HardlinkFromStorage(urlMeta.ContentFile, urlMeta.Md5, "");
        }

        if (!oldContentLink) {
            urlMeta = TUrlMeta();
        }

        YQL_LOG(INFO) << "UrlMeta: " << urlMetaFile << ", ETag=" << urlMeta.ETag
            << ", ContentFile=" << urlMeta.ContentFile << ", Md5=" << urlMeta.Md5
            << ", LastModified=" << urlMeta.LastModified;

        TFileLinkPtr result;
        TString etag;
        TString lastModified;
        if (isArc) {
            const auto urlContentFile = BuildUrlContentFileName(url, etag, lastModified);

            result = Storage.Put(urlContentFile, TString(), TString(), [&convertedUrl, &url, this](const TFsPath& dstFile) {
                try {
                    return this->FileExporter->ExportToFile(Config, convertedUrl, url, dstFile);
                } catch (...) {
                    NFs::Remove(dstFile);
                    throw;
                }
            });

        } else {
            TFetchResultPtr fr1 = FetchWithETagAndLastModified(convertedUrl, oauthToken, urlMeta.ETag, urlMeta.LastModified);
            switch (fr1->GetRetCode()) {
            case HTTP_NOT_MODIFIED:
                Y_ENSURE(oldContentLink); // should not fire
                return oldContentLink;
            case HTTP_OK:
                break;
            default:
                ythrow yexception() << "Url " << convertedUrl << " cannot be accessed, code: " << fr1->GetRetCode();
            }

            std::tie(etag, lastModified) = ExtractETagAndLastModified(*fr1);
            if (urlMeta.ETag && urlMeta.ETag == etag && oldContentLink) {
                // for non-empty etags server should reply with code 304 Not modified
                // but some servers like github.com do not support IfNoneMatch (but ETag is not empty)
                // no etag is supported (previously and now) or server responded with status code 200 and we have something in cache
                // access already checked by FetchWithETagAndLastModified
                return oldContentLink;
            }

            if (urlMeta.ETag && etag) {
                YQL_LOG(INFO) << "ETag for url " << convertedUrl << " has been changed from " << urlMeta.ETag << " to " << etag << ". We have to download new version";
            }
            else if (urlMeta.LastModified && lastModified) {
                YQL_LOG(INFO) << "LastModified for url " << convertedUrl << " has been changed from " << urlMeta.LastModified << " to " << lastModified << ". We have to download new version";
            }

            // todo: remove oldContentLink ?
            const auto urlContentFile = BuildUrlContentFileName(url, etag, lastModified);

            result = Storage.Put(urlContentFile, TString(), TString(), [&fr1, &convertedUrl](const TFsPath& dstFile) {
                try {
                    return CopyToFile(convertedUrl, *fr1, dstFile);
                } catch (...) {
                    NFs::Remove(dstFile);
                    throw;
                }
            });
        }

        // save meta using rename for atomicity
        urlMeta.ETag = etag;
        urlMeta.LastModified = lastModified;
        urlMeta.ContentFile = result->GetStorageFileName();
        urlMeta.Md5 = result->GetMd5();
        auto metaTmpFile = Storage.GetTemp() / Storage.GetTempName();
        urlMeta.SaveTo(metaTmpFile);
        SetCacheFilePermissions(metaTmpFile);

        Storage.MoveToStorage(metaTmpFile, urlMetaFile);

        return result;

    }

    TFetchResultPtr FetchWithETagAndLastModified(const TString& url, const TString& oauthToken, const TString& oldEtag, const TString& oldLastModified) {
        // more details about ETag and ModifiedSince: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.26
        THttpHeaders headers;
        if (!oauthToken.empty()) {
            headers.AddHeader(THttpInputHeader("Authorization", "OAuth " + oauthToken));
        }

        // ETag has priority over modification time
        if (!oldEtag.empty()) {
            headers.AddHeader(THttpInputHeader("If-None-Match", oldEtag));
        } else if (!oldLastModified.empty()) {
            headers.AddHeader(THttpInputHeader("If-Modified-Since", oldLastModified));
        }
        const auto parsedUrl = ParseURL(url);
        if (parsedUrl.GetHost() == "yt.yandex-team.ru" || parsedUrl.GetHost().EndsWith(".yt.yandex-team.ru")) {
            auto guid = CreateGuidAsString();
            headers.AddHeader(THttpInputHeader("X-YT-Correlation-Id", guid));
            YQL_LOG(INFO) << "Use Correlation-Id=" << guid << " for " << url;
        }

        const auto timeout = TDuration::MilliSeconds(Config.GetSocketTimeoutMs());
        try {
            return Fetch(parsedUrl, headers, timeout);
        } catch (const std::exception& e) {
            // remap exception type to leverage retry logic
            throw TDownloadError() << e.what();
        }
    }

    static TString BuildUrlMetaFileName(const THttpURL& url) {
        return MD5::Calc(url.PrintS(THttpURL::FlagNoFrag | THttpURL::FlagHostAscii)) + ".url_meta";
    }

    static TString BuildUrlContentFileName(const THttpURL& url, const TString& etag, const TString& lastModified) {
        TString needle = etag ? etag : lastModified;
        return MD5::Calc(needle + url.PrintS(THttpURL::FlagNoFrag | THttpURL::FlagHostAscii)) + ".url";
    }

    static std::pair<ui64, TString> CopyToFile(const TString& url, IFetchResult& src, const TString& dstFile) {
        TFile outFile(dstFile, CreateAlways | ARW | AX);
        TUnbufferedFileOutput out(outFile);
        TMd5OutputStream md5Out(out);

        THttpInput& httpStream = src.GetStream();
        TDownloadStream input(httpStream);
        const ui64 size = TransferData(&input, &md5Out);
        auto result = std::make_pair(size, md5Out.Finalize());
        out.Finish();
        outFile.Close();
        SetCacheFilePermissions(dstFile);

        ui64 contentLength = 0;
        // additional check for not compressed data
        if (!httpStream.ContentEncoded() && httpStream.GetContentLength(contentLength) && contentLength != size) {
            // let's retry this error
            ythrow TDownloadError() << "Size mismatch while downloading url " << url << ", downloaded size: " << size << ", ContentLength: " << contentLength;
        }

        if (auto trailers = httpStream.Trailers()) {
            if (auto header = trailers->FindHeader("X-YT-Error")) {
                ythrow TDownloadError() << "X-YT-Error=" << header->Value();
            }
        }

        i64 dstFileLen = GetFileLength(dstFile.c_str());
        if (dstFileLen == -1) {
            ythrow TSystemError() << "cannot get file length: " << dstFile;
        }

        YQL_ENSURE(static_cast<ui64>(dstFileLen) == size);
        return result;
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
    TStorage Storage;
    const TFileStorageConfig Config;
    TUrlMapper Mapper;
    TPatternGroup AllowedUrls;
    TAtomic QueueStarted;
    bool SupportArcLinks;
    THolder<IThreadPool> MtpQueue;
    TMultiResourceLock MultiResourceLock;
    std::unique_ptr<IFileExporter> FileExporter;
};

TFileStoragePtr CreateFileStorage(const TFileStorageConfig& params) {
    Y_ENSURE(0 != params.GetMaxFiles(), "FileStorage: MaxFiles must be greater than 0");
    Y_ENSURE(0 != params.GetMaxSizeMb(), "FileStorage: MaxSizeMb must be greater than 0");
    return new TFileStorageImpl(params);
}

} // NYql
