#include "file_storage.h"
#include "file_storage_decorator.h"
#include "storage.h"
#include "url_meta.h"

#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>
#include <ydb/library/yql/core/file_storage/download/download_stream.h>
#include <ydb/library/yql/core/file_storage/http_download/http_download.h>
#include <ydb/library/yql/core/file_storage/defs/provider.h>

#include <ydb/library/yql/utils/fetch/fetch.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/log/context.h>
#include <ydb/library/yql/utils/multi_resource_lock.h>
#include <ydb/library/yql/utils/md5_stream.h>
#include <ydb/library/yql/utils/retry.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/cache/cache.h>
#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/guid.h>
#include <util/generic/yexception.h>

#include <util/string/builder.h>
#include <util/stream/str.h>
#include <util/stream/file.h>
#include <util/stream/null.h>
#include <util/system/env.h>
#include <util/system/fs.h>
#include <util/system/fstat.h>
#include <util/system/guard.h>
#include <util/system/shellcommand.h>
#include <util/system/sysstat.h>
#include <util/system/utime.h>
#include <util/folder/path.h>


namespace NYql {

class TFileStorageImpl: public IFileStorage {
public:
    explicit TFileStorageImpl(const TFileStorageConfig& params, const std::vector<NFS::IDownloaderPtr>& downloaders)
        : Storage(params.GetMaxFiles(), ui64(params.GetMaxSizeMb()) << 20ull, params.GetPath())
        , Config(params)
        , UseFakeChecksums(GetEnv("YQL_LOCAL") == "1")
    {
        Downloaders.push_back(MakeHttpDownloader(params));
        Downloaders.insert(Downloaders.begin(), downloaders.begin(), downloaders.end());
    }

    ~TFileStorageImpl() {
    }

    TFileLinkPtr PutFile(const TString& file, const TString& outFileName = {}) final {
        YQL_LOG(INFO) << "PutFile to cache: " << file;
        const auto md5 = FileChecksum(file);
        const TString storageFileName = md5 + ".file";
        auto lock = MultiResourceLock.Acquire(storageFileName);
        return Storage.Put(storageFileName, outFileName, md5, [&file, &md5](const TFsPath& dstFile) {
            NFs::HardLinkOrCopy(file, dstFile);
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
        });
    }

    TFileLinkPtr PutFileStripped(const TString& file, const TString& originalMd5 = {}) final {
        YQL_LOG(INFO) << "PutFileStripped to cache: " << file << ". Provided MD5: " << originalMd5;
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
            ui64 size;
            TString md5;
            TShellCommand cmd("strip", {file, "-o", dstPath.GetPath()});
            cmd.Run().Wait();
            if (*cmd.GetExitCode() != 0) {
                ythrow yexception() << "'strip' exited with code " << *cmd.GetExitCode() << ". Stderr: " << cmd.GetError();
            }
            md5 = MD5::File(dstPath.GetPath());
            size = TFile(dstPath.GetPath(), OpenExisting | RdOnly).GetLength();
            YQL_LOG(DEBUG) << "Strip " << file << " to " << dstPath.GetPath();
            return std::make_pair(size, md5);
        });

        strippedMeta.ContentFile = result->GetStorageFileName();
        strippedMeta.Md5 = result->GetMd5();
        auto metaTmpFile = Storage.GetTemp() / Storage.GetTempName();
        strippedMeta.SaveTo(metaTmpFile);
        Storage.MoveToStorage(metaTmpFile, strippedMetaFile);
        return result;
    }

    TFileLinkPtr PutInline(const TString& data) final {
        const auto md5 = MD5::Calc(data);
        const TString storageFileName = md5 + ".file";
        YQL_LOG(INFO) << "PutInline to cache. md5=" << md5;
        auto lock = MultiResourceLock.Acquire(storageFileName);
        return Storage.Put(storageFileName, TString(), md5, [&data, &md5](const TFsPath& dstFile) {
            TStringInput in(data);
            TFile outFile(dstFile, CreateAlways | ARW | AX);
            TUnbufferedFileOutput out(outFile);
            auto result = std::make_pair(TransferData(&in, &out), md5);
            outFile.Close();

            i64 length = GetFileLength(dstFile.c_str());
            if (length == -1) {
                ythrow TSystemError() << "cannot get file length: " << dstFile;
            }

            YQL_ENSURE(data.size() == static_cast<ui64>(length));
            return result;
        });
    }

    TFileLinkPtr PutUrl(const TString& urlStr, const TString& token) final {
        try {
            YQL_LOG(INFO) << "PutUrl to cache: " << urlStr;
            THttpURL url = ParseURL(urlStr);
            for (const auto& d: Downloaders) {
                if (d->Accept(url)) {
                    return PutUrl(url, token, d);
                }
            }

            ythrow yexception() << "Unsupported url: " << urlStr;
        } catch (const std::exception& e) {
            const TString msg = TStringBuilder() << "FileStorage: Failed to download file by URL \"" << urlStr << "\", details: " << e.what();
            YQL_LOG(ERROR) << msg;
            YQL_LOG_CTX_THROW yexception() << msg;
        }
    }

    NThreading::TFuture<TFileLinkPtr> PutFileAsync(const TString& /*file*/, const TString& /*outFileName*/) final {
        ythrow yexception() << "Async method is not implemeted";
    }

    NThreading::TFuture<TFileLinkPtr> PutInlineAsync(const TString& /*data*/) final {
        ythrow yexception() << "Async method is not implemeted";
    }

    NThreading::TFuture<TFileLinkPtr> PutUrlAsync(const TString& /*url*/, const TString& /*token*/) final {
        ythrow yexception() << "Async method is not implemeted";
    }

    TFsPath GetRoot() const final {
        return Storage.GetRoot();
    }

    TFsPath GetTemp() const final {
        return Storage.GetTemp();
    }

    const TFileStorageConfig& GetConfig() const final {
        return Config;
    }

private:
    TFileLinkPtr PutUrl(const THttpURL& url, const TString& token, const NFS::IDownloaderPtr& downloader) {
        return WithRetry<TDownloadError>(Config.GetRetryCount(), [&, this]() {
            return this->DoPutUrl(url, token, downloader);
        }, [&](const auto& e, int attempt, int attemptCount) {
            YQL_LOG(WARN) << "Error while downloading url " << url.PrintS() << ", attempt " << attempt << "/" << attemptCount << ", details: " << e.what();
            Sleep(TDuration::MilliSeconds(Config.GetRetryDelayMs()));
        });
    }

    TFileLinkPtr DoPutUrl(const THttpURL& url, const TString& token, const NFS::IDownloaderPtr& downloader) {
        const auto urlMetaFile = BuildUrlMetaFileName(url, token);
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

        NFS::TDataProvider puller;
        TString etag;
        TString lastModified;
        std::tie(puller, etag, lastModified) = downloader->Download(url, token, urlMeta.ETag, urlMeta.LastModified);
        if (!puller) {
            Y_ENSURE(oldContentLink); // should not fire
            return oldContentLink;
        }
        if (urlMeta.ETag && urlMeta.ETag == etag && oldContentLink) {
            // for non-empty etags server should reply with code 304 Not modified
            // but some servers like github.com do not support IfNoneMatch (but ETag is not empty)
            // no etag is supported (previously and now) or server responded with status code 200 and we have something in cache
            // access already checked by FetchWithETagAndLastModified
            return oldContentLink;
        }
        if (urlMeta.ETag && etag) {
            YQL_LOG(INFO) << "ETag for url " << url.PrintS() << " has been changed from " << urlMeta.ETag << " to " << etag << ". We have to download new version";
        }
        else if (urlMeta.LastModified && lastModified) {
            YQL_LOG(INFO) << "LastModified for url " << url.PrintS() << " has been changed from " << urlMeta.LastModified << " to " << lastModified << ". We have to download new version";
        }

        // todo: remove oldContentLink ?
        const auto urlChecksum = BuildUrlChecksum(url, token, etag, lastModified);
        const auto urlContentFile = BuildUrlContentFileName(urlChecksum);
        TFileLinkPtr result = Storage.Put(urlContentFile, TString(), UseFakeChecksums ? urlChecksum : TString(), puller);

        // save meta using rename for atomicity
        urlMeta.ETag = etag;
        urlMeta.LastModified = lastModified;
        urlMeta.ContentFile = result->GetStorageFileName();
        urlMeta.Md5 = result->GetMd5();
        auto metaTmpFile = Storage.GetTemp() / Storage.GetTempName();
        urlMeta.SaveTo(metaTmpFile);

        Storage.MoveToStorage(metaTmpFile, urlMetaFile);

        return result;
    }

    TString FileChecksum(const TString& file) const {
        if (UseFakeChecksums) {
            // Avoid heavy binaries recalculation in local mode (YQL-15353):
            // calculate MD5 sum of file name instead of file contents
            return MD5::Calc(file);
        } else {
            return MD5::File(file);
        }
    }

    static TString BuildUrlMetaFileName(const THttpURL& url, const TString& token) {
        return MD5::Calc(TStringBuilder() << token << url.PrintS(THttpURL::FlagNoFrag | THttpURL::FlagHostAscii)) + ".url_meta";
    }

    static TString BuildUrlChecksum(const THttpURL& url, const TString& token, const TString& etag, const TString& lastModified) {
        TString needle = etag ? etag : lastModified;
        return MD5::Calc(TStringBuilder() << token << needle << url.PrintS(THttpURL::FlagNoFrag | THttpURL::FlagHostAscii));
    }

    static TString BuildUrlContentFileName(const TString& urlChecksum) {
        return urlChecksum + ".url";
    }

private:
    TStorage Storage;
    const TFileStorageConfig Config;
    std::vector<NFS::IDownloaderPtr> Downloaders;
    TMultiResourceLock MultiResourceLock;
    const bool UseFakeChecksums;   // YQL-15353
};

class TFileStorageWithAsync: public TFileStorageDecorator {
public:
    TFileStorageWithAsync(TFileStoragePtr fs)
        : TFileStorageDecorator(std::move(fs))
        , QueueStarted(0)
    {
        auto numThreads = Inner_->GetConfig().GetThreads();
        if (1 == numThreads) {
            MtpQueue.Reset(new TFakeThreadPool());
        } else {
            MtpQueue.Reset(new TSimpleThreadPool(TThreadPoolParams{"FileStorage"}));
        }

        // do not call MtpQueue->Start here as we have to do it _after_ fork
    }

    ~TFileStorageWithAsync() {
        MtpQueue->Stop();
    }

    NThreading::TFuture<TFileLinkPtr> PutFileAsync(const TString& file, const TString& outFileName) final {
        return DoAsync([file, outFileName](const TFileStoragePtr& fs) {
            return fs->PutFile(file, outFileName);
        });
    }
    NThreading::TFuture<TFileLinkPtr> PutInlineAsync(const TString& data) final {
        return DoAsync([data](const TFileStoragePtr& fs) {
            return fs->PutInline(data);
        });
    }
    NThreading::TFuture<TFileLinkPtr> PutUrlAsync(const TString& url, const TString& token) final {
        return DoAsync([url, token](const TFileStoragePtr& fs) {
            return fs->PutUrl(url, token);
        });
    }

private:
    NThreading::TFuture<TFileLinkPtr> DoAsync(std::function<TFileLinkPtr(const TFileStoragePtr&)> action) {
        if (AtomicTryLock(&QueueStarted)) {
            MtpQueue->Start(Inner_->GetConfig().GetThreads());
        }
        auto logCtx = NLog::CurrentLogContextPath();
        return NThreading::Async([logCtx, fs = Inner_, action]() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
            return action(fs);
        }, *MtpQueue);
    }

private:
    TAtomic QueueStarted;
    THolder<IThreadPool> MtpQueue;
};


TFileStoragePtr CreateFileStorage(const TFileStorageConfig& params, const std::vector<NFS::IDownloaderPtr>& downloaders) {
    Y_ENSURE(0 != params.GetMaxFiles(), "FileStorage: MaxFiles must be greater than 0");
    Y_ENSURE(0 != params.GetMaxSizeMb(), "FileStorage: MaxSizeMb must be greater than 0");
    return new TFileStorageImpl(params, downloaders);
}

TFileStoragePtr WithAsync(TFileStoragePtr fs) {
    return new TFileStorageWithAsync(std::move(fs));
}

void LoadFsConfigFromFile(TStringBuf path, TFileStorageConfig& params) {
    auto fs = TFsPath(path);
    try {
        ParseFromTextFormat(fs, params, EParseFromTextFormatOption::AllowUnknownField);
    } catch (const yexception& e) {
        ythrow yexception() << "Bad format of file storage settings: " << e;
    }

    TString prefix = fs.GetName();
    TString ext = fs.GetExtension();
    if (ext) {
        ext.prepend('.');
        prefix = prefix.substr(0, prefix.length() - ext.length());
    }
    prefix.append('_');
    TVector<TFsPath> children;
    fs.Parent().List(children);
    for (auto c: children) {
        if (c.IsFile()) {
            const auto name = c.GetName();
            TStringBuf key(name);
            if (key.SkipPrefix(prefix) && (!ext || key.ChopSuffix(ext))) {
                auto configData = TIFStream(c).ReadAll();
                (*params.MutableDownloaderConfig())[key] = configData;
            }
        }
    }
}

void LoadFsConfigFromResource(TStringBuf path, TFileStorageConfig& params) {
    TString configData = NResource::Find(path);

    try {
        TStringInput in(configData);
        ParseFromTextFormat(in, params, EParseFromTextFormatOption::AllowUnknownField);
    } catch (const yexception& e) {
        ythrow yexception() << "Bad format of file storage settings: " << e;
    }

    auto fs = TFsPath(path);
    TString prefix = fs;
    TString ext = fs.GetExtension();
    if (ext) {
        ext.prepend('.');
        prefix = prefix.substr(0, prefix.length() - ext.length());
    }
    prefix.append('_');

    for (auto res: NResource::ListAllKeys()) {
        TStringBuf key{res};
        if (key.SkipPrefix(prefix) && (!ext || key.ChopSuffix(ext))) {
            configData = NResource::Find(res);
            (*params.MutableDownloaderConfig())[key] = configData;
        }
    }
}

} // NYql
