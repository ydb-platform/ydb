#include "fs_storage.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/client/AWSError.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/S3Errors.h>

#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/system/fs.h>
#include <util/generic/guid.h>

#include <optional>
#include <type_traits>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FS_WRAPPER

#define YDB_LOG_IF_ACTIVE(LOG_MACRO, ...) \
    do {                                  \
        if (TlsActivationContext) {       \
            LOG_MACRO(__VA_ARGS__);       \
        }                                 \
    } while (false)

namespace NKikimr::NWrappers::NExternalStorage {

namespace {

class TFsOperationActor : public TActorBootstrapped<TFsOperationActor> {
private:
    const TString BasePath;

    struct TMultipartUploadSession {
        const TString Key;
        TFile File;
        ui64 TotalSize = 0;

        explicit TMultipartUploadSession(const TString& key)
            : Key(key)
            , File(key, OpenAlways | WrOnly | ForAppend)
        {
            FsyncParentDir(key);
            File.Flock(LOCK_EX | LOCK_NB);
            File.Resize(0);
        }
    };

    THashMap<TString, TMultipartUploadSession> ActiveUploads;
    static constexpr std::pair<ui64, ui64> EmptyRange = std::make_pair(0, 0);
    static constexpr auto InvalidRange = EmptyRange;

    template<typename TEvResponse>
    struct RequiresKey : std::true_type {};

    template<>
    struct RequiresKey<TEvListObjectsResponse> : std::false_type {};

    template<>
    struct RequiresKey<TEvDeleteObjectsResponse> : std::false_type {};

    template<typename TEvResponse>
    static constexpr bool HasKeyConstructor() {
        return RequiresKey<TEvResponse>::value;
    }

    static void FsyncParentDir(const TFsPath& fsPath) {
        TFile dirFd(fsPath.Parent().GetPath(), RdOnly | Seq);
        dirFd.Flush();
    }

    static void FsyncParentDir(const TString& filePath) {
        FsyncParentDir(TFsPath(filePath));
    }

    template<typename TEvResponse>
    void ReplySuccess(const NActors::TActorId& sender, const std::optional<TString>& key) {
        typename TEvResponse::TAwsResult awsResult;
        Aws::Utils::Outcome<typename TEvResponse::TAwsResult, Aws::S3::S3Error> outcome(std::move(awsResult));

        std::unique_ptr<TEvResponse> response;
        if constexpr (HasKeyConstructor<TEvResponse>()) {
            Y_ENSURE(key, "Key is required for this response type");
            response = std::make_unique<TEvResponse>(*key, std::move(outcome));
        } else {
            response = std::make_unique<TEvResponse>(std::move(outcome));
        }
        this->Send(sender, response.release());
    }

    struct TReplyErrorOpts {
        TString ErrorMessage;
        Aws::S3::S3Errors ErrorType = Aws::S3::S3Errors::INTERNAL_FAILURE;
        bool Retryable = false;
        TString ExceptionName = "FsStorageError";
        std::optional<Aws::Http::HttpResponseCode> ResponseCode = std::nullopt;
    };

    template<typename TEvResponse>
    auto CreateOutcome(const TReplyErrorOpts& opts) {
        Aws::Client::AWSError<Aws::S3::S3Errors> awsError(
            opts.ErrorType, opts.ExceptionName, opts.ErrorMessage, opts.Retryable);
        if (opts.ResponseCode) {
            awsError.SetResponseCode(*opts.ResponseCode);
        }
        return Aws::Utils::Outcome<typename TEvResponse::TAwsResult, Aws::S3::S3Error>(
            Aws::S3::S3Error(std::move(awsError)));
    }

    template<typename TEvResponse, typename... Args>
    void ReplyError(const NActors::TActorId& sender, TReplyErrorOpts opts, Args&&... args) {
        auto outcome = CreateOutcome<TEvResponse>(opts);
        auto response = std::make_unique<TEvResponse>(std::forward<Args>(args)..., std::move(outcome));
        this->Send(sender, response.release());
    }

    static std::optional<TReplyErrorOpts> ClassifyFsError(int status) {
        switch (status) {
            case EACCES:
            case EPERM:
            case EROFS:
                return TReplyErrorOpts{
                    .ErrorType = Aws::S3::S3Errors::ACCESS_DENIED,
                    .ExceptionName = "FsPermissionDenied",
                    .ResponseCode = Aws::Http::HttpResponseCode::FORBIDDEN,
                };
            case EDQUOT:
                return TReplyErrorOpts{
                    .ErrorType = Aws::S3::S3Errors::ACCESS_DENIED,
                    .ExceptionName = "FsQuotaExceeded",
                    .ResponseCode = Aws::Http::HttpResponseCode::INSUFFICIENT_STORAGE,
                };
            case ENOENT:
            case ENOTDIR:
                return TReplyErrorOpts{
                    .ErrorType = Aws::S3::S3Errors::NO_SUCH_KEY,
                    .ExceptionName = "FsPathNotFound",
                    .ResponseCode = Aws::Http::HttpResponseCode::NOT_FOUND,
                };
            case EISDIR:
            case ENAMETOOLONG:
            case EINVAL:
            case ELOOP:
            case EXDEV:
                return TReplyErrorOpts{
                    .ErrorType = Aws::S3::S3Errors::INVALID_PARAMETER_VALUE,
                    .ExceptionName = "FsInvalidPath",
                    .ResponseCode = Aws::Http::HttpResponseCode::BAD_REQUEST,
                };
            case EFBIG:
                return TReplyErrorOpts{
                    .ErrorType = Aws::S3::S3Errors::INVALID_PARAMETER_VALUE,
                    .ExceptionName = "FsFileTooLarge",
                    .ResponseCode = Aws::Http::HttpResponseCode::BAD_REQUEST,
                };
            case ENOSPC:
                return TReplyErrorOpts{
                    .ErrorType = Aws::S3::S3Errors::INVALID_PARAMETER_VALUE,
                    .ExceptionName = "FsNoSpaceLeft",
                    .ResponseCode = Aws::Http::HttpResponseCode::INSUFFICIENT_STORAGE,
                };
            case EWOULDBLOCK:
#if EAGAIN != EWOULDBLOCK
            case EAGAIN:
#endif
                return TReplyErrorOpts{
                    .ErrorMessage = "File is locked by another process",
                    .ErrorType = Aws::S3::S3Errors::INTERNAL_FAILURE,
                    .Retryable = true,
                    .ExceptionName = "FsFileLocked",
                    .ResponseCode = Aws::Http::HttpResponseCode::SERVICE_UNAVAILABLE,
                };
            default:
                return std::nullopt;
        }
    }

    template<typename TEvResponse, typename... Args>
    void ReplyFsSystemError(const NActors::TActorId& sender, const TString& context, const TSystemError& ex, Args&&... args) {
        const auto msg = TStringBuilder() << context
            << ", error# " << ex.what()
            << ", errno# " << ex.Status();

        auto opts = ClassifyFsError(ex.Status()).value_or(TReplyErrorOpts{});

        if (opts.Retryable) {
            YDB_LOG_WARN(msg);
        } else {
            YDB_LOG_ERROR(msg);
        }

        if (opts.ErrorMessage.empty()) {
            opts.ErrorMessage = ex.what();
        }
        ReplyError<TEvResponse>(sender, std::move(opts), std::forward<Args>(args)...);
    }

public:
    explicit TFsOperationActor(const TString& basePath)
        : BasePath(basePath)
    {
    }

    ~TFsOperationActor() {
        CleanupActiveSessions();
    }

    void Bootstrap() {
        YDB_LOG_TRACE("TFsOperationActor Bootstrap called");
        Become(&TThis::StateWork);
    }

    void PassAway() override {
        CleanupActiveSessions();
        NActors::TActorBootstrapped<TFsOperationActor>::PassAway();
    }

private:
    void CleanupActiveSessions() {
        YDB_LOG_IF_ACTIVE(YDB_LOG_DEBUG, "TFsOperationActor: cleaning up active MPU",
            {"sessions", ActiveUploads.size()});
        for (auto& [uploadId, session] : ActiveUploads) {
            try {
                const TString filePath = session.Key;
                NFs::Remove(filePath);
                session.File.Close();

                YDB_LOG_IF_ACTIVE(YDB_LOG_TRACE, "TFsOperationActor: closed and deleted incomplete file",
                    {"uploadId", uploadId},
                    {"file", filePath});
            } catch (const std::exception& ex) {
                YDB_LOG_IF_ACTIVE(YDB_LOG_WARN, "Failed to cleanup MPU session",
                    {"uploadId", uploadId},
                    {"error", ex.what()});
            }
        }
        ActiveUploads.clear();
    }

public:

    STATEFN(StateWork) {
        YDB_LOG_TRACE("TFsOperationActor StateWork received event type",
            {"type", ev->GetTypeRewrite()});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPutObjectRequest, Handle);
            hFunc(TEvGetObjectRequest, Handle);
            hFunc(TEvHeadObjectRequest, Handle);
            hFunc(TEvDeleteObjectRequest, Handle);
            hFunc(TEvCheckObjectExistsRequest, Handle);
            hFunc(TEvListObjectsRequest, Handle);
            hFunc(TEvDeleteObjectsRequest, Handle);
            hFunc(TEvCreateMultipartUploadRequest, Handle);
            hFunc(TEvUploadPartRequest, Handle);
            hFunc(TEvCompleteMultipartUploadRequest, Handle);
            hFunc(TEvAbortMultipartUploadRequest, Handle);
            hFunc(TEvUploadPartCopyRequest, Handle);
            sFunc(NActors::TEvents::TEvPoison, PassAway);
            default:
                YDB_LOG_WARN("TFsOperationActor StateWork received unknown event type",
                    {"type", ev->GetTypeRewrite()});
        }
    }

    TString GetIncompletePath(const char* key) {
        return TStringBuilder() << key << ".incomplete";
    }

    void Handle(TEvPutObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const auto& body = ev->Get()->Body;
        const TString key = TString(request.GetKey().data(), request.GetKey().size());

        YDB_LOG_DEBUG("PutObject",
            {"key", key},
            {"size", body.size()});

        try {
            TFsPath fsPath(key);
            fsPath.Parent().MkDirs();

            TMultipartUploadSession session(key);
            session.File.Write(body.data(), body.size());
            session.File.Flush();
            session.File.Close();

            ReplySuccess<TEvPutObjectResponse>(ev->Sender, key);
        } catch (const TSystemError& ex) {
            ReplyFsSystemError<TEvPutObjectResponse>(ev->Sender,
                TStringBuilder() << "PutObject failed with system error"
                    << ": key# " << key, ex, key);
        } catch (const std::exception& ex) {
            YDB_LOG_ERROR("PutObject failed",
                {"key", key},
                {"error", ex.what()});
            ReplyError<TEvPutObjectResponse>(ev->Sender, {.ErrorMessage = ex.what()}, key);
        }
    }

    void Handle(TEvGetObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());

        YDB_LOG_DEBUG("GetObject",
            {"key", key});

        try {
            TFile file(key, RdOnly);
            const i64 fileSize = file.GetLength();

            if (fileSize == 0) {
                Aws::S3::Model::GetObjectResult awsResult;
                awsResult.SetContentLength(0);
                awsResult.SetETag("\"fs-file\"");
                Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult, Aws::S3::S3Error> outcome(std::move(awsResult));

                auto response = std::make_unique<TEvGetObjectResponse>(key, EmptyRange, std::move(outcome), TString());
                Send(ev->Sender, response.release());
                return;
            }

            TString rangeStr(request.GetRange().data(), request.GetRange().size());
            std::pair<ui64, ui64> range;

            if (!rangeStr.empty()) {
                if (!TEvGetObjectResponse::TryParseRange(rangeStr, range)) {
                    ReplyError<TEvGetObjectResponse>(ev->Sender,
                        {.ErrorMessage = TStringBuilder() << "Invalid range format: " << rangeStr},
                        key, InvalidRange);
                    return;
                }
            } else {
                range = std::make_pair(0, fileSize - 1);
            }

            ui64 start = range.first;
            ui64 end = range.second;
            if (start > end) {
                ReplyError<TEvGetObjectResponse>(ev->Sender,
                    {.ErrorMessage = TStringBuilder() << "Invalid range: start > end: " << rangeStr},
                    key, InvalidRange);
                return;
            }
            const ui64 length = end - start + 1;

            if ((i64)start >= fileSize) {
                ReplyError<TEvGetObjectResponse>(ev->Sender, {.ErrorMessage = "Range out of bounds"}, key, range);
                return;
            }

            TString data;
            data.resize(length);
            file.Seek(start, sSet);
            size_t bytesRead = file.Read(data.begin(), length);
            data.resize(bytesRead);

            YDB_LOG_INFO("GetObject read",
                {"bytes", bytesRead},
                {"key", key});

            Aws::S3::Model::GetObjectResult awsResult;
            awsResult.SetContentLength(bytesRead);
            awsResult.SetETag("\"fs-file\"");
            Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult, Aws::S3::S3Error> outcome(std::move(awsResult));

            auto response = std::make_unique<TEvGetObjectResponse>(key, range, std::move(outcome), std::move(data));
            Send(ev->Sender, response.release());
        } catch (const TSystemError& ex) {
            ReplyFsSystemError<TEvGetObjectResponse>(ev->Sender,
                TStringBuilder() << "GetObject failed with system error"
                    << ": key# " << key, ex, key, InvalidRange);
        } catch (const std::exception& ex) {
            YDB_LOG_ERROR("GetObject error",
                {"key", key},
                {"error", ex.what()});
            ReplyError<TEvGetObjectResponse>(ev->Sender,
                {.ErrorMessage = TString("File read error: ") + ex.what()},
                key, InvalidRange);
        }
    }

    void Handle(TEvHeadObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());

        YDB_LOG_DEBUG("HeadObject",
            {"key", key});

        try {
            TFile file(key, RdOnly | Seq);
            const i64 fileSize = file.GetLength();

            YDB_LOG_INFO("HeadObject",
                {"fileSize", fileSize},
                {"key", key});

            Aws::S3::Model::HeadObjectResult awsResult;
            awsResult.SetContentLength(fileSize);
            awsResult.SetETag("\"fs-file\"");

            Aws::Utils::Outcome<Aws::S3::Model::HeadObjectResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            auto response = std::make_unique<TEvHeadObjectResponse>(key, std::move(outcome));
            Send(ev->Sender, response.release());
        } catch (const TSystemError& ex) {
            ReplyFsSystemError<TEvHeadObjectResponse>(ev->Sender,
                TStringBuilder() << "HeadObject failed with system error"
                    << ": key# " << key, ex, key);
        } catch (const std::exception& ex) {
            YDB_LOG_ERROR("HeadObject error",
                {"key", key},
                {"error", ex.what()});
            ReplyError<TEvHeadObjectResponse>(ev->Sender,
                {.ErrorMessage = TStringBuilder() << "File head error: " << ex.what()}, key);
        }
    }

    void Handle(TEvDeleteObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        YDB_LOG_WARN("DeleteObject: not implemented");
        ReplyError<TEvDeleteObjectResponse>(ev->Sender, {.ErrorMessage = "Not implemented"}, key);
    }

    void Handle(TEvCheckObjectExistsRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        YDB_LOG_WARN("CheckObjectExists: not implemented");
        ReplyError<TEvCheckObjectExistsResponse>(ev->Sender, {.ErrorMessage = "Not implemented"}, key);
    }

    static constexpr int DefaultMaxListKeys = 1000;

    bool ListFilesRecursive(const TFsPath& dir, const TString& marker, int maxKeys,
        Aws::S3::Model::ListObjectsResult& result)
    {
        TVector<TString> children;
        dir.ListNames(children);

        THashSet<TString> directories;
        for (const auto& name : children) {
            TFsPath child = dir / name;
            if (child.IsDirectory()) {
                directories.insert(name);
            }
        }

        Sort(children.begin(), children.end(), [&directories](const TString& a, const TString& b) {
            TString keyA = directories.contains(a) ? (a + "/") : a;
            TString keyB = directories.contains(b) ? (b + "/") : b;
            return keyA < keyB;
        });

        for (const auto& name : children) {
            TFsPath child = dir / name;
            if (child.IsSymlink()) {
                continue;
            }
            if (child.IsFile()) {
                const TString& path = child.GetPath();
                if (!marker.empty() && path <= marker) {
                    continue;
                }
                if (static_cast<int>(result.GetContents().size()) >= maxKeys) {
                    return true;
                }
                Aws::S3::Model::Object obj;
                obj.SetKey(Aws::String(path.data(), path.size()));
                result.AddContents(std::move(obj));
            } else if (directories.contains(name)) {
                if (ListFilesRecursive(child, marker, maxKeys, result)) {
                    return true;
                }
            }
        }
        return false;
    }

    void Handle(TEvListObjectsRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString requestPrefix = TString(request.GetPrefix().data(), request.GetPrefix().size());
        const TString prefix = requestPrefix.empty() ? BasePath : requestPrefix;
        const TString marker = TString(request.GetMarker().data(), request.GetMarker().size());
        const int maxKeys = request.GetMaxKeys() > 0
            ? std::min(request.GetMaxKeys(), DefaultMaxListKeys)
            : DefaultMaxListKeys;

        YDB_LOG_DEBUG("ListObjects",
            {"prefix", prefix},
            {"marker", marker},
            {"maxKeys", maxKeys});

        try {
            TFsPath dirPath(prefix);
            TFsPath basePath(BasePath);

            Aws::S3::Model::ListObjectsResult awsResult;
            bool truncated = false;

            if (dirPath.Exists()) {
                TFsPath realDirPath = dirPath.RealPath();
                TFsPath realBasePath = basePath.RealPath();
                if (!realDirPath.IsNonStrictSubpathOf(realBasePath)) {
                    auto errorMsg = TStringBuilder() << "Prefix outside of base path"
                        << ": prefix# " << dirPath.GetPath()
                        << ", basePath# " << basePath.GetPath();
                    if (realDirPath.GetPath() != dirPath.GetPath()) {
                        errorMsg << ", resolvedPrefix# " << realDirPath.GetPath();
                    }
                    if (basePath.GetPath() != realBasePath.GetPath()) {
                        errorMsg << ", resolvedBasePath# " << realBasePath.GetPath();
                    }
                    ReplyError<TEvListObjectsResponse>(ev->Sender, {.ErrorMessage = errorMsg});
                    return;
                }

                if (dirPath.IsDirectory()) {
                    truncated = ListFilesRecursive(dirPath, marker, maxKeys, awsResult);
                }
            }

            awsResult.SetIsTruncated(truncated);

            Aws::Utils::Outcome<Aws::S3::Model::ListObjectsResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            auto response = std::make_unique<TEvListObjectsResponse>(std::move(outcome));
            Send(ev->Sender, response.release());
        } catch (const TSystemError& ex) {
            ReplyFsSystemError<TEvListObjectsResponse>(ev->Sender,
                TStringBuilder() << "ListObjects failed with system error"
                    << ": prefix# " << prefix, ex);
        } catch (const std::exception& ex) {
            YDB_LOG_ERROR("ListObjects failed",
                {"prefix", prefix},
                {"error", ex.what()});
            ReplyError<TEvListObjectsResponse>(ev->Sender,
                {.ErrorMessage = TString("ListObjects error: ") + ex.what()});
        }
    }

    void Handle(TEvDeleteObjectsRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        YDB_LOG_WARN("DeleteObjects: not implemented",
            {"objectsCount", request.GetDelete().GetObjects().size()});
        ReplyError<TEvDeleteObjectsResponse>(ev->Sender, {.ErrorMessage = "Not implemented"});
    }

    void Handle(TEvCreateMultipartUploadRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString originalKey = TString(request.GetKey().data(), request.GetKey().size());

        YDB_LOG_DEBUG("CreateMultipartUpload",
            {"key", originalKey});

        try {
            const TString key = GetIncompletePath(originalKey.c_str());
            const TString uploadId = TStringBuilder() << key << "_" << CreateGuidAsString();
            TFsPath fsPath(key);
            fsPath.Parent().MkDirs();

            ActiveUploads.emplace(uploadId, TMultipartUploadSession(key));

            YDB_LOG_INFO("CreateMultipartUpload: file opened with exclusive lock",
                {"key", key},
                {"uploadId", uploadId});

            Aws::S3::Model::CreateMultipartUploadResult awsResult;
            awsResult.SetKey(request.GetKey());
            awsResult.SetUploadId(uploadId.c_str());

            Aws::Utils::Outcome<Aws::S3::Model::CreateMultipartUploadResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            auto response = std::make_unique<TEvCreateMultipartUploadResponse>(originalKey, std::move(outcome));
            this->Send(ev->Sender, response.release());
        } catch (const TSystemError& ex) {
            ReplyFsSystemError<TEvCreateMultipartUploadResponse>(ev->Sender,
                TStringBuilder() << "CreateMultipartUpload failed with system error"
                    << ": key# " << originalKey, ex, originalKey);
        } catch (const std::exception& ex) {
            YDB_LOG_ERROR("CreateMultipartUpload failed",
                {"key", originalKey},
                {"error", ex.what()});
            ReplyError<TEvCreateMultipartUploadResponse>(ev->Sender, {.ErrorMessage = ex.what()}, originalKey);
        }
    }

    void Handle(TEvUploadPartRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const auto& body = ev->Get()->Body;
        const TString originalKey = TString(request.GetKey().data(), request.GetKey().size());
        const TString uploadId = TString(request.GetUploadId().data(), request.GetUploadId().size());
        const int partNumber = request.GetPartNumber();

        YDB_LOG_DEBUG("UploadPart",
            {"key", originalKey},
            {"uploadId", uploadId},
            {"part", partNumber},
            {"size", body.size()});

        try {
            const TString key = GetIncompletePath(originalKey.c_str());
            auto it = ActiveUploads.find(uploadId);
            if (it == ActiveUploads.end()) {
                // If the UploadId is not found in ActiveUploads,
                // it means that a restart has occurred and all parts have started being written again
                // so we can simply create a new session.
                if (partNumber != 1) {
                    const TString errorMsg = TStringBuilder()
                        << "Cannot create new upload session for part " << partNumber
                        << " (uploadId: " << uploadId << "). Session must start with part 1.";
                    YDB_LOG_ERROR(errorMsg,
                        {"part", partNumber},
                        {"uploadId", uploadId});
                    ReplyError<TEvUploadPartResponse>(ev->Sender,
                        {.ErrorMessage = errorMsg, .ErrorType = Aws::S3::S3Errors::INTERNAL_FAILURE},
                        originalKey);
                    return;
                }
                it = ActiveUploads.emplace(uploadId, TMultipartUploadSession(key)).first;
            }

            auto& session = it->second;

            session.File.Write(body.data(), body.size());
            session.File.Flush();
            session.TotalSize += body.size();

            YDB_LOG_INFO("UploadPart: written under lock",
                {"uploadId", uploadId},
                {"part", partNumber},
                {"totalSize", session.TotalSize});

            const TString etag = TStringBuilder() << "\"part" << partNumber << "\"";

            Aws::S3::Model::UploadPartResult awsResult;
            awsResult.SetETag(etag.c_str());

            Aws::Utils::Outcome<Aws::S3::Model::UploadPartResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            auto response = std::make_unique<TEvUploadPartResponse>(originalKey, std::move(outcome));
            this->Send(ev->Sender, response.release());
        } catch (const TSystemError& ex) {
            ReplyFsSystemError<TEvUploadPartResponse>(ev->Sender,
                TStringBuilder() << "UploadPart failed with system error"
                    << ": key# " << originalKey
                    << ", uploadId# " << uploadId, ex, originalKey);
        } catch (const std::exception& ex) {
            YDB_LOG_ERROR("UploadPart failed",
                {"key", originalKey},
                {"uploadId", uploadId},
                {"error", ex.what()});
            ReplyError<TEvUploadPartResponse>(ev->Sender, {.ErrorMessage = ex.what()}, originalKey);
        }
    }

    void Handle(TEvCompleteMultipartUploadRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        const TString uploadId = TString(request.GetUploadId().data(), request.GetUploadId().size());

        YDB_LOG_DEBUG("CompleteMultipartUpload",
            {"key", key},
            {"uploadId", uploadId});

        try {
            const TString incompleteKey = GetIncompletePath(key.c_str());
            auto it = ActiveUploads.find(uploadId);
            if (it == ActiveUploads.end()) {
                ReplyError<TEvCompleteMultipartUploadResponse>(ev->Sender,
                    {
                        .ErrorMessage = TStringBuilder() << "Upload session not found: uploadId# " << uploadId,
                        .ErrorType = Aws::S3::S3Errors::INTERNAL_FAILURE,
                        .Retryable = true,
                        .ExceptionName = "FsCompleteMultipartUploadFailed",
                    },
                    key);
                return;
            }

            auto& session = it->second;
            session.File.Flush();

            if (!NFs::Rename(incompleteKey, key)) {
                const TString errorMsg = TStringBuilder()
                    << "Failed to rename " << incompleteKey << " to " << key
                    << ": " << LastSystemErrorText();
                YDB_LOG_ERROR(errorMsg,
                    {"incompleteKey", incompleteKey},
                    {"key", key});

                session.File.Close();
                NFs::Remove(incompleteKey);
                ActiveUploads.erase(it);

                ReplyError<TEvCompleteMultipartUploadResponse>(ev->Sender,
                    {
                        .ErrorMessage = errorMsg,
                        .ErrorType = Aws::S3::S3Errors::INTERNAL_FAILURE,
                        .Retryable = true,
                        .ExceptionName = "FsCompleteMultipartUploadFailed",
                    },
                    key);
                return;
            }
            FsyncParentDir(key);
            session.File.Close();

            YDB_LOG_INFO("CompleteMultipartUpload: file mv",
                {"uploadId", uploadId},
                {"totalSize", session.TotalSize},
                {"from", incompleteKey},
                {"to", key});

            ActiveUploads.erase(it);

            Aws::S3::Model::CompleteMultipartUploadResult awsResult;
            awsResult.SetKey(request.GetKey());
            const TString etag = "\"completed\"";
            awsResult.SetETag(etag.c_str());

            Aws::Utils::Outcome<Aws::S3::Model::CompleteMultipartUploadResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            auto response = std::make_unique<TEvCompleteMultipartUploadResponse>(key, std::move(outcome));
            this->Send(ev->Sender, response.release());
        } catch (const TSystemError& ex) {
            ReplyFsSystemError<TEvCompleteMultipartUploadResponse>(ev->Sender,
                TStringBuilder() << "CompleteMultipartUpload failed with system error"
                    << ": key# " << key
                    << ", uploadId# " << uploadId, ex, key);
        } catch (const std::exception& ex) {
            YDB_LOG_ERROR("CompleteMultipartUpload failed",
                {"key", key},
                {"uploadId", uploadId},
                {"error", ex.what()});
            ReplyError<TEvCompleteMultipartUploadResponse>(ev->Sender, {.ErrorMessage = ex.what()}, key);
        }
    }

    void Handle(TEvAbortMultipartUploadRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        const TString uploadId = TString(request.GetUploadId().data(), request.GetUploadId().size());

        YDB_LOG_DEBUG("AbortMultipartUpload",
            {"key", key},
            {"uploadId", uploadId});

        try {
            auto it = ActiveUploads.find(uploadId);
            if (it == ActiveUploads.end()) {
                YDB_LOG_WARN("AbortMultipartUpload: session not found",
                    {"uploadId", uploadId});
            } else {
                auto& session = it->second;
                const TString filePath = session.Key;

                bool removed = NFs::Remove(filePath);
                if (!removed) {
                    YDB_LOG_WARN("AbortMultipartUpload: failed to delete incomplete file",
                        {"uploadId", uploadId},
                        {"file", filePath});
                }
                ActiveUploads.erase(it);

                YDB_LOG_INFO("AbortMultipartUpload: file deleted, lock released",
                    {"uploadId", uploadId});
            }

            ReplySuccess<TEvAbortMultipartUploadResponse>(ev->Sender, key);
        } catch (const std::exception& ex) {
            YDB_LOG_ERROR("AbortMultipartUpload failed",
                {"key", key},
                {"uploadId", uploadId},
                {"error", ex.what()});
            ReplyError<TEvAbortMultipartUploadResponse>(ev->Sender, {.ErrorMessage = ex.what()}, key);
        }
    }

    void Handle(TEvUploadPartCopyRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());

        YDB_LOG_WARN("UploadPartCopy: not implemented");
        ReplyError<TEvUploadPartCopyResponse>(ev->Sender, {.ErrorMessage = "Not implemented"}, key);
    }
};

} // anonymous namespace

TFsExternalStorage::TFsExternalStorage(const TString& basePath)
    : BasePath(basePath)
{
}

TFsExternalStorage::~TFsExternalStorage() {
    Shutdown();
}

void TFsExternalStorage::EnsureActor() const {
    if (ActorCreated) {
        return;
    }

    auto actor = new TFsOperationActor(BasePath);
    OperationActorId = TlsActivationContext->AsActorContext().Register(
        actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    ActorCreated = true;

    YDB_LOG_INFO("TFsExternalStorage: Created persistent actor",
        {"operationActorId", OperationActorId});
}

void TFsExternalStorage::Shutdown() {
    if (ActorCreated && TlsActivationContext) {
        YDB_LOG_INFO("TFsExternalStorage: Shutting down actor",
            {"operationActorId", OperationActorId});
        TlsActivationContext->AsActorContext().Send(OperationActorId, new NActors::TEvents::TEvPoison());
        ActorCreated = false;
    }
}

template <typename TEvPtr>
void TFsExternalStorage::ExecuteImpl(TEvPtr& ev) const {
    EnsureActor();
    TlsActivationContext->AsActorContext().Send(ev->Forward(OperationActorId));
}

void TFsExternalStorage::Execute(TEvPutObjectRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvGetObjectRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvHeadObjectRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvDeleteObjectRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvCheckObjectExistsRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvListObjectsRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvDeleteObjectsRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvUploadPartRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvUploadPartCopyRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

} // NKikimr::NWrappers::NExternalStorage
