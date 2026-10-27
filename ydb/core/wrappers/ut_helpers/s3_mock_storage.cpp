#include "s3_mock_storage.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/client/AWSError.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/http/URI.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/S3Errors.h>

#include <ydb/library/actors/core/actor.h>

#include <util/string/builder.h>

#include <optional>
#include <util/string/cast.h>

namespace NKikimr::NWrappers::NTestHelpers {

namespace {

using namespace NExternalStorage;
using TStatus = TS3Mock::TStorage::EStatus;
using TResult = TS3Mock::TStorage::TResult;

TString ToTString(const Aws::String& value) {
    return TString(value.data(), value.size());
}

// The key of a response event, as the S3 client fills it from the request
std::optional<TString> KeyOf(const Aws::String& key) {
    return ToTString(key);
}

// What the S3 client makes of an error response of the HTTP mock: a 404 with a body names its error code (NoSuchKey,
// NoSuchUpload); without a body (HEAD) it is RESOURCE_NOT_FOUND; a 400 names the code of an XML body, if any.
// None of them is retryable by the client.
Aws::S3::S3Error MakeError(const TResult& result, bool bodyless) {
    Y_ABORT_UNLESS(result.Status != TStatus::Ok);
    if (result.Status == TStatus::NotFound) {
        if (bodyless) {
            Aws::Client::AWSError<Aws::S3::S3Errors> error(Aws::S3::S3Errors::RESOURCE_NOT_FOUND, "", "", false);
            error.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
            return Aws::S3::S3Error(std::move(error));
        }
        const auto type = result.Error == "NoSuchUpload" ? Aws::S3::S3Errors::NO_SUCH_UPLOAD : Aws::S3::S3Errors::NO_SUCH_KEY;
        Aws::Client::AWSError<Aws::S3::S3Errors> error(type, result.Error.c_str(), "", false);
        error.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
        return Aws::S3::S3Error(std::move(error));
    }

    TString code;
    if (TStringBuf message = result.Error; message.SkipPrefix("<Error><Code>")) {
        code = TString(message.Before('<'));
    }
    Aws::Client::AWSError<Aws::S3::S3Errors> error(Aws::S3::S3Errors::UNKNOWN, code.c_str(), result.Error.c_str(), false);
    error.SetResponseCode(Aws::Http::HttpResponseCode::BAD_REQUEST);
    return Aws::S3::S3Error(std::move(error));
}

class TS3MockStorageOperator: public IExternalStorageOperator {
public:
    TS3MockStorageOperator(std::shared_ptr<TS3Mock::TStorage> storage, const TString& bucket, bool useVirtualAddressing)
        : Storage(std::move(storage))
        , Bucket(bucket)
        , UseVirtualAddressing(useVirtualAddressing)
    {
    }

    void Execute(TEvListObjectsRequest::TPtr& ev) const override {
        const auto& request = ev->Get()->GetRequest();
        const TString bucketName = UseVirtualAddressing ? TString() : "/" + Bucket;
        Aws::S3::Model::ListObjectsResult result;
        result.SetName(bucketName.c_str());
        result.SetIsTruncated(false);
        for (const auto& [key, size] : Storage->List(bucketName, ToTString(request.GetPrefix()))) {
            Aws::S3::Model::Object object;
            object.SetKey(key.c_str());
            object.SetSize(size);
            result.AddContents(std::move(object));
        }
        Reply(ev->Sender, std::make_unique<TEvListObjectsResponse>(TEvListObjectsResponse::TOutcome(std::move(result))));
    }

    void Execute(TEvGetObjectRequest::TPtr& ev) const override {
        const auto& request = ev->Get()->GetRequest();
        const TString rangeValue = ToTString(request.GetRange());
        std::pair<ui64, ui64> range;
        Y_ABORT_UNLESS(request.RangeHasBeenSet() && TEvGetObjectResponse::TryParseRange(rangeValue, range));

        auto read = Storage->Read(Path(request.GetKey()), &rangeValue, false);
        if (read.Status != TStatus::Ok) {
            Reply(ev->Sender, std::make_unique<TEvGetObjectResponse>(KeyOf(request.GetKey()), range,
                TEvGetObjectResponse::TOutcome(MakeError(read, false))));
            return;
        }
        Aws::S3::Model::GetObjectResult result;
        result.SetETag(read.ETag.c_str());
        result.SetContentLength(read.Body.size());
        result.SetAcceptRanges("bytes");
        result.SetContentRange((TStringBuilder() << "bytes " << read.Range.first << "-" << read.Range.second << "/" << read.ObjectSize).c_str());
        Reply(ev->Sender, std::make_unique<TEvGetObjectResponse>(KeyOf(request.GetKey()), range,
            TEvGetObjectResponse::TOutcome(std::move(result)), std::move(read.Body)));
    }

    void Execute(TEvHeadObjectRequest::TPtr& ev) const override {
        const auto& request = ev->Get()->GetRequest();
        Reply(ev->Sender, std::make_unique<TEvHeadObjectResponse>(KeyOf(request.GetKey()), Head(request.GetKey())));
    }

    void Execute(TEvCheckObjectExistsRequest::TPtr& ev) const override {
        const auto& request = ev->Get()->GetRequest();
        Reply(ev->Sender, std::make_unique<TEvCheckObjectExistsResponse>(Head(request.GetKey()), ev->Get()->GetRequestContext()));
    }

    void Execute(TEvPutObjectRequest::TPtr& ev) const override {
        const auto& request = ev->Get()->GetRequest();
        Cerr << "S3_MOCK::HttpServeWrite: " << Path(request.GetKey()) << " /  / " << ev->Get()->Body.size() << Endl;
        const auto put = Storage->Put(Path(request.GetKey()), ev->Get()->Body);
        Aws::S3::Model::PutObjectResult result;
        result.SetETag(put.ETag.c_str());
        Reply(ev->Sender, std::make_unique<TEvPutObjectResponse>(KeyOf(request.GetKey()), TEvPutObjectResponse::TOutcome(std::move(result))));
    }

    void Execute(TEvDeleteObjectRequest::TPtr& ev) const override {
        const auto& request = ev->Get()->GetRequest();
        const auto deleted = Storage->Delete(Path(request.GetKey()));
        TEvDeleteObjectResponse::TOutcome outcome = deleted.Status == TStatus::Ok
            ? TEvDeleteObjectResponse::TOutcome(Aws::S3::Model::DeleteObjectResult())
            : TEvDeleteObjectResponse::TOutcome(MakeError(deleted, true));
        Reply(ev->Sender, std::make_unique<TEvDeleteObjectResponse>(KeyOf(request.GetKey()), std::move(outcome)));
    }

    void Execute(TEvDeleteObjectsRequest::TPtr& ev) const override {
        // The HTTP mock has no multi-object delete: POST ?delete is a bad request
        Reply(ev->Sender, std::make_unique<TEvDeleteObjectsResponse>(
            TEvDeleteObjectsResponse::TOutcome(MakeError(TResult::BadRequest({}), true))));
    }

    void Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const override {
        const auto& request = ev->Get()->GetRequest();
        const auto created = Storage->CreateMultipartUpload(Path(request.GetKey()));
        Aws::S3::Model::CreateMultipartUploadResult result;
        result.SetKey(request.GetKey());
        result.SetUploadId(created.UploadId.c_str());
        Reply(ev->Sender, std::make_unique<TEvCreateMultipartUploadResponse>(KeyOf(request.GetKey()),
            TEvCreateMultipartUploadResponse::TOutcome(std::move(result))));
    }

    void Execute(TEvUploadPartRequest::TPtr& ev) const override {
        const auto& request = ev->Get()->GetRequest();
        const auto uploaded = Storage->UploadPart(Path(request.GetKey()), ToTString(request.GetUploadId()),
            ToString(request.GetPartNumber()), ev->Get()->Body);
        TEvUploadPartResponse::TOutcome outcome = [&] {
            if (uploaded.Status != TStatus::Ok) {
                return TEvUploadPartResponse::TOutcome(MakeError(uploaded, false));
            }
            Aws::S3::Model::UploadPartResult result;
            result.SetETag(uploaded.ETag.c_str());
            return TEvUploadPartResponse::TOutcome(std::move(result));
        }();
        Reply(ev->Sender, std::make_unique<TEvUploadPartResponse>(KeyOf(request.GetKey()), std::move(outcome)));
    }

    void Execute(TEvUploadPartCopyRequest::TPtr& ev) const override {
        const auto& request = ev->Get()->GetRequest();
        const TString sourceRange = ToTString(request.GetCopySourceRange());
        const auto copied = Storage->UploadPartCopy(Path(request.GetKey()), ToTString(request.GetUploadId()),
            ToString(request.GetPartNumber()), ToTString(request.GetCopySource()),
            request.CopySourceRangeHasBeenSet() ? &sourceRange : nullptr);
        TEvUploadPartCopyResponse::TOutcome outcome = [&] {
            if (copied.Status != TStatus::Ok) {
                return TEvUploadPartCopyResponse::TOutcome(MakeError(copied, false));
            }
            Aws::S3::Model::CopyPartResult part;
            part.SetETag(copied.ETag.c_str());
            Aws::S3::Model::UploadPartCopyResult result;
            result.SetCopyPartResult(std::move(part));
            return TEvUploadPartCopyResponse::TOutcome(std::move(result));
        }();
        Reply(ev->Sender, std::make_unique<TEvUploadPartCopyResponse>(KeyOf(request.GetKey()), std::move(outcome)));
    }

    void Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const override {
        const auto& request = ev->Get()->GetRequest();
        const TString path = Path(request.GetKey());
        const TString uploadId = ToTString(request.GetUploadId());
        TEvCompleteMultipartUploadResponse::TOutcome outcome = [&] {
            if (!Storage->HasMultipartUpload(path, uploadId)) {
                return TEvCompleteMultipartUploadResponse::TOutcome(MakeError(TResult::NotFound("NoSuchUpload"), false));
            }
            TVector<TString> etags;
            for (const auto& part : request.GetMultipartUpload().GetParts()) {
                const ui32 partNumber = part.GetPartNumber();
                if (!partNumber) {
                    return TEvCompleteMultipartUploadResponse::TOutcome(MakeError(TResult::BadRequest("Invalid request"), false));
                }
                if (etags.size() < partNumber) {
                    etags.resize(partNumber);
                }
                etags[partNumber - 1] = ToTString(part.GetETag());
            }
            const auto completed = Storage->CompleteMultipartUpload(path, uploadId, etags);
            if (completed.Status != TStatus::Ok) {
                return TEvCompleteMultipartUploadResponse::TOutcome(MakeError(completed, false));
            }
            Aws::S3::Model::CompleteMultipartUploadResult result;
            result.SetKey(request.GetKey());
            result.SetETag(completed.ETag.c_str());
            return TEvCompleteMultipartUploadResponse::TOutcome(std::move(result));
        }();
        Reply(ev->Sender, std::make_unique<TEvCompleteMultipartUploadResponse>(KeyOf(request.GetKey()), std::move(outcome)));
    }

    void Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const override {
        const auto& request = ev->Get()->GetRequest();
        const auto aborted = Storage->AbortMultipartUpload(Path(request.GetKey()), ToTString(request.GetUploadId()));
        TEvAbortMultipartUploadResponse::TOutcome outcome = aborted.Status == TStatus::Ok
            ? TEvAbortMultipartUploadResponse::TOutcome(Aws::S3::Model::AbortMultipartUploadResult())
            : TEvAbortMultipartUploadResponse::TOutcome(MakeError(aborted, false));
        Reply(ev->Sender, std::make_unique<TEvAbortMultipartUploadResponse>(KeyOf(request.GetKey()), std::move(outcome)));
    }

private:
    TString DoDebugString() const override {
        return "type:S3_MOCK;";
    }

    // The path of the key in an HTTP request of the S3 client: bucket and key as URL-encoded path segments
    TString Path(const Aws::String& key) const {
        Aws::Http::URI uri;
        if (!UseVirtualAddressing) {
            uri.AddPathSegment(Bucket.c_str());
        }
        uri.AddPathSegments(key);
        return ToTString(uri.GetURLEncodedPath());
    }

    Aws::Utils::Outcome<Aws::S3::Model::HeadObjectResult, Aws::S3::S3Error> Head(const Aws::String& key) const {
        const auto read = Storage->Read(Path(key), nullptr, true);
        if (read.Status != TStatus::Ok) {
            return MakeError(read, true);
        }
        Aws::S3::Model::HeadObjectResult result;
        result.SetETag(read.ETag.c_str());
        result.SetContentLength(read.ObjectSize);
        return result;
    }

    // As the S3 client does: through the reply adapter, from no particular actor
    template <class TEvResponse>
    void Reply(const NActors::TActorId& recipient, std::unique_ptr<TEvResponse>&& response) const {
        TlsActivationContext->ActorSystem()->Send(ReplyAdapter.GetRecipient(recipient),
            ReplyAdapter.RebuildReplyEvent(std::move(response)).release());
    }

    const std::shared_ptr<TS3Mock::TStorage> Storage;
    const TString Bucket;
    const bool UseVirtualAddressing;
};

class TS3MockStorageConfig: public IExternalStorageConfig {
public:
    TS3MockStorageConfig(std::shared_ptr<TS3Mock::TStorage> storage, const TString& bucket, bool useVirtualAddressing)
        : Storage(std::move(storage))
        , Bucket(bucket)
        , UseVirtualAddressing(useVirtualAddressing)
    {
    }

protected:
    IExternalStorageOperator::TPtr DoConstructStorageOperator(bool /*verbose*/) const override {
        return std::make_shared<TS3MockStorageOperator>(Storage, Bucket, UseVirtualAddressing);
    }

    TString DoGetStorageId() const override {
        return "s3_mock/" + Bucket;
    }

private:
    const std::shared_ptr<TS3Mock::TStorage> Storage;
    const TString Bucket;
    const bool UseVirtualAddressing;
};

} // anonymous

IExternalStorageConfig::TPtr MakeS3MockStorageConfig(std::shared_ptr<TS3Mock::TStorage> storage, const TString& bucket, bool useVirtualAddressing) {
    return std::make_shared<TS3MockStorageConfig>(std::move(storage), bucket, useVirtualAddressing);
}

} // NKikimr::NWrappers::NTestHelpers
