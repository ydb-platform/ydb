#include "fake_storage.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/internal/AWSHttpResourceClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/ResponseStream.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <contrib/libs/curl/include/curl/curl.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/digest/md5/md5.h>
#include <util/string/cast.h>

#ifndef KIKIMR_DISABLE_S3_OPS
namespace NKikimr::NWrappers::NExternalStorage {

using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace Aws::Utils::Stream;

namespace {

static bool TryParseRange(const TString& str, std::pair<ui64, ui64>& range) {
    TStringBuf buf(str);
    if (!buf.SkipPrefix("bytes=")) {
        return false;
    }

    ui64 start;
    if (!TryFromString(buf.NextTok('-'), start)) {
        return false;
    }

    ui64 end;
    if (!TryFromString(buf, end)) {
        return false;
    }

    range = std::make_pair(start, end);
    return true;
}
}

TEvListObjectsResponse::TResult TFakeExternalStorage::BuildListObjectsResult(const TEvListObjectsRequest::TRequest& request) const {
    auto& bucket = GetBucket(AwsToString(request.GetBucket()));
    auto& awsPrefix = request.GetPrefix();
    const TString prefix(awsPrefix.data(), awsPrefix.size());
    THolder<TEvListObjectsResponse> result;
    TGuard<TMutex> g(Mutex);
    TEvListObjectsResponse::TAwsResult awsResult;
    for (auto&& i : bucket) {
        if (!!prefix && !i.first.StartsWith(prefix)) {
            continue;
        }
        Aws::S3::Model::Object objectMeta;
        objectMeta.SetKey(i.first);
        objectMeta.SetSize(i.second.size());
        awsResult.AddContents(std::move(objectMeta));
        break;
    }
    awsResult.SetIsTruncated(bucket.GetSize() > 1);
    return awsResult;
}

void TFakeExternalStorage::Execute(TEvListObjectsRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const {
    auto awsResult = BuildListObjectsResult(ev->Get()->GetRequest());
    auto result = std::make_unique<TEvListObjectsResponse>(awsResult);
    adapter.Reply(ev->Sender, std::move(result));
}

void TFakeExternalStorage::Execute(TEvGetObjectRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const {
    TGuard<TMutex> g(Mutex);
    auto& bucket = GetBucket(AwsToString(ev->Get()->GetRequest().GetBucket()));
    const TString key = AwsToString(ev->Get()->GetRequest().GetKey());
    auto object = bucket.GetObject(key);
    TString data;

    std::pair<ui64, ui64> range;
    auto awsRange = ev->Get()->GetRequest().GetRange();
    Y_ABORT_UNLESS(awsRange.size());
    const TString strRange(awsRange.data(), awsRange.size());
    AFL_VERIFY(TryParseRange(strRange, range))("original", strRange);

    if (!!object) {
        AFL_DEBUG(NKikimrServices::S3_WRAPPER)("method", "GetObject")("id", key)("range", strRange)("object_exists", true);
        Aws::S3::Model::GetObjectResult awsResult;
        awsResult.WithAcceptRanges(awsRange).SetETag(MD5::Calc(*object));
        data = *object;
        data = data.substr(range.first, range.second - range.first + 1);

        Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult, Aws::S3::S3Error> awsOutcome(std::move(awsResult));
        std::unique_ptr<TEvGetObjectResponse> result(new TEvGetObjectResponse(key, range, std::move(awsOutcome), std::move(data)));
        adapter.Reply(ev->Sender, std::move(result));
    } else {
        AFL_DEBUG(NKikimrServices::S3_WRAPPER)("method", "GetObject")("id", key)("range", strRange)("object_exists", false);
        Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult, Aws::S3::S3Error> awsOutcome;
        std::unique_ptr<TEvGetObjectResponse> result(new TEvGetObjectResponse(key, range, std::move(awsOutcome), std::move(data)));
        adapter.Reply(ev->Sender, std::move(result));
    }
}

void TFakeExternalStorage::Execute(TEvHeadObjectRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const {
    TGuard<TMutex> g(Mutex);
    auto& bucket = GetBucket(AwsToString(ev->Get()->GetRequest().GetBucket()));
    const TString key = AwsToString(ev->Get()->GetRequest().GetKey());
    auto object = bucket.GetObject(key);
    if (object) {
        Aws::S3::Model::HeadObjectResult awsResult;
        awsResult.SetETag(MD5::Calc(*object));
        awsResult.SetContentLength(object->size());
        Aws::Utils::Outcome<Aws::S3::Model::HeadObjectResult, Aws::S3::S3Error> awsOutcome(awsResult);
        std::unique_ptr<TEvHeadObjectResponse> result(new TEvHeadObjectResponse(key, std::move(awsOutcome)));
        adapter.Reply(ev->Sender, std::move(result));
    } else {
        Aws::Utils::Outcome<Aws::S3::Model::HeadObjectResult, Aws::S3::S3Error> awsOutcome;
        std::unique_ptr<TEvHeadObjectResponse> result(new TEvHeadObjectResponse(key, std::move(awsOutcome)));
        adapter.Reply(ev->Sender, std::move(result));
    }
}

void TFakeExternalStorage::Execute(TEvPutObjectRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const {
    TGuard<TMutex> g(Mutex);
    const TString key = AwsToString(ev->Get()->GetRequest().GetKey());
    AFL_DEBUG(NKikimrServices::S3_WRAPPER)("method", "PutObject")("id", key);
    auto& bucket = MutableBucket(AwsToString(ev->Get()->GetRequest().GetBucket()));
    bucket.PutObject(key, ev->Get()->Body);
    Aws::S3::Model::PutObjectResult awsResult;

    std::unique_ptr<TEvPutObjectResponse> result(new TEvPutObjectResponse(key, awsResult));
    adapter.Reply(ev->Sender, std::move(result));
}

void TFakeExternalStorage::Execute(TEvDeleteObjectRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const {
    TGuard<TMutex> g(Mutex);
    Aws::S3::Model::DeleteObjectResult awsResult;
    auto& bucket = MutableBucket(AwsToString(ev->Get()->GetRequest().GetBucket()));
    const TString key = AwsToString(ev->Get()->GetRequest().GetKey());
    AFL_DEBUG(NKikimrServices::S3_WRAPPER)("method", "DeleteObject")("id", key);
    bucket.Remove(key);

    std::unique_ptr<TEvDeleteObjectResponse> result(new TEvDeleteObjectResponse(key, awsResult));
    adapter.Reply(ev->Sender, std::move(result));
}

void TFakeExternalStorage::Execute(TEvDeleteObjectsRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const {
    TGuard<TMutex> g(Mutex);
    auto& bucket = MutableBucket(AwsToString(ev->Get()->GetRequest().GetBucket()));
    Aws::S3::Model::DeleteObjectsResult awsResult;
    for (auto&& awsKey : ev->Get()->GetRequest().GetDelete().GetObjects()) {
        const TString key = AwsToString(awsKey.GetKey());
        bucket.Remove(key);
        Aws::S3::Model::DeletedObject dObject;
        dObject.WithKey(key);
        awsResult.AddDeleted(std::move(dObject));
    }

    std::unique_ptr<TEvDeleteObjectsResponse> result(new TEvDeleteObjectsResponse(awsResult));
    adapter.Reply(ev->Sender, std::move(result));
}

void TFakeExternalStorage::Execute(TEvCreateMultipartUploadRequest::TPtr& /*ev*/, const TReplyAdapterContainer& /*adapter*/) const {
}

void TFakeExternalStorage::Execute(TEvUploadPartRequest::TPtr& /*ev*/, const TReplyAdapterContainer& /*adapter*/) const {
}

void TFakeExternalStorage::Execute(TEvCompleteMultipartUploadRequest::TPtr& /*ev*/, const TReplyAdapterContainer& /*adapter*/) const {
}

void TFakeExternalStorage::Execute(TEvAbortMultipartUploadRequest::TPtr& /*ev*/, const TReplyAdapterContainer& /*adapter*/) const {
}

void TFakeExternalStorage::Execute(TEvCheckObjectExistsRequest::TPtr& ev, const TReplyAdapterContainer& adapter) const {
    TGuard<TMutex> g(Mutex);

    auto& bucket = GetBucket(AwsToString(ev->Get()->GetRequest().GetBucket()));
    const TString key = AwsToString(ev->Get()->GetRequest().GetKey());
    auto object = bucket.GetObject(key);
    std::unique_ptr<TEvCheckObjectExistsResponse> result;
    if (object) {
        Aws::S3::Model::HeadObjectResult awsResult;
        awsResult.SetETag(MD5::Calc(*object));
        awsResult.SetContentLength(object->size());
        result.reset(new TEvCheckObjectExistsResponse(awsResult, ev->Get()->GetRequestContext()));
        Y_DEBUG_ABORT_UNLESS(result->IsSuccess());
    } else {
        Aws::Utils::Outcome<Aws::S3::Model::HeadObjectResult, Aws::S3::S3Error> awsOutcome;
        result.reset(new TEvCheckObjectExistsResponse(awsOutcome, ev->Get()->GetRequestContext()));
        Y_DEBUG_ABORT_UNLESS(!result->IsSuccess());
    }

    adapter.Reply(ev->Sender, std::move(result));
}

void TFakeExternalStorage::Execute(TEvUploadPartCopyRequest::TPtr& /*ev*/, const TReplyAdapterContainer& /*adapter*/) const {
}

}

#endif // KIKIMR_DISABLE_S3_OPS
