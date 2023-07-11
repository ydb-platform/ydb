#include "fake_storage.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/internal/AWSHttpResourceClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/ResponseStream.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <contrib/libs/curl/include/curl/curl.h>

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/log.h>
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

void TFakeExternalStorage::Execute(TEvListObjectsRequest::TPtr& ev) const {
    auto awsResult = BuildListObjectsResult(ev->Get()->GetRequest());
    auto result = MakeHolder<TEvListObjectsResponse>(awsResult);
    TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
}

void TFakeExternalStorage::Execute(TEvGetObjectRequest::TPtr& ev) const {
    TGuard<TMutex> g(Mutex);
    auto& bucket = GetBucket(AwsToString(ev->Get()->GetRequest().GetBucket()));
    const TString key = AwsToString(ev->Get()->GetRequest().GetKey());
    auto object = bucket.GetObject(key);
    TString data;

    if (!!object) {
        Aws::S3::Model::GetObjectResult awsResult;
        awsResult.SetETag(MD5::Calc(*object));
        data = *object;

        auto awsRange = ev->Get()->GetRequest().GetRange();
        if (awsRange.size()) {
            const TString strRange(awsRange.data(), awsRange.size());
            std::pair<ui64, ui64> range;
            if (!TryParseRange(strRange, range)) {
                Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult, Aws::S3::S3Error> awsOutcome;
                THolder<TEvGetObjectResponse> result(new TEvGetObjectResponse(key, std::move(awsOutcome), std::move(data)));
                TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
                return;
            } else {
                data = data.substr(range.first, range.second - range.first + 1);
            }
        }

        Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult, Aws::S3::S3Error> awsOutcome(std::move(awsResult));
        THolder<TEvGetObjectResponse> result(new TEvGetObjectResponse(key, std::move(awsOutcome), std::move(data)));
        TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
    } else {
        Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult, Aws::S3::S3Error> awsOutcome;
        THolder<TEvGetObjectResponse> result(new TEvGetObjectResponse(key, std::move(awsOutcome), std::move(data)));
        TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
    }
}

void TFakeExternalStorage::Execute(TEvHeadObjectRequest::TPtr& ev) const {
    TGuard<TMutex> g(Mutex);
    auto& bucket = GetBucket(AwsToString(ev->Get()->GetRequest().GetBucket()));
    const TString key = AwsToString(ev->Get()->GetRequest().GetKey());
    auto object = bucket.GetObject(key);
    if (object) {
        Aws::S3::Model::HeadObjectResult awsResult;
        awsResult.SetETag(MD5::Calc(*object));
        awsResult.SetContentLength(object->size());
        Aws::Utils::Outcome<Aws::S3::Model::HeadObjectResult, Aws::S3::S3Error> awsOutcome(awsResult);
        THolder<TEvHeadObjectResponse> result(new TEvHeadObjectResponse(key, std::move(awsOutcome)));
        TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
    } else {
        Aws::Utils::Outcome<Aws::S3::Model::HeadObjectResult, Aws::S3::S3Error> awsOutcome;
        THolder<TEvHeadObjectResponse> result(new TEvHeadObjectResponse(key, std::move(awsOutcome)));
        TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
    }
}

void TFakeExternalStorage::Execute(TEvPutObjectRequest::TPtr& ev) const {
    TGuard<TMutex> g(Mutex);
    const TString key = AwsToString(ev->Get()->GetRequest().GetKey());
    auto& bucket = MutableBucket(AwsToString(ev->Get()->GetRequest().GetBucket()));
    bucket.PutObject(key, ev->Get()->Body);
    Aws::S3::Model::PutObjectResult awsResult;

    THolder<TEvPutObjectResponse> result(new TEvPutObjectResponse(key, awsResult));
    TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
}

void TFakeExternalStorage::Execute(TEvDeleteObjectRequest::TPtr& ev) const {
    TGuard<TMutex> g(Mutex);
    Aws::S3::Model::DeleteObjectResult awsResult;
    auto& bucket = MutableBucket(AwsToString(ev->Get()->GetRequest().GetBucket()));
    const TString key = AwsToString(ev->Get()->GetRequest().GetKey());
    bucket.Remove(key);

    THolder<TEvDeleteObjectResponse> result(new TEvDeleteObjectResponse(key, awsResult));
    TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
}

void TFakeExternalStorage::Execute(TEvDeleteObjectsRequest::TPtr& ev) const {
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

    THolder<TEvDeleteObjectsResponse> result(new TEvDeleteObjectsResponse(awsResult));
    TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
}

void TFakeExternalStorage::Execute(TEvCreateMultipartUploadRequest::TPtr& /*ev*/) const {
}

void TFakeExternalStorage::Execute(TEvUploadPartRequest::TPtr& /*ev*/) const {
}

void TFakeExternalStorage::Execute(TEvCompleteMultipartUploadRequest::TPtr& /*ev*/) const {
}

void TFakeExternalStorage::Execute(TEvAbortMultipartUploadRequest::TPtr& /*ev*/) const {
}

void TFakeExternalStorage::Execute(TEvCheckObjectExistsRequest::TPtr& ev) const {
    TGuard<TMutex> g(Mutex);
    auto awsResult = BuildListObjectsResult(ev->Get()->GetRequest());
    THolder<TEvCheckObjectExistsResponse> result(new TEvCheckObjectExistsResponse(awsResult, ev->Get()->GetRequestContext()));
    TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());

}

void TFakeExternalStorage::Execute(TEvUploadPartCopyRequest::TPtr& /*ev*/) const {
}

}

#endif // KIKIMR_DISABLE_S3_OPS
