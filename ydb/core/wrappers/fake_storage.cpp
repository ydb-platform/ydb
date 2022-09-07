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

void TFakeExternalStorage::Execute(TEvListObjectsRequest::TPtr& ev) const {
    auto& awsPrefix = ev->Get()->GetRequest().GetPrefix();
    const TString prefix(awsPrefix.data(), awsPrefix.size());
    THolder<TEvListObjectsResponse> result;
    if (!prefix) {
        TEvListObjectsResponse::TOutcome awsOutcome;
        result = MakeHolder<TEvListObjectsResponse>(awsOutcome);
        TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
    } else {
        TGuard<TMutex> g(Mutex);
        TEvListObjectsResponse::TResult awsResult;
        for (auto&& i : Data) {
            if (!i.first.StartsWith(prefix)) {
                continue;
            } else {
                Aws::S3::Model::Object objectMeta;
                objectMeta.SetKey(i.first);
                objectMeta.SetSize(i.second.size());
                awsResult.AddContents(std::move(objectMeta));
            }
        }
        TEvListObjectsResponse::TOutcome awsOutcome(awsResult);
        result = MakeHolder<TEvListObjectsResponse>(awsOutcome);
        TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
    }
}

void TFakeExternalStorage::Execute(TEvGetObjectRequest::TPtr& ev) const {
    TGuard<TMutex> g(Mutex);
    auto& awsKey = ev->Get()->Request.GetKey();
    const TString key(awsKey.data(), awsKey.size());
    auto it = Data.find(key);
    TString data;

    if (it != Data.end()) {
        Aws::S3::Model::GetObjectResult awsResult;
        awsResult.SetETag(MD5::Calc(it->second));
        data = it->second;

        auto awsRange = ev->Get()->Request.GetRange();
        if (awsRange.size()) {
            const TString strRange(awsRange.data(), awsRange.size());
            std::pair<ui64, ui64> range;
            if (!TryParseRange(strRange, range)) {
                Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult, Aws::S3::S3Error> awsOutcome;
                THolder<TEvGetObjectResponse> result(new TEvGetObjectResponse(key, awsOutcome, std::move(data)));
                TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
                return;
            } else {
                data = data.substr(range.first, range.second - range.first + 1);
            }
        }

        Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult, Aws::S3::S3Error> awsOutcome(std::move(awsResult));
        THolder<TEvGetObjectResponse> result(new TEvGetObjectResponse(key, awsOutcome, std::move(data)));
        TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
    } else {
        Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult, Aws::S3::S3Error> awsOutcome;
        THolder<TEvGetObjectResponse> result(new TEvGetObjectResponse(key, awsOutcome, std::move(data)));
        TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
    }
}

void TFakeExternalStorage::Execute(TEvHeadObjectRequest::TPtr& ev) const {
    TGuard<TMutex> g(Mutex);
    auto& awsKey = ev->Get()->Request.GetKey();
    const TString key(awsKey.data(), awsKey.size());

    auto it = Data.find(key);
    if (it != Data.end()) {
        Aws::S3::Model::HeadObjectResult awsResult;
        awsResult.SetETag(MD5::Calc(it->second));
        awsResult.SetContentLength(it->second.size());
        Aws::Utils::Outcome<Aws::S3::Model::HeadObjectResult, Aws::S3::S3Error> awsOutcome(awsResult);
        THolder<TEvHeadObjectResponse> result(new TEvHeadObjectResponse(key, awsOutcome));
        TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
    } else {
        Aws::Utils::Outcome<Aws::S3::Model::HeadObjectResult, Aws::S3::S3Error> awsOutcome;
        THolder<TEvHeadObjectResponse> result(new TEvHeadObjectResponse(key, awsOutcome));
        TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
    }
}

void TFakeExternalStorage::Execute(TEvPutObjectRequest::TPtr& ev) const {
    TGuard<TMutex> g(Mutex);
    Aws::S3::Model::PutObjectResult awsResult;
    auto& awsKey = ev->Get()->Request.GetKey();
    const TString key(awsKey.data(), awsKey.size());
    Data[key] = ev->Get()->Body;

    THolder<TEvPutObjectResponse> result(new TEvPutObjectResponse(key, awsResult));
    TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
}

void TFakeExternalStorage::Execute(TEvDeleteObjectRequest::TPtr& ev) const {
    TGuard<TMutex> g(Mutex);
    Aws::S3::Model::DeleteObjectResult awsResult;
    auto& awsKey = ev->Get()->Request.GetKey();
    const TString key(awsKey.data(), awsKey.size());
    Data.erase(key);

    THolder<TEvDeleteObjectResponse> result(new TEvDeleteObjectResponse(key, awsResult));
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
    auto& awsKey = ev->Get()->GetRequest().GetKey();
    const TString key(awsKey.data(), awsKey.size());

    auto it = Data.find(key);
    if (it != Data.end()) {
        Aws::S3::Model::HeadObjectResult awsResult;
        awsResult.SetETag(MD5::Calc(it->second));
        awsResult.SetContentLength(it->second.size());
        THolder<TEvCheckObjectExistsResponse> result(new TEvCheckObjectExistsResponse(ev->Get()->GetRequest(), awsResult, ev->Get()->GetRequestContext()));
        TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
    } else {
        Aws::Utils::Outcome<Aws::S3::Model::HeadObjectResult, Aws::S3::S3Error> awsOutcome;
        THolder<TEvCheckObjectExistsResponse> result(new TEvCheckObjectExistsResponse(ev->Get()->GetRequest(), awsOutcome, ev->Get()->GetRequestContext()));
        TlsActivationContext->ActorSystem()->Send(ev->Sender, result.Release());
    }

}
}

#endif // KIKIMR_DISABLE_S3_OPS
