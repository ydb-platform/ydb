#pragma once

#include "common.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/GetObjectRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/GetObjectResult.h>

namespace NKikimr::NWrappers::NExternalStorage {

class TEvGetObjectRequest: public TGenericRequest<TEvGetObjectRequest, EvGetObjectRequest, Aws::S3::Model::GetObjectRequest> {
private:
    using TBase = TGenericRequest<TEvGetObjectRequest, EvGetObjectRequest, Aws::S3::Model::GetObjectRequest>;
public:
    using TBase::TBase;
};

class TEvGetObjectResponse: public TResponseWithBody<TEvGetObjectResponse, EvGetObjectResponse, Aws::S3::Model::GetObjectResult, Aws::String> {
private:
    using TBase = TResponseWithBody<TEvGetObjectResponse, EvGetObjectResponse, Aws::S3::Model::GetObjectResult, Aws::String>;
    std::pair<ui64, ui64> ReadInterval;

public:
    ui32 GetReadIntervalLength() const {
        return ReadInterval.second - ReadInterval.first + 1;
    }

    const std::pair<ui64, ui64>& GetReadInterval() const {
        return ReadInterval;
    }

    static TResult ResultFromOutcome(const TOutcome& outcome) {
        if (outcome.IsSuccess()) {
            return outcome.GetResult().GetETag();
        } else {
            return outcome.GetError();
        }
    }

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

    explicit TEvGetObjectResponse(const TBase::TKey& key, const typename TBase::TOutcome& outcome)
        : TBase(key, outcome)
    {
        Y_ABORT_UNLESS(false);
    }

    explicit TEvGetObjectResponse(const TBase::TKey& key, const std::pair<ui64, ui64> range, const typename TBase::TOutcome& outcome)
        : TBase(key, outcome)
        , ReadInterval(range)
    {
    }

    explicit TEvGetObjectResponse(const TBase::TKey& key, const std::pair<ui64, ui64> range, const typename TBase::TOutcome& outcome, TString&& body)
        : TBase(key, outcome, std::move(body))
        , ReadInterval(range)
    {
    }
};

}
