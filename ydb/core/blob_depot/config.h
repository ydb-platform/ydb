#pragma once

#include "blob_limits.h"

#include <ydb/core/protos/blob_depot_config.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>

#include <util/datetime/base.h>
#include <util/generic/strbuf.h>
#include <util/string/builder.h>
#include <util/string/split.h>

#include <optional>

namespace NKikimrConfig {
    class TAwsClientConfig;
}

namespace NKikimr::NBlobDepot {

    // Includes time waiting for a balancer, an SDK worker, and the upload itself.
    inline constexpr TDuration DefaultS3PutTimeout = TDuration::Minutes(10);
    inline constexpr TDuration MaxS3PutTimeout = TDuration::Days(1);

    TDuration GetS3PutTimeout(const NKikimrSchemeOp::TS3Settings& settings,
        const NKikimrConfig::TAwsClientConfig& defaults);

    inline TString MakeS3BasePath(const NKikimrSchemeOp::TS3Settings& settings, TStringBuf name) {
        // The SDK drops empty segments in point requests. LIST and batch DELETE use literal keys.
        const TString rawPath = TStringBuilder() << settings.GetObjectKeyPattern() << '/' << name;
        TString path;
        path.reserve(rawPath.size());
        for (TStringBuf segment : StringSplitter(rawPath).Split('/').SkipEmpty()) {
            if (path) {
                path += '/';
            }
            path += segment;
        }
        return path;
    }

    inline std::optional<TStringBuf> ValidateS3BasePath(TStringBuf basePath) {
        if (basePath.empty()) {
            return "The effective S3 key prefix must be nonempty";
        }
        for (TStringBuf segment : StringSplitter(basePath).Split('/')) {
            if (segment == "." || segment == "..") {
                return "The effective S3 key prefix must not contain dot segments";
            }
        }
        return std::nullopt;
    }

    inline std::optional<TStringBuf> ValidateDataCooling(const NKikimrBlobDepot::TS3BackendSettings& settings) {
        if (!settings.HasAsyncMode()) {
            return std::nullopt;
        }

        const auto& async = settings.GetAsyncMode();
        if (!settings.HasSettings() || settings.GetSettings().GetBucket().empty()) {
            return "AsyncMode requires S3 settings with a bucket";
        }
        if (async.HasMaxPendingBytes() && async.GetMaxPendingBytes() <= async.GetHotTargetBytes()) {
            return "MaxPendingBytes must be greater than HotTargetBytes";
        }
        if (async.HasMaxPendingBytes() && async.GetMaxPendingBytes() < MaxWriteCreditBytes) {
            return "MaxPendingBytes must accommodate a maximum-sized blob and its footer";
        }
        if (async.HasThrottleStartBytes() || async.HasThrottleMaxBytesPerSecond()) {
            if (!async.HasMaxPendingBytes() || !async.HasThrottleStartBytes() ||
                    async.GetThrottleStartBytes() >= async.GetMaxPendingBytes() || !async.GetThrottleMaxBytesPerSecond()) {
                return "Throttling requires ThrottleStartBytes below MaxPendingBytes and a positive ThrottleMaxBytesPerSecond";
            }
        }
        if (!async.GetUploadPutsInFlight() || !async.GetTargetObjectBytes()) {
            return "UploadPutsInFlight and TargetObjectBytes must be positive";
        }
        if (async.GetRepackBelowLivePercent() > 100) {
            return "RepackBelowLivePercent must be at most 100";
        }
        return std::nullopt;
    }

} // NKikimr::NBlobDepot
