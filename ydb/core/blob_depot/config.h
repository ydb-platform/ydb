#pragma once

#include "blob_limits.h"

#include <ydb/core/protos/blob_depot_config.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
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
    inline constexpr ui32 MaxCoolingObjectBytes = 64 << 20;
    inline constexpr ui32 MaxCoolingUploadsInFlight = 32;
    inline constexpr ui64 MaxCoolingUploadBufferBytes = 512ULL << 20;

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

    inline std::optional<TStringBuf> ValidateDataCooling(const NKikimrBlobDepot::TS3BackendSettings& settings) {
        if (!settings.HasAsyncMode()) {
            return std::nullopt;
        }

        const auto& async = settings.GetAsyncMode();
        if (!settings.HasSettings() || settings.GetSettings().GetBucket().empty()) {
            return "AsyncMode requires S3 settings with a bucket";
        }
        if (!async.GetUploadPutsInFlight() || !async.GetTargetObjectBytes()) {
            return "UploadPutsInFlight and TargetObjectBytes must be positive";
        }
        if (async.GetTargetObjectBytes() > MaxCoolingObjectBytes) {
            return "TargetObjectBytes must be at most 64 MiB";
        }
        if (async.GetUploadPutsInFlight() > MaxCoolingUploadsInFlight) {
            return "UploadPutsInFlight must be at most 32";
        }
        // A single blob larger than the pack target is uploaded in its own object.
        const ui64 bufferBytes = Max(MaxBlobSize, async.GetTargetObjectBytes());
        if (bufferBytes * async.GetUploadPutsInFlight() > MaxCoolingUploadBufferBytes) {
            return "Concurrent cooling upload buffers must fit within 512 MiB";
        }
        return std::nullopt;
    }

} // NKikimr::NBlobDepot
