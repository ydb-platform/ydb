#pragma once

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/strbuf.h>

#include <array>

namespace NYql::NSo::NConstants {

// ── HTTP request / response size limits ──────────────────────────────────────

// Maximum byte length of a single HTTP GET request URL.
// Used when batching label values into GET query parameters: each batch is
// sized so that the resulting URL stays below this threshold.
// This is a Solomon API protocol constraint, not a user-configurable parameter.
constexpr ui64 MaxHttpGetRequestSize = 4_KB;

// Maximum number of bytes downloaded in a single HTTP listing response.
// Passed to IHTTPGateway::Download as the response-body size cap.
// This is a transport-level safety limit, not a user-configurable parameter.
constexpr ui64 MaxHttpGetResponseSize = 100_MB;

// ── API pagination ────────────────────────────────────────────────────────────

// Maximum number of label values returned per labels-listing API call.
// Matches the Solomon API hard limit for the "limit" parameter.
constexpr ui64 LabelsListingLimit = 100'000;

// ── gRPC / HTTP authentication ────────────────────────────────────────────────

// Value of the x-client-id header sent with every Solomon API call (both HTTP
// and gRPC). Identifies this client to the Solomon backend for rate-limiting
// and observability purposes.
constexpr TStringBuf ClientId = "yandex-query";

// ── Downsampling / time constants ─────────────────────────────────────────────

// Points older than this threshold are automatically downsampled by the Solomon
// backend (weekly rollup). Used to split the points-count query into a
// "raw" part (newer than cutoff) and a "downsampled" part (older).
constexpr TDuration DownsamplingCutoff = TDuration::Days(7);

// Default downsampling grid interval applied when the user has disabled
// explicit downsampling but the requested time range extends beyond
// DownsamplingCutoff (i.e. into the automatically-downsampled region).
constexpr TDuration DefaultGridInterval = TDuration::Minutes(5);

// ── HTTP retry policy ─────────────────────────────────────────────────────────

// HTTP status codes that indicate a transient server-side error and should
// trigger a retry. 429 = Too Many Requests, 502/503/504 = gateway errors.
constexpr std::array<long, 4> RetriableHttpCodes = {429, 502, 503, 504};

} // namespace NYql::NSo::NConstants
