#pragma once

#include <library/cpp/json/json_reader.h>

#include <map>
#include <vector>

namespace NYql::NSo {

enum EStatus {
    STATUS_OK,
    STATUS_RETRIABLE_ERROR,
    STATUS_FATAL_ERROR
};

template <typename T>
class TSolomonClientResponse {
public:
    TSolomonClientResponse();
    explicit TSolomonClientResponse(const TString& error);
    explicit TSolomonClientResponse(T&& result);

    TSolomonClientResponse(const TSolomonClientResponse&) = default;
    TSolomonClientResponse(TSolomonClientResponse&&) = default;

public:
    EStatus Status;
    TString Error;
    T Result;
};

struct TMetric {
    std::map<TString, TString> Labels;
    TString Type;
};

struct TTimeseries {
    TMetric Metric;
    std::vector<int64_t> Timestamps;
    std::vector<double> Values;
};

struct TGetLabelsResult {
    std::vector<TString> Labels;
};
using TGetLabelsResponse = TSolomonClientResponse<TGetLabelsResult>;

struct TListMetricsResult {
    std::vector<TMetric> Metrics;
    ui64 PagesCount;
};
using TListMetricsResponse = TSolomonClientResponse<TListMetricsResult>;

struct TGetPointsCountResult {
    std::vector<ui64> PointsCount;
};
using TGetPointsCountResponse = TSolomonClientResponse<TGetPointsCountResult>;

struct TGetDataResult {
    std::vector<TTimeseries> Timeseries;
};
using TGetDataResponse = TSolomonClientResponse<TGetDataResult>;

} // namespace NYql::NSo
