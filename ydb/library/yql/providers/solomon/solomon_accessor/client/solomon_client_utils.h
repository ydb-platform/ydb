#pragma once

#include <library/cpp/json/json_reader.h>

#include <ydb/library/yql/providers/solomon/common/util.h>

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
    explicit TSolomonClientResponse(const TString& error, EStatus status = STATUS_FATAL_ERROR);
    explicit TSolomonClientResponse(T&& result, ui64 downloadedBytes);

public:
    EStatus Status;
    TString Error;
    T Result;
    ui64 DownloadedBytes = 0;
};

struct TGetLabelsResult {
    std::vector<TString> Labels;
};
using TGetLabelsResponse = TSolomonClientResponse<TGetLabelsResult>;

struct TListMetricsResult {
    std::vector<TMetric> Metrics;
    ui64 PagesCount;
    ui64 TotalCount;
};
using TListMetricsResponse = TSolomonClientResponse<TListMetricsResult>;

struct TGetPointsCountResult {
    ui64 PointsCount;
};
using TGetPointsCountResponse = TSolomonClientResponse<TGetPointsCountResult>;

struct TGetDataResult {
    std::vector<TTimeseries> Timeseries;
};
using TGetDataResponse = TSolomonClientResponse<TGetDataResult>;

} // namespace NYql::NSo
