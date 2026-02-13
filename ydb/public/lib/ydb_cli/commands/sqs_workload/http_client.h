#pragma once

#include "sqs_workload_stats_collector.h"

#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/curl/CurlHttpClient.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/utils/json/JsonSerializer.h>

namespace NYdb::NConsoleClient {

    class TMeasuringHttpClient: public Aws::Http::HttpClient {
    public:
        explicit TMeasuringHttpClient(const Aws::Client::ClientConfiguration& cfg);

        std::shared_ptr<Aws::Http::HttpResponse> MakeRequest(
            const std::shared_ptr<Aws::Http::HttpRequest>& request,
            Aws::Utils::RateLimits::RateLimiterInterface* readLimiter = nullptr,
            Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter =
                nullptr) const override;

        void SetStatsCollector(
            std::shared_ptr<TSqsWorkloadStatsCollector> statsCollector);

    private:
        std::shared_ptr<Aws::Http::HttpClient> Inner;
        std::shared_ptr<TSqsWorkloadStatsCollector> StatsCollector = nullptr;
    };

    class TMeasuringHttpClientFactory: public Aws::Http::HttpClientFactory {
    public:
        explicit TMeasuringHttpClientFactory(
            std::shared_ptr<TSqsWorkloadStatsCollector> statsCollector);

        std::shared_ptr<Aws::Http::HttpClient> CreateHttpClient(
            const Aws::Client::ClientConfiguration& cfg) const override;

        std::shared_ptr<Aws::Http::HttpRequest>
        CreateHttpRequest(const Aws::String& uri, Aws::Http::HttpMethod method,
                          const Aws::IOStreamFactory& streamFactory) const override;

        std::shared_ptr<Aws::Http::HttpRequest>
        CreateHttpRequest(const Aws::Http::URI& uri, Aws::Http::HttpMethod method,
                          const Aws::IOStreamFactory& streamFactory) const override;

    private:
        std::shared_ptr<TSqsWorkloadStatsCollector> StatsCollector;
    };

    void InitMeasuringHttpClient(
        std::shared_ptr<TSqsWorkloadStatsCollector> statsCollector);

} // namespace NYdb::NConsoleClient
