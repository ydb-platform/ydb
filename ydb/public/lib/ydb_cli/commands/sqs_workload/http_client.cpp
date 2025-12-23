#include "http_client.h"
#include "consts.h"

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/DateTime.h>

namespace NYdb::NConsoleClient {

    namespace {

        const TString LATENCY_METRIC = "RequestLatency";

    } // namespace

    TMeasuringHttpClient::TMeasuringHttpClient(
        const Aws::Client::ClientConfiguration& cfg)
        : Inner(Aws::MakeShared<Aws::Http::CurlHttpClient>("InnerCurl", cfg))
    {
    }

    std::shared_ptr<Aws::Http::HttpResponse> TMeasuringHttpClient::MakeRequest(
        const std::shared_ptr<Aws::Http::HttpRequest>& request,
        Aws::Utils::RateLimits::RateLimiterInterface* readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter) const {
        auto resp = Inner->MakeRequest(request, readLimiter, writeLimiter);

        ui64 latency = 0;
        for (const auto& [key, value] : request->GetRequestMetrics()) {
            if (key == LATENCY_METRIC) {
                latency = value;
                break;
            }
        }

        if (StatsCollector) {
            auto action = request->GetHeaderValue(AMZ_TARGET_HEADER);
            if (action == SQS_TARGET_RECEIVE_MESSAGE) {
                StatsCollector->AddReceiveRequestDoneEvent(
                    TSqsWorkloadStats::ReceiveRequestDoneEvent{latency});
            } else if (action == SQS_TARGET_SEND_MESSAGE_BATCH) {
                StatsCollector->AddSendRequestDoneEvent(
                    TSqsWorkloadStats::SendRequestDoneEvent{latency});
            } else if (action == SQS_TARGET_DELETE_MESSAGE_BATCH) {
                StatsCollector->AddDeleteRequestDoneEvent(
                    TSqsWorkloadStats::DeleteRequestDoneEvent{latency});
            }
        }

        return resp;
    }

    void TMeasuringHttpClient::SetStatsCollector(
        std::shared_ptr<TSqsWorkloadStatsCollector> statsCollector)
    {
        StatsCollector = statsCollector;
    }

    TMeasuringHttpClientFactory::TMeasuringHttpClientFactory(
        std::shared_ptr<TSqsWorkloadStatsCollector> statsCollector)
        : StatsCollector(statsCollector)
    {
    }

    std::shared_ptr<Aws::Http::HttpClient>
    TMeasuringHttpClientFactory::CreateHttpClient(
        const Aws::Client::ClientConfiguration& cfg) const {
        auto httpClient =
            Aws::MakeShared<TMeasuringHttpClient>("MeasureClient", cfg);
        httpClient->SetStatsCollector(StatsCollector);
        return httpClient;
    }

    std::shared_ptr<Aws::Http::HttpRequest>
    TMeasuringHttpClientFactory::CreateHttpRequest(
        const Aws::String& uri, Aws::Http::HttpMethod method,
        const Aws::IOStreamFactory& streamFactory) const {
        return CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
    }

    std::shared_ptr<Aws::Http::HttpRequest>
    TMeasuringHttpClientFactory::CreateHttpRequest(
        const Aws::Http::URI& uri, Aws::Http::HttpMethod method,
        const Aws::IOStreamFactory& streamFactory) const {
        auto req = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>(
            "MeasureClientReq", uri, method);
        req->SetResponseStreamFactory(streamFactory);
        return req;
    }

    void InitMeasuringHttpClient(
        std::shared_ptr<TSqsWorkloadStatsCollector> statsCollector)
    {
        Aws::Http::CleanupHttp();
        Aws::Http::SetHttpClientFactory(
            Aws::MakeShared<TMeasuringHttpClientFactory>("MeasureFactory",
                                                         statsCollector));
        Aws::Http::InitHttp();
    }

} // namespace NYdb::NConsoleClient
