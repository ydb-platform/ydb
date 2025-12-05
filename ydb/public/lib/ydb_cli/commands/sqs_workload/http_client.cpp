#include "http_client.h"
#include "consts.h"

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/DateTime.h>

namespace NYdb::NConsoleClient {

TMeasuringHttpClient::TMeasuringHttpClient(const Aws::Client::ClientConfiguration& cfg)
    : Inner(Aws::MakeShared<Aws::Http::CurlHttpClient>("InnerCurl", cfg))
{}

std::shared_ptr<Aws::Http::HttpResponse> TMeasuringHttpClient::MakeRequest(
    const std::shared_ptr<Aws::Http::HttpRequest>& request,
    Aws::Utils::RateLimits::RateLimiterInterface* readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter) const
{
    auto start = TInstant::Now();
    auto resp = Inner->MakeRequest(request, readLimiter, writeLimiter);
    auto end = TInstant::Now();
    auto ms = (end - start).MilliSeconds();

    auto action = request->GetHeaderValue(kSQSWorkloadActionHeader);
    if (action == kSQSWorkloadActionReceive) {
        StatsCollector->AddReceiveRequestDoneEvent(TSqsWorkloadStats::ReceiveRequestDoneEvent{ms});
    } else if (action == kSQSWorkloadActionSend) {
        StatsCollector->AddSendRequestDoneEvent(TSqsWorkloadStats::SendRequestDoneEvent{ms});
    } else if (action == kSQSWorkloadActionDelete) {
        StatsCollector->AddDeleteRequestDoneEvent(TSqsWorkloadStats::DeleteRequestDoneEvent{ms});
    }

    return resp;
}

void TMeasuringHttpClient::SetStatsCollector(std::shared_ptr<TSqsWorkloadStatsCollector> statsCollector)
{
    StatsCollector = statsCollector;
}

TMeasuringHttpClientFactory::TMeasuringHttpClientFactory(std::shared_ptr<TSqsWorkloadStatsCollector> statsCollector)
    : StatsCollector(statsCollector)
{}

std::shared_ptr<Aws::Http::HttpClient> TMeasuringHttpClientFactory::CreateHttpClient(
    const Aws::Client::ClientConfiguration& cfg) const
{
    auto httpClient = Aws::MakeShared<TMeasuringHttpClient>("MeasureClient", cfg);
    httpClient->SetStatsCollector(StatsCollector);
    return httpClient;
}

std::shared_ptr<Aws::Http::HttpRequest> TMeasuringHttpClientFactory::CreateHttpRequest(
    const Aws::String& uri,
    Aws::Http::HttpMethod method,
    const Aws::IOStreamFactory& streamFactory) const
{
    return CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
}

std::shared_ptr<Aws::Http::HttpRequest> TMeasuringHttpClientFactory::CreateHttpRequest(
    const Aws::Http::URI& uri,
    Aws::Http::HttpMethod method,
    const Aws::IOStreamFactory& streamFactory) const
{
    auto req = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>(
        "MeasureClientReq", uri, method);
    req->SetResponseStreamFactory(streamFactory);
    return req;
}

void InitMeasuringHttpClient(std::shared_ptr<TSqsWorkloadStatsCollector> statsCollector)
{
    Aws::Http::CleanupHttp();
    Aws::Http::SetHttpClientFactory(
        Aws::MakeShared<TMeasuringHttpClientFactory>("MeasureFactory", statsCollector));
    Aws::Http::InitHttp();
}

void DestroyMeasuringHttpClient()
{
    Aws::Http::CleanupHttp();
}

} // namespace NYdb::NConsoleClient


