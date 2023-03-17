#include "yql_http_mock_gateway.h"

#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {
namespace {

using TKeyType = std::tuple<TString, IHTTPGateway::THeaders, TString>;

class TKeyHash {
public:
    TKeyHash() : Hash() {}

    size_t operator()(const TKeyType& key) const {
        const auto& headers = std::get<1U>(key);
        auto initHash = CombineHashes(Hash(std::get<0U>(key)), Hash(std::get<2U>(key)));
        return std::accumulate(headers.cbegin(), headers.cend(), initHash,
                               [this](size_t hash, const TString& item) { return CombineHashes(hash, Hash(item)); });
    }
public:
    const std::hash<TString> Hash;
};

class THTTPMockGateway : public IHTTPMockGateway {
friend class IHTTPMockGateway;
public:
    using TPtr = std::shared_ptr<THTTPMockGateway>;

    THTTPMockGateway() {
    }

    ~THTTPMockGateway() {
    }

    static TString PrintKey(const TKeyType& key) {
        TStringBuilder ret;
        ret << "{ Url: \"" << std::get<0>(key) << "\"";
        ret << " Headers: [";
        for (const TString& header : std::get<1>(key)) {
            ret << " \"" << header << "\"";
        }
        ret << " ] Data: \"" << std::get<2>(key) << "\" }";
        return std::move(ret);
    }

    void Upload(TString, THeaders, TString, TOnResult, bool, IRetryPolicy<long>::TPtr) {}

    void Delete(TString, THeaders, TOnResult, IRetryPolicy<long>::TPtr) {}

    void Download(
            TString url,
            THeaders headers,
            std::size_t offset,
            std::size_t sizeLimit,
            TOnResult callback,
            TString data,
            IRetryPolicy<long>::TPtr retryPolicy)
    {

        Y_UNUSED(sizeLimit);
        Y_UNUSED(offset);
        Y_UNUSED(retryPolicy);

        auto key = TKeyType(url, headers, data);
        if (RequestsResponse.contains(key)) {
            for (auto response : RequestsResponse[key]) {
                callback(response());
            }
        } else if (DefaultResponse) {
            callback(DefaultResponse(url, headers, data));
        } else {
            YQL_ENSURE(false, "There isn't any response callback for " + PrintKey(key));
        }
    }

    TCancelHook Download(
            TString,
            THeaders,
            std::size_t,
            std::size_t,
            TOnDownloadStart,
            TOnNewDataPart,
            TOnDownloadFinish,
            const ::NMonitoring::TDynamicCounters::TCounterPtr&) final {
        return {};
    }

    ui64 GetBuffersSizePerStream() final {
        return 0;
    }

    void AddDefaultResponse(TDataDefaultResponse response) {
        DefaultResponse = response;
    }

    void AddDownloadResponse(
            TString url,
            THeaders headers,
            TString data,
            TDataResponse response) {

        auto& entry = RequestsResponse[TKeyType(url, headers, data)];
        entry.emplace_back(std::move(response));
    }

private:
    std::unordered_map<TKeyType, std::vector<TDataResponse>, TKeyHash> RequestsResponse;
    TDataDefaultResponse DefaultResponse;
};
}


IHTTPMockGateway::TPtr IHTTPMockGateway::Make() {
    return std::make_shared<THTTPMockGateway>();
}

}
