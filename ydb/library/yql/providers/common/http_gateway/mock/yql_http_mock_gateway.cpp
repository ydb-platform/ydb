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

    void Download(
            TString url,
            IHTTPGateway::THeaders headers,
            std::size_t expectedSize,
            IHTTPGateway::TOnResult callback,
            TString data,
            IRetryPolicy<long>::TPtr retryPolicy)
    {

        Y_UNUSED(expectedSize);
        Y_UNUSED(retryPolicy);

        auto key = TKeyType(url, headers, data);
        if (RequestsResponse.contains(key)) {
            for (auto response : RequestsResponse[key]) {
                callback(response());
            }
        } else if (DefaultResponse) {
            callback(DefaultResponse(url, headers, data));
        } else {
            YQL_ENSURE(false, "There isn't any response callback at url "  + url);
        }
    }

     virtual void Download(
            TString ,
            IHTTPGateway::THeaders ,
            std::size_t ,
            IHTTPGateway::TOnNewDataPart ,
            IHTTPGateway::TOnDowloadFinsh ) {
    }

    void AddDefaultResponse(TDataDefaultResponse response) {
        DefaultResponse = response;
    }

    void AddDownloadResponse(
            TString url,
            IHTTPGateway::THeaders headers,
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