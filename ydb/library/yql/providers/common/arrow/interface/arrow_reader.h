#pragma once

#include <library/cpp/threading/future/future.h>
#include <util/generic/string.h>
#include <arrow/api.h>
#include <vector>
#include <memory>

#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

namespace NYql {

struct TArrowFileCookie;

class TArrowFileDesc {
public:
    TArrowFileDesc(
        const TString& url, 
        IHTTPGateway::TPtr gateway,
        IHTTPGateway::THeaders headers,
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        size_t size,
        const TString& format = ""
    );

    TString Url;
    IHTTPGateway::TPtr Gateway;
    IHTTPGateway::THeaders Headers;
    IHTTPGateway::TRetryPolicy::TPtr RetryPolicy;
    TString Format;
    size_t Size;
    bool IsLocal;
    TMaybe<TString> Contents;
    std::shared_ptr<TArrowFileCookie> Cookie;
};

class TArrowReaderSettings {
public:
    explicit TArrowReaderSettings(size_t poolSize = 10);
    size_t PoolSize;
};

class IArrowReader {
public:
    using TPtr = std::shared_ptr<IArrowReader>;

    class TSchemaResponse {
    public:
        TSchemaResponse(std::shared_ptr<arrow::Schema> schema, int numRowGroups, std::shared_ptr<TArrowFileCookie> cookie);
        std::shared_ptr<arrow::Schema> Schema;
        int NumRowGroups;
        std::shared_ptr<TArrowFileCookie> Cookie;
    };

    virtual NThreading::TFuture<TSchemaResponse> GetSchema(const TArrowFileDesc& desc) const = 0;
    virtual NThreading::TFuture<std::shared_ptr<arrow::Table>> ReadRowGroup(
        const TArrowFileDesc& desc,
        int rowGroupIndex,
        const std::vector<int>& columnIndices
    ) const = 0;
    virtual ~IArrowReader() = default;    
};

IArrowReader::TPtr MakeArrowReader(const TArrowReaderSettings& settings);

}
