#include "arrow_reader.h"

namespace NYql {
TArrowFileDesc::TArrowFileDesc(const TString& url, IHTTPGateway::TPtr gateway, IHTTPGateway::THeaders headers, const IRetryPolicy<long>::TPtr& retryPolicy, size_t size, const TString& format)
                                : Url(url), Gateway(gateway), Headers(headers), RetryPolicy(retryPolicy), Format(format), Size(size), IsLocal(url.StartsWith("file://"))
{

}

IArrowReader::TSchemaResponse::TSchemaResponse(std::shared_ptr<arrow::Schema> schema, int numRowGroups) : Schema(schema), NumRowGroups(numRowGroups)
{

}

TArrowReaderSettings::TArrowReaderSettings(size_t poolSize) : PoolSize(poolSize)
{

}

}
