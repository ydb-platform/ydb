#pragma once

#include <ydb/library/yql/providers/s3/object_listers/yql_s3_list.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <library/cpp/threading/future/future.h>

#include <memory>
#include <variant>
#include <vector>

namespace NYql {

struct TS3ListingOptions {
    bool IsPartitionedDataset = false;
    bool IsConcurrentListing = false;
    ui64 MaxResultSet = std::numeric_limits<ui64>::max();
};

IOutputStream& operator<<(IOutputStream& stream, const TS3ListingOptions& options);

class IS3ListingStrategy {
public:
    using TPtr = std::shared_ptr<IS3ListingStrategy>;

    virtual NThreading::TFuture<NS3Lister::TListResult> List(
        const NS3Lister::TListingRequest& listingRequest, const TS3ListingOptions& options) = 0;

    virtual ~IS3ListingStrategy() = default;
};

IS3ListingStrategy::TPtr MakeS3ListingStrategy(
    const IHTTPGateway::TPtr& httpGateway,
    const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
    const NS3Lister::IS3ListerFactory::TPtr& listerFactory,
    ui64 minDesiredDirectoriesOfFilesPerQuery,
    size_t maxParallelOps,
    bool allowLocalFiles);

} // namespace NYql
