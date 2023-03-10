#pragma once

#include <ydb/library/yql/providers/s3/object_listers/yql_s3_list.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <library/cpp/threading/future/future.h>

#include <memory>
#include <variant>
#include <vector>

namespace NYql {

class IS3ListerFactory {
public:
    using TPtr = std::shared_ptr<IS3ListerFactory>;

    virtual NThreading::TFuture<NS3Lister::IS3Lister::TPtr> Make(
        const IHTTPGateway::TPtr& httpGateway,
        const NS3Lister::TListingRequest& listingRequest,
        const TMaybe<TString>& delimiter,
        bool allowLocalFiles) = 0;

    virtual ~IS3ListerFactory() = default;
};

IS3ListerFactory::TPtr MakeS3ListerFactory(size_t maxParallelOps);

enum class ES3ListingOptions : ui8 {
    NoOptions = 0,
    UnPartitionedDataset = 1,
    PartitionedDataset = 2
};

IOutputStream& operator<<(IOutputStream& stream, ES3ListingOptions option);

class IS3ListingStrategy {
public:
    using TPtr = std::shared_ptr<IS3ListingStrategy>;

    virtual NThreading::TFuture<NS3Lister::TListResult> List(
        const NS3Lister::TListingRequest& listingRequest, ES3ListingOptions options) = 0;

    virtual ~IS3ListingStrategy() = default;
};

IS3ListingStrategy::TPtr MakeS3ListingStrategy(
    const IHTTPGateway::TPtr& httpGateway,
    const IS3ListerFactory::TPtr& listerFactory,
    ui64 maxFilesPerQueryFiles,
    ui64 maxFilesPerQueryDirectory,
    ui64 minDesiredDirectoriesOfFilesPerQuery,
    bool allowLocalFiles);

} // namespace NYql
