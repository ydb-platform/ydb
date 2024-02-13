#pragma once

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NYdb {
namespace NS3Internal {

struct TS3ListingSettings : public TOperationRequestSettings<TS3ListingSettings> {};


class TS3ListingResult : public TStatus {
    friend class TS3InternalClient;
private:
    TS3ListingResult(TResultSet&& commonPrefixes, TResultSet&& contents, TStatus&& status);

public:
    const TResultSet& GetCommonPrefixes() const;
    const TResultSet& GetContents() const;

private:
    TResultSet CommonPrefixes;
    TResultSet Contents;
};

using TAsyncS3ListingResult = NThreading::TFuture<TS3ListingResult>;


class TS3InternalClient {
    class TImpl;

public:
    TS3InternalClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncS3ListingResult S3Listing(const TString& tableName,
                           TValue&& keyPrefix,
                           const TString& pathColumnPrefix,
                           const TString& pathColumnDelimiter,
                           TValue&& startAfterKeySuffix,
                           ui32 maxKeys,
                           const TVector<TString> &columnsToReturn,
                           const TS3ListingSettings& settings = TS3ListingSettings());

private:
    std::shared_ptr<TImpl> Impl_;
};

}}
