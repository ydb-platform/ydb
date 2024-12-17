#pragma once

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NYdb {
namespace NObjectStorage {

struct TObjectStorageListingSettings : public TOperationRequestSettings<TObjectStorageListingSettings> {};


class TObjectStorageListingResult : public TStatus {
    friend class TObjectStorageClient;
private:
    TObjectStorageListingResult(std::vector<std::string>&& commonPrefixes, TResultSet&& contents, TString nextContinuationToken, bool isTruncated, TStatus&& status);

public:
    const std::vector<std::string>& GetCommonPrefixes() const;
    const TResultSet& GetContents() const;
    const TString& GetContinuationToken() const;
    bool GetIsTruncated() const;

private:
    std::vector<std::string> CommonPrefixes;
    TResultSet Contents;
    TString NextContinuationToken;
    bool IsTruncated;
};

using TAsyncObjectStorageListingResult = NThreading::TFuture<TObjectStorageListingResult>;


class TObjectStorageClient {
    class TImpl;

public:
    TObjectStorageClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncObjectStorageListingResult List(const TString& tableName,
                           TValue&& keyPrefix,
                           const TString& pathColumnPrefix,
                           const TString& pathColumnDelimiter,
                           TString continuationToken,
                           TValue&& startAfterKeySuffix,
                           ui32 maxKeys,
                           const TVector<TString> &columnsToReturn,
                           const TObjectStorageListingSettings& settings = TObjectStorageListingSettings());

private:
    std::shared_ptr<TImpl> Impl_;
};

}}
