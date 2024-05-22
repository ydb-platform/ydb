#include "ydb_object_storage.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/draft/ydb_object_storage_v1.grpc.pb.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

namespace NYdb {
namespace NObjectStorage {

TObjectStorageListingResult::TObjectStorageListingResult(std::vector<std::string>&& commonPrefixes, TResultSet&& contents, TString nextContinuationToken, bool isTruncated, TStatus&& status)
    : TStatus(std::move(status))
    , CommonPrefixes(std::move(commonPrefixes))
    , Contents(std::move(contents))
    , NextContinuationToken(nextContinuationToken)
    , IsTruncated(isTruncated)
{}

const std::vector<std::string>& TObjectStorageListingResult::GetCommonPrefixes() const {
    return CommonPrefixes;
}

const TResultSet& TObjectStorageListingResult::GetContents() const {
    return Contents;
}

const TString& TObjectStorageListingResult::GetContinuationToken() const {
    return NextContinuationToken;
}

bool TObjectStorageListingResult::GetIsTruncated() const {
    return IsTruncated;
}

void SetProtoValue(Ydb::TypedValue& out, TValue&& in) {
    *out.mutable_type() = TProtoAccessor::GetProto(in.GetType());
    *out.mutable_value() = TProtoAccessor::GetProto(in);
}


class TObjectStorageClient::TImpl : public TClientImplCommon<TObjectStorageClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings) {}

    TAsyncObjectStorageListingResult List(const TString& tableName,
                           TValue&& keyPrefix,
                           const TString& pathColumnPrefix,
                           const TString& pathColumnDelimiter,
                           TString& continuationToken,
                           TValue&& startAfterKeySuffix,
                           ui32 maxKeys,
                           const TVector<TString> columnsToReturn,
                           const TObjectStorageListingSettings& settings)
    {
        auto request = MakeRequest<Ydb::ObjectStorage::ListingRequest>();
        request.set_table_name(tableName);
        SetProtoValue(*request.mutable_key_prefix(), std::move(keyPrefix));
        request.set_path_column_prefix(pathColumnPrefix);
        request.set_path_column_delimiter(pathColumnDelimiter);
        if (continuationToken) {
            request.Setcontinuation_token(continuationToken);
        }
        SetProtoValue(*request.mutable_start_after_key_suffix(), std::move(startAfterKeySuffix));
        request.set_max_keys(maxKeys);
        for (auto& c : columnsToReturn) {
            request.add_columns_to_return(c);
        }

        auto promise = NThreading::NewPromise<TObjectStorageListingResult>();

        auto extractor = [promise]
            (Ydb::ObjectStorage::ListingResponse* response, TPlainStatus status) mutable {
                std::vector<std::string> commonPrefixes;
                Ydb::ResultSet contents;
                if (response) {
                    Ydb::StatusIds::StatusCode msgStatus = response->status();
                    NYql::TIssues issues;
                    NYql::IssuesFromMessage(response->issues(), issues);
                    status = TPlainStatus(static_cast<EStatus>(msgStatus), std::move(issues));    
                    
                    for (auto commonPrefix : response->Getcommon_prefixes()) {
                        commonPrefixes.push_back(commonPrefix);
                    }
                    contents = std::move(response->Getcontents());
                }

                TObjectStorageListingResult val(std::move(commonPrefixes), std::move(contents), response->next_continuation_token(), response->is_truncated(), TStatus(std::move(status)));
                promise.SetValue(std::move(val));
            };

        Connections_->Run<Ydb::ObjectStorage::V1::ObjectStorageService, Ydb::ObjectStorage::ListingRequest, Ydb::ObjectStorage::ListingResponse>(
            std::move(request),
            extractor,
            &Ydb::ObjectStorage::V1::ObjectStorageService::Stub::AsyncList,
            DbDriverState_,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }
};


TObjectStorageClient::TObjectStorageClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TAsyncObjectStorageListingResult TObjectStorageClient::List(const TString& tableName,
                                          TValue&& keyPrefix,
                                          const TString& pathColumnPrefix,
                                          const TString& pathColumnDelimiter,
                                          TString continuationToken,
                                          TValue&& startAfterKeySuffix,
                                          ui32 maxKeys,
                                          const TVector<TString>& columnsToReturn,
                                          const TObjectStorageListingSettings& settings)
{
    return Impl_->List(tableName,
                            std::move(keyPrefix),
                            pathColumnPrefix,
                            pathColumnDelimiter,
                            continuationToken,
                            std::move(startAfterKeySuffix),
                            maxKeys,
                            columnsToReturn,
                            settings);
}

}}
