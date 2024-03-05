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

TObjectStorageListingResult::TObjectStorageListingResult(std::vector<std::string>&& commonPrefixes, TResultSet&& contents, TStatus&& status)
    : TStatus(std::move(status))
    , CommonPrefixes(std::move(commonPrefixes))
    , Contents(std::move(contents))
{}

const std::vector<std::string>& TObjectStorageListingResult::GetCommonPrefixes() const {
    return CommonPrefixes;
}

const TResultSet& TObjectStorageListingResult::GetContents() const {
    return Contents;
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
                           TValue&& startAfterKeySuffix,
                           ui32 maxKeys,
                           const TVector<TString> columnsToReturn,
                           const TObjectStorageListingSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::ObjectStorage::ListingRequest>(settings);
        request.set_table_name(tableName);
        SetProtoValue(*request.mutable_key_prefix(), std::move(keyPrefix));
        request.set_path_column_prefix(pathColumnPrefix);
        request.set_path_column_delimiter(pathColumnDelimiter);
        SetProtoValue(*request.mutable_start_after_key_suffix(), std::move(startAfterKeySuffix));
        request.set_max_keys(maxKeys);
        for (auto& c : columnsToReturn) {
            request.add_columns_to_return(c);
        }

        auto promise = NThreading::NewPromise<TObjectStorageListingResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::ObjectStorage::ListingResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
                std::vector<std::string> commonPrefixes;
                for (auto commonPrefix : result.Getcommon_prefixes()) {
                    commonPrefixes.push_back(commonPrefix);
                }
                TResultSet contents(result.Getcontents());

                TObjectStorageListingResult val(std::move(commonPrefixes), std::move(contents), TStatus(std::move(status)));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::ObjectStorage::V1::ObjectStorageService, Ydb::ObjectStorage::ListingRequest, Ydb::ObjectStorage::ListingResponse>(
            std::move(request),
            extractor,
            &Ydb::ObjectStorage::V1::ObjectStorageService::Stub::AsyncList,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
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
                                          TValue&& startAfterKeySuffix,
                                          ui32 maxKeys,
                                          const TVector<TString>& columnsToReturn,
                                          const TObjectStorageListingSettings& settings)
{
    return Impl_->List(tableName,
                            std::move(keyPrefix),
                            pathColumnPrefix,
                            pathColumnDelimiter,
                            std::move(startAfterKeySuffix),
                            maxKeys,
                            columnsToReturn,
                            settings);
}

}}
