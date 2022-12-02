#include "ydb_s3_internal.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/draft/ydb_s3_internal_v1.grpc.pb.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

namespace NYdb {
namespace NS3Internal {

TS3ListingResult::TS3ListingResult(TResultSet&& commonPrefixes, TResultSet&& contents, ui32 keySuffixSize, TStatus&& status)
    : TStatus(std::move(status))
    , CommonPrefixes(std::move(commonPrefixes))
    , Contents(std::move(contents))
    , KeySuffixSize(keySuffixSize)
{}

const TResultSet& TS3ListingResult::GetCommonPrefixes() const {
    return CommonPrefixes;
}

const TResultSet& TS3ListingResult::GetContents() const {
    return Contents;
}

ui32 TS3ListingResult::GetKeySuffixSize() const {
    return KeySuffixSize;
}

void SetProtoValue(Ydb::TypedValue& out, TValue&& in) {
    *out.mutable_type() = TProtoAccessor::GetProto(in.GetType());
    *out.mutable_value() = TProtoAccessor::GetProto(in);
}


class TS3InternalClient::TImpl : public TClientImplCommon<TS3InternalClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings) {}

    TAsyncS3ListingResult S3Listing(const TString& tableName,
                           TValue&& keyPrefix,
                           const TString& pathColumnPrefix,
                           const TString& pathColumnDelimiter,
                           TValue&& startAfterKeySuffix,
                           ui32 maxKeys,
                           const TVector<TString> columnsToReturn,
                           const TS3ListingSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::S3Internal::S3ListingRequest>(settings);
        request.set_table_name(tableName);
        SetProtoValue(*request.mutable_key_prefix(), std::move(keyPrefix));
        request.set_path_column_prefix(pathColumnPrefix);
        request.set_path_column_delimiter(pathColumnDelimiter);
        SetProtoValue(*request.mutable_start_after_key_suffix(), std::move(startAfterKeySuffix));
        request.set_max_keys(maxKeys);
        for (auto& c : columnsToReturn) {
            request.add_columns_to_return(c);
        }

        auto promise = NThreading::NewPromise<TS3ListingResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::S3Internal::S3ListingResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
                TResultSet commonPrefixes(result.Getcommon_prefixes());
                TResultSet contents(result.Getcontents());

                TS3ListingResult val(std::move(commonPrefixes), std::move(contents), result.Getkey_suffix_size(),
                    TStatus(std::move(status)));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::S3Internal::V1::S3InternalService, Ydb::S3Internal::S3ListingRequest, Ydb::S3Internal::S3ListingResponse>(
            std::move(request),
            extractor,
            &Ydb::S3Internal::V1::S3InternalService::Stub::AsyncS3Listing,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }
};


TS3InternalClient::TS3InternalClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TAsyncS3ListingResult TS3InternalClient::S3Listing(const TString& tableName,
                                          TValue&& keyPrefix,
                                          const TString& pathColumnPrefix,
                                          const TString& pathColumnDelimiter,
                                          TValue&& startAfterKeySuffix,
                                          ui32 maxKeys,
                                          const TVector<TString>& columnsToReturn,
                                          const TS3ListingSettings& settings)
{
    return Impl_->S3Listing(tableName,
                            std::move(keyPrefix),
                            pathColumnPrefix,
                            pathColumnDelimiter,
                            std::move(startAfterKeySuffix),
                            maxKeys,
                            columnsToReturn,
                            settings);
}

}}

