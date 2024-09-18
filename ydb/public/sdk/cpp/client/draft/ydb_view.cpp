#include "ydb_view.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/draft/ydb_view_v1.grpc.pb.h>
#include <ydb/public/api/protos/draft/ydb_view.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NYdb {
namespace NView {

TViewDescription::TViewDescription(const Ydb::View::DescribeViewResult& desc)
    : QueryText_(desc.query_text())
{
}

const TString& TViewDescription::GetQueryText() const {
    return QueryText_;
}

TDescribeViewResult::TDescribeViewResult(TStatus&& status, Ydb::View::DescribeViewResult&& desc)
    : NScheme::TDescribePathResult(std::move(status), desc.self())
    , Proto_(std::make_unique<Ydb::View::DescribeViewResult>(std::move(desc)))
{
}

TViewDescription TDescribeViewResult::GetViewDescription() const {
    return TViewDescription(*Proto_);
}

const Ydb::View::DescribeViewResult& TDescribeViewResult::GetProto() const {
    return *Proto_;
}

class TViewClient::TImpl : public TClientImplCommon<TViewClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    TAsyncDescribeViewResult DescribeView(const TString& path, const TDescribeViewSettings& settings) {
        using namespace Ydb::View;

        auto request = MakeOperationRequest<DescribeViewRequest>(settings);
        request.set_path(path);

        auto promise = NThreading::NewPromise<TDescribeViewResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                DescribeViewResult result;
                if (any) {
                    any->UnpackTo(&result);
                }

                TDescribeViewResult val(TStatus(std::move(status)), std::move(result));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<V1::ViewService, DescribeViewRequest, DescribeViewResponse>(
            std::move(request),
            extractor,
            &V1::ViewService::Stub::AsyncDescribeView,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

};

TViewClient::TViewClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(std::make_shared<TImpl>(CreateInternalInterface(driver), settings))
{
}

TAsyncDescribeViewResult TViewClient::DescribeView(const TString& path, const TDescribeViewSettings& settings) {
    return Impl_->DescribeView(path, settings);
}

} // NView

const Ydb::View::DescribeViewResult& TProtoAccessor::GetProto(const NView::TDescribeViewResult& result) {
    return result.GetProto();
}

} // NYdb
