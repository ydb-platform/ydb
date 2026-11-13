#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_tablet.h>

#include <ydb/public/api/grpc/draft/ydb_tablet_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/src/client/operation/impl.h>

namespace NYdb::inline Dev::NTablet {

    TReassignTabletsOperation::TReassignTabletsOperation(TStatus&& status, Ydb::Operations::Operation&& operation)
        : TOperation(std::move(status), std::move(operation))
    {
        Ydb::Tablet::ReassignTabletsMetadata metadata;
        GetProto().metadata().UnpackTo(&metadata);
        Metadata_.SelectedTablets = metadata.selected_tablets();
        Metadata_.CompletedTablets = metadata.completed_tablets();
    }

    const TReassignTabletsOperation::TMetadata& TReassignTabletsOperation::Metadata() const {
        return Metadata_;
    }

    class TTabletClient::TImpl: public TClientImplCommon<TTabletClient::TImpl> {
    public:
        TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
            : TClientImplCommon(std::move(connections), settings)
        {
        }

        TAsyncReassignTabletsOperation ReassignTablets(const TReassignTabletsSettings& settings) {
            auto request = MakeRequest<Ydb::Tablet::ReassignTabletsRequest>();
            request.set_storage_pool(TStringType{settings.StoragePool_});
            request.set_group(settings.Group_);
            if (settings.MaxInFlight_) {
                request.set_max_in_flight(*settings.MaxInFlight_);
            }
            if (settings.Percent_) {
                request.set_percent(*settings.Percent_);
            }
            request.set_async(settings.Async_);

            auto promise = NThreading::NewPromise<TReassignTabletsOperation>();
            auto extractor = [promise](Ydb::Operations::Operation* operation, TPlainStatus status) mutable {
                if (operation) {
                    promise.SetValue(TReassignTabletsOperation(TStatus(std::move(status)), std::move(*operation)));
                } else {
                    promise.SetValue(TReassignTabletsOperation(TStatus(std::move(status))));
                }
            };
            Connections_->RunDeferred<Ydb::Tablet::V1::TabletService,
                                      Ydb::Tablet::ReassignTabletsRequest, Ydb::Tablet::ReassignTabletsResponse>(
                std::move(request), std::move(extractor),
                &Ydb::Tablet::V1::TabletService::Stub::AsyncReassignTablets,
                DbDriverState_, INITIAL_DEFERRED_CALL_DELAY, TRpcRequestSettings::Make(settings));
            return promise.GetFuture();
        }
    };

    TTabletClient::TTabletClient(const TDriver& driver, const TCommonClientSettings& settings)
        : Impl_(std::make_shared<TImpl>(CreateInternalInterface(driver), settings))
    {
    }

    TAsyncReassignTabletsOperation TTabletClient::ReassignTablets(const TReassignTabletsSettings& settings) {
        return Impl_->ReassignTablets(settings);
    }

} // namespace NYdb::inline Dev::NTablet

namespace NYdb::inline Dev::NOperation {

    template NThreading::TFuture<NTablet::TReassignTabletsOperation> TOperationClient::Get(const TOperation::TOperationId& id);

} // namespace NYdb::inline Dev::NOperation
