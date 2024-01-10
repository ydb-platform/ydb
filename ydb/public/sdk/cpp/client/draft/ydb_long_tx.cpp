#include "ydb_long_tx.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

namespace NYdb {
namespace NLongTx {

namespace {

struct TOpSettings : public TOperationRequestSettings<TOpSettings> {
};

}

class TClient::TImpl: public TClientImplCommon<TClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {}

    TAsyncBeginTxResult BeginTx(Ydb::LongTx::BeginTransactionRequest::TxTypeId txType,
                                const TOpSettings& settings = TOpSettings()) {
        auto request = MakeOperationRequest<Ydb::LongTx::BeginTransactionRequest>(settings);
        request.set_tx_type(txType);

        return RunOperation<Ydb::LongTx::V1::LongTxService,
                            Ydb::LongTx::BeginTransactionRequest, Ydb::LongTx::BeginTransactionResponse, TLongTxBeginResult>(
            std::move(request),
            &Ydb::LongTx::V1::LongTxService::Stub::AsyncBeginTx,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncCommitTxResult CommitTx(const TString& txId,
                                  const TOpSettings& settings = TOpSettings()) {
        auto request = MakeOperationRequest<Ydb::LongTx::CommitTransactionRequest>(settings);
        request.set_tx_id(txId);

        return RunOperation<Ydb::LongTx::V1::LongTxService,
                            Ydb::LongTx::CommitTransactionRequest, Ydb::LongTx::CommitTransactionResponse, TLongTxCommitResult>(
            std::move(request),
            &Ydb::LongTx::V1::LongTxService::Stub::AsyncCommitTx,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncRollbackTxResult RollbackTx(const TString& txId,
                                      const TOpSettings& settings = TOpSettings()) {
        auto request = MakeOperationRequest<Ydb::LongTx::RollbackTransactionRequest>(settings);
        request.set_tx_id(txId);

        return RunOperation<Ydb::LongTx::V1::LongTxService,
                            Ydb::LongTx::RollbackTransactionRequest, Ydb::LongTx::RollbackTransactionResponse, TLongTxRollbackResult>(
            std::move(request),
            &Ydb::LongTx::V1::LongTxService::Stub::AsyncRollbackTx,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncWriteResult Write(const TString& txId, const TString& table, const TString& dedupId,
                            const TString& data, Ydb::LongTx::Data::Format format,
                            const TOpSettings& settings = TOpSettings()) {
        auto request = MakeOperationRequest<Ydb::LongTx::WriteRequest>(settings);
        request.set_tx_id(txId);
        request.set_path(table);
        request.set_dedup_id(dedupId);

        auto req_data = request.mutable_data();
        req_data->set_format(format);
        req_data->set_data(data);

        return RunOperation<Ydb::LongTx::V1::LongTxService,
                            Ydb::LongTx::WriteRequest, Ydb::LongTx::WriteResponse, TLongTxWriteResult>(
            std::move(request),
            &Ydb::LongTx::V1::LongTxService::Stub::AsyncWrite,
            TRpcRequestSettings::Make(settings));
    }

};

TClient::TClient(const TDriver& driver, const TClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TClient::TAsyncBeginTxResult TClient::BeginWriteTx() {
    return Impl_->BeginTx(Ydb::LongTx::BeginTransactionRequest::WRITE);
}

TClient::TAsyncCommitTxResult TClient::CommitTx(const TString& txId) {
    return Impl_->CommitTx(txId);
}

TClient::TAsyncRollbackTxResult TClient::RollbackTx(const TString& txId) {
    return Impl_->RollbackTx(txId);
}

TClient::TAsyncWriteResult TClient::Write(const TString& txId, const TString& table, const TString& dedupId,
                                          const TString& data, Ydb::LongTx::Data::Format format) {
    return Impl_->Write(txId, table, dedupId, data, format);
}


} // namespace NLongTx
} // namespace NYdb
