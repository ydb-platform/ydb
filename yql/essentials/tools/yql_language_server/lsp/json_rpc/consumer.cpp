#include "consumer.h"

#include "marshal.h"
#include "exception.h"

#include <yql/essentials/tools/yql_language_server/lsp/consumer/map.h>

namespace NLsp::NJsonRpc {

namespace {

template <typename T>
class TJsonRpcExceptionHandler final: public IConsumer<T> {
public:
    TJsonRpcExceptionHandler(TJsonRpcOutbox::TPtr out, IConsumer<T>::TPtr consumer)
        : Out_(std::move(out))
        , Consumer_(std::move(consumer))
    {
    }

    void Receive(T value) override {
        TMaybe<TJsonRpcMessageId> id;
        if constexpr (std::is_same_v<T, TJsonRpcRequest>) {
            id = value.Id;
        }

        try {
            Consumer_->Receive(std::move(value));
        } catch (const TJsonRpcException& e) {
            Out_->Receive({.Result = std::unexpected(e.ToProtocol()), .Id = std::move(id).GetOrElse({})});
        } catch (...) {
            auto e = TJsonRpcException::Unknown(std::current_exception());
            Out_->Receive({.Result = std::unexpected(e.ToProtocol()), .Id = std::move(id).GetOrElse({})});
        }
    }

    void Stop() override {
        Consumer_->Stop();
    }

private:
    TJsonRpcOutbox::TPtr Out_;
    IConsumer<T>::TPtr Consumer_;
};

} // namespace

IConsumer<TJsonRpcRequest>::TPtr JsonRpcExceptionHandling(
    TJsonRpcOutbox::TPtr out,
    IConsumer<TJsonRpcRequest>::TPtr consumer)
{
    return new TJsonRpcExceptionHandler<TJsonRpcRequest>(std::move(out), std::move(consumer));
}

IConsumer<TString>::TPtr JsonRpcExceptionHandling(
    TJsonRpcOutbox::TPtr out,
    IConsumer<TString>::TPtr consumer)
{
    return new TJsonRpcExceptionHandler<TString>(std::move(out), std::move(consumer));
}

IConsumer<TString>::TPtr JsonRpcMarshalling(IConsumer<TJsonRpcRequest>::TPtr consumer) {
    return Map<TString, TJsonRpcRequest>(UnMarshal, std::move(consumer));
}

IConsumer<TJsonRpcResponse>::TPtr JsonRpcMarshalling(IConsumer<TString>::TPtr consumer) {
    return Map<TJsonRpcResponse, TString>(Marshal, std::move(consumer));
}

} // namespace NLsp::NJsonRpc
