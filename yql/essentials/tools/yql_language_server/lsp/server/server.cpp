#include "server.h"

#include "base_protocol.h"
#include "parallel.h"

#include <yql/essentials/tools/yql_language_server/lsp/consumer/blocking_queue.h>
#include <yql/essentials/tools/yql_language_server/lsp/consumer/println.h>
#include <yql/essentials/tools/yql_language_server/lsp/json_rpc/consumer.h>

#include <library/cpp/threading/blocking_queue/blocking_queue.h>

#include <util/thread/pool.h>
#include <util/generic/scope.h>

namespace NLsp {

namespace {

void StartReader(IInputStream& in, IOutputStream& mout, IConsumer<TString>::TPtr lout) {
    Y_DEFER {
        lout->Stop();
    };

    lout = LinePrinting(mout, std::move(lout));
    LspBaseProtocolReader(in, lout);
}

void StartWriter(TBlockingQueuePtr<TString> outbox, IOutputStream& cout, IOutputStream& mout) {
    auto lout = LspBaseProtocolWriter(cout);
    lout = LinePrinting(mout, std::move(lout));

    while (TMaybe<TString> x = outbox->Pop()) {
        lout->Receive(std::move(*x));
    }
}

} // namespace

void LspServe(
    IInputStream& cin,
    IOutputStream& cout,
    IOutputStream& mout,
    TLspServerOptions options,
    TLspListenerFactory factory)
{
    auto pool = CreateThreadPool(options.Threads + 1);
    auto outbox = std::make_shared<TBlockingQueue<TString>>(/*maxSize=*/2 * options.Threads);

    pool->SafeAddFunc([outbox, &cout, &mout]() mutable {
        StartWriter(std::move(outbox), cout, mout);
    });

    auto outs = Consumer(outbox);
    auto out = NJsonRpc::JsonRpcMarshalling(std::move(outs));

    auto listener = factory(out);
    listener = NJsonRpc::JsonRpcExceptionHandling(out, std::move(listener));
    listener = Parallel(std::move(pool), std::move(listener));

    auto inbox = NJsonRpc::JsonRpcMarshalling(std::move(listener));
    inbox = NJsonRpc::JsonRpcExceptionHandling(out, std::move(inbox));

    StartReader(cin, mout, std::move(inbox));
}

} // namespace NLsp
