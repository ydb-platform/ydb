#pragma once

#include "measurement.h"
#include <ydb/tests/tools/parsed_query_bench/proto/execute_query.pb.h>

#include <google/protobuf/util/message_differencer.h>
#include <util/generic/vector.h>

#include <iostream>
#include <memory>
#include <type_traits>

namespace NKikimr::NKqp::NParsedQueryBench {

namespace NReadyClient {

using TRequest = Ydb::Query::Benchmark::ExecuteQueryRequest;

template <class TPayload>
TMeasurementTime Measure(const TRequest& envelope, const TPayload& seed, const TRequest& reference,
    const TString& originalBytes)
{
    auto payload = std::make_unique<TPayload>(seed);
    const auto start = TMeasurementTime::Now();
    TRequest request(envelope);
    if constexpr (std::is_same_v<TPayload, Ydb::Query::QueryContent>) {
        request.set_allocated_query_content(payload.release());
    } else {
        request.set_allocated_parsed_query(payload.release());
    }
    TString wire;
    const bool ok = request.SerializeToString(&wire);
    const auto done = TMeasurementTime::Now();
    Y_ENSURE(ok && wire.size() == originalBytes.size(), "Ready request size changed");
    if (wire != originalBytes) {
        TRequest decoded;
        DecodeRequest(wire, decoded);
        Y_ENSURE(google::protobuf::util::MessageDifferencer::Equals(reference, decoded),
            "Ready request did not preserve the original message");
    }
    return {done.Wall - start.Wall, done.Cpu - start.Cpu};
}

} // namespace NReadyClient

template <class TInputs, class TNames>
int RunReadyClient(size_t warmup, size_t repeats, const TInputs& inputs, const TNames& stageNames) {
    using NReadyClient::TRequest;
    Y_ENSURE(!inputs.empty(), "Expected at least one ready request");
    TVector<TRequest> requests(inputs.size());
    TRequest envelope;
    for (size_t i = 0; i < inputs.size(); ++i) {
        DecodeRequest(inputs[i].Bytes, requests[i]);
        TRequest current(requests[i]);
        current.clear_query();
        if (i == 0) {
            envelope.Swap(&current);
        } else {
            Y_ENSURE(google::protobuf::util::MessageDifferencer::Equals(envelope, current),
                "Ready request envelopes differ");
        }
    }
    std::cout << "stage,iteration,bytes,ready_wall_ns,ready_cpu_ns\n";
    for (size_t round = 0; round < warmup + repeats; ++round) {
        for (size_t slot = 0; slot < inputs.size(); ++slot) {
            const size_t index = (round + slot) % inputs.size();
            const auto& request = requests[index];
            const auto& input = inputs[index];
            const auto elapsed = request.has_query_content()
                ? NReadyClient::Measure(envelope, request.query_content(), request, input.Bytes)
                : NReadyClient::Measure(envelope, request.parsed_query(), request, input.Bytes);
            if (round >= warmup) {
                std::cout << stageNames[static_cast<size_t>(input.Stage)] << ',' << round - warmup << ','
                    << input.Bytes.size() << ',' << elapsed.Wall << ',' << elapsed.Cpu << '\n';
            }
        }
    }
    return 0;
}

} // namespace NKikimr::NKqp::NParsedQueryBench
