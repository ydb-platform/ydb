#include <ydb/core/blobstorage/backpressure/queue_backpressure_server.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/vector.h>
#include <util/stream/null.h>
#include <util/stream/str.h>
#include <util/system/yassert.h>

#include <algorithm>

namespace {

using namespace NKikimr;
using namespace NKikimr::NBackpressure;

constexpr size_t MaxClients = 8;
constexpr size_t MaxOperations = 512;

struct TInFlight {
    TActorId ActorId;
    TMessageId MsgId;
    ui64 Cost;
};

struct TClientModel {
    ui64 ClientId = 0;
    TActorId ActorId;
    TMessageId Next;
};

TActorId MakeActorId(ui64 clientId) {
    return TActorId(1, 1, clientId + 1, 1);
}

TClientModel& PickClient(TVector<TClientModel>& clients, FuzzedDataProvider& fdp) {
    if (clients.empty() || (clients.size() < MaxClients && fdp.ConsumeBool())) {
        const ui64 id = clients.size() + 1;
        clients.push_back({id, MakeActorId(id), {}});
    }
    return clients[fdp.ConsumeIntegralInRange<size_t>(0, clients.size() - 1)];
}

void RunQueueBackpressureFuzz(FuzzedDataProvider& fdp) {
    const bool checkMsgId = fdp.ConsumeBool();
    const ui64 maxCost = fdp.ConsumeIntegralInRange<ui64>(4, 256);
    const ui64 minLow = fdp.ConsumeIntegralInRange<ui64>(1, 16);
    const ui64 maxLow = fdp.ConsumeIntegralInRange<ui64>(minLow, 64);
    TQueueBackpressure<ui64> queue(
        checkMsgId,
        maxCost,
        fdp.ConsumeIntegralInRange<ui64>(1, 32),
        minLow,
        maxLow,
        fdp.ConsumeIntegralInRange<ui64>(1, 90),
        fdp.ConsumeIntegralInRange<ui64>(1, 64),
        fdp.ConsumeIntegralInRange<ui64>(1, 96),
        TDuration::MilliSeconds(fdp.ConsumeIntegralInRange<ui64>(1, 1000)));

    TVector<TClientModel> clients;
    TVector<TInFlight> inFlight;
    TInstant now = TInstant::MilliSeconds(1);

    for (size_t step = 0; step < MaxOperations && fdp.remaining_bytes(); ++step) {
        now += TDuration::MilliSeconds(fdp.ConsumeIntegralInRange<ui32>(0, 50));
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 4)) {
            case 0:
            case 1:
            case 2: {
                TClientModel& client = PickClient(clients, fdp);
                TMessageId msgId = client.Next;
                if (checkMsgId && fdp.ConsumeIntegralInRange<unsigned>(0, 7) == 0) {
                    msgId.MsgId += fdp.ConsumeIntegralInRange<ui64>(1, 3);
                }
                const ui64 cost = fdp.ConsumeIntegralInRange<ui64>(1, maxCost + 32);
                auto feedback = queue.Push(client.ClientId, client.ActorId, msgId, cost, now);

                Y_ABORT_UNLESS(feedback.first.ActorId == client.ActorId);
                Y_ABORT_UNLESS(feedback.first.ActualWindowSize <= feedback.first.MaxWindowSize);
                if (feedback.first.Status == NKikimrBlobStorage::TWindowFeedback::Success) {
                    inFlight.push_back({client.ActorId, msgId, cost});
                    client.Next = feedback.first.ExpectedMsgId;
                } else {
                    client.Next = feedback.first.ExpectedMsgId;
                }
                break;
            }

            case 3: {
                if (!inFlight.empty()) {
                    const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, inFlight.size() - 1);
                    const TInFlight item = inFlight[index];
                    auto feedback = queue.Processed(item.ActorId, item.MsgId, item.Cost, now);
                    Y_ABORT_UNLESS(feedback.first.Status == NKikimrBlobStorage::TWindowFeedback::Processed);
                    inFlight.erase(inFlight.begin() + index);
                }
                break;
            }

            case 4: {
                TStringStream out;
                queue.Output(out, now);
                break;
            }
        }

        ui64 observedCost = 0;
        queue.ForEachWindow([&](const auto& window) {
            observedCost += window->GetCost();
            Y_ABORT_UNLESS(window->GetCost() <= maxCost + 32 || observedCost >= window->GetCost());
        });

        ui64 modelCost = 0;
        for (const auto& item : inFlight) {
            modelCost += item.Cost;
        }
        Y_ABORT_UNLESS(observedCost == modelCost);
    }

    while (!inFlight.empty()) {
        const TInFlight item = inFlight.back();
        inFlight.pop_back();
        queue.Processed(item.ActorId, item.MsgId, item.Cost, now);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    RunQueueBackpressureFuzz(fdp);
    return 0;
}
