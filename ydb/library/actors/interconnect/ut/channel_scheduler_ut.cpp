#include <ydb/library/actors/interconnect/channel_scheduler.h>
#include <ydb/library/actors/interconnect/events_local.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(ChannelScheduler) {

    Y_UNIT_TEST(PriorityTraffic) {
        auto common = MakeIntrusive<TInterconnectProxyCommon>();
        common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        std::shared_ptr<IInterconnectMetrics> ctr = CreateInterconnectCounters(common);
        ctr->SetPeerInfo("peer", "1");
        auto callback = [](THolder<IEventBase>) {};
        TEventHolderPool pool(common, callback);
        TSessionParams p;
        TChannelScheduler scheduler(1, {}, ctr, 64 << 20, p);

        ui32 numEvents = 0;

        auto pushEvent = [&](size_t size, int channel) {
            TString payload(size, 'X');
            auto ev = MakeHolder<IEventHandle>(1, 0, TActorId(), TActorId(), MakeIntrusive<TEventSerializedData>(payload, TEventSerializationInfo{}), 0);
            auto& ch = scheduler.GetOutputChannel(channel);
            const bool wasWorking = ch.IsWorking();
            ch.Push(*ev, pool);
            if (!wasWorking) {
                scheduler.AddToHeap(ch, 0);
            }
            ++numEvents;
        };

        for (ui32 i = 0; i < 100; ++i) {
            pushEvent(10000, 1);
        }

        for (ui32 i = 0; i < 1000; ++i) {
            pushEvent(1000, 2);
        }

        std::map<ui16, ui32> run;
        ui32 step = 0;

        std::deque<std::map<ui16, ui32>> window;

        NInterconnect::TOutgoingStream stream;

        for (; numEvents; ++step) {
            TTcpPacketOutTask task(p, stream, stream);

            if (step == 100) {
                for (ui32 i = 0; i < 200; ++i) {
                    pushEvent(1000, 3);
                }
            }

            std::map<ui16, ui32> ch;

            while (numEvents) {
                TEventOutputChannel *channel = scheduler.PickChannelWithLeastConsumedWeight();
                ui32 before = task.GetDataSize();
                ui64 weightConsumed = task.GetDataSize();
                numEvents -= channel->FeedBuf(task, 0);
                weightConsumed = task.GetDataSize() - weightConsumed;
                ui32 after = task.GetDataSize();
                Y_ABORT_UNLESS(after >= before);
                scheduler.FinishPick(weightConsumed, 0);
                const ui32 bytesAdded = after - before;
                if (!bytesAdded) {
                    break;
                }
                ch[channel->ChannelId] += bytesAdded;
            }

            scheduler.Equalize();

            for (const auto& [key, value] : ch) {
                run[key] += value;
            }
            window.push_back(ch);

            if (window.size() == 32) {
                for (const auto& [key, value] : window.front()) {
                    run[key] -= value;
                    if (!run[key]) {
                        run.erase(key);
                    }
                }
                window.pop_front();
            }

            double mean = 0.0;
            for (const auto& [key, value] : run) {
                mean += value;
            }
            mean /= run.size();

            double dev = 0.0;
            for (const auto& [key, value] : run) {
                dev += (value - mean) * (value - mean);
            }
            dev = sqrt(dev / run.size());

            double devToMean = dev / mean;

            Cerr << step << ": ";
            for (const auto& [key, value] : run) {
                Cerr << "ch" << key << "=" << value << " ";
            }
            Cerr << "mean# " << mean << " dev# " << dev << " part# " << devToMean;

            Cerr << Endl;

            UNIT_ASSERT(devToMean < 1);
        }
    }

}
