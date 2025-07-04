#include <ydb/library/actors/interconnect/rdma/ut/utils.h>

#include <ydb/library/actors/interconnect/channel_scheduler.h>
#include <ydb/library/actors/interconnect/events_local.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/interconnect/ut/protos/interconnect_test.pb.h>
#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>

#include <util/string/builder.h>

using namespace NActors;

struct TEvTestSerialization : public TEventPB<TEvTestSerialization, NInterconnectTest::TEvTestSerialization, 123> {};

Y_UNIT_TEST_SUITE(RdmaSerialization) {

    TEvTestSerialization* MakeTestEvent(ui64 blobId, NInterconnect::NRdma::IMemPool* memPool = nullptr) {
        auto ev = new TEvTestSerialization();
        ev->Record.SetBlobID(blobId);
        ev->Record.SetBuffer("hello world");
        if (!memPool) {
            ev->AddPayload(TRope(TString(5000, 'X')));
        } else {
            auto buf = memPool->AllocRcBuf(5000);
            std::fill(buf.GetDataMut(), buf.GetDataMut() + 5000, 'X');
            ev->AddPayload(TRope(std::move(buf)));
            UNIT_ASSERT_VALUES_EQUAL(ev->GetPayload().back().size(), 5000);
        }
        UNIT_ASSERT(ev->AllowExternalDataChannel());
        return ev;
    }

    Y_UNIT_TEST(SerializeToRope) {
        auto common = MakeIntrusive<TInterconnectProxyCommon>();
        common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        std::shared_ptr<IInterconnectMetrics> ctr = CreateInterconnectCounters(common);
        ctr->SetPeerInfo(1, "peer", "1");
        auto callback = [](THolder<IEventBase>) {};
        TEventHolderPool pool(common, callback);
        TSessionParams p;
        p.UseExternalDataChannel = true;
        TEventOutputChannel channel(1, 1, 64 << 20, ctr, p, nullptr);

        auto ev = MakeTestEvent(123);
        auto evHandle = MakeHolder<IEventHandle>(TActorId(), TActorId(), ev);

        channel.Push(*evHandle, pool);

        NInterconnect::TOutgoingStream main, xdc;
        TTcpPacketOutTask task(p, main, xdc);

        UNIT_ASSERT(channel.FeedBuf(task, 0));

        TVector<TConstIoVec> mainData, xdcData;
        main.ProduceIoVec(mainData, 100, 10000);
        xdc.ProduceIoVec(xdcData, 100, 10000);

        ui32 totalXdcSize = 0;
        for (const auto& [_, len] : xdcData) {
            totalXdcSize += len;
        }


        auto allocRcBuf = [](ui32 size) {
            return TRcBuf::Uninitialized(size);
        };
        auto serializedRope = ev->SerializeToRope(allocRcBuf);
        UNIT_ASSERT(serializedRope.has_value());
        auto rope = serializedRope->ConvertToString();
        // 6 1 -120 39 88x5000 8 123 18 11 104 101 108 108 111 32 119 111 114 108 100

        UNIT_ASSERT_VALUES_EQUAL(totalXdcSize, rope.size());
        ui32 index = 0;
        for (const auto& [ptr, len] : xdcData) {
            for (size_t i = 0; i < len; ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C((i32)(((char*)ptr)[i]), (i32)rope[index], "Index: " << index);
                ++index;
            }
        }

        auto serializationInfo = ev->CreateSerializationInfo();
        auto parsedEventHandle = std::make_unique<IEventHandle>(
            TActorId(),
            ev->Type(),
            ~IEventHandle::FlagExtendedFormat,
            TActorId(),
            TActorId(),
            MakeIntrusive<TEventSerializedData>(std::move(*serializedRope), std::move(serializationInfo)),
            0,
            TScopeId(),
            NWilson::TTraceId()
        );
        auto parsedEvent = parsedEventHandle->Get<TEvTestSerialization>();
        UNIT_ASSERT(parsedEvent);
        UNIT_ASSERT_VALUES_EQUAL(parsedEvent->Record.GetBlobID(), 123);
        UNIT_ASSERT_VALUES_EQUAL(parsedEvent->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(parsedEvent->GetPayload().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parsedEvent->GetPayload()[0].GetSize(), 5000);
        UNIT_ASSERT_VALUES_EQUAL(parsedEvent->GetPayload()[0].ConvertToString(), TString(5000, 'X'));
    }

    TChannelPart ParseChannelPart(const char** ptr) {
        UNIT_ASSERT_VALUES_EQUAL(sizeof(TChannelPart), 2 * sizeof(ui16));
        TChannelPart part = *reinterpret_cast<const TChannelPart*>(*ptr);
        *ptr += sizeof(TChannelPart);
        return part;
    }

    Y_UNIT_TEST(OutputChannelRdmaRead) {
        auto common = MakeIntrusive<TInterconnectProxyCommon>();
        common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        std::shared_ptr<IInterconnectMetrics> ctr = CreateInterconnectCounters(common);
        ctr->SetPeerInfo(1, "peer", "1");
        auto callback = [](THolder<IEventBase>) {};
        TEventHolderPool pool(common, callback);
        TSessionParams p;
        p.UseExternalDataChannel = true;
        p.UseRdma = true;
        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        TEventOutputChannel channel(1, 1, 64 << 20, ctr, p, memPool);

        auto* ev = MakeTestEvent(123, memPool.get());
        auto evHandle = MakeHolder<IEventHandle>(TActorId(), TActorId(), ev);
        channel.Push(*evHandle, pool);

        NInterconnect::TOutgoingStream main, xdc;
        TTcpPacketOutTask task(p, main, xdc);
        UNIT_ASSERT(channel.FeedBuf(task, 0));

        TVector<TConstIoVec> mainData, xdcData;
        main.ProduceIoVec(mainData, 100, 10000);
        xdc.ProduceIoVec(xdcData, 100, 10000);

        UNIT_ASSERT_VALUES_EQUAL(xdcData.size(), 0);
        UNIT_ASSERT_VALUES_UNEQUAL(mainData.size(), 0);
        TVector<char> mainDataFlat;
        for (const auto& [ptr, len] : mainData) {
            for (size_t i = 0; i < len; ++i) {
                mainDataFlat.push_back(((char*)ptr)[i]);
            }
        }
        for (i32 x: mainDataFlat) {
            Cerr << static_cast<i32>(x) << " ";
        }
        Cerr << Endl;
        const char* ptr = mainDataFlat.data();
        char* end = mainDataFlat.data() + mainDataFlat.size();
        while (*ptr == -66) {
            ++ptr;
        }

        // sections
        auto sectionsChannelPart = ParseChannelPart(&ptr);
        UNIT_ASSERT_VALUES_EQUAL(sectionsChannelPart.GetChannel(), 1);
        TVector<ui32> sectionSizes;
        for (ui32 i = 0; i < 3; ++i) {
            const auto cmd = static_cast<EXdcCommand>(*ptr++);
            UNIT_ASSERT(cmd == EXdcCommand::DECLARE_SECTION_RDMA);
            const ui64 headroom = NInterconnect::NDetail::DeserializeNumber(&ptr, end);
            const ui64 size = NInterconnect::NDetail::DeserializeNumber(&ptr, end);
            const ui64 tailroom = NInterconnect::NDetail::DeserializeNumber(&ptr, end);
            const ui64 alignment = NInterconnect::NDetail::DeserializeNumber(&ptr, end);
            UNIT_ASSERT_VALUES_EQUAL(headroom, 0);
            UNIT_ASSERT_VALUES_EQUAL(tailroom, 0);
            UNIT_ASSERT_VALUES_EQUAL(alignment, 0);
            sectionSizes.push_back(size);
            Cerr << "Section size: " << size << Endl;
        }

        // read command
        auto cmdChannelPart = ParseChannelPart(&ptr);
        NActorsInterconnect::TRdmaCreds creds;
        UNIT_ASSERT_VALUES_EQUAL(cmdChannelPart.GetChannel(), 1);
        {
            const auto cmd = static_cast<EXdcCommand>(*ptr++);
            UNIT_ASSERT(cmd == NActors::EXdcCommand::RDMA_READ);
            const ui16 credsSerializedSize = ReadUnaligned<ui16>(ptr);
            Cerr << "Rdma read command size: " << credsSerializedSize << Endl;
            ptr += sizeof(ui16);
            Y_ABORT_UNLESS(creds.ParseFromArray(ptr, credsSerializedSize));
            for (const auto& cred : creds.GetCreds()) {
                Cerr << "Rdma cred: " << cred.GetAddress() << " " << cred.GetRkey() << " " << cred.GetSize() << Endl;
            }
        }

        // read rdma data
        auto rdma = InitLocalRdmaStuff();

        TVector<TMemRegionPtr> memRegions;
        for (const auto& cred: creds.GetCreds()) {
            auto regToRead = memPool->Alloc(cred.GetSize());
            ReadOneMemRegion(
                rdma, rdma->Qp2,
                reinterpret_cast<void*>(cred.GetAddress()), cred.GetRkey(), cred.GetSize(),
                regToRead
            );
            memRegions.emplace_back(std::move(regToRead));
        }
        ui32 totalSize = 0;
        for (const auto& reg : memRegions) {
            totalSize += reg->GetSize();
        }
        UNIT_ASSERT_VALUES_EQUAL(totalSize, sectionSizes[0] + sectionSizes[1] + sectionSizes[2]);
        UNIT_ASSERT_VALUES_EQUAL(memRegions[1]->GetSize(), 5000);
        for (size_t i = 0; i < sectionSizes[1]; ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                ((char*)memRegions[1]->GetAddr())[i],
                'X',
                "Index: " << i
            );
        }
    }

    class TSendActor: public TActorBootstrapped<TSendActor> {
    public:
        TSendActor(TActorId recipient, IEventBase* ev)
            : Recipient(recipient)
            , Event(ev)
        {}

        void Bootstrap() {
            Send(Recipient, Event);
            Cerr << "Sent event to " << Recipient.ToString() << Endl;
            PassAway();
        }

    private:
        TActorId Recipient;
        IEventBase* Event;
    };

    class TReceiveActor: public TActorBootstrapped<TReceiveActor> {
    public:
        TReceiveActor(std::function<void(TEvTestSerialization::TPtr)> check)
            : Check(check)
        {}

        void Bootstrap() {
            Become(&TReceiveActor::StateFunc);
        }
        void Handle(TEvTestSerialization::TPtr& ev) {
            Check(ev);
            ReceivedEvents++;
        }
        STRICT_STFUNC(StateFunc,
            hFunc(TEvTestSerialization, Handle);
        )
    public:
        ui32 ReceivedEvents = 0;
    private:
        std::function<void(TEvTestSerialization::TPtr)> Check;
    };

    Y_UNIT_TEST(SendRdma) {
        TTestICCluster cluster(2);
        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        auto* ev = MakeTestEvent(123, memPool.get());

        auto recieverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
            Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 5000);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(5000, 'X'));
        });
        const TActorId receiver = cluster.RegisterActor(recieverPtr, 1);

        Sleep(TDuration::MilliSeconds(1000));

        auto senderPtr = new TSendActor(receiver, ev);
        cluster.RegisterActor(senderPtr, 2);

        Sleep(TDuration::MilliSeconds(1000));
        UNIT_ASSERT_VALUES_EQUAL(recieverPtr->ReceivedEvents, 1);
    }

    Y_UNIT_TEST(SendMix) {
        TTestICCluster cluster(2);

        ui32 index = 0;
        auto recieverPtr = new TReceiveActor([&index](TEvTestSerialization::TPtr ev) {
            Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), index++);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 5000);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(5000, 'X'));
        });
        const TActorId receiver = cluster.RegisterActor(recieverPtr, 1);

        Sleep(TDuration::MilliSeconds(1000));

        const ui32 numEvents = 10;
        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        for (ui32 i = 0; i < numEvents; ++i) {
            const bool isRdma = i % 2 == 0;
            auto* ev = MakeTestEvent(i, isRdma ? memPool.get() : nullptr);
            auto senderPtr = new TSendActor(receiver, ev);
            cluster.RegisterActor(senderPtr, 2);
        }

        Sleep(TDuration::MilliSeconds(1000));
        UNIT_ASSERT_VALUES_EQUAL(recieverPtr->ReceivedEvents, numEvents);
    }

    struct TEventsForTest {
        std::vector<TEvTestSerialization*> Events;
        std::unordered_map<ui64, std::function<void(TEvTestSerialization*)>> Checks;
        std::shared_ptr<NInterconnect::NRdma::IMemPool> MemPool;

        TEventsForTest(ui32 numEvents)
            : MemPool(NInterconnect::NRdma::CreateDummyMemPool())
        {
            Generate(numEvents, MemPool.get());
        }

        void Generate(ui32 numEvents, NInterconnect::NRdma::IMemPool* memPool) {
            for (ui32 i = 0; i < numEvents; ++i) {
                const bool isInline = i % 3 == 0;
                const bool isXdc = i % 3 == 1;
                const bool isRdma = i % 3 == 2;
                const ui32 numPayloads = i % 5 + (isXdc || isRdma);

                auto ev = new TEvTestSerialization();
                ev->Record.SetBlobID(i);
                ev->Record.SetBuffer(TStringBuilder{} << "hello world " << i);
                for (ui32 j = 0; j < numPayloads; ++j) {
                    if (isInline) {
                        ev->AddPayload(TRope(TString(10 + j, j + i)));
                    } else if (isXdc) {
                        ev->AddPayload(TRope(TString(5000 + j, j + i)));
                    } else if (isRdma) {
                        auto buf = memPool->AllocRcBuf(5000 + j);
                        std::fill(buf.GetDataMut(), buf.GetDataMut() + 5000 + j, j + i);
                        ev->AddPayload(TRope(std::move(buf)));
                        UNIT_ASSERT_VALUES_EQUAL(ev->GetPayload().back().size(), 5000 + j);
                    }
                }

                if (isXdc || isRdma) {
                    UNIT_ASSERT(ev->AllowExternalDataChannel());
                }

                Events.push_back(ev);

                Checks.emplace(i, [i, numPayloads, isInline](TEvTestSerialization* ev) {
                    UNIT_ASSERT_VALUES_EQUAL(ev->Record.GetBlobID(), i);
                    UNIT_ASSERT_VALUES_EQUAL(ev->Record.GetBuffer(), TStringBuilder{} << "hello world " << i);
                    UNIT_ASSERT_VALUES_EQUAL(ev->GetPayload().size(), numPayloads);
                    for (ui32 j = 0; j < numPayloads; ++j) {
                        ui32 payloadSize = isInline ? 10 + j : 5000 + j;
                        UNIT_ASSERT_VALUES_EQUAL(ev->GetPayload()[j].GetSize(), payloadSize);
                        UNIT_ASSERT_VALUES_EQUAL(ev->GetPayload()[j].ConvertToString(), TString(payloadSize, j + i));
                    }
                });

            }

            std::random_shuffle(Events.begin(), Events.end());
        }
    };

    Y_UNIT_TEST(SendMixBig) {
        TTestICCluster cluster(2);
        TEventsForTest events(1000);

        auto recieverPtr = new TReceiveActor([&events](TEvTestSerialization::TPtr ev) {
            ui64 blobId = ev->Get()->Record.GetBlobID();
            auto checkIt = events.Checks.find(blobId);
            UNIT_ASSERT(checkIt != events.Checks.end());
            checkIt->second(ev->Get());
            events.Checks.erase(checkIt);
        });
        const TActorId receiver = cluster.RegisterActor(recieverPtr, 1);
        Sleep(TDuration::MilliSeconds(1000));

        for (auto* ev : events.Events) {
            auto senderPtr = new TSendActor(receiver, ev);
            cluster.RegisterActor(senderPtr, 2);
        }

        for (ui32 attempt = 0; attempt < 10 && !events.Checks.empty(); ++attempt) {
            Sleep(TDuration::MilliSeconds(1000));
        }
        UNIT_ASSERT_VALUES_EQUAL(events.Checks.size(), 0);
    }

}
