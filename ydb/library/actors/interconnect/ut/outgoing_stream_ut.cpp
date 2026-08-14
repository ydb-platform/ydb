#include <ydb/library/actors/interconnect/packet.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/random/entropy.h>
#include <util/random/fast.h>
#include <util/stream/null.h>
#include <util/system/compiler.h>
#include <util/system/datetime.h>

#define Ctest Cnull

Y_UNIT_TEST_SUITE(OutgoingStream) {
    using TOutStream = NInterconnect::TOutgoingStreamT<4096>;

    std::shared_ptr<NInterconnect::NRdma::IMemPool> CreateWarmedSlotMemPool() {
        auto memPool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, {});
        for (size_t size = 512; size <= static_cast<size_t>(memPool->GetMaxAllocSz()); size <<= 1) {
            auto region = memPool->Alloc(size, NInterconnect::NRdma::IMemPool::EMPTY);
            if (!region) {
                Cerr << "Skipping RDMA outgoing stream test, SlotMemPool allocation failed" << Endl;
                return {};
            }
            region.Reset();
        }
        return memPool;
    }

    class TTrackingMemPool final : public NInterconnect::NRdma::IMemPool {
    public:
        explicit TTrackingMemPool(std::shared_ptr<NInterconnect::NRdma::IMemPool> underlying)
            : Underlying(std::move(underlying))
        {
            UNIT_ASSERT(Underlying);
        }

        int GetMaxAllocSz() const noexcept override {
            return Underlying->GetMaxAllocSz();
        }

        TString GetName() const noexcept override {
            return Underlying->GetName();
        }

        bool Contains(const void* ptr, size_t size) const noexcept {
            const auto* begin = static_cast<const char*>(ptr);
            const auto* end = begin + size;
            for (const auto& range : Ranges) {
                if (range.Begin <= begin && end <= range.End) {
                    return true;
                }
            }
            return false;
        }

    protected:
        NInterconnect::NRdma::TMemRegionPtr AllocImpl(int size, ui32 flags) noexcept override {
            auto region = Underlying->Alloc(size, flags);
            if (region) {
                const auto* begin = static_cast<const char*>(region->GetAddr());
                Ranges.push_back(TRange{begin, begin + region->GetSize()});
            }
            return region;
        }

        void Free(NInterconnect::NRdma::TMemRegion&&, NInterconnect::NRdma::TChunk&) noexcept override {
            Y_ABORT("tracking mem pool does not own chunks");
        }

        void DealocateMr(NInterconnect::NRdma::TChunk*) noexcept override {
            Y_ABORT("tracking mem pool does not own chunks");
        }

    private:
        void Tick(NMonotonic::TMonotonic) noexcept override {
        }

        struct TRange {
            const char* Begin = nullptr;
            const char* End = nullptr;
        };

        std::shared_ptr<NInterconnect::NRdma::IMemPool> Underlying;
        std::vector<TRange> Ranges;
    };

    void OutgoingTest(bool withExternal, std::shared_ptr<NInterconnect::NRdma::IMemPool> allocator = {}) {
        struct {
            ui32 ZcCounter = 0; // ZcCounter to handle zero copy async transfer from some event queue
            std::vector<char> Buffer;
        } ev;
        ev.Buffer.resize(4 << 20);

        TReallyFastRng32 rng(EntropyPool());
        for (char *p = ev.Buffer.data(); p != ev.Buffer.data() + ev.Buffer.size(); p += sizeof(ui32)) {
            *reinterpret_cast<ui32*>(p) = rng();
        }

        for (ui32 nIter = 0; nIter < 10; ++nIter) {
            Ctest << "nIter# " << nIter << Endl;

            size_t base = 0; // number of dropped bytes
            size_t sendOffset = 0; // offset to base
            size_t pending = 0; // number of bytes in queue

            TOutStream stream(allocator);
            bool zcSync = false;

            size_t numRewindsRemain = 10;
            
            ui32 zcTransferId = 0; // Emulate zc copy counter

            while (base != ev.Buffer.size()) {
                const size_t bytesToEnd = ev.Buffer.size() - (base + sendOffset);

                Ctest << "base# " << base << " sendOffset# " << sendOffset << " pending# " << pending
                    << " bytesToEnd# " << bytesToEnd;

                UNIT_ASSERT_VALUES_EQUAL(stream.CalculateOutgoingSize(), pending + sendOffset);
                UNIT_ASSERT_VALUES_EQUAL(stream.CalculateUnsentSize(), pending);

                const size_t maxBuffers = 128;
                std::vector<NActors::TConstIoVec> iov;
                std::vector<TOutStream::TBufController> ctrl;
                stream.ProduceIoVec(iov, maxBuffers, Max<size_t>(), withExternal ? &ctrl : nullptr);

                if (withExternal) {
                    Y_ABORT_UNLESS(iov.size() == ctrl.size());
                    if (zcSync == false) {
                        for (auto& x : ctrl) {
                            if (x.ZcReady()) {
                                x.Update(++zcTransferId);
                            }
                        }
                    }
                }
                size_t offset = base + sendOffset;
                for (const auto& [ptr, len] : iov) {
                    UNIT_ASSERT(memcmp(ev.Buffer.data() + offset, ptr, len) == 0);
                    offset += len;
                }
                UNIT_ASSERT(iov.size() == maxBuffers || offset == base + sendOffset + pending);

                const char *nextData = ev.Buffer.data() + base + sendOffset + pending;
                const size_t nextDataMaxLen = bytesToEnd - pending;
                const size_t nextDataLen = nextDataMaxLen ? rng() % Min<size_t>(16384, nextDataMaxLen) + 1 : 0;

                if (size_t bytesToScan = sendOffset + pending) {
                    bytesToScan = rng() % bytesToScan + 1;
                    size_t offset = base + sendOffset + pending - bytesToScan;
                    stream.ScanLastBytes(bytesToScan, [&](TContiguousSpan span) {
                        UNIT_ASSERT(offset + span.size() <= base + sendOffset + pending);
                        UNIT_ASSERT(memcmp(ev.Buffer.data() + offset, span.data(), span.size()) == 0);
                        offset += span.size();
                    });
                    UNIT_ASSERT_VALUES_EQUAL(offset, base + sendOffset + pending);
                }

                enum class EAction {
                    COPY_APPEND,
                    WRITE,
                    REF_APPEND,
                    ADVANCE,
                    REWIND,
                    DROP,
                    BOOKMARK,
                    EMULATE_ZC_USAGE,
                };

                std::vector<EAction> actions;
                if (nextDataLen) {
                    actions.push_back(EAction::COPY_APPEND);
                    actions.push_back(EAction::WRITE);
                    actions.push_back(EAction::REF_APPEND);
                    actions.push_back(EAction::BOOKMARK);
                }
                if (numRewindsRemain && sendOffset > 65536) {
                    actions.push_back(EAction::REWIND);
                }
                actions.push_back(EAction::ADVANCE);
                actions.push_back(EAction::DROP);

                if (withExternal) {
                    actions.push_back(EAction::EMULATE_ZC_USAGE);
                }

                switch (actions[rng() % actions.size()]) {
                    case EAction::COPY_APPEND: {
                        Ctest << " COPY_APPEND nextDataLen# " << nextDataLen;
                        auto span = stream.AcquireSpanForWriting(nextDataLen);
                        UNIT_ASSERT(span.size() != 0);
                        memcpy(span.data(), nextData, span.size());
                        stream.Append(span, static_cast<ui32*>(nullptr));
                        pending += span.size();
                        break;
                    }

                    case EAction::WRITE:
                        Ctest << " WRITE nextDataLen# " << nextDataLen;
                        stream.Write({nextData, nextDataLen});
                        pending += nextDataLen;
                        break;

                    case EAction::REF_APPEND:
                        Ctest << " REF_APPEND nextDataLen# " << nextDataLen;
                        stream.Append({nextData, nextDataLen}, &ev.ZcCounter);
                        pending += nextDataLen;
                        break;

                    case EAction::ADVANCE: {
                        const size_t advance = rng() % Min<size_t>(4096, pending + 1);
                        Ctest << " ADVANCE advance# " << advance;
                        stream.Advance(advance);
                        sendOffset += advance;
                        pending -= advance;
                        break;
                    }

                    case EAction::REWIND:
                        Ctest << " REWIND";
                        stream.Rewind();
                        pending += sendOffset;
                        sendOffset = 0;
                        --numRewindsRemain;
                        break;

                    case EAction::DROP: {
                        const size_t drop = rng() % Min<size_t>(65536, sendOffset + 1);
                        Ctest << " DROP drop# " << drop;
                        stream.DropFront(drop);
                        base += drop;
                        sendOffset -= drop;
                        break;
                    }

                    case EAction::BOOKMARK: {
                        Ctest << " BOOKMARK nextDataLen# " << nextDataLen;
                        auto bookmark = stream.Bookmark(nextDataLen);
                        stream.WriteBookmark(std::move(bookmark), {nextData, nextDataLen});
                        pending += nextDataLen;
                        break;
                    }

                    case EAction::EMULATE_ZC_USAGE:
                        if (zcSync == false) {
                            UNIT_ASSERT_VALUES_EQUAL(ev.ZcCounter, zcTransferId);
                            zcSync = true;
                        }
                        break;
                }

                Ctest << Endl;
            }
            ev.ZcCounter = 0;
        }
    }

    Y_UNIT_TEST(Basic) {
        OutgoingTest(false);
    }

    Y_UNIT_TEST(WithExternalLife) {
        OutgoingTest(true);
    }

    Y_UNIT_TEST(RdmaMemory) {
        auto memPool = CreateWarmedSlotMemPool();
        if (!memPool) {
            return;
        }
        OutgoingTest(false, std::move(memPool));
    }

    Y_UNIT_TEST(RdmaMemoryPreallocateNoAlloc) {
        auto memPool = CreateWarmedSlotMemPool();
        if (!memPool) {
            return;
        }
        TOutStream stream(memPool);

        std::vector<char> data(128 * 1024);
        TReallyFastRng32 rng(EntropyPool());
        for (char *p = data.data(); p != data.data() + data.size(); p += sizeof(ui32)) {
            *reinterpret_cast<ui32*>(p) = rng();
        }

        UNIT_ASSERT(stream.PreallocateForWriting(data.size()));

        size_t offset = 0;
        while (offset != data.size()) {
            auto span = stream.AcquireSpanForWritingNoAlloc(data.size() - offset);
            UNIT_ASSERT(span.size());
            memcpy(span.data(), data.data() + offset, span.size());
            stream.Append(span, static_cast<ui32*>(nullptr));
            offset += span.size();
        }

        UNIT_ASSERT_VALUES_EQUAL(stream.CalculateOutgoingSize(), data.size());
        UNIT_ASSERT_VALUES_EQUAL(stream.CalculateUnsentSize(), data.size());

        std::vector<NActors::TConstIoVec> iov;
        stream.ProduceIoVec(iov, Max<size_t>(), Max<size_t>());

        offset = 0;
        for (const auto& [ptr, len] : iov) {
            UNIT_ASSERT(memcmp(data.data() + offset, ptr, len) == 0);
            offset += len;
        }
        UNIT_ASSERT_VALUES_EQUAL(offset, data.size());

        stream.Advance(data.size());
        stream.DropFront(data.size());
        UNIT_ASSERT_VALUES_EQUAL(stream.CalculateOutgoingSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(stream.CalculateUnsentSize(), 0);
    }

    Y_UNIT_TEST(PacketBuilderUsesPreallocatedRdmaMemoryForInternalStream) {
        auto slotMemPool = CreateWarmedSlotMemPool();
        if (!slotMemPool) {
            return;
        }
        auto memPool = std::make_shared<TTrackingMemPool>(std::move(slotMemPool));
        NInterconnect::TOutgoingStream mainStream(memPool);
        NInterconnect::TOutgoingStream xdcStream;

        UNIT_ASSERT(mainStream.PreallocateForWriting(TTcpPacketBuf::FullPacketSize));

        TSessionParams params;
        params.AllowRdmaSendReceive = true;
        TTcpPacketOutTask packet(params, mainStream, xdcStream, true);

        const ui64 bookmarked = 42;
        auto bookmark = packet.Bookmark(sizeof(bookmarked));
        packet.WriteBookmark(std::move(bookmark), &bookmarked, sizeof(bookmarked));

        const TString payload(TTcpPacketBuf::PacketDataLen / 2, 'X');
        packet.Write<false>(payload.data(), payload.size());
        const TString appendedPayload(128, 'A');
        auto appendSpan = packet.AcquireSpanForWriting<false>().SubSpan(0, appendedPayload.size());
        memcpy(appendSpan.data(), appendedPayload.data(), appendSpan.size());
        packet.Append<false>(appendSpan.data(), appendSpan.size(), nullptr, false);
        auto rdmaBuf = memPool->AllocRcBuf(128, NInterconnect::NRdma::IMemPool::EMPTY);
        UNIT_ASSERT(rdmaBuf);
        memset(rdmaBuf->UnsafeGetDataMut(), 'R', rdmaBuf->GetSize());
        const auto rdmaBufRegion = NInterconnect::NRdma::TryExtractFromRcBuf(*rdmaBuf);
        UNIT_ASSERT(!rdmaBufRegion.Empty());
        packet.AppendRdma(rdmaBuf->GetData(), rdmaBuf->GetSize(), rdmaBufRegion.GetMemRegion(), false);
        packet.Finish(1, 0);

        std::vector<NActors::TConstIoVec> iov;
        mainStream.ProduceIoVec(iov, Max<size_t>(), Max<size_t>());
        UNIT_ASSERT(!iov.empty());

        size_t total = 0;
        for (const auto& [ptr, len] : iov) {
            UNIT_ASSERT_C(memPool->Contains(ptr, len),
                TStringBuilder() << "iovec ptr# " << ptr << " len# " << len << " is not backed by RDMA memory");
            total += len;
        }
        UNIT_ASSERT_VALUES_EQUAL(total, packet.GetPacketSize());
        UNIT_ASSERT_VALUES_EQUAL(xdcStream.CalculateOutgoingSize(), 0);

        std::vector<NInterconnect::NRdma::TSendSge> rdmaChunks;
        const size_t rdmaBytes = mainStream.ProduceRdmaSendVec(rdmaChunks, Max<size_t>(), Max<size_t>());
        UNIT_ASSERT(!rdmaChunks.empty());
        UNIT_ASSERT_VALUES_EQUAL(rdmaBytes, packet.GetPacketSize());
        for (const auto& chunk : rdmaChunks) {
            UNIT_ASSERT(chunk.MemRegion);
            UNIT_ASSERT(chunk.Data);
            UNIT_ASSERT(chunk.Size);
        }
    }

    template <typename TCallback>
    void ReportLatency(const char* allocatorName, const char* name, size_t iterations, TCallback&& callback) {
        const TInstant start = TInstant::Now();
        for (size_t i = 0; i != iterations; ++i) {
            callback(i);
        }
        const TDuration elapsed = TInstant::Now() - start;
        Cerr << "OutgoingStreamLatency " << allocatorName << "." << name
            << " iterations# " << iterations
            << " ns/op# " << static_cast<double>(elapsed.NanoSeconds()) / iterations
            << Endl;
    }

    void PublicMethodsLatencyBenchmarkImpl(const char* allocatorName, std::shared_ptr<NInterconnect::NRdma::IMemPool> allocator) {
        constexpr size_t iterations = 100000;
        char payload[64];
        memset(payload, 7, sizeof(payload));

        TOutStream acquireStream(allocator);
        ReportLatency(allocatorName, "AcquireSpanForWriting+Append", iterations, [&](size_t) {
            auto& stream = acquireStream;
            auto span = stream.AcquireSpanForWriting(sizeof(payload));
            memcpy(span.data(), payload, span.size());
            stream.Append(span, static_cast<ui32*>(nullptr));
            Y_DO_NOT_OPTIMIZE_AWAY(stream.GetSendQueueSize());
        });

        TOutStream writeStream(allocator);
        ReportLatency(allocatorName, "Write", iterations, [&](size_t) {
            auto& stream = writeStream;
            stream.Write({payload, sizeof(payload)});
            Y_DO_NOT_OPTIMIZE_AWAY(stream.GetSendQueueSize());
        });

        TOutStream bookmarkStream(allocator);
        ReportLatency(allocatorName, "Bookmark+WriteBookmark", iterations, [&](size_t) {
            auto& stream = bookmarkStream;
            auto bookmark = stream.Bookmark(sizeof(payload));
            stream.WriteBookmark(std::move(bookmark), {payload, sizeof(payload)});
            Y_DO_NOT_OPTIMIZE_AWAY(stream.GetSendQueueSize());
        });

        TOutStream noAllocStream(allocator);
        ReportLatency(allocatorName, "Preallocate+AcquireNoAlloc+Append", iterations, [&](size_t) {
            auto& stream = noAllocStream;
            Y_ABORT_UNLESS(stream.PreallocateForWriting(sizeof(payload)));
            auto span = stream.AcquireSpanForWritingNoAlloc(sizeof(payload));
            memcpy(span.data(), payload, span.size());
            stream.Append(span, static_cast<ui32*>(nullptr));
            Y_DO_NOT_OPTIMIZE_AWAY(stream.GetSendQueueSize());
        });

        TOutStream stream(allocator);
        for (size_t i = 0; i != 256; ++i) {
            stream.Append({payload, sizeof(payload)}, static_cast<ui32*>(nullptr));
        }

        ReportLatency(allocatorName, "ProduceIoVec", iterations, [&](size_t) {
            //std::vector<NActors::TConstIoVec> iov;
            //iov.reserve(16);
            //???? StackVes slow ????
            TStackVec<NActors::TConstIoVec, 16, false> iov;
            stream.ProduceIoVec(iov, 16, 1024);
            Y_DO_NOT_OPTIMIZE_AWAY(iov.size());
        });

        ReportLatency(allocatorName, "ScanLastBytes", iterations, [&](size_t) {
            size_t bytes = 0;
            stream.ScanLastBytes(sizeof(payload), [&](TContiguousSpan span) {
                bytes += span.size();
            });
            Y_DO_NOT_OPTIMIZE_AWAY(bytes);
        });

        ReportLatency(allocatorName, "CalculateSizes", iterations, [&](size_t) {
            size_t value = stream.CalculateOutgoingSize() + stream.CalculateUnsentSize() + stream.GetSendQueueSize();
            Y_DO_NOT_OPTIMIZE_AWAY(value);
        });

        ReportLatency(allocatorName, "Rewind", iterations, [&](size_t) {
            stream.RewindToEnd();
            stream.Rewind();
            Y_DO_NOT_OPTIMIZE_AWAY(stream.CalculateUnsentSize());
        });

        TOutStream dropStream(allocator);
        for (size_t i = 0; i != iterations; ++i) {
            dropStream.Write({payload, sizeof(payload)});
        }

        ReportLatency(allocatorName, "Advance+DropFront", iterations, [&](size_t) {
            dropStream.Advance(sizeof(payload));
            dropStream.DropFront(sizeof(payload));
            Y_DO_NOT_OPTIMIZE_AWAY(dropStream.GetSendQueueSize());
        });
    }

    Y_UNIT_TEST(PublicMethodsLatencyBenchmark) {
        PublicMethodsLatencyBenchmarkImpl("malloc", {});
        if (auto memPool = CreateWarmedSlotMemPool()) {
            PublicMethodsLatencyBenchmarkImpl("rdma", std::move(memPool));
        }
    }
}
