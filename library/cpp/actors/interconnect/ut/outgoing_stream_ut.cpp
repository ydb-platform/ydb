#include <library/cpp/actors/interconnect/outgoing_stream.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/random/entropy.h>
#include <util/stream/null.h>

#define Ctest Cnull

Y_UNIT_TEST_SUITE(OutgoingStream) {
    Y_UNIT_TEST(Basic) {
        std::vector<char> buffer;
        buffer.resize(4 << 20);

        TReallyFastRng32 rng(EntropyPool());
        for (char *p = buffer.data(); p != buffer.data() + buffer.size(); p += sizeof(ui32)) {
            *reinterpret_cast<ui32*>(p) = rng();
        }

        for (ui32 nIter = 0; nIter < 10; ++nIter) {
            Cerr << "nIter# " << nIter << Endl;

            size_t base = 0; // number of dropped bytes
            size_t sendOffset = 0; // offset to base
            size_t pending = 0; // number of bytes in queue

            NInterconnect::TOutgoingStreamT<4096> stream;

            size_t numRewindsRemain = 10;

            while (base != buffer.size()) {
                const size_t bytesToEnd = buffer.size() - (base + sendOffset);

                Ctest << "base# " << base << " sendOffset# " << sendOffset << " pending# " << pending
                    << " bytesToEnd# " << bytesToEnd;

                UNIT_ASSERT_VALUES_EQUAL(stream.CalculateOutgoingSize(), pending + sendOffset);
                UNIT_ASSERT_VALUES_EQUAL(stream.CalculateUnsentSize(), pending);

                const size_t maxBuffers = 128;
                std::vector<NActors::TConstIoVec> iov;
                stream.ProduceIoVec(iov, maxBuffers, Max<size_t>());
                size_t offset = base + sendOffset;
                for (const auto& [ptr, len] : iov) {
                    UNIT_ASSERT(memcmp(buffer.data() + offset, ptr, len) == 0);
                    offset += len;
                }
                UNIT_ASSERT(iov.size() == maxBuffers || offset == base + sendOffset + pending);

                const char *nextData = buffer.data() + base + sendOffset + pending;
                const size_t nextDataMaxLen = bytesToEnd - pending;
                const size_t nextDataLen = nextDataMaxLen ? rng() % Min<size_t>(16384, nextDataMaxLen) + 1 : 0;

                if (size_t bytesToScan = sendOffset + pending) {
                    bytesToScan = rng() % bytesToScan + 1;
                    size_t offset = base + sendOffset + pending - bytesToScan;
                    stream.ScanLastBytes(bytesToScan, [&](TContiguousSpan span) {
                        UNIT_ASSERT(offset + span.size() <= base + sendOffset + pending);
                        UNIT_ASSERT(memcmp(buffer.data() + offset, span.data(), span.size()) == 0);
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
                    BOOKMARK
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

                switch (actions[rng() % actions.size()]) {
                    case EAction::COPY_APPEND: {
                        Ctest << " COPY_APPEND nextDataLen# " << nextDataLen;
                        auto span = stream.AcquireSpanForWriting(nextDataLen);
                        UNIT_ASSERT(span.size() != 0);
                        memcpy(span.data(), nextData, span.size());
                        stream.Append(span);
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
                        stream.Append({nextData, nextDataLen});
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

                    case EAction::BOOKMARK:
                        Ctest << " BOOKMARK nextDataLen# " << nextDataLen;
                        auto bookmark = stream.Bookmark(nextDataLen);
                        stream.WriteBookmark(std::move(bookmark), {nextData, nextDataLen});
                        pending += nextDataLen;
                        break;
                }

                Ctest << Endl;
            }
        }
    }
}
