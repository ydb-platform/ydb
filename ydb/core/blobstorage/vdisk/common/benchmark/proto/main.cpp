#include <cstring>
#include <vector>

#include <library/cpp/actors/core/event.h>
#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/int128/int128.h>
#include <library/cpp/testing/benchmark/bench.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/system/yassert.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace {

template <size_t bytes>
class Buffer
    : public IInputStream
    , public IOutputStream
{
public:
    size_t DoRead(void* buf, size_t len) override {
        size_t target = std::min(bytes - ReadPos, len);
        if (target < len) {
            throw yexception();
        }
        std::memcpy(buf, Buffer + ReadPos, target);
        ReadPos += target;
        return target;
    }

    void DoWrite(const void* buf, size_t len) override {
        size_t target = std::min(bytes - WritePos, len);
        if (target < len) {
            throw yexception();
        }
        std::memcpy(Buffer + WritePos, buf, target);
        WritePos += target;
    }

    void Flush() {
        ReadPos = WritePos = 0;
    }
private:
    size_t ReadPos = 0;
    size_t WritePos = 0;
    std::byte Buffer[bytes];
};

template <typename Event, size_t bytes>
void SerDeLoop(Event& ev, Buffer<bytes>& buffer, size_t iterations, size_t read_iterations) {
    for (size_t i = 0; i < iterations; ++i) {
        ev.Save(&buffer);

        for (size_t j = 0; j < read_iterations; ++j) {
            ev.Load(&buffer);
            buffer.Flush();
        }
    }
}

const char* storeString32 =
"................................"
;

const char* storeString64 =
"................................"
"................................"
;

const char* storeString128 =
"................................"
"................................"
"................................"
"................................"
;

const char* storeString256 =
"................................"
"................................"
"................................"
"................................"
"................................"
"................................"
"................................"
"................................"
;

struct TTestStats {
    ui128 BytesWritten{0};
    size_t EvCount{0};
    TDuration SerializeDuration{TDuration::MilliSeconds(0)};
    TDuration DeserializeDuration{TDuration::MilliSeconds(0)};

    TTestStats& operator+=(const TTestStats& that) {
        BytesWritten += that.BytesWritten;
        EvCount += that.EvCount;
        SerializeDuration += that.SerializeDuration;
        DeserializeDuration += that.DeserializeDuration;
        return *this;
    }
};

using TEvVPut = NKikimr::TEvBlobStorage::TEvVPut;
// using TEvVPut = NKikimrBlobStorage::TEvVPut;
using NActors::TChunkSerializer;

template <typename InitFunc>
ui128 InitializeEvents(std::span<TEvVPut> evs, InitFunc& initFunc) {
    ui128 bytesWritten = 0;
    for (auto& ev : evs) {
        bytesWritten += initFunc(ev);
    }
    return bytesWritten;
}

void SerializeEvents(std::span<TEvVPut> evs, std::span<TChunkSerializer*> streams) {
    for (size_t i = 0; i < evs.size(); ++i) {
        auto& ev = evs[i];
        auto* stream = streams[i];
        ev.SerializeToArcadiaStream(stream);
    }
}

void DeserializeEvents(std::span<TEvVPut> evs, std::span<NActors::TEventSerializationInfo> infos, std::vector<NActors::IEventBase*>& trash, std::span<NActors::TAllocChunkSerializer> streams) {
    for (size_t i = 0; i < evs.size(); ++i) {
        auto& stream = streams[i];
        auto& info = infos[i];
        auto data = stream.Release(std::move(info));
        auto ev = TEvVPut::Load(data.Get());
        trash[i] = ev;
    }
}

template <size_t max_bytes, typename InitFunc>
TTestStats DoTest(size_t evCount, InitFunc&& initFunc) {
    TTestStats stats;
    stats.EvCount = evCount;

    std::vector<TEvVPut> evs(evCount);
    std::vector<NActors::TEventSerializationInfo> infos;
    infos.reserve(evs.size());
    for (auto& ev : evs) {
        infos.emplace_back(ev.CreateSerializationInfo());
    }
    std::vector<NActors::IEventBase*> trash(evCount);
    std::vector<NActors::TAllocChunkSerializer> streams(evCount);
    std::vector<TChunkSerializer*> serializers;
    serializers.reserve(streams.size());
    for (auto& s : streams) {
        serializers.push_back(&s);
    }

    stats.BytesWritten = InitializeEvents(evs, initFunc);

    {
        auto start = TInstant::Now();
        SerializeEvents(evs, serializers);
        auto end = TInstant::Now();
        stats.SerializeDuration = end - start;
    }

    {
        auto start = TInstant::Now();
        DeserializeEvents(evs, infos, trash, streams);
        auto end = TInstant::Now();
        stats.DeserializeDuration = end - start;
    }

    for (auto it : trash) {
        delete it;
    }

    return stats;
}

template <std::derived_from<NActors::IEventBase> Event>
void SerDeLoopIteration(Event& ev, size_t iterations) {
    for (size_t i = 0; i < iterations; ++i) {
        NActors::TAllocChunkSerializer serializer;
        ev.SerializeToArcadiaStream(&serializer);
        auto data = serializer.Release(ev.CreateSerializationInfo());
        Event::Load(data.Get());
    }
}

} // namespace

int main() {
    constexpr static size_t kLogMin = 10;
    constexpr static size_t kLogMax = 18;
    constexpr static size_t kIterations = 80;

    TTestStats stats;
    for (size_t k = kLogMin; k < kLogMax; ++k) {
        for (size_t i = 0; i < kIterations; ++i) {
            stats += DoTest<128>(1U << k, [](TEvVPut& ev) {
                Y_UNUSED(ev);
                return 0;
            });
        }
    }
    Cout
    << stats.BytesWritten << " BytesWritten" << Endl
    << stats.EvCount << " EvCount" << Endl
    << stats.SerializeDuration << " SerializeDuration" << Endl
    << stats.DeserializeDuration << " DeserializeDuration" << Endl
    << Endl
    << stats.EvCount / stats.SerializeDuration.MilliSeconds() << " Ev/ms write" << Endl
    << stats.BytesWritten / stats.SerializeDuration.MilliSeconds() << " B/ms write" << Endl
    << stats.EvCount / stats.DeserializeDuration.MilliSeconds() << " Ev/ms read" << Endl
    << stats.BytesWritten / stats.DeserializeDuration.MilliSeconds() << " B/ms read" << Endl
    ;
}
