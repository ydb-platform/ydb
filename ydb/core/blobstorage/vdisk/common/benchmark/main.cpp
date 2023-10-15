#include "util/generic/yexception.h"
#include "util/stream/input.h"
#include "util/stream/output.h"
#include "ydb/core/protos/blobstorage.pb.h"
#include <cstring>
#include <util/system/yassert.h>
#include <library/cpp/testing/benchmark/bench.h>

#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

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
void SerDeLoop(Event& ev, Buffer<bytes>& buffer, size_t iterations) {
    for (size_t i = 0; i < iterations; ++i) {
        ev.Save(&buffer);

        ev.Load(&buffer);
        buffer.Flush();
        ev.Load(&buffer);
        buffer.Flush();
        ev.Load(&buffer);
        buffer.Flush();
    }
}

} // namespace

Y_CPU_BENCHMARK(Put, info) {
    NKikimrBlobStorage::TEvVPut evPut;
    Buffer<2048> buf;
    SerDeLoop(evPut, buf, info.Iterations());
}

Y_CPU_BENCHMARK(Get, info) {
    NKikimrBlobStorage::TEvVGet evGet;
    Buffer<2048> buf;
    SerDeLoop(evGet, buf, info.Iterations());
}
