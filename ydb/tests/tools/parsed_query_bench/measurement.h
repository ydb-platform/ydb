#pragma once

#include <google/protobuf/io/coded_stream.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/system/datetime.h>

#include <chrono>
#include <ctime>

namespace NKikimr::NKqp::NParsedQueryBench {

inline constexpr size_t MaxRequestBytes = 16 << 20;
inline constexpr int MaxRequestRecursionDepth = 256;

template <class T>
void DecodeRequest(const TString& bytes, T& request) {
    Y_ENSURE(bytes.size() <= MaxRequestBytes, "Request exceeds 16 MiB");
    google::protobuf::io::CodedInputStream input(reinterpret_cast<const ui8*>(bytes.data()), bytes.size());
    input.SetRecursionLimit(MaxRequestRecursionDepth);
    Y_ENSURE(request.ParseFromCodedStream(&input) && input.ConsumedEntireMessage(), "Invalid protobuf request");
}

struct TMeasurementTime {
    i64 Wall;
    i64 Cpu;

    static TMeasurementTime Now() {
#ifdef _win_
        const i64 cpu = ThreadCPUTime() * 1000;
#else
        timespec ts;
        Y_ENSURE(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0, "Thread CPU clock unavailable");
        const i64 cpu = i64(ts.tv_sec) * 1'000'000'000 + ts.tv_nsec;
#endif
        const auto wall = std::chrono::steady_clock::now().time_since_epoch();
        return {std::chrono::duration_cast<std::chrono::nanoseconds>(wall).count(), cpu};
    }
};

} // namespace NKikimr::NKqp::NParsedQueryBench
