#pragma once

#include <library/cpp/json/json_value.h>
#include <util/system/types.h>

#include <array>
#include <cerrno>

#ifdef __linux__
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace NKikimr {

// Benchmark instrumentation only. All syscalls are outside the wall/CPU timer;
// the counters exclude kernel and hypervisor execution and support multiplexing.
class TBenchCounters {
#ifdef __linux__
    struct TCounter {
        const char* Name;
        ui32 Type;
        ui64 Config;
        int Fd = -1;
        int Error = 0;
        double Total = 0;
        ui64 PreviousEnabled = 0;
        ui64 PreviousRunning = 0;
    };

    std::array<TCounter, 8> Counters{{
        {"cycles", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES},
        {"instructions", PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS},
        {"reference_cycles", PERF_TYPE_HARDWARE, PERF_COUNT_HW_REF_CPU_CYCLES},
        {"cache_misses", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES},
        {"branches", PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_INSTRUCTIONS},
        {"branch_misses", PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES},
        {"context_switches", PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CONTEXT_SWITCHES},
        {"cpu_migrations", PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CPU_MIGRATIONS},
    }};
#endif

public:
    TBenchCounters() {
#ifdef __linux__
        for (auto& counter : Counters) {
            perf_event_attr attr{};
            attr.size = sizeof(attr);
            attr.type = counter.Type;
            attr.config = counter.Config;
            attr.disabled = 1;
            attr.exclude_kernel = 1;
            attr.exclude_hv = 1;
            attr.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
            counter.Fd = syscall(SYS_perf_event_open, &attr, 0, -1, -1, 0);
            if (counter.Fd < 0) counter.Error = errno;
        }
#endif
    }

    ~TBenchCounters() {
#ifdef __linux__
        for (auto& counter : Counters) if (counter.Fd >= 0) close(counter.Fd);
#endif
    }

    void Start() {
#ifdef __linux__
        for (auto& counter : Counters) {
            if (counter.Fd >= 0 && !counter.Error) {
                if (ioctl(counter.Fd, PERF_EVENT_IOC_RESET, 0) || ioctl(counter.Fd, PERF_EVENT_IOC_ENABLE, 0)) {
                    counter.Error = errno;
                }
            }
        }
#endif
    }

    void Stop() {
#ifdef __linux__
        for (auto& counter : Counters) {
            if (counter.Fd >= 0 && !counter.Error) {
                struct { ui64 Value, Enabled, Running; } result{};
                if (ioctl(counter.Fd, PERF_EVENT_IOC_DISABLE, 0)
                    || read(counter.Fd, &result, sizeof(result)) != sizeof(result)) {
                    counter.Error = errno ? errno : EIO;
                } else if (result.Running > counter.PreviousRunning) {
                    counter.Total += double(result.Value) * (result.Enabled - counter.PreviousEnabled)
                        / (result.Running - counter.PreviousRunning);
                    counter.PreviousEnabled = result.Enabled;
                    counter.PreviousRunning = result.Running;
                } else {
                    counter.Error = EBUSY;
                }
            }
        }
#endif
    }

    NJson::TJsonValue Report(ui64 iterations, size_t logicalBytes) const {
        NJson::TJsonValue report;
#ifdef __linux__
        for (const auto& counter : Counters) {
            auto& value = report[counter.Name];
            if (counter.Error) value["errno"] = counter.Error;
            else {
                value["total_scaled"] = counter.Total;
                value["per_blob"] = counter.Total / iterations;
                value["per_logical_byte"] = counter.Total / iterations / logicalBytes;
            }
        }
#endif
        return report;
    }
};

} // namespace NKikimr
