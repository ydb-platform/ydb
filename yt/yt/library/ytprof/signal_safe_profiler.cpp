#include "signal_safe_profiler.h"
#include "symbolize.h"

#if defined(_linux_)
#include <link.h>
#include <sys/syscall.h>
#include <sys/prctl.h>

#include <util/system/yield.h>

#include <csignal>
#include <functional>

#include <util/generic/yexception.h>
#endif

#include <yt/yt/core/misc/proc.h>

#include <library/cpp/yt/cpu_clock/clock.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

TProfileLocation::operator size_t() const
{
    size_t hash = Tid;
    hash = CombineHashes(hash, std::hash<TString>()(ThreadName));

    for (auto ip : Backtrace) {
        hash = CombineHashes(hash, ip);
    }

    for (const auto& tag : Tags) {
        hash = CombineHashes(hash,
            CombineHashes(
                std::hash<TString>{}(tag.first),
                std::hash<std::variant<TString, i64>>{}(tag.second)));
    }

    return hash;
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void* AcquireFiberTagStorage()
{
    return nullptr;
}

Y_WEAK std::vector<std::pair<TString, std::variant<TString, i64>>> ReadFiberTags(void* /* storage */)
{
    return {};
}

Y_WEAK void ReleaseFiberTagStorage(void* /* storage */)
{ }

Y_WEAK TCpuInstant GetTraceContextTimingCheckpoint()
{
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

#if not defined(_linux_)

void TSignalSafeProfiler::Start()
{ }

void TSignalSafeProfiler::Stop()
{ }

NProto::Profile TSignalSafeProfiler::ReadProfile()
{
    return {};
}

void TSignalSafeProfiler::RecordSample(NBacktrace::TFramePointerCursor* /*cursor*/, i64 /*value*/)
{ }

void TSignalSafeProfiler::DequeueSamples()
{ }

#endif

////////////////////////////////////////////////////////////////////////////////

TSignalSafeProfiler::TSignalSafeProfiler(TSignalSafeProfilerOptions options)
    : Options_(options)
    , Queue_(options.RingBufferLogSize)
{ }

TSignalSafeProfiler::~TSignalSafeProfiler()
{
    YT_VERIFY(Stop_);
}

#if defined(_linux_)

void TSignalSafeProfiler::Start()
{
    if (!IsProfileBuild()) {
        throw yexception() << "frame pointers not available; rebuild with --build=profile";
    }

    EnableProfiler();
    Stop_ = false;
    BackgroundThread_ = std::thread([this] {
        DequeueSamples();
    });
}

void TSignalSafeProfiler::Stop()
{
    if (Stop_) {
        return;
    }

    Stop_ = true;
    DisableProfiler();
    BackgroundThread_.join();
}

TCpuDuration GetActionRunTime()
{
    auto fiberStartTime = GetTraceContextTimingCheckpoint();
    if (fiberStartTime == 0) {
        return 0;
    }

    return GetCpuInstant() - fiberStartTime;
}

void TSignalSafeProfiler::RecordSample(NBacktrace::TFramePointerCursor* cursor, i64 value)
{
    int count = 0;
    bool pushTid = false;
    bool pushFiberStorage = false;
    bool pushValue = false;
    bool pushActionRunTime = false;
    int tagIndex = 0;

    auto tagsPtr = GetCpuProfilerTags();

    uintptr_t threadName[2] = {};
    prctl(PR_GET_NAME, (unsigned long)threadName, 0UL, 0UL, 0UL);
    int namePushed = 0;

    auto ok = Queue_.TryPush([&] () -> std::pair<const void*, bool> {
        if (!pushValue) {
            pushValue = true;
            return {reinterpret_cast<const void*>(value), true};
        }

        if (!pushTid) {
            pushTid = true;
            return {reinterpret_cast<void*>(GetCurrentThreadId()), true};
        }

        if (Options_.RecordActionRunTime && !pushActionRunTime) {
            pushActionRunTime = true;
            return {reinterpret_cast<const void*>(GetActionRunTime()), true};
        }

        if (namePushed < 2) {
            return {reinterpret_cast<const void*>(threadName[namePushed++]), true};
        }

        if (!pushFiberStorage) {
            pushFiberStorage = true;
            return {AcquireFiberTagStorage(), true};
        }

        if (tagIndex < MaxActiveTags) {
            if (tagsPtr) {
                auto tag = (*tagsPtr)[tagIndex].GetFromSignal();
                tagIndex++;
                return {reinterpret_cast<const void*>(tag.Release()), true};
            } else {
                tagIndex++;
                return {nullptr, true};
            }
        }

        if (count > Options_.MaxBacktraceSize) {
            return {nullptr, false};
        }

        if (cursor->IsFinished()) {
            return {nullptr, false};
        }

        auto ip = cursor->GetCurrentIP();
        if (count != 0) {
            // First IP points to next executing instruction.
            // All other IP's are return addresses.
            // Subtract 1 to get accurate line information for profiler.
            ip = reinterpret_cast<const void*>(reinterpret_cast<uintptr_t>(ip) - 1);
        }

        cursor->MoveNext();
        count++;
        return {ip, true};
    });

    if (!ok) {
        QueueOverflows_++;
    }
}

void TSignalSafeProfiler::DequeueSamples()
{
    TProfileLocation sample;
    while (!Stop_) {
        Sleep(Options_.DequeuePeriod);

        while (true) {
            sample.Backtrace.clear();
            sample.Tags.clear();

            std::optional<i64> value;
            std::optional<size_t> tid;
            std::optional<TCpuDuration> actionRunTime;
            uintptr_t threadName[2] = {};
            int namePopped = 0;
            std::optional<void*> fiberStorage;

            int tagIndex = 0;
            bool ok = Queue_.TryPop([&] (void* ip) {
                if (!value) {
                    value = reinterpret_cast<i64>(ip);
                    return;
                }

                if (!tid) {
                    tid = reinterpret_cast<size_t>(ip);
                    return;
                }

                if (Options_.RecordActionRunTime && !actionRunTime) {
                    actionRunTime = reinterpret_cast<TCpuDuration>(ip);
                    return;
                }

                if (namePopped < 2) {
                    threadName[namePopped++] = reinterpret_cast<uintptr_t>(ip);
                    return;
                }

                if (!fiberStorage) {
                    fiberStorage = ip;
                    return;
                }

                if (tagIndex < MaxActiveTags) {
                    auto tag = reinterpret_cast<TProfilerTag*>(ip);
                    if (tag) {
                        if (tag->StringValue) {
                            sample.Tags.emplace_back(tag->Name, *tag->StringValue);
                        } else {
                            sample.Tags.emplace_back(tag->Name, *tag->IntValue);
                        }
                    }
                    tagIndex++;
                    return;
                }

                sample.Backtrace.push_back(reinterpret_cast<ui64>(ip));
            });

            if (!ok) {
                break;
            }

            sample.ThreadName = TString{reinterpret_cast<char*>(threadName)};
            sample.Tid = *tid;
            for (auto& tag : ReadFiberTags(*fiberStorage)) {
                sample.Tags.push_back(std::move(tag));
            }
            ReleaseFiberTagStorage(*fiberStorage);

            if (Options_.RecordActionRunTime) {
                sample.Tags.emplace_back(
                    "action_run_time_us",
                    static_cast<i64>(CpuDurationToDuration(*actionRunTime).MicroSeconds()));
            }

            auto& counter = Counters_[sample];
            counter.Count++;
            counter.Total += *value;
        }
    }
}

NProto::Profile TSignalSafeProfiler::ReadProfile()
{
    NProto::Profile profile;
    profile.add_string_table();

    THashMap<TString, ui64> stringTable;
    auto stringify = [&] (const TString& str) -> i64 {
        if (auto it = stringTable.find(str); it != stringTable.end()) {
            return it->second;
        } else {
            auto nameId = profile.string_table_size();
            profile.add_string_table(str);
            stringTable[str] = nameId;
            return nameId;
        }
    };

    AnnotateProfile(&profile, stringify);

    THashMap<uintptr_t, ui64> locations;
    for (const auto& [backtrace, counters] : Counters_) {
        auto sample = profile.add_sample();

        sample->add_value(counters.Count);
        sample->add_value(EncodeValue(counters.Total));

        auto label = sample->add_label();
        label->set_key(stringify("tid"));
        label->set_num(backtrace.Tid);

        label = sample->add_label();
        label->set_key(stringify("thread"));
        label->set_str(stringify(backtrace.ThreadName));

        for (auto tag : backtrace.Tags) {
            auto label = sample->add_label();
            label->set_key(stringify(tag.first));

            if (auto intValue = std::get_if<i64>(&tag.second)) {
                label->set_num(*intValue);
            } else if (auto strValue = std::get_if<TString>(&tag.second)) {
                label->set_str(stringify(*strValue));
            }
        }

        for (auto ip : backtrace.Backtrace) {
            auto it = locations.find(ip);
            if (it != locations.end()) {
                sample->add_location_id(it->second);
                continue;
            }

            auto locationId = locations.size() + 1;

            auto location = profile.add_location();
            location->set_address(ip);
            location->set_id(locationId);

            sample->add_location_id(locationId);
            locations[ip] = locationId;
        }
    }

    if (QueueOverflows_ > 0) {
        profile.add_comment(stringify("ytprof.queue_overflows=" + std::to_string(QueueOverflows_)));
    }

    return profile;
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
