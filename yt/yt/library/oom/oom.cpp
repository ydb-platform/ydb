#include "oom.h"

#include <thread>
#include <mutex>

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>

#include <library/cpp/yt/assert/assert.h>
#include <library/cpp/yt/logging/logger.h>

#include <yt/yt/library/ytprof/heap_profiler.h>
#include <yt/yt/library/ytprof/profile.h>

#include <util/datetime/base.h>
#include <util/system/file.h>
#include <util/stream/output.h>
#include <util/stream/file.h>
#include <util/string/split.h>
#include <util/system/fs.h>

namespace NYT {

static NYT::NLogging::TLogger Logger{"OOM"};

////////////////////////////////////////////////////////////////////////////////

static const char* TCMallocStats[] = {
    "tcmalloc.per_cpu_caches_active",
    "generic.virtual_memory_used",
    "generic.physical_memory_used",
    "generic.bytes_in_use_by_app",
    "generic.heap_size",
    "tcmalloc.central_cache_free",
    "tcmalloc.cpu_free",
    "tcmalloc.page_heap_free",
    "tcmalloc.page_heap_unmapped",
    "tcmalloc.page_algorithm",
    "tcmalloc.max_total_thread_cache_bytes",
    "tcmalloc.thread_cache_free",
    "tcmalloc.thread_cache_count",
    "tcmalloc.local_bytes",
    "tcmalloc.external_fragmentation_bytes",
    "tcmalloc.metadata_bytes",
    "tcmalloc.transfer_cache_free",
    "tcmalloc.hard_usage_limit_bytes",
    "tcmalloc.desired_usage_limit_bytes",
    "tcmalloc.required_bytes"
};

void OOMWatchdog(TOOMOptions options)
{
    while (true) {
        auto rss = GetProcessMemoryUsage().Rss;

        if (options.MemoryLimit && static_cast<i64>(rss) > *options.MemoryLimit) {
            auto profile = NYTProf::ReadHeapProfile(tcmalloc::ProfileType::kHeap);

            TFileOutput output(options.HeapDumpPath);
            NYTProf::WriteProfile(&output, profile);
            output.Finish();

            auto rctDump = TRefCountedTracker::Get()->GetDebugInfo();
            for (const auto& line : StringSplitter(rctDump).Split('\n')) {
                YT_LOG_DEBUG("RCT %v", line.Token());
            }

            auto parseMemoryAmount = [] (const TStringBuf strValue) {
                const TStringBuf kbSuffix = " kB";
                YT_VERIFY(strValue.EndsWith(kbSuffix));
                auto startPos = strValue.find_first_not_of(' ');
                auto valueString = strValue.substr(
                    startPos,
                    strValue.size() - kbSuffix.size() - startPos);
                return FromString<ui64>(valueString) * 1_KB;
            };

            ui64 rssAnon = 0;
            ui64 rssFile = 0;
            ui64 rssShmem = 0;

            TFileInput statusFile(Format("/proc/self/status"));
            TString line;
            while (statusFile.ReadLine(line)) {
                const TStringBuf rssAnonHeader = "RssAnon:\t";
                if (line.StartsWith(rssAnonHeader)) {
                    rssAnon = parseMemoryAmount(line.substr(rssAnonHeader.size()));
                    continue;
                }

                const TStringBuf rssFileHeader = "RssFile:\t";
                if (line.StartsWith(rssFileHeader)) {
                    rssFile = parseMemoryAmount(line.substr(rssFileHeader.size()));
                    continue;
                }

                const TStringBuf rssShmemHeader = "RssShmem:\t";
                if (line.StartsWith(rssShmemHeader)) {
                    rssShmem = parseMemoryAmount(line.substr(rssShmemHeader.size()));
                    continue;
                }
            }

            YT_LOG_DEBUG("Memory statistis (RssTotal: %v, RssAnon: %v, RssFile %v, RssShmem: %v, TCMalloc: %v)",
                rss,
                rssAnon,
                rssFile,
                rssShmem,
                MakeFormattableView(
                    MakeRange(TCMallocStats),
                    [&] (auto* builder, auto metric) {
                        auto value = tcmalloc::MallocExtension::GetNumericProperty(metric);
                        builder->AppendFormat("%v: %v", metric, value);
                    }));

            YT_LOG_FATAL("Early OOM triggered (MemoryUsage: %v, MemoryLimit: %v, HeapDump: %v, CurrentWorkingDirectory: %v)",
                rss,
                *options.MemoryLimit,
                options.HeapDumpPath,
                NFs::CurrentWorkingDirectory());
        }

        Sleep(TDuration::MilliSeconds(10));
    }
}

void EnableEarlyOOMWatchdog(TOOMOptions options)
{
    static std::once_flag onceFlag;

    std::call_once(onceFlag, [options] {
        std::thread(OOMWatchdog, options).detach();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
