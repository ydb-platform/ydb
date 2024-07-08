#include "monitor.h"
#include "tcmalloc.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/prof/tag.h>
#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/lfalloc/dbg_info/dbg_info.h>
#include <library/cpp/malloc/api/malloc.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/stream/str.h>
#include <util/stream/file.h>


namespace NKikimr {
    using TDynamicCountersPtr = TIntrusivePtr<::NMonitoring::TDynamicCounters>;
    using TDynamicCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

    namespace {
        class TLfAllocMonitor: public IAllocMonitor {
            struct TMemCounters {
                TDynamicCounterPtr Count;
                TDynamicCounterPtr Amount;

                void Init(TDynamicCountersPtr group) {
                    Count = group->GetCounter("count");
                    Amount = group->GetCounter("amount");
                }

                void Set(ui64 count, ui64 amount) {
                    *Count = count;
                    *Amount = amount;
                }
            };

            struct TPerTagCounters {
                THashMap<TString, TMemCounters> BySize;
                TMemCounters Total;
            };

            struct TTagStats {
                struct TBucket {
                    ui64 Count = 0;
                    ui64 Size = 0;

                    void Update(ui64 count, ui64 size) {
                        Count += count;
                        Size += size;
                    }
                };

                TVector<TBucket> Buckets;
                TBucket Total;
            };

        private:
            TDynamicCountersPtr CounterGroup;

            THashMap<TString, TPerTagCounters> PerTag;
            TMemCounters Total;

        public:
            TLfAllocMonitor(TDynamicCountersPtr group) {
                CounterGroup = group->GetSubgroup("component", "lfalloc_profile");
            }

            void Update(TDuration interval) override {
                Y_UNUSED(interval);
#ifdef PROFILE_MEMORY_ALLOCATIONS
                int maxTag = 0;
                int numSizes = 0;
                auto info = NAllocDbg::GetPerTagAllocInfo(true, maxTag, numSizes);

                THashMap<TString, TTagStats> stats;

                auto& totalStats = stats["total"];
                totalStats.Buckets.resize(numSizes);

                // enumerate through all memory tags
                for (int tag = 0; tag < maxTag; ++tag) {
                    auto tagName = NProfiling::GetTag(tag);
                    if (tagName == nullptr) {
                        tagName = "__DEFAULT__";
                    }

                    auto& tagStats = stats[tagName];
                    tagStats.Buckets.resize(numSizes);

                    // enumerate through all sizes of objects
                    for (int sizeIdx = 0; sizeIdx < numSizes; ++sizeIdx) {
                        const auto& entry = info[tag * numSizes + sizeIdx];
                        if (entry.Count <= 0) {
                            continue;
                        }

                        tagStats.Buckets[sizeIdx].Update(entry.Count, entry.Size);
                        tagStats.Total.Update(entry.Count, entry.Size);

                        totalStats.Buckets[sizeIdx].Update(entry.Count, entry.Size);
                        totalStats.Total.Update(entry.Count, entry.Size);
                    }
                }

                // update counters
                for (const auto& [tagName, tagStats] : stats) {
                    const auto& total = tagStats.Total;
                    TPerTagCounters& perTag = PerTag[tagName];

                    if (total.Count == 0 && total.Size == 0 && !perTag.Total.Count) {
                        // Skip the tag that didn't have any allocations so far
                        continue;
                    }

                    auto tagCounters = CounterGroup->GetSubgroup("tag", tagName);
                    if (!perTag.Total.Count) {
                        perTag.Total.Init(tagCounters->GetSubgroup("bucket", "total"));
                    }

                    perTag.Total.Set(total.Count, total.Size);

                    for (int sizeIdx = 0; sizeIdx < numSizes; ++sizeIdx) {
                        const auto sizeName = ToString(sizeIdx);

                        const auto& bucket = tagStats.Buckets[sizeIdx];
                        TMemCounters& bySize = perTag.BySize[sizeName];

                        if (bucket.Count == 0 && bucket.Size == 0 && !bySize.Count) {
                            // Skip the bucket that didn't have any allocations so far
                            continue;
                        }

                        if (!bySize.Count) {
                            bySize.Init(tagCounters->GetSubgroup("bucket", sizeName));
                        }

                        bySize.Set(bucket.Count, bucket.Size);
                    }
                }
#endif
            }

            void Dump(IOutputStream& out, const TString& relPath) override {
                Y_UNUSED(relPath);
#ifdef PROFILE_MEMORY_ALLOCATIONS
                int maxTag = 0;
                int numSizes = 0;
                auto info = NAllocDbg::GetPerTagAllocInfo(true, maxTag, numSizes);

                HTML(out) {
                    TAG(TH3) {
                        out << "LFAlloc" << Endl;
                    }
                    out << "<hr>" << Endl;
                    TABLE_SORTABLE_CLASS("table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {
                                    out << "<span data-toggle='tooltip' "
                                           "title='Allocation Tag'>Tag</span>";
                                }
                                TABLEH() {
                                    out << "<span data-toggle='tooltip' "
                                           "title='Size bucket'>Bucket</span>";
                                }
                                TABLEH() {
                                    out << "<span data-toggle='tooltip' "
                                           "title='Count of allocated objects'>Count</span>";
                                }
                                TABLEH() {
                                    out << "<span data-toggle='tooltip' "
                                           "title='Total amount of allocated RAM space'>"
                                           "Total Space</span>";
                                }
                            }
                        }

                        TABLEBODY() {
                            // enumerate through all memory tags
                            for (int tag = 0; tag < maxTag; ++tag) {
                                auto tagName = NProfiling::GetTag(tag);
                                if (tagName == nullptr) {
                                    tagName = "__DEFAULT__";
                                }

                                size_t totalAmountForTag = 0;
                                size_t totalCountForTag = 0;
                                ui32 tagRecordCount = 0;

                                // enumerate through all sizes of objects
                                for (int sizeIdx = 0; sizeIdx < numSizes; ++sizeIdx) {
                                    const auto& entry = info[tag * numSizes + sizeIdx];
                                    if (entry.Count <= 0) {
                                        continue;
                                    }

                                    totalCountForTag += entry.Count;
                                    totalAmountForTag += entry.Size;
                                    ++tagRecordCount;

                                    TABLER() {
                                        TABLED() {
                                            out << tagName;
                                        }
                                        TABLED() {
                                            out << sizeIdx;
                                        }
                                        TABLED() {
                                            out << entry.Count;
                                        }
                                        TABLED() {
                                            out << entry.Size;
                                        }
                                    }
                                }

                                if (totalCountForTag > 0 && tagRecordCount > 1) {
                                    TABLER() {
                                        TABLED() {
                                            out << tagName;
                                        }
                                        TABLED() {
                                            out << "overall";
                                        }
                                        TABLED() {
                                            out << totalCountForTag;
                                        }
                                        TABLED() {
                                            out << totalAmountForTag;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    HTML_TAG() {
                        out << "<script>$(document).ready(function()"
                               "{$('[data-toggle=\"tooltip\"]').tooltip();}</script>";
                    }
                }
#else
                HTML(out) {
                    TAG(TH3) {
                        out << "LFAlloc" << Endl;
                    }
                    out << "<hr>" << Endl;
                    out << "PROFILE_MEMORY_ALLOCATIONS is off" << Endl;
                }
#endif
            }
        };

        struct TFakeAllocMonitor: public IAllocMonitor {
            void Update(TDuration interval) override {
                Y_UNUSED(interval);
            }

            void Dump(IOutputStream& out, const TString& relPath) override {
                Y_UNUSED(out);
                Y_UNUSED(relPath);
            }
        };

        std::unique_ptr<IAllocMonitor> CreateAllocMonitor(TDynamicCountersPtr group) {
            const auto& info = NMalloc::MallocInfo();
            TStringBuf name(info.Name);

            std::unique_ptr<IAllocMonitor> monitor;
            if (name.StartsWith("lf")) {
                monitor = std::make_unique<TLfAllocMonitor>(std::move(group));
            } else if (name.StartsWith("tc")) {
                monitor = std::move(CreateTcMallocMonitor(std::move(group)));
            }

            return monitor ? std::move(monitor) : std::make_unique<TFakeAllocMonitor>();
        }

        class TMemProfMonitor: public TActorBootstrapped<TMemProfMonitor> {
            struct TDumpLogConfig {
                static constexpr double RssUsageHard = 0.9;
                static constexpr double RssUsageSoft = 0.85;
                static constexpr TDuration RepeatInterval = TDuration::Seconds(10);
                static constexpr TDuration DumpInterval = TDuration::Minutes(10);
            };

            enum {
                EvDumpLogStats = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvEnd
            };

            struct TEvDumpLogStats : public TEventLocal<TEvDumpLogStats, EvDumpLogStats> {};

            const TDuration Interval;
            const std::unique_ptr<IAllocMonitor> AllocMonitor;
            const TString FilePathPrefix;

            TInstant LogMemoryStatsTime = TInstant::Now() - TDumpLogConfig::DumpInterval;

            bool IsDangerous = false;

        public:
            static constexpr EActivityType ActorActivityType() {
                return EActivityType::ACTORLIB_STATS;
            }

            TMemProfMonitor(TDuration interval, std::unique_ptr<IAllocMonitor> allocMonitor, const TString& filePathPrefix)
                : Interval(interval)
                , AllocMonitor(std::move(allocMonitor))
                , FilePathPrefix(filePathPrefix)
            {}

            void Bootstrap(const TActorContext& ctx) {
                NActors::TMon* mon = AppData(ctx)->Mon;
                if (!mon) {
                    LOG_ERROR(ctx, NKikimrServices::MEMORY_PROFILER,
                              "Could not register actor page, 'mon' is null");
                    Die(ctx);
                    return;
                }

                LOG_NOTICE_S(ctx, NKikimrServices::MEMORY_PROFILER, "Bootstrapped");

                auto* indexPage = mon->RegisterIndexPage("memory", "Memory");
                mon->RegisterActorPage(
                    indexPage, "statistics", "Statistics",
                    false, ctx.ExecutorThread.ActorSystem, ctx.SelfID);

                AllocMonitor->RegisterPages(mon, ctx.ExecutorThread.ActorSystem, ctx.SelfID);
                AllocMonitor->RegisterControls(AppData(ctx)->Icb);

                Become(&TThis::StateWork);
                ctx.Schedule(Interval, new TEvents::TEvWakeup());
            }

        private:
            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    CFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
                    HFunc(TEvDumpLogStats, HandleDump);
                    HFunc(NMon::TEvHttpInfo, HandleHttpInfo);
                }
            }

            void LogMemoryStats(const TActorContext& ctx, size_t limit) noexcept {
                LogMemoryStatsTime = TInstant::Now();

                if (FilePathPrefix) {
                    try {
                        TString name = LogMemoryStatsTime.ToStringUpToSeconds();
                        SubstGlobal(name, ':', '-');
                        TString fileName = FilePathPrefix + name + ".mem";
                        TFileOutput out(fileName);
                        AllocMonitor->DumpForLog(out, limit);
                        LOG_WARN_S(ctx, NKikimrServices::MEMORY_PROFILER, "Memory stats saved to " + fileName);
                    } catch (const std::exception& err) {
                        LOG_WARN_S(ctx, NKikimrServices::MEMORY_PROFILER, err.what());
                    }
                } else {
                    TStringStream out;
                    AllocMonitor->DumpForLog(out, limit);
                    TVector<TString> split;
                    Split(out.Str(), "\n", split);
                    for (const auto& line : split) {
                        LOG_WARN_S(ctx, NKikimrServices::MEMORY_PROFILER, line);
                    }
                }
            }

            void LogMemoryStatsIfNeeded(const TActorContext& ctx, TMemoryUsage memoryUsage) noexcept {
                auto usage = memoryUsage.Usage();
                LOG_DEBUG_S(ctx, NKikimrServices::MEMORY_PROFILER, memoryUsage.ToString());
                if (IsDangerous && usage < TDumpLogConfig::RssUsageSoft) {
                    IsDangerous = false;
                } else if (!IsDangerous && usage > TDumpLogConfig::RssUsageHard) {
                    if (TInstant::Now() - LogMemoryStatsTime > TDumpLogConfig::DumpInterval) {
                        IsDangerous = true;
                        LogMemoryStats(ctx, 256);
                        ctx.Schedule(TDumpLogConfig::RepeatInterval, new TEvDumpLogStats);
                    }
                }
            }

            void HandleWakeup(const TActorContext& ctx) noexcept {
                AllocMonitor->Update(Interval);

                std::optional<TMemoryUsage> memoryUsage = TAllocState::TryGetMemoryUsage();
                if (memoryUsage) {
                    LogMemoryStatsIfNeeded(ctx, memoryUsage.value());
                }
                
                ctx.Schedule(Interval, new TEvents::TEvWakeup());
            }

            void HandleDump(TEvDumpLogStats::TPtr&, const TActorContext& ctx) noexcept {
                if (IsDangerous) {
                    std::optional<TMemoryUsage> memoryUsage = TAllocState::TryGetMemoryUsage();
                    if (memoryUsage) {
                        LOG_WARN_S(ctx, NKikimrServices::MEMORY_PROFILER, memoryUsage->ToString());
                        LogMemoryStats(ctx, 256);
                    }
                }
            }

            void HandleHttpInfo(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) noexcept {
                TString action = ev->Get()->Request.GetParams().Get("action");
                TString relPath(ev->Get()->Request.GetPath());
                TStringStream out;
                if (action == "log" && relPath == "/memory/heap") {
                    LogMemoryStats(ctx, 2048);
                    out << "<p>Output dumped to log</p>"
                        << "<p><a href=\"/memory/heap\">Return</a></p>\n";
                } else {
                    AllocMonitor->Dump(out, relPath);
                }
                ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(out.Str()));
            }
        };
    }

    IActor* CreateMemProfMonitor(ui32 intervalSec, TDynamicCountersPtr counters, const TString& filePathPrefix) {
        return new TMemProfMonitor(
            TDuration::Seconds(intervalSec),
            CreateAllocMonitor(GetServiceCounters(counters, "utils")),
            filePathPrefix);
    }
}
