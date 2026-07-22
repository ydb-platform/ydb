#include "cgroup_v2.h"

#include "cgroup_events.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/strip.h>

namespace NActors {

    namespace {

        std::optional<TString> TryReadFile(const TFsPath& path) {
            try {
                return TFileInput(path.GetPath()).ReadAll();
            } catch (...) {
                return std::nullopt;
            }
        }

        std::optional<ui64> TryReadUInt(const TFsPath& path) {
            auto content = TryReadFile(path);
            if (!content) {
                return std::nullopt;
            }

            ui64 value = 0;
            if (!TryFromString(StripString(*content), value)) {
                return std::nullopt;
            }
            return value;
        }

        std::optional<TCGroupV2Limit> TryReadLimit(const TFsPath& path) {
            auto content = TryReadFile(path);
            if (!content) {
                return std::nullopt;
            }

            const TString value = StripString(*content);
            if (value == "max") {
                return TCGroupV2Limit{.Unlimited = true};
            }

            ui64 parsed = 0;
            if (!TryFromString(value, parsed)) {
                return std::nullopt;
            }
            return TCGroupV2Limit{.Value = parsed};
        }

        template<class TCallback>
        void ParseKeyValueFile(const TString& content, TCallback&& callback) {
            TStringInput input(content);
            TString line;
            while (input.ReadLine(line)) {
                TVector<TString> fields;
                StringSplitter(line).Split(' ').SkipEmpty().Collect(&fields);
                if (fields.size() != 2) {
                    continue;
                }

                ui64 value = 0;
                if (TryFromString(fields[1], value)) {
                    callback(TStringBuf(fields[0]), value);
                }
            }
        }

        std::optional<TString> FindUnifiedCGroupPath(const TString& procSelfCGroup) {
            auto content = TryReadFile(procSelfCGroup);
            if (!content) {
                return std::nullopt;
            }

            TStringInput input(*content);
            TString line;
            while (input.ReadLine(line)) {
                if (!line.StartsWith("0::")) {
                    continue;
                }

                TString path = line.substr(3);
                if (path.empty()) {
                    path = "/";
                } else if (path.front() != '/') {
                    path.prepend('/');
                }
                return path;
            }
            return std::nullopt;
        }

        TFsPath MakeCGroupDirectory(const TString& root, const TString& cGroupPath) {
            if (cGroupPath == "/") {
                return TFsPath(root);
            }
            return TFsPath(root) / cGroupPath.substr(1);
        }

        std::optional<TCGroupV2CpuStats> ReadCpuStats(const TFsPath& directory) {
            auto content = TryReadFile(directory / "cpu.stat");
            if (!content) {
                return std::nullopt;
            }

            TCGroupV2CpuStats result;
            ParseKeyValueFile(*content, [&result](TStringBuf key, ui64 value) {
                if (key == "usage_usec") {
                    result.UsageUsec = value;
                } else if (key == "user_usec") {
                    result.UserUsec = value;
                } else if (key == "system_usec") {
                    result.SystemUsec = value;
                } else if (key == "nr_periods") {
                    result.NrPeriods = value;
                } else if (key == "nr_throttled") {
                    result.NrThrottled = value;
                } else if (key == "throttled_usec") {
                    result.ThrottledUsec = value;
                } else if (key == "nr_bursts") {
                    result.NrBursts = value;
                } else if (key == "burst_usec") {
                    result.BurstUsec = value;
                }
            });

            if (auto max = TryReadFile(directory / "cpu.max")) {
                TVector<TString> fields;
                StringSplitter(StripString(*max)).Split(' ').SkipEmpty().Collect(&fields);
                if (fields.size() == 2) {
                    if (fields[0] == "max") {
                        result.QuotaUsec = TCGroupV2Limit{.Unlimited = true};
                    } else {
                        ui64 quota = 0;
                        if (TryFromString(fields[0], quota)) {
                            result.QuotaUsec = TCGroupV2Limit{.Value = quota};
                        }
                    }

                    ui64 period = 0;
                    if (TryFromString(fields[1], period)) {
                        result.PeriodUsec = period;
                    }
                }
            }

            return result;
        }

        std::optional<TCGroupV2MemoryStats> ReadMemoryStats(const TFsPath& directory) {
            auto current = TryReadUInt(directory / "memory.current");
            if (!current) {
                return std::nullopt;
            }

            TCGroupV2MemoryStats result;
            result.CurrentBytes = *current;
            result.PeakBytes = TryReadUInt(directory / "memory.peak");
            result.MaxBytes = TryReadLimit(directory / "memory.max");
            result.HighBytes = TryReadLimit(directory / "memory.high");
            result.SwapCurrentBytes = TryReadUInt(directory / "memory.swap.current");
            result.SwapMaxBytes = TryReadLimit(directory / "memory.swap.max");

            if (auto stat = TryReadFile(directory / "memory.stat")) {
                ParseKeyValueFile(*stat, [&result](TStringBuf key, ui64 value) {
                    if (key == "anon") {
                        result.AnonBytes = value;
                    } else if (key == "file") {
                        result.FileBytes = value;
                    } else if (key == "kernel") {
                        result.KernelBytes = value;
                    }
                });
            }

            if (auto events = TryReadFile(directory / "memory.events")) {
                ParseKeyValueFile(*events, [&result](TStringBuf key, ui64 value) {
                    if (key == "low") {
                        result.LowEvents = value;
                    } else if (key == "high") {
                        result.HighEvents = value;
                    } else if (key == "max") {
                        result.MaxEvents = value;
                    } else if (key == "oom") {
                        result.OomEvents = value;
                    } else if (key == "oom_kill") {
                        result.OomKillEvents = value;
                    } else if (key == "oom_group_kill") {
                        result.OomGroupKillEvents = value;
                    } else if (key == "sock_throttled") {
                        result.SockThrottledEvents = value;
                    }
                });
            }

            return result;
        }

        std::optional<TCGroupV2IoStats> ReadIoStats(const TFsPath& directory) {
            auto content = TryReadFile(directory / "io.stat");
            if (!content) {
                return std::nullopt;
            }

            TCGroupV2IoStats result;
            TStringInput input(*content);
            TString line;
            while (input.ReadLine(line)) {
                TVector<TString> fields;
                StringSplitter(line).Split(' ').SkipEmpty().Collect(&fields);
                if (fields.empty()) {
                    continue;
                }

                TCGroupV2IoDeviceStats device;
                device.Device = fields[0];
                for (size_t index = 1; index < fields.size(); ++index) {
                    const size_t separator = fields[index].find('=');
                    if (separator == TString::npos) {
                        continue;
                    }

                    const TStringBuf key(fields[index].data(), separator);
                    ui64 value = 0;
                    if (!TryFromString(TStringBuf(fields[index]).SubStr(separator + 1), value)) {
                        continue;
                    }

                    if (key == "rbytes") {
                        device.ReadBytes = value;
                    } else if (key == "wbytes") {
                        device.WriteBytes = value;
                    } else if (key == "rios") {
                        device.ReadOperations = value;
                    } else if (key == "wios") {
                        device.WriteOperations = value;
                    } else if (key == "dbytes") {
                        device.DiscardBytes = value;
                    } else if (key == "dios") {
                        device.DiscardOperations = value;
                    }
                }

                result.ReadBytes += device.ReadBytes;
                result.WriteBytes += device.WriteBytes;
                result.ReadOperations += device.ReadOperations;
                result.WriteOperations += device.WriteOperations;
                result.DiscardBytes += device.DiscardBytes;
                result.DiscardOperations += device.DiscardOperations;
                result.Devices.push_back(std::move(device));
            }

            return result;
        }

        std::optional<TCGroupV2PidsStats> ReadPidsStats(const TFsPath& directory) {
            auto current = TryReadUInt(directory / "pids.current");
            if (!current) {
                return std::nullopt;
            }

            TCGroupV2PidsStats result;
            result.Current = *current;
            result.Peak = TryReadUInt(directory / "pids.peak");
            result.Max = TryReadLimit(directory / "pids.max");
            if (auto events = TryReadFile(directory / "pids.events")) {
                ParseKeyValueFile(*events, [&result](TStringBuf key, ui64 value) {
                    if (key == "max") {
                        result.MaxEvents = value;
                    }
                });
            }
            return result;
        }

        std::optional<TCGroupV2PressureStats> ReadPressureStats(const TFsPath& path) {
            auto content = TryReadFile(path);
            if (!content) {
                return std::nullopt;
            }

            TCGroupV2PressureStats result;
            TStringInput input(*content);
            TString line;
            while (input.ReadLine(line)) {
                TVector<TString> fields;
                StringSplitter(line).Split(' ').SkipEmpty().Collect(&fields);
                if (fields.empty() || (fields[0] != "some" && fields[0] != "full")) {
                    continue;
                }

                TCGroupV2PressureLine pressure;
                bool parsed = false;
                for (size_t index = 1; index < fields.size(); ++index) {
                    const size_t separator = fields[index].find('=');
                    if (separator == TString::npos) {
                        continue;
                    }

                    const TStringBuf key(fields[index].data(), separator);
                    const TStringBuf value = TStringBuf(fields[index]).SubStr(separator + 1);
                    if (key == "avg10") {
                        parsed |= TryFromString(value, pressure.Avg10);
                    } else if (key == "avg60") {
                        parsed |= TryFromString(value, pressure.Avg60);
                    } else if (key == "avg300") {
                        parsed |= TryFromString(value, pressure.Avg300);
                    } else if (key == "total") {
                        parsed |= TryFromString(value, pressure.TotalUsec);
                    }
                }

                if (parsed) {
                    if (fields[0] == "some") {
                        result.Some = pressure;
                    } else {
                        result.Full = pressure;
                    }
                }
            }

            return result;
        }

        std::optional<TCGroupV2Stats> ReadCGroupV2Stats(const TCGroupV2StatsConfig& config) {
            auto cGroupPath = FindUnifiedCGroupPath(config.ProcSelfCGroup);
            if (!cGroupPath) {
                return std::nullopt;
            }

            const TFsPath directory = MakeCGroupDirectory(config.CGroupRoot, *cGroupPath);
            TCGroupV2Stats result;
            result.CGroupPath = *cGroupPath;
            result.Cpu = ReadCpuStats(directory);
            result.Memory = ReadMemoryStats(directory);
            result.Io = ReadIoStats(directory);
            result.Pids = ReadPidsStats(directory);
            result.CpuPressure = ReadPressureStats(directory / "cpu.pressure");
            result.MemoryPressure = ReadPressureStats(directory / "memory.pressure");
            result.IoPressure = ReadPressureStats(directory / "io.pressure");
            return result;
        }

        TCGroupMemoryStatsPtr MakeMemoryStats(const TCGroupV2StatsPtr& stats) {
            if (!stats || !stats->Memory) {
                return nullptr;
            }

            const auto& source = *stats->Memory;
            TCGroupMemoryStats result;
            result.Version = ECGroupVersion::V2;
            result.CGroupPath = stats->CGroupPath;
            result.CurrentBytes = source.CurrentBytes;
            result.PeakBytes = source.PeakBytes;
            result.MaxBytes = source.MaxBytes;
            result.HighBytes = source.HighBytes;
            result.SwapCurrentBytes = source.SwapCurrentBytes;
            result.SwapMaxBytes = source.SwapMaxBytes;
            result.AnonBytes = source.AnonBytes;
            result.FileBytes = source.FileBytes;
            result.KernelBytes = source.KernelBytes;
            result.HighEvents = source.HighEvents;
            result.MaxEvents = source.MaxEvents;
            if (stats->MemoryPressure && stats->MemoryPressure->Full) {
                result.PressureFullAvg10 = stats->MemoryPressure->Full->Avg10;
            }
            return std::make_shared<const TCGroupMemoryStats>(std::move(result));
        }

        enum : ui32 {
            EvReadStats = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        struct TEvReadStats : TEventLocal<TEvReadStats, EvReadStats> {
            TActorId Recipient;
            ui64 Cookie = 0;
            bool MemoryOnly = false;

            TEvReadStats(TActorId recipient, ui64 cookie, bool memoryOnly)
                : Recipient(recipient)
                , Cookie(cookie)
                , MemoryOnly(memoryOnly)
            {
            }
        };

        class TCGroupV2StatsActor : public TActorBootstrapped<TCGroupV2StatsActor> {
        public:
            explicit TCGroupV2StatsActor(TCGroupV2StatsConfig config)
                : Config(std::move(config))
            {
            }

            void Bootstrap() {
                Become(&TThis::StateWork);
            }

            STRICT_STFUNC(StateWork,
                hFunc(TEvReadStats, Handle)
                cFunc(TEvents::TSystem::Poison, PassAway)
            )

        private:
            void Handle(TEvReadStats::TPtr& ev) {
                const TMonotonic now = TActivationContext::Monotonic();
                if (!CacheInitialized || Config.RefreshPeriod == TDuration::Zero() || now >= NextRefresh) {
                    try {
                        auto stats = ReadCGroupV2Stats(Config);
                        CachedStats = stats
                            ? std::make_shared<const TCGroupV2Stats>(std::move(*stats))
                            : nullptr;
                        CacheInitialized = true;
                        NextRefresh = now + Config.RefreshPeriod;
                    } catch (...) {
                        Reply(*ev->Get(), nullptr);
                        return;
                    }
                }

                Reply(*ev->Get(), CachedStats);
            }

            void Reply(const TEvReadStats& request, TCGroupV2StatsPtr stats) {
                if (request.MemoryOnly) {
                    Send(request.Recipient,
                        new TEvCGroupMemoryStats(MakeMemoryStats(stats)),
                        0,
                        request.Cookie);
                } else {
                    Send(request.Recipient,
                        new TEvCGroupV2Stats(std::move(stats)),
                        0,
                        request.Cookie);
                }
            }

        private:
            const TCGroupV2StatsConfig Config;
            TCGroupV2StatsPtr CachedStats;
            TMonotonic NextRefresh;
            bool CacheInitialized = false;
        };

    } // namespace

    TCGroupV2StatsSubSystem::TCGroupV2StatsSubSystem(TCGroupV2StatsConfig config)
        : Config(std::move(config))
    {
        Y_ABORT_UNLESS(Config.RefreshPeriod >= TDuration::Zero(),
            "cgroup v2 stats refresh period must not be negative");
    }

    void TCGroupV2StatsSubSystem::ReadStats(const TActorId& recipient, ui64 cookie) const {
        Y_ABORT_UNLESS(recipient, "cannot send cgroup v2 stats to an empty actor id");
        TActorSystem* actorSystem = nullptr;
        TActorId readerActorId;
        {
            TGuard<TMutex> guard(Mutex);
            Y_ABORT_UNLESS(ActorSystem, "cgroup v2 stats subsystem is not running");
            actorSystem = ActorSystem;
            if (!Stopping) {
                readerActorId = ReaderActorId;
            }
        }

        if (!readerActorId || !actorSystem->Send(readerActorId,
                new TEvReadStats(recipient, cookie, false))) {
            actorSystem->Send(recipient, new TEvCGroupV2Stats(nullptr), 0, cookie);
        }
    }

    void TCGroupV2StatsSubSystem::ReadMemoryStats(
            const TActorId& recipient,
            ui64 cookie) const {
        Y_ABORT_UNLESS(recipient, "cannot send cgroup v2 memory stats to an empty actor id");
        TActorSystem* actorSystem = nullptr;
        TActorId readerActorId;
        {
            TGuard<TMutex> guard(Mutex);
            Y_ABORT_UNLESS(ActorSystem, "cgroup v2 stats subsystem is not running");
            actorSystem = ActorSystem;
            if (!Stopping) {
                readerActorId = ReaderActorId;
            }
        }

        if (!readerActorId || !actorSystem->Send(readerActorId,
                new TEvReadStats(recipient, cookie, true))) {
            actorSystem->Send(recipient, new TEvCGroupMemoryStats(nullptr), 0, cookie);
        }
    }

    void TCGroupV2StatsSubSystem::OnAfterStart(TActorSystem& actorSystem) {
        const TActorId readerActorId = actorSystem.Register(
            new TCGroupV2StatsActor(Config), TMailboxType::Simple, Config.ExecutorPoolId);

        TGuard<TMutex> guard(Mutex);
        ActorSystem = &actorSystem;
        ReaderActorId = readerActorId;
        Stopping = false;
    }

    void TCGroupV2StatsSubSystem::OnBeforeStop(TActorSystem& actorSystem) {
        TActorId readerActorId;
        {
            TGuard<TMutex> guard(Mutex);
            Stopping = true;
            readerActorId = ReaderActorId;
        }
        if (readerActorId) {
            actorSystem.Send(readerActorId, new TEvents::TEvPoison());
        }
    }

    void TCGroupV2StatsSubSystem::OnAfterStop(TActorSystem&) {
        TGuard<TMutex> guard(Mutex);
        ActorSystem = nullptr;
        ReaderActorId = {};
    }

    std::unique_ptr<TCGroupV2StatsSubSystem> MakeCGroupV2StatsSubSystem(TCGroupV2StatsConfig config) {
        return std::make_unique<TCGroupV2StatsSubSystem>(std::move(config));
    }

    const TCGroupV2StatsSubSystem& GetCGroupV2StatsSubSystem(const TActorSystem& actorSystem) {
        auto* subSystem = actorSystem.GetSubSystem<TCGroupV2StatsSubSystem>();
        Y_ABORT_UNLESS(subSystem, "cgroup v2 stats subsystem is not registered");
        return *subSystem;
    }

    const TCGroupV2StatsSubSystem& GetCGroupV2StatsSubSystem() {
        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        auto* subSystem = actorSystem->GetSubSystem<TCGroupV2StatsSubSystem>();
        Y_ABORT_UNLESS(subSystem, "cgroup v2 stats subsystem is not registered");
        return *subSystem;
    }

} // namespace NActors
