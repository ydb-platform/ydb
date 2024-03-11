#include "profiler.h"
#include "tcmalloc.h"

#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/prof/tag.h>
#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/malloc/api/malloc.h>
#include <library/cpp/monlib/service/pages/templates.h>

#if defined(PROFILE_MEMORY_ALLOCATIONS)
#include <library/cpp/lfalloc/alloc_profiler/profiler.h>
#endif

#if defined(_linux_) && !defined(WITH_VALGRIND)
#define EXEC_PROFILER_ENABLED
#include <library/cpp/execprofile/profile.h>
#endif

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/printf.h>

#include <stdio.h>

namespace NActors {
    using TDynamicCountersPtr = TIntrusivePtr<::NMonitoring::TDynamicCounters>;
    using TDynamicCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

    namespace {

#if defined(PROFILE_MEMORY_ALLOCATIONS)
        struct TAllocDumper : NAllocProfiler::TAllocationStatsDumper {
            using NAllocProfiler::TAllocationStatsDumper::TAllocationStatsDumper;

            TString FormatTag(int tag) override {
                auto tagName = NProfiling::GetTag(tag);
                return tagName ? tagName : "__DEFAULT__";
            }
        };

        class TLfAllocProfiler: public IProfilerLogic {
        public:
            void Start() override {
                NAllocProfiler::StartAllocationSampling(true);
            }

            void Stop(IOutputStream& out, size_t limit, bool forLog) override {
                Y_UNUSED(forLog);
                TAllocDumper dumper(out);
                NAllocProfiler::StopAllocationSampling(dumper, limit);
            }
        };

#endif // PROFILE_MEMORY_ALLOCATIONS

#if defined(EXEC_PROFILER_ENABLED)
        class TExecProfiler: public IProfilerLogic {
        public:
            void Start() override {
                ResetProfile();
                BeginProfiling();
            }

            void Stop(IOutputStream& out, size_t limit, bool forLog) override {
                Y_UNUSED(limit);
                Y_UNUSED(forLog);

                char* buf = nullptr;
                size_t len = 0;
                FILE* stream = open_memstream(&buf, &len);
                Y_ABORT_UNLESS(stream);

                EndProfiling(stream);
                fflush(stream);
                fclose(stream);

                out.Write(buf, len);
                free(buf);
            }
        };
#endif // EXEC_PROFILER_ENABLED

        struct TFakeProfiler: public IProfilerLogic {
            void Start() override {
            }

            void Stop(IOutputStream& out, size_t limit, bool forLog) override {
                Y_UNUSED(out);
                Y_UNUSED(limit);
                Y_UNUSED(forLog);
            }
        };

        std::unique_ptr<IProfilerLogic> CreateProfiler() {
            const auto& info = NMalloc::MallocInfo();
            TStringBuf name(info.Name);

            std::unique_ptr<IProfilerLogic> profiler;

#if defined(PROFILE_MEMORY_ALLOCATIONS)
            if (name.StartsWith("lf")) {
                profiler = std::make_unique<TLfAllocProfiler>();
            }
#endif // PROFILE_MEMORY_ALLOCATIONS

            if (name.StartsWith("tc")) {
                profiler = std::move(NKikimr::CreateTcMallocProfiler());
            }

            if (profiler) {
                return std::move(profiler);
            }

#if defined(EXEC_PROFILER_ENABLED)
            return std::make_unique<TExecProfiler>();
#endif // EXEC_PROFILER_ENABLED

            return std::make_unique<TFakeProfiler>();
        }

        class TProfilerActor: public TActor<TProfilerActor> {
            struct TCounters {
                TDynamicCounterPtr IsProfiling;
            };

        private:
            const TDynamicCountersPtr DynamicCounters;
            const TString Dir;
            const std::unique_ptr<IProfilerLogic> Profiler;

            TCounters Counters;
            bool IsProfiling = false;
            ui32 Count = 0;
            TInstant StartTime;
            TInstant StopTime;

        public:
            static constexpr EActivityType ActorActivityType() {
                return EActivityType::ACTORLIB_STATS;
            }

            TProfilerActor(TDynamicCountersPtr counters, TString dir, std::unique_ptr<IProfilerLogic> profiler)
                : TActor(&TThis::StateWork)
                , DynamicCounters(std::move(counters))
                , Dir(std::move(dir))
                , Profiler(std::move(profiler))
            {
                if (DynamicCounters) {
                    Counters.IsProfiling = DynamicCounters->GetCounter("IsProfiling");
                }
            }

        private:
            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvProfiler::TEvStart, HandleStart);
                    HFunc(TEvProfiler::TEvStop, HandleStop);
                    HFunc(NMon::TEvHttpInfo, HandleMonInfo);
                }
            }

            void DumpProfilingTimes(IOutputStream& out);

            void HandleStart(TEvProfiler::TEvStart::TPtr& ev, const TActorContext& ctx);
            void HandleStop(TEvProfiler::TEvStop::TPtr& ev, const TActorContext& ctx);
            void HandleMonInfo(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx);

            bool StartProfiler();
            bool StopProfiler(TString& profile);
            bool StopProfilerFile(TString& outFileName, TString& err);
            bool StopProfilerDumpToLog(const TActorContext& ctx);

            void OutputControlHtml(IOutputStream& os, const TString& action, bool isOk);
            void OutputResultHtml(IOutputStream& os, const TString& action, const TString& profile, const TString& fileName, bool isOk, const TString& err);
        };

        void TProfilerActor::DumpProfilingTimes(IOutputStream& out) {
            out << "Profiling started: " << StartTime << Endl
                << "Profiling stopped: " << StopTime << Endl
                << Endl;
        }

        void TProfilerActor::HandleStart(TEvProfiler::TEvStart::TPtr& ev, const NActors::TActorContext& ctx) {
            bool isOk = StartProfiler();
            ctx.Send(ev->Sender, new TEvProfiler::TEvStartResult(ev->Get()->Cookie(), isOk));
        }

        void TProfilerActor::HandleStop(TEvProfiler::TEvStop::TPtr& ev, const NActors::TActorContext& ctx) {
            TString result;
            bool isOk = StopProfiler(result);
            ctx.Send(ev->Sender, new TEvProfiler::TEvStopResult(ev->Get()->Cookie(), result, isOk));
        }

        void TProfilerActor::HandleMonInfo(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
            TString action = ev->Get()->Request.GetParams().Get("action");
            TString profile;
            TString fileName;
            TString err;

            bool isOk = true;
            if (action == "start") {
                isOk = StartProfiler();
            } else if (action == "stop-display") {
                isOk = StopProfiler(profile);
            } else if (action == "stop-save") {
                isOk = StopProfilerFile(fileName, err);
            } else if (action == "stop-log") {
                isOk = StopProfilerDumpToLog(ctx);
            }

            TStringStream out;
            HTML(out) {
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-12") {
                        OutputControlHtml(out, action, isOk);
                    }
                }
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-12") {
                        OutputResultHtml(out, action, profile, fileName, isOk, err);
                    }
                }
            }

            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(out.Str()));
        }

        bool TProfilerActor::StartProfiler() {
            if (IsProfiling) {
                return false;
            }

            IsProfiling = true;
            StartTime = TInstant::Now();
            if (DynamicCounters) {
                *Counters.IsProfiling = 1;
            }

            Profiler->Start();
            return true;
        }

        bool TProfilerActor::StopProfiler(TString& profile) {
            if (!IsProfiling) {
                profile.clear();
                return false;
            }

            IsProfiling = false;
            StopTime = TInstant::Now();
            if (DynamicCounters) {
                *Counters.IsProfiling = 0;
            }

            TStringOutput out(profile);
            DumpProfilingTimes(out);
            Profiler->Stop(out, 256, false);
            return true;
        }

        bool TProfilerActor::StopProfilerDumpToLog(const TActorContext& ctx) {
            if (!IsProfiling) {
                return false;
            }

            IsProfiling = false;
            StopTime = TInstant::Now();
            if (DynamicCounters) {
                *Counters.IsProfiling = 0;
            }

            TStringStream out;
            DumpProfilingTimes(out);
            Profiler->Stop(out, 2048, true);

            TVector<TString> split;
            Split(out.Str(), "\n", split);
            for (const auto& line : split) {
                LOG_WARN_S(ctx, NKikimrServices::MEMORY_PROFILER, line);
            }
            return true;
        }

        bool TProfilerActor::StopProfilerFile(TString& outFileName, TString& err) {
            if (!IsProfiling) {
                outFileName.clear();
                return false;
            }

            IsProfiling = false;
            StopTime = TInstant::Now();
            if (DynamicCounters) {
                *Counters.IsProfiling = 0;
            }

            outFileName = Sprintf("%s/%s.%d.%u.profile", Dir.c_str(), getprogname(), (int)getpid(), ++Count);
            Cerr << "Dumping profile to " << outFileName << Endl;

            try {
                TFileOutput out(outFileName);
                DumpProfilingTimes(out);
                Profiler->Stop(out, 4096, false);
            } catch (const yexception& e) {
                err = "Failed to dump profile: ";
                err += e.what();
                Cerr << err << Endl;
                return false;
            }

            return true;
        }

        void TProfilerActor::OutputControlHtml(IOutputStream& os, const TString& action, bool isOk) {
            os << "<p>";
            if (IsProfiling) {
                os << "<a class=\"btn btn-primary\" href=\"?action=stop-display\">Stop & display</a> \n";
                os << "<a class=\"btn btn-primary\" href=\"?action=stop-log\">Stop & dump to log</a>\n";
                os << "<a class=\"btn btn-primary\" href=\"?action=stop-save\">Stop & save to file</a>\n";
            } else {
                os << "<a class=\"btn btn-primary\" href=\"?action=start\">Start</a>\n";
                if (action == "stop-display" && isOk) {
                    os << " <a class=\"btn btn-default\" onclick=\""
                       << "javascript:$('.container').removeClass('container').addClass('container-fluid');$(this).hide();"
                       << "\">Full width</a>";
                }
            }
            os << "</p>";
        }

        void TProfilerActor::OutputResultHtml(IOutputStream& os, const TString& action, const TString& profile, const TString& fileName, bool isOk, const TString& err) {
            if (action == "stop-display") {
                if (isOk) {
                    os << "<div style=\"overflow-y: scroll;\"><pre "
                       << "style=\"overflow: auto;word-wrap: normal;white-space: pre;\">"
                       << EncodeHtmlPcdata(profile) << "</pre></div>\n";
                } else {
                    os << "<p>Error stopping profiling.</p>";
                }
            } else if (action == "stop-save") {
                if (isOk) {
                    os << "<p>Output saved to: " << fileName << "</p>";
                } else {
                    os << "<p>Error stopping profiling, ";
                    os << "filename: " << fileName;
                    os << "</p><p>Error: " << err << "</p>";
                }
            } else if (action == "stop-log") {
                if (isOk) {
                    os << "<p>Output dumped to log</p>";
                } else {
                    os << "<p>Error stopping profiling.</p>";
                }
            } else if (action == "start") {
                if (isOk) {
                    os << "<p>Profiling started OK.</p>";
                } else {
                    os << "<p>Error starting profiling.</p>";
                }
            }
        }
    }

    IActor* CreateProfilerActor(TDynamicCountersPtr counters, TString dir, std::unique_ptr<IProfilerLogic> profiler) {
        return new TProfilerActor(
            std::move(counters),
            std::move(dir),
            profiler ? std::move(profiler) : CreateProfiler());
    }
}
