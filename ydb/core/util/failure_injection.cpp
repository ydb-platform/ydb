#include "failure_injection.h"
#include <ydb/core/protos/services.pb.h>
#include <util/system/mutex.h>
#include <util/generic/queue.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/lwtrace/all.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>

using namespace NActors;

namespace NKikimr {

    using namespace NLWTrace;

    namespace {

        class TFailureInjectionManager {
            struct TFailureQueueItem {
                TString Name;
                TMaybe<TParams> Params;
                ui32 HitCount;
            };
            TDeque<TFailureQueueItem> FailureQ;
            TMutex Mutex;
            volatile bool Committed = false;

        public:
            void Inject(const TString& name, const TParams& params) {
                if (Committed) {
                    with_lock (Mutex) {
                        if (FailureQ) {
                            TFailureQueueItem& item = FailureQ.front();
                            if (item.Name == name && CompareParams(item.Params, params) && !--item.HitCount) {
                                FailureQ.pop_front();
                                if (FailureQ.empty()) {
                                    InjectFailure();
                                }
                            }
                        }
                    }
                }
            }

            void EnqueueFailureItem(const TString& name, const TMaybe<TParams>& params, ui32 hitCount = 1) {
                with_lock (Mutex) {
                    FailureQ.push_back(TFailureQueueItem{name, params, hitCount});
                }
            }

            void Commit() {
                Committed = true;
            }

            void DumpQueue(IOutputStream& str) { 
                HTML(str) {
                    TABLE() {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {
                                    str << "Probe name";
                                }
                                TABLEH() {
                                    str << "Remaining hit count";
                                }
                            }
                        }
                        TABLEBODY() {
                            with_lock (Mutex) {
                                for (const TFailureQueueItem& item : FailureQ) {
                                    TABLER() {
                                        TABLED() {
                                            str << item.Name;
                                        }
                                        TABLED() {
                                            str << item.HitCount;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

        private:
            static bool CompareParams(const TMaybe<TParams>& x, const TParams& /*y*/) {
                return !x || /* *x == y */ true; // FIXME: implement parameter comparison
            }

            void InjectFailure() {
                raise(SIGKILL);
            }
        };

        class TTraceActionExecutor : public TCustomActionExecutor {
            TFailureInjectionManager *Manager = nullptr;
            TString Name;

        public:
            TTraceActionExecutor(TProbe *probe, TFailureInjectionManager *manager, TString name)
                : TCustomActionExecutor(probe, true /*destructive*/)
                , Manager(manager)
                , Name(std::move(name))
            {}

        private:
            bool DoExecute(TOrbit&, const TParams& params) override {
                Manager->Inject(Name, params);
                return true;
            }
        };

        class TFailureInjectionActor : public TActorBootstrapped<TFailureInjectionActor> {
            TManager TraceManager;
            TVector<TString> Probes;
            TFailureInjectionManager Manager;
            bool Enabled = false;

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_FAILURE_INJECTION;
            }

            TFailureInjectionActor()
                : TraceManager(*Singleton<TProbeRegistry>(), true)
            {}

            void Bootstrap(const TActorContext& /*ctx*/) {
                struct TCallback {
                    TVector<TString>& Probes;

                    TCallback(TVector<TString>& probes)
                        : Probes(probes)
                    {}

                    void Push(const TProbe *probe) {
                        if (!strcmp(probe->Event.Groups[0], "FAIL_INJECTION_PROVIDER")) {
                            Probes.push_back(probe->Event.Name);
                        }
                    }
                };

                TCallback callback(Probes);
                TraceManager.ReadProbes(callback);

                Become(&TFailureInjectionActor::StateFunc);
            }

            void Enable() {
                if (!Enabled) {
                    TQuery query;
                    for (const TString& name : Probes) {
                        TString actionName = "FailureInjection_" + name;

                        auto& block = *query.AddBlocks();
                        auto& desc = *block.MutableProbeDesc();
                        desc.SetName(name);
                        desc.SetProvider("FAIL_INJECTION_PROVIDER");
                        auto& action = *block.AddAction();
                        auto& custom = *action.MutableCustomAction();
                        custom.SetName(actionName);

                        auto factory = [=](TProbe *probe, const TCustomAction& /*action*/, TSession* /*trace*/) {
                            return new TTraceActionExecutor(probe, &Manager, name);
                        };
                        TraceManager.RegisterCustomAction(actionName, factory);
                    }

                    TraceManager.New("env", query);
                    Manager.Commit();
                    Enabled = true;
                }
            }

            void HandlePoison(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
                ctx.Send(ev->Sender, new TEvents::TEvPoisonTaken);
                Die(ctx);
            }

            void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
                TStringStream str;

                const auto& params = ev->Get()->Request.GetParams();
                if (params.Has("queue")) {
                    TString queue = params.Get("queue");
                    if (queue) {
                        ProcessQueue(str, queue);
                    }
                }
                if (params.Has("probe")) {
                    TString probe = params.Get("probe");
                    if (probe) {
                        try {
                            TString hc = params.Has("hitcount") ? params.Get("hitcount") : TString();
                            ui32 hitCount = hc ? FromString<ui32>(hc) : 1;
                            Manager.EnqueueFailureItem(probe, {}, hitCount);
                        } catch (const yexception& ex) {
                            HTML(str) {
                                DIV() {
                                    str << "<h1><font color=red>" << ex.what() << "</font></h1>";
                                }
                            }
                        }
                    }
                }
                if (params.Has("enable")) {
                    Enable();
                }

                HTML(str) {
                    DIV() {
                        Manager.DumpQueue(str);
                    }

                    FORM_CLASS("form-horizontal") {
                        DIV_CLASS("control-group") {
                            LABEL_CLASS_FOR("control-label", "probe") {
                                str << "Probe";
                            }
                            DIV_CLASS("controls") {
                                str << "<select id=\"probe\" name=\"probe\">";
                                for (const TString& probe : Probes) {
                                    str << "<option value=\"" << probe << "\">" << probe << "</option>";
                                }
                                str << "</select>";
                            }

                            LABEL_CLASS_FOR("control-label", "hitcount") {
                                str << "Hit count";
                            }
                            DIV_CLASS("controls") {
                                str << "<input type=\"number\" id=\"hitcount\" name=\"hitcount\">";
                            }

                            LABEL_CLASS_FOR("control-label", "queue") {
                                str << "Queue definition string";
                            }
                            DIV_CLASS("controls") {
                                str << "<input id=\"queue\" name=\"queue\">";
                            }
                        }
                        DIV_CLASS("control-group") {
                            DIV_CLASS("controls") {
                                str << "<button type=\"submit\" name=\"submit\" class=\"btn btn-default\">Add to queue</button>";
                            }
                        }
                    }
                }
                ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), ev->Get()->SubRequestId));
            }

            void ProcessQueue(IOutputStream& str, const TString& queue) { 
                TVector<std::tuple<TString, TMaybe<TParams>, ui32>> items;

                HTML(str) {
                    size_t pos = 0;
                    for (;;) {
                        TString probe;
                        for (; pos < queue.size() && queue[pos] != ';' && queue[pos] != '#'; ++pos) {
                            probe.append(queue[pos]);
                        }
                        if (std::find(Probes.begin(), Probes.end(), probe) == Probes.end()) {
                            DIV() {
                                str << "<h1><font color=red>Probe " << probe << " does not exist</font></h1>";
                            }
                            return;
                        }
                        ui32 hitCount = 1;
                        if (pos < queue.size() && queue[pos] == '#') {
                            for (++pos, hitCount = 0; pos < queue.size() && isdigit(queue[pos]); ++pos) {
                                hitCount = hitCount * 10 + (queue[pos] - '0');
                            }
                        }
                        if (pos < queue.size() && queue[pos] != ';') {
                            DIV() {
                                str << "<h1><font color=red>Missing semicolon</font></h1>";
                            }
                            return;
                        }
                        items.emplace_back(std::move(probe), Nothing(), hitCount);
                        if (pos < queue.size()) {
                            ++pos;
                        } else {
                            break;
                        }
                    }
                }

                for (const auto& item : items) {
                    Manager.EnqueueFailureItem(std::get<0>(item), std::get<1>(item), std::get<2>(item));
                }
            }

            STRICT_STFUNC(StateFunc,
                HFunc(TEvents::TEvPoisonPill, HandlePoison)
                HFunc(NMon::TEvHttpInfo, Handle)
            )
        };

    } // anon

    IActor *CreateFailureInjectionActor() {
        return new TFailureInjectionActor();
    }

} // NKikimr
