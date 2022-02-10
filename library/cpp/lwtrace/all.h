#pragma once

#include "control.h"
#include "event.h"
#include "preprocessor.h"
#include "probe.h"
#include "start.h"

//
// Full documentation: https://wiki.yandex-team.ru/development/poisk/arcadia/library/lwtrace/
//
// Short usage instruction:
//
// 1. Declare probes provider in header file 'probes.h':
//       #include <yweb/robot/kiwi/lwtrace/all.h>
//       #define MY_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \   // name of your provider
//           PROBE(MyProbe, GROUPS("MyGroup1", "MyGroup2"), TYPES(), NAMES()) \   // probe specification w/o argiments
//           PROBE(MyAnotherProbe, GROUPS("MyGroup2"), TYPES(int, TString), NAMES("arg1", "arg2")) \   // another probe with arguments
//           PROBE(MyScopedProbe, GROUPS(), TYPES(ui64, int), NAMES("duration", "stage")) \   // scoped probe with argument
//           /**/
//       LWTRACE_DECLARE_PROVIDER(MY_PROVIDER)
//
// 2. Define provider in source file 'provider.cpp':
//       #include "probes.h"
//       LWTRACE_DEFINE_PROVIDER(MY_PROVIDER)
//
// 3. Call probes from provider in your code:
//       #include "probes.h"
//       int main() {
//           GLOBAL_LWPROBE(MY_PROVIDER, MyProbe);
//           GLOBAL_LWPROBE(MY_PROVIDER, MyAnotherProbe, 123, stroka);
//           ... or ...
//           LWTRACE_USING(MY_PROVIDER); // expands into using namespace
//           LWPROBE(MyProbe);
//           LWPROBE(MyAnotherProbe, 123, stroka);
//       }
//
// 4. Attach provider to your monitoring service:
//       #include <yweb/robot/kiwi/util/monservice.h>
//       #include "probes.h"
//       class TMyMonSrvc: public NKiwi::TMonService {
//           TMyMonSrvc(TProbeRegistry& probes)
//           {
//               THolder<NKiwi::TTraceMonPage> tr(new NKiwi::TTraceMonPage());
//               tr->GetProbes().AddProbesList(LWTRACE_GET_PROBES(MY_PROVIDER));
//               Register(tr.Release());
//           }
//       };
//
// 5. Compile and run application
//
// 6. Create file 'mysuper.tr' with trace query:
//        Blocks { # Log all calls to probes from MyGroup1
//            ProbeDesc { Group: "MyGroup1" }
//            Action {
//                LogAction { LogTimestamp: true }
//            }
//        }
//        Blocks { # Log first 10 calls to MyAnother with arg1 > 1000
//            ProbeDesc { Name: "MyAnotherProbe"; Provider: "MY_PROVIDER" }
//            Predicate {
//                Operators { Type: OT_GT; Param: "arg1"; Value: "1000" }
//            }
//            Action {
//                LogAction { MaxRecords: 10 }
//            }
//        }
//        Blocks { # Start following executon of all 4 blocks each time MyAnotherProbe was called with arg2 == "start"
//            ProbeDesc { Name: "MyAnotherProbe"; Provider: "MY_PROVIDER" }
//            Predicate {
//                Operators { Type: OT_EQ; Param: "arg2"; Value: "start" }
//            }
//            Action { StartAction {} }
//        }
//        Blocks { # Stop following executon of all 4 blocks each time MyAnotherProbe was called with arg2 == "stop"
//            ProbeDesc { Name: "MyAnotherProbe"; Provider: "MY_PROVIDER" }
//            Predicate {
//                Operators { Type: OT_EQ; Param: "arg2"; Value: "stop" }
//            }
//            Action { StopAction {} }
//        }
//
// 7. Send trace query to the server with HTTP POST:
//       yweb/robot/kiwi/scripts/trace.sh new uniq-id-for-my-trace hostname:monport < mysuper.tr
//
// 8. With browser go to: http://hostname:monport/trace
//
// 9. Delete query from server:
//       yweb/robot/kiwi/scripts/trace.sh delete uniq-id-for-my-trace hostname:monport
//
//
// CONFIGURATION AND SUPPORT:
// 1. Turning off all calls to probes.
//       Add to project's CMakeLists.txt: add_definitions(-DLWTRACE_DISABLE_PROBES)
//
// 2. Turning off all calls to events.
//       Add to project's CMakeLists.txt: add_definitions(-DLWTRACE_DISABLE_EVENTS)
//
// 3. Increasing maximum number of probe parameters:
//       Add more lines in FOREACH_PARAMNUM macro definition in preprocessor.h
//
//
// ISSUES
// 1. Note that executors for different blocks are attached in order of their declaration in trace script.
//    Executor can be called by a program as soon as it is attached, therefore there is no guarantee that
//    all blocks are started simultaneously
//

#ifndef LWTRACE_DISABLE

// Declare user provider (see USAGE INSTRUCTION)
#define LWTRACE_DECLARE_PROVIDER(provider) LWTRACE_DECLARE_PROVIDER_I(provider)

// Define user provider (see USAGE INSTRUCTION)
#define LWTRACE_DEFINE_PROVIDER(provider) LWTRACE_DEFINE_PROVIDER_I(provider)

// Import names from provider
#define LWTRACE_USING(provider) using namespace LWTRACE_GET_NAMESPACE(provider);

// Probes and events list accessor
#define LWTRACE_GET_PROBES(provider) LWTRACE_GET_PROBES_I(provider)
#define LWTRACE_GET_EVENTS(provider) LWTRACE_GET_EVENTS_I(provider)

#ifndef LWTRACE_DISABLE_PROBES

// Call a probe
// NOTE: LWPROBE() should be used in source files only with previous call to LWTRACE_USING(MY_PROVIDER)
// NOTE: in header files GLOBAL_LWPROBE() should be used instead
// NOTE: if lots of calls needed in header file, it's convenient to define/undef the following macro:
// NOTE: #define MY_PROBE(name, ...) GLOBAL_LWPROBE(MY_PROVIDER, name, ## __VA_ARGS__)
#define GLOBAL_LWPROBE(provider, probe, ...) LWPROBE_I(LWTRACE_GET_NAMESPACE(provider)::LWTRACE_GET_NAME(probe), ##__VA_ARGS__)
#define LWPROBE(probe, ...) LWPROBE_I(LWTRACE_GET_NAME(probe), ##__VA_ARGS__)
#define GLOBAL_LWPROBE_ENABLED(provider, probe) LWPROBE_ENABLED_I(LWTRACE_GET_NAMESPACE(provider)::LWTRACE_GET_NAME(probe))
#define LWPROBE_ENABLED(probe) LWPROBE_ENABLED_I(LWTRACE_GET_NAME(probe))
#define LWPROBE_OBJ(probe, ...) LWPROBE_I(probe, ##__VA_ARGS__)

// Calls a probe when scope is beeing left
// NOTE: arguments are passed by value and stored until scope exit
// NOTE: probe should be declared with first params of type ui64, argument for which is automaticaly generated (duration in microseconds)
// NOTE: *_DURATION() macros take through "..." all arguments except the first one
#define GLOBAL_LWPROBE_DURATION(provider, probe, ...) LWPROBE_DURATION_I(LWTRACE_GET_NAMESPACE(provider)::LWTRACE_GET_TYPE(probe), lwtrace_scoped_##provider##probe, LWTRACE_GET_NAMESPACE(provider)::LWTRACE_GET_NAME(probe), ##__VA_ARGS__)
#define LWPROBE_DURATION(probe, ...) LWPROBE_DURATION_I(LWTRACE_GET_TYPE(probe), lwtrace_scoped_##probe, LWTRACE_GET_NAME(probe), ##__VA_ARGS__)

// Probe with orbit support
#define GLOBAL_LWTRACK(provider, probe, orbit, ...) LWTRACK_I(LWTRACE_GET_NAMESPACE(provider)::LWTRACE_GET_NAME(probe), orbit, ##__VA_ARGS__)
#define LWTRACK(probe, orbit, ...) LWTRACK_I(LWTRACE_GET_NAME(probe), orbit, ##__VA_ARGS__)
#define LWTRACK_OBJ(probe, orbit, ...) LWTRACK_I(probe, orbit, ##__VA_ARGS__)

#else
#define GLOBAL_LWPROBE(provider, probe, ...)
#define LWPROBE(probe, ...)
#define GLOBAL_LWPROBE_ENABLED(provider, probe) false
#define LWPROBE_ENABLED(probe) false
#define LWPROBE_OBJ(probe, ...) Y_UNUSED(probe)
#define GLOBAL_LWPROBE_DURATION(provider, probe, ...)
#define LWPROBE_DURATION(probe, ...)
#define GLOBAL_LWTRACK(provider, probe, orbit, ...)
#define LWTRACK(probe, orbit, ...)
#define LWTRACK_OBJ(probe, orbit, ...) Y_UNUSED(probe)
#endif

#ifndef LWTRACE_DISABLE_EVENTS

// Call an event
// NOTE: LWEVENT() should be used in source files only with previous call to LWTRACE_USING(MY_PROVIDER)
// NOTE: in header files GLOBAL_LWEVENT() should be used instead
// NOTE: if lots of calls needed in header file, it's convenient to define/undef the following macro:
// NOTE: #define MY_EVENT(name, ...) GLOBAL_LWEVENT(MY_PROVIDER, name, ## __VA_ARGS__)
#define GLOBAL_LWEVENT(provider, event, ...) LWEVENT_I(LWTRACE_GET_NAMESPACE(provider)::LWTRACE_GET_NAME(event), ##__VA_ARGS__)
#define LWEVENT(event, ...) LWEVENT_I(LWTRACE_GET_NAME(event), ##__VA_ARGS__)

#else
#define GLOBAL_LWEVENT(provider, event, ...)
#define LWEVENT(event, ...)
#endif

#else

#define LWTRACE_DECLARE_PROVIDER(provider)
#define LWTRACE_DEFINE_PROVIDER(provider)
#define LWTRACE_USING(provider)
#define LWTRACE_GET_PROBES(provider) NULL
#define LWTRACE_GET_EVENTS(provider) NULL
#define GLOBAL_LWPROBE(provider, probe, ...)
#define LWPROBE(probe, ...)
#define GLOBAL_LWPROBE_ENABLED(provider, probe) false
#define LWPROBE_ENABLED(probe) false
#define GLOBAL_LWPROBE_DURATION(provider, probe, ...)
#define LWPROBE_DURATION(probe, ...)
#define GLOBAL_LWTRACK(provider, probe, orbit, ...)
#define LWTRACK(probe, orbit, ...)
#define GLOBAL_LWEVENT(provider, event, ...)
#define LWEVENT(event, ...)

#endif
