#pragma once

#include "check.h"
#include "perf.h"
#include "symbol.h"

#include <util/generic/hide_ptr.h>
#include <util/system/platform.h>

#include <stddef.h> //size_t

#ifdef _win_
#ifndef LWTRACE_DISABLE
#define LWTRACE_DISABLE
#endif // LWTRACE_DISABLE
#endif // _win_

// Maximum number of executors that can be attached to a probe
#define LWTRACE_MAX_ACTIONS 100

// Maximum number of groups that can be assigned to a probe
#define LWTRACE_MAX_GROUPS 100

#ifndef LWTRACE_DISABLE

/*
 *  WARNING: All macros define in this header must be considered as implementation details and should NOT be used directly
 *  WARNING: See lwtrace/all.h for macros that represent a user interface of lwtrace library
 *
 */

// Use for code generation to handle parameter types. USAGE:
// 1. #define FOREACH_PARAMTYPE_MACRO(n, t, v, your_p1, your_p2) your code snippet
// 2. FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO, your_p1_value, your_p1_value)
//    FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO, your_p1_another_value, your_p1_another_value)
// 3. #undef FOREACH_PARAMTYPE_MACRO
// Type order matters!
#define FOREACH_PARAMTYPE(MACRO, ...)                         \
    MACRO("i64", i64, I64, ##__VA_ARGS__)                     \
    MACRO("ui64", ui64, Ui64, ##__VA_ARGS__)                  \
    MACRO("double", double, Double, ##__VA_ARGS__)            \
    MACRO("string", TString, Str, ##__VA_ARGS__)              \
    MACRO("symbol", NLWTrace::TSymbol, Symbol, ##__VA_ARGS__) \
    MACRO("check", NLWTrace::TCheck, Check, ##__VA_ARGS__)    \
    /**/

// Used with FOREACH_PARAMTYPE to handle TNil parameter type also
#define FOR_NIL_PARAMTYPE(MACRO, ...)     \
    MACRO(NULL, TNil, Nil, ##__VA_ARGS__) \
    /**/

// Used for math statements
#define FOR_MATH_PARAMTYPE(MACRO, ...)                     \
    MACRO("i64", i64, I64, ##__VA_ARGS__)                  \
    MACRO("ui64", ui64, Ui64, ##__VA_ARGS__)               \
    MACRO("check", NLWTrace::TCheck, Check, ##__VA_ARGS__) \
    /**/

// Use for code generation to handle parameter lists
// NOTE: this is the only place to change if more parameters needed
#define FOREACH_PARAMNUM(MACRO, ...) \
    MACRO(0, ##__VA_ARGS__)          \
    MACRO(1, ##__VA_ARGS__)          \
    MACRO(2, ##__VA_ARGS__)          \
    MACRO(3, ##__VA_ARGS__)          \
    MACRO(4, ##__VA_ARGS__)          \
    MACRO(5, ##__VA_ARGS__)          \
    MACRO(6, ##__VA_ARGS__)          \
    MACRO(7, ##__VA_ARGS__)          \
    MACRO(8, ##__VA_ARGS__)          \
    MACRO(9, ##__VA_ARGS__)          \
    MACRO(10, ##__VA_ARGS__)         \
    MACRO(11, ##__VA_ARGS__)         \
    MACRO(12, ##__VA_ARGS__)         \
    MACRO(13, ##__VA_ARGS__)         \
    MACRO(14, ##__VA_ARGS__)         \
    MACRO(15, ##__VA_ARGS__)         \
    MACRO(16, ##__VA_ARGS__)         \
    /**/

#define FOREACH_LEFT_TYPE(MACRO, ...) \
    MACRO(__VA_ARGS__, OT_VARIABLE)   \
    MACRO(__VA_ARGS__, OT_LITERAL)    \
    MACRO(__VA_ARGS__, OT_PARAMETER)  \
    /**/

#define FOREACH_RIGHT_TYPE(MACRO, ...) \
    MACRO(__VA_ARGS__, OT_VARIABLE)    \
    MACRO(__VA_ARGS__, OT_LITERAL)     \
    MACRO(__VA_ARGS__, OT_PARAMETER)   \
    /**/

#define FOREACH_DESTINATION_TYPE(MACRO, ...) \
    MACRO(__VA_ARGS__, OT_VARIABLE)          \
    /**/

// Auxilary macros
#define LWTRACE_EXPAND(x) x
#define LWTRACE_EAT(...)

// Eat last/first comma trick
#define LWTRACE_COMMA(bit) LWTRACE_COMMA_##bit()
#define LWTRACE_COMMA_0()
#define LWTRACE_COMMA_1() ,

// Macros to pack/unpack tuples, e.g. (x,y,z)
#define LWTRACE_UNROLL(...) __VA_ARGS__
#define LWTRACE_ENROLL(...) (__VA_ARGS__)

// Param types list handling macros
#define LWTRACE_TEMPLATE_PARAMS_I(i) (1) class TP##i = TNil LWTRACE_COMMA
#define LWTRACE_TEMPLATE_PARAMS LWTRACE_EXPAND(LWTRACE_EAT FOREACH_PARAMNUM(LWTRACE_TEMPLATE_PARAMS_I)(0))
#define LWTRACE_TEMPLATE_PARAMS_NODEF_I(i) (1) class TP##i LWTRACE_COMMA
#define LWTRACE_TEMPLATE_PARAMS_NODEF LWTRACE_EXPAND(LWTRACE_EAT FOREACH_PARAMNUM(LWTRACE_TEMPLATE_PARAMS_NODEF_I)(0))
#define LWTRACE_TEMPLATE_ARGS_I(i) (1) TP##i LWTRACE_COMMA
#define LWTRACE_TEMPLATE_ARGS LWTRACE_EXPAND(LWTRACE_EAT FOREACH_PARAMNUM(LWTRACE_TEMPLATE_ARGS_I)(0))
#define LWTRACE_FUNCTION_PARAMS_I(i) (1) typename ::NLWTrace::TParamTraits<TP##i>::TFuncParam p##i = ERROR_not_enough_parameters() LWTRACE_COMMA
#define LWTRACE_FUNCTION_PARAMS LWTRACE_EXPAND(LWTRACE_EAT FOREACH_PARAMNUM(LWTRACE_FUNCTION_PARAMS_I)(0))
#define LWTRACE_PREPARE_PARAMS_I(i, params) params.Param[i].template CopyConstruct<typename ::NLWTrace::TParamTraits<TP##i>::TStoreType>(::NLWTrace::TParamTraits<TP##i>::ToStoreType(p##i));
#define LWTRACE_PREPARE_PARAMS(params)                     \
    do {                                                   \
        FOREACH_PARAMNUM(LWTRACE_PREPARE_PARAMS_I, params) \
    } while (false)
#define LWTRACE_COUNT_PARAMS_I(i) +(std::is_same<TP##i, ::NLWTrace::TNil>::value ? 0 : 1)
#define LWTRACE_COUNT_PARAMS (0 FOREACH_PARAMNUM(LWTRACE_COUNT_PARAMS_I))
#define LWTRACE_MAX_PARAMS_I(i) +1
#define LWTRACE_MAX_PARAMS (0 FOREACH_PARAMNUM(LWTRACE_MAX_PARAMS_I))

// Determine maximum sizeof(t) over all supported types
#define LWTRACE_MAX_PARAM_SIZE_TA_TAIL_I(n, t, v) v,
#define LWTRACE_MAX_PARAM_SIZE_TA_TAIL                  \
    FOREACH_PARAMTYPE(LWTRACE_MAX_PARAM_SIZE_TA_TAIL_I) \
    0
#define LWTRACE_MAX_PARAM_SIZE_TP_I(n, t, v) , size_t v = 0
#define LWTRACE_MAX_PARAM_SIZE_TP size_t Head = 0 FOREACH_PARAMTYPE(LWTRACE_MAX_PARAM_SIZE_TP_I)
#define LWTRACE_MAX_PARAM_SIZE_TA_I(n, t, v) , sizeof(t)
#define LWTRACE_MAX_PARAM_SIZE_TA 0 FOREACH_PARAMTYPE(LWTRACE_MAX_PARAM_SIZE_TA_I)
#define LWTRACE_MAX_PARAM_SIZE ::NLWTrace::TMaxParamSize<LWTRACE_MAX_PARAM_SIZE_TA>::Result

namespace NLWTrace {
    template <LWTRACE_MAX_PARAM_SIZE_TP>
    struct TMaxParamSize {
        static const size_t Tail = TMaxParamSize<LWTRACE_MAX_PARAM_SIZE_TA_TAIL>::Result;
        static const size_t Result = (Head > Tail ? Head : Tail);
    };

    template <>
    struct TMaxParamSize<> {
        static const size_t Result = 0;
    };

}

// Define stuff that is needed to register probes before main()
#define LWTRACE_REGISTER_PROBES(provider)                                                           \
    namespace LWTRACE_GET_NAMESPACE(provider) {                                                     \
        struct TInitLWTrace##provider {                                                             \
            TInitLWTrace##provider() {                                                              \
                Singleton<NLWTrace::TProbeRegistry>()->AddProbesList(LWTRACE_GET_PROBES(provider)); \
            }                                                                                       \
            /* This may not be in anonymous namespace because otherwise */                          \
            /* it is called twice when .so loaded twice */                                          \
        }* InitLWTrace##provider = Singleton<TInitLWTrace##provider>();                             \
    }                                                                                               \
    /**/

// Macro for TSignature POD structure static initialization
#define LWTRACE_SIGNATURE_CTOR(types, names)                                                                          \
    {                                                                                                                 \
        /* ParamTypes */ ::NLWTrace::TUserSignature<LWTRACE_EXPAND(LWTRACE_UNROLL types)>::ParamTypes,                \
            /* ParamNames */ {LWTRACE_EXPAND(LWTRACE_UNROLL names)},                                                  \
            /* ParamCount */ ::NLWTrace::TUserSignature<LWTRACE_EXPAND(LWTRACE_UNROLL types)>::ParamCount,            \
            /* SerializeParams */ &::NLWTrace::TUserSignature<LWTRACE_EXPAND(LWTRACE_UNROLL types)>::SerializeParams, \
            /* CloneParams */ &::NLWTrace::TUserSignature<LWTRACE_EXPAND(LWTRACE_UNROLL types)>::CloneParams,         \
            /* DestroyParams */ &::NLWTrace::TUserSignature<LWTRACE_EXPAND(LWTRACE_UNROLL types)>::DestroyParams,     \
            /* SerializeToPb */ &::NLWTrace::TUserSignature<LWTRACE_EXPAND(LWTRACE_UNROLL types)>::SerializeToPb,     \
            /* DeserializeFromPb */ &::NLWTrace::TUserSignature<LWTRACE_EXPAND(LWTRACE_UNROLL types)>::DeserializeFromPb\
    }

// Macro for TEvent POD structure static initialization
#define LWTRACE_EVENT_CTOR(name, groups, types, names)                   \
    {                                                                    \
        /* Name */ #name,                                                \
            /* Groups */ {LWTRACE_EXPAND(LWTRACE_UNROLL_GROUPS groups)}, \
            /* Signature */ LWTRACE_SIGNATURE_CTOR(types, names)         \
    }

// Macro for TProbe POD structure static initialization
#define LWTRACE_PROBE_CTOR(name, groups, types, names)              \
    {                                                               \
        /* Event */ LWTRACE_EVENT_CTOR(name, groups, types, names), \
            /* ExecutorsCount */ 0,                                 \
            /* Lock */ {0},                                         \
            /* Executors */ {0},                                    \
            /* Front */ 0,                                          \
            /* Back */ 0                                            \
    }

// Provider static data accessors
#define LWTRACE_GET_NAMESPACE(provider) NLWTrace_##provider
#define LWTRACE_GET_NAME(name) lwtrace_##name
#define LWTRACE_GET_TYPE(name) TLWTrace_##name
#define LWTRACE_GET_PROBES_I(provider) gProbes##provider
#define LWTRACE_GET_EVENTS_I(provider) gEvents##provider

// Declaration of provider static data
#define LWTRACE_DECLARE_PROBE(name, groups, types, names)                                        \
    typedef ::NLWTrace::TUserProbe<LWTRACE_EXPAND(LWTRACE_UNROLL types)> LWTRACE_GET_TYPE(name); \
    extern LWTRACE_GET_TYPE(name) LWTRACE_GET_NAME(name);                                        \
    /**/
#define LWTRACE_DECLARE_EVENT(name, groups, types, names)                                        \
    typedef ::NLWTrace::TUserEvent<LWTRACE_EXPAND(LWTRACE_UNROLL types)> LWTRACE_GET_TYPE(name); \
    extern LWTRACE_GET_TYPE(name) LWTRACE_GET_NAME(name);                                        \
    /**/
#define LWTRACE_DECLARE_PROVIDER_I(provider)                                                                   \
    namespace LWTRACE_GET_NAMESPACE(provider) {                                                                \
        provider(LWTRACE_DECLARE_PROBE, LWTRACE_DECLARE_EVENT, LWTRACE_ENROLL, LWTRACE_ENROLL, LWTRACE_ENROLL) \
    }                                                                                                          \
    extern ::NLWTrace::TProbe* LWTRACE_GET_PROBES_I(provider)[];                                               \
    extern ::NLWTrace::TEvent* LWTRACE_GET_EVENTS_I(provider)[];                                               \
    /**/

// Initialization of provider static data
#define LWTRACE_UNROLL_GROUPS(x) #x, LWTRACE_UNROLL
#define LWTRACE_DEFINE_PROBE(name, groups, types, names)  \
    LWTRACE_GET_TYPE(name)                                \
    LWTRACE_GET_NAME(name) =                              \
        {LWTRACE_PROBE_CTOR(name, groups, types, names)}; \
    /**/
#define LWTRACE_DEFINE_EVENT(name, groups, types, names)  \
    LWTRACE_GET_TYPE(name)                                \
    LWTRACE_GET_NAME(name) =                              \
        {LWTRACE_EVENT_CTOR(name, groups, types, names)}; \
    /**/
#define LWTRACE_PROBE_ADDRESS_I(name, groups, types, names) \
    LWTRACE_GET_NAME(name).Probe, /**/
#define LWTRACE_PROBE_ADDRESS(provider) \
    &LWTRACE_GET_NAMESPACE(provider)::LWTRACE_PROBE_ADDRESS_I /**/
#define LWTRACE_EVENT_ADDRESS_I(name, groups, types, names) \
    LWTRACE_GET_NAME(name).Event, /**/
#define LWTRACE_EVENT_ADDRESS(provider) \
    &LWTRACE_GET_NAMESPACE(provider)::LWTRACE_EVENT_ADDRESS_I /**/
#define LWTRACE_DEFINE_PROVIDER_I(provider)                                                                            \
    namespace LWTRACE_GET_NAMESPACE(provider) {                                                                        \
        provider(LWTRACE_DEFINE_PROBE, LWTRACE_DEFINE_EVENT, (provider)LWTRACE_ENROLL, LWTRACE_ENROLL, LWTRACE_ENROLL) \
    }                                                                                                                  \
    ::NLWTrace::TProbe* LWTRACE_GET_PROBES_I(provider)[] = {                                                           \
        provider(LWTRACE_PROBE_ADDRESS(provider), LWTRACE_EAT, LWTRACE_ENROLL, LWTRACE_ENROLL, LWTRACE_ENROLL)         \
            NULL};                                                                                                     \
    ::NLWTrace::TEvent* LWTRACE_GET_EVENTS_I(provider)[] = {                                                           \
        provider(LWTRACE_EAT, LWTRACE_EVENT_ADDRESS(provider), LWTRACE_ENROLL, LWTRACE_ENROLL, LWTRACE_ENROLL)         \
            NULL};                                                                                                     \
    LWTRACE_REGISTER_PROBES(provider)
    /**/

#define LWPROBE_I(probe, ...)                                       \
    do {                                                            \
        if ((probe).Probe.GetExecutorsCount() > 0) {                \
            ::NLWTrace::TScopedThreadCpuTracker _cpuTracker(probe); \
            TReadSpinLockGuard g((probe).Probe.Lock);               \
            if ((probe).Probe.GetExecutorsCount() > 0) {            \
                (probe)(__VA_ARGS__);                               \
            }                                                       \
        }                                                           \
    } while (false) /**/

#define LWPROBE_ENABLED_I(probe) ((probe).Probe.GetExecutorsCount() > 0)

#define LWPROBE_DURATION_I(probetype, uniqid, probe, ...) probetype ::TScopedDuration uniqid(probe, 0 /* fake P0 - used for duration */, ##__VA_ARGS__);

#define LWTRACK_I(probe, orbit, ...)                                    \
    do {                                                                \
        if ((probe).Probe.GetExecutorsCount() > 0) {                    \
            ::NLWTrace::TScopedThreadCpuTracker _cpuTracker(probe);     \
            TReadSpinLockGuard g((probe).Probe.Lock);                   \
            if ((probe).Probe.GetExecutorsCount() > 0) {                \
                (probe).Run(orbit, ##__VA_ARGS__);                      \
            }                                                           \
        } else {                                                        \
            auto& _orbit = (orbit);                                     \
            if (HasShuttles(_orbit)) {                                  \
                ::NLWTrace::TScopedThreadCpuTracker _cpuTracker(probe); \
                (probe).RunShuttles(_orbit, ##__VA_ARGS__);             \
            }                                                           \
        }                                                               \
    } while (false) /**/

#define LWEVENT_I(event, ...) (event)(__VA_ARGS__)

#else
#define LWTRACE_MAX_PARAM_SIZE sizeof(void*)
#define LWTRACE_MAX_PARAMS 1
#define FOREACH_PARAMTYPE(MACRO, ...)

#endif
