#pragma once

#include "wilson_trace.h"

#include <library/cpp/string_utils/base64/base64.h>

#include <library/cpp/actors/core/log.h>

namespace NWilson {
#if !defined(_win_)
// works only for those compilers, who trait C++ as ISO IEC 14882, not their own standard

#define __UNROLL_PARAMS_8(N, F, X, ...) \
    F(X, N - 8)                         \
    __UNROLL_PARAMS_7(N, F, ##__VA_ARGS__)
#define __UNROLL_PARAMS_7(N, F, X, ...) \
    F(X, N - 7)                         \
    __UNROLL_PARAMS_6(N, F, ##__VA_ARGS__)
#define __UNROLL_PARAMS_6(N, F, X, ...) \
    F(X, N - 6)                         \
    __UNROLL_PARAMS_5(N, F, ##__VA_ARGS__)
#define __UNROLL_PARAMS_5(N, F, X, ...) \
    F(X, N - 5)                         \
    __UNROLL_PARAMS_4(N, F, ##__VA_ARGS__)
#define __UNROLL_PARAMS_4(N, F, X, ...) \
    F(X, N - 4)                         \
    __UNROLL_PARAMS_3(N, F, ##__VA_ARGS__)
#define __UNROLL_PARAMS_3(N, F, X, ...) \
    F(X, N - 3)                         \
    __UNROLL_PARAMS_2(N, F, ##__VA_ARGS__)
#define __UNROLL_PARAMS_2(N, F, X, ...) \
    F(X, N - 2)                         \
    __UNROLL_PARAMS_1(N, F, ##__VA_ARGS__)
#define __UNROLL_PARAMS_1(N, F, X) F(X, N - 1)
#define __UNROLL_PARAMS_0(N, F)
#define __EX(...) __VA_ARGS__
#define __NUM_PARAMS(...) __NUM_PARAMS_SELECT_N(__VA_ARGS__, __NUM_PARAMS_SEQ)
#define __NUM_PARAMS_SELECT_N(...) __EX(__NUM_PARAMS_SELECT(__VA_ARGS__))
#define __NUM_PARAMS_SELECT(X, _1, _2, _3, _4, _5, _6, _7, _8, N, ...) N
#define __NUM_PARAMS_SEQ 8, 7, 6, 5, 4, 3, 2, 1, 0, ERROR
#define __CAT(X, Y) X##Y
#define __UNROLL_PARAMS_N(N, F, ...) __EX(__CAT(__UNROLL_PARAMS_, N)(N, F, ##__VA_ARGS__))
#define __UNROLL_PARAMS(F, ...) __UNROLL_PARAMS_N(__NUM_PARAMS(X, ##__VA_ARGS__), F, ##__VA_ARGS__)
#define __EX2(F, X, INDEX) __INVOKE(F, __EX X, INDEX)
#define __INVOKE(F, ...) F(__VA_ARGS__)

#define __DECLARE_PARAM(X, INDEX) __EX2(__DECLARE_PARAM_X, X, INDEX)
#define __DECLARE_PARAM_X(TYPE, NAME, INDEX)                   \
    static const struct T##NAME##Param                         \
        : ::NWilson::TParamBinder<INDEX, TYPE> {               \
        T##NAME##Param() {                                     \
        }                                                      \
        using ::NWilson::TParamBinder<INDEX, TYPE>::operator=; \
    } NAME;

#define __TUPLE_PARAM(X, INDEX) __EX2(__TUPLE_PARAM_X, X, INDEX)
#define __TUPLE_PARAM_X(TYPE, NAME, INDEX) TYPE,

#define __OUTPUT_PARAM(X, INDEX) __EX2(__OUTPUT_PARAM_X, X, INDEX)
#define __OUTPUT_PARAM_X(TYPE, NAME, INDEX) str << (INDEX ? ", " : "") << #NAME << "# " << std::get<INDEX>(ParamPack);

#define __FILL_PARAM(P, INDEX)                 \
    do {                                       \
        const auto& boundParam = (NParams::P); \
        boundParam.Apply(event.ParamPack);     \
    } while (false);

#define DECLARE_WILSON_EVENT(EVENT_NAME, ...)                    \
    namespace N##EVENT_NAME##Params {                            \
        __UNROLL_PARAMS(__DECLARE_PARAM, ##__VA_ARGS__)          \
                                                                 \
        using TParamPack = std::tuple<                           \
            __UNROLL_PARAMS(__TUPLE_PARAM, ##__VA_ARGS__) char>; \
    }                                                            \
    struct T##EVENT_NAME {                                       \
        using TParamPack = N##EVENT_NAME##Params::TParamPack;    \
        TParamPack ParamPack;                                    \
                                                                 \
        void Output(IOutputStream& str) {                        \
            str << #EVENT_NAME << "{";                           \
            __UNROLL_PARAMS(__OUTPUT_PARAM, ##__VA_ARGS__)       \
            str << "}";                                          \
        }                                                        \
    };

    template <size_t INDEX, typename T>
    class TBoundParam {
        mutable T Value;

    public:
        TBoundParam(T&& value)
            : Value(std::move(value))
        {
        }

        template <typename TParamPack>
        void Apply(TParamPack& pack) const {
            std::get<INDEX>(pack) = std::move(Value);
        }
    };

    template <size_t INDEX, typename T>
    struct TParamBinder {
        template <typename TValue>
        TBoundParam<INDEX, T> operator=(const TValue& value) const {
            return TBoundParam<INDEX, T>(TValue(value));
        }

        template <typename TValue>
        TBoundParam<INDEX, T> operator=(TValue&& value) const {
            return TBoundParam<INDEX, T>(std::move(value));
        }
    };

// generate wilson event having parent TRACE_ID and span TRACE_ID to become parent of logged event
#define WILSON_TRACE(CTX, TRACE_ID, EVENT_NAME, ...)             \
    if (::NWilson::TraceEnabled(CTX)) {                          \ 
        ::NWilson::TTraceId* __traceId = (TRACE_ID);             \
        if (__traceId && *__traceId) {                           \
            TInstant now = Now();                                \
            T##EVENT_NAME event;                                 \
            namespace NParams = N##EVENT_NAME##Params;           \
            __UNROLL_PARAMS(__FILL_PARAM, ##__VA_ARGS__)         \
            ::NWilson::TraceEvent((CTX), __traceId, event, now); \
        }                                                        \
    } 

    inline ui32 GetNodeId(const NActors::TActorSystem& actorSystem) {
        return actorSystem.NodeId;
    }
    inline ui32 GetNodeId(const NActors::TActivationContext& ac) {
        return GetNodeId(*ac.ExecutorThread.ActorSystem);
    }

    constexpr ui32 WilsonComponentId = 430; // kikimrservices: wilson
 
    template <typename TActorSystem> 
    bool TraceEnabled(const TActorSystem& ctx) { 
        const auto* loggerSettings = ctx.LoggerSettings(); 
        return loggerSettings && loggerSettings->Satisfies(NActors::NLog::PRI_DEBUG, WilsonComponentId);
    } 
 
    template <typename TActorSystem, typename TEvent>
    void TraceEvent(const TActorSystem& actorSystem, TTraceId* traceId, TEvent&& event, TInstant timestamp) {
        // ensure that we are not using obsolete TraceId
        traceId->CheckConsistency();

        // store parent id (for logging) and generate child trace id
        TTraceId parentTraceId(std::move(*traceId));
        *traceId = parentTraceId.Span();

        // create encoded string buffer containing timestamp
        const ui64 timestampValue = timestamp.GetValue();
        const size_t base64size = Base64EncodeBufSize(sizeof(timestampValue));
        char base64[base64size];
        char* end = Base64Encode(base64, reinterpret_cast<const ui8*>(&timestampValue), sizeof(timestampValue));

        // cut trailing padding character to save some space
        Y_VERIFY(end > base64 && end[-1] == '=');
        --end;

        // generate log record
        TString finalMessage;
        TStringOutput s(finalMessage);
        s << GetNodeId(actorSystem) << " " << TStringBuf(base64, end) << " ";
        traceId->Output(s, parentTraceId);
        s << " ";
        event.Output(s);

        // output wilson event FIXME: special facility for wilson events w/binary serialization
        NActors::MemLogAdapter(actorSystem, NActors::NLog::PRI_DEBUG, WilsonComponentId, std::move(finalMessage));
    }

#else

#define DECLARE_WILSON_EVENT(...)
#define WILSON_TRACE(...)

#endif

} // NWilson
