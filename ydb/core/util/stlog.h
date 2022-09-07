#pragma once

#include <ydb/core/protos/services.pb.h>
#include <library/cpp/actors/core/log.h>
#include <google/protobuf/text_format.h>

// special hack for gcc
static struct STLOG_PARAM_T {} STLOG_PARAM;

namespace NKikimr::NStLog {

#define STLOG_EXPAND(X) X

#ifdef _MSC_VER
#define STLOG_NARG(...) STLOG_NARG_S1(STLOG_NARG_WRAP(__VA_ARGS__))
#define STLOG_NARG_WRAP(...) X,__VA_ARGS__
#define STLOG_NARG_S1(...) STLOG_EXPAND(STLOG_NARG_IMPL(__VA_ARGS__,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0))
#else
#define STLOG_NARG(...) STLOG_NARG_IMPL(X,##__VA_ARGS__,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0)
#endif
#define STLOG_NARG_IMPL(X,P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11,P12,P13,P14,P15,P16,N,...) N

#define STLOG_PASTE(A, B) A ## _ ## B

#define STLOG_PARAM(NAME, VALUE) ::NKikimr::NStLog::TUnboundParam(#NAME) = VALUE

#define STLOG_PARAMS(...) STLOG_PARAMS_S1(STLOG_NARG(__VA_ARGS__), __VA_ARGS__)
#define STLOG_PARAMS_S1(N, ...) STLOG_EXPAND(STLOG_PASTE(STLOG_PARAMS, N)(__VA_ARGS__))

#define STLOG_PARAMS_0()
#define STLOG_PARAMS_1(KV) .AppendBoundParam(STLOG_PARAM KV)
#define STLOG_PARAMS_2(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_1(__VA_ARGS__))
#define STLOG_PARAMS_3(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_2(__VA_ARGS__))
#define STLOG_PARAMS_4(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_3(__VA_ARGS__))
#define STLOG_PARAMS_5(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_4(__VA_ARGS__))
#define STLOG_PARAMS_6(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_5(__VA_ARGS__))
#define STLOG_PARAMS_7(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_6(__VA_ARGS__))
#define STLOG_PARAMS_8(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_7(__VA_ARGS__))
#define STLOG_PARAMS_9(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_8(__VA_ARGS__))
#define STLOG_PARAMS_10(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_9(__VA_ARGS__))
#define STLOG_PARAMS_11(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_10(__VA_ARGS__))
#define STLOG_PARAMS_12(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_11(__VA_ARGS__))
#define STLOG_PARAMS_13(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_12(__VA_ARGS__))
#define STLOG_PARAMS_14(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_13(__VA_ARGS__))
#define STLOG_PARAMS_15(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_14(__VA_ARGS__))
#define STLOG_PARAMS_16(KV, ...) STLOG_PARAMS_1(KV) STLOG_EXPAND(STLOG_PARAMS_15(__VA_ARGS__))

#define STLOGX(CTX, PRIO, COMP, MARKER, TEXT, ...) \
    do { \
        auto getPrio = [&] { using namespace NActors::NLog; return (PRIO); }; \
        auto getComp = [&] { using namespace NKikimrServices; using namespace NActorsServices; return (COMP); }; \
        auto makeMessage = [&] { \
            struct MARKER {}; \
            using Tag = MARKER; \
            return ::NKikimr::NStLog::TMessage<Tag>(__FILE__, __LINE__, #MARKER, TStringBuilder() << TEXT) \
                STLOG_PARAMS(__VA_ARGS__); \
        }; \
        LOG_LOG_S((CTX), getPrio(), getComp(), makeMessage()); \
    } while (false)

#define STLOG(PRIO, COMP, MARKER, TEXT, ...) \
    do { \
        if (TActivationContext *ctxp = TlsActivationContext) { \
            auto getPrio = [&] { using namespace NActors::NLog; return (PRIO); }; \
            auto getComp = [&] { using namespace NKikimrServices; using namespace NActorsServices; return (COMP); }; \
            auto makeMessage = [&] { \
                struct MARKER {}; \
                using Tag = MARKER; \
                return ::NKikimr::NStLog::TMessage<Tag>(__FILE__, __LINE__, #MARKER, TStringBuilder() << TEXT) \
                    STLOG_PARAMS(__VA_ARGS__); \
            }; \
            LOG_LOG_S(*ctxp, getPrio(), getComp(), makeMessage()); \
        } \
    } while (false)

#define STLOG_DEBUG_FAIL(COMP, MARKER, TEXT, ...) \
    do { \
        if (TActivationContext *ctxp = TlsActivationContext) { \
            auto getComp = [&] { using namespace NKikimrServices; using namespace NActorsServices; return (COMP); }; \
            auto makeMessage = [&] { \
                struct MARKER {}; \
                using Tag = MARKER; \
                return ::NKikimr::NStLog::TMessage<Tag>(__FILE__, __LINE__, #MARKER, TStringBuilder() << TEXT) \
                    STLOG_PARAMS(__VA_ARGS__); \
            }; \
            Y_VERIFY_DEBUG_S(false, makeMessage()); \
            LOG_LOG_S(*ctxp, NLog::PRI_CRIT, getComp(), makeMessage()); \
        } \
    } while (false)

    template<typename T>
    class THasToStringMethod {
        // check the signature if it exists
        template<typename X> static constexpr typename std::is_same<decltype(&X::ToString), TString(X::*)()const>::type check(int);
        // in case when there is no such signature
        template<typename>   static constexpr std::false_type check(...);
    public:
        static constexpr bool value = decltype(check<T>(0))::value;
    };

    template<typename T> struct TIsIterable { static constexpr bool value = false; };
    template<typename T, typename Y> struct TIsIterable<std::deque<T, Y>> { static constexpr bool value = true; };
    template<typename T, typename Y> struct TIsIterable<std::list<T, Y>> { static constexpr bool value = true; };
    template<typename T, typename Y> struct TIsIterable<std::vector<T, Y>> { static constexpr bool value = true; };
    template<typename T> struct TIsIterable<NProtoBuf::RepeatedField<T>> { static constexpr bool value = true; };
    template<typename T> struct TIsIterable<NProtoBuf::RepeatedPtrField<T>> { static constexpr bool value = true; };

    template<typename Base, typename T>
    class TBoundParam : public Base {
        T Value;

    public:
        template<typename U>
        TBoundParam(const Base& base, U&& value)
            : Base(base)
            , Value(std::forward<U>(value))
        {}

        void WriteToStream(IOutputStream& s) const {
            Base::WriteToStream(s);
            s << "# ";
            OutputParam(s, Value);
        }

    private:
        template<typename TValue>
        static void OutputParam(IOutputStream& s, const std::optional<TValue>& value) {
            if (value) {
                OutputParam(s, *value);
            } else {
                s << "<null>";
            }
        }

        template<typename TValue>
        static void OutputParam(IOutputStream& s, const TValue& value) {
            if constexpr (google::protobuf::is_proto_enum<TValue>::value) {
                const google::protobuf::EnumDescriptor *e = google::protobuf::GetEnumDescriptor<TValue>();
                if (const auto *val = e->FindValueByNumber(value)) {
                    s << val->name();
                } else {
                    s << static_cast<int>(value);
                }
            } else if constexpr (std::is_same_v<TValue, bool>) {
                s << (value ? "true" : "false");
            } else if constexpr (std::is_base_of_v<google::protobuf::Message, TValue>) {
                google::protobuf::TextFormat::Printer p;
                p.SetSingleLineMode(true);
                TString str;
                if (p.PrintToString(value, &str)) {
                    s << "{" << str << "}";
                } else {
                    s << "<error>";
                }
            } else if constexpr (THasToStringMethod<TValue>::value) {
                s << value.ToString();
            } else if constexpr (std::is_pointer_v<TValue> && !std::is_same_v<std::remove_cv_t<std::remove_pointer_t<TValue>>, char>) {
                if (value) {
                    OutputParam(s, *value);
                } else {
                    s << "<null>";
                }
            } else if constexpr (TIsIterable<TValue>::value) {
                auto begin = std::begin(value);
                auto end = std::end(value);
                bool first = true;
                s << "[";
                for (; begin != end; ++begin) {
                    if (first) {
                        first = false;
                    } else {
                        s << " ";
                    }
                    OutputParam(s, *begin);
                }
                s << "]";
            } else {
                s << value;
            }
        }
    };

    class TUnboundParam {
        const char *Name;

    public:
        TUnboundParam(const char *name)
            : Name(name)
        {}

        template<typename T>
        NKikimr::NStLog::TBoundParam<TUnboundParam, T> operator =(T&& value) {
            return {*this, std::forward<T>(value)};
        }

        void WriteToStream(IOutputStream& s) const {
            s << Name;
        }
    };

    template<typename Tag, typename... TParams>
    class TMessage {
        const char *File;
        int Line;
        const char *Marker;
        TString Text;
        std::tuple<TParams...> Params;
        static constexpr size_t NumParams = sizeof...(TParams);

    public:
        template<typename... TArgs>
        TMessage(const char *file, int line, const char *marker, TString text, std::tuple<TParams...>&& params = {})
            : File(file)
            , Line(line)
            , Marker(marker)
            , Text(std::move(text))
            , Params(std::move(params))
        {}

        template<typename Base, typename T>
        TMessage<Tag, TParams..., TBoundParam<Base, T>> AppendBoundParam(TBoundParam<Base, T>&& p) {
            return {File, Line, Marker, std::move(Text), std::tuple_cat(Params, std::make_tuple(std::move(p)))};
        }

        TMessage<Tag, TParams...> AppendBoundParam(const STLOG_PARAM_T&) {
            return std::move(*this);
        }

        void WriteToStream(IOutputStream& s) const {
            const char *p = strrchr(File, '/');
            p = p ? p + 1 : File;
            s << "{" << Marker << "@" << p << ":" << Line << "} " << Text;
            WriteParams<0>(s, nullptr);
        }

    private:
        template<size_t Index>
        void WriteParams(IOutputStream& s, std::enable_if_t<Index != NumParams>*) const {
            s << " ";
            std::get<Index>(Params).WriteToStream(s);
            WriteParams<Index + 1>(s, nullptr);
        }

        // out-of-range handler
        template<size_t Index>
        void WriteParams(IOutputStream&, ...) const {}
    };

}

template<typename Tag, typename... TParams>
IOutputStream& operator <<(IOutputStream& s, const NKikimr::NStLog::TMessage<Tag, TParams...>& message) {
    message.WriteToStream(s);
    return s;
}
