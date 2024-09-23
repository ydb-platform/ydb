#pragma once

#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/base/blobstorage_common.h>
#include <library/cpp/json/json_writer.h>
#include <google/protobuf/text_format.h>

// special hack for gcc
static struct STLOG_PARAM_T {} STLOG_PARAM;

namespace NKikimr::NStLog {

    static constexpr bool OutputLogJson = false;

    void ProtobufToJson(const NProtoBuf::Message& m, NJson::TJsonWriter& json);

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

#define STLOG_STREAM(NAME, JSON_OVERRIDE, MARKER, TEXT, ...) \
    struct MARKER {}; \
    TStringStream NAME; \
    if constexpr (JSON_OVERRIDE ? JSON_OVERRIDE > 0 : ::NKikimr::NStLog::OutputLogJson) { \
        NJson::TJsonWriter __json(&NAME, false); \
        ::NKikimr::NStLog::TMessage<MARKER>(__FILE__, __LINE__, #MARKER)STLOG_PARAMS(__VA_ARGS__).WriteToJson(__json) << TEXT; \
    } else { \
        ::NKikimr::NStLog::TMessage<MARKER>(__FILE__, __LINE__, #MARKER)STLOG_PARAMS(__VA_ARGS__).WriteToStream(NAME) << TEXT; \
    }

#define STLOGX(CTX, PRIO, COMP, ...) \
    do { \
        auto& ctx = (CTX); \
        const auto priority = [&]{ using namespace NActors::NLog; return (PRIO); }(); \
        const auto component = [&]{ using namespace NKikimrServices; using namespace NActorsServices; return (COMP); }(); \
        if (IS_LOG_PRIORITY_ENABLED(priority, component)) { \
            STLOG_STREAM(__stream, 0, __VA_ARGS__); \
            ::NActors::MemLogAdapter(ctx, priority, component, __stream.Str()); \
        }; \
    } while (false)

#define STLOG(...) if (TActivationContext *ctxp = TlsActivationContext; !ctxp); else STLOGX(*ctxp, __VA_ARGS__)

#define STLOGJX(CTX, PRIO, COMP, ...) \
    do { \
        auto& ctx = (CTX); \
        const auto priority = [&]{ using namespace NActors::NLog; return (PRIO); }(); \
        const auto component = [&]{ using namespace NKikimrServices; using namespace NActorsServices; return (COMP); }(); \
        if (IS_LOG_PRIORITY_ENABLED(priority, component)) { \
            STLOG_STREAM(__stream, 1, __VA_ARGS__); \
            ::NActors::MemLogAdapter(ctx, priority, component, __stream.Str()); \
        }; \
    } while (false)

#define STLOGJ(...) if (TActivationContext *ctxp = TlsActivationContext; !ctxp); else STLOGJX(*ctxp, __VA_ARGS__)

#define STLOG_DEBUG_FAIL(COMP, ...) \
    do { \
        if (TActivationContext *ctxp = TlsActivationContext) { \
            const auto component = [&]{ using namespace NKikimrServices; using namespace NActorsServices; return (COMP); }(); \
            STLOG_STREAM(__stream, 0, __VA_ARGS__); \
            const TString message = __stream.Str(); \
            Y_VERIFY_DEBUG_S(false, message); \
            LOG_LOG_S(*ctxp, NLog::PRI_CRIT, component, message); \
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

    template<typename T> struct TIsIdWrapper { static constexpr bool value = false; };
    template<typename TType, typename TTag> struct TIsIdWrapper<TIdWrapper<TType, TTag>> { static constexpr bool value = true; };

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
            OutputParam(s << "# ", Value);
        }

        void WriteToJson(NJson::TJsonWriter& json) const {
            Base::WriteToJson(json);
            OutputParam(json, Value);
        }

    private:
        template<typename Tx> struct TOptionalTraits { static constexpr bool HasOptionalValue = false; };
        template<> struct TOptionalTraits<const char*> { static constexpr bool HasOptionalValue = false; };
        template<> struct TOptionalTraits<char*> { static constexpr bool HasOptionalValue = false; };
        template<typename Tx> struct TOptionalTraits<std::optional<Tx>> { static constexpr bool HasOptionalValue = true; };
        template<typename Tx> struct TOptionalTraits<TMaybe<Tx>> { static constexpr bool HasOptionalValue = true; };
        template<typename Tx> struct TOptionalTraits<Tx*> { static constexpr bool HasOptionalValue = true; };

        template<typename TValue>
        static void OutputParam(IOutputStream& s, const TValue& value) {
            using Tx = std::decay_t<TValue>;

            if constexpr (google::protobuf::is_proto_enum<Tx>::value) {
                const google::protobuf::EnumDescriptor *e = google::protobuf::GetEnumDescriptor<Tx>();
                if (const auto *val = e->FindValueByNumber(value)) {
                    s << val->name();
                } else {
                    s << static_cast<int>(value);
                }
            } else if constexpr (std::is_same_v<Tx, bool>) {
                s << (value ? "true" : "false");
            } else if constexpr (std::is_base_of_v<google::protobuf::Message, Tx>) {
                google::protobuf::TextFormat::Printer p;
                p.SetSingleLineMode(true);
                TString str;
                if (p.PrintToString(value, &str)) {
                    s << "{" << str << "}";
                } else {
                    s << "<error>";
                }
            } else if constexpr (THasToStringMethod<Tx>::value) {
                s << value.ToString();
            } else if constexpr (TOptionalTraits<Tx>::HasOptionalValue) {
                if (value) {
                    OutputParam(s, *value);
                } else {
                    s << "<null>";
                }
            } else if constexpr (TIsIterable<Tx>::value) {
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

        template<typename TValue>
        static void OutputParam(NJson::TJsonWriter& json, const TValue& value) {
            using Tx = std::decay_t<TValue>;

            if constexpr (google::protobuf::is_proto_enum<Tx>::value) {
                const google::protobuf::EnumDescriptor *e = google::protobuf::GetEnumDescriptor<Tx>();
                if (const auto *val = e->FindValueByNumber(value)) {
                    json.Write(val->name());
                } else {
                    json.Write(static_cast<int>(value));
                }
            } else if constexpr (std::is_base_of_v<google::protobuf::Message, Tx>) {
                ProtobufToJson(value, json);
            } else if constexpr (TOptionalTraits<Tx>::HasOptionalValue) {
                if (value) {
                    OutputParam(json, *value);
                } else {
                    json.WriteNull();
                }
            } else if constexpr (TIsIterable<Tx>::value) {
                json.OpenArray();
                auto begin = std::begin(value);
                auto end = std::end(value);
                for (; begin != end; ++begin) {
                    OutputParam(json, *begin);
                }
                json.CloseArray();
            } else if constexpr (TIsIdWrapper<Tx>::value){
                json.Write(value.GetRawId());
            } else if constexpr (std::is_constructible_v<NJson::TJsonValue, Tx>) {
                json.Write(value);
            } else {
                TStringStream stream;
                OutputParam(stream, value);
                json.Write(stream.Str());
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

        void WriteToJson(NJson::TJsonWriter& json) const {
            json.WriteKey(Name);
        }
    };

    template<typename Tag, typename... TParams>
    class TMessage {
        const char *File;
        int Line;
        const char *Marker;
        std::tuple<TParams...> Params;
        static constexpr size_t NumParams = sizeof...(TParams);

    public:
        template<typename... TArgs>
        TMessage(const char *file, int line, const char *marker, std::tuple<TParams...>&& params = {})
            : File(file)
            , Line(line)
            , Marker(marker)
            , Params(std::move(params))
        {}

        template<typename Base, typename T>
        TMessage<Tag, TParams..., TBoundParam<Base, T>> AppendBoundParam(TBoundParam<Base, T>&& p) {
            return {File, Line, Marker, std::tuple_cat(Params, std::make_tuple(std::move(p)))};
        }

        TMessage<Tag, TParams...> AppendBoundParam(const STLOG_PARAM_T&) {
            return std::move(*this);
        }

        struct TStreamWriter {
            const TMessage *Self;
            IOutputStream& Stream;

            template<typename T>
            TStreamWriter& operator <<(const T& value) {
                Stream << value;
                return *this;
            }

            TStreamWriter(const TMessage *self, IOutputStream& stream)
                : Self(self)
                , Stream(stream)
            {
                Self->WriteHeaderToStream(Stream);
            }

            ~TStreamWriter() {
                Self->WriteParamsToStream(Stream);
            }
        };

        TStreamWriter WriteToStream(IOutputStream& s) const {
            return {this, s};
        }

        struct TJsonWriter {
            const TMessage *Self;
            NJson::TJsonWriter& Json;
            TStringStream Stream;

            template<typename T>
            TJsonWriter& operator <<(const T& value) {
                Stream << value;
                return *this;
            }

            TJsonWriter(const TMessage *self, NJson::TJsonWriter& json)
                : Self(self)
                , Json(json)
            {}

            ~TJsonWriter() {
                Json.OpenMap();
                if (Self->Header()) {
                    Json.WriteKey("Marker");
                    Json.Write(Self->Marker);
                    Json.WriteKey("File");
                    Json.Write(Self->GetFileName());
                    Json.WriteKey("Line");
                    Json.Write(Self->Line);
                }
                Json.WriteKey("Text");
                Json.Write(Stream.Str());
                Self->WriteParamsToJson(Json);
                Json.CloseMap();
            }
        };

        TJsonWriter WriteToJson(NJson::TJsonWriter& json) const {
            return {this, json};
        }

    private:
        bool Header() const {
            return *File;
        }

        const char *GetFileName() const {
            const char *p = strrchr(File, '/');
            return p ? p + 1 : File;
        }

        void WriteHeaderToStream(IOutputStream& s) const {
            s << "{" << Marker << "@" << GetFileName() << ":" << Line << "} ";
        }

        void WriteParamsToStream(IOutputStream& s) const {
            WriteParams<0>(&s);
        }

        void WriteParamsToJson(NJson::TJsonWriter& json) const {
            WriteParams<0>(&json);
        }
        
        template<size_t Index, typename = std::enable_if_t<Index != NumParams>>
        void WriteParams(IOutputStream *s) const {
            std::get<Index>(Params).WriteToStream(*s << " ");
            WriteParams<Index + 1>(s);
        }

        template<size_t Index, typename = std::enable_if_t<Index != NumParams>>
        void WriteParams(NJson::TJsonWriter *json) const {
            std::get<Index>(Params).WriteToJson(*json);
            WriteParams<Index + 1>(json);
        }

        template<size_t Index>
        void WriteParams(...) const {}
    };

}
