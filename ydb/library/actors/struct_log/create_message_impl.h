#pragma once

#include "create_message.h"
#include "key_name.h"

#include <ydb/core/base/id_wrapper.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/maybe.h>
#include <util/string/join.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/generated_enum_reflection.h>
#include <google/protobuf/generated_enum_util.h>
#include <google/protobuf/repeated_ptr_field.h>

#include <utility>

namespace NActors::NStructuredLog {

class TCreateMessageArg {
public:
    TCreateMessageArg() = default;

    static void OutputProtobufMessage(IOutputStream& s, const google::protobuf::Message& value);
    static void OutputProtobufEnum(IOutputStream& s, int enumValue, const google::protobuf::EnumDescriptor* descriptor);

    template <typename T>
    class THasToStringMethod {
        // check the signature if it exists
        template <typename X> static constexpr typename std::is_same<decltype(&X::ToString), TString(X::*)()const>::type check(int);
        // in case when there is no such signature
        template <typename>   static constexpr std::false_type check(...);
    public:
        static constexpr bool value = decltype(check<T>(0))::value;
    };

    template<typename Tx> struct TOptionalTraits { static constexpr bool HasOptionalValue = false; };
    template<> struct TOptionalTraits<const char*> { static constexpr bool HasOptionalValue = false; };
    template<> struct TOptionalTraits<char*> { static constexpr bool HasOptionalValue = false; };
    template<typename Tx> struct TOptionalTraits<std::optional<Tx>> { static constexpr bool HasOptionalValue = true; };
    template<typename Tx> struct TOptionalTraits<TMaybe<Tx>> { static constexpr bool HasOptionalValue = true; };
    template<typename Tx> struct TOptionalTraits<Tx*> { static constexpr bool HasOptionalValue = true; };
    template<typename... Ts> struct TOptionalTraits<std::unique_ptr<Ts...>> { static constexpr bool HasOptionalValue = true; };
    template<typename... Ts> struct TOptionalTraits<std::shared_ptr<Ts...>> { static constexpr bool HasOptionalValue = true; };

    template<typename T> struct TIsIterable { static constexpr bool value = false; };
    template<typename T, size_t S> struct TIsIterable<std::span<T, S>> { static constexpr bool value = true; };
    template<typename T, typename Y> struct TIsIterable<std::deque<T, Y>> { static constexpr bool value = true; };
    template<typename T, typename Y> struct TIsIterable<std::list<T, Y>> { static constexpr bool value = true; };
    template<typename T, typename Y> struct TIsIterable<std::vector<T, Y>> { static constexpr bool value = true; };
    template<typename T, typename Y> struct TIsIterable<TVector<T, Y>> { static constexpr bool value = true; };
    template<typename T, typename X, typename Y, typename Z> struct TIsIterable<THashSet<T, X, Y, Z>> { static constexpr bool value = true; };
    template<typename... Ts> struct TIsIterable<std::set<Ts...>> { static constexpr bool value = true; };
    template<typename... Ts> struct TIsIterable<std::unordered_set<Ts...>> { static constexpr bool value = true; };
    template<typename T> struct TIsIterable<NProtoBuf::RepeatedField<T>> { static constexpr bool value = true; };
    template<typename T> struct TIsIterable<NProtoBuf::RepeatedPtrField<T>> { static constexpr bool value = true; };

    template<typename T> struct TIsIterableKV { static constexpr bool value = false; };
    template<typename... Ts> struct TIsIterableKV<THashMap<Ts...>> { static constexpr bool value = true; };
    template<typename... Ts> struct TIsIterableKV<TMap<Ts...>> { static constexpr bool value = true; };
    template<typename... Ts> struct TIsIterableKV<std::map<Ts...>> { static constexpr bool value = true; };
    template<typename... Ts> struct TIsIterableKV<std::unordered_map<Ts...>> { static constexpr bool value = true; };

    template<typename T> struct TIsVariant { static constexpr bool value = false; };
    template<typename... Ts> struct TIsVariant<std::variant<Ts...>> { static constexpr bool value = true; };

    template<typename T> struct TIsTuple { static constexpr bool value = false; };
    template<typename... Ts> struct TIsTuple<std::tuple<Ts...>> { static constexpr bool value = true; };

    template<typename TValue>
    static void OutputParam(IOutputStream& s, const TValue& value) {
        using Tx = std::decay_t<TValue>;

        if constexpr (google::protobuf::is_proto_enum<Tx>::value) {
            const google::protobuf::EnumDescriptor* e = google::protobuf::GetEnumDescriptor<Tx>();
            OutputProtobufEnum(s, static_cast<int>(value), e);
        } else if constexpr (std::is_base_of_v<google::protobuf::Message, Tx>) {
            OutputProtobufMessage(s, value);
        } else if constexpr (std::is_same_v<Tx, bool>) {
            s << (value ? "true" : "false");
        } else if constexpr (THasToStringMethod<Tx>::value) {
            s << value.ToString();
        } else if constexpr (TOptionalTraits<Tx>::HasOptionalValue) {
            if (value) {
                OutputParam(s, *value);
            } else {
                s << "<null>";
            }
        } else if constexpr (TIsIterable<Tx>::value) {
            s << "[" << JoinSeq(", ", value) << "]";
        } else if constexpr (TIsIterableKV<Tx>::value) {
            s << '{';
            for (bool first = true; const auto& [k, v] : value) {
                if (first) {
                    first = false;
                } else {
                    s << ' ';
                }
                OutputParam(s, k);
                s << ':';
                OutputParam(s, v);
            }
            s << '}';
        } else if constexpr (TIsVariant<Tx>::value) {
            std::visit([&](auto& x) { OutputParam(s, x); }, value);
        } else if constexpr (TIsTuple<Tx>::value) {
            s << '[';
            std::apply([&](const auto&... args) { s << Join(":", args...); }, value);
            s << ']';
        } else {
            s << value;
        }
    }

    // Native types support
    template <typename T, typename K = TKeyName>
    TCreateMessageArg(K&& name, const T& value) {
        if constexpr (std::is_same<T, TStructuredMessage>::value) {
            TCreateMessageGuard::GetBuildMessage().AppendSubMessage({std::move(name)}, value);
        } else if constexpr (std::is_same<T, TMaybe<TStructuredMessage>>::value) {
            if (value.Defined()) {
                TCreateMessageGuard::GetBuildMessage().AppendSubMessage({std::move(name)}, value.GetRef());
            }
        } else if constexpr (TNativeTypeSupport<T>::value) {
            TCreateMessageGuard::GetBuildMessage().AppendValue({std::move(name)}, value);
        } else {
            TStringStream stream;
            OutputParam(stream, value);
            TCreateMessageGuard::GetBuildMessage().AppendValue({std::move(name)}, stream.Str());
        }
    }

    TCreateMessageArg(const TStructuredMessage& message);
    TCreateMessageArg(const TMaybe<TStructuredMessage>& message);

    TCreateMessageArg(const TCreateMessageArg&) = delete;
    TCreateMessageArg(TCreateMessageArg&&) = delete;
    TCreateMessageArg& operator=(const TCreateMessageArg&) = delete;
    TCreateMessageArg& operator=(TCreateMessageArg&&) = delete;

    void* operator new(std::size_t sz) = delete;
    void* operator new[](std::size_t sz) = delete;
};

}  // namespace NActors::NStructuredLog
