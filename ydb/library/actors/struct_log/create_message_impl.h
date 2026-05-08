#pragma once

#include "create_message.h"
#include "key_name.h"

#include <ydb/core/base/id_wrapper.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/maybe.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/generated_enum_reflection.h>
#include <google/protobuf/generated_enum_util.h>
#include <google/protobuf/repeated_ptr_field.h>
#include <utility>

namespace NActors::NStructuredLog {

template <typename>
static constexpr bool IsMaybeImpl = false;
template <typename T>
static constexpr bool IsMaybeImpl<TMaybe<T>> = true;
template <typename T>
static constexpr bool IsMaybe = IsMaybeImpl<std::decay_t<T>>;

template <typename>
static constexpr bool IsOptionalImpl = false;
template <typename T>
static constexpr bool IsOptionalImpl<std::optional<T>> = true;
template <typename T>
static constexpr bool IsOptional = IsOptionalImpl<std::decay_t<T>>;

template <typename>
static constexpr bool IsPointerImpl = false;
template <>
static constexpr bool IsPointerImpl<char*> = false;
template <>
static constexpr bool IsPointerImpl<const char*> = false;
template <typename T>
static constexpr bool IsPointerImpl<std::shared_ptr<T>> = true;
template <typename T>
static constexpr bool IsPointerImpl<std::unique_ptr<T>> = true;
template <typename T>
static constexpr bool IsPointerImpl<T*> = true;

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

    template<typename T> struct TIsIdWrapper { static constexpr bool value = false; };
    template<typename TType, typename TTag> struct TIsIdWrapper<TIdWrapper<TType, TTag>> { static constexpr bool value = true; };

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
        } else if constexpr (std::is_same_v<Tx, bool>) {
            s << (value ? "true" : "false");
        } else if constexpr (std::is_base_of_v<google::protobuf::Message, Tx>) {
            OutputProtobufMessage(s, value);
        } else if constexpr (THasToStringMethod<Tx>::value) {
            s << value.ToString();
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
            std::apply([&](const auto&... args) { OutputParam(s, args...); }, value);
            s << ']';
        } else {
            s << value;
        }
    }

    template <typename TValue, typename... TRest>
    static void OutputParam(IOutputStream& s, const TValue& first, const TRest&... rest) {
        OutputParam(s, first);
        s << ':';
        OutputParam(s, rest...);
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
        } else if constexpr (IsPointerImpl<T>) {
            TStringStream stream;
            if (value != nullptr) {
                OutputParam(stream, *value);
            } else {
                stream << "<null>";
            }
            TCreateMessageGuard::GetBuildMessage().AppendValue({std::move(name)}, stream.Str());
        } else if constexpr (IsMaybe<T>) {
            TStringStream stream;
            if (value.Defined()) {
                OutputParam(stream, value.GetRef());
            } else {
                stream << "<null>";
            }
            TCreateMessageGuard::GetBuildMessage().AppendValue({std::move(name)}, stream.Str());
        } else if constexpr (IsOptional<T>) {
            TStringStream stream;
            if (value.has_value()) {
                OutputParam(stream, value.value());
            } else {
                stream << "<null>";
            }
            TCreateMessageGuard::GetBuildMessage().AppendValue({std::move(name)}, stream.Str());
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
