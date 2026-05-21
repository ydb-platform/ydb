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

    template <typename T>
    class THasToStructuredMessageMethod {
        // check the signature if it exists
        template <typename X> static constexpr typename std::is_same<decltype(&X::ToStructuredMessage), TStructuredMessage(X::*)()const>::type check(int);
        // in case when there is no such signature
        template <typename>   static constexpr std::false_type check(...);
    public:
        static constexpr bool value = decltype(check<T>(0))::value;
    };

    template <typename Tx> struct TOptionalTraits { static constexpr bool HasOptionalValue = false; };
    template <> struct TOptionalTraits<const char*> { static constexpr bool HasOptionalValue = false; };
    template <> struct TOptionalTraits<char*> { static constexpr bool HasOptionalValue = false; };
    template <> struct TOptionalTraits<const void*> { static constexpr bool HasOptionalValue = false; };
    template <> struct TOptionalTraits<void*> { static constexpr bool HasOptionalValue = false; };
    template <typename Tx> struct TOptionalTraits<std::optional<Tx>> { static constexpr bool HasOptionalValue = true; };
    template <typename Tx> struct TOptionalTraits<TMaybe<Tx>> { static constexpr bool HasOptionalValue = true; };
    template <typename Tx> struct TOptionalTraits<Tx*> { static constexpr bool HasOptionalValue = true; };
    template <typename... Ts> struct TOptionalTraits<std::unique_ptr<Ts...>> { static constexpr bool HasOptionalValue = true; };
    template <typename... Ts> struct TOptionalTraits<std::shared_ptr<Ts...>> { static constexpr bool HasOptionalValue = true; };

    template <typename T> struct TIsIterable { static constexpr bool value = false; };
    template <typename T, size_t S> struct TIsIterable<std::span<T, S>> { static constexpr bool value = true; };
    template <typename T, typename Y> struct TIsIterable<std::deque<T, Y>> { static constexpr bool value = true; };
    template <typename T, typename Y> struct TIsIterable<std::list<T, Y>> { static constexpr bool value = true; };
    template <typename T, typename Y> struct TIsIterable<std::vector<T, Y>> { static constexpr bool value = true; };
    template <typename T, typename Y> struct TIsIterable<TVector<T, Y>> { static constexpr bool value = true; };
    template <typename T, typename X, typename Y, typename Z> struct TIsIterable<THashSet<T, X, Y, Z>> { static constexpr bool value = true; };
    template <typename... Ts> struct TIsIterable<std::set<Ts...>> { static constexpr bool value = true; };
    template <typename... Ts> struct TIsIterable<std::unordered_set<Ts...>> { static constexpr bool value = true; };
    template <typename T> struct TIsIterable<NProtoBuf::RepeatedField<T>> { static constexpr bool value = true; };
    template <typename T> struct TIsIterable<NProtoBuf::RepeatedPtrField<T>> { static constexpr bool value = true; };

    template <typename T> struct TIsIterableKV { static constexpr bool value = false; };
    template <typename... Ts> struct TIsIterableKV<THashMap<Ts...>> { static constexpr bool value = true; };
    template <typename... Ts> struct TIsIterableKV<TMap<Ts...>> { static constexpr bool value = true; };
    template <typename... Ts> struct TIsIterableKV<std::map<Ts...>> { static constexpr bool value = true; };
    template <typename... Ts> struct TIsIterableKV<std::unordered_map<Ts...>> { static constexpr bool value = true; };

    template <typename T> struct TIsVariant { static constexpr bool value = false; };
    template <typename... Ts> struct TIsVariant<std::variant<Ts...>> { static constexpr bool value = true; };

    template <typename T> struct TIsTuple { static constexpr bool value = false; };
    template <typename... Ts> struct TIsTuple<std::tuple<Ts...>> { static constexpr bool value = true; };

    // TCreateMessageArg doesn't use Out<T> and uses own mechanics to convert various value types into string representation, but
    // it is uses Out<T> to convert simple standard types ultimately.
    // See details follow.
    template <typename TValue>
    static void OutputParam(IOutputStream& s, const TValue& value) {
        using Tx = std::decay_t<TValue>;

        // Firstly, it is required to convert protobuf enums and messages into string
        if constexpr (google::protobuf::is_proto_enum<Tx>::value) {
            const google::protobuf::EnumDescriptor* e = google::protobuf::GetEnumDescriptor<Tx>();
            OutputProtobufEnum(s, static_cast<int>(value), e);
        } else if constexpr (std::is_base_of_v<google::protobuf::Message, Tx>) {
            OutputProtobufMessage(s, value);
        } else if constexpr (std::is_same_v<Tx, bool>) {
            // It is better to write "true/false" strings into log (instead of "1/0")
            s << (value ? "true" : "false");
        } else if constexpr (THasToStringMethod<Tx>::value) {
            // By default, Out<T> can't write classes with ToString() method. (see TStateStorageInfo as example)
            // Because of this, OutputParam must be able to process various standard containers, variants, tuples, etc...
            s << value.ToString();
        } else if constexpr (TOptionalTraits<Tx>::HasOptionalValue) {
            // YDB uses several ways to store/pass optional values (see TOptionalTraits<T> below).
            // So, it is required to process optional data using this OutputParam<TValue> (instead of Out<T>).
            if (value) {
                OutputParam(s, *value);
            } else {
                s << "<null>";
            }
        } else if constexpr (TIsIterable<Tx>::value) {
            // It is unable to use JoinSeq(value) because of container item must be processed by OutputParam<TValue> (instead of Out<T>).
            // As example - std::vector<NKikimr::NBlobDepot::TS3Locator>, where TS3Locator has ToString
            auto begin = std::begin(value);
            auto end = std::end(value);
            bool first = true;
            s << "[";
            for (; begin != end; ++begin) {
                if (first) {
                    first = false;
                } else {
                    s << ", ";
                }
                OutputParam(s, *begin);
            }
            s << "]";
        } else if constexpr (TIsIterableKV<Tx>::value) {
            // It is required to use self-made code because of container item must be processed by OutputParam<TValue> (instead of Out<T>).
            // As example - std::vector<NKikimr::NBlobDepot::TS3Locator>, where TS3Locator has ToString() method
            s << '{';
            for (bool first = true; const auto& [k, v] : value) {
                if (first) {
                    first = false;
                } else {
                    s << ", ";
                }
                OutputParam(s, k);
                s << ':';
                OutputParam(s, v);
            }
            s << '}';
        } else if constexpr (TIsVariant<Tx>::value) {
            // It is required to use self-made code because of variant item must be processed by OutputParam<TValue> (instead of Out<T>).
            // as example - NKikimr::NStorage::TDistributedConfigKeeper::TBinding is used as variant item in distconf_scatter_gather.cpp
            std::visit([&](auto& x) { OutputParam(s, x); }, value);
        } else if constexpr (TIsTuple<Tx>::value) {
            // It is required to use self-made code because of tuple items must be processed by OutputParam<TValue> (instead of Out<T>).
            // See  distconf_binding.cpp as example. Is uses TNodeLocation inside tuple, and TNodeLocation has ToString method
            s << '(';
            std::apply([&](const auto&... args) { OutputParam(s, args...); }, value);
            s << ')';
        } else {
            // Finally, OutputParam<T> uses Out<T> to process simple standard types
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
        } else if constexpr (THasToStructuredMessageMethod<std::decay_t<T>>::value) {
            auto message = value.ToStructuredMessage();
            TCreateMessageGuard::GetBuildMessage().AppendSubMessage({std::move(name)}, message);
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
