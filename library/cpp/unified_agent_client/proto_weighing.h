#pragma once

#include <library/cpp/unified_agent_client/f_maybe.h>

#include <util/generic/deque.h>
#include <util/generic/string.h>

namespace NUnifiedAgent::NPW {
    class TLengthDelimited;

    class TFieldLink {
    public:
        TFieldLink(TLengthDelimited* container, bool repeated = false, size_t keySize = 1);

        void SetValueSize(bool empty, size_t size);

    private:
        TLengthDelimited* Container;
        int OuterSize;
        bool Repeated;
        size_t KeySize;
    };

    class TLengthDelimited {
    public:
        explicit TLengthDelimited(const TFMaybe<TFieldLink>& link = Nothing());

        void IncSize(int sizeDelta);

        size_t ByteSizeLong() const {
            return static_cast<size_t>(ByteSize);
        }

    private:
        TFMaybe<TFieldLink> Link;
        int ByteSize;
    };

    using TMessage = TLengthDelimited;

    template <typename T>
    class TRepeatedField: public TLengthDelimited {
    public:
        static_assert(std::is_same_v<T, ui32> ||
                      std::is_same_v<T, ui64> ||
                      std::is_same_v<T, i64>,
                      "type is not supported");

        using TLengthDelimited::TLengthDelimited;

        void Add(T value);
    };

    template <typename T>
    class TRepeatedPtrField {
    public:
        explicit TRepeatedPtrField(TMessage* message, size_t keySize = 1)
            : Message(message)
            , Children()
            , KeySize(keySize)
        {
        }

        size_t GetSize() const {
            return Children.size();
        }

        T& Get(size_t index) {
            return Children[index];
        }

        T& Add() {
            if constexpr (std::is_constructible<T, TFieldLink>::value) {
                Children.emplace_back(TFieldLink(Message, true, KeySize));
            } else {
                Children.emplace_back(Message);
            }
            return Children.back();
        }

    private:
        TMessage* Message;
        TDeque<T> Children;
        size_t KeySize;
    };

    template <typename T>
    class TNumberField {
    public:
        static_assert(std::is_same_v<T, ui32> ||
                      std::is_same_v<T, ui64> ||
                      std::is_same_v<T, i64>,
                      "type is not supported");

        explicit TNumberField(const TFieldLink& link);

        void SetValue(T value);

    private:
        TFieldLink Link;
    };

    template <typename T>
    class TFixedNumberField {
    public:
        static_assert(std::is_same_v<T, ui32> ||
                      std::is_same_v<T, ui64> ||
                      std::is_same_v<T, i64>,
                      "type is not supported");

        explicit TFixedNumberField(const TFieldLink& link);

        void SetValue();

    private:
        TFieldLink Link;
    };

    class TStringField {
    public:
        explicit TStringField(const TFieldLink& link);

        void SetValue(const TString& value);

    private:
        TFieldLink Link;
    };

    extern template class TNumberField<ui64>;
    extern template class TNumberField<ui32>;
    extern template class TNumberField<i64>;
    extern template class TFixedNumberField<ui64>;
    extern template class TFixedNumberField<ui32>;
    extern template class TFixedNumberField<i64>;
    extern template class TRepeatedField<ui64>;
    extern template class TRepeatedField<ui32>;
    extern template class TRepeatedField<i64>;
}
