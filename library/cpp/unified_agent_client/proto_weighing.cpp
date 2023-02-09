#include "proto_weighing.h"

#include <google/protobuf/io/coded_stream.h>

namespace NUnifiedAgent::NPW {
    template <typename T>
    inline size_t SizeOf(T value);

    using CodedOutputStream = google::protobuf::io::CodedOutputStream;

    template <>
    inline size_t SizeOf(ui64 value) {
        return CodedOutputStream::VarintSize64(value);
    }

    template <>
    inline size_t SizeOf(ui32 value) {
        return CodedOutputStream::VarintSize32(value);
    }

    template <>
    inline size_t SizeOf(i64 value) {
        return CodedOutputStream::VarintSize64(static_cast<google::protobuf::uint64>(value));
    }

    TFieldLink::TFieldLink(TLengthDelimited* container, bool repeated, size_t keySize)
        : Container(container)
        , OuterSize(0)
        , Repeated(repeated)
        , KeySize(keySize)
    {
    }

    void TFieldLink::SetValueSize(bool empty, size_t size) {
        const auto newOuterSize = empty && !Repeated ? 0 : KeySize + static_cast<int>(size);
        Container->IncSize(newOuterSize - OuterSize);
        OuterSize = newOuterSize;
    }

    TLengthDelimited::TLengthDelimited(const TFMaybe<TFieldLink>& link)
        : Link(link)
        , ByteSize(0)
    {
    }

    void TLengthDelimited::IncSize(int sizeDelta) {
        ByteSize += sizeDelta;
        if (Link) {
            const auto byteSize = static_cast<ui32>(ByteSize);
            Link->SetValueSize(false, byteSize + SizeOf(byteSize));
        }
    }

    template <typename T>
    void TRepeatedField<T>::Add(T value) {
        IncSize(static_cast<int>(SizeOf(value)));
    }

    template <typename T>
    TNumberField<T>::TNumberField(const TFieldLink& link)
        : Link(link)
    {
    }

    template <typename T>
    void TNumberField<T>::SetValue(T value) {
        Link.SetValueSize(value == 0, SizeOf(value));
    }

    template <typename T>
    TFixedNumberField<T>::TFixedNumberField(const TFieldLink& link)
        : Link(link)
    {
    }

    template <typename T>
    void TFixedNumberField<T>::SetValue() {
        Link.SetValueSize(false, sizeof(T));
    }

    TStringField::TStringField(const TFieldLink& link)
        : Link(link)
    {
    }

    void TStringField::SetValue(const TString& value) {
        Link.SetValueSize(value.Empty(), value.Size() + SizeOf(static_cast<ui32>(value.Size())));
    }

    template class TNumberField<ui64>;
    template class TNumberField<ui32>;
    template class TNumberField<i64>;
    template class TFixedNumberField<ui64>;
    template class TFixedNumberField<ui32>;
    template class TFixedNumberField<i64>;
    template class TRepeatedField<ui64>;
    template class TRepeatedField<ui32>;
    template class TRepeatedField<i64>;
}
