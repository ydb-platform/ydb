#include "node.h"
#include "yson.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <library/cpp/yson_pull/exceptions.h>
#include <library/cpp/yson_pull/reader.h>
#include <library/cpp/yson_pull/writer.h>

#include <util/string/builder.h>

namespace NYql::NDom {

using namespace NUdf;
using namespace NYsonPull;

namespace {

[[noreturn]] Y_NO_INLINE void UnexpectedEvent(EEventType ev) {
    UdfTerminate((::TStringBuilder() << "Unexpected event: " << ev).c_str());
}

TUnboxedValuePod ParseScalar(const TScalar& scalar, const IValueBuilder* valueBuilder) {
    switch (scalar.Type()) {
        case EScalarType::Entity:
            return MakeEntity();

        case EScalarType::Boolean:
            return MakeBool(scalar.AsBoolean());

        case EScalarType::Int64:
            return MakeInt64(scalar.AsInt64());

        case EScalarType::UInt64:
            return MakeUint64(scalar.AsUInt64());

        case EScalarType::Float64:
            return MakeDouble(scalar.AsFloat64());

        case EScalarType::String:
            return MakeString(scalar.AsString(), valueBuilder);
    }
}

TUnboxedValue ParseAttributes(TReader& reader, const IValueBuilder* valueBuilder);
TUnboxedValue ParseDict(TReader& reader, const IValueBuilder* valueBuilder);

TUnboxedValue ParseList(TReader& reader, const IValueBuilder* valueBuilder) {
    TSmallVec<TUnboxedValue, TStdAllocatorForUdf<TUnboxedValue>> items;
    for (;;) {
        const auto& ev = reader.NextEvent();
        switch (ev.Type()) {
            case EEventType::BeginList:
                items.emplace_back(ParseList(reader, valueBuilder));
                break;
            case EEventType::EndList:
                return MakeList(items.data(), items.size(), valueBuilder);
            case EEventType::BeginMap:
                items.emplace_back(ParseDict(reader, valueBuilder));
                break;
            case EEventType::BeginAttributes:
                items.emplace_back(ParseAttributes(reader, valueBuilder));
                break;
            case EEventType::Scalar:
                items.emplace_back(ParseScalar(ev.AsScalar(), valueBuilder));
                break;
            default:
                UnexpectedEvent(ev.Type());
        }
    }
}

TUnboxedValue ParseDict(TReader& reader, const IValueBuilder* valueBuilder) {
    TSmallVec<TPair, TStdAllocatorForUdf<TPair>> items;
    for (;;) {
        const auto& evKey = reader.NextEvent();
        if (evKey.Type() == EEventType::EndMap) {
            return MakeDict(items.data(), items.size());
        }

        Y_ASSERT(evKey.Type() == EEventType::Key);
        auto key = valueBuilder->NewString(evKey.AsString());
        const auto& ev = reader.NextEvent();
        switch (ev.Type()) {
            case EEventType::BeginList:
                items.emplace_back(std::make_pair(std::move(key), ParseList(reader, valueBuilder)));
                break;
            case EEventType::BeginMap:
                items.emplace_back(std::make_pair(std::move(key), ParseDict(reader, valueBuilder)));
                break;
            case EEventType::BeginAttributes:
                items.emplace_back(std::make_pair(std::move(key), ParseAttributes(reader, valueBuilder)));
                break;
            case EEventType::Scalar:
                items.emplace_back(std::make_pair(std::move(key), ParseScalar(ev.AsScalar(), valueBuilder)));
                break;
            default:
                UnexpectedEvent(ev.Type());
        }
    }
}

TUnboxedValue ParseValue(TReader& reader, const IValueBuilder* valueBuilder);

TUnboxedValue ParseAttributes(TReader& reader, const IValueBuilder* valueBuilder) {
    TSmallVec<TPair, TStdAllocatorForUdf<TPair>> items;
    for (;;) {
        const auto& evKey = reader.NextEvent();
        if (evKey.Type() == EEventType::EndAttributes) {
            break;
        }

        Y_ASSERT(evKey.Type() == EEventType::Key);
        auto key = valueBuilder->NewString(evKey.AsString());
        const auto& ev = reader.NextEvent();
        switch (ev.Type()) {
            case EEventType::BeginList:
                items.emplace_back(std::make_pair(std::move(key), ParseList(reader, valueBuilder)));
                break;
            case EEventType::BeginMap:
                items.emplace_back(std::make_pair(std::move(key), ParseDict(reader, valueBuilder)));
                break;
            case EEventType::BeginAttributes:
                items.emplace_back(std::make_pair(std::move(key), ParseAttributes(reader, valueBuilder)));
                break;
            case EEventType::Scalar:
                items.emplace_back(std::make_pair(std::move(key), ParseScalar(ev.AsScalar(), valueBuilder)));
                break;
            default:
                UnexpectedEvent(ev.Type());
        }
    }

    return MakeAttr(ParseValue(reader, valueBuilder), items.data(), items.size());
}

TUnboxedValue ParseValue(TReader& reader, const IValueBuilder* valueBuilder) {
    const auto& ev = reader.NextEvent();
    switch (ev.Type()) {
        case EEventType::BeginList:
            return ParseList(reader, valueBuilder);
        case EEventType::BeginMap:
            return ParseDict(reader, valueBuilder);
        case EEventType::BeginAttributes:
            return ParseAttributes(reader, valueBuilder);
        case EEventType::Scalar:
            return ParseScalar(ev.AsScalar(), valueBuilder);
        default:
            UnexpectedEvent(ev.Type());
    }
}

/////////////////////////////////////

bool CheckValue(TReader& reader);

bool CheckDict(TReader& reader) {
    for (;;) {
        const auto& evKey = reader.NextEvent();
        if (evKey.Type() == EEventType::EndMap)
            return true;

        if (evKey.Type() != EEventType::Key)
            return false;

        if (CheckValue(reader))
            continue;
        else
            return false;
    }
}

bool CheckAttributes(TReader& reader) {
    for (;;) {
        const auto& evKey = reader.NextEvent();
        if (evKey.Type() == EEventType::EndAttributes)
            break;

        if (evKey.Type() != EEventType::Key)
            return false;

        if (CheckValue(reader))
            continue;
        else
            return false;
    }

    return CheckValue(reader);
}

bool CheckList(TReader& reader) {
    for (;;) {
        const auto& ev = reader.NextEvent();
        switch (ev.Type()) {
            case EEventType::BeginList:
                if (CheckList(reader))
                    break;
                else
                    return false;
            case EEventType::BeginMap:
                if (CheckDict(reader))
                    break;
                else
                    return false;
            case EEventType::BeginAttributes:
                if (CheckAttributes(reader))
                    break;
                else
                    return false;
            case EEventType::Scalar:
                break;
            case EEventType::EndList:
                return true;
            default:
                return false;
        }
    }
}

bool CheckValue(TReader& reader) {
    const auto& ev = reader.NextEvent();
    switch (ev.Type()) {
        case EEventType::BeginList:
            if (CheckList(reader))
                break;
            else
                return false;
        case EEventType::BeginMap:
            if (CheckDict(reader))
                break;
            else
                return false;
        case EEventType::BeginAttributes:
            if (CheckAttributes(reader))
                break;
            else
                return false;
        case EEventType::Scalar:
            break;
        default:
            return false;
    }
    return true;
}

void WriteValue(TWriter& writer, const TUnboxedValue& x) {
    switch (GetNodeType(x)) {
        case ENodeType::String:
            writer.String(x.AsStringRef());
            break;
        case ENodeType::Bool:
            writer.Boolean(x.Get<bool>());
            break;
        case ENodeType::Int64:
            writer.Int64(x.Get<i64>());
            break;
        case ENodeType::Uint64:
            writer.UInt64(x.Get<ui64>());
            break;
        case ENodeType::Double:
            writer.Float64(x.Get<double>());
            break;
        case ENodeType::Entity:
            writer.Entity();
            break;
        case ENodeType::List:
            writer.BeginList();
            if (x.IsBoxed()) {
                if (const auto elements = x.GetElements()) {
                    const auto size = x.GetListLength();
                    for (ui64 i = 0; i < size; ++i) {
                        WriteValue(writer, elements[i]);
                    }
                } else {
                    const auto it = x.GetListIterator();
                    for (TUnboxedValue v; it.Next(v); WriteValue(writer, v))
                        continue;
                }
            }
            writer.EndList();
            break;
        case ENodeType::Dict:
            writer.BeginMap();
            if (x.IsBoxed()) {
                TUnboxedValue key, payload;
                for (const auto it = x.GetDictIterator(); it.NextPair(key, payload);) {
                    writer.Key(key.AsStringRef());
                    WriteValue(writer, payload);
                }
            }
            writer.EndMap();
            break;
        case ENodeType::Attr: {
            writer.BeginAttributes();
            TUnboxedValue key, payload;
            for (const auto it = x.GetDictIterator(); it.NextPair(key, payload);) {
                writer.Key(key.AsStringRef());
                WriteValue(writer, payload);
            }

            writer.EndAttributes();
            WriteValue(writer, x.GetVariantItem());
        }
        break;
    }
}

void SerializeYsonDomImpl(const NUdf::TUnboxedValue& dom, TWriter& writer) {
    writer.BeginStream();
    WriteValue(writer, dom);
    writer.EndStream();
}

}

NUdf::TUnboxedValue TryParseYsonDom(const TStringBuf yson, const NUdf::IValueBuilder* valueBuilder) {
    auto reader = TReader(NInput::FromMemory(yson), EStreamType::Node);
    const auto& begin = reader.NextEvent();
    Y_ASSERT(begin.Type() == EEventType::BeginStream);
    auto value = ParseValue(reader, valueBuilder);
    const auto& end = reader.NextEvent();
    Y_ASSERT(end.Type() == EEventType::EndStream);
    return value;
}

bool IsValidYson(const TStringBuf yson) try {
    auto reader = TReader(NInput::FromMemory(yson), EStreamType::Node);
    const auto& begin = reader.NextEvent();
    if (begin.Type() != EEventType::BeginStream)
        return false;
    if (!CheckValue(reader))
        return false;
    const auto& end = reader.NextEvent();
    return end.Type() == EEventType::EndStream;
} catch (const NException::TBadStream&) {
    return false;
}

TString SerializeYsonDomToBinary(const NUdf::TUnboxedValue& dom) {
    TString result;
    TWriter writer = MakeBinaryWriter(NOutput::FromString(&result), EStreamType::Node);
    SerializeYsonDomImpl(dom, writer);
    return result;
}

TString SerializeYsonDomToText(const NUdf::TUnboxedValue& dom) {
    TString result;
    TWriter writer = MakeTextWriter(NOutput::FromString(&result), EStreamType::Node);
    SerializeYsonDomImpl(dom, writer);
    return result;
}

TString SerializeYsonDomToPrettyText(const NUdf::TUnboxedValue& dom) {
    TString result;
    TWriter writer = MakePrettyTextWriter(NOutput::FromString(&result), EStreamType::Node);
    SerializeYsonDomImpl(dom, writer);
    return result;
}

}
