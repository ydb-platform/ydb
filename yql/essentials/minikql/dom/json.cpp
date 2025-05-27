#include "json.h"
#include "node.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/stream/input.h>
#include <util/stream/str.h>
#include <util/generic/stack.h>
#include <util/system/yassert.h>
#include <util/system/compiler.h>

#include <cmath>
#include <ctype.h>

namespace NYql::NDom {

using namespace NUdf;
using namespace NJson;

namespace {

size_t AsciiSize(const TStringBuf& str) {
    size_t s = 0U;
    while (s < str.size() && isascii(str[s]))
        ++s;
    return s;
}

TString EncodeUtf(const TStringBuf& str, size_t from)
{
    TString result(str.substr(0, from));
    while (from < str.size()) {
        const auto c = str[from++];
        if (isascii(c)) {
            result.append(c);
        } else {
            result.append((c >> '\x06') & '\x03' | '\xC0');
            result.append(c & '\x3F' | '\x80');
        }
    }

    return result;
}

TString DecodeUtf(const TStringBuf& str, size_t from)
{
    TString result(str);
    auto i = from;
    while (from < str.size()) {
        const auto c = str[from++];
        if (isascii(c)) {
            result[i++] = c;
        } else if ((c & '\xFC') == '\xC0') {
            result[i++] = ((c & '\x03') << '\x06') | (str[from++] & '\x3F');
        } else {
            ythrow yexception() << "Unicode symbols with codes greater than 255 are not supported.";
        }
    }
    result.resize(i);
    return result;
}

template<bool DecodeUtf8>
class TDomCallbacks : public TJsonCallbacks {
public:
    TDomCallbacks(const IValueBuilder* valueBuilder, bool throwException)
        : TJsonCallbacks(throwException)
        , ValueBuilder(valueBuilder)
    {
        Result.push({});
    }

    bool OnNull() override {
        return PushToCurrentCollection(MakeEntity());
    }

    bool OnBoolean(bool value) override {
        return PushToCurrentCollection(MakeBool(value));
    }

    bool OnInteger(long long value) override {
        return PushToCurrentCollection(MakeInt64(static_cast<i64>(value)));
    }

    bool OnUInteger(unsigned long long value) override {
        return PushToCurrentCollection(MakeUint64(static_cast<ui64>(value)));
    }

    bool OnDouble(double value) override {
        if (Y_UNLIKELY(std::isinf(value))) {
            ythrow yexception() << "JSON number is infinite";
        }

        return PushToCurrentCollection(MakeDouble(value));
    }

    bool OnString(const TStringBuf& value) override {
        if constexpr (DecodeUtf8) {
            if (const auto from = AsciiSize(value); from < value.size()) {
                return PushToCurrentCollection(MakeString(DecodeUtf(value, from), ValueBuilder));
            }
        }
        return PushToCurrentCollection(MakeString(value, ValueBuilder));
    }

    bool OnOpenMap() override {
        return OnCollectionOpen();
    }

    bool OnMapKey(const TStringBuf& value) override {
        return OnString(value);
    }

    bool OnCloseMap() override {
        Y_DEBUG_ABORT_UNLESS(!Result.empty());
        auto& items = Result.top();
        Y_DEBUG_ABORT_UNLESS(items.size() % 2 == 0);

        TSmallVec<TPair, TStdAllocatorForUdf<TPair>> pairs;
        for (size_t i = 0; i < items.size(); i += 2) {
            pairs.emplace_back(std::move(items[i]), std::move(items[i + 1]));
        }

        Result.pop();
        return PushToCurrentCollection(MakeDict(pairs.data(), pairs.size()));
    }

    bool OnOpenArray() override {
        return OnCollectionOpen();
    }

    bool OnCloseArray() override {
        Y_DEBUG_ABORT_UNLESS(!Result.empty());
        auto& items = Result.top();
        TUnboxedValue list = MakeList(items.data(), items.size(), ValueBuilder);
        Result.pop();
        return PushToCurrentCollection(std::move(list));
    }

    bool OnEnd() override {
        return IsResultSingle();
    }

    TUnboxedValue GetResult() && {
        Y_DEBUG_ABORT_UNLESS(IsResultSingle());
        return std::move(Result.top()[0]);
    }

private:
    bool OnCollectionOpen() {
        Result.emplace();
        return true;
    }

    bool PushToCurrentCollection(TUnboxedValue&& value) {
        Y_DEBUG_ABORT_UNLESS(!Result.empty());
        Result.top().emplace_back(std::move(value));
        return true;
    }

    bool IsResultSingle() {
        return Result.size() == 1 && Result.top().size() == 1;
    }

    const IValueBuilder* ValueBuilder;

    using TUnboxedValues = TSmallVec<TUnboxedValue, TStdAllocatorForUdf<TUnboxedValue>>;
    std::stack<TUnboxedValues, TSmallVec<TUnboxedValues, TStdAllocatorForUdf<TUnboxedValues>>> Result;
};

class TTestCallbacks : public TJsonCallbacks {
public:
    TTestCallbacks()
        : TJsonCallbacks(false)
    {}

    bool OnNull() final { return true; }

    bool OnBoolean(bool) final { return true; }

    bool OnInteger(long long) final { return true; }

    bool OnUInteger(unsigned long long) final { return true; }

    bool OnDouble(double value) final { return !std::isinf(value); }

    bool OnString(const TStringBuf&) final { return true; }

    bool OnOpenMap() final { return true; }

    bool OnMapKey(const TStringBuf&) final { return true; }

    bool OnCloseMap() final { return true; }

    bool OnOpenArray() final { return true; }

    bool OnCloseArray() final { return true; }

    bool OnEnd() final {
        if (HasResult)
            return false;

        return HasResult = true;
    }

 private:
     bool HasResult = false;
};

bool IsEntity(const TUnboxedValuePod value) {
    switch (GetNodeType(value)) {
        case ENodeType::Entity: return true;
        case ENodeType::Attr: return IsEntity(value.GetVariantItem().Release());
        default: return false;
    }
}

template<bool SkipMapEntity, bool EncodeUtf8>
void WriteValue(const TUnboxedValuePod value, TJsonWriter& writer);

template<bool SkipMapEntity, bool EncodeUtf8>
void WriteArray(const TUnboxedValuePod value, TJsonWriter& writer) {
    writer.OpenArray();
    if (value.IsBoxed()) {
        if (const auto elements = value.GetElements()) {
            const auto size = value.GetListLength();
            for (ui64 i = 0; i < size; ++i) {
                WriteValue<SkipMapEntity, EncodeUtf8>(elements[i], writer);
            }
        } else {
            const auto it = value.GetListIterator();
            for (TUnboxedValue v; it.Next(v); WriteValue<SkipMapEntity, EncodeUtf8>(v, writer))
                continue;
        }
    }
    writer.CloseArray();
}

template<bool SkipMapEntity, bool EncodeUtf8>
void WriteMap(const TUnboxedValuePod value, TJsonWriter& writer) {
    writer.OpenMap();
    if (value.IsBoxed()) {
        TUnboxedValue key, payload;
        for (const auto it = value.GetDictIterator(); it.NextPair(key, payload);) {
            if constexpr (SkipMapEntity)
                if (IsEntity(payload))
                    continue;
            const TStringBuf str = key.AsStringRef();
            if constexpr (EncodeUtf8)
                if (const auto from = AsciiSize(str); from < str.size())
                    writer.WriteKey(EncodeUtf(str, from));
                else
                    writer.WriteKey(str);
            else
                writer.WriteKey(str);
            WriteValue<SkipMapEntity, EncodeUtf8>(payload, writer);
        }
    }
    writer.CloseMap();
}

template<bool SkipMapEntity, bool EncodeUtf8>
void WriteValue(const TUnboxedValuePod value, TJsonWriter& writer) {
    switch (GetNodeType(value)) {
        case ENodeType::String: {
            const TStringBuf str = value.AsStringRef();
            if constexpr (EncodeUtf8) {
                if (const auto from = AsciiSize(str); from < str.size()) {
                    return writer.Write(EncodeUtf(str, from));
                }
            }
            return writer.Write(str);
        }
        case ENodeType::Bool:
            return writer.Write(value.Get<bool>());
        case ENodeType::Int64:
            return writer.Write(value.Get<i64>());
        case ENodeType::Uint64:
            return writer.Write(value.Get<ui64>());
        case ENodeType::Double:
            return writer.Write(value.Get<double>());
        case ENodeType::Entity:
             return writer.WriteNull();
        case ENodeType::List:
            return WriteArray<SkipMapEntity, EncodeUtf8>(value, writer);
        case ENodeType::Dict:
            return WriteMap<SkipMapEntity, EncodeUtf8>(value, writer);
        case ENodeType::Attr:
            writer.OpenMap();
            writer.WriteKey("$attributes");
            WriteMap<SkipMapEntity, EncodeUtf8>(value, writer);
            writer.WriteKey("$value");
            WriteValue<SkipMapEntity, EncodeUtf8>(value.GetVariantItem().Release(), writer);
            writer.CloseMap();
    }
}

}

bool IsValidJson(const TStringBuf json) {
    TMemoryInput input(json.data(), json.size());
    TTestCallbacks callbacks;
    return ReadJson(&input, &callbacks);
}

TUnboxedValue TryParseJsonDom(const TStringBuf json, const IValueBuilder* valueBuilder, bool dencodeUtf8) {
    TMemoryInput input(json.data(), json.size());
    if (dencodeUtf8) {
        TDomCallbacks<true> callbacks(valueBuilder, /* throwException */ true);
        if (!ReadJson(&input, &callbacks)) {
            UdfTerminate("Internal error: parser error occurred but corresponding callback was not called");
        }
        return std::move(callbacks).GetResult();
    } else {
        TDomCallbacks<false> callbacks(valueBuilder, /* throwException */ true);
        if (!ReadJson(&input, &callbacks)) {
            UdfTerminate("Internal error: parser error occurred but corresponding callback was not called");
        }
        return std::move(callbacks).GetResult();
    }
}

TString SerializeJsonDom(const NUdf::TUnboxedValuePod dom, bool skipMapEntity, bool encodeUtf8, bool writeNanAsString) {
    TStringStream output;
    TJsonWriterConfig config;

    config.SetFormatOutput(false);
    config.WriteNanAsString = writeNanAsString;
    
    config.FloatToStringMode = EFloatToStringMode::PREC_AUTO;
    TJsonWriter writer(&output, config);
    if (skipMapEntity)
        if (encodeUtf8)
            WriteValue<true, true>(dom, writer);
        else
            WriteValue<true, false>(dom, writer);
    else
        if (encodeUtf8)
            WriteValue<false, true>(dom, writer);
        else
            WriteValue<false, false>(dom, writer);
    writer.Flush();
    return output.Str();
}

}
