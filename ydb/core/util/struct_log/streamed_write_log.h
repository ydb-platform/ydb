#pragma once

#include "key_name.h"
#include "structured_message.h"

#include <map>

namespace NKikimr::NStructLog {

template <typename T>
struct TNamedValue {
    TKeyName Name;
    const T& Value;

    TNamedValue(TKeyName&& name, const T& value)
        : Name(name), Value(value) {}

    TNamedValue(const TNamedValue&) = delete;
    TNamedValue(TNamedValue&&) = delete;
    TNamedValue& operator=(const TNamedValue&) = delete;
    TNamedValue& operator=(TNamedValue&&) = delete;

    void* operator new(std::size_t sz) = delete;
    void* operator new[](std::size_t sz) = delete;
};

class TStructuredMessageStreamBuilder {
public:

    struct TExtractedValueName {
        const char* Text;
        unsigned TextLength;
        unsigned Start{0};
        unsigned End{0};

        TExtractedValueName() = default;
        constexpr TExtractedValueName(const TExtractedValueName&) = default;
        constexpr TExtractedValueName(TExtractedValueName&&) = default;

        template<unsigned N>
        constexpr TExtractedValueName(const char(&text)[N]) : Text(text), TextLength(N-1) {
            Init();
        }

        TExtractedValueName(const char* text, std::size_t length) : Text(text), TextLength(length) {
            Init();
        }

        constexpr bool empty() const {
            return (Start == 0) && (End == 0);
        }

        TExtractedValueName& operator=(const TExtractedValueName& value) {
            Text = value.Text;
            TextLength = value.TextLength;
            Start = value.Start;
            End = value.End;
            return *this;
        }

    protected:
        constexpr void Init() {
            End = TextLength - 1;
            while (End > 0 && Text[End] == ' ') {
                End--;
            }
            if (End == 0 || (Text[End] != ':' && Text[End] != '#')) {
                End = 0;
                return;
            }

            Start = End - 1;
            while (Start > 0 && Text[Start] != ' ') {
                Start--;
            }
            if (Text[Start] == ' ') {
                Start++;
            }
        }
    };

    const TStructuredMessage& GetStructMessage() const {
        return Message;
    }

    template <typename T, typename V = typename std::enable_if<TNativeTypeSupport<T>::value>::type >
    inline TStructuredMessageStreamBuilder& AppendValue(const T& value) {
        if (!CurrentKey.empty()) {
            Message.AppendValue({TKeyName(CurrentKey.Text + CurrentKey.Start, CurrentKey.End - CurrentKey.Start)}, value);
            CurrentKey = TExtractedValueName();
        } else {
            auto keyName = TString("__key") + std::to_string(UnnamedKeyNum++);
            Message.AppendValue({std::move(keyName)}, value);
        }
        return *this;
    }

    template <typename T, typename V = typename std::enable_if<TNativeTypeSupport<T>::value>::type >
    inline TStructuredMessageStreamBuilder& AppendValue(std::vector<TKeyName>&& name, const T& value) {
        Message.AppendValue(std::move(name), value);
        return *this;
    }

    struct TParseResult {
        std::map<TKeyName, TString> FixedValues;
        TExtractedValueName CurrentKey;
    };

    TParseResult Parse(const char* text, std::size_t length) {
        TParseResult result;

        while(length != 0) {
            // look up marker
            auto markerGrid = strchr(text, '#');
            auto markerColon = strchr(text, ':');

            const char* markerPos;
            if (markerGrid == nullptr && markerColon != nullptr) markerPos = markerColon;
            else if (markerGrid != nullptr && markerColon == nullptr) markerPos = markerGrid;
            else if (markerGrid != nullptr && markerColon != nullptr) markerPos = std::min(markerGrid, markerColon);
            else break;

            auto fieldName = markerPos;
            while (fieldName != text && *fieldName != ' ') fieldName--;
            if (*fieldName == ' ') fieldName++;

            auto valueStart = markerPos + 1;
            while (valueStart < text + length && *valueStart == ' ') valueStart++;

            if (valueStart == text + length) {
                result.CurrentKey.Text = text;
                result.CurrentKey.TextLength = length;
                result.CurrentKey.Start = fieldName - text;
                result.CurrentKey.End = markerPos - text;
                break;
            }

            auto valueFinish = valueStart;
            while (valueFinish < text + length && *valueFinish != ' ') valueFinish++;

            // add value
            TString name(fieldName, markerPos - fieldName);
            TString value(valueStart, valueFinish - valueStart);
            result.FixedValues[name] = value;

            // shift
            auto size = valueFinish - text;
            length -= size;
            text += size;
        }

        return result;
    }

    TStructuredMessageStreamBuilder& AppendText(const char* text, std::size_t length) {
        auto parseResult = Parse(text, length);

        for(auto& item: parseResult.FixedValues) {
            Message.AppendValue({item.first}, item.second);
        }
        CurrentKey = parseResult.CurrentKey;

        return *this;
    }

    inline TStructuredMessageStreamBuilder& AppendFixedText(const char* text, std::size_t length) {
        static thread_local std::unordered_map<const char*, TParseResult> cached;
        auto it = cached.find(text);
        if (it == end(cached)) {
            auto parseResult = Parse(text, length);
            it = cached.insert({text, parseResult}).first;
        }

        for(auto& item: it->second.FixedValues) {
            Message.AppendValue({item.first}, item.second);
        }
        CurrentKey = it->second.CurrentKey;

        return *this;
    }

    template<typename T, typename V = typename std::enable_if<TNativeTypeSupport<T>::value>::type>
    inline TStructuredMessageStreamBuilder& operator<<(const T& item) {
        AppendValue(item);
        return *this;
    }

    template<typename T>
    inline TStructuredMessageStreamBuilder& operator<<(TNamedValue<T>&& item) {
        AppendValue({std::move(item.Name)}, item.Value, item.InsertDataToText);
        return *this;
    }

    template<unsigned N>
    inline TStructuredMessageStreamBuilder& operator<<(const char(&text)[N]) {
        AppendFixedText(text, N - 1);
        return *this;
    }

    inline TStructuredMessageStreamBuilder& operator<<(const TString& text) {
        if (!CurrentKey.empty()) {
            // @todo А могут ли в text тут быть маркеры?
            AppendValue(text);
        } else {
            AppendText(text.c_str(), text.size());
        }
        return *this;
    }

    void Reset() {
        Message.Clear();
        UnnamedKeyNum = 1;
        CurrentKey = TExtractedValueName();
    }
private:
    TStructuredMessage Message;
    unsigned UnnamedKeyNum{1};
    TExtractedValueName CurrentKey;
};

}