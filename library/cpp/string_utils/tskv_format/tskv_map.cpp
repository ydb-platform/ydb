#include "tskv_map.h"

namespace {
    void Split(const TStringBuf& kv, TStringBuf& key, TStringBuf& value, bool& keyHasEscapes) {
        size_t delimiter = 0;
        keyHasEscapes = false;
        for (delimiter = 0; delimiter < kv.size() && kv[delimiter] != '='; ++delimiter) {
            if (kv[delimiter] == '\\') {
                ++delimiter;
                keyHasEscapes = true;
            }
        }

        if (delimiter < kv.size()) {
            key = kv.Head(delimiter);
            value = kv.Tail(delimiter + 1);
        } else {
            throw yexception() << "Incorrect tskv format";
        }
    }

    TStringBuf DeserializeTokenToBuffer(const TStringBuf& token, TString& buffer) {
        size_t tokenStart = buffer.size();
        NTskvFormat::Unescape(token, buffer);
        return TStringBuf(buffer).Tail(tokenStart);
    }

    void DeserializeTokenToString(const TStringBuf& token, TString& result, bool unescape) {
        if (unescape) {
            result.clear();
            NTskvFormat::Unescape(token, result);
        } else {
            result = token;
        }

    }
}

void NTskvFormat::NDetail::DeserializeKvToStringBufs(const TStringBuf& kv, TStringBuf& key, TStringBuf& value, TString& buffer, bool unescape) {
    bool keyHasEscapes = false;
    Split(kv, key, value, keyHasEscapes);
    if (unescape) {
        if (keyHasEscapes) {
            key = DeserializeTokenToBuffer(key, buffer);
        }
        if (value.Contains('\\')) {
            value = DeserializeTokenToBuffer(value, buffer);
        }
    }
}

void NTskvFormat::NDetail::DeserializeKvToStrings(const TStringBuf& kv, TString& key, TString& value, bool unescape) {
    TStringBuf keyBuf, valueBuf;
    bool keyHasEscapes = false;
    Split(kv, keyBuf, valueBuf, keyHasEscapes);

    Y_UNUSED(keyHasEscapes);
    DeserializeTokenToString(keyBuf, key, unescape);
    DeserializeTokenToString(valueBuf, value, unescape);
}
