#include "json_value_output.h"

#include <library/cpp/json/json_reader.h>

namespace NProtobufJson {
    template <typename T>
    void TJsonValueOutput::WriteImpl(const T& t) {
        Y_ASSERT(Context.top().Type == TContext::JSON_ARRAY || Context.top().Type == TContext::JSON_AFTER_KEY);

        if (Context.top().Type == TContext::JSON_AFTER_KEY) {
            Context.top().Value = t;
            Context.pop();
        } else {
            Context.top().Value.AppendValue(t);
        }
    }

    void TJsonValueOutput::DoWrite(const TStringBuf& s) {
        WriteImpl(s);
    }

    void TJsonValueOutput::DoWrite(const TString& s) {
        WriteImpl(s);
    }

    void TJsonValueOutput::DoWrite(int i) {
        WriteImpl(i);
    }

    void TJsonValueOutput::DoWrite(unsigned int i) {
        WriteImpl(i);
    }

    void TJsonValueOutput::DoWrite(long long i) {
        WriteImpl(i);
    }

    void TJsonValueOutput::DoWrite(unsigned long long i) {
        WriteImpl(i);
    }

    void TJsonValueOutput::DoWrite(float f) {
        WriteImpl(f);
    }

    void TJsonValueOutput::DoWrite(double f) {
        WriteImpl(f);
    }

    void TJsonValueOutput::DoWrite(bool b) {
        WriteImpl(b);
    }

    void TJsonValueOutput::DoWriteNull() {
        WriteImpl(NJson::JSON_NULL);
    }

    void TJsonValueOutput::DoBeginList() {
        Y_ASSERT(Context.top().Type == TContext::JSON_ARRAY || Context.top().Type == TContext::JSON_AFTER_KEY);

        if (Context.top().Type == TContext::JSON_AFTER_KEY) {
            Context.top().Type = TContext::JSON_ARRAY;
            Context.top().Value.SetType(NJson::JSON_ARRAY);
        } else {
            Context.emplace(TContext::JSON_ARRAY, Context.top().Value.AppendValue(NJson::JSON_ARRAY));
        }
    }

    void TJsonValueOutput::DoEndList() {
        Y_ASSERT(Context.top().Type == TContext::JSON_ARRAY);
        Context.pop();
    }

    void TJsonValueOutput::DoBeginObject() {
        Y_ASSERT(Context.top().Type == TContext::JSON_ARRAY || Context.top().Type == TContext::JSON_AFTER_KEY);

        if (Context.top().Type == TContext::JSON_AFTER_KEY) {
            Context.top().Type = TContext::JSON_MAP;
            Context.top().Value.SetType(NJson::JSON_MAP);
        } else {
            Context.emplace(TContext::JSON_MAP, Context.top().Value.AppendValue(NJson::JSON_MAP));
        }
    }

    void TJsonValueOutput::DoWriteKey(const TStringBuf& key) {
        Y_ASSERT(Context.top().Type == TContext::JSON_MAP);
        Context.emplace(TContext::JSON_AFTER_KEY, Context.top().Value[key]);
    }

    void TJsonValueOutput::DoEndObject() {
        Y_ASSERT(Context.top().Type == TContext::JSON_MAP);
        Context.pop();
    }

    void TJsonValueOutput::DoWriteRawJson(const TStringBuf& str) {
        Y_ASSERT(Context.top().Type == TContext::JSON_ARRAY || Context.top().Type == TContext::JSON_AFTER_KEY);

        if (Context.top().Type == TContext::JSON_AFTER_KEY) {
            NJson::ReadJsonTree(str, &Context.top().Value);
            Context.pop();
        } else {
            NJson::ReadJsonTree(str, &Context.top().Value.AppendValue(NJson::JSON_UNDEFINED));
        }
    }

}
