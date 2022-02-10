#pragma once

#include "json_output.h"

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/stack.h>

namespace NProtobufJson {
    class TJsonValueOutput: public IJsonOutput {
    public:
        TJsonValueOutput(NJson::TJsonValue& value)
            : Root(value)
        {
            Context.emplace(TContext::JSON_AFTER_KEY, Root);
        }

        void DoWrite(const TStringBuf& s) override;
        void DoWrite(const TString& s) override;
        void DoWrite(int i) override;
        void DoWrite(unsigned int i) override;
        void DoWrite(long long i) override;
        void DoWrite(unsigned long long i) override;
        void DoWrite(float f) override;
        void DoWrite(double f) override;
        void DoWrite(bool b) override;
        void DoWriteNull() override;

        void DoBeginList() override;
        void DoEndList() override;

        void DoBeginObject() override;
        void DoWriteKey(const TStringBuf& key) override;
        void DoEndObject() override;

        void DoWriteRawJson(const TStringBuf& str) override;

    private:
        template <typename T>
        void WriteImpl(const T& t);

        struct TContext {
            enum EType {
                JSON_MAP,
                JSON_ARRAY,
                JSON_AFTER_KEY,
            };

            TContext(EType type, NJson::TJsonValue& value)
                : Type(type)
                , Value(value)
            {
            }

            EType Type;
            NJson::TJsonValue& Value;
        };

        NJson::TJsonValue& Root;
        TStack<TContext, TVector<TContext>> Context;
    };

}
