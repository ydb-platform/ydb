#pragma once

#include "json_output.h"
#include "config.h"

#include <library/cpp/json/json_writer.h>

#include <util/string/builder.h>
#include <util/generic/store_policy.h>

namespace NProtobufJson {
    class TBaseJsonWriterOutput: public IJsonOutput {
    public:
        TBaseJsonWriterOutput(NJson::TJsonWriter& writer)
            : Writer(writer)
        {
        }

    private:
        void DoWrite(int i) override {
            Writer.Write(i);
        }
        void DoWrite(unsigned int i) override {
            Writer.Write(i);
        }
        void DoWrite(long long i) override {
            Writer.Write(i);
        }
        void DoWrite(unsigned long long i) override {
            Writer.Write(i);
        }
        void DoWrite(float f) override {
            Writer.Write(f);
        }
        void DoWrite(double f) override {
            Writer.Write(f);
        }
        void DoWrite(bool b) override {
            Writer.Write(b);
        }
        void DoWriteNull() override {
            Writer.WriteNull();
        }
        void DoWrite(const TStringBuf& s) override {
            Writer.Write(s);
        }
        void DoWrite(const TString& s) override {
            Writer.Write(s);
        }

        void DoBeginList() override {
            Writer.OpenArray();
        }
        void DoEndList() override {
            Writer.CloseArray();
        }

        void DoBeginObject() override {
            Writer.OpenMap();
        }
        void DoWriteKey(const TStringBuf& key) override {
            Writer.Write(key);
        }
        void DoEndObject() override {
            Writer.CloseMap();
        }

        void DoWriteRawJson(const TStringBuf& str) override {
            Writer.UnsafeWrite(str);
        }

        NJson::TJsonWriter& Writer;
    };

    class TJsonWriterOutput: public TEmbedPolicy<NJson::TJsonWriter>, public TBaseJsonWriterOutput {
    public:
        TJsonWriterOutput(IOutputStream* outputStream, const NJson::TJsonWriterConfig& cfg)
            : TEmbedPolicy<NJson::TJsonWriter>(outputStream, cfg)
            , TBaseJsonWriterOutput(*Ptr())
        {
        }

        TJsonWriterOutput(IOutputStream* outputStream, const TProto2JsonConfig& cfg)
            : TEmbedPolicy<NJson::TJsonWriter>(outputStream, CreateJsonWriterConfig(cfg))
            , TBaseJsonWriterOutput(*Ptr())
        {
        }

    protected:
        static NJson::TJsonWriterConfig CreateJsonWriterConfig(const TProto2JsonConfig& cfg);
    };

    class TJsonStringWriterOutput: public TEmbedPolicy<TStringOutput>, public TJsonWriterOutput {
    public:
        template <typename TConfig>
        TJsonStringWriterOutput(TString* str, const TConfig& cfg)
            : TEmbedPolicy<TStringOutput>(*str)
            // If Unbuffered = false, TJsonWriter uses its own string and then flushes it into TStringOutput.
            , TJsonWriterOutput(TEmbedPolicy<TStringOutput>::Ptr(), CreateJsonWriterConfig(cfg).SetUnbuffered(true))
        {
        }
    };

}
