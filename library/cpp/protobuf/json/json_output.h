#pragma once

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>

namespace NProtobufJson {
    class IJsonOutput {
    public:
        template <typename T>
        IJsonOutput& Write(const T& t) {
            DoWrite(t);
            return *this;
        }
        IJsonOutput& WriteNull() {
            DoWriteNull();
            return *this;
        }

        IJsonOutput& BeginList() {
            DoBeginList();
            return *this;
        }
        IJsonOutput& EndList() {
            DoEndList();
            return *this;
        }

        IJsonOutput& BeginObject() {
            DoBeginObject();
            return *this;
        }
        IJsonOutput& WriteKey(const TStringBuf& key) {
            DoWriteKey(key);
            return *this;
        }
        IJsonOutput& EndObject() {
            DoEndObject();
            return *this;
        }

        IJsonOutput& WriteRawJson(const TStringBuf& str) {
            DoWriteRawJson(str);
            return *this;
        }

        virtual ~IJsonOutput() {
        }

    protected:
        virtual void DoWrite(const TStringBuf& s) = 0;
        virtual void DoWrite(const TString& s) = 0;
        virtual void DoWrite(int i) = 0;
        void DoWrite(long i) {
            DoWrite(static_cast<long long>(i));
        }
        virtual void DoWrite(long long i) = 0;
        virtual void DoWrite(unsigned int i) = 0;
        void DoWrite(unsigned long i) {
            DoWrite(static_cast<unsigned long long>(i));
        }
        virtual void DoWrite(unsigned long long i) = 0;
        virtual void DoWrite(float f) = 0;
        virtual void DoWrite(double f) = 0;
        virtual void DoWrite(bool b) = 0;
        virtual void DoWriteNull() = 0;

        virtual void DoBeginList() = 0;
        virtual void DoEndList() = 0;

        virtual void DoBeginObject() = 0;
        virtual void DoWriteKey(const TStringBuf& key) = 0;
        virtual void DoEndObject() = 0;

        virtual void DoWriteRawJson(const TStringBuf& str) = 0;
    };

    using TJsonMapOutputPtr = THolder<IJsonOutput>;

}
