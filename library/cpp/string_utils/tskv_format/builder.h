#pragma once

#include "escape.h"

#include <util/stream/str.h>

namespace NTskvFormat {
    class TLogBuilder {
    private:
        TStringStream Out;

    public:
        TLogBuilder() = default;

        TLogBuilder(TStringBuf logType, ui32 unixtime) {
            Begin(logType, unixtime);
        }

        TLogBuilder(TStringBuf logType) {
            Begin(logType);
        }

        TLogBuilder& Add(TStringBuf fieldName, TStringBuf fieldValue) {
            if (!Out.Empty()) {
                Out << '\t';
            }
            Escape(fieldName, Out.Str());
            Out << '=';
            Escape(fieldValue, Out.Str());

            return *this;
        }

        TLogBuilder& AddUnescaped(TStringBuf fieldName, TStringBuf fieldValue) {
            if (!Out.Empty()) {
                Out << '\t';
            }
            Out << fieldName << '=' << fieldValue;
            return *this;
        }

        TLogBuilder& Begin(TStringBuf logType, ui32 unixtime) {
            Out << "tskv\ttskv_format=" << logType << "\tunixtime=" << unixtime;
            return *this;
        }

        TLogBuilder& Begin(TStringBuf logType) {
            Out << "tskv\ttskv_format=" << logType;
            return *this;
        }

        TLogBuilder& End() {
            Out << '\n';
            return *this;
        }

        TLogBuilder& Clear() {
            Out.Clear();
            return *this;
        }

        TString& Str() {
            return Out.Str();
        }
    };

}
