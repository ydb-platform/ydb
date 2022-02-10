#pragma once

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/generic/vector.h>

namespace NYql {
namespace NUtils {

///////////////////////////////////////////////////////////////////////////////
// TCsvInputStream
///////////////////////////////////////////////////////////////////////////////
class TCsvInputStream {
public:
    TCsvInputStream(IInputStream& slave, char delimiter = ';');

    TVector<TString> ReadLine();
    TVector<TString> ReadLineWithEscaping();

private:
    IInputStream& Slave_;
    const char    Delim_;
};


///////////////////////////////////////////////////////////////////////////////
// TCsvInputBuffer
///////////////////////////////////////////////////////////////////////////////
class TCsvInputBuffer {
public:
    TCsvInputBuffer(const TStringBuf& buffer, char delimiter = ';');

    TVector<TString> ReadLine();
    TVector<TString> ReadLineWithEscaping();

private:
    TStringBuf Buffer_;
    const char Delim_;
};


///////////////////////////////////////////////////////////////////////////////
// TCsvOutputStream
///////////////////////////////////////////////////////////////////////////////
class TCsvOutputStream: public IOutputStream {
public:
    TCsvOutputStream(IOutputStream& slave, char delimiter = ';', bool quoteItems = true);

    // hack for output empty values
    inline TCsvOutputStream& operator<<(const TString& value) {
        DoWrite(value.data(), value.size());
        return *this;
    }

    // hack for output empty values
    inline TCsvOutputStream& operator<<(const char* value) {
        DoWrite(value, strlen(value));
        return *this;
    }

    inline TCsvOutputStream& operator<<(const TVector<TString>& values) {
        for(const TString& value: values) {
            (*this) << value;
        }
        (*this) << Endl;
        return *this;
    }

private:
    void DoWrite(const void* buf, size_t len) override final;
    void DoFlush() override;

private:
    IOutputStream& Slave_;
    bool WasNL_;
    const char Delim_;
    const bool QuoteItems_;
};

} // namspace NUtils
} // namspace NYql
