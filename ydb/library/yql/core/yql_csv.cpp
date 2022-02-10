#include "yql_csv.h"

#include <util/string/split.h>
#include <util/string/escape.h>

namespace {

const char QUOTE_CH = '"';
const char ESCAPE_CH = '\\';
const char NULL_CH = '\0';

class TCsvLineParser {
public:
    TCsvLineParser(const TStringBuf& line, char delimiter)
        : Cur_(line.begin())
        , End_(line.end())
        , Delim_(delimiter)
        , Prev_(NULL_CH) /* must differ from ESCAPE_CH */
    {
    }

    bool Next(TStringBuf& token) {
        if (Cur_ > End_) return false;

        const char* tokenStart = Cur_;
        while (Cur_ < End_ && (Prev_ == ESCAPE_CH || *Cur_ != QUOTE_CH) && *Cur_ != Delim_) Prev_ = *Cur_++;

        if (Cur_ == End_) {
            token = { tokenStart, Cur_ };
            Prev_ = NULL_CH;
            Cur_++;
            return true;
        }

        if (Prev_ != ESCAPE_CH && *Cur_ == QUOTE_CH) {
            Prev_ = *Cur_++;
            tokenStart = Cur_;
            while (Cur_ < End_) {
                // find non escaped quote char
                if (*Cur_ == QUOTE_CH && Prev_ != ESCAPE_CH) break;
                Prev_ = *Cur_++;
            }

            if (Cur_ == End_)
                ythrow yexception() << "expected closing \"";

            token = { tokenStart, Cur_ };
            Prev_ = *Cur_++; // skip closing quote char

            if (Cur_ != End_ && *Cur_ != Delim_)
                ythrow yexception() << "expected end of line or delimiter";

            Prev_ = *Cur_++; // move out of buffer or skip delimiter
            return true;
        } else if (*Cur_ == Delim_) {
            token = { tokenStart, Cur_ };
            Prev_ = *Cur_++;
            return true;
        }

        return false;
    }

private:
    const char* Cur_;
    const char* End_;
    const char  Delim_;
    char Prev_;
};

} // namspace

namespace NYql {
namespace NUtils {

///////////////////////////////////////////////////////////////////////////////
// TCsvInputStream
///////////////////////////////////////////////////////////////////////////////
TCsvInputStream::TCsvInputStream(IInputStream& slave, char delimiter)
    : Slave_(slave)
    , Delim_(delimiter)
{
}

TVector<TString> TCsvInputStream::ReadLine()
{
    TVector<TString> parts;
    TString line;

    if (Slave_.ReadLine(line)) {
        TCsvLineParser lineParser(line, Delim_);
        TStringBuf token;
        while (lineParser.Next(token)) {
            parts.push_back(UnescapeC(token.data(), token.size()));
        }
    }

    return parts;
}

TVector<TString> TCsvInputStream::ReadLineWithEscaping()
{
    TVector<TString> parts;
    TString line;

    if (Slave_.ReadLine(line)) {
        TCsvLineParser lineParser(line, Delim_);
        TStringBuf token;
        while (lineParser.Next(token)) {
            parts.push_back(TString(token));
        }
    }

    return parts;
}

///////////////////////////////////////////////////////////////////////////////
// TCsvInputStream
///////////////////////////////////////////////////////////////////////////////
TCsvInputBuffer::TCsvInputBuffer(const TStringBuf& buffer, char delimiter)
    : Buffer_(buffer)
    , Delim_(delimiter)
{
}

TVector<TString> TCsvInputBuffer::ReadLine()
{
    TVector<TString> parts;
    TStringBuf line;

    if (Buffer_.ReadLine(line)) {
        TCsvLineParser lineParser(line, Delim_);
        TStringBuf token;
        while (lineParser.Next(token)) {
            parts.push_back(UnescapeC(token.data(), token.size()));
        }
    }

    return parts;
}

TVector<TString> TCsvInputBuffer::ReadLineWithEscaping()
{
    TVector<TString> parts;
    TStringBuf line;

    if (Buffer_.ReadLine(line)) {
        TCsvLineParser lineParser(line, Delim_);
        TStringBuf token;
        while (lineParser.Next(token)) {
            parts.push_back(TString(token.data(), token.size()));
        }
    }

    return parts;
}


///////////////////////////////////////////////////////////////////////////////
// TCsvOutputStream
///////////////////////////////////////////////////////////////////////////////
TCsvOutputStream::TCsvOutputStream(IOutputStream& slave, char delimiter, bool quoteItems)
    : Slave_(slave)
    , WasNL_(true)
    , Delim_(delimiter)
    , QuoteItems_(quoteItems)
{
}

void TCsvOutputStream::DoWrite(const void* buf, size_t len)
{
    TStringBuf charBuf(reinterpret_cast<const char*>(buf), len);
    if (charBuf == TStringBuf("\n")) {
        WasNL_ = true;
        Slave_.Write(buf, len);
    } else {
        if (!WasNL_) Slave_.Write(Delim_);
        WasNL_ = false;
        if (QuoteItems_) {
            Slave_.Write(QUOTE_CH);
        }
        Slave_.Write(EscapeC(charBuf));
        if (QuoteItems_) {
            Slave_.Write(QUOTE_CH);
        }
    }
}

void TCsvOutputStream::DoFlush() {
    Slave_.Flush();
}

} // namspace NUtils
} // namspace NYql
