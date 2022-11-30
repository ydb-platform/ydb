#pragma once

#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/ptr.h>
#include <util/stream/input.h>
#include <util/stream/output.h>

class TStringDataInputStream: public IInputStream {
protected:
    const char* (*GetLine)(size_t);
    size_t BufferSize;
    TArrayHolder<char> OutBuf;
    size_t Line;
    size_t LineLength;
    size_t Cursor;

private:
    bool NextLine() {
        Cursor = 0;
        const TStringBuf str(GetLine(Line));
        if (str.empty()) {
            LineLength = 0;
            return false;
        }

        ++Line;

        const size_t size = Base64DecodeBufSize(str.size());

        if (BufferSize < size) {
            OutBuf.Reset(new char[size]);
            BufferSize = size;
        }

        LineLength = Base64Decode(str, OutBuf.Get()).size();
        Y_ASSERT(LineLength <= BufferSize);

        return LineLength > 0;
    }

    size_t ReadDataFromLine(char* buf, size_t len) {
        size_t n = Min(len, LineLength - Cursor);
        memcpy(static_cast<void*>(buf), static_cast<void*>(OutBuf.Get() + Cursor), n);
        Cursor += n;
        return n;
    }

protected:
    size_t DoRead(void* buf, size_t len) override {
        size_t readed = 0;
        while (readed < len) {
            readed += ReadDataFromLine(static_cast<char*>(buf) + readed, len - readed);
            if (readed < len && !NextLine())
                break;
        }
        return readed;
    }

public:
    TStringDataInputStream(const char* (*getLine)(size_t), size_t bufferSize = 0)
        : GetLine(getLine)
        , BufferSize(bufferSize)
        , OutBuf(new char[BufferSize])
        , Line(0)
        , LineLength(0)
        , Cursor(0)
    {
    }
};

class TStringDataOutputStream: public IOutputStream {
public:
    TStringDataOutputStream(IOutputStream* out, const int maxOutStrLen)
        : mStream(out)
        , MaxReadLen(Base64DecodeBufSize(maxOutStrLen))
        , BufRead(new unsigned char[MaxReadLen])
        , BufReadOffset(0)
        , BufOut(new char[maxOutStrLen + 1]){};

    ~TStringDataOutputStream() override {
        try {
            Finish();
        } catch (...) {
        }
    }

private:
    IOutputStream* mStream;
    size_t MaxReadLen;
    TArrayHolder<unsigned char> BufRead;
    size_t BufReadOffset;
    TArrayHolder<char> BufOut;

    void WriteLine() {
        if (BufReadOffset > 0) {
            mStream->Write("        \"");
            Y_ASSERT(BufReadOffset <= MaxReadLen);
            char* b = BufOut.Get();
            char* e = Base64Encode(b, BufRead.Get(), BufReadOffset);
            mStream->Write(b, e - b);
            mStream->Write("\",\n");
            BufReadOffset = 0;
        }
    }

    void DoWrite(const void* buf, size_t size) override {
        size_t res = Min(MaxReadLen - BufReadOffset, size);
        size_t buf_offset = 0;
        while (res > 0) {
            memcpy(BufRead.Get() + BufReadOffset, (const char*)buf + buf_offset, res);
            BufReadOffset += res;
            if (BufReadOffset < MaxReadLen)
                return;
            WriteLine();
            buf_offset += res;
            size -= res;
            res = Min(MaxReadLen - BufReadOffset, size);
        }
    }

    void DoFlush() override {
        WriteLine();
    }

    void DoFinish() override {
        DoFlush();
    }
};
