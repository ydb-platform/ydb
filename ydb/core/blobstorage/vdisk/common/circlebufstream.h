#pragma once

#include <util/stream/output.h>
#include <util/stream/str.h>

////////////////////////////////////////////////////////////////////////////
// TCircleBufStringStream
// It works like TStringStream with Circle Buffer inside
////////////////////////////////////////////////////////////////////////////
static constexpr char CircleBufStringStreamSkipPrefix[] = "<SKIPPED>\n...";

template <size_t BufferSize>
class TCircleBufStringStream : public IOutputStream {
public:
    TCircleBufStringStream() {
        Buffer.resize(BufferSize);
    }

    TString Str() const {
        SelfCheck();
        if (Skipped) {
            return CircleBufStringStreamSkipPrefix +
                Buffer.substr(StartPos, BufferSize - StartPos) +
                Buffer.substr(0, EndPos);
        } else {
            return Buffer.substr(StartPos, EndPos);
        }
    }

    TString ToString() const {
        TStringStream str;
        str << "{StartPos# " << StartPos
            << " EndPos# " << EndPos
            << " Skipped# " << Skipped
            << " Buffer# '" << Buffer << "'}";
        return str.Str();
    }

private:
    TString Buffer;
    size_t StartPos = 0;
    size_t EndPos = 0;
    bool Skipped = false;

    void SelfCheck() const {
        Y_DEBUG_ABORT_UNLESS(Buffer.size() == BufferSize);
        if (Skipped) {
            Y_DEBUG_ABORT_UNLESS(StartPos == EndPos);
        } else {
            Y_DEBUG_ABORT_UNLESS(StartPos == 0 && StartPos <= EndPos);
        }
    }

    void DoWrite(const void* buf, size_t len) override {
        SelfCheck();

        // calculate safe append len (don't write outside the buffer)
        const size_t appendLen = Min(BufferSize - EndPos, len);

        // write appendLen bytes
        Buffer.replace(EndPos, appendLen, static_cast<const char*>(buf), appendLen);
        Y_DEBUG_ABORT_UNLESS(Buffer.size() == BufferSize);
        if (StartPos < EndPos || (StartPos == EndPos && !Skipped)) {
            EndPos += appendLen;
        } else {
            Y_DEBUG_ABORT_UNLESS(Skipped && StartPos == EndPos);
            EndPos += appendLen;
            StartPos = EndPos;
        }

        // fix buffer overflow
        if (EndPos == BufferSize) {
            StartPos = EndPos = 0;
            Skipped = true;
        }

        if (len > appendLen) {
            DoWrite(static_cast<const char*>(buf) + appendLen, len - appendLen);
        }
    }

    void DoWriteC(char c) override {
        DoWrite(&c, 1);
    }
};
