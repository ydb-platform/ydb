#include "yql_codec_buf.h"

namespace NYql {
namespace NCommon {

NKikimr::NMiniKQL::TStatKey InputBytes("Job_InputBytes", true);
NKikimr::NMiniKQL::TStatKey OutputBytes("Job_OutputBytes", true);

ui32 TInputBuf::CopyVarUI32(TVector<char>& yson) {
    char cmd = Read();
    yson.push_back(cmd);
    ui32 shift = 0;
    ui32 value = cmd & 0x7f;
    while (cmd & 0x80) {
        shift += 7;
        cmd = Read();
        yson.push_back(cmd);
        value |= ui32(cmd & 0x7f) << shift;
    }

    return value;
}

ui64 TInputBuf::CopyVarUI64(TVector<char>& yson) {
    char cmd = Read();
    yson.push_back(cmd);
    ui64 shift = 0;
    ui64 value = cmd & 0x7f;
    while (cmd & 0x80) {
        shift += 7;
        cmd = Read();
        yson.push_back(cmd);
        value |= ui64(cmd & 0x7f) << shift;
    }

    return value;
}

ui32 TInputBuf::ReadVarUI32Slow(char cmd) {
    ui32 shift = 0;
    ui32 value = cmd & 0x7f;
    for (;;) {
        shift += 7;
        cmd = Read();
        value |= ui32(cmd & 0x7f) << shift;
        if (!(cmd & 0x80)) {
            break;
        }
    }

    return value;
}

ui64 TInputBuf::ReadVarUI64Slow(char cmd) {
    ui64 shift = 0;
    ui64 value = cmd & 0x7f;
    for (;;) {
        shift += 7;
        cmd = Read();
        value |= ui64(cmd & 0x7f) << shift;
        if (!(cmd & 0x80)) {
            break;
        }
    }

    return value;
}


TStringBuf TInputBuf::ReadYtString(ui32 lookAhead) {
    i32 length = ReadVarI32();
    CHECK_STRING_LENGTH(length);
    if (Current_ + length + lookAhead  <= End_) {
        TStringBuf ret(Current_, length);
        Current_ += length;
        return ret;
    }

    String_.resize(length);
    ReadMany(String_.data(), String_.size());
    return TStringBuf(String_.data(), String_.size());
}

void TInputBuf::Reset() {
    if (End_) {
        Source_->ReturnBlock();
    }
    Current_ = End_ = nullptr;
}

void TInputBuf::Fill() {
    if (Current_ < End_) {
        return;
    }

    if (!Source_) {
        return;
    }

    bool blockSwitch = false;
    if (End_) {
        Source_->ReturnBlock();
        blockSwitch = true;
    }

    if (ReadTimer_) {
        ReadTimer_->Acquire();
    }

    std::tie(Current_, End_) = Source_->NextFilledBlock();
    blockSwitch = blockSwitch && Current_ != End_;
    MKQL_ADD_STAT(JobStats_, InputBytes, End_ - Current_);
    if (ReadTimer_) {
        ReadTimer_->Release();
    }
    if (blockSwitch && OnNextBlockCallback_) {
        OnNextBlockCallback_();
    }
}

bool TInputBuf::TryReadSlow(char& value) {
    End_ = Current_;
    Fill();
    if (End_ == Current_) {
        return false;
    }

    value = *Current_++;
    return true;
}

extern "C" char InputBufReadSlowThunk(TInputBuf& in) {
    return in.ReadSlow();
}

extern "C" void InputBufReadManySlowThunk(TInputBuf& in, char* buffer, size_t count) {
    return in.ReadManySlow(buffer, count);
}

extern "C" void InputBufSkipManySlowThunk(TInputBuf& in, size_t count) {
    return in.SkipManySlow(count);
}

char TInputBuf::ReadSlow() {
    End_ = Current_;
    Fill();
    if (Y_UNLIKELY(End_ <= Current_)) {
        ythrow yexception() << "Unexpected end of stream";
    }
    return *Current_++;
}

void TInputBuf::ReadManySlow(char* buffer, size_t count) {
    while (count > 0) {
        Fill();
        if (Y_UNLIKELY(End_ <= Current_)) {
            ythrow yexception() << "Unexpected end of stream";
        }
        size_t toCopy = Min<size_t>(count, End_ - Current_);
        memcpy(buffer, Current_, toCopy);
        count -= toCopy;
        buffer += toCopy;
        Current_ += toCopy;
    }
}

void TInputBuf::SkipManySlow(size_t count) {
    while (count > 0) {
        Fill();
        if (Y_UNLIKELY(End_ <= Current_)) {
            ythrow yexception() << "Unexpected end of stream";
        }
        size_t toSkip = Min<size_t>(count, End_ - Current_);
        count -= toSkip;
        Current_ += toSkip;
    }
}


TOutputBuf::TOutputBuf(IBlockWriter& target, NKikimr::NMiniKQL::TStatTimer* writeTimer)
    : Target_(target)
    , WriteTimer_(writeTimer)
{
    std::tie(Begin_, End_) = Target_.NextEmptyBlock();
    Current_ = Begin_;
}

void TOutputBuf::WriteVarUI32(ui32 value) {
    for (;;) {
        char cmd = value & 0x7f;
        value >>= 7;
        if (value) {
            cmd |= 0x80;
            Write(cmd);
        } else {
            Write(cmd);
            break;
        }
    }
}

void TOutputBuf::WriteVarUI64(ui64 value) {
    for (;;) {
        char cmd = value & 0x7f;
        value >>= 7;
        if (value) {
            cmd |= 0x80;
            Write(cmd);
        } else {
            Write(cmd);
            break;
        }
    }
}

extern "C" void OutputBufFlushThunk(TOutputBuf& out) {
    out.Flush();
}

extern "C" void OutputBufWriteManySlowThunk(TOutputBuf& out, const char* buffer, size_t count) {
    out.WriteManySlow(buffer, count);
}

void TOutputBuf::Flush() {
    if (Current_ > Begin_) {
        if (WriteTimer_) {
            WriteTimer_->Acquire();
        }
        const ui64 avail = Current_ - Begin_;
        MKQL_ADD_STAT(JobStats_, OutputBytes, avail);
        WrittenBytes_ += avail;
        Target_.ReturnBlock(avail, RecordBoundary_ ? std::make_optional(RecordBoundary_ - Begin_) : std::nullopt);
        std::tie(Begin_, End_) = Target_.NextEmptyBlock();
        Current_ = Begin_;
        RecordBoundary_ = nullptr;
        if (WriteTimer_) {
            WriteTimer_->Release();
        }
   }
}

void TOutputBuf::WriteManySlow(const char* buffer, size_t count) {
    // write current buffer
    size_t remain = End_ - Current_;
    Y_ASSERT(remain < count);
    memcpy(Current_, buffer, remain);
    Current_ += remain;
    Flush();
    buffer += remain;
    count -= remain;
    while (count >= size_t(End_ - Begin_)) {
        size_t toWrite = End_ - Begin_;
        memcpy(Current_, buffer, toWrite);
        Current_ += toWrite;
        Flush();
        buffer += toWrite;
        count -= toWrite;
    }

    // keep tail
    memcpy(Current_, buffer, count);
    Current_ += count;
}

}
}
