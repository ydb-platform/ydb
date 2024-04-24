#pragma once
#include <ydb/library/yql/minikql/mkql_stats_registry.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/yson/zigzag.h>

#include <util/generic/yexception.h>
#include <util/generic/maybe.h>
#ifndef LLVM_BC
#include <util/datetime/base.h>
#else
using TInstant = ui64;
#endif
#include <util/generic/vector.h>

#include <utility>
#include <functional>
#include <optional>

namespace NYql {
namespace NCommon {

class TTimeoutException : public yexception {
};

struct IBlockReader {
    virtual ~IBlockReader() = default;
    virtual void SetDeadline(TInstant deadline) = 0;

    virtual std::pair<const char*, const char*> NextFilledBlock() = 0;
    virtual void ReturnBlock() = 0;
    virtual bool Retry(const TMaybe<ui32>& rangeIndex, const TMaybe<ui64>& rowIndex, const std::exception_ptr& error) = 0;
};

struct IBlockWriter {
    virtual ~IBlockWriter() = default;

    virtual void SetRecordBoundaryCallback(std::function<void()> callback) = 0;

    virtual std::pair<char*, char*> NextEmptyBlock() = 0;
    virtual void ReturnBlock(size_t avail, std::optional<size_t> lastRecordBoundary) = 0;
    virtual void Finish() = 0;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////

extern NKikimr::NMiniKQL::TStatKey InputBytes;
class TInputBuf;
extern "C" char InputBufReadSlowThunk(TInputBuf& in);
extern "C" void InputBufReadManySlowThunk(TInputBuf& in, char* buffer, size_t count);
extern "C" void InputBufSkipManySlowThunk(TInputBuf& in, size_t count);

class TInputBuf {
friend char InputBufReadSlowThunk(TInputBuf& in);
friend void InputBufReadManySlowThunk(TInputBuf& in, char* buffer, size_t count);
friend void InputBufSkipManySlowThunk(TInputBuf& in, size_t count);

public:
    TInputBuf(NKikimr::NMiniKQL::TSamplingStatTimer* readTimer)
        : ReadTimer_(readTimer)
    {}

    TInputBuf(IBlockReader& source, NKikimr::NMiniKQL::TSamplingStatTimer* readTimer)
        : TInputBuf(readTimer)
    {
        SetSource(source);
    }

    void SetSource(IBlockReader& source) {
        Source_ = &source;
        Current_ = nullptr;
        End_ = nullptr;
        String_.clear();
    }

    void SetStats(NKikimr::NMiniKQL::IStatsRegistry* jobStats) {
        JobStats_ = jobStats;
    }

    void SetNextBlockCallback(std::function<void()> cb) {
        OnNextBlockCallback_ = std::move(cb);
    }

    bool TryRead(char& value) {
        if (Y_LIKELY(Current_ < End_)) {
            value = *Current_++;
            return true;
        }

        return TryReadSlow(value);
    }

    char Read() {
        if (Y_LIKELY(Current_ < End_)) {
            return *Current_++;
        }

        return InputBufReadSlowThunk(*this);
    }

    ui32 ReadVarUI32() {
        char cmd = Read();
        if (Y_LIKELY(!(cmd & 0x80))) {
            return cmd;
        }

        return ReadVarUI32Slow(cmd);
    }

    ui32 CopyVarUI32(TVector<char>& yson);
    ui64 CopyVarUI64(TVector<char>& yson);
    ui32 ReadVarUI32Slow(char cmd);

    ui64 ReadVarUI64() {
        char cmd = Read();
        if (Y_LIKELY(!(cmd & 0x80))) {
            return cmd;
        }

        return ReadVarUI64Slow(cmd);
    }

    ui64 ReadVarUI64Slow(char cmd);

    i64 ReadVarI64() {
        return NYson::ZigZagDecode64(ReadVarUI64());
    }

    i32 ReadVarI32() {
        return NYson::ZigZagDecode32(ReadVarUI32());
    }

    i64 CopyVarI64(TVector<char>& yson) {
        return NYson::ZigZagDecode64(CopyVarUI64(yson));
    }

    i32 CopyVarI32(TVector<char>& yson) {
        return NYson::ZigZagDecode32(CopyVarUI32(yson));
    }

    TStringBuf ReadYtString(ui32 lookAhead = 0);

    TVector<char>& YsonBuffer() {
        return String_;
    }

    void ReadMany(char* buffer, size_t count) {
        if (Y_LIKELY(Current_ + count <= End_)) {
            memcpy(buffer, Current_, count);
            Current_ += count;
            return;
        }

        return InputBufReadManySlowThunk(*this, buffer, count);
    }

    void SkipMany(size_t count) {
        if (Y_LIKELY(Current_ + count <= End_)) {
            Current_ += count;
            return;
        }

        return InputBufSkipManySlowThunk(*this, count);
    }

    void CopyMany(size_t count, TVector<char>& yson) {
        auto origSize = yson.size();
        yson.resize(origSize + count);
        ReadMany(yson.data() + origSize, count);
    }

    // Call it on error
    void Reset();

private:
    void Fill();
    bool TryReadSlow(char& value);
    char ReadSlow();
    void ReadManySlow(char* buffer, size_t count);
    void SkipManySlow(size_t count);

private:
    IBlockReader* Source_ = nullptr;
    NKikimr::NMiniKQL::TSamplingStatTimer* ReadTimer_;
    NKikimr::NMiniKQL::IStatsRegistry* JobStats_ = nullptr;
    const char* Current_ = nullptr;
    const char* End_ = nullptr;
    TVector<char> String_;
    std::function<void()> OnNextBlockCallback_;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////

extern NKikimr::NMiniKQL::TStatKey OutputBytes;

class TOutputBuf;
extern "C" void OutputBufFlushThunk(TOutputBuf& out);
extern "C" void OutputBufWriteManySlowThunk(TOutputBuf& out, const char* buffer, size_t count);

class TOutputBuf {
friend void OutputBufWriteManySlowThunk(TOutputBuf& out, const char* buffer, size_t count);

public:
    TOutputBuf(IBlockWriter& target, NKikimr::NMiniKQL::TStatTimer* writeTimer);

    ui64 GetWrittenBytes() const {
        return WrittenBytes_;
    }

    void SetStats(NKikimr::NMiniKQL::IStatsRegistry* jobStats) {
        JobStats_ = jobStats;
    }

    void Write(char value) {
        if (Y_LIKELY(Current_ < End_)) {
            *Current_++ = value;
            return;
        }

        WriteSlow(value);
    }

    void WriteMany(TStringBuf str) {
        WriteMany(str.data(), str.size());
    }

    void WriteMany(const char* buffer, size_t count) {
        if (Y_LIKELY(Current_ + count <= End_)) {
            memcpy(Current_, buffer, count);
            Current_ += count;
            return;
        }

        OutputBufWriteManySlowThunk(*this, buffer, count);
    }

    void WriteVarI32(i32 value) {
        WriteVarUI32(NYson::ZigZagEncode32(value));
    }

    void WriteVarI64(i64 value) {
        WriteVarUI64(NYson::ZigZagEncode64(value));
    }

    void WriteVarUI32(ui32 value);
    void WriteVarUI64(ui64 value);

    void Flush();

    void Finish() {
        Flush();
        if (WriteTimer_) {
            WriteTimer_->Acquire();
        }

        Target_.Finish();
        if (WriteTimer_) {
            WriteTimer_->Release();
        }
    }

    void OnRecordBoundary() {
        RecordBoundary_ = Current_;
    }

private:
    void WriteSlow(char value) {
        OutputBufFlushThunk(*this);
        *Current_++ = value;
    }

    void WriteManySlow(const char* buffer, size_t count);

private:
    IBlockWriter& Target_;
    NKikimr::NMiniKQL::TStatTimer* WriteTimer_;
    NKikimr::NMiniKQL::IStatsRegistry* JobStats_ = nullptr;
    char* Begin_ = nullptr;
    char* Current_ = nullptr;
    char* End_ = nullptr;
    char* RecordBoundary_ = nullptr;
    ui64 WrittenBytes_ = 0;
};

#define CHECK_EXPECTED(read, expected) \
    YQL_ENSURE(read == expected, "Expected char: " << TString(1, expected).Quote() << ", but read: " << TString(1, read).Quote());

#define EXPECTED(buf, expected) \
    { char read = buf.Read(); CHECK_EXPECTED(read, expected); }

#define EXPECTED_COPY(buf, expected, yson) \
    { char read = buf.Read(); CHECK_EXPECTED(read, expected); yson.push_back(read); }

#define CHECK_STRING_LENGTH(length) \
    YQL_ENSURE(length >= 0 && length < (1 << 30), "Bad string length: " << length);

#define CHECK_STRING_LENGTH_UNSIGNED(length) \
    YQL_ENSURE(length < (1 << 30), "Bad string length: " << length);

} // NCommon
} // NYql
