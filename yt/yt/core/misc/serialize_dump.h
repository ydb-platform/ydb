#pragma once

#include <yt/yt/core/misc/checksum.h>

#include <library/cpp/yt/string/format.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSerializationDumper
{
public:
    void ConfigureMode(ESerializationDumpMode value);


    Y_FORCE_INLINE void BeginIndentedBlock()
    {
        ++IndentDepth_;
    }

    Y_FORCE_INLINE void EndIndentBlock()
    {
        --IndentDepth_;
    }


    Y_FORCE_INLINE void BeginSuspendedBlock()
    {
        ContentDumpLock_ += SuspendLockDelta;
    }

    Y_FORCE_INLINE void EndSuspendedBlock()
    {
        ContentDumpLock_ -= SuspendLockDelta;
    }

    Y_FORCE_INLINE bool IsContentDumpActive() const
    {
        return ContentDumpLock_ > 0;
    }

    Y_FORCE_INLINE bool IsChecksumDumpActive() const
    {
        return Mode_ == ESerializationDumpMode::Checksum;
    }


    void DisableScopeFiltering();
    void BeginScopeFilterMatchBlock(TStringBuf path);
    void EndScopeFilterMatchBlock(TStringBuf path);


    void SetFieldName(TStringBuf name)
    {
        FieldName_ = name;
    }


    template <class... TArgs>
    void WriteContent(const char* format, const TArgs&... args)
    {
        BeginWrite();
        ScratchBuilder_.AppendChar(' ', IndentDepth_ * 2);
        if (FieldName_) {
            ScratchBuilder_.AppendString(FieldName_);
            ScratchBuilder_.AppendString(": ");
            FieldName_ = {};
        }
        ScratchBuilder_.AppendFormat(format, args...);
        ScratchBuilder_.AppendChar('\n');
        EndWrite();
    }

    void WriteChecksum(TStringBuf path, TChecksum checksum)
    {
        BeginWrite();
        ScratchBuilder_.AppendFormat("%v => %x\n", path, checksum);
        EndWrite();
    }

private:
    ESerializationDumpMode Mode_ = ESerializationDumpMode::None;

    int IndentDepth_ = 0;

    // Positive values indicate that content dump is active.
    // Initial value is effective -INF to suppress any dump.
    static constexpr i64 ScopeFilterMatchLockDelta = +1;
    static constexpr i64 SuspendLockDelta = -(1LL << 32);
    i64 ContentDumpLock_ = -(1LL << 62);

    TStringBuf FieldName_;
    TStringBuilder ScratchBuilder_;

    void BeginWrite()
    {
        ScratchBuilder_.Reset();
    }

    void EndWrite()
    {
        auto buffer = ScratchBuilder_.GetBuffer();
        fwrite(buffer.begin(), buffer.length(), 1, stderr);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSerializeDumpIndentGuard
    : private TNonCopyable
{
public:
    explicit TSerializeDumpIndentGuard(TSerializationDumper* dumper)
        : Dumper_(dumper)
    {
        Dumper_->BeginIndentedBlock();
    }

    TSerializeDumpIndentGuard(TSerializeDumpIndentGuard&& other)
        : Dumper_(other.Dumper_)
    {
        other.Dumper_ = nullptr;
    }

    ~TSerializeDumpIndentGuard()
    {
        if (Dumper_) {
            Dumper_->EndIndentBlock();
        }
    }

    //! Needed for SERIALIZATION_DUMP_INDENT.
    explicit operator bool() const
    {
        return false;
    }

private:
    TSerializationDumper* Dumper_;
};

////////////////////////////////////////////////////////////////////////////////

class TSerializeDumpSuspendGuard
    : private TNonCopyable
{
public:
    explicit TSerializeDumpSuspendGuard(TSerializationDumper* dumper)
        : Dumper_(dumper)
    {
        Dumper_->BeginSuspendedBlock();
    }

    TSerializeDumpSuspendGuard(TSerializeDumpSuspendGuard&& other)
        : Dumper_(other.Dumper_)
    {
        other.Dumper_ = nullptr;
    }

    ~TSerializeDumpSuspendGuard()
    {
        if (Dumper_) {
            Dumper_->EndSuspendedBlock();
        }
    }

    //! Needed for SERIALIZATION_DUMP_SUSPEND.
    explicit operator bool() const
    {
        return false;
    }

private:
    TSerializationDumper* Dumper_;
};

////////////////////////////////////////////////////////////////////////////////

#define SERIALIZATION_DUMP_WRITE(context, ...) \
    if (Y_LIKELY(!(context).Dumper().IsContentDumpActive())) { \
    } else \
        (context).Dumper().WriteContent(__VA_ARGS__)

#define SERIALIZATION_DUMP_INDENT(context) \
    if (auto SERIALIZATION_DUMP_INDENT__Guard = NYT::TSerializeDumpIndentGuard(&(context).Dumper())) { \
        Y_UNREACHABLE(); \
    } else

#define SERIALIZATION_DUMP_SUSPEND(context) \
    if (auto SERIALIZATION_DUMP_SUSPEND__Guard = NYT::TSerializeDumpSuspendGuard(&(context).Dumper())) { \
        Y_UNREACHABLE(); \
    } else

////////////////////////////////////////////////////////////////////////////////

inline TString DumpRangeToHex(TRef data)
{
    TStringBuilder builder;
    builder.AppendChar('<');
    for (const char* ptr = data.Begin(); ptr != data.End(); ++ptr) {
        ui8 ch = *ptr;
        builder.AppendChar(IntToHexLowercase[ch >> 4]);
        builder.AppendChar(IntToHexLowercase[ch & 0xf]);
    }
    builder.AppendChar('>');
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TSerializationDumpPodWriter
{
    template <class C>
    static void Do(C& context, const T& value)
    {
        if constexpr(CFormattable<T>) {
            SERIALIZATION_DUMP_WRITE(context, "pod %v", value);
        } else {
            SERIALIZATION_DUMP_WRITE(context, "pod[%v] %v", sizeof(T), DumpRangeToHex(TRef::FromPod(value)));
        }
    }
};

#define XX(type) \
    template <> \
    struct TSerializationDumpPodWriter<type> \
    { \
        template <class C> \
        static void Do(C& context, const type& value) \
        { \
            SERIALIZATION_DUMP_WRITE(context, #type " %v", value); \
        } \
    };

XX(i8)
XX(ui8)
XX(i16)
XX(ui16)
XX(i32)
XX(ui32)
XX(i64)
XX(ui64)
XX(float)
XX(double)
XX(char)
XX(bool)
XX(TInstant)
XX(TDuration)

#undef XX

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
