#pragma once

#include <library/cpp/yt/string/format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSerializationDumper
{
public:
    Y_FORCE_INLINE bool IsEnabled() const
    {
        return Enabled_;
    }

    Y_FORCE_INLINE void SetEnabled(bool value)
    {
        Enabled_ = value;
    }

    Y_FORCE_INLINE void Indent()
    {
        ++IndentCount_;
    }

    Y_FORCE_INLINE void Unindent()
    {
        --IndentCount_;
    }


    Y_FORCE_INLINE void Suspend()
    {
        ++SuspendCount_;
    }

    Y_FORCE_INLINE void Resume()
    {
        --SuspendCount_;
    }

    Y_FORCE_INLINE bool IsSuspended() const
    {
        return SuspendCount_ > 0;
    }


    Y_FORCE_INLINE bool IsActive() const
    {
        return IsEnabled() && !IsSuspended();
    }


    void SetFieldName(TStringBuf name)
    {
        FieldName_ = name;
    }

    template <class... TArgs>
    void Write(const char* format, const TArgs&... args)
    {
        if (!IsActive()) {
            return;
        }

        TStringBuilder builder;
        builder.AppendChar(' ', IndentCount_ * 2);
        if (FieldName_) {
            builder.AppendString(FieldName_);
            builder.AppendString(": ");
            FieldName_ = {};
        }
        builder.AppendFormat(format, args...);
        builder.AppendChar('\n');
        auto buffer = builder.GetBuffer();
        fwrite(buffer.begin(), buffer.length(), 1, stderr);
    }

private:
    bool Enabled_ = false;
    int IndentCount_ = 0;
    int SuspendCount_ = 0;
    TStringBuf FieldName_;
};

class TSerializeDumpIndentGuard
    : private TNonCopyable
{
public:
    explicit TSerializeDumpIndentGuard(TSerializationDumper* dumper)
        : Dumper_(dumper)
    {
        Dumper_->Indent();
    }

    TSerializeDumpIndentGuard(TSerializeDumpIndentGuard&& other)
        : Dumper_(other.Dumper_)
    {
        other.Dumper_ = nullptr;
    }

    ~TSerializeDumpIndentGuard()
    {
        if (Dumper_) {
            Dumper_->Unindent();
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

class TSerializeDumpSuspendGuard
    : private TNonCopyable
{
public:
    explicit TSerializeDumpSuspendGuard(TSerializationDumper* dumper)
        : Dumper_(dumper)
    {
        Dumper_->Suspend();
    }

    TSerializeDumpSuspendGuard(TSerializeDumpSuspendGuard&& other)
        : Dumper_(other.Dumper_)
    {
        other.Dumper_ = nullptr;
    }

    ~TSerializeDumpSuspendGuard()
    {
        if (Dumper_) {
            Dumper_->Resume();
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

#define SERIALIZATION_DUMP_WRITE(context, ...) \
    if (Y_LIKELY(!(context).Dumper().IsActive())) { \
    } else \
        (context).Dumper().Write(__VA_ARGS__)

#define SERIALIZATION_DUMP_INDENT(context) \
    if (auto SERIALIZATION_DUMP_INDENT__Guard = NYT::TSerializeDumpIndentGuard(&(context).Dumper())) { \
        Y_UNREACHABLE(); \
    } else

#define SERIALIZATION_DUMP_SUSPEND(context) \
    if (auto SERIALIZATION_DUMP_SUSPEND__Guard = NYT::TSerializeDumpSuspendGuard(&(context).Dumper())) { \
        Y_UNREACHABLE(); \
    } else

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
