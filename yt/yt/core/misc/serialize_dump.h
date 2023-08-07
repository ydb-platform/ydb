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

    Y_FORCE_INLINE void SetLowerWriteCountDumpLimit(i64 lowerLimit)
    {
        LowerWriteCountDumpLimit_ = lowerLimit;
    }

    Y_FORCE_INLINE void SetUpperWriteCountDumpLimit(i64 upperLimit)
    {
        UpperWriteCountDumpLimit_ = upperLimit;
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


    template <class... TArgs>
    void Write(const char* format, const TArgs&... args)
    {
        if (!IsActive())
            return;

        if (WriteCount_ < LowerWriteCountDumpLimit_) {
            ++WriteCount_;
            return;
        }
        if (WriteCount_ >= UpperWriteCountDumpLimit_) {
            SetEnabled(false);
            return;
        }

        TStringBuilder builder;
        builder.AppendString("DUMP ");
        builder.AppendChar(' ', IndentCount_ * 2);
        builder.AppendFormat(format, args...);
        builder.AppendChar('\n');
        auto buffer = builder.GetBuffer();
        fwrite(buffer.begin(), buffer.length(), 1, stderr);

        ++WriteCount_;
    }

    void IncrementWriteCountIfNotSuspended()
    {
        if (!IsSuspended()) {
            ++WriteCount_;
        }
    }

    void ReportWriteCount()
    {
        TStringBuilder builder;
        builder.AppendFormat("%v\n", WriteCount_);
        auto buffer = builder.GetBuffer();
        fwrite(buffer.begin(), buffer.length(), 1, stdout);
        fflush(stdout);
    }

private:
    bool Enabled_ = false;
    int IndentCount_ = 0;
    int SuspendCount_ = 0;

    i64 WriteCount_ = 0;
    i64 LowerWriteCountDumpLimit_ = 0;
    i64 UpperWriteCountDumpLimit_ = std::numeric_limits<i64>::max();
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
    if (Y_LIKELY(!(context).Dumper().IsActive())) \
    { \
        if ((context).GetEnableTotalWriteCountReport()) \
            (context).Dumper().IncrementWriteCountIfNotSuspended(); \
    } \
    else \
        (context).Dumper().Write(__VA_ARGS__)

#define SERIALIZATION_DUMP_INDENT(context) \
    if (auto SERIALIZATION_DUMP_INDENT__Guard = NYT::TSerializeDumpIndentGuard(&(context).Dumper())) \
        { YT_ABORT(); } \
    else

#define SERIALIZATION_DUMP_SUSPEND(context) \
    if (auto SERIALIZATION_DUMP_SUSPEND__Guard = NYT::TSerializeDumpSuspendGuard(&(context).Dumper())) \
        { YT_ABORT(); } \
    else

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
        if constexpr(TFormatTraits<T>::HasCustomFormatValue) {
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
