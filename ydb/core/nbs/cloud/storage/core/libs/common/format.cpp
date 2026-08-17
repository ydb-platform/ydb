#include "format.h"

#include <util/stream/format.h>
#include <util/stream/str.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

TString FormatTimestamp(TInstant ts)
{
    return ts.ToStringLocalUpToSeconds();
}

TString FormatDuration(TDuration duration)
{
    ui64 value = duration.MicroSeconds();

    static constexpr ui64 MILLISECOND = 1000;
    static constexpr ui64 SECOND = 1000 * MILLISECOND;
    static constexpr ui64 MINUTE = 60 * SECOND;
    static constexpr ui64 HOUR = 60 * MINUTE;
    static constexpr ui64 DAY = 24 * HOUR;

    TStringStream out;
    if (value == 0) {
        out << value;
    } else if (value < MILLISECOND) {
        out << value << "us";
    } else if (value < SECOND) {
        out << Prec((double)value / MILLISECOND, PREC_POINT_DIGITS, 3) << "ms";
    } else if (value < MINUTE) {
        out << Prec((double)value / SECOND, PREC_POINT_DIGITS, 3) << "s";
    } else if (value < HOUR) {
        out << value / MINUTE << "m " << (value % MINUTE) / SECOND << "s";
    } else if (value < DAY) {
        out << value / HOUR << "h " << (value % HOUR) / MINUTE << "m "
            << (value % MINUTE) / SECOND << "s";
    } else {
        out << value / DAY << "d " << (value % DAY) / HOUR << "h "
            << (value % HOUR) / MINUTE << "m " << (value % MINUTE) / SECOND
            << "s";
    }
    return out.Str();
}

TString FormatByteSize(ui64 size)
{
    static constexpr ui64 KB = 1024;
    static constexpr ui64 MB = 1024 * KB;
    static constexpr ui64 GB = 1024 * MB;
    static constexpr ui64 TB = 1024 * GB;

    TStringStream out;
    if (size < KB) {
        out << size << " B";
    } else if (size < MB) {
        out << Prec((double)size / KB, PREC_POINT_DIGITS, 2) << " KiB";
    } else if (size < GB) {
        out << Prec((double)size / MB, PREC_POINT_DIGITS, 2) << " MiB";
    } else if (size < TB) {
        out << Prec((double)size / GB, PREC_POINT_DIGITS, 2) << " GiB";
    } else {
        out << Prec((double)size / TB, PREC_POINT_DIGITS, 2) << " TiB";
    }
    return out.Str();
}

}   // namespace NYdb::NBS
