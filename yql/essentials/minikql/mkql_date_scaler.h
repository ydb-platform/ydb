#pragma once

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_types.h>

namespace NKikimr::NMiniKQL {
using TScaledDate = i64;

constexpr TScaledDate DateScale = 86400000000ll;
constexpr TScaledDate DatetimeScale = 1000000ll;

template <typename TSrc>
inline TScaledDate ToScaledDate(typename TSrc::TLayout src);

template <typename TDst>
inline
    typename TDst::TLayout
    FromScaledDate(TScaledDate src);

template <>
inline TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TDate>>(typename NUdf::TDataType<NUdf::TDate>::TLayout src) {
    return src * DateScale;
}

template <>
inline NUdf::TDataType<NUdf::TDate>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TDate>>(TScaledDate src) {
    return src / DateScale;
}

template <>
inline TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TDatetime>>(typename NUdf::TDataType<NUdf::TDatetime>::TLayout src) {
    return src * DatetimeScale;
}

template <>
inline NUdf::TDataType<NUdf::TDatetime>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TDatetime>>(TScaledDate src) {
    return src / DatetimeScale;
}

template <>
inline TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTimestamp>>(typename NUdf::TDataType<NUdf::TTimestamp>::TLayout src) {
    return src;
}

template <>
inline NUdf::TDataType<NUdf::TTimestamp>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTimestamp>>(TScaledDate src) {
    return src;
}

template <>
inline TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TInterval>>(typename NUdf::TDataType<NUdf::TInterval>::TLayout src) {
    return src;
}

template <>
inline NUdf::TDataType<NUdf::TInterval>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TInterval>>(TScaledDate src) {
    return src;
}

template <>
inline TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTzDate>>(typename NUdf::TDataType<NUdf::TTzDate>::TLayout src) {
    return src * DateScale;
}

template <>
inline NUdf::TDataType<NUdf::TTzDate>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTzDate>>(TScaledDate src) {
    return src / DateScale;
}

template <>
inline TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTzDatetime>>(typename NUdf::TDataType<NUdf::TTzDatetime>::TLayout src) {
    return src * DatetimeScale;
}

template <>
inline NUdf::TDataType<NUdf::TTzDatetime>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTzDatetime>>(TScaledDate src) {
    return src / DatetimeScale;
}

template <>
inline TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTzTimestamp>>(typename NUdf::TDataType<NUdf::TTzTimestamp>::TLayout src) {
    return src;
}

template <>
inline NUdf::TDataType<NUdf::TTzTimestamp>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTzTimestamp>>(TScaledDate src) {
    return src;
}

template <>
inline TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TDate32>>(typename NUdf::TDataType<NUdf::TDate32>::TLayout src) {
    return src * DateScale;
}

template <>
inline NUdf::TDataType<NUdf::TDate32>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TDate32>>(TScaledDate src) {
    return src / DateScale;
}

template <>
inline TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TDatetime64>>(typename NUdf::TDataType<NUdf::TDatetime64>::TLayout src) {
    return src * DatetimeScale;
}

template <>
inline NUdf::TDataType<NUdf::TDatetime64>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TDatetime64>>(TScaledDate src) {
    return src / DatetimeScale;
}

template <>
inline TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTimestamp64>>(typename NUdf::TDataType<NUdf::TTimestamp64>::TLayout src) {
    return src;
}

template <>
inline NUdf::TDataType<NUdf::TTimestamp64>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTimestamp64>>(TScaledDate src) {
    return src;
}

template <>
inline TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTzDate32>>(typename NUdf::TDataType<NUdf::TTzDate32>::TLayout src) {
    return src * DateScale;
}

template <>
inline NUdf::TDataType<NUdf::TTzDate32>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTzDate32>>(TScaledDate src) {
    return src / DateScale;
}

template <>
inline TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTzDatetime64>>(typename NUdf::TDataType<NUdf::TTzDatetime64>::TLayout src) {
    return src * DatetimeScale;
}

template <>
inline NUdf::TDataType<NUdf::TTzDatetime64>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTzDatetime64>>(TScaledDate src) {
    return src / DatetimeScale;
}

template <>
inline TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTzTimestamp64>>(typename NUdf::TDataType<NUdf::TTzTimestamp64>::TLayout src) {
    return src;
}

template <>
inline NUdf::TDataType<NUdf::TTzTimestamp64>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTzTimestamp64>>(TScaledDate src) {
    return src;
}

template <>
inline TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TInterval64>>(typename NUdf::TDataType<NUdf::TInterval64>::TLayout src) {
    return src;
}

template <>
inline NUdf::TDataType<NUdf::TInterval64>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TInterval64>>(TScaledDate src) {
    return src;
}

} // namespace NKikimr::NMiniKQL
