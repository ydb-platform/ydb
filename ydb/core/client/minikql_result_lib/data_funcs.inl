#pragma once


#define ENSURE_SCHEME_TYPE(expected, gotName) \
    do { \
        Y_ENSURE(expected == NScheme::NTypeIds::gotName, "Data scheme type mismatch: expected " << expected << ", but got " #gotName << "."); \
    } while (0);


template <>
inline bool HasData<bool>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_SCHEME_TYPE(schemeType, Bool);
    return value.HasBool();
}

template <>
inline bool HasData<i32>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_SCHEME_TYPE(schemeType, Int32);
    return value.HasInt32();
}

template <>
inline bool HasData<ui32>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_SCHEME_TYPE(schemeType, Uint32);
    return value.HasUint32();
}

template <>
inline bool HasData<i64>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_SCHEME_TYPE(schemeType, Int64);
    return value.HasInt64();
}

template <>
inline bool HasData<ui64>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_SCHEME_TYPE(schemeType, Uint64);
    return value.HasUint64();
}

template <>
inline bool HasData<float>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_SCHEME_TYPE(schemeType, Float);
    return value.HasFloat();
}

template <>
inline bool HasData<double>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_SCHEME_TYPE(schemeType, Double);
    return value.HasDouble();
}

template <>
inline bool HasData<TStringBuf>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    if (schemeType == NScheme::NTypeIds::Utf8) {
        return value.HasText();
    } else {
        return value.HasBytes();
    }
}

#undef ENSURE_SCHEME_TYPE

#define ENSURE_HAS_DATA(type, value, schemeType) \
    do { \
        Y_ENSURE(HasData<type>(value, schemeType), "No data of type " #type "."); \
    } while (0);

template <>
inline bool GetData<bool>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_HAS_DATA(bool, value, schemeType);
    return value.GetBool();
}

template <>
inline i32 GetData<i32>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_HAS_DATA(i32, value, schemeType);
    return value.GetInt32();
}

template <>
inline ui32 GetData<ui32>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_HAS_DATA(ui32, value, schemeType);
    return value.GetUint32();
}

template <>
inline i64 GetData<i64>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_HAS_DATA(i64, value, schemeType);
    return value.GetInt64();
}

template <>
inline ui64 GetData<ui64>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_HAS_DATA(ui64, value, schemeType);
    return value.GetUint64();
}

template <>
inline float GetData<float>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_HAS_DATA(float, value, schemeType);
    return value.GetFloat();
}

template <>
inline double GetData<double>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_HAS_DATA(double, value, schemeType);
    return value.GetDouble();
}

template <>
inline TStringBuf GetData<TStringBuf>(const TProtoValue& value, NScheme::TTypeId schemeType) {
    ENSURE_HAS_DATA(TStringBuf, value, schemeType);
    if (schemeType == NScheme::NTypeIds::Utf8) {
        return value.GetText();
    } else {
        return value.GetBytes();
    }
}

#undef ENSURE_HAS_DATA
