#ifndef POSITION_INDEPENDENT_VALUE_INL_H
#error "Direct inclusion of this file is not allowed, position_independent_value.h"
// For the sake of sane code completion.
#include "position_independent_value.h"
#endif

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T FromPositionIndependentValue(const TPIValue& positionIndependentValue)
{
    TUnversionedValue asUnversioned;
    MakeUnversionedFromPositionIndependent(&asUnversioned, positionIndependentValue);
    return FromUnversionedValue<T>(asUnversioned);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE const char* GetStringPosition(const TPIValue& value)
{
    return reinterpret_cast<char*>(value.Data.StringOffset + reinterpret_cast<int64_t>(&value.Data.StringOffset));
}

Y_FORCE_INLINE void SetStringPosition(TPIValue* value, const char* string)
{
    value->Data.StringOffset = reinterpret_cast<int64_t>(string) - reinterpret_cast<int64_t>(&value->Data.StringOffset);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void MakeUnversionedFromPositionIndependent(TUnversionedValue* destination, const TPIValue& source)
{
    destination->Id = source.Id;
    destination->Type = source.Type;
    destination->Flags = source.Flags;
    destination->Length = source.Length;

    if (IsStringLikeType(source.Type)) {
        destination->Data.String = GetStringPosition(source);
    } else {
        destination->Data.Uint64 = source.Data.Uint64;
    }
}

Y_FORCE_INLINE void MakePositionIndependentFromUnversioned(TPIValue* destination, const TUnversionedValue& source)
{
    destination->Id = source.Id;
    destination->Type = source.Type;
    destination->Flags = source.Flags;
    destination->Length = source.Length;

    if (IsStringLikeType(source.Type)) {
        SetStringPosition(destination, source.Data.String);
    } else {
        destination->Data.Uint64 = source.Data.Uint64;
    }
}

Y_FORCE_INLINE void CopyPositionIndependent(TPIValue* destination, const TPIValue& source)
{
    destination->Id = source.Id;
    destination->Type = source.Type;
    destination->Flags = source.Flags;
    destination->Length = source.Length;

    if (IsStringLikeType(source.Type)) {
        SetStringPosition(destination, GetStringPosition(source));
    } else {
        destination->Data.Uint64 = source.Data.Uint64;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

