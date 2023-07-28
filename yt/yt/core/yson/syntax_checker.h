#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EYsonItemType, ui8,
    (EndOfStream)
    (BeginMap)
    (EndMap)
    (BeginAttributes)
    (EndAttributes)
    (BeginList)
    (EndList)
    (EntityValue)
    (BooleanValue)
    (Int64Value)
    (Uint64Value)
    (DoubleValue)
    (StringValue)
);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EYsonState, ui8,
    (Terminated)
    (ExpectValue)
    (ExpectAttributelessValue)

    // top level list fragment
    (InsideListFragmentExpectAttributelessValue)
    (InsideListFragmentExpectValue)
    (InsideListFragmentExpectSeparator)

    // top level map fragment
    (InsideMapFragmentExpectKey)
    (InsideMapFragmentExpectEquality)
    (InsideMapFragmentExpectAttributelessValue)
    (InsideMapFragmentExpectValue)
    (InsideMapFragmentExpectSeparator)

    (InsideMapExpectKey)
    (InsideMapExpectEquality)
    (InsideMapExpectAttributelessValue)
    (InsideMapExpectValue)
    (InsideMapExpectSeparator)

    (InsideAttributeMapExpectKey)
    (InsideAttributeMapExpectEquality)
    (InsideAttributeMapExpectAttributelessValue)
    (InsideAttributeMapExpectValue)
    (InsideAttributeMapExpectSeparator)

    (InsideListExpectAttributelessValue)
    (InsideListExpectValue)
    (InsideListExpectSeparator)
);

////////////////////////////////////////////////////////////////////////////////

class TYsonSyntaxChecker
{
public:
    TYsonSyntaxChecker(EYsonType ysonType, int nestingLevelLimit);

    Y_FORCE_INLINE void OnSimpleNonstring(EYsonItemType itemType);
    Y_FORCE_INLINE void OnString();
    Y_FORCE_INLINE void OnFinish();
    Y_FORCE_INLINE void OnEquality();
    Y_FORCE_INLINE void OnSeparator();
    Y_FORCE_INLINE void OnBeginList();
    Y_FORCE_INLINE void OnEndList();
    Y_FORCE_INLINE void OnBeginMap();
    Y_FORCE_INLINE void OnEndMap();
    Y_FORCE_INLINE void OnAttributesBegin();

    Y_FORCE_INLINE void OnAttributesEnd();

    Y_FORCE_INLINE size_t GetNestingLevel() const;
    Y_FORCE_INLINE bool IsOnValueBoundary(size_t nestingLevel) const;

    Y_FORCE_INLINE bool IsOnKey() const;

    //
    // If called right after OnSeparator, tells
    // whether it is a list separator.
    Y_FORCE_INLINE bool IsListSeparator() const;
    // Y_FORCE_INLINE bool IsOnListItemStart(bool isSimple) const;

private:
    template <bool isString>
    Y_FORCE_INLINE void OnSimple(EYsonItemType itemType);
    Y_FORCE_INLINE void IncrementNestingLevel();
    Y_FORCE_INLINE void DecrementNestingLevel();

    static TStringBuf StateExpectationString(EYsonState state);
    void ThrowUnexpectedToken(TStringBuf token, TStringBuf extraMessage = {});

private:
    TCompactVector<EYsonState, 16> StateStack_;

    // We don't use stack size, we compute depth level precisely to be compatible with old yson parser.
    ui32 NestingLevel_ = 0;
    ui32 NestingLevelLimit_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

#define SYNTAX_CHECKER_INL_H_
#include "syntax_checker-inl.h"
#undef SYNTAX_CHECKER_INL_H_
