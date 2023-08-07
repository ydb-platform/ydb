#include "syntax_checker.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NYson::NDetail {

////////////////////////////////////////////////////////////////////////////////

TYsonSyntaxChecker::TYsonSyntaxChecker(EYsonType ysonType, int nestingLevelLimit)
    : NestingLevelLimit_(static_cast<ui32>(nestingLevelLimit))
{
    StateStack_.push_back(EYsonState::Terminated);
    switch (ysonType) {
        case EYsonType::Node:
            StateStack_.push_back(EYsonState::ExpectValue);
            break;
        case EYsonType::ListFragment:
            StateStack_.push_back(EYsonState::InsideListFragmentExpectValue);
            break;
        case EYsonType::MapFragment:
            StateStack_.push_back(EYsonState::InsideMapFragmentExpectKey);
            break;
        default:
            YT_ABORT();
    }
}

TStringBuf TYsonSyntaxChecker::StateExpectationString(EYsonState state)
{
    switch (state) {
        case EYsonState::Terminated:
            return "no further tokens (yson is completed)";

        case EYsonState::ExpectValue:
        case EYsonState::ExpectAttributelessValue:
        case EYsonState::InsideListFragmentExpectAttributelessValue:
        case EYsonState::InsideListFragmentExpectValue:
        case EYsonState::InsideMapFragmentExpectAttributelessValue:
        case EYsonState::InsideMapFragmentExpectValue:
        case EYsonState::InsideMapExpectAttributelessValue:
        case EYsonState::InsideMapExpectValue:
        case EYsonState::InsideAttributeMapExpectAttributelessValue:
        case EYsonState::InsideAttributeMapExpectValue:
        case EYsonState::InsideListExpectAttributelessValue:
        case EYsonState::InsideListExpectValue:
            return "value";

        case EYsonState::InsideMapFragmentExpectKey:
        case EYsonState::InsideMapExpectKey:
            return "key";
        case EYsonState::InsideAttributeMapExpectKey:
            return "attribute key";

        case EYsonState::InsideMapFragmentExpectEquality:
        case EYsonState::InsideMapExpectEquality:
        case EYsonState::InsideAttributeMapExpectEquality:
            return "=";

        case EYsonState::InsideListFragmentExpectSeparator:
        case EYsonState::InsideMapFragmentExpectSeparator:
        case EYsonState::InsideMapExpectSeparator:
        case EYsonState::InsideListExpectSeparator:
        case EYsonState::InsideAttributeMapExpectSeparator:
            return ";";
    }
    YT_ABORT();
}

void TYsonSyntaxChecker::ThrowUnexpectedToken(TStringBuf token, TStringBuf extraMessage)
{
    THROW_ERROR_EXCEPTION("Unexpected %Qv, expected %Qv%v",
        token,
        StateExpectationString(StateStack_.back()),
        extraMessage)
        << TErrorAttribute("yson_parser_state", StateStack_.back());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson::NDetail
