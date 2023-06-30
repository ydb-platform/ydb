#ifndef SYNTAX_CHECKER_INL_H_
#error "Direct inclusion of this file is not allowed, include syntax_checker.h"
// For the sake of sane code completion.
#include "syntax_checker.h"
#endif

namespace NYT::NYson::NDetail {

////////////////////////////////////////////////////////////////////////////////

void TYsonSyntaxChecker::OnSimpleNonstring(EYsonItemType itemType)
{
    OnSimple<false>(itemType);
}

void TYsonSyntaxChecker::OnString()
{
    OnSimple<true>(EYsonItemType::StringValue);
}

void TYsonSyntaxChecker::OnFinish()
{
    const auto state = StateStack_.back();
    switch (state) {
        case EYsonState::Terminated:
        case EYsonState::InsideListFragmentExpectValue:
        case EYsonState::InsideListFragmentExpectSeparator:
        case EYsonState::InsideMapFragmentExpectKey:
        case EYsonState::InsideMapFragmentExpectSeparator:
            return;
        default:
            ThrowUnexpectedToken("finish");
    }
}

void TYsonSyntaxChecker::OnEquality()
{
    switch (StateStack_.back()) {
        case EYsonState::InsideMapExpectEquality:
            StateStack_.back() = EYsonState::InsideMapExpectValue;
            break;
        case EYsonState::InsideAttributeMapExpectEquality:
            StateStack_.back() = EYsonState::InsideAttributeMapExpectValue;
            break;
        case EYsonState::InsideMapFragmentExpectEquality:
            StateStack_.back() = EYsonState::InsideMapFragmentExpectValue;
            break;
        default:
            ThrowUnexpectedToken("=");
    }
}

void TYsonSyntaxChecker::OnSeparator()
{
    switch (StateStack_.back()) {
        case EYsonState::InsideMapFragmentExpectSeparator:
            StateStack_.back() = EYsonState::InsideMapFragmentExpectKey;
            break;
        case EYsonState::InsideMapExpectSeparator:
            StateStack_.back() = EYsonState::InsideMapExpectKey;
            break;
        case EYsonState::InsideAttributeMapExpectSeparator:
            StateStack_.back() = EYsonState::InsideAttributeMapExpectKey;
            break;
        case EYsonState::InsideListExpectSeparator:
            StateStack_.back() = EYsonState::InsideListExpectValue;
            break;
        case EYsonState::InsideListFragmentExpectSeparator:
            StateStack_.back() = EYsonState::InsideListFragmentExpectValue;
            break;
        default:
            if (StateStack_.back() == EYsonState::Terminated) {
                ThrowUnexpectedToken(";", Format("; maybe you should use yson_type = %Qlv", EYsonType::ListFragment));
            }
            ThrowUnexpectedToken(";");
    }
}

void TYsonSyntaxChecker::OnBeginList()
{
    switch (StateStack_.back()) {
        case EYsonState::ExpectValue:
        case EYsonState::ExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideListExpectValue;
            break;
        case EYsonState::InsideListFragmentExpectValue:
        case EYsonState::InsideListFragmentExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideListFragmentExpectSeparator;
            StateStack_.push_back(EYsonState::InsideListExpectValue);
            break;
        case EYsonState::InsideListExpectValue:
        case EYsonState::InsideListExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideListExpectSeparator;
            StateStack_.push_back(EYsonState::InsideListExpectValue);
            break;
        case EYsonState::InsideMapFragmentExpectValue:
        case EYsonState::InsideMapFragmentExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideMapFragmentExpectSeparator;
            StateStack_.push_back(EYsonState::InsideListExpectValue);
            break;
        case EYsonState::InsideMapExpectValue:
        case EYsonState::InsideMapExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideMapExpectSeparator;
            StateStack_.push_back(EYsonState::InsideListExpectValue);
            break;
        case EYsonState::InsideAttributeMapExpectAttributelessValue:
        case EYsonState::InsideAttributeMapExpectValue:
            StateStack_.back() = EYsonState::InsideAttributeMapExpectSeparator;
            StateStack_.push_back(EYsonState::InsideListExpectValue);
            break;
        default:
            ThrowUnexpectedToken("[");
    }
    IncrementNestingLevel();
}

void TYsonSyntaxChecker::OnEndList()
{
    switch (StateStack_.back()) {
        case EYsonState::InsideListExpectValue:
        case EYsonState::InsideListExpectSeparator:
            StateStack_.pop_back();
            break;
        default:
            ThrowUnexpectedToken("]");
    }
    DecrementNestingLevel();
}

void TYsonSyntaxChecker::OnBeginMap()
{
    switch (StateStack_.back()) {
        case EYsonState::ExpectValue:
        case EYsonState::ExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideMapExpectKey;
            break;
        case EYsonState::InsideListFragmentExpectValue:
        case EYsonState::InsideListFragmentExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideListFragmentExpectSeparator;
            StateStack_.push_back(EYsonState::InsideMapExpectKey);
            break;
        case EYsonState::InsideListExpectValue:
        case EYsonState::InsideListExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideListExpectSeparator;
            StateStack_.push_back(EYsonState::InsideMapExpectKey);
            break;
        case EYsonState::InsideMapFragmentExpectValue:
        case EYsonState::InsideMapFragmentExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideMapFragmentExpectSeparator;
            StateStack_.push_back(EYsonState::InsideMapExpectKey);
            break;
        case EYsonState::InsideMapExpectValue:
        case EYsonState::InsideMapExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideMapExpectSeparator;
            StateStack_.push_back(EYsonState::InsideMapExpectKey);
            break;
        case EYsonState::InsideAttributeMapExpectAttributelessValue:
        case EYsonState::InsideAttributeMapExpectValue:
            StateStack_.back() = EYsonState::InsideAttributeMapExpectSeparator;
            StateStack_.push_back(EYsonState::InsideMapExpectKey);
            break;
        default:
            ThrowUnexpectedToken("{");
    }
    IncrementNestingLevel();
}

void TYsonSyntaxChecker::OnEndMap()
{
    switch (StateStack_.back()) {
        case EYsonState::InsideMapExpectKey:
        case EYsonState::InsideMapExpectSeparator:
            StateStack_.pop_back();
            break;
        default:
            ThrowUnexpectedToken("}");
    }
    DecrementNestingLevel();
}

void TYsonSyntaxChecker::OnAttributesBegin()
{
    switch (StateStack_.back()) {
        case EYsonState::ExpectValue:
            StateStack_.back() = EYsonState::ExpectAttributelessValue;
            StateStack_.push_back(EYsonState::InsideAttributeMapExpectKey);
            break;
        case EYsonState::InsideListFragmentExpectValue:
            StateStack_.back() = EYsonState::InsideListFragmentExpectAttributelessValue;
            StateStack_.push_back(EYsonState::InsideAttributeMapExpectKey);
            break;
        case EYsonState::InsideMapFragmentExpectValue:
            StateStack_.back() = EYsonState::InsideMapFragmentExpectAttributelessValue;
            StateStack_.push_back(EYsonState::InsideAttributeMapExpectKey);
            break;
        case EYsonState::InsideMapExpectValue:
            StateStack_.back() = EYsonState::InsideMapExpectAttributelessValue;
            StateStack_.push_back(EYsonState::InsideAttributeMapExpectKey);
            break;
        case EYsonState::InsideAttributeMapExpectValue:
            StateStack_.back() = EYsonState::InsideAttributeMapExpectAttributelessValue;
            StateStack_.push_back(EYsonState::InsideAttributeMapExpectKey);
            break;
        case EYsonState::InsideListExpectValue:
            StateStack_.back() = EYsonState::InsideListExpectAttributelessValue;
            StateStack_.push_back(EYsonState::InsideAttributeMapExpectKey);
            break;

        case EYsonState::InsideListFragmentExpectAttributelessValue:
        case EYsonState::InsideMapFragmentExpectAttributelessValue:
        case EYsonState::ExpectAttributelessValue:
        case EYsonState::InsideMapExpectAttributelessValue:
        case EYsonState::InsideListExpectAttributelessValue:
        case EYsonState::InsideAttributeMapExpectAttributelessValue:
            THROW_ERROR_EXCEPTION("Value cannot have several attribute maps");
        default:
            ThrowUnexpectedToken("<");
    }
    IncrementNestingLevel();
}

void TYsonSyntaxChecker::OnAttributesEnd()
{
    switch (StateStack_.back()) {
        case EYsonState::InsideAttributeMapExpectKey:
        case EYsonState::InsideAttributeMapExpectSeparator:
            StateStack_.pop_back();
            break;
        default:
            ThrowUnexpectedToken(">");
    }
    DecrementNestingLevel();
}

size_t TYsonSyntaxChecker::GetNestingLevel() const
{
    return NestingLevel_;
}

bool TYsonSyntaxChecker::IsOnValueBoundary(size_t nestingLevel) const
{
    if (NestingLevel_ != nestingLevel) {
        return false;
    }
    switch (StateStack_.back()) {
        case EYsonState::Terminated:
        case EYsonState::ExpectValue:
        case EYsonState::InsideListFragmentExpectValue:
        case EYsonState::InsideListFragmentExpectSeparator:
        case EYsonState::InsideMapFragmentExpectEquality:
        case EYsonState::InsideMapFragmentExpectSeparator:
        case EYsonState::InsideMapExpectEquality:
        case EYsonState::InsideMapExpectSeparator:
        case EYsonState::InsideAttributeMapExpectEquality:
        case EYsonState::InsideAttributeMapExpectSeparator:
        case EYsonState::InsideListExpectValue:
        case EYsonState::InsideListExpectSeparator:
            return true;

        default:
            return false;
    }
}

bool TYsonSyntaxChecker::IsOnKey() const
{
    switch (StateStack_.back()) {
        case EYsonState::InsideMapFragmentExpectEquality:
        case EYsonState::InsideMapExpectEquality:
        case EYsonState::InsideAttributeMapExpectEquality:
            return true;
        default:
            return false;
    }
}

bool TYsonSyntaxChecker::IsListSeparator() const
{
    switch (StateStack_.back()) {
        case EYsonState::InsideListFragmentExpectValue:
        case EYsonState::InsideListExpectValue:
            return true;
        default:
            return false;
    }
}

// bool TYsonSyntaxChecker::IsOnListItemStart(bool isSimple) const
// {
//     switch (StateStack_.back()) {
//         case EYsonState::InsideListFragmentExpectSeparator:
//         case EYsonState::InsideListExpectSeparator:
//             // Simple list item.
//             return isSimple;
//         case EYsonState::InsideListFragmentExpectValue:
//         case EYsonState::InsideListExpectValue:
//         case EYsonState::InsideMapFragmentExpectKey:
//         case EYsonState::InsideMapExpectKey: {
//             // Possibly composite list item.
//             if (StateStack_.size() < 2) {
//                 return false;
//             }
//             auto previousState = StateStack_[StateStack_.size() - 2];
//             return !isSimple && (previousState == EYsonState::InsideListExpectSeparator);
//         }
//         case EYsonState::InsideAttributeMapExpectKey: {
//             // Possibly composite list item.
//             if (StateStack_.size() < 2) {
//                 return false;
//             }
//             auto previousState = StateStack_[StateStack_.size() - 2];
//             return (previousState == EYsonState::InsideListExpectAttributelessValue);
//         }
//         default:
//             return false;
//     }
// }

template <bool isString>
void TYsonSyntaxChecker::OnSimple(EYsonItemType itemType)
{
    switch (StateStack_.back()) {
        case EYsonState::ExpectValue:
        case EYsonState::ExpectAttributelessValue:
            StateStack_.pop_back();
            break;
        case EYsonState::InsideListFragmentExpectValue:
        case EYsonState::InsideListFragmentExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideListFragmentExpectSeparator;
            break;
        case EYsonState::InsideListExpectValue:
        case EYsonState::InsideListExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideListExpectSeparator;
            break;
        case EYsonState::InsideMapFragmentExpectKey:
            if constexpr (isString) {
                StateStack_.back() = EYsonState::InsideMapFragmentExpectEquality;
            } else {
                THROW_ERROR_EXCEPTION("Cannot parse key of map fragment; expected string got %Qv",
                    itemType);
            }
            break;
        case EYsonState::InsideMapFragmentExpectValue:
        case EYsonState::InsideMapFragmentExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideMapFragmentExpectSeparator;
            break;
        case EYsonState::InsideMapExpectKey:
            if constexpr (isString) {
                StateStack_.back() = EYsonState::InsideMapExpectEquality;
            } else {
                THROW_ERROR_EXCEPTION("Cannot parse key of map; expected \"string\" got %Qv",
                    itemType);
            }
            break;
        case EYsonState::InsideMapExpectValue:
        case EYsonState::InsideMapExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideMapExpectSeparator;
            break;
        case EYsonState::InsideAttributeMapExpectKey:
            if constexpr (isString) {
                StateStack_.back() = EYsonState::InsideAttributeMapExpectEquality;
            } else {
                THROW_ERROR_EXCEPTION("Cannot parse key of attribute map; expected \"string\" got %Qv",
                    itemType);
            }
            break;
        case EYsonState::InsideAttributeMapExpectAttributelessValue:
        case EYsonState::InsideAttributeMapExpectValue:
            StateStack_.back() = EYsonState::InsideAttributeMapExpectSeparator;
            break;
        default:
            ThrowUnexpectedToken("value");
    }
}

void TYsonSyntaxChecker::IncrementNestingLevel()
{
    ++NestingLevel_;
    if (NestingLevel_ >= NestingLevelLimit_) {
        THROW_ERROR_EXCEPTION("Depth limit exceeded while parsing YSON")
            << TErrorAttribute("limit", NestingLevelLimit_);
    }
}

void TYsonSyntaxChecker::DecrementNestingLevel()
{
    YT_ASSERT(NestingLevel_ > 0);
    --NestingLevel_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson::NDetail

