#include "query_common.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

const char* GetUnaryOpcodeLexeme(EUnaryOp opcode)
{
    switch (opcode) {
        case EUnaryOp::Plus:  return "+";
        case EUnaryOp::Minus: return "-";
        case EUnaryOp::Not:   return "NOT";
        case EUnaryOp::BitNot:return "~";
        default:              YT_ABORT();
    }
}

const char* GetBinaryOpcodeLexeme(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Plus:           return "+";
        case EBinaryOp::Minus:          return "-";
        case EBinaryOp::Multiply:       return "*";
        case EBinaryOp::Divide:         return "/";
        case EBinaryOp::Modulo:         return "%";
        case EBinaryOp::LeftShift:      return "<<";
        case EBinaryOp::RightShift:     return ">>";
        case EBinaryOp::BitAnd:         return "&";
        case EBinaryOp::BitOr:          return "|";
        case EBinaryOp::And:            return "AND";
        case EBinaryOp::Or:             return "OR";
        case EBinaryOp::Equal:          return "=";
        case EBinaryOp::NotEqual:       return "!=";
        case EBinaryOp::Less:           return "<";
        case EBinaryOp::LessOrEqual:    return "<=";
        case EBinaryOp::Greater:        return ">";
        case EBinaryOp::GreaterOrEqual: return ">=";
        case EBinaryOp::Concatenate:    return "||";
        default:                        YT_ABORT();
    }
}

EBinaryOp GetReversedBinaryOpcode(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Less:           return EBinaryOp::Greater;
        case EBinaryOp::LessOrEqual:    return EBinaryOp::GreaterOrEqual;
        case EBinaryOp::Greater:        return EBinaryOp::Less;
        case EBinaryOp::GreaterOrEqual: return EBinaryOp::LessOrEqual;
        default:                        return opcode;
    }
}

EBinaryOp GetInversedBinaryOpcode(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Equal:          return EBinaryOp::NotEqual;
        case EBinaryOp::NotEqual:       return EBinaryOp::Equal;
        case EBinaryOp::Less:           return EBinaryOp::GreaterOrEqual;
        case EBinaryOp::LessOrEqual:    return EBinaryOp::Greater;
        case EBinaryOp::Greater:        return EBinaryOp::LessOrEqual;
        case EBinaryOp::GreaterOrEqual: return EBinaryOp::Less;
        default:                        YT_ABORT();
    }
}

bool IsArithmeticalBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Plus:
        case EBinaryOp::Minus:
        case EBinaryOp::Multiply:
        case EBinaryOp::Divide:
            return true;
        default:
            return false;
    }
}

bool IsIntegralBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Modulo:
        case EBinaryOp::LeftShift:
        case EBinaryOp::RightShift:
        case EBinaryOp::BitOr:
        case EBinaryOp::BitAnd:
            return true;
        default:
            return false;
    }
}

bool IsLogicalBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::And:
        case EBinaryOp::Or:
            return true;
        default:
            return false;
    }
}

bool IsRelationalBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Equal:
        case EBinaryOp::NotEqual:
        case EBinaryOp::Less:
        case EBinaryOp::LessOrEqual:
        case EBinaryOp::Greater:
        case EBinaryOp::GreaterOrEqual:
            return true;
        default:
            return false;
    }
}

bool IsStringBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Concatenate:
            return true;
        default:
            return false;
    }
}

TValue CastValueWithCheck(TValue value, EValueType targetType)
{
    if (value.Type == targetType || value.Type == EValueType::Null) {
        return value;
    }

    if (value.Type == EValueType::Int64) {
        if (targetType == EValueType::Double) {
            auto int64Value = value.Data.Int64;
            if (i64(double(int64Value)) != int64Value) {
                THROW_ERROR_EXCEPTION("Failed to cast %v to double: inaccurate conversion", int64Value);
            }
            value.Data.Double = int64Value;
        } else {
            YT_VERIFY(targetType == EValueType::Uint64);
        }
    } else if (value.Type == EValueType::Uint64) {
        if (targetType == EValueType::Int64) {
            if (value.Data.Uint64 > std::numeric_limits<i64>::max()) {
                THROW_ERROR_EXCEPTION(
                    "Failed to cast %vu to int64: value is greater than maximum", value.Data.Uint64);
            }
        } else if (targetType == EValueType::Double) {
            auto uint64Value = value.Data.Uint64;
            if (ui64(double(uint64Value)) != uint64Value) {
                THROW_ERROR_EXCEPTION("Failed to cast %vu to double: inaccurate conversion", uint64Value);
            }
            value.Data.Double = uint64Value;
        } else {
            YT_ABORT();
        }
    } else if (value.Type == EValueType::Double) {
        auto doubleValue = value.Data.Double;
        if (targetType == EValueType::Uint64) {
            if (double(ui64(doubleValue)) != doubleValue) {
                THROW_ERROR_EXCEPTION("Failed to cast %v to uint64: inaccurate conversion", doubleValue);
            }
            value.Data.Uint64 = doubleValue;
        } else if (targetType == EValueType::Int64) {
            if (double(i64(doubleValue)) != doubleValue) {
                THROW_ERROR_EXCEPTION("Failed to cast %v to int64: inaccurate conversion", doubleValue);
            }
            value.Data.Int64 = doubleValue;
        } else {
            YT_ABORT();
        }
    } else {
        YT_ABORT();
    }

    value.Type = targetType;
    return value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
