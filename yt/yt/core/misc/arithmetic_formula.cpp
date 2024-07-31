#include "arithmetic_formula.h"
#include "phoenix.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

#include <library/cpp/yt/misc/variant.h>

#include <util/generic/hash.h>

#include <algorithm>

namespace NYT {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EEvaluationContext,
    (Boolean)
    (Arithmetic)
);

bool IsSymbolAllowedInName(char c, EEvaluationContext context, bool isFirst)
{
    const static THashSet<char> extraAllowedBooleanVariableTokens{'/', '-', '.', ':'};

    if (std::isalpha(c) || c == '_') {
        return true;
    }
    if (std::isdigit(c)) {
        return !isFirst || context == EEvaluationContext::Boolean;
    }
    if (context == EEvaluationContext::Boolean && extraAllowedBooleanVariableTokens.contains(c)) {
        return true;
    }
    return false;
}

void ValidateFormulaVariable(const TString& variable, EEvaluationContext context)
{
    if (variable.empty()) {
        THROW_ERROR_EXCEPTION("Variable should not be empty");
    }
    for (char c : variable) {
        if (!IsSymbolAllowedInName(c, context, /*isFirst*/ false)) {
            THROW_ERROR_EXCEPTION("Invalid character %Qv in variable %Qv", c, variable);
        }
    }
    if (context == EEvaluationContext::Boolean) {
        if (std::all_of(variable.begin(), variable.end(), [] (char c) {return std::isdigit(c);})) {
            THROW_ERROR_EXCEPTION("All digits characters are prohibited for boolean variable %Qv", variable);
        }
    }
    if (!IsSymbolAllowedInName(variable[0], context, /*isFirst*/ true)) {
        THROW_ERROR_EXCEPTION("Invalid first character in variable %Qv", variable);
    }
    if (variable == "in") {
        THROW_ERROR_EXCEPTION("Invalid variable name %Qv", variable);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ValidateArithmeticFormulaVariable(const TString& variable)
{
    ValidateFormulaVariable(variable, EEvaluationContext::Arithmetic);
}

void ValidateBooleanFormulaVariable(const TString& variable)
{
    ValidateFormulaVariable(variable, EEvaluationContext::Boolean);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void ThrowError(const TString& formula, int position, const TString& message, EEvaluationContext evaluationContext)
{
    const static int maxContextSize = 30;

    int contextStart = std::max(0, position - maxContextSize / 2);
    TString context = formula.substr(contextStart, maxContextSize);
    int contextPosition = std::min(position, maxContextSize / 2);

    TStringBuilder builder;
    builder.AppendFormat(
        "Error while parsing %v formula:\n%v\n",
        evaluationContext == EEvaluationContext::Arithmetic ? "arithmetic" : "boolean",
        formula);
    builder.AppendChar(' ', position);
    builder.AppendFormat("^\n%v", message);
    THROW_ERROR_EXCEPTION(builder.Flush())
        << TErrorAttribute("context", context)
        << TErrorAttribute("context_pos", contextPosition);
}

// NB: 'Set' type cannot appear in parsed/tokenized formula, it is needed only for CheckTypeConsistency.
#define FOR_EACH_TOKEN(func) \
    func(0, Variable) \
    func(0, Number) \
    func(0, BooleanLiteral) \
    func(0, Set) \
    func(0, LeftBracket) \
    func(0, RightBracket) \
    func(1, In) \
    func(2, Comma) \
    func(3, LogicalOr) \
    func(4, LogicalAnd) \
    func(5, BitwiseOr) \
    func(6, BitwiseXor) \
    func(7, BitwiseAnd) \
    func(8, Equals) \
    func(8, NotEquals) \
    func(9, Less) \
    func(9, Greater) \
    func(9, LessOrEqual) \
    func(9, GreaterOrEqual) \
    func(10, Plus) \
    func(10, Minus) \
    func(11, Multiplies) \
    func(11, Divides) \
    func(11, Modulus) \
    func(12, LogicalNot)

#define EXTRACT_FIELD_NAME(x, y) (y)
#define EXTRACT_PRECEDENCE(x, y) x,

DEFINE_ENUM(EFormulaTokenType,
    FOR_EACH_TOKEN(EXTRACT_FIELD_NAME)
);

static int Precedence(EFormulaTokenType type) {
    static constexpr int precedence[] =
    {
        FOR_EACH_TOKEN(EXTRACT_PRECEDENCE)
    };
    int index = static_cast<int>(type);
    YT_VERIFY(0 <= index && index < static_cast<int>(sizeof(precedence) / sizeof(*precedence)));
    return precedence[index];
}

#undef FOR_EACH_TOKEN
#undef EXTRACT_FIELD_NAME
#undef EXTRACT_PRECEDENCE

struct TFormulaToken
{
    EFormulaTokenType Type;
    int Position;
    TString Name;
    i64 Number = 0;
};

bool operator==(const TFormulaToken& lhs, const TFormulaToken& rhs)
{
    return lhs.Type == rhs.Type && lhs.Name == rhs.Name && lhs.Number == rhs.Number;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TGenericFormulaImpl
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TString, Formula);
    DEFINE_BYVAL_RO_PROPERTY(size_t, Hash);

public:
    TGenericFormulaImpl(const TString& formula, size_t hash, std::vector<TFormulaToken> parsedFormula);

    bool operator==(const TGenericFormulaImpl& other) const;

    bool IsEmpty() const;

    int Size() const;

    i64 Eval(const THashMap<TString, i64>& values, EEvaluationContext context) const;

    THashSet<TString> GetVariables() const;

private:
    std::vector<TFormulaToken> ParsedFormula_;

    static std::vector<TFormulaToken> Tokenize(const TString& formula, EEvaluationContext context);
    static std::vector<TFormulaToken> Parse(
        const TString& formula,
        const std::vector<TFormulaToken>& tokens,
        EEvaluationContext context);
    static size_t CalculateHash(const std::vector<TFormulaToken>& tokens);
    static void CheckTypeConsistency(
        const TString& formula,
        const std::vector<TFormulaToken>& tokens,
        EEvaluationContext context);

    friend TIntrusivePtr<TGenericFormulaImpl> MakeGenericFormulaImpl(const TString& formula, EEvaluationContext context);
};

////////////////////////////////////////////////////////////////////////////////

TGenericFormulaImpl::TGenericFormulaImpl(
    const TString& formula,
    size_t hash,
    std::vector<TFormulaToken> parsedFormula)
    : Formula_(formula)
    , Hash_(hash)
    , ParsedFormula_(std::move(parsedFormula))
{ }

bool TGenericFormulaImpl::operator==(const TGenericFormulaImpl& other) const
{
    return std::equal(
        ParsedFormula_.begin(),
        ParsedFormula_.end(),
        other.ParsedFormula_.begin(),
        other.ParsedFormula_.end());
}

bool TGenericFormulaImpl::IsEmpty() const
{
    return ParsedFormula_.empty();
}

int TGenericFormulaImpl::Size() const
{
    return ParsedFormula_.size();
}

i64 TGenericFormulaImpl::Eval(const THashMap<TString, i64>& values, EEvaluationContext context) const
{
    auto variableValue = [&] (const TString& var) -> i64 {
        auto iter = values.find(var);
        if (iter == values.end()) {
            if (context == EEvaluationContext::Boolean) {
                return 0;
            } else {
                THROW_ERROR_EXCEPTION("Undefined variable %Qv", var)
                    << TErrorAttribute("formula", Formula_)
                    << TErrorAttribute("values", values);
            }
        }
        return iter->second;
    };

#define APPLY_BINARY_OP(op) \
    YT_VERIFY(stack.size() >= 2); \
    { \
        YT_VERIFY(std::holds_alternative<i64>(stack.back())); \
        auto top = std::get<i64>(stack.back()); \
        stack.pop_back(); \
        YT_VERIFY(std::holds_alternative<i64>(stack.back())); \
        stack.back() = static_cast<i64>(std::get<i64>(stack.back()) op top); \
    }

    std::vector<std::variant<i64, std::vector<i64>>> stack;
    for (const auto& token : ParsedFormula_) {
        switch (token.Type) {
            case EFormulaTokenType::Variable:
                stack.push_back(variableValue(token.Name));
                break;
            case EFormulaTokenType::Number:
            case EFormulaTokenType::BooleanLiteral:
                stack.push_back(token.Number);
                break;
            case EFormulaTokenType::LogicalNot:
                YT_VERIFY(!stack.empty());
                YT_VERIFY(std::holds_alternative<i64>(stack.back()));
                stack.back() = static_cast<i64>(!std::get<i64>(stack.back()));
                break;
            case EFormulaTokenType::LogicalOr:
                APPLY_BINARY_OP(||);
                break;
            case EFormulaTokenType::LogicalAnd:
                APPLY_BINARY_OP(&&);
                break;
            case EFormulaTokenType::BitwiseOr:
                APPLY_BINARY_OP(|);
                break;
            case EFormulaTokenType::BitwiseXor:
                APPLY_BINARY_OP(^);
                break;
            case EFormulaTokenType::BitwiseAnd:
                APPLY_BINARY_OP(&);
                break;
            case EFormulaTokenType::Equals:
                APPLY_BINARY_OP(==);
                break;
            case EFormulaTokenType::NotEquals:
                APPLY_BINARY_OP(!=);
                break;
            case EFormulaTokenType::Less:
                APPLY_BINARY_OP(<);
                break;
            case EFormulaTokenType::Greater:
                APPLY_BINARY_OP(>);
                break;
            case EFormulaTokenType::LessOrEqual:
                APPLY_BINARY_OP(<=);
                break;
            case EFormulaTokenType::GreaterOrEqual:
                APPLY_BINARY_OP(>=);
                break;
            case EFormulaTokenType::Plus:
                APPLY_BINARY_OP(+);
                break;
            case EFormulaTokenType::Minus:
                APPLY_BINARY_OP(-);
                break;
            case EFormulaTokenType::Multiplies:
                APPLY_BINARY_OP(*);
                break;
            case EFormulaTokenType::Divides:
            case EFormulaTokenType::Modulus:
                YT_VERIFY(stack.size() >= 2);
                {
                    YT_VERIFY(std::holds_alternative<i64>(stack.back()));
                    i64 top = std::get<i64>(stack.back());
                    if (top == 0) {
                        THROW_ERROR_EXCEPTION("Division by zero in formula %Qv", Formula_)
                            << TErrorAttribute("values", values);
                    }
                    stack.pop_back();
                    YT_VERIFY(std::holds_alternative<i64>(stack.back()));
                    if (std::get<i64>(stack.back()) == std::numeric_limits<i64>::min() && top == -1) {
                        THROW_ERROR_EXCEPTION("Division of INT64_MIN by -1 in formula %Qv", Formula_)
                            << TErrorAttribute("values", values);
                    }
                    if (token.Type == EFormulaTokenType::Divides) {
                        stack.back() = std::get<i64>(stack.back()) / top;
                    } else {
                        stack.back() = std::get<i64>(stack.back()) % top;
                    }
                }
                break;
            case EFormulaTokenType::Comma:
                YT_VERIFY(stack.size() >= 2);
                {
                    YT_VERIFY(std::holds_alternative<i64>(stack.back()));
                    i64 top = std::get<i64>(stack.back());
                    stack.pop_back();
                    Visit(stack.back(),
                        [&] (i64 v) {
                            std::vector<i64> vector{v, top};
                            stack.back() = vector;
                        },
                        [&] (std::vector<i64>& v) {
                            v.push_back(top);
                        });
                }
                break;
            case EFormulaTokenType::In:
                YT_VERIFY(stack.size() >= 2);
                {
                    auto set = std::holds_alternative<i64>(stack.back())
                        ? std::vector<i64>{std::get<i64>(stack.back())}
                        : std::move(std::get<std::vector<i64>>(stack.back()));
                    stack.pop_back();
                    YT_VERIFY(std::holds_alternative<i64>(stack.back()));
                    i64 element = std::get<i64>(stack.back());
                    stack.pop_back();
                    stack.push_back(static_cast<i64>(std::find(
                        set.begin(),
                        set.end(),
                        element) != set.end()));
                }
                break;
            default:
                YT_ABORT();
        }
    }
    if (stack.empty()) {
        if (context == EEvaluationContext::Arithmetic) {
            THROW_ERROR_EXCEPTION("Empty arithmetic formula cannot be evaluated");
        }
        return true;
    }
    YT_VERIFY(stack.size() == 1);
    YT_VERIFY(std::holds_alternative<i64>(stack.back()));
    return std::get<i64>(stack[0]);

#undef APPLY_BINARY_OP
}

THashSet<TString> TGenericFormulaImpl::GetVariables() const
{
    THashSet<TString> variables;
    for (const auto& token : ParsedFormula_) {
        if (token.Type == EFormulaTokenType::Variable) {
            variables.insert(token.Name);
        }
    }
    return variables;
}

std::vector<TFormulaToken> TGenericFormulaImpl::Tokenize(const TString& formula, EEvaluationContext context)
{
    std::vector<TFormulaToken> result;
    size_t pos = 0;

    auto throwError = [&] (int position, const TString& message) {
        ThrowError(formula, position, message, context);
    };

    auto skipWhitespace = [&] {
        while (pos < formula.size() && std::isspace(formula[pos])) {
            ++pos;
        }
    };

    auto extractSpecialToken = [&] () -> std::optional<EFormulaTokenType> {
        char first = formula[pos];
        char second = pos + 1 < formula.size() ? formula[pos + 1] : '\0';
        if (first == 'i' && second == 'n') {
            char third = pos + 2 < formula.size() ? formula[pos + 2] : '\0';
            if (IsSymbolAllowedInName(third, context, /*isFirst*/ false)) {
                return std::nullopt;
            } else {
                pos += 2;
                return EFormulaTokenType::In;
            }
        }
        switch (first) {
            case '^':
                ++pos;
                return EFormulaTokenType::BitwiseXor;
            case '+':
                ++pos;
                return EFormulaTokenType::Plus;
            case '-':
                ++pos;
                return EFormulaTokenType::Minus;
            case '*':
                ++pos;
                return EFormulaTokenType::Multiplies;
            case '/':
                ++pos;
                return EFormulaTokenType::Divides;
            case '%':
                ++pos;
                return EFormulaTokenType::Modulus;
            case '(':
                ++pos;
                return EFormulaTokenType::LeftBracket;
            case ')':
                ++pos;
                return EFormulaTokenType::RightBracket;
            case ',':
                ++pos;
                return EFormulaTokenType::Comma;
            case '=':
                if (second != '=') {
                    throwError(pos + 1, "Unexpected character");
                }
                pos += 2;
                return EFormulaTokenType::Equals;
            case '!':
                switch (second) {
                    case '=':
                        pos += 2;
                        return EFormulaTokenType::NotEquals;
                    default:
                        ++pos;
                        return EFormulaTokenType::LogicalNot;
                }
            case '&':
                switch (second) {
                    case '&':
                        pos += 2;
                        return EFormulaTokenType::LogicalAnd;
                    default:
                        ++pos;
                        return EFormulaTokenType::BitwiseAnd;
                }
            case '|':
                switch (second) {
                    case '|':
                        pos += 2;
                        return EFormulaTokenType::LogicalOr;
                    default:
                        ++pos;
                        return EFormulaTokenType::BitwiseOr;
                }
            case '<':
                switch (second) {
                    case '=':
                        pos += 2;
                        return EFormulaTokenType::LessOrEqual;
                    default:
                        ++pos;
                        return EFormulaTokenType::Less;
                }
            case '>':
                switch (second) {
                    case '=':
                        pos += 2;
                        return EFormulaTokenType::GreaterOrEqual;
                    default:
                        ++pos;
                        return EFormulaTokenType::Greater;
                }
            default:
                return std::nullopt;
        }
    };

    auto extractNumber = [&] {
        size_t start = pos;
        if (formula[pos] == '-') {
            ++pos;
        }
        if (pos == formula.size() || !std::isdigit(formula[pos])) {
            throwError(pos, "Expected digit");
        }
        while (pos < formula.size() && std::isdigit(formula[pos])) {
            ++pos;
        }
        return IntFromString<i64, 10>(TStringBuf(formula, start, pos - start));
    };

    auto extractVariable = [&] {
        TString name;
        while (pos < formula.size() && IsSymbolAllowedInName(formula[pos], context, /*isFirst*/ name.empty())) {
            name += formula[pos++];
        }
        ValidateFormulaVariable(name, context);
        return name;
    };

    auto extractBooleanLiteral = [&] {
        YT_VERIFY(formula[pos] == '%');
        ++pos;

        size_t start = pos;
        while (pos < formula.size() && std::isalpha(formula[pos])) {
            ++pos;
        }

        TStringBuf buf(formula, start, pos - start);
        if (buf == "false") {
            return 0;
        } else if (buf == "true") {
            return 1;
        } else {
            throwError(pos, Format("Invalid literal %Qv", buf));
            YT_ABORT();
        }
    };

    bool expectBinaryOperator = false;

    while (pos < formula.size()) {
        char c = formula[pos];
        if (std::isspace(c)) {
            skipWhitespace();
            if (pos == formula.size()) {
                break;
            }
            c = formula[pos];
        }

        TFormulaToken token;
        token.Position = pos;

        if (context == EEvaluationContext::Arithmetic && (std::isdigit(c) || (c == '-' && !expectBinaryOperator))) {
            token.Type = EFormulaTokenType::Number;
            token.Number = extractNumber();
            expectBinaryOperator = true;
        } else if (!expectBinaryOperator && c == '%') {
            token.Type = EFormulaTokenType::BooleanLiteral;
            token.Number = extractBooleanLiteral();
            expectBinaryOperator = true;
        } else if (auto optionalType = extractSpecialToken()) {
            token.Type = *optionalType;
            expectBinaryOperator = token.Type == EFormulaTokenType::RightBracket;
        } else if (IsSymbolAllowedInName(formula[pos], context, true)) {
            token.Type = EFormulaTokenType::Variable;
            token.Name = extractVariable();
            expectBinaryOperator = true;
        } else {
            throwError(pos, "Unexpected character");
        }

        result.push_back(token);
    }

    return result;
}

std::vector<TFormulaToken> TGenericFormulaImpl::Parse(
    const TString& formula,
    const std::vector<TFormulaToken>& tokens,
    EEvaluationContext context)
{
    std::vector<TFormulaToken> result;
    std::vector<TFormulaToken> stack;
    bool expectSubformula = true;

    if (tokens.empty()) {
        return result;
    }

    auto throwError = [&] (int position, const TString& message) {
        ThrowError(formula, position, message, context);
    };

    auto finishSubformula = [&] {
        while (!stack.empty() && stack.back().Type != EFormulaTokenType::LeftBracket) {
            result.push_back(stack.back());
            stack.pop_back();
        }
    };

    auto processBinaryOp = [&] (const TFormulaToken& token) {
        while (!stack.empty() && Precedence(stack.back().Type) >= Precedence(token.Type)) {
            result.push_back(stack.back());
            stack.pop_back();
        }
    };

    for (const auto& token : tokens) {
        switch (token.Type) {
            case EFormulaTokenType::Variable:
            case EFormulaTokenType::Number:
            case EFormulaTokenType::BooleanLiteral:
                if (!expectSubformula) {
                    throwError(token.Position, "Unexpected variable");
                }
                result.push_back(token);
                expectSubformula = false;
                break;

            case EFormulaTokenType::LogicalNot:
            case EFormulaTokenType::LeftBracket:
                if (!expectSubformula) {
                    throwError(token.Position, "Unexpected token");
                }
                stack.push_back(token);
                break;

            case EFormulaTokenType::RightBracket:
                if (expectSubformula) {
                    throwError(token.Position, "Unexpected token");
                }
                finishSubformula();
                if (stack.empty()) {
                    throwError(token.Position, "Unmatched ')'");
                }
                stack.pop_back();
                break;

            default:
                if (expectSubformula) {
                    throwError(token.Position, "Unexpected token");
                }
                processBinaryOp(token);
                stack.push_back(token);
                expectSubformula = true;
                break;
        }
    }

    if (expectSubformula) {
        throwError(formula.size(), "Unfinished formula");
    }
    finishSubformula();
    if (!stack.empty()) {
        throwError(stack.back().Position, "Unmatched '('");
    }

    if (context == EEvaluationContext::Boolean) {
        for (const auto& token : result) {
            switch (token.Type) {
                case EFormulaTokenType::BitwiseAnd:
                case EFormulaTokenType::BitwiseOr:
                case EFormulaTokenType::LogicalNot:
                case EFormulaTokenType::Variable:
                case EFormulaTokenType::BooleanLiteral:
                    break;
                default:
                    throwError(
                        token.Position,
                        "Invalid token in boolean formula (only '!', '&', '|', '(', ')', '%false', '%true' are allowed)");
                    YT_ABORT();
            }
        }
    }

    CheckTypeConsistency(formula, result, context);

    return result;
}

size_t TGenericFormulaImpl::CalculateHash(const std::vector<TFormulaToken>& tokens)
{
    size_t result = 0x18a92ea497f9bb1e;

    for (const auto& token : tokens) {
        HashCombine(result, static_cast<size_t>(token.Type));
        HashCombine(result, ComputeHash(token.Name));
        HashCombine(result, static_cast<size_t>(token.Number));
    }

    return result;
}

void TGenericFormulaImpl::CheckTypeConsistency(
    const TString& formula,
    const std::vector<TFormulaToken>& tokens,
    EEvaluationContext context)
{
    auto validateIsNumber = [&] (EFormulaTokenType type, int position) {
        if (type != EFormulaTokenType::Number) {
            ThrowError(formula, position, "Type mismatch: expected \"number\", got \"set\"", context);
        }
    };

    std::vector<EFormulaTokenType> stack;
    for (const auto& token : tokens) {
        switch (token.Type) {
            case EFormulaTokenType::Variable:
            case EFormulaTokenType::Number:
            case EFormulaTokenType::BooleanLiteral:
                stack.push_back(EFormulaTokenType::Number);
                break;
            case EFormulaTokenType::LogicalNot:
                YT_VERIFY(!stack.empty());
                validateIsNumber(stack.back(), token.Position);
                break;
            case EFormulaTokenType::LogicalOr:
            case EFormulaTokenType::LogicalAnd:
            case EFormulaTokenType::BitwiseOr:
            case EFormulaTokenType::BitwiseXor:
            case EFormulaTokenType::BitwiseAnd:
            case EFormulaTokenType::Equals:
            case EFormulaTokenType::NotEquals:
            case EFormulaTokenType::Less:
            case EFormulaTokenType::Greater:
            case EFormulaTokenType::LessOrEqual:
            case EFormulaTokenType::GreaterOrEqual:
            case EFormulaTokenType::Plus:
            case EFormulaTokenType::Minus:
            case EFormulaTokenType::Multiplies:
            case EFormulaTokenType::Divides:
            case EFormulaTokenType::Modulus:
                YT_VERIFY(stack.size() >= 2);
                validateIsNumber(stack.back(), token.Position);
                stack.pop_back();
                validateIsNumber(stack.back(), token.Position);
                break;
            case EFormulaTokenType::Comma:
                YT_VERIFY(stack.size() >= 2);
                validateIsNumber(stack.back(), token.Position);
                stack.pop_back();
                stack.back() = EFormulaTokenType::Set;
                break;
            case EFormulaTokenType::In:
                YT_VERIFY(stack.size() >= 2);
                stack.pop_back();
                validateIsNumber(stack.back(), token.Position);
                break;
            default:
                YT_ABORT();
        }
    }
    if (!stack.empty()) {
        validateIsNumber(stack.back(), 0);
    }
}

TIntrusivePtr<TGenericFormulaImpl> MakeGenericFormulaImpl(const TString& formula, EEvaluationContext context)
{
    auto tokens = TGenericFormulaImpl::Tokenize(formula, context);
    auto parsed = TGenericFormulaImpl::Parse(formula, tokens, context);
    auto hash = TGenericFormulaImpl::CalculateHash(parsed);
    return New<TGenericFormulaImpl>(formula, hash, parsed);
}

////////////////////////////////////////////////////////////////////////////////

TArithmeticFormula::TArithmeticFormula()
    : Impl_(MakeGenericFormulaImpl(TString(), EEvaluationContext::Arithmetic))
{ }

TArithmeticFormula::TArithmeticFormula(TIntrusivePtr<TGenericFormulaImpl> impl)
    : Impl_(std::move(impl))
{ }

TArithmeticFormula::TArithmeticFormula(const TArithmeticFormula& other) = default;
TArithmeticFormula::TArithmeticFormula(TArithmeticFormula&& other) = default;
TArithmeticFormula& TArithmeticFormula::operator=(const TArithmeticFormula& other) = default;
TArithmeticFormula& TArithmeticFormula::operator=(TArithmeticFormula&& other) = default;
TArithmeticFormula::~TArithmeticFormula() = default;

bool TArithmeticFormula::operator==(const TArithmeticFormula& other) const
{
    return *Impl_ == *other.Impl_;
}

bool TArithmeticFormula::IsEmpty() const
{
    return Impl_->IsEmpty();
}

int TArithmeticFormula::Size() const
{
    return Impl_->Size();
}

size_t TArithmeticFormula::GetHash() const
{
    return Impl_->GetHash();
}

TString TArithmeticFormula::GetFormula() const
{
    return Impl_->GetFormula();
}

i64 TArithmeticFormula::Eval(const THashMap<TString, i64>& values) const
{
    return Impl_->Eval(values, EEvaluationContext::Arithmetic);
}

THashSet<TString> TArithmeticFormula::GetVariables() const
{
    return Impl_->GetVariables();
}

TArithmeticFormula MakeArithmeticFormula(const TString& formula)
{
    auto impl = MakeGenericFormulaImpl(formula, EEvaluationContext::Arithmetic);
    return TArithmeticFormula(std::move(impl));
}

void Serialize(const TArithmeticFormula& arithmeticFormula, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .Value(arithmeticFormula.GetFormula());
}

void Deserialize(TArithmeticFormula& arithmeticFormula, NYTree::INodePtr node)
{
    arithmeticFormula = MakeArithmeticFormula(node->AsString()->GetValue());
}

void TArithmeticFormula::Save(TStreamSaveContext& context) const
{
    using NYT::Save;
    Save(context, GetFormula());
}

void TArithmeticFormula::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    auto formula = Load<TString>(context);
    Impl_ = MakeGenericFormulaImpl(formula, EEvaluationContext::Arithmetic);
}

////////////////////////////////////////////////////////////////////////////////

TBooleanFormulaTags::TBooleanFormulaTags(THashSet<TString> tags)
    : Tags_(std::move(tags))
{
    for (const auto& key: Tags_) {
        PreparedTags_[key] = 1;
    }
}

const THashSet<TString>& TBooleanFormulaTags::GetSourceTags() const
{
    return Tags_;
}

void TBooleanFormulaTags::Save(TStreamSaveContext& context) const
{
    using NYT::Save;
    Save(context, Tags_);
}

void TBooleanFormulaTags::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    *this = TBooleanFormulaTags(Load<THashSet<TString>>(context));
}

bool TBooleanFormulaTags::operator==(const TBooleanFormulaTags& other) const
{
    return Tags_ == other.Tags_;
}

void Serialize(const TBooleanFormulaTags& tags, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .Value(tags.GetSourceTags());
}

void Deserialize(TBooleanFormulaTags& tags, NYTree::INodePtr node)
{
    tags = TBooleanFormulaTags(ConvertTo<THashSet<TString>>(node));
}

void FormatValue(TStringBuilderBase* builder, const TBooleanFormulaTags& tags, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", tags.GetSourceTags());
}

////////////////////////////////////////////////////////////////////////////////

TBooleanFormula::TBooleanFormula()
    : Impl_(MakeGenericFormulaImpl(TString(), EEvaluationContext::Boolean))
{ }

TBooleanFormula::TBooleanFormula(TIntrusivePtr<TGenericFormulaImpl> impl)
    : Impl_(std::move(impl))
{ }

TBooleanFormula::TBooleanFormula(const TBooleanFormula& other) = default;
TBooleanFormula::TBooleanFormula(TBooleanFormula&& other) = default;
TBooleanFormula& TBooleanFormula::operator=(const TBooleanFormula& other) = default;
TBooleanFormula& TBooleanFormula::operator=(TBooleanFormula&& other) = default;
TBooleanFormula::~TBooleanFormula() = default;

bool TBooleanFormula::operator==(const TBooleanFormula& other) const
{
    return *Impl_ == *other.Impl_;
}

bool TBooleanFormula::IsEmpty() const
{
    return Impl_->IsEmpty();
}

int TBooleanFormula::Size() const
{
    return Impl_->Size();
}

size_t TBooleanFormula::GetHash() const
{
    return Impl_->GetHash();
}

TString TBooleanFormula::GetFormula() const
{
    return Impl_->GetFormula();
}

bool TBooleanFormula::IsSatisfiedBy(const std::vector<TString>& value) const
{
    THashMap<TString, i64> values;
    for (const auto& key: value) {
        values[key] = 1;
    }
    return Impl_->Eval(values, EEvaluationContext::Boolean);
}

bool TBooleanFormula::IsSatisfiedBy(const THashSet<TString>& value) const
{
    return IsSatisfiedBy(std::vector<TString>(value.begin(), value.end()));
}

bool TBooleanFormula::IsSatisfiedBy(const TBooleanFormulaTags& tags) const
{
    return Impl_->Eval(tags.PreparedTags_, EEvaluationContext::Boolean);
}

TBooleanFormula MakeBooleanFormula(const TString& formula)
{
    auto impl = MakeGenericFormulaImpl(formula, EEvaluationContext::Boolean);
    return TBooleanFormula(std::move(impl));
}

TBooleanFormula operator&(const TBooleanFormula& lhs, const TBooleanFormula& rhs)
{
    if (lhs.IsEmpty()) {
        return rhs;
    }
    if (rhs.IsEmpty()) {
        return lhs;
    }
    return MakeBooleanFormula(Format("(%v) & (%v)", lhs.GetFormula(), rhs.GetFormula()));
}

TBooleanFormula operator|(const TBooleanFormula& lhs, const TBooleanFormula& rhs)
{
    if (lhs.IsEmpty() || rhs.IsEmpty()) {
        return MakeBooleanFormula("%true");
    }
    return MakeBooleanFormula(Format("(%v) | (%v)", lhs.GetFormula(), rhs.GetFormula()));
}

TBooleanFormula operator!(const TBooleanFormula& formula)
{
    if (formula.IsEmpty()) {
        return MakeBooleanFormula("%false");
    }
    return MakeBooleanFormula(Format("!(%v)", formula.GetFormula()));
}

void Serialize(const TBooleanFormula& booleanFormula, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .Value(booleanFormula.GetFormula());
}

void Deserialize(TBooleanFormula& booleanFormula, NYTree::INodePtr node)
{
    booleanFormula = MakeBooleanFormula(node->AsString()->GetValue());
}

void Deserialize(TBooleanFormula& booleanFormula, TYsonPullParserCursor* cursor)
{
    MaybeSkipAttributes(cursor);
    EnsureYsonToken("TBooleanFormula", *cursor, EYsonItemType::StringValue);
    booleanFormula = MakeBooleanFormula(ExtractTo<TString>(cursor));
}

void TBooleanFormula::Save(TStreamSaveContext& context) const
{
    using NYT::Save;
    Save(context, GetFormula());
}

void TBooleanFormula::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    auto formula = Load<TString>(context);
    Impl_ = MakeGenericFormulaImpl(formula, EEvaluationContext::Boolean);
}

void FormatValue(TStringBuilderBase* builder, const TBooleanFormula& booleanFormula, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", booleanFormula.GetFormula());
}

////////////////////////////////////////////////////////////////////////////////

TTimeFormula::TTimeFormula() = default;
TTimeFormula::TTimeFormula(const TTimeFormula& other) = default;
TTimeFormula::TTimeFormula(TTimeFormula&& other) = default;
TTimeFormula& TTimeFormula::operator=(const TTimeFormula& other) = default;
TTimeFormula& TTimeFormula::operator=(TTimeFormula&& other) = default;
TTimeFormula::~TTimeFormula() = default;

bool TTimeFormula::operator==(const TTimeFormula& other) const
{
    return Formula_ == other.Formula_;
}

bool TTimeFormula::IsEmpty() const
{
    return Formula_.IsEmpty();
}

int TTimeFormula::Size() const
{
    return Formula_.Size();
}

size_t TTimeFormula::GetHash() const
{
    return Formula_.GetHash();
}

TString TTimeFormula::GetFormula() const
{
    return Formula_.GetFormula();
}

bool TTimeFormula::IsSatisfiedBy(TInstant time) const
{
    struct tm tm;
    time.LocalTime(&tm);
    return Formula_.Eval({
            {"hours", tm.tm_hour},
            {"minutes", tm.tm_min}}) != 0;
}

TTimeFormula::TTimeFormula(TArithmeticFormula&& arithmeticFormula)
    : Formula_(std::move(arithmeticFormula))
{ }

TTimeFormula MakeTimeFormula(const TString& formula)
{
    const static THashSet<TString> allowedVariables{"minutes", "hours"};

    auto arithmeticFormula = MakeArithmeticFormula(formula);

    for (const auto& variable : arithmeticFormula.GetVariables()) {
        if (!allowedVariables.contains(variable)) {
            THROW_ERROR_EXCEPTION("Invalid variable %Qv in time formula %Qv",
                variable,
                formula);
        }
    }
    return TTimeFormula{std::move(arithmeticFormula)};
}

void Serialize(const TTimeFormula& timeFormula, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .Value(timeFormula.GetFormula());
}

void Deserialize(TTimeFormula& timeFormula, INodePtr node)
{
    timeFormula = MakeTimeFormula(node->AsString()->GetValue());
}

void Deserialize(TTimeFormula& timeFormula, TYsonPullParserCursor* cursor)
{
    MaybeSkipAttributes(cursor);
    EnsureYsonToken("TTimeFormula", *cursor, EYsonItemType::StringValue);
    timeFormula = MakeTimeFormula(ExtractTo<TString>(cursor));
}

void TTimeFormula::Save(TStreamSaveContext& context) const
{
    using NYT::Save;
    Save(context, Formula_);
}

void TTimeFormula::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    Formula_ = Load<TArithmeticFormula>(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
