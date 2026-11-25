#include "ast.h"
#include "private.h"

#include <library/cpp/yt/misc/variant.h>
#include <library/cpp/yt/misc/optional.h>

#include <util/string/cast.h>
#include <util/string/escape.h>

#include <array>
#include <charconv>
#include <cmath>
#include <iterator>
#include <limits>
#include <stack>
#include <system_error>

namespace NYT::NQueryClient::NAst {

////////////////////////////////////////////////////////////////////////////////

bool operator==(TNullLiteralValue, TNullLiteralValue)
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TTupleItemIndexAccessor TDoubleOrDotIntToken::AsDotInt() const
{
    if (Representation.empty() || Representation[0] != '.') {
        THROW_ERROR_EXCEPTION("Expected dot token and then integer token, but got %Qv",
            Representation);
    }

    return FromString<TTupleItemIndexAccessor>(Representation.substr(1));
}

double TDoubleOrDotIntToken::AsDouble() const
{
    return FromString<double>(Representation);
}

////////////////////////////////////////////////////////////////////////////////

size_t TColumnReferenceHasher::operator()(const TColumnReference& reference) const
{
    size_t result = 0;
    HashCombine(result, reference.ColumnName);
    HashCombine(result, reference.TableName);
    return result;
}

bool TColumnReferenceEqComparer::operator()(const TColumnReference& lhs, const TColumnReference& rhs) const
{
    return
        std::tie(lhs.ColumnName, lhs.TableName) ==
        std::tie(rhs.ColumnName, rhs.TableName);
}

////////////////////////////////////////////////////////////////////////////////

bool TCompositeTypeMemberAccessor::IsEmpty() const
{
    return NestedStructOrTupleItemAccessor.empty() && !DictOrListItemAccessor.has_value();
}

size_t TReferenceHasher::operator()(const TReference& reference) const
{
    size_t result = 0;
    HashCombine(result, reference.ColumnName);
    HashCombine(result, reference.TableName);

    for (const auto& item : reference.CompositeTypeAccessor.NestedStructOrTupleItemAccessor) {
        Visit(item,
            [&] (const TStructMemberAccessor& structMember) {
                HashCombine(result, structMember);
            },
            [&] (const TTupleItemIndexAccessor& index) {
                HashCombine(result, index);
            });
    }

    if (reference.CompositeTypeAccessor.DictOrListItemAccessor) {
        if (std::ssize(*reference.CompositeTypeAccessor.DictOrListItemAccessor) != 1) {
            THROW_ERROR_EXCEPTION("Expression inside of the list or dict item accessor should be scalar")
                .With("source", FormatReference(reference));
        }
        HashCombine(result, reference.CompositeTypeAccessor.DictOrListItemAccessor->front());
    }

    return result;
}

bool TReferenceEqComparer::operator()(const TReference& lhs, const TReference& rhs) const
{
    return lhs == rhs;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool ExpressionListEqual(const T& lhs, const T& rhs)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t index = 0; index < lhs.size(); ++index) {
        if (*lhs[index] != *rhs[index]) {
            return false;
        }
    }
    return true;
}

bool operator==(const TExpressionList& lhs, const TExpressionList& rhs)
{
    return ExpressionListEqual(lhs, rhs);
}

bool operator==(const TIdentifierList& lhs, const TIdentifierList& rhs)
{
    return ExpressionListEqual(lhs, rhs);
}

bool operator==(const TExpression& lhs, const TExpression& rhs)
{
    if (const auto* typedLhs = lhs.As<TLiteralExpression>()) {
        const auto* typedRhs = rhs.As<TLiteralExpression>();
        if (!typedRhs) {
            return false;
        }
        return typedLhs->Value == typedRhs->Value;
    } else if (const auto* typedLhs = lhs.As<TReferenceExpression>()) {
        const auto* typedRhs = rhs.As<TReferenceExpression>();
        if (!typedRhs) {
            return false;
        }
        return typedLhs->Reference == typedRhs->Reference;
    } else if (const auto* typedLhs = lhs.As<TAliasExpression>()) {
        const auto* typedRhs = rhs.As<TAliasExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->Name == typedRhs->Name &&
            *typedLhs->Expression == *typedRhs->Expression;
    } else if (const auto* typedLhs = lhs.As<TFunctionExpression>()) {
        const auto* typedRhs = rhs.As<TFunctionExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->FunctionName == typedRhs->FunctionName &&
            typedLhs->Arguments == typedRhs->Arguments;
    } else if (const auto* typedLhs = lhs.As<TUnaryOpExpression>()) {
        const auto* typedRhs = rhs.As<TUnaryOpExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->Opcode == typedRhs->Opcode &&
            typedLhs->Operand == typedRhs->Operand;
    } else if (const auto* typedLhs = lhs.As<TBinaryOpExpression>()) {
        const auto* typedRhs = rhs.As<TBinaryOpExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->Opcode == typedRhs->Opcode &&
            typedLhs->Lhs == typedRhs->Lhs &&
            typedLhs->Rhs == typedRhs->Rhs;
    } else if (const auto* typedLhs = lhs.As<TInExpression>()) {
        const auto* typedRhs = rhs.As<TInExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->Expr == typedRhs->Expr &&
            typedLhs->Values == typedRhs->Values;
    } else if (const auto* typedLhs = lhs.As<TBetweenExpression>()) {
        const auto* typedRhs = rhs.As<TBetweenExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->Expr == typedRhs->Expr &&
            typedLhs->Values == typedRhs->Values;
    } else if (const auto* typedLhs = lhs.As<TTransformExpression>()) {
        const auto* typedRhs = rhs.As<TTransformExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->Expr == typedRhs->Expr &&
            typedLhs->From == typedRhs->From &&
            typedLhs->To == typedRhs->To &&
            typedLhs->DefaultExpr == typedRhs->DefaultExpr;
    } else if (const auto* typedLhs = lhs.As<TCaseExpression>()) {
        const auto* typedRhs = rhs.As<TCaseExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->OptionalOperand == typedRhs->OptionalOperand &&
            typedLhs->WhenThenExpressions == typedRhs->WhenThenExpressions &&
            typedLhs->DefaultExpression == typedRhs->DefaultExpression;
    } else if (const auto* typedLhs = lhs.As<TLikeExpression>()) {
        const auto* typedRhs = rhs.As<TLikeExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->Text == typedRhs->Text &&
            typedLhs->Opcode == typedRhs->Opcode &&
            typedLhs->Pattern == typedRhs->Pattern &&
            typedLhs->EscapeCharacter == typedRhs->EscapeCharacter;
    } else if (const auto* typedLhs = lhs.As<TQueryExpression>()) {
        const auto* typedRhs = rhs.As<TQueryExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->Query == typedRhs->Query;
    } else {
        YT_ABORT();
    }
}

TStringBuf TExpression::GetSource(TStringBuf source) const
{
    auto begin = SourceLocation.first;
    auto end = SourceLocation.second;

    return source.substr(begin, end - begin);
}

////////////////////////////////////////////////////////////////////////////////

TStringBuf GetSource(TSourceLocation sourceLocation, TStringBuf source)
{
    auto begin = sourceLocation.first;
    auto end = sourceLocation.second;

    return source.substr(begin, end - begin);
}

bool operator==(const TTableDescriptor& lhs, const TTableDescriptor& rhs)
{
    return
        std::tie(lhs.Path, lhs.Alias, lhs.Hint) ==
        std::tie(rhs.Path, rhs.Alias, rhs.Hint);
}

bool operator==(const TJoin& lhs, const TJoin& rhs)
{
    return
        std::tie(lhs.IsLeft, lhs.Table, lhs.Fields, lhs.Lhs, lhs.Rhs, lhs.Predicate) ==
        std::tie(rhs.IsLeft, rhs.Table, rhs.Fields, rhs.Lhs, rhs.Rhs, rhs.Predicate);
}

bool TQuery::operator==(const TQuery& other) const
{
    if (FromClause.index() != other.FromClause.index()) {
        return false;
    }

    auto areAstHeadEqual = [] (const TQueryAstHeadPtr& lhs, const TQueryAstHeadPtr& rhs) {
        if (!lhs || !rhs) {
            return lhs == rhs;
        }
        return
            lhs->Ast == rhs->Ast &&
            lhs->Alias == rhs->Alias;
    };

    auto fromClauseEqual = Visit(FromClause,
        [&] (const TTableDescriptor& table) {
            return table == std::get<TTableDescriptor>(other.FromClause);
        },
        [&] (TQueryAstHeadPtr subquery) {
            return areAstHeadEqual(subquery, std::get<TQueryAstHeadPtr>(other.FromClause));
        },
        [&] (const TExpressionList& expressions) {
            return expressions == std::get<TExpressionList>(other.FromClause);
        },
        [&] (const TUnionAllClause& unionAllClause) {
            const auto& otherUnionAllClause = std::get<TUnionAllClause>(other.FromClause);

            if (unionAllClause.Alias != otherUnionAllClause.Alias) {
                return false;
            }

            if (unionAllClause.Subqueries.size() != otherUnionAllClause.Subqueries.size()) {
                return false;
            }

            for (int index = 0; index < std::ssize(unionAllClause.Subqueries); ++index) {
                if (!areAstHeadEqual(
                    unionAllClause.Subqueries[index],
                    otherUnionAllClause.Subqueries[index]))
                {
                    return false;
                }
            }

            return true;
        });

    return
        fromClauseEqual &&
        WithIndex == other.WithIndex &&
        Joins == other.Joins &&
        SelectExprs == other.SelectExprs &&
        WherePredicate == other.WherePredicate &&
        GroupExprs == other.GroupExprs &&
        TotalsMode == other.TotalsMode &&
        HavingPredicate == other.HavingPredicate &&
        OrderExpressions == other.OrderExpressions &&
        Offset == other.Offset &&
        Limit == other.Limit;
}

////////////////////////////////////////////////////////////////////////////////

struct TFormatOptions
{
    bool ExpandAliases = true;
    bool IsFinal = false;
    bool Concise = false;
};

void FormatExpressions(TStringBuilderBase* builder, const TExpressionList& exprs, int depth, const TFormatOptions& options);
void FormatExpression(TStringBuilderBase* builder, const TExpression& expr, int depth, const TFormatOptions& options);
void FormatExpression(TStringBuilderBase* builder, const TExpressionList& expr, int depth, const TFormatOptions& options);
void FormatQuery(TStringBuilderBase* builder, const TQuery& query, const TFormatOptions& options);

////////////////////////////////////////////////////////////////////////////////

void FormatElision(TStringBuilderBase* builder, int size)
{
    builder->AppendFormat(" ..[%v]", size);
}

namespace {

void FormatDoubleLiteral(TStringBuilderBase* builder, double value)
{
    THROW_ERROR_EXCEPTION_IF(std::isnan(value), "NaN is not supported in query literals");

    // Allow a sign, a decimal point, and a signed three-digit exponent.
    std::array<char, std::numeric_limits<double>::max_digits10 + 7> buffer;
    auto [end, error] = std::to_chars(buffer.data(), buffer.data() + std::ssize(buffer), value);
    THROW_ERROR_EXCEPTION_IF(error != std::errc{}, "Failed to format double literal");

    auto str = TStringBuf(buffer.data(), end);
    builder->AppendString(str);
    bool hasNoDecimalPointOrExponent = str.find_first_of(".e") == TStringBuf::npos;
    if (std::isfinite(value) && hasNoDecimalPointOrExponent) {
        builder->AppendChar('.');
    }
}

} // namespace

void FormatLiteralValue(TStringBuilderBase* builder, const TLiteralValue& value, const TFormatOptions& options)
{
    Visit(value,
        [&] (TNullLiteralValue) {
            builder->AppendString("null");
        },
        [&] (i64 value) {
            builder->AppendFormat("%v", value);
        },
        [&] (ui64 value) {
            builder->AppendFormat("%vu", value);
        },
        [&] (double value) {
            FormatDoubleLiteral(builder, value);
        },
        [&] (bool value) {
            builder->AppendFormat("%v", value ? "true" : "false");
        },
        [&] (const std::string& value) {
            if (options.Concise && value.size() > MaxConciseStringLength) {
                builder->AppendChar('"');
                builder->AppendString(EscapeC(TString(value.substr(0, ElidedStringLength))));
                FormatElision(builder, value.size());
                builder->AppendChar('"');
            } else {
                builder->AppendChar('"');
                builder->AppendString(EscapeC(TString(value)));
                builder->AppendChar('"');
            }
        });
}

std::vector<TStringBuf> GetKeywords()
{
    std::vector<TStringBuf> result;

#define XX(keyword) result.push_back(#keyword);

    XX(from)
    XX(index)
    XX(where)
    XX(having)
    XX(offset)
    XX(limit)
    XX(array)
    XX(join)
    XX(using)
    XX(group)
    XX(by)
    XX(with)
    XX(totals)
    XX(order)
    XX(by)
    XX(asc)
    XX(desc)
    XX(left)
    XX(as)
    XX(on)
    XX(unnest)
    XX(and)
    XX(or)
    XX(not)
    XX(null)
    XX(between)
    XX(in)
    XX(transform)
    XX(case)
    XX(when)
    XX(then)
    XX(else)
    XX(end)
    XX(like)
    XX(ilike)
    XX(rlike)
    XX(regexp)
    XX(escape)
    XX(false)
    XX(true)
    XX(inf)
    XX(union)
    XX(all)

#undef XX

    std::sort(result.begin(), result.end());

    return result;
}

bool IsKeyword(TStringBuf str)
{
    static auto keywords = GetKeywords();

    return std::binary_search(keywords.begin(), keywords.end(), str, [] (TStringBuf str, TStringBuf keyword) {
        return std::lexicographical_compare(
            str.begin(),
            str.end(),
            keyword.begin(),
            keyword.end(), [] (char a, char b) {
                return tolower(a) < tolower(b);
            });
    });
}

bool IsValidId(TStringBuf str)
{
    if (str.empty()) {
        return false;
    }

    auto isNum = [] (char ch) {
        return
            ch >= '0' && ch <= '9';
    };

    auto isAlpha = [] (char ch) {
        return
            ch >= 'a' && ch <= 'z' ||
            ch >= 'A' && ch <= 'Z' ||
            ch == '_';
    };

    if (!isAlpha(str[0])) {
        return false;
    }

    for (size_t index = 1; index < str.length(); ++index) {
        char ch = str[index];
        if (!isAlpha(ch) && !isNum(ch)) {
            return false;
        }
    }

    if (IsKeyword(str)) {
        return false;
    }

    return true;
}

std::string EscapeBackticks(TStringBuf data)
{
    TStringBuilder builder;
    builder.Reserve(data.size());

    for (char ch : data) {
        if (ch == '`') {
            builder.AppendChar('\\');
            builder.AppendChar('`');
        } else {
            builder.AppendChar(ch);
        }
    }

    return builder.Flush();
}

void FormatId(TStringBuilderBase* builder, TStringBuf id, const TFormatOptions& options)
{
    if (options.IsFinal || IsValidId(id)) {
        builder->AppendString(id);
    } else {
        builder->AppendChar('`');
        builder->AppendString(EscapeBackticks(EscapeC(id)));
        builder->AppendChar('`');
    }
}

void FormatColumnReference(TStringBuilderBase* builder, const TColumnReference& ref, const TFormatOptions& options)
{
    // TODO(lukyan): Do not use final = true if query has any table aliases.

    if (ref.TableName) {
        FormatId(builder, *ref.TableName, options);
        builder->AppendChar('.');
    }

    FormatId(builder, ref.ColumnName, options);
}

void FormatReference(TStringBuilderBase* builder, const TReference& ref, int depth, const TFormatOptions& options)
{
    FormatColumnReference(builder, ref, options);

    for (const auto& item : ref.CompositeTypeAccessor.NestedStructOrTupleItemAccessor) {
        Visit(item,
            [&] (const TStructMemberAccessor& structMember) {
                builder->AppendChar('.');
                builder->AppendString(structMember);
            },
            [&] (const TTupleItemIndexAccessor& index) {
                builder->AppendChar('.');
                builder->AppendFormat("%v", index);
            });
    }

    if (ref.CompositeTypeAccessor.DictOrListItemAccessor) {
        // Preserving existing behavior, see YT-28316.
        auto compatOptions = options;
        compatOptions.IsFinal = false;
        builder->AppendChar('[');
        FormatExpressions(builder, *ref.CompositeTypeAccessor.DictOrListItemAccessor, depth + 1, compatOptions);
        builder->AppendChar(']');
    }
}

void FormatTableDescriptor(TStringBuilderBase* builder, const TTableDescriptor& descriptor, const TFormatOptions& options)
{
    // Preserving existing behavior, see YT-28316.
    auto compatOptions = options;
    compatOptions.IsFinal = false;
    FormatId(builder, descriptor.Path, compatOptions);
    if (descriptor.Alias) {
        builder->AppendString(" AS ");
        FormatId(builder, *descriptor.Alias, compatOptions);
    }
    if (descriptor.Hint != TTableHint()) {
        Format(builder, " WITH HINT %v", descriptor.Hint);
    }
}

void FormatExpression(TStringBuilderBase* builder, const TExpression& expr, int depth, const TFormatOptions& options)
{
    if (depth >= MaxExpressionDepth) {
        THROW_ERROR_EXCEPTION("Maximum expression depth exceeded")
            .With("max_expression_depth", MaxExpressionDepth);
    }
    // Preserving existing behavior, see YT-28316.
    auto compatOptions = options;
    compatOptions.IsFinal = false;
    auto printTuple = [&] (TStringBuilderBase* builder, const TLiteralValueTuple& tuple) {
        bool needParens = tuple.size() > 1;
        if (needParens) {
            builder->AppendChar('(');
        }
        auto formatter = [&] (TStringBuilderBase* builder, const TLiteralValue& value) {
            FormatLiteralValue(builder, value, options);
        };
        // Tuples are not elided; their structure is needed to understand the query.
        JoinToString(
            builder,
            tuple.begin(),
            tuple.end(),
            formatter);
        if (needParens) {
            builder->AppendChar(')');
        }
    };

    auto printTuples = [&] (TStringBuilderBase* builder, const TLiteralValueTupleList& list) {
        if (options.Concise && list.size() > MaxConciseListLength) {
            JoinToString(
                builder,
                list.begin(),
                list.begin() + ElidedListLength,
                printTuple);
            FormatElision(builder, list.size());
        } else {
            JoinToString(
                builder,
                list.begin(),
                list.end(),
                printTuple);
        }
    };

    auto printRanges = [&] (TStringBuilderBase* builder, const TLiteralValueRangeList& list) {
        JoinToString(
            builder,
            list.begin(),
            list.end(),
            [&] (TStringBuilderBase* builder, const std::pair<TLiteralValueTuple, TLiteralValueTuple>& range) {
                printTuple(builder, range.first);
                builder->AppendString(" AND ");
                printTuple(builder, range.second);
            });
    };

    if (auto* typedExpr = expr.As<TLiteralExpression>()) {
        FormatLiteralValue(builder, typedExpr->Value, options);
    } else if (auto* typedExpr = expr.As<TReferenceExpression>()) {
        FormatReference(builder, typedExpr->Reference, depth + 1, options);
    } else if (auto* typedExpr = expr.As<TAliasExpression>()) {
        if (options.ExpandAliases) {
            builder->AppendChar('(');
            FormatExpression(builder, *typedExpr->Expression, depth + 1, compatOptions);
            builder->AppendString(" as ");
            FormatId(builder, typedExpr->Name, options);
            builder->AppendChar(')');
        } else {
            FormatId(builder, typedExpr->Name, options);
        }
    } else if (auto* typedExpr = expr.As<TFunctionExpression>()) {
        builder->AppendString(typedExpr->FunctionName);
        builder->AppendChar('(');
        FormatExpressions(builder, typedExpr->Arguments, depth + 1, compatOptions);
        builder->AppendChar(')');
    } else if (auto* typedExpr = expr.As<TUnaryOpExpression>()) {
        builder->AppendString(GetUnaryOpcodeLexeme(typedExpr->Opcode));
        builder->AppendChar('(');
        FormatExpressions(builder, typedExpr->Operand, depth + 1, compatOptions);
        builder->AppendChar(')');
    } else if (auto* typedExpr = expr.As<TBinaryOpExpression>()) {
        builder->AppendChar('(');
        FormatExpressions(builder, typedExpr->Lhs, depth + 1, compatOptions);
        builder->AppendChar(')');
        builder->AppendString(GetBinaryOpcodeLexeme(typedExpr->Opcode));
        builder->AppendChar('(');
        FormatExpressions(builder, typedExpr->Rhs, depth + 1, compatOptions);
        builder->AppendChar(')');
    } else if (auto* typedExpr = expr.As<TInExpression>()) {
        builder->AppendChar('(');
        FormatExpressions(builder, typedExpr->Expr, depth + 1, compatOptions);
        builder->AppendString(") IN (");
        printTuples(builder, typedExpr->Values);
        builder->AppendChar(')');
    } else if (auto* typedExpr = expr.As<TBetweenExpression>()) {
        builder->AppendChar('(');
        FormatExpressions(builder, typedExpr->Expr, depth + 1, compatOptions);
        builder->AppendString(") BETWEEN (");
        printRanges(builder, typedExpr->Values);
        builder->AppendChar(')');
    } else if (auto* typedExpr = expr.As<TTransformExpression>()) {
        builder->AppendString("TRANSFORM(");
        size_t argumentCount = typedExpr->Expr.size();
        auto needParenthesis = argumentCount > 1;
        if (needParenthesis) {
            builder->AppendChar('(');
        }
        FormatExpressions(builder, typedExpr->Expr, depth + 1, compatOptions);
        if (needParenthesis) {
            builder->AppendChar(')');
        }
        builder->AppendString(", (");
        printTuples(builder, typedExpr->From);
        builder->AppendString("), (");
        printTuples(builder, typedExpr->To);
        builder->AppendChar(')');

        if (typedExpr->DefaultExpr) {
            builder->AppendString(", ");
            FormatExpression(builder, *typedExpr->DefaultExpr, depth + 1, compatOptions);
        }

        builder->AppendChar(')');
    } else if (auto* typedExpr = expr.As<TCaseExpression>()) {
        builder->AppendString("CASE");

        if (typedExpr->OptionalOperand) {
            builder->AppendChar(' ');
            FormatExpression(builder, *typedExpr->OptionalOperand, depth + 1, compatOptions);
        }

        for (const auto& item : typedExpr->WhenThenExpressions) {
            builder->AppendString(" WHEN ");
            FormatExpression(builder, item.Condition, depth + 1, compatOptions);
            builder->AppendString(" THEN ");
            FormatExpression(builder, item.Result, depth + 1, compatOptions);
        }

        if (typedExpr->DefaultExpression) {
            builder->AppendString(" ELSE ");
            FormatExpression(builder, *typedExpr->DefaultExpression, depth + 1, compatOptions);
        }

        builder->AppendString(" END");
    } else if (auto* typedExpr = expr.As<TLikeExpression>()) {
        FormatExpressions(builder, typedExpr->Text, depth + 1, compatOptions);

        builder->AppendChar(' ');
        builder->AppendString(GetStringMatchOpcodeLexeme(typedExpr->Opcode));
        builder->AppendChar(' ');

        FormatExpressions(builder, typedExpr->Pattern, depth + 1, compatOptions);

        if (typedExpr->EscapeCharacter) {
            builder->AppendString(" ESCAPE ");
            FormatExpressions(builder, *typedExpr->EscapeCharacter, depth + 1, compatOptions);
        }
    } else if (auto* typedExpr = expr.As<TQueryExpression>()) {
        builder->AppendChar('(');
        builder->AppendString(" SELECT ");
        FormatQuery(builder, typedExpr->Query, compatOptions);
        builder->AppendChar(')');
    } else {
        YT_ABORT();
    }
}

void FormatExpression(TStringBuilderBase* builder, const TExpressionList& exprs, int depth, const TFormatOptions& options)
{
    YT_VERIFY(exprs.size() > 0);
    if (exprs.size() > 1) {
        builder->AppendChar('(');
    }
    FormatExpressions(builder, exprs, depth, options);
    if (exprs.size() > 1) {
        builder->AppendChar(')');
    }
}

void FormatExpressions(TStringBuilderBase* builder, const TExpressionList& exprs, int depth, const TFormatOptions& options)
{
    auto formatter = [&] (TStringBuilderBase* builder, const TExpressionPtr& expr) {
        FormatExpression(builder, *expr, depth, options);
    };
    if (options.Concise && exprs.size() > MaxConciseListLength) {
        JoinToString(
            builder,
            exprs.begin(),
            exprs.begin() + ElidedListLength,
            formatter);
        FormatElision(builder, exprs.size());
    } else {
        JoinToString(
            builder,
            exprs.begin(),
            exprs.end(),
            formatter);
    }
}

void FormatJoin(TStringBuilderBase* builder, const TJoin& join, const TFormatOptions& options)
{
    if (join.IsLeft) {
        builder->AppendString(" LEFT");
    }
    builder->AppendString(" JOIN ");
    FormatTableDescriptor(builder, join.Table, options);
    if (join.Fields.empty()) {
        builder->AppendString(" ON (");
        FormatExpressions(builder, join.Lhs, /*depth*/ 0, options);
        builder->AppendString(") = (");
        FormatExpressions(builder, join.Rhs, /*depth*/ 0, options);
        builder->AppendChar(')');
    } else {
        builder->AppendString(" USING ");
        auto formatter = [&] (TStringBuilderBase* builder, const TReferenceExpressionPtr& referenceExpr) {
            FormatReference(builder, referenceExpr->Reference, /*depth*/ 0, options);
        };
        // Joins are not elided; their structure is needed to understand the query.
        const auto& fields = join.Fields;
        JoinToString(
            builder,
            fields.begin(),
            fields.end(),
            formatter);
    }
    if (join.Predicate) {
        builder->AppendString(" AND (");
        FormatExpression(builder, *join.Predicate, /*depth*/ 0, options);
        builder->AppendChar(')');
    }
}

void FormatArrayJoin(TStringBuilderBase* builder, const TArrayJoin& join, const TFormatOptions& options)
{
    if (join.IsLeft) {
        builder->AppendString(" LEFT");
    }
    builder->AppendString(" ARRAY JOIN ");
    TFormatOptions aliasOptions = options;
    aliasOptions.ExpandAliases = true;
    auto formatter = [&] (TStringBuilderBase* builder, const TExpressionPtr& expr) {
        auto* alias = expr->As<TAliasExpression>();
        YT_VERIFY(alias);
        FormatExpression(builder, *alias->Expression, /*depth*/ 0, aliasOptions);
        builder->AppendString(" AS ");
        builder->AppendString(alias->Name);
    };
    const auto& columns = join.Columns;
    if (options.Concise && columns.size() > MaxConciseListLength) {
        JoinToString(
            builder,
            columns.begin(),
            columns.begin() + ElidedListLength,
            formatter);
        FormatElision(builder, columns.size());
    } else {
        JoinToString(
            builder,
            columns.begin(),
            columns.end(),
            formatter);
    }
    if (join.Predicate) {
        builder->AppendString(" AND (");
        FormatExpression(builder, *join.Predicate, /*depth*/ 0, options);
        builder->AppendChar(')');
    }
}

void FormatUnionAllSubqueries(
    TStringBuilderBase* builder,
    const TUnionAllClause& unionAll,
    const TFormatOptions& options)
{
    builder->AppendChar('(');
    bool first = true;
    for (const auto* subquery : unionAll.Subqueries) {
        if (!first) {
            builder->AppendString(" UNION ALL ");
        }
        first = false;
        builder->AppendString("SELECT ");
        FormatQuery(builder, subquery->Ast, options);
    }
    builder->AppendChar(')');
}

void FormatQuery(TStringBuilderBase* builder, const TQuery& query, const TFormatOptions& options)
{
    if (query.SelectExprs) {
        auto formatter = [&] (TStringBuilderBase* builder, const TExpressionPtr& expr) {
            FormatExpression(builder, *expr, /*depth*/ 0, options);
        };
        const auto& exprs = *query.SelectExprs;
        if (options.Concise && exprs.size() > MaxConciseListLength) {
            JoinToString(
                builder,
                exprs.begin(),
                exprs.begin() + ElidedListLength,
                formatter);
            FormatElision(builder, exprs.size());
        } else {
            JoinToString(
                builder,
                exprs.begin(),
                exprs.end(),
                formatter);
        }
    } else {
        builder->AppendString("*");
    }

    builder->AppendString(" FROM ");
    Visit(query.FromClause,
        [&] (const TTableDescriptor& tableDescriptor) {
            FormatTableDescriptor(builder, tableDescriptor, options);
        },
        [&] (const TQueryAstHeadPtr& subquery) {
            builder->AppendChar('(');
            builder->AppendString("SELECT ");
            FormatQuery(builder, subquery->Ast, options);
            builder->AppendChar(')');
            if (subquery->Alias) {
                builder->AppendString(" AS ");
                FormatId(builder, *subquery->Alias, options);
            }
        },
        [&] (const NAst::TExpressionList& expression) {
            builder->AppendChar('(');
            FormatExpression(builder, expression, /*depth*/ 0, options);
            builder->AppendChar(')');
        },
        [&] (const TUnionAllClause& unionAll) {
            FormatUnionAllSubqueries(builder, unionAll, options);
            if (unionAll.Alias) {
                builder->AppendString(" AS ");
                FormatId(builder, *unionAll.Alias, options);
            }
        });

    if (query.WithIndex) {
        builder->AppendString(" WITH INDEX ");
        FormatTableDescriptor(builder, *query.WithIndex, options);
    }

    for (const auto& join : query.Joins) {
        Visit(join,
            [&] (const TJoin& tableJoin) {
                FormatJoin(builder, tableJoin, options);
            },
            [&] (const TArrayJoin& arrayJoin) {
                FormatArrayJoin(builder, arrayJoin, options);
            });
    }

    if (query.WherePredicate) {
        builder->AppendString(" WHERE ");
        TFormatOptions aliasOptions = options;
        aliasOptions.ExpandAliases = true;
        FormatExpression(builder, *query.WherePredicate, /*depth*/ 0, aliasOptions);
    }

    if (query.GroupExprs) {
        builder->AppendString(" GROUP BY ");
        FormatExpressions(builder, *query.GroupExprs, /*depth*/ 0, options);
        if (query.TotalsMode == ETotalsMode::BeforeHaving) {
            builder->AppendString(" WITH TOTALS");
        }
    }

    if (query.HavingPredicate) {
        builder->AppendString(" HAVING ");
        FormatExpression(builder, *query.HavingPredicate, /*depth*/ 0, options);
    }

    if (query.GroupExprs && query.TotalsMode == ETotalsMode::AfterHaving) {
        builder->AppendString(" WITH TOTALS");
    }

    if (!query.OrderExpressions.empty()) {
        builder->AppendString(" ORDER BY ");
        JoinToString(
            builder,
            query.OrderExpressions.begin(),
            query.OrderExpressions.end(),
            [&] (TStringBuilderBase* builder, const TOrderExpression& orderExpression) {
                FormatExpression(builder, orderExpression.Expressions, /*depth*/ 0, options);
                if (orderExpression.Descending) {
                    builder->AppendString(" DESC");
                }
            });
    }

    if (query.Offset) {
        builder->AppendFormat(" OFFSET %v", *query.Offset);
    }

    if (query.Limit) {
        builder->AppendFormat(" LIMIT %v", *query.Limit);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::string FormatLiteralValue(const TLiteralValue& value)
{
    TStringBuilder builder;
    FormatLiteralValue(&builder, value, {});
    return builder.Flush();
}

std::string FormatId(TStringBuf id)
{
    TStringBuilder builder;
    FormatId(&builder, id, {});
    return builder.Flush();
}

std::string FormatReference(const TReference& ref)
{
    TStringBuilder builder;
    FormatReference(&builder, ref, /*depth*/ 0, {});
    return builder.Flush();
}

std::string FormatExpression(const TExpression& expr)
{
    TStringBuilder builder;
    FormatExpression(&builder, expr, /*depth*/ 0, {});
    return builder.Flush();
}

std::string FormatExpression(const TExpressionList& exprs)
{
    TStringBuilder builder;
    FormatExpression(&builder, exprs, /*depth*/ 0, {});
    return builder.Flush();
}

std::string FormatExpressionConcise(const TExpression& expr)
{
    TStringBuilder builder;
    FormatExpression(&builder, expr, /*depth*/ 0, {.ExpandAliases = false, .IsFinal = true, .Concise = true});
    return builder.Flush();
}

std::string FormatJoin(const TJoin& join)
{
    TStringBuilder builder;
    FormatJoin(&builder, join, {});
    return builder.Flush();
}

std::string FormatQuery(const TQuery& query)
{
    TStringBuilder builder;
    FormatQuery(&builder, query, {});
    return builder.Flush();
}

std::string FormatQueryConcise(const TQuery& query)
{
    TStringBuilder builder;
    FormatQuery(&builder, query, {.ExpandAliases = false, .IsFinal = true, .Concise = true});
    return builder.Flush();
}

std::string InferColumnName(const TExpression& expr)
{
    TStringBuilder builder;
    FormatExpression(&builder, expr, /*depth*/ 0, {.ExpandAliases = false, .IsFinal = true});
    return builder.Flush();
}

std::string InferColumnName(const TColumnReference& ref)
{
    TStringBuilder builder;
    FormatColumnReference(&builder, ref, {.IsFinal = true});
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetMainTablePath(const TQuery& query)
{
    return Visit(query.FromClause,
        [] (const TTableDescriptor& table) {
            return table.Path;
        },
        [] (const TQueryAstHeadPtr& subquery) {
            return GetMainTablePath(subquery->Ast);
        },
        [] (const TExpressionList& /*expression*/) -> NYPath::TYPath {
            THROW_ERROR_EXCEPTION("Unknown main table path");
        },
        [] (const TUnionAllClause& unionAll) -> NYPath::TYPath {
            if (unionAll.Subqueries.empty()) {
                THROW_ERROR_EXCEPTION("UNION ALL clause has no subqueries");
            }
            return GetMainTablePath(unionAll.Subqueries.front()->Ast);
        });
}

////////////////////////////////////////////////////////////////////////////////

NAst::TExpressionPtr BuildAndExpression(
    TObjectsHolder* holder,
    NAst::TExpressionPtr lhs,
    NAst::TExpressionPtr rhs)
{
    if (lhs && !rhs) {
        return lhs;
    }
    if (rhs && !lhs) {
        return rhs;
    }
    if (!lhs && !rhs) {
        return holder->New<NAst::TLiteralExpression>(TSourceLocation(), NAst::TLiteralValue(true));
    }
    return holder->New<NAst::TBinaryOpExpression>(
        TSourceLocation(),
        NQueryClient::EBinaryOp::And,
        NAst::TExpressionList{std::move(lhs)},
        NAst::TExpressionList{std::move(rhs)});
}

NAst::TExpressionPtr BuildOrExpression(
    TObjectsHolder* holder,
    NAst::TExpressionPtr lhs,
    NAst::TExpressionPtr rhs)
{
    if (lhs && !rhs) {
        return lhs;
    }
    if (rhs && !lhs) {
        return rhs;
    }
    if (!lhs && !rhs) {
        return holder->New<NAst::TLiteralExpression>(TSourceLocation(), NAst::TLiteralValue(false));
    }
    return holder->New<NAst::TBinaryOpExpression>(
        TSourceLocation(),
        NQueryClient::EBinaryOp::Or,
        NAst::TExpressionList{std::move(lhs)},
        NAst::TExpressionList{std::move(rhs)});
}

NAst::TExpressionPtr BuildConcatenationExpression(
    TObjectsHolder* holder,
    NAst::TExpressionPtr lhs,
    NAst::TExpressionPtr rhs,
    TStringBuf separator)
{
    if (lhs && !rhs) {
        return lhs;
    }
    if (rhs && !lhs) {
        return rhs;
    }
    if (!lhs && !rhs) {
        return holder->New<NAst::TLiteralExpression>(TSourceLocation(), NAst::TLiteralValue(""));
    }

    auto keySeparator = holder->New<NAst::TLiteralExpression>(TSourceLocation(), std::string(separator));
    lhs = holder->New<NAst::TBinaryOpExpression>(
        TSourceLocation(),
        NQueryClient::EBinaryOp::Concatenate,
        NAst::TExpressionList{std::move(lhs)},
        NAst::TExpressionList{std::move(keySeparator)});

    return holder->New<NAst::TBinaryOpExpression>(
        TSourceLocation(),
        NQueryClient::EBinaryOp::Concatenate,
        NAst::TExpressionList{std::move(lhs)},
        NAst::TExpressionList{std::move(rhs)});
}

NAst::TExpressionPtr BuildBinaryOperationTree(
    TObjectsHolder* context,
    std::vector<NAst::TExpressionPtr> leaves,
    EBinaryOp opCode)
{
    if (leaves.empty()) {
        return nullptr;
    }
    if (leaves.size() == 1) {
        return leaves[0];
    }
    std::deque<NAst::TExpressionPtr> expressionQueue(leaves.begin(), leaves.end());
    while (expressionQueue.size() != 1) {
        auto lhs = expressionQueue.front();
        expressionQueue.pop_front();
        auto rhs = expressionQueue.front();
        expressionQueue.pop_front();
        expressionQueue.emplace_back(context->New<NAst::TBinaryOpExpression>(
            NQueryClient::NullSourceLocation,
            opCode,
            NAst::TExpressionList{std::move(lhs)},
            NAst::TExpressionList{std::move(rhs)}));
    }
    return expressionQueue.front();
}

////////////////////////////////////////////////////////////////////////////////

TExpressionPtr MakeOrExpression(TObjectsHolder* holder, TSourceLocation sourceLocation, TExpressionPtr lhs, TExpressionPtr rhs)
{
    if (auto literalExpr = lhs->As<TLiteralExpression>()) {
        auto value = literalExpr->Value;
        if (std::holds_alternative<bool>(value)) {
            return std::get<bool>(value) ? lhs : rhs;
        }
    }

    if (auto literalExpr = rhs->As<TLiteralExpression>()) {
        auto value = literalExpr->Value;
        if (std::holds_alternative<bool>(value)) {
            return std::get<bool>(value) ? rhs : lhs;
        }
    }

    return holder->New<TBinaryOpExpression>(
        sourceLocation,
        EBinaryOp::Or,
        TExpressionList{lhs},
        TExpressionList{rhs});
}

TExpressionPtr MakeOrExpression(
    TObjectsHolder* holder,
    const TSourceLocation& sourceLocation,
    TRange<TExpressionPtr> expressions)
{
    if (expressions.empty()) {
        return holder->New<TLiteralExpression>(sourceLocation, false);
    }

    if (expressions.size() == 1) {
        return expressions.Front();
    }

    i64 midIndex = expressions.size() / 2;

    auto lhs = MakeOrExpression(
        holder,
        sourceLocation,
        expressions.Slice(0, midIndex));

    auto rhs = MakeOrExpression(
        holder,
        sourceLocation,
        expressions.Slice(midIndex, expressions.size()));

    return MakeOrExpression(holder, sourceLocation, lhs, rhs);
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TLiteralValue> ToLiteralValue(TUnaryOpExpression* expr)
{
    TExpressionPtr currentOperandExpr = expr;
    std::optional<TLiteralExpressionPtr> operandLiteral;
    std::stack<EUnaryOp> opcodes;

    while (auto* currentUnaryExpression = currentOperandExpr->As<TUnaryOpExpression>()) {
        if (currentUnaryExpression->Operand.size() != 1) {
            return std::nullopt;
        }

        currentOperandExpr = currentUnaryExpression->Operand[0];
        opcodes.push(currentUnaryExpression->Opcode);
    }

    if (auto* currentOperandLiteralExpr = currentOperandExpr->As<TLiteralExpression>()) {
        operandLiteral = currentOperandLiteralExpr;
    } else {
        return std::nullopt;
    }

    auto applyOperator = [] (EUnaryOp opcode, TLiteralValue value) -> std::optional<TLiteralValue> {
        switch (opcode) {
            case EUnaryOp::Minus: {
                if (const auto* data = std::get_if<i64>(&value)) {
                    return -(*data);
                } else if (const auto* data = std::get_if<ui64>(&value)) {
                    return -(*data);
                } else if (const auto* data = std::get_if<double>(&value)) {
                    return -(*data);
                } else {
                    return std::nullopt;
                }
                break;
            }

            case EUnaryOp::Plus: {
                if (const auto* data = std::get_if<i64>(&value)) {
                    return *data;
                } else if (const auto* data = std::get_if<ui64>(&value)) {
                    return *data;
                } else if (const auto* data = std::get_if<double>(&value)) {
                    return *data;
                } else {
                    return std::nullopt;
                }
                break;
            }

            case EUnaryOp::BitNot: {
                if (const auto* data = std::get_if<i64>(&value)) {
                    return ~(*data);
                } else if (const auto* data = std::get_if<ui64>(&value)) {
                    return ~(*data);
                } else {
                    return std::nullopt;
                }
                break;
            }

            case EUnaryOp::Not: {
                if (const auto* data = std::get_if<bool>(&value)) {
                    return !(*data);
                } else {
                    return std::nullopt;
                }
                break;
            }

            default:
                return std::nullopt;
        }
    };

    auto value = (*operandLiteral)->Value;

    while (!opcodes.empty()) {
        auto opcode = opcodes.top();
        opcodes.pop();

        if (auto updatedValue = applyOperator(opcode, value)) {
            value = *updatedValue;
        } else {
            return std::nullopt;
        }
    }

    return value;
}

std::optional<TLiteralValueTuple> ToLiteralValueTuple(const TExpressionTuple& exprTuple)
{
    auto literalValueTuple = TLiteralValueTuple();
    for (auto* expr : exprTuple) {
        if (auto* unaryOp = expr->As<TUnaryOpExpression>()) {
            if (auto literal = ToLiteralValue(unaryOp)) {
                literalValueTuple.push_back(*literal);
            } else {
                return std::nullopt;
            }
        } else if (auto* literal = expr->As<TLiteralExpression>()) {
            literalValueTuple.push_back(literal->Value);
        } else {
            return std::nullopt;
        }
    }

    return literalValueTuple;
}

std::optional<TLiteralValueRange> ToLiteralValueRange(const TExpressionRange& exprTuple)
{
    auto lower = ToLiteralValueTuple(exprTuple.first);
    auto upper = ToLiteralValueTuple(exprTuple.second);
    if (!lower.has_value() || !upper.has_value()) {
        return std::nullopt;
    }
    return std::make_pair(*lower, *upper);
}

////////////////////////////////////////////////////////////////////////////////

TExpressionList MakeInExpressionFromExpressionTupleList(
    TObjectsHolder* holder,
    const TSourceLocation& sourceLocation,
    const TExpressionList& expr,
    const TExpressionTupleList& expressions)
{
    auto inLiterals = TLiteralValueTupleList();
    auto inExpressions = TExpressionTupleList();

    for (const auto& exprTuple : expressions) {
        auto literalValueTuple = ToLiteralValueTuple(exprTuple);
        if (literalValueTuple) {
            inLiterals.push_back(*literalValueTuple);
        } else {
            inExpressions.push_back(exprTuple);
        }
    }

    TExpressionPtr resultExpression = holder->New<TLiteralExpression>(sourceLocation, false);

    if (!inLiterals.empty()) {
        resultExpression = holder->New<TInExpression>(
            sourceLocation,
            expr,
            inLiterals);
    }

    if (!inExpressions.empty()) {
        std::vector<TExpressionPtr> eqExpressions;
        i64 inExpressionsCount = std::ssize(inExpressions);
        eqExpressions.reserve(inExpressionsCount);

        for (const auto& inExpression : inExpressions) {
            eqExpressions.push_back(
                holder->New<TBinaryOpExpression>(
                    sourceLocation,
                    EBinaryOp::Equal,
                    expr,
                    inExpression));
        }

        resultExpression = MakeOrExpression(
            holder,
            sourceLocation,
            resultExpression,
                MakeOrExpression(
                holder,
                sourceLocation,
                TRange(eqExpressions)));
    }

    return TExpressionList{resultExpression};
}

////////////////////////////////////////////////////////////////////////////////

TExpressionList MakeBetweenExpressionFromExpressionTupleRangeList(
    TObjectsHolder* holder,
    const TSourceLocation& sourceLocation,
    const TExpressionList& expr,
    const TExpressionRangeList& expressions)
{
    TLiteralValueRangeList literalRanges;
    literalRanges.reserve(expressions.size());

    for (const auto& range : expressions) {
        auto literalRange = ToLiteralValueRange(range);

        THROW_ERROR_EXCEPTION_IF(!literalRange, "Expressions are not supported in multiple ranges BETWEEN clause");

        literalRanges.push_back(*literalRange);
    }

    return MakeExpression<TBetweenExpression>(holder, sourceLocation, expr, literalRanges);
}

TExpressionList MakeBetweenExpressionFromExpressionTupleRange(
    TObjectsHolder* holder,
    const TSourceLocation& sourceLocation,
    const TExpressionList& expr,
    const TExpressionTuple& lower,
    const TExpressionTuple& upper)
{
    auto* lowerExpression = holder->New<TBinaryOpExpression>(
        sourceLocation,
        EBinaryOp::GreaterOrEqual,
        expr,
        lower);

    auto* upperExpression = holder->New<TBinaryOpExpression>(
        sourceLocation,
        EBinaryOp::LessOrEqual,
        expr,
        upper);

    auto* resultExpression = holder->New<TBinaryOpExpression>(
        sourceLocation,
        EBinaryOp::And,
        TExpressionList{lowerExpression},
        TExpressionList{upperExpression});

    return TExpressionList{resultExpression};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NAst
