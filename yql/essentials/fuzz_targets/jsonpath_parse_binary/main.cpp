#include <yql/essentials/minikql/jsonpath/parser/parser.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace {

using namespace NYql;
using namespace NYql::NJsonPath;

constexpr size_t MaxInputSize = 4 * 1024;
constexpr size_t MaxParseErrors = 4;
constexpr size_t MaxReadItems = 256;

bool IsBinaryOperation(EJsonPathItemType type) {
    switch (type) {
        case EJsonPathItemType::BinaryAdd:
        case EJsonPathItemType::BinarySubstract:
        case EJsonPathItemType::BinaryMultiply:
        case EJsonPathItemType::BinaryDivide:
        case EJsonPathItemType::BinaryModulo:
        case EJsonPathItemType::BinaryLess:
        case EJsonPathItemType::BinaryLessEqual:
        case EJsonPathItemType::BinaryGreater:
        case EJsonPathItemType::BinaryGreaterEqual:
        case EJsonPathItemType::BinaryEqual:
        case EJsonPathItemType::BinaryNotEqual:
        case EJsonPathItemType::BinaryAnd:
        case EJsonPathItemType::BinaryOr:
            return true;
        default:
            return false;
    }
}

void ConsumeItemPayload(const TJsonPathItem& item) {
    switch (item.Type) {
        case EJsonPathItemType::Variable:
        case EJsonPathItemType::StringLiteral:
        case EJsonPathItemType::MemberAccess:
            (void)item.GetString();
            break;
        case EJsonPathItemType::NumberLiteral:
            (void)item.GetNumber();
            break;
        case EJsonPathItemType::BooleanLiteral:
            (void)item.GetBoolean();
            break;
        case EJsonPathItemType::ArrayAccess:
            (void)item.GetSubscripts();
            break;
        case EJsonPathItemType::FilterPredicate:
            (void)item.GetFilterPredicateOffset();
            break;
        case EJsonPathItemType::StartsWithPredicate:
            (void)item.GetStartsWithPrefixOffset();
            break;
        case EJsonPathItemType::LikeRegexPredicate:
            (void)item.GetRegex();
            break;
        default:
            if (IsBinaryOperation(item.Type)) {
                (void)item.GetBinaryOpArguments();
            }
            break;
    }
}

void TraverseBinaryJsonPath(const TJsonPathPtr& path) {
    TJsonPathReader reader(path);
    (void)reader.GetMode();

    TVector<const TJsonPathItem*> stack;
    THashSet<const TJsonPathItem*> seen;
    stack.push_back(&reader.ReadFirst());

    size_t reads = 0;
    while (!stack.empty() && reads++ < MaxReadItems) {
        const TJsonPathItem* item = stack.back();
        stack.pop_back();
        if (!seen.insert(item).second) {
            continue;
        }

        ConsumeItemPayload(*item);

        if (item->InputItemOffset) {
            stack.push_back(&reader.ReadInput(*item));
        }

        switch (item->Type) {
            case EJsonPathItemType::ArrayAccess:
                for (const auto& subscript : item->GetSubscripts()) {
                    stack.push_back(&reader.ReadFromSubscript(subscript));
                    if (subscript.IsRange()) {
                        stack.push_back(&reader.ReadToSubscript(subscript));
                    }
                }
                break;
            case EJsonPathItemType::FilterPredicate:
                stack.push_back(&reader.ReadFilterPredicate(*item));
                break;
            case EJsonPathItemType::StartsWithPredicate:
                stack.push_back(&reader.ReadPrefix(*item));
                break;
            default:
                if (IsBinaryOperation(item->Type)) {
                    stack.push_back(&reader.ReadLeftOperand(*item));
                    stack.push_back(&reader.ReadRightOperand(*item));
                }
                break;
        }
    }
}

void ExerciseJsonPath(TStringBuf jsonPath) {
    TIssues astIssues;
    const TAstNodePtr ast = ParseJsonPathAst(jsonPath, astIssues, MaxParseErrors);
    if (ast && astIssues.Empty()) {
        TJsonPathBuilder builder;
        ast->Accept(builder);
        TraverseBinaryJsonPath(builder.ShrinkAndGetResult());
    }

    TIssues parseIssues;
    const TJsonPathPtr parsed = ParseJsonPath(jsonPath, parseIssues, MaxParseErrors);
    if (parsed && parseIssues.Empty()) {
        TraverseBinaryJsonPath(parsed);
    }
}

TString QuoteMember(TStringBuf value) {
    TString result;
    result.reserve(value.size() + 4);
    result += "$.\"";
    for (char c : value) {
        if (c == '"' || c == '\\') {
            result += '\\';
        }
        result += c;
    }
    result += '"';
    return result;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    if (size > MaxInputSize) {
        return 0;
    }

    try {
        const TString input(reinterpret_cast<const char*>(data), size);
        ExerciseJsonPath(input);

        FuzzedDataProvider provider(data, size);
        const bool addMode = provider.ConsumeBool();
        const bool wrapAsMember = provider.ConsumeBool();
        const TString payload = provider.ConsumeRemainingBytesAsString();

        ExerciseJsonPath(payload);

        if (wrapAsMember) {
            ExerciseJsonPath(QuoteMember(payload));
        }

        if (addMode) {
            ExerciseJsonPath(TString("strict ") + payload);
            ExerciseJsonPath(TString("lax ") + payload);
        }
    } catch (...) {
    }

    return 0;
}
