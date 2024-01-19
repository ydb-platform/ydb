#include "validator.h"

#include "util/string/vector.h"
#include <util/string/cast.h>
#include <util/system/types.h>

#include <utility>

namespace NKikimr::NYamlConfig::NValidator {

using namespace NFyaml;

TValidationResult::TIssue::TIssue() {}

TValidationResult::TIssue::TIssue(const TString& NodePath, const TString& Problem)
    : NodePath(NodePath)
    , Problem(Problem) {}

TValidationResult::TIssue::TIssue(TIssue&& other)
    : NodePath(std::move(other.NodePath))
    , Problem(std::move(other.Problem)) {}

TValidationResult::TIssue::TIssue(const TIssue& other)
    : NodePath(other.NodePath)
    , Problem(other.Problem) {}

TValidationResult::TIssue& TValidationResult::TIssue::operator=(TIssue&& other) {
    NodePath = std::move(other.NodePath);
    Problem = std::move(other.Problem);
    return *this;
}

TValidationResult::TIssue& TValidationResult::TIssue::operator=(const TIssue& other) {
    NodePath = other.NodePath;
    Problem = other.Problem;
    return *this;
}

TValidationResult::TValidationResult() {}

TValidationResult::TValidationResult(TVector<TValidationResult::TIssue> issues)
    : Issues(std::move(issues)) {}

TValidationResult::TValidationResult(TValidationResult&& validationResult)
    : Issues(std::move(validationResult.Issues)) {}

TValidationResult::TValidationResult(const TValidationResult& validationResult)
    : Issues(validationResult.Issues) {}

TValidationResult& TValidationResult::operator=(TValidationResult&& validationResult) {
    Issues = std::move(validationResult.Issues);
    return *this;
}

TValidationResult& TValidationResult::operator=(const TValidationResult& validationResult) {
    Issues = validationResult.Issues;
    return *this;
}

bool TValidationResult::Ok() {
    return Issues.empty();
}

void TValidationResult::AddValidationErrors(TValidationResult validationResult) {
    for (auto& issue : validationResult.Issues) {
        Issues.emplace_back(std::move(issue));
    }
}

void TValidationResult::AddIssue(TIssue issue) {
    Issues.emplace_back(std::move(issue));
}

bool TValidationResult::TIssue::operator<(const TValidationResult::TIssue& other) const {
    return std::forward_as_tuple(Problem, NodePath) < std::forward_as_tuple(other.Problem, other.NodePath);
}

bool TValidationResult::TIssue::operator==(const TValidationResult::TIssue& other) const {
    return std::forward_as_tuple(Problem, NodePath) == std::forward_as_tuple(other.Problem, other.NodePath);
}

TValidationResult TValidator::Validate(const TString& documentStr) {
    try {
        TDocument document = TDocument::Parse(documentStr);
        return Validate(document.Root());
    } catch (TFyamlEx e) {
        TValidationResult validationResult;
        for (const auto& error : e.Errors()) {
            validationResult.Issues.emplace_back("", error);
        }
        return validationResult;
    }
}

TValidator::TValidator(ENodeType nodeType)
    : NodeType_(nodeType) {}

TValidator::TValidator(TValidator&& validator)
    : Required_(validator.Required_)
    , NodeType_(validator.NodeType_) {}

TValidator::~TValidator() {}

TGenericValidator::TGenericValidator()
    : TBase(ENodeType::Generic) {}

TGenericValidator::TGenericValidator(TGenericValidator&& validator)
    : TBase(std::move(validator))
    , ValidatorPtrs_(std::move(validator.ValidatorPtrs_)) {}

TValidationResult TGenericValidator::Validate(const NFyaml::TNodeRef& node) {
    if (ValidatorPtrs_.size() == 1) {
        return ValidatorPtrs_[0]->Validate(node);
    }

    TVector<TValidationResult> validationResults;

    for (const auto& validatorPtr : ValidatorPtrs_) {
        TValidationResult result = validatorPtr->Validate(node);
        if (result.Ok()) {
            return {};
        } else {
            validationResults.push_back(result);
        }
    }

    TString message = "There was errors in all scenarios:\n";

    int scenarioIndex = 1;
    bool addNewLine = false;
    for (const TValidationResult& vr : validationResults) {
        TString validationMesage;
        bool oneIssue = vr.Issues.size() == 1;
        if (oneIssue) {
            validationMesage = vr.Issues[0].Problem;
        } else {
            bool addNewline = false;
            for (const TValidationResult::TIssue& issue : vr.Issues) {
                // maybe fix for multiline messages
                validationMesage += ToString(addNewline ? "\n" : "" ) + "  - " + issue.Problem;
                addNewline = true;
            }
        }
        message += ToString(addNewLine ? "\n" : "") + "  " + ToString(scenarioIndex) + ") " + (oneIssue ? "" : "\n") + validationMesage;
        scenarioIndex++;
        addNewLine = true;
    }

    TValidationResult result = TVector<TValidationResult::TIssue>{{
        node.Path(),
        message
    }};

    // put checks code into separete function
    // think about all combinations of checker name and errors
    if (result.Ok()) {
        performChecks(result, node);
    }

    return result;
}

void TGenericValidator::AddValidator(TSimpleSharedPtr<TValidator> validatorPtr) {
    ValidatorPtrs_.emplace_back(std::move(validatorPtr));
}

TMapValidator::TMapValidator()
    : TBase(ENodeType::Map) {}

TMapValidator::TMapValidator(TMapValidator&& validator)
    : TBase(std::move(validator))
    , Children_(std::move(validator.Children_))
    , Opaque_(validator.Opaque_) {}

TMapValidator::TMapValidator(THashMap<TString, TSimpleSharedPtr<TValidator>>&& children, bool Opaque)
    : TBase(ENodeType::Map)
    , Children_(std::move(children))
    , Opaque_(Opaque) {}

TValidationResult TMapValidator::Validate(const TNodeRef& node) {
    if (node.Type() != NFyaml::ENodeType::Mapping) {
        return TValidationResult({{
            node.Path(),
            "Node must be Map"
        }});
    }

    TString nodePath = node.Path();
    if (nodePath == "/") nodePath = "";

    TValidationResult validationResult;
    for (const auto& [name, validatorPtr] : Children_) {
        TValidator& validator = *validatorPtr.Get();

        if (node.Map().Has(name)) {
            validationResult.AddValidationErrors(validator.Validate(node.Map().at(name)));
        } else if (validator.Required_) {
            validationResult.AddIssue({
                nodePath + "/" + name,
                "Node is required"
            });
        }
    }

    if (!Opaque_) {
        THashSet<TString> allowed;
        for (const auto& child : Children_) {
            allowed.insert(child.first);
        }

        for (const auto& mapNode : node.Map()) {
            // should check for non-scalar?
            TString name = mapNode.Key().Scalar();
            if (!allowed.contains(name)) {
                validationResult.AddIssue({
                    nodePath + "/" + name,
                    "Unexpected node"
                });
            }
        }
    }

    if (validationResult.Ok()) {
        performChecks(validationResult, node);
    }

    return validationResult;
}

TArrayValidator::TArrayValidator()
    : TBase(ENodeType::Array) {}

TArrayValidator::TArrayValidator(TArrayValidator&& validator)
    : TBase(std::move(validator))
    , ItemValidatorPtr_(std::move(validator.ItemValidatorPtr_))
    , Unique_(validator.Unique_) {}

TArrayValidator::TArrayValidator(TSimpleSharedPtr<TValidator> itemValidatorPtr, bool Unique)
    : TBase(ENodeType::Array)
    , ItemValidatorPtr_(std::move(itemValidatorPtr))
    , Unique_(Unique) {}

TValidationResult TArrayValidator::Validate(const TNodeRef& node) {
    if (node.Type() != NFyaml::ENodeType::Sequence) {
        return TValidationResult({{
            node.Path(),
            "Node must be Array"
        }});
    }

    TValidationResult validationResult;
    for (const auto& item : node.Sequence()) {
        validationResult.AddValidationErrors(ItemValidatorPtr_->Validate(item));
    }

    THashMap<TString, size_t> valueToIndex;
    if (Unique_) {
        for (size_t i = 0; i < node.Sequence().size(); ++i) {
            const auto& item = node.Sequence()[i];
            if (item.Type() != NFyaml::ENodeType::Scalar) {
                ythrow yexception() << "Can't check uniqueness of non-scalar fields";
            }
            TString value = item.Scalar();
            if (valueToIndex.contains(value)) {
                TString i1 = ToString(valueToIndex[value]);
                TString i2 = ToString(i);
                validationResult.AddIssue({
                    node.Path(),
                    "items with indexes " + i1 + " and " + i2 + " are conflicting"});
            }
            valueToIndex[value] = i;
        }
    }

    if (validationResult.Ok()) {
        performChecks(validationResult, node);
    }

    return validationResult;
}

TInt64Validator::TInt64Validator()
    : TBase(ENodeType::Int64) {}

TInt64Validator::TInt64Validator(TInt64Validator&& validator)
    : TBase(std::move(validator))
    , Min_(validator.Min_)
    , Max_(validator.Max_) {}

TInt64Validator::TInt64Validator(i64 min, i64 max)
    : TBase(ENodeType::Int64)
    , Min_(min)
    , Max_(max) {}

TValidationResult TInt64Validator::Validate(const TNodeRef& node) {
    if (node.Type() != NFyaml::ENodeType::Scalar) {
        return TValidationResult({{
            node.Path(),
            "Node must be Scalar(Int64)"
        }});
    }

    TValidationResult validationResult;

    if (auto res = TryFromString<i64>(node.Scalar())) {
        i64 intRes = res.GetRef();
        if (Min_ > intRes) {
            validationResult.AddIssue({
                node.Path(),
                TString("Value must be greater or equal to min value(i.e >= ") + ToString(Min_) + ")"
            });
        }
        if (Max_ < intRes) {
            validationResult.AddIssue({
                node.Path(),
                TString("Value must be less or equal to max value(i.e <= ") + ToString(Max_) + ")"
            });
        }
    } else {
        validationResult.AddIssue({
            node.Path(),
            "Can't convert value to Int64"
        });
    }

    if (validationResult.Ok()) {
        performChecks(validationResult, node);
    }

    return validationResult;
}

void TInt64Validator::SetMin(i64 min) {
    Min_ = min;
}

void TInt64Validator::SetMax(i64 max) {
    Max_ = max;
}

TStringValidator::TStringValidator()
    : TBase(ENodeType::String) {}

TStringValidator::TStringValidator(TStringValidator&& validator)
    : TBase(std::move(validator)) {}

TValidationResult TStringValidator::Validate(const TNodeRef& node) {
    if (node.Type() != NFyaml::ENodeType::Scalar) {
        return TValidationResult({{
            node.Path(),
            "Node must be Scalar(String)"
        }});
    }

    TValidationResult validationResult;

    if (validationResult.Ok()) {
        performChecks(validationResult, node);
    }

    return validationResult;
}

TBoolValidator::TBoolValidator()
    : TBase(ENodeType::Bool) {}

TBoolValidator::TBoolValidator(TBoolValidator&& validator)
    : TBase(std::move(validator)) {}

TValidationResult TBoolValidator::Validate(const TNodeRef& node) {
    if (node.Type() != NFyaml::ENodeType::Scalar) {
        return TValidationResult({{
            node.Path(),
            "Node must be Scalar(Bool)"
        }});
    }

    TString value = node.Scalar();
    value.to_lower(); // is this correct to_lower?
    if (value != "true" && value != "false") {
        return TValidationResult({{
            node.Path(),
            "Value must be either true or false"
        }});
    }

    TValidationResult validationResult;

    performChecks(validationResult, node);

    return validationResult;
}

TEnumValidator::TEnumValidator()
    : TBase(ENodeType::Enum) {}

TEnumValidator::TEnumValidator(TEnumValidator&& validator)
    : TBase(std::move(validator)), Items_(std::move(validator.Items_)) {}

TValidationResult TEnumValidator::Validate(const TNodeRef& node) {
    if (node.Type() != NFyaml::ENodeType::Scalar) {
        return TValidationResult({{
            node.Path(),
            "Node must be Scalar(Enum)"
        }});
    }

    TValidationResult validationResult;

    TString value = node.Scalar();
    value.to_lower();
    if (!Items_.contains(value)) {
        TString variants = JoinStrings(Items_.begin(), Items_.end(), ", ");

        validationResult.Issues.push_back({
            node.Path(),
            "Node value must be one of the following: " + variants + ". (it was \"" + node.Scalar() + "\")"});
    }

    if (validationResult.Ok()) {
        performChecks(validationResult, node);
    }

    return validationResult;
}

} // namespace NKikimr::NYamlConfig::NValidator

IOutputStream& operator<<(IOutputStream& out, const NKikimr::NYamlConfig::NValidator::TValidationResult::TIssue& issue) {
    return out << issue.NodePath << ": " << issue.Problem;
}

Y_DECLARE_OUT_SPEC(, NKikimr::NYamlConfig::NValidator::TValidationResult, out, result) {
for (const auto& issue : result.Issues) {
        out << issue << ";" << Endl;
    }
}
