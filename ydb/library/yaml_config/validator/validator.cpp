#include "validator.h"

#include <util/string/cast.h>
#include <util/system/types.h>

#include <utility>

using namespace NFyaml;

namespace NYamlConfig::NValidator {

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
    TDocument document = TDocument::Parse(documentStr);
    return Validate(document.Root());
}

TValidator::TValidator() {}

TValidator::TValidator(TValidator&& validator)
    : Required_(validator.Required_) {}

TValidator::~TValidator() {}

TGenericValidator::TGenericValidator() {}

TGenericValidator::TGenericValidator(TGenericValidator&& validator)
    : TValidator(std::move(validator))
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

    return TVector<TValidationResult::TIssue>{{
        node.Path(),
        message
    }};
}

void TGenericValidator::AddValidator(TSimpleSharedPtr<TValidator> validatorPtr) {
    ValidatorPtrs_.emplace_back(std::move(validatorPtr));
}

TMapValidator::TMapValidator() {}

TMapValidator::TMapValidator(TMapValidator&& validator)
    : TValidator(std::move(validator))
    , Children_(std::move(validator.Children_))
    , Opaque_(validator.Opaque_) {}

TMapValidator::TMapValidator(THashMap<TString, TSimpleSharedPtr<TValidator>>&& children, bool Opaque)
    : Children_(std::move(children))
    , Opaque_(Opaque) {}

TValidationResult TMapValidator::Validate(const TNodeRef& node) {
    if (node.Type() != ENodeType::Mapping) {
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

    return validationResult;
}

TArrayValidator::TArrayValidator() {}

TArrayValidator::TArrayValidator(TArrayValidator&& validator)
    : TValidator(std::move(validator))
    , ItemValidatorPtr_(std::move(validator.ItemValidatorPtr_))
    , Unique_(validator.Unique_) {}

TArrayValidator::TArrayValidator(TSimpleSharedPtr<TValidator> itemValidatorPtr, bool Unique)
    : ItemValidatorPtr_(std::move(itemValidatorPtr)), Unique_(Unique) {}

TValidationResult TArrayValidator::Validate(const TNodeRef& node) {
    if (node.Type() != ENodeType::Sequence) {
        return TValidationResult({{
            node.Path(),
            "Node must be Array"
        }});
    }

    TValidationResult validationResult;
    for (const auto& item : node.Sequence()) {
        validationResult.AddValidationErrors(ItemValidatorPtr_->Validate(item));
    }

    // TODO: check uniqueness
    Y_UNUSED(Unique_);
    return validationResult;
}

TInt64Validator::TInt64Validator() {}

TInt64Validator::TInt64Validator(TInt64Validator&& validator)
    : TValidator(std::move(validator))
    , Min_(validator.Min_)
    , Max_(validator.Max_) {}

TInt64Validator::TInt64Validator(i64 min, i64 max)
    : Min_(min)
    , Max_(max) {}

TValidationResult TInt64Validator::Validate(const TNodeRef& node) {
    if (node.Type() != ENodeType::Scalar) {
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

    return validationResult;
}

void TInt64Validator::SetMin(i64 min) {
    Min_ = min;
}

void TInt64Validator::SetMax(i64 max) {
    Max_ = max;
}

TStringValidator::TStringValidator() {}

TStringValidator::TStringValidator(TStringValidator&& validator)
    : TValidator(std::move(validator)) {}

TValidationResult TStringValidator::Validate(const TNodeRef& node) {
    if (node.Type() != ENodeType::Scalar) {
        return TValidationResult({{
            node.Path(),
            "Node must be Scalar(String)"
        }});
    }
    return {};
}

TBoolValidator::TBoolValidator() {}

TBoolValidator::TBoolValidator(TBoolValidator&& validator)
    : TValidator(std::move(validator)) {}

TValidationResult TBoolValidator::Validate(const TNodeRef& node) {
    if (node.Type() != ENodeType::Scalar) {
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
    return {};
}

} // namespace NYamlConfig::NValidator

IOutputStream& operator<<(IOutputStream& out, const NYamlConfig::NValidator::TValidationResult::TIssue& issue) {
    return out << issue.NodePath << ": " << issue.Problem;
}

Y_DECLARE_OUT_SPEC(, NYamlConfig::NValidator::TValidationResult, out, result) {
for (const auto& issue : result.Issues) {
        out << issue << ";" << Endl;
    }
}
