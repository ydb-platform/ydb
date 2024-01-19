#pragma once

#include "fwd.h"

#include "validator_checks.h"

#include <ydb/library/fyamlcpp/fyamlcpp.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <util/stream/output.h>
#include <util/generic/hash.h>
#include <util/system/types.h>
#include <util/generic/ylimits.h>

namespace NKikimr::NYamlConfig::NValidator {

class TValidationResult {
    friend class TCheckContext;
    friend class TGenericValidator;
    friend class TMapValidator;
    friend class TArrayValidator;
    friend class TInt64Validator;
    friend class TStringValidator;
    friend class TBoolValidator;
    template <typename TThis, typename TContext> friend class NDetail::TValidatorCommonOps;

public:
    struct TIssue {
        TString NodePath;
        TString Problem;

        TIssue();
        TIssue(const TString& NodePath, const TString& Problem);
        TIssue(const TIssue& other);
        TIssue(TIssue&& other);
        TIssue& operator=(TIssue&& other);
        TIssue& operator=(const TIssue& other);

        bool operator<(const TValidationResult::TIssue& other) const;
        bool operator==(const TValidationResult::TIssue& other) const;
    };

    TValidationResult();
    TValidationResult(TVector<TIssue> issues);

    TValidationResult(const TValidationResult& validationResult);
    TValidationResult(TValidationResult&& validationResult);

    TValidationResult& operator=(TValidationResult&& validationResult);
    TValidationResult& operator=(const TValidationResult& validationResult);

    bool Ok();

    TVector<TIssue> Issues;

private:
    void AddValidationErrors(TValidationResult validationResult);
    void AddIssue(TIssue issue);
};

class TValidator {
    friend TSimpleSharedPtr<TValidator> NYamlConfig::NValidator::NDetail::CreateValidatorPtr(const TSimpleSharedPtr<NDetail::TBuilder>& builder);
    friend class TMapValidator; // for .Required_
    friend class TGenericBuilder; // for .Required_
    friend class TMapNodeWrapper; // NodeType
    friend class TArrayNodeWrapper; // NodeType

public:
    virtual TValidationResult Validate(const NFyaml::TNodeRef& node) = 0;
    TValidationResult Validate(const TString& document);

    virtual ~TValidator();

protected:
    bool Required_ = true;

    TValidator(ENodeType nodeType);
    TValidator(TValidator&& validator);

private:
    ENodeType NodeType_;
};

namespace NDetail {

template <typename TThis, typename TContext>
class TValidatorCommonOps : public TValidator {
protected:
    THashMap<TString, std::function<void(TContext&)>> Checkers_;

    TValidatorCommonOps<TThis, TContext>(ENodeType nodeType);
    TValidatorCommonOps<TThis, TContext>(TValidatorCommonOps<TThis, TContext>&& builder);

    void performChecks(TValidationResult& validationResult, const NFyaml::TNodeRef& node);
};

template <typename TThis, typename TContext>
TValidatorCommonOps<TThis, TContext>::TValidatorCommonOps(ENodeType nodeType)
    : TValidator(nodeType) {}

template <typename TThis, typename TContext>
TValidatorCommonOps<TThis, TContext>::TValidatorCommonOps(TValidatorCommonOps<TThis, TContext>&& builder)
    : TValidator(std::move(builder))
    , Checkers_(std::move(builder.Checkers_)) {}

template <typename TThis, typename TContext>
void TValidatorCommonOps<TThis, TContext>::performChecks(TValidationResult& validationResult, const NFyaml::TNodeRef& node) {
    TThis* This = static_cast<TThis*>(this);
    for (auto& [checkName, checker] : Checkers_) {
        // TODO: change node.Path() to stored path in validator (or maybe node.Path is better?)
        TString nodePath = node.Path();
        if (nodePath == "/") {
            nodePath = "";
        }

        TContext context(node, nodePath, This);
        try {
            checker(context);
        } catch (NValidator::TCheckException& e) {
            context.AddError(e.what());
            context.someErrorOccured_ = true;
        } catch (NValidator::TFailException& e) {
            context.someErrorOccured_ = true;
        }
        
        if (context.someErrorOccured_) {
            if (context.Errors_.empty()) {
                validationResult.AddIssue({node.Path(), checkName});
            } else if (context.Errors_.size() == 1) {
                validationResult.AddIssue({node.Path(), "Check \"" + checkName + "\" failed: " + context.Errors_[0]});
            } else {
                TString errorMessages;
                
                //TODO: add multiline errors support(and maybe extract big
                //error creation into separete function)
                bool newLine = false;
                for (auto& error : context.Errors_) {
                    if (newLine) {
                        errorMessages += "\n";
                    }
                    errorMessages += error;
                    newLine = true;
                }

                validationResult.AddIssue({node.Path(), "Check \"" + checkName + "\" failed: \n" + errorMessages});
            }
        }
    }
}

}

class TGenericValidator : public NDetail::TValidatorCommonOps<TGenericValidator, TGenericCheckContext> {
    friend class TGenericNodeWrapper;
    friend class TGenericBuilder;

    using TBase = NDetail::TValidatorCommonOps<TGenericValidator, TGenericCheckContext>;

public:
    TGenericValidator();

    TGenericValidator(TGenericValidator&& validator);

    TValidationResult Validate(const NFyaml::TNodeRef& node) override;
    using TValidator::Validate;

private:
    TVector<TSimpleSharedPtr<TValidator>> ValidatorPtrs_;

    void AddValidator(TSimpleSharedPtr<TValidator> validatorPtr);
};

class TMapValidator : public NDetail::TValidatorCommonOps<TMapValidator, TMapCheckContext> {
    friend class TMapNodeWrapper;
    friend class TMapBuilder;

    using TBase = NDetail::TValidatorCommonOps<TMapValidator, TMapCheckContext>;

public:
    TMapValidator();

    TMapValidator(TMapValidator&& validator);

    TValidationResult Validate(const NFyaml::TNodeRef& node) override;
    using TValidator::Validate;

private:
    THashMap<TString, TSimpleSharedPtr<TValidator>> Children_;
    bool Opaque_;

    TMapValidator(THashMap<TString, TSimpleSharedPtr<TValidator>>&& children, bool Opaque);
};

class TArrayValidator : public NDetail::TValidatorCommonOps<TArrayValidator, TArrayCheckContext> {
    friend class TArrayNodeWrapper;
    friend class TArrayBuilder;

    using TBase = NDetail::TValidatorCommonOps<TArrayValidator, TArrayCheckContext>;

public:
    TArrayValidator();

    TArrayValidator(TArrayValidator&& validator);

    TValidationResult Validate(const NFyaml::TNodeRef& node) override;
    using TValidator::Validate;

private:
    TSimpleSharedPtr<TValidator> ItemValidatorPtr_;
    bool Unique_;

    TArrayValidator(TSimpleSharedPtr<TValidator> itemValidatorPtr, bool Unique);
};

class TInt64Validator : public NDetail::TValidatorCommonOps<TInt64Validator, TInt64CheckContext> {
    friend class TInt64NodeWrapper;
    friend class TInt64Builder;

    using TBase = NDetail::TValidatorCommonOps<TInt64Validator, TInt64CheckContext>;

public:
    TInt64Validator();

    TInt64Validator(TInt64Validator&& validator);

    TValidationResult Validate(const NFyaml::TNodeRef& node) override;
    using TValidator::Validate;

private:
    i64 Min_ = Min<i64>();
    i64 Max_ = Max<i64>();

    TInt64Validator(i64 min, i64 max);

    void SetMin(i64 min);
    void SetMax(i64 max);
};

class TStringValidator : public NDetail::TValidatorCommonOps<TStringValidator, TStringCheckContext> {
    friend class TStringNodeWrapper;
    friend class TStringBuilder;

    using TBase = NDetail::TValidatorCommonOps<TStringValidator, TStringCheckContext>;

public:
    TStringValidator();

    TStringValidator(TStringValidator&& validator);

    TValidationResult Validate(const NFyaml::TNodeRef& node) override;
    using TValidator::Validate;
};

class TBoolValidator : public NDetail::TValidatorCommonOps<TBoolValidator, TBoolCheckContext> {
    friend class TBoolNodeWrapper;
    friend class TBoolBuilder;

    using TBase = NDetail::TValidatorCommonOps<TBoolValidator, TBoolCheckContext>;

public:
    TBoolValidator();

    TBoolValidator(TBoolValidator&& validator);

    TValidationResult Validate(const NFyaml::TNodeRef& node) override;
    using TValidator::Validate;
};

class TEnumValidator : public NDetail::TValidatorCommonOps<TEnumValidator, TEnumCheckContext> {
    friend class TEnumNodeWrapper;
    friend class TEnumBuilder;

    using TBase = NDetail::TValidatorCommonOps<TEnumValidator, TEnumCheckContext>;

public:
    TEnumValidator();

    TEnumValidator(TEnumValidator&& validator);

    TValidationResult Validate(const NFyaml::TNodeRef& node) override;
    using TValidator::Validate;

private:
    THashSet<TString> Items_;
};

} // namespace NKikimr::NYamlConfig::NValidator

IOutputStream& operator<<(IOutputStream& out, const NKikimr::NYamlConfig::NValidator::TValidationResult::TIssue& issue);
