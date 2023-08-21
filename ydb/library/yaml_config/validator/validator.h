#pragma once

#include <library/cpp/yaml/fyamlcpp/fyamlcpp.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <util/stream/output.h>
#include <util/generic/hash.h>
#include <util/system/types.h>
#include <util/generic/ylimits.h>

namespace NYamlConfig::NValidator {

class TValidator;
class TGenericValidator;
class TMapValidator;
class TArrayValidator;
class TInt64Validator;
class TStringValidator;
class TBoolValidator;

class TGenericBuilder;

namespace NDetail {

class TBuilder;

TSimpleSharedPtr<TValidator> CreateValidatorPtr(const TSimpleSharedPtr<NDetail::TBuilder>& builder);

}

class TValidationResult {
    friend class TGenericValidator;
    friend class TMapValidator;
    friend class TArrayValidator;
    friend class TInt64Validator;
    friend class TStringValidator;
    friend class TBoolValidator;

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

public:
    TValidator();

    virtual TValidationResult Validate(const NFyaml::TNodeRef& node) = 0;
    TValidationResult Validate(const TString& document);

    virtual ~TValidator();

protected:
    TValidator(TValidator&& validator);

private:
    bool Required_ = true;
};

class TGenericValidator : public TValidator {
public:
    TGenericValidator();

    TGenericValidator(TGenericValidator&& validator);

    TValidationResult Validate(const NFyaml::TNodeRef& node) override;
    using TValidator::Validate;

    void AddValidator(TSimpleSharedPtr<TValidator> validatorPtr);

private:
    TVector<TSimpleSharedPtr<TValidator>> ValidatorPtrs_;
};

class TMapValidator : public TValidator {
public:
    TMapValidator();

    TMapValidator(TMapValidator&& validator);

    explicit TMapValidator(THashMap<TString, TSimpleSharedPtr<TValidator>>&& children, bool Opaque);

    TValidationResult Validate(const NFyaml::TNodeRef& node) override;
    using TValidator::Validate;

private:
    THashMap<TString, TSimpleSharedPtr<TValidator>> Children_;
    bool Opaque_;
};

class TArrayValidator : public TValidator {
public:
    TArrayValidator();

    TArrayValidator(TArrayValidator&& validator);

    TArrayValidator(TSimpleSharedPtr<TValidator> itemValidatorPtr, bool Unique);

    TValidationResult Validate(const NFyaml::TNodeRef& node) override;
    using TValidator::Validate;

private:
    TSimpleSharedPtr<TValidator> ItemValidatorPtr_;
    bool Unique_;
};

class TInt64Validator : public TValidator {
public:
    TInt64Validator();

    TInt64Validator(TInt64Validator&& validator);
    
    TInt64Validator(i64 min, i64 max);

    TValidationResult Validate(const NFyaml::TNodeRef& node) override;
    using TValidator::Validate;

private:
    i64 Min_ = Min<i64>();
    i64 Max_ = Max<i64>();

    void SetMin(i64 min);
    void SetMax(i64 max);
};

class TStringValidator : public TValidator {
public:
    TStringValidator();

    TStringValidator(TStringValidator&& validator);

    TValidationResult Validate(const NFyaml::TNodeRef& node) override;
    using TValidator::Validate;
};

class TBoolValidator : public TValidator {
public:
    TBoolValidator();

    TBoolValidator(TBoolValidator&& validator);

    TValidationResult Validate(const NFyaml::TNodeRef& node) override;
    using TValidator::Validate;
};

} // namespace NYamlConfig::NValidator

IOutputStream& operator<<(IOutputStream& out, const NYamlConfig::NValidator::TValidationResult::TIssue& issue);
