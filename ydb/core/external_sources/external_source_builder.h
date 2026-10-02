#pragma once 

#include "external_source.h"

#include <library/cpp/regex/pcre/regexp.h>
#include <util/generic/set.h>

namespace NKikimr::NExternalSource {

typedef std::function<void(const TString&, const TString&)> TValidator;
typedef std::function<bool(const ::google::protobuf::Map<TProtoStringType, TProtoStringType>&)> TCondition;

///
/// Builder to create an external data source with validations
///
class TExternalSourceBuilder {
public:
    struct TAuthHolder {
        TString Auth;

        // When auth has to be used
        TCondition UseCondition;
    };

    struct TConditionalValidator {
        TValidator Validator;

        // When validator has to be applied
        TCondition ApplyCondition;
    };

public: 
    explicit TExternalSourceBuilder(const TString& name);

    ~TExternalSourceBuilder() = default;

    ///
    /// Add auth methods which are returned from the "source" only if a condition is true. 
    /// A condition is applied to source's ddl in @sa IExternalSource::GetAuthMethods
    /// call.
    /// 
    TExternalSourceBuilder& Auth(const TVector<TString>& authMethods, TCondition condition);

    TExternalSourceBuilder& Auth(const TVector<TString>& authMethods) {
        return Auth(authMethods, [](const ::google::protobuf::Map<TProtoStringType, TProtoStringType>&){
            return true;
        });
    }

    ///
    /// Add property which can be in a "source".
    ///
    /// @param name         name of a property
    /// @param validator    validator which is applied to a property from a source's ddl 
    ///                     in @sa IExternalSource::ValidateExternalDataSource call
    /// @param condition    condition that defines to use validator or not, if condition returns true 
    ///                     for source's ddl then validator is applied; otherwise, validator is skiped; 
    ///                     condition is executed in @sa IExternalSource::ValidateExternalDataSource call 
    ///                     before validator 
    ///
    TExternalSourceBuilder& Property(const TString name, TValidator validator, TCondition condition);

    TExternalSourceBuilder& Properties(const TSet<TString>& properties, TValidator validator, TCondition condition);

    TExternalSourceBuilder& HostnamePatterns(const std::vector<TRegExMatch>& patterns);

    ///
    ///  Create external data source
    ///
    IExternalSource::TPtr Build();

    TExternalSourceBuilder& Property(const TString name, TValidator validator) {
        return Property(name, validator, [](const ::google::protobuf::Map<TProtoStringType, TProtoStringType>&){
            return true;
        });
    }

    TExternalSourceBuilder& Property(const TString name) {
        return Property(name, [](const TString&, const TString&){});
    }

    TExternalSourceBuilder& Properties(const TSet<TString>& properties, TValidator validator) {
        return Properties(properties, validator, [](const ::google::protobuf::Map<TProtoStringType, TProtoStringType>&){
            return true;
        });
    }

    TExternalSourceBuilder& Properties(const TSet<TString>& properties) {
        return Properties(properties, [](const TString&, const TString&){});
    }

 private: 
    TString Name_;
    std::vector<TAuthHolder> AuthMethodsForCheck_;
    std::unordered_map<TString, TConditionalValidator> AvailableProperties_;
    std::vector<TRegExMatch> HostnamePatterns_;
};

///
/// Create a condition that returns "true" if a source's ddl has 
/// property "p" with value equals to "v"
///  
TCondition GetHasSettingCondition(const TString& p, const TString& v);

/// 
/// Create a validator which check that source's ddl has a property with non empty value  
///  
TValidator GetRequiredValidator();

/// 
/// Create a validator which check that source's ddl has a property with a value from list   
///
/// @param values       list of allowed values 
/// @param required     allow property without value 
///
TValidator GetIsInListValidator(const std::unordered_set<TString>& values, bool required);

} // NKikimr::NExternalSource
