#include "external_source_builder.h"
#include "validation_functions.h"

#include <util/string/join.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NExternalSource {
namespace {

class TValidatedExternalDataSource final : public IExternalSource {
public:
    TValidatedExternalDataSource(
        const TString& name, 
        const std::vector<TExternalSourceBuilder::TAuthHolder>& authMethods, 
        const std::unordered_map<TString, TExternalSourceBuilder::TConditionalValidator>& availableProperties, 
        const std::vector<TRegExMatch>& hostnamePatterns)
        : Name_(name)
        , AuthMethodsForCheck_(authMethods)
        , AvailableProperties_(availableProperties)
        , HostnamePatterns_(hostnamePatterns)
    {

    }

    virtual TString Pack(const NKikimrExternalSources::TSchema&,
                         const NKikimrExternalSources::TGeneral&) const override {
        ythrow TExternalSourceException() << "Internal error. Only external table supports pack operation";
    }

    virtual TString GetName() const override {
        return Name_;
    }

    virtual bool HasExternalTable() const override {
        return false;
    }

    virtual TVector<TString> GetAuthMethods() const override {
        TVector<TString> result;

        for (auto a : AuthMethodsForCheck_) {
            result.push_back(a.Auth);
        }

        return result;
    }

    TVector<TString> GetAuthMethods(const TString& externalDataSourceDescription) const {
        NKikimrSchemeOp::TExternalDataSourceDescription proto;

        if (!proto.ParseFromString(externalDataSourceDescription)) {
            ythrow TExternalSourceException() 
                << "Internal error. "
                << "Couldn't parse protobuf with external data source description";
        }

        TVector<TString> result;

        for (auto a : AuthMethodsForCheck_) {
            if (a.UseCondition(proto.GetProperties().GetProperties())) {
                result.push_back(a.Auth);
            }
        }
        
        return result;
    }

    virtual TMap<TString, TVector<TString>> GetParameters(const TString&) const override {
        ythrow TExternalSourceException() << "Internal error. Only external table supports parameters";
    }

    virtual void ValidateExternalDataSource(const TString& externalDataSourceDescription) const override {
        NKikimrSchemeOp::TExternalDataSourceDescription proto;

        if (!proto.ParseFromString(externalDataSourceDescription)) {
            ythrow TExternalSourceException() 
                << "Internal error. "
                << "Couldn't parse protobuf with external data source description";
        }

        auto properties = proto.GetProperties().GetProperties();
        std::unordered_set<TString> validatedProperties;

        for (const auto& [key, value] : properties) {
            auto p = AvailableProperties_.find(key);

            if (AvailableProperties_.end() == p) {
                throw TExternalSourceException() << "Unsupported property: " << key;
            }

            // validate property value 
            if (p->second.ApplyCondition(properties)) {
                p->second.Validator(key, value);
            }

            validatedProperties.emplace(key);
        }

        // validate properties that has been left 
        for (const auto& [property, validator] : AvailableProperties_) {
            if (validatedProperties.contains(property)) {
                continue;
            }

            if (validator.ApplyCondition(properties)) {
                validator.Validator(property, "");
            }
        }

        ValidateHostname(HostnamePatterns_, proto.GetLocation());
    }

    virtual NThreading::TFuture<std::shared_ptr<TMetadata>> LoadDynamicMetadata(std::shared_ptr<TMetadata> meta) override {
        return NThreading::MakeFuture(std::move(meta));
    }

    virtual bool CanLoadDynamicMetadata() const override {
        return false;
    }

private: 
    const TString Name_;
    const std::vector<TExternalSourceBuilder::TAuthHolder> AuthMethodsForCheck_;
    const std::unordered_map<TString, TExternalSourceBuilder::TConditionalValidator> AvailableProperties_;
    const std::vector<TRegExMatch> HostnamePatterns_;
};

} // unnamed

TExternalSourceBuilder::TExternalSourceBuilder(const TString& name) 
    : Name_(name)
{
}

TExternalSourceBuilder& TExternalSourceBuilder::Auth(const TVector<TString>& authMethods, TCondition condition) {
    for (auto a : authMethods) {
        AuthMethodsForCheck_.push_back(TExternalSourceBuilder::TAuthHolder{a, condition});
    }

    return *this;
}

TExternalSourceBuilder& TExternalSourceBuilder::Property(TString name, TValidator validator, TCondition condition) {
    AvailableProperties_.emplace(name, TExternalSourceBuilder::TConditionalValidator{validator, condition});
    return *this;
} 

TExternalSourceBuilder& TExternalSourceBuilder::Properties(const TSet<TString>& availableProperties, TValidator validator, TCondition condition) {
    for (auto p : availableProperties) {
        Property(p, validator, condition);
    }

    return *this;
}

TExternalSourceBuilder& TExternalSourceBuilder::HostnamePatterns(const std::vector<TRegExMatch>& patterns) {
    HostnamePatterns_.insert(
        HostnamePatterns_.end(), patterns.begin(), patterns.end());
    return *this;
}

IExternalSource::TPtr TExternalSourceBuilder::Build() {
    return MakeIntrusive<TValidatedExternalDataSource>(
        std::move(Name_), std::move(AuthMethodsForCheck_), std::move(AvailableProperties_), std::move(HostnamePatterns_));
}

TCondition GetHasSettingCondition(const TString& property, const TString& value) {
    return [property, value](const ::google::protobuf::Map<TProtoStringType, TProtoStringType>& properties) -> bool {
        auto it = properties.find(property);
        return properties.end() != it && value == it->second;
    };
}

TValidator GetRequiredValidator() {
    return [](const TString& property, const TString& value){
        if (!value.empty()) {
            return;
        }

        throw TExternalSourceException() << "required property: " << property << " is not set";
    };
}

TValidator GetIsInListValidator(const std::unordered_set<TString>& values, bool required) {
    auto joinedValues = JoinSeq(", ", values);

    return [values, required, joinedValues](const TString& property, const TString& value){
        if (value.empty() && required) {
            throw TExternalSourceException() << " required property: " << property << " is not set";
        }

        if (value.empty()) {
            return;
        }

        if (!values.contains(value)) {
            throw TExternalSourceException() 
                << " property: " << property 
                << " has wrong value: " << value 
                << " allowed values: " << joinedValues;
        }
    };
}

} // NKikimr::NExternalSource
