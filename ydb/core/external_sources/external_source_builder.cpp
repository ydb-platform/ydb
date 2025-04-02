#include "external_source_builder.h"
#include "validation_functions.h"

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

    virtual TVector<TString> GetAuthMethods(const TString& externalDataSourceDescription) const override {
        NKikimrSchemeOp::TExternalDataSourceDescription proto;

        if (!proto.ParseFromString(externalDataSourceDescription)) {
            ythrow TExternalSourceException() 
                << "Internal error. "
                << "Couldn't parse protobuf with external data source description";
        }

        TVector<TString> rez;

        for (auto a: AuthMethodsForCheck_) {
            if (a.WhenToUseIt(proto.GetProperties().GetProperties())) {
                rez.push_back(a.Auth);
            }
        }
        
        return rez;
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
        std::unordered_set<TString> validated;

        for (const auto& [key, value]: properties) {
            auto p = AvailableProperties_.find(key);

            if (AvailableProperties_.end() == p) {
                throw TExternalSourceException() << "Unsupported property: " << key;
            }

            // validate property value 
            if (p->second.NeedToValidate(properties)) {
                p->second.Validator(key, value);
            }

            validated.emplace(key);
        }

        // validate properties that has been left 
        for (const auto& [prop, validator]: AvailableProperties_) {
            if (validated.contains(prop)) {
                continue;
            }

            if (validator.NeedToValidate(properties)) {
                validator.Validator(prop, "");
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

}

TExternalSourceBuilder::TExternalSourceBuilder(const TString& name) 
    : Name_(name)
{
}

TExternalSourceBuilder& TExternalSourceBuilder::Auth(const TVector<TString>& authMethods, TCondition condition) {
    for (auto a: authMethods) {
        AuthMethodsForCheck_.push_back(TExternalSourceBuilder::TAuthHolder{a, condition});
    }

    return *this;
}

TExternalSourceBuilder& TExternalSourceBuilder::Property(TString name, TValidator validator, TCondition condition) {
    AvailableProperties_.emplace(name, TExternalSourceBuilder::TConditionalValidator{validator, condition});
    return *this;
} 

TExternalSourceBuilder& TExternalSourceBuilder::Properties(const TSet<TString>& availableProperties, TValidator validator, TCondition condition) {
    for (auto p: availableProperties) {
        Property(p, validator, condition);
    }

    return *this;
}

TExternalSourceBuilder& TExternalSourceBuilder::HostnamePattern(std::vector<TRegExMatch> pattern) {
    HostnamePatterns_.insert(
        HostnamePatterns_.end(), pattern.begin(), pattern.end());
    return *this;
}

IExternalSource::TPtr TExternalSourceBuilder::Build() {
    return MakeIntrusive<TValidatedExternalDataSource>(
        std::move(Name_), std::move(AuthMethodsForCheck_), std::move(AvailableProperties_), std::move(HostnamePatterns_));
}

TCondition HasSettingCondition(const TString& p, const TString& v) {
    auto fnc = [p, v](const ::google::protobuf::Map<TProtoStringType, TProtoStringType>& props) -> bool {
        auto itr = props.find(p);
        return props.end() != itr && v == itr->second;
    };

    return fnc;
}

TValidator RequiredValidator() {
    auto fnc = [](const TString& name, const TString& value){
        if (!value.empty()) {
            return;
        }

        throw TExternalSourceException() << "required property: " << name << " is not set";
    };

    return fnc;
}

TValidator IsInListValidator(const std::unordered_set<TString>& values, bool required) {
    auto join = [](const TString& a, const TString& b ){
        return a.empty() ? b : a + ',' + b; 
    };

    auto str = std::accumulate(values.begin(), values.end(), TString{}, join);

    auto f = [values, required, str](const TString& name, const TString& value){
        if (value.empty() && required) {
            throw TExternalSourceException() << " required property: " << name << " is not set";
        }

        if (value.empty()) {
            return;
        }

        if (!values.contains(value)) {
            throw TExternalSourceException() 
                << " property: " << name 
                << " has wrong value: " << value 
                << " allowed values: " << str;
        }
    };

    return f;
}

}