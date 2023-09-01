#include "external_data_source.h"
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NExternalSource {

namespace {

struct TExternalDataSource : public IExternalSource {
    TExternalDataSource(const TString& name, const TVector<TString>& authMethods, const TSet<TString>& availableProperties)
        : Name(name)
        , AuthMethods(authMethods)
        , AvailableProperties(availableProperties)
    {}
    
    virtual TString Pack(const NKikimrExternalSources::TSchema&,
                         const NKikimrExternalSources::TGeneral&) const override {
        ythrow TExternalSourceException() << "Only external table supports pack operation";
    }

    virtual TString GetName() const override {
        return Name;
    }

    virtual bool HasExternalTable() const override {
        return false;
    }

    virtual TVector<TString> GetAuthMethods() const override {
        return AuthMethods;
    }

    virtual TMap<TString, TVector<TString>> GetParameters(const TString&) const override {
        ythrow TExternalSourceException() << "Only external table supports parameters";
    }

    virtual void ValidateProperties(const TString& properties) const override {
        NKikimrSchemeOp::TExternalDataSourceProperties proto;
        if (!proto.ParseFromString(properties)) {
            ythrow TExternalSourceException() << "Internal error. Couldn't parse protobuf with properties for external data source";
        }

        for (const auto& [key, value]: proto.GetProperties()) {
            if (AvailableProperties.contains(key)) {
                continue;
            }
            ythrow TExternalSourceException() << "Unsupported property: " << key;
        }
    }
private:
    const TString Name;
    const TVector<TString> AuthMethods;
    const TSet<TString> AvailableProperties;
};

}

IExternalSource::TPtr CreateExternalDataSource(const TString& name, const TVector<TString>& authMethods, const TSet<TString>& availableProperties) {
    return MakeIntrusive<TExternalDataSource>(name, authMethods, availableProperties);
}

}
