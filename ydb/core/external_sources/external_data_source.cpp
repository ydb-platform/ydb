#include "external_data_source.h"

namespace NKikimr::NExternalSource {

namespace {

struct TExternalDataSource : public IExternalSource {
    TExternalDataSource(const TString& name, const TVector<TString>& authMethods)
        : Name(name)
        , AuthMethods(authMethods)
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
private:
    const TString Name;
    const TVector<TString> AuthMethods;
};

}

IExternalSource::TPtr CreateExternalDataSource(const TString& name, const TVector<TString>& authMethods) {
    return MakeIntrusive<TExternalDataSource>(name, authMethods);
}

}
