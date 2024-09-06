#include "external_data_source.h"
#include "validation_functions.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NExternalSource {

namespace {

struct TExternalDataSource : public IExternalSource {
    TExternalDataSource(const TString& name, const TVector<TString>& authMethods, const TSet<TString>& availableProperties, const std::vector<TRegExMatch>& hostnamePatterns)
        : Name(name)
        , AuthMethods(authMethods)
        , AvailableProperties(availableProperties)
        , HostnamePatterns(hostnamePatterns)
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

    bool DataSourceMustHaveDataBaseName(const TProtoStringType& sourceType) const {
        return IsIn({"Greenplum", "PostgreSQL", "MySQL", "MsSQLServer", "ClickHouse"}, sourceType);
    }

    virtual void ValidateExternalDataSource(const TString& externalDataSourceDescription) const override {
        NKikimrSchemeOp::TExternalDataSourceDescription proto;
        if (!proto.ParseFromString(externalDataSourceDescription)) {
            ythrow TExternalSourceException() << "Internal error. Couldn't parse protobuf with external data source description";
        }

        for (const auto& [key, value]: proto.GetProperties().GetProperties()) {
            if (AvailableProperties.contains(key)) {
                continue;
            }
            ythrow TExternalSourceException() << "Unsupported property: " << key;
        }

        if (DataSourceMustHaveDataBaseName(proto.GetSourceType()) && !proto.GetProperties().GetProperties().contains("database_name")) {
            ythrow TExternalSourceException() << proto.GetSourceType() << " source must provide database_name";
        }

        // oracle must have property service_name
        if (proto.GetSourceType() == "Oracle" && !proto.GetProperties().GetProperties().contains("service_name")) {
            ythrow TExternalSourceException() << proto.GetSourceType() << " source must provide service_name";
        }

        ValidateHostname(HostnamePatterns, proto.GetLocation());
    }

    virtual NThreading::TFuture<std::shared_ptr<TMetadata>> LoadDynamicMetadata(std::shared_ptr<TMetadata> meta) override {
        return NThreading::MakeFuture(std::move(meta));
    }

    virtual bool CanLoadDynamicMetadata() const override {
        return false;
    }

private:
    const TString Name;
    const TVector<TString> AuthMethods;
    const TSet<TString> AvailableProperties;
    const std::vector<TRegExMatch> HostnamePatterns;
};

}

IExternalSource::TPtr CreateExternalDataSource(const TString& name, const TVector<TString>& authMethods, const TSet<TString>& availableProperties, const std::vector<TRegExMatch>& hostnamePatterns) {
    return MakeIntrusive<TExternalDataSource>(name, authMethods, availableProperties, hostnamePatterns);
}

}
