#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <util/generic/map.h>
#include <util/generic/string.h>

#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NKikimr::NExternalSource {

struct TExternalSourceException: public yexception {
};

namespace NAuth {

struct TNone {
    static constexpr std::string_view Method = "NONE";
};

struct TAws {
    static constexpr std::string_view Method = "AWS";

    TAws(const TString& accessKey, const TString& secretAccessKey, const TString& region)
        : AccessKey{accessKey}
        , SecretAccessKey{secretAccessKey}
        , Region{region}
    {}

    TString AccessKey;
    TString SecretAccessKey;
    TString Region;
};

struct TServiceAccount {
    static constexpr std::string_view Method = "SERVICE_ACCOUNT";

    TServiceAccount(TString serviceAccountId, TString serviceAccountIdSignature)
        : ServiceAccountId{std::move(serviceAccountId)}
        , ServiceAccountIdSignature{std::move(serviceAccountIdSignature)}
    {}

    TString ServiceAccountId;
    TString ServiceAccountIdSignature;
};

using TAuth = std::variant<TNone, TServiceAccount, TAws>;

std::string_view GetMethod(const TAuth& auth);

inline TAuth MakeNone() {
    return TAuth{std::in_place_type_t<TNone>{}};
}

inline TAuth MakeServiceAccount(const TString& serviceAccountId, const TString& serviceAccountIdSignature) {
    return TAuth{std::in_place_type_t<TServiceAccount>{}, serviceAccountId, serviceAccountIdSignature};
}

inline TAuth MakeAws(const TString& accessKey, const TString& secretAccessKey, const TString& region) {
    return TAuth{std::in_place_type_t<TAws>{}, accessKey, secretAccessKey, region};
}
}

using TAuth = NAuth::TAuth;

struct TMetadata {
    bool Changed = false;
    TString TableLocation;
    TString DataSourceLocation;
    TString DataSourcePath;
    TString Type;

    THashMap<TString, TString> Attributes;

    TAuth Auth;

    NKikimrExternalSources::TSchema Schema;
};

struct IExternalSource : public TThrRefBase {
    using TPtr = TIntrusivePtr<IExternalSource>;

    /*
        Packs TSchema, TGeneral into some string in arbitrary
        format: proto, json, text, and others. The output returns a
        string called content. Further, this string will be stored inside.
        After that, it is passed to the GetParameters method.
        Can throw an exception in case of an error.
    */
    virtual TString Pack(const NKikimrExternalSources::TSchema& schema,
                         const NKikimrExternalSources::TGeneral& general) const = 0;

    /*
        If this source supports external table than this method will return true
    */
    virtual bool HasExternalTable() const = 0;

    /*
        The name of the data source that is used inside the
        implementation during the read/write phase. Must match provider name.
    */
    virtual TString GetName() const = 0;

    /*
        List of auth methods supported by the source
    */
    virtual TVector<TString> GetAuthMethods() const = 0;

    /*
        At the input, a string with the name of the content is passed,
        which is obtained from the Pack method and returns a list of
        parameters that will be put in the AST of the source. Also,
        this data will be displayed in the viewer.
        Can throw an exception in case of an error
    */
    virtual TMap<TString, TVector<TString>> GetParameters(const TString& content) const = 0;

    /*
        Validation of external data source properties.
        If an error occurs, an exception is thrown.
    */
    virtual void ValidateExternalDataSource(const TString& externalDataSourceDescription) const = 0;

    /*
        Retrieve additional metadata from runtime data, enrich provided metadata
    */
    virtual NThreading::TFuture<std::shared_ptr<TMetadata>> LoadDynamicMetadata(std::shared_ptr<TMetadata> meta) = 0;

    /*
        A method that should tell whether there is an implementation
        of the previous method.
    */
    virtual bool CanLoadDynamicMetadata() const = 0;
};

}
