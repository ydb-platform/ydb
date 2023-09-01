#pragma once

#include <util/generic/map.h>
#include <util/generic/string.h>

#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NKikimr::NExternalSource {

struct TExternalSourceException: public yexception {
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
    virtual void ValidateProperties(const TString& properties) const = 0;
};

}
