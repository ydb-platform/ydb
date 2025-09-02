#pragma once

#include "url_lister.h"

#include <library/cpp/yson/node/node.h>

#include <yql/essentials/core/credentials/yql_credentials.h>
#include <yql/essentials/core/url_preprocessing/interface/url_preprocessing.h>

#include <util/generic/ptr.h>


namespace NYql {

class IUrlListerManager: public TThrRefBase {
public:
    virtual TVector<TUrlListEntry> ListUrl(const TString& url, const TString& tokenName) const = 0;
    virtual TVector<TUrlListEntry> ListUrlRecursive(const TString& url, const TString& tokenName, const TString& separator, ui32 foldersLimit) const = 0;

public:
    virtual TIntrusivePtr<IUrlListerManager> Clone() const = 0;

    virtual void SetCredentials(TCredentials::TPtr credentials) = 0;
    virtual void SetUrlPreprocessing(IUrlPreprocessing::TPtr urlPreprocessing) = 0;
    virtual void SetParameters(const NYT::TNode& parameters) = 0;
};

using IUrlListerManagerPtr = TIntrusivePtr<IUrlListerManager>;

}
