#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>


namespace NYql {

enum class EUrlListEntryType {
    FILE = 1,
    DIRECTORY = 2,
};


struct TUrlListEntry {
    TString Url;
    TString Name;
    EUrlListEntryType Type;
};


class IUrlLister : public TThrRefBase {
public:
    virtual bool Accept(const TString& url) const = 0;
    virtual TVector<TUrlListEntry> ListUrl(const TString& url, const TString& token) const = 0;
};
using IUrlListerPtr = TIntrusivePtr<IUrlLister>;

}
