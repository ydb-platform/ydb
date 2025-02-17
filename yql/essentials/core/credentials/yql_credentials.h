#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/ptr.h>
#include <util/generic/hash.h>

#include <functional>
#include <unordered_set>

namespace NYql {

struct TCredential {
    const TString Category;
    const TString Subcategory;
    const TString Content;

    TCredential(const TString& category, const TString& subcategory, const TString& content)
        : Category(category)
        , Subcategory(subcategory)
        , Content(content)
    {
    }
};

struct TUserCredentials {
    TString OauthToken;
    TString BlackboxSessionIdCookie;
};

class TCredentials: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TCredentials>;

    TCredentials() = default;
    ~TCredentials() = default;

    void AddCredential(const TString& alias, const TCredential& cred);
    void SetUserCredentials(const TUserCredentials& userCredentials) {
        UserCredentials_ = userCredentials;
    }
    
    void SetGroups(std::unordered_set<TString>&& groups) {
        Groups_ = std::move(groups);
    }

    const std::unordered_set<TString>& GetGroups() const {
        return Groups_;
    }

    const TCredential* FindCredential(const TStringBuf& name) const;
    TString FindCredentialContent(const TStringBuf& name1, const TStringBuf& name2, const TString& defaultContent) const;

    const TUserCredentials& GetUserCredentials() const  {
        return UserCredentials_;
    }
    void ForEach(const std::function<void(const TString, const TCredential&)>& callback) const;

private:
    THashMap<TString, TCredential> CredentialTable_;
    TUserCredentials UserCredentials_;
    std::unordered_set<TString> Groups_;
};

} // namespace NYql
