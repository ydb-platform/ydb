#include "url_lister_manager.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/utils/fetch/fetch.h>
#include <yql/essentials/utils/log/log.h>

#include <util/generic/maybe.h>
#include <util/generic/ylimits.h>

#include <tuple>


namespace NYql::NPrivate {

class TUrlListerManager: public IUrlListerManager {
public:
    TUrlListerManager(
        TVector<IUrlListerPtr> urlListers
    )
        : UrlListers_(std::move(urlListers))
    {}

    TVector<TUrlListEntry> ListUrl(const TString& url, const TString& tokenName) const override {
        auto [preprocessedUrl, alias] = GetPreparedUrlAndAlias(url);

        TString token = GetToken(tokenName, alias);

        for (const auto& urlLister: UrlListers_) {
            if (urlLister->Accept(preprocessedUrl)) {
                return urlLister->ListUrl(preprocessedUrl, token);
            }
        }

        throw yexception() << "Unsupported url for listing content: " << url;
    }

    TVector<TUrlListEntry> ListUrlRecursive(const TString& url, const TString& tokenName, const TString& separator, ui32 foldersLimit) const override {
        auto [preprocessedUrl, alias] = GetPreparedUrlAndAlias(url);

        TString token = GetToken(tokenName, alias);

        for (const auto& urlLister: UrlListers_) {
            if (urlLister->Accept(preprocessedUrl)) {
                return ListRecursive(urlLister, preprocessedUrl, token, separator,
                                    foldersLimit == 0 ? Max<ui32>() : foldersLimit
                                );
            }
        }
        throw yexception() << "Unsupported url for listing content: " << url;
    }

    IUrlListerManagerPtr Clone() const override {
        auto clone = MakeUrlListerManager(UrlListers_);

        clone->SetCredentials(Credentials_);
        clone->SetUrlPreprocessing(UrlPreprocessing_);

        if (Parameters_) {
            clone->SetParameters(*Parameters_);
        }

        return clone;
    }

    void SetCredentials(TCredentials::TPtr credentials) override {
        Credentials_ = std::move(credentials);
    }

    void SetUrlPreprocessing(IUrlPreprocessing::TPtr urlPreprocessing) override {
        UrlPreprocessing_ = std::move(urlPreprocessing);
    }

    void SetParameters(const NYT::TNode& parameters) override {
        Parameters_ = parameters;
    }

private:
    std::pair<TString, TString> GetPreparedUrlAndAlias(const TString& url) const {
        TString urlWithoutParameters = SubstParameters(url, Parameters_, nullptr);
        TString preprocessedUrl = urlWithoutParameters;

        TString alias;
        if (UrlPreprocessing_) {
            std::tie(preprocessedUrl, alias) = UrlPreprocessing_->Preprocess(urlWithoutParameters);
        }

        return {preprocessedUrl, alias};
    }

    TString GetToken(const TString& tokenName, const TString& alias) const {
        TMaybe<TString> token;

        if (tokenName) {
            if (!Credentials_) {
                throw yexception() << "Missing credentials";
            }

            auto credential = Credentials_->FindCredential(tokenName);
            if (!credential) {
                throw yexception() << "Unknown token name: " << tokenName;
            }

            token = credential->Content;
        }

        if (!token && alias && Credentials_) {
            if (auto credential = Credentials_->FindCredential("default_" + alias)) {
                token = credential->Content;
            }
        }

        return *token.OrElse("");
    }

    TVector<TUrlListEntry> ListRecursive(const IUrlListerPtr& urlLister, const TString& url, const TString& token, const TString& separator, ui32 foldersLimit) const {
        TVector<TUrlListEntry> urlsQueue = {{url, "", EUrlListEntryType::DIRECTORY}};
        THashSet<TString> visitedUrls;
        TVector<TUrlListEntry> result;

        while (urlsQueue) {
            TUrlListEntry currentEntry = std::move(urlsQueue.back());
            if (!visitedUrls.insert(currentEntry.Url).second) {
                continue;
            }

            urlsQueue.pop_back();
            YQL_LOG(TRACE) << "UrlListManager listing url: " << currentEntry.Url;

            auto [preprocessedUrl, alias] = GetPreparedUrlAndAlias(currentEntry.Url);

            Y_ENSURE(urlLister->Accept(preprocessedUrl), "URL: '" << currentEntry.Url << "' is not supported by the lister");

            TVector<TUrlListEntry> subEntries;
            try {
                subEntries = urlLister->ListUrl(preprocessedUrl, token);
            } catch (const std::exception& e) {
                throw yexception() << "UrlListerManager: failed to list URL '" << currentEntry.Url << "', details: " << e.what();
            }
            for (auto& entry : subEntries) {
                TUrlListEntry newEntry = {
                        entry.Url,
                        TStringBuilder() << currentEntry.Name << separator << entry.Name,
                        entry.Type
                    };
                if (entry.Type == EUrlListEntryType::DIRECTORY) {
                    urlsQueue.push_back(std::move(newEntry));
                    if (--foldersLimit == 0) {
                        throw yexception() << "Maximum subfolders limit reached while recursively listing URLs";
                    }
                } else {
                    result.push_back(std::move(newEntry));
                }
            }
        }

        return result;
    }

    TVector<IUrlListerPtr> UrlListers_;

    TCredentials::TPtr Credentials_;
    IUrlPreprocessing::TPtr UrlPreprocessing_;
    TMaybe<NYT::TNode> Parameters_;
};

}

namespace NYql {

IUrlListerManagerPtr MakeUrlListerManager(
    TVector<IUrlListerPtr> urlListers
) {
    return MakeIntrusive<NPrivate::TUrlListerManager>(std::move(urlListers));
}

}
