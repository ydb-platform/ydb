#include "url_lister_manager.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/utils/fetch/fetch.h>

#include <util/generic/maybe.h>

#include <tuple>


namespace NYql::NPrivate {

class TUrlListerManager: public IUrlListerManager {
public:
    TUrlListerManager(
        TVector<IUrlListerPtr> urlListers
    )
        : UrlListers(std::move(urlListers))
    {
    }

public:
    TVector<TUrlListEntry> ListUrl(const TString& url, const TString& tokenName) const override {
        auto urlWithoutParameters = SubstParameters(url, Parameters, nullptr);
        auto preprocessedUrl = urlWithoutParameters;

        TString alias;
        if (UrlPreprocessing) {
            std::tie(preprocessedUrl, alias) = UrlPreprocessing->Preprocess(urlWithoutParameters);
        }

        TMaybe<TString> token;
        if (tokenName) {
            if (!Credentials) {
                ythrow yexception() << "Missing credentials";
            }

            auto credential = Credentials->FindCredential(tokenName);
            if (!credential) {
                ythrow yexception() << "Unknown token name: " << tokenName;
            }

            token = credential->Content;
        }

        if (!token && alias && Credentials) {
            if (auto credential = Credentials->FindCredential("default_" + alias)) {
                token = credential->Content;
            }
        }

        token = token.OrElse("");

        for (const auto& urlLister: UrlListers) {
            if (urlLister->Accept(preprocessedUrl)) {
                return urlLister->ListUrl(preprocessedUrl, *token);
            }
        }

        ythrow yexception() << "Unsupported package url: " << url;
    }

public:
    IUrlListerManagerPtr Clone() const override {
        auto clone = MakeUrlListerManager(UrlListers);

        clone->SetCredentials(Credentials);
        clone->SetUrlPreprocessing(UrlPreprocessing);

        if (Parameters) {
            clone->SetParameters(*Parameters);
        }

        return clone;
    }

    void SetCredentials(TCredentials::TPtr credentials) override {
        Credentials = std::move(credentials);
    }

    void SetUrlPreprocessing(IUrlPreprocessing::TPtr urlPreprocessing) override {
        UrlPreprocessing = std::move(urlPreprocessing);
    }

    void SetParameters(const NYT::TNode& parameters) override {
        Parameters = parameters;
    }

private:
    TVector<IUrlListerPtr> UrlListers;

    TCredentials::TPtr Credentials;
    IUrlPreprocessing::TPtr UrlPreprocessing;
    TMaybe<NYT::TNode> Parameters;
};

}

namespace NYql {

IUrlListerManagerPtr MakeUrlListerManager(
    TVector<IUrlListerPtr> urlListers
) {
    return MakeIntrusive<NPrivate::TUrlListerManager>(std::move(urlListers));
}

}
