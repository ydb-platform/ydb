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
        : UrlListers_(std::move(urlListers))
    {
    }

public:
    TVector<TUrlListEntry> ListUrl(const TString& url, const TString& tokenName) const override {
        auto urlWithoutParameters = SubstParameters(url, Parameters_, nullptr);
        auto preprocessedUrl = urlWithoutParameters;

        TString alias;
        if (UrlPreprocessing_) {
            std::tie(preprocessedUrl, alias) = UrlPreprocessing_->Preprocess(urlWithoutParameters);
        }

        TMaybe<TString> token;
        if (tokenName) {
            if (!Credentials_) {
                ythrow yexception() << "Missing credentials";
            }

            auto credential = Credentials_->FindCredential(tokenName);
            if (!credential) {
                ythrow yexception() << "Unknown token name: " << tokenName;
            }

            token = credential->Content;
        }

        if (!token && alias && Credentials_) {
            if (auto credential = Credentials_->FindCredential("default_" + alias)) {
                token = credential->Content;
            }
        }

        token = token.OrElse("");

        for (const auto& urlLister: UrlListers_) {
            if (urlLister->Accept(preprocessedUrl)) {
                return urlLister->ListUrl(preprocessedUrl, *token);
            }
        }

        ythrow yexception() << "Unsupported package url: " << url;
    }

public:
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
