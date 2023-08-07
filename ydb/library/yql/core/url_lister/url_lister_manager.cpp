#include "url_lister_manager.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/utils/fetch/fetch.h>


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
    TVector<TUrlListEntry> ListUrl(const THttpURL& url, const TString& tokenName) const override {
        auto urlString = SubstParameters(url.PrintS(), Parameters, nullptr);

        if (UrlPreprocessing) {
            auto [preprocessedUrlString, alias] = UrlPreprocessing->Preprocess(urlString);
            urlString = preprocessedUrlString;
        }

        auto patchedUrl = ParseURL(urlString);

        TString token;
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

        for (const auto& urlLister: UrlListers) {
            if (urlLister->Accept(patchedUrl)) {
                return urlLister->ListUrl(patchedUrl, token);
            }
        }

        ythrow yexception() << "Unsupported package url: " << urlString;
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
