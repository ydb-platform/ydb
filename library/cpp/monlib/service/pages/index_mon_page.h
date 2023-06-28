#pragma once

#include "mon_page.h"

#include <list>

namespace NMonitoring {
    struct TIndexMonPage: public IMonPage {
        TMutex Mtx;
        using TPages = std::list<TMonPagePtr>;
        TPages Pages; // a list of pages to maintain specific order
        using TPagesByPath = THashMap<TString, TPages::iterator>;
        TPagesByPath PagesByPath;

        TIndexMonPage(const TString& path, const TString& title)
            : IMonPage(path, title)
        {
        }

        ~TIndexMonPage() override {
        }

        bool IsIndex() const override {
            return true;
        }

        void Output(IMonHttpRequest& request) override;
        void OutputIndexPage(IMonHttpRequest& request);
        virtual void OutputIndex(IOutputStream& out, bool pathEndsWithSlash);
        virtual void OutputCommonJsCss(IOutputStream& out);
        void OutputHead(IOutputStream& out);
        void OutputBody(IMonHttpRequest& out);

        void Register(TMonPagePtr page);
        TIndexMonPage* RegisterIndexPage(const TString& path, const TString& title);

        IMonPage* FindPage(const TString& relativePath);
        TIndexMonPage* FindIndexPage(const TString& relativePath);
        IMonPage* FindPageByAbsolutePath(const TString& absolutePath);

        void SortPages();
        void ClearPages();
    };

}
