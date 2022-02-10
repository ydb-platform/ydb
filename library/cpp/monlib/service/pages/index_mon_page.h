#pragma once

#include "mon_page.h"

namespace NMonitoring {
    struct TIndexMonPage: public IMonPage { 
        TMutex Mtx; 
        typedef TVector<TMonPagePtr> TPages; 
        TPages Pages; 
        typedef THashMap<TString, TMonPagePtr> TPagesByPath; 
        TPagesByPath PagesByPath; 

        TIndexMonPage(const TString& path, const TString& title) 
            : IMonPage(path, title) 
        { 
        } 

        ~TIndexMonPage() override { 
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

        void SortPages(); 
        void ClearPages();
    }; 

} 
