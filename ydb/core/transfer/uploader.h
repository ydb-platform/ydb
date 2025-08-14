#pragma once

#include "events.h"
#include "logging.h"
#include "scheme.h"

#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr::NReplication::NTransfer {

template<typename TData>
class TTableUploader : public TActorBootstrapped<TTableUploader<TData>> {
    using TThis = TTableUploader<TData>;

    static constexpr size_t MaxRetries = 9;

public:
    TTableUploader(const TActorId& parentActor, const TScheme::TPtr& scheme, std::unordered_map<TString, std::shared_ptr<TData>>&& data)
        : ParentActor(parentActor)
        , Scheme(scheme)
        , Data(std::move(data))
    {
    }

    void Bootstrap() {
        TThis::Become(&TThis::StateWork);
        DoRequests();
    }

private:
    void DoRequests() {
        for (const auto& [tablePath, data] : Data) {
            DoUpload(tablePath, data);
        }
    }

    IActor* CreateUploaderInternal(const TString& tablePath, const std::shared_ptr<TData>& data, ui64 cookie);

    void DoUpload(const TString& tablePath, const std::shared_ptr<TData>& data) {
        auto cookie = ++Cookie;

        TActivationContext::AsActorContext().RegisterWithSameMailbox(
            CreateUploaderInternal(tablePath, data, cookie)
        );
        CookieMapping[cookie] = tablePath;
    }

    std::string GetLogPrefix() const {
        return "RowTableUploader: ";
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev) {
        auto it = CookieMapping.find(ev->Cookie);
        if (it == CookieMapping.end()) {
            LOG_W("Processed unknown cookie " << ev->Cookie);
            return;
        }

        auto& tablePath = it->second;

        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            Data.erase(tablePath);
            if (Data.empty()) {
                return ReplyOkAndDie();
            }

            CookieMapping.erase(ev->Cookie);
            return;
        }

        auto withRetry = ev->Get()->Status != Ydb::StatusIds::SCHEME_ERROR;
        auto& retry = Retries[tablePath];
        if (withRetry && retry < MaxRetries) {
            size_t timeout = size_t(1000) << retry;
            TThis::Schedule(TDuration::MilliSeconds(timeout + RandomNumber<size_t>(timeout >> 2)), new NTransferPrivate::TEvRetryTable(tablePath));
            ++retry;
            CookieMapping.erase(ev->Cookie);
            return;
        }

        ReplyErrorAndDie(std::move(ev->Get()->Issues));
    }

    void Handle(NTransferPrivate::TEvRetryTable::TPtr& ev) {
        auto& tablePath = ev->Get()->TablePath;
        
        auto it = Data.find(tablePath);
        if (it == Data.end()) {
            return ReplyErrorAndDie(TStringBuilder() << "Unexpected retry for table '" << tablePath << "'");
        }

        DoUpload(tablePath, it->second);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);

            sFunc(TEvents::TEvPoison, TThis::PassAway);
            hFunc(NTransferPrivate::TEvRetryTable, Handle);
        }
    }

    void ReplyOkAndDie() {
        NYql::TIssues issues;
        TThis::Send(ParentActor, new NTransferPrivate::TEvWriteCompleeted(Ydb::StatusIds::SUCCESS, std::move(issues)));
        TThis::PassAway();
    }

    void ReplyErrorAndDie(const TString& error) {
        NYql::TIssues issues;
        issues.AddIssue(error);
        ReplyErrorAndDie(std::move(issues));
    }

    void ReplyErrorAndDie(NYql::TIssues&& issues) {
        TThis::Send(ParentActor, new NTransferPrivate::TEvWriteCompleeted(Ydb::StatusIds::INTERNAL_ERROR, std::move(issues)));
        TThis::PassAway();
    }

private:
    const TActorId ParentActor;
    const TScheme::TPtr Scheme;
    // Table path -> Data
    std::unordered_map<TString, std::shared_ptr<TData>> Data;

    ui64 Cookie = 0;
    // Cookie -> Table path
    std::unordered_map<ui64, TString> CookieMapping;
    std::unordered_map<TString, size_t> Retries;
};


} // namespace NKikimr::NReplication::NTransfer
