#pragma once

#include "events.h"
#include "logging.h"
#include "scheme.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NReplication::NTransfer {

template<typename TData>
class TTableUploader : public TActorBootstrapped<TTableUploader<TData>> {
    using TThis = TTableUploader<TData>;
    using TBase = TActorBootstrapped<TTableUploader<TData>>;

    static constexpr size_t MaxSchemeRetries = 3;

public:
    TTableUploader(const TActorId& parentActor,
        const TString& database, const TScheme::TPtr& scheme,
        std::unordered_map<TString, std::shared_ptr<TData>>&& data
    )
        : ParentActor(parentActor)
        , Database(database)
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

    IActor* CreateUploaderInternal(const TString& database, const TString& tablePath, const std::shared_ptr<TData>& data, ui64 cookie);

    void DoUpload(const TString& tablePath, const std::shared_ptr<TData>& data) {
        auto cookie = ++Cookie;

        auto actorId = TActivationContext::AsActorContext().RegisterWithSameMailbox(
            CreateUploaderInternal(Database, tablePath, data, cookie)
        );
        CookieMapping[cookie] = {tablePath, actorId};
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

        auto& tablePath = it->second.first;
        const auto status = ev->Get()->Status;

        if (status == Ydb::StatusIds::SUCCESS) {
            Data.erase(tablePath);
            if (Data.empty()) {
                return ReplyOkAndDie();
            }

            CookieMapping.erase(ev->Cookie);
            return;
        }

        const auto schemeError = status == Ydb::StatusIds::SCHEME_ERROR
            || status == Ydb::StatusIds::BAD_REQUEST
            || status == Ydb::StatusIds::UNAUTHORIZED;

        auto& retry = Retries[tablePath];
        auto withRetry = retry.Backoff.HasMore() && retry.SchemeCount < MaxSchemeRetries;
        if (withRetry) {
            LOG_D("Schedule retry: table=" << tablePath
                << ", iteration=" << retry.Backoff.GetIteration()
                << ", error=" << status << " " << ev->Get()->Issues.ToOneLineString());

            TThis::Schedule(retry.Backoff.Next(), new NTransferPrivate::TEvRetryTable(tablePath));
            if (schemeError) {
                ++retry.SchemeCount;
            } else {
                retry.SchemeCount = 0;
            }
            CookieMapping.erase(ev->Cookie);
            return;
        }

        ReplyErrorAndDie(status, std::move(ev->Get()->Issues));
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

    void PassAway() override {
        for (auto& [_, v] : CookieMapping) {
            TThis::Send(v.second, new TEvents::TEvPoison());
        }

        TBase::PassAway();
    }

    void ReplyOkAndDie() {
        NYql::TIssues issues;
        TThis::Send(ParentActor, new NTransferPrivate::TEvWriteCompleeted(Ydb::StatusIds::SUCCESS, std::move(issues)));
        TThis::PassAway();
    }

    void ReplyErrorAndDie(const TString& error) {
        NYql::TIssues issues;
        issues.AddIssue(error);
        ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, std::move(issues));
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        LOG_E("Upload error: error=" << status << " " << issues.ToOneLineString());

        TThis::Send(ParentActor, new NTransferPrivate::TEvWriteCompleeted(status, std::move(issues)));
        TThis::PassAway();
    }

private:
    const TActorId ParentActor;
    const TString Database;
    const TScheme::TPtr Scheme;
    // Table path -> Data
    std::unordered_map<TString, std::shared_ptr<TData>> Data;

    ui64 Cookie = 0;
    // Cookie -> <Table path, Actor>
    std::unordered_map<ui64, std::pair<TString, TActorId>> CookieMapping;

    struct Retry {
        TBackoff Backoff = TBackoff(TDuration::Seconds(1), TDuration::Minutes(1));
        size_t SchemeCount = 0;
    };
    std::unordered_map<TString, Retry> Retries;
};


} // namespace NKikimr::NReplication::NTransfer
