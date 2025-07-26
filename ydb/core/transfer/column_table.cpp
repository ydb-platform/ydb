#include "events.h"
#include "logging.h"
#include "table_kind_state.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_columns.h>
//#include <ydb/core/tx/tx_proxy/upload_rows_common_impl.h>

namespace NKikimr::NReplication::NTransfer {

namespace {

class TTableUploader : public TActorBootstrapped<TTableUploader> {
    static constexpr size_t MaxRetries = 7;

public:
    TTableUploader(const TActorId& parentActor, const TScheme& scheme, std::unordered_map<TString, std::shared_ptr<arrow::RecordBatch>>&& data)
        : ParentActor(parentActor)
        , Scheme(scheme)
        , Data(std::move(data))
    {
    }

    void Bootstrap() {
        Become(&TTableUploader::StateWork);
        DoRequests();
    }

private:
    void DoRequests() {
        for (const auto& [tablePath, data] : Data) {
            DoUpload(tablePath, data);
        }
    }

    void DoUpload(const TString& tablePath, const std::shared_ptr<arrow::RecordBatch>& data) {
        auto cookie = ++Cookie;

        TActivationContext::AsActorContext().RegisterWithSameMailbox(
            NTxProxy::CreateUploadColumnsInternal(SelfId(), tablePath, Scheme.Types, data, cookie)
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
            CookieMapping.erase(ev->Cookie);

            if (Data.empty()) {
                ReplyOkAndDie();
            }

            return;
        }

        auto retry = ev->Get()->Status != Ydb::StatusIds::SCHEME_ERROR;
        if (retry && Retry < MaxRetries) {
            Schedule(TDuration::Seconds(1 << Retry), new NTransferPrivate::TEvRetryTable(tablePath));
            ++Retry;
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

            sFunc(TEvents::TEvPoison, PassAway);
            hFunc(NTransferPrivate::TEvRetryTable, Handle);
        }
    }

    void ReplyOkAndDie() {
        NYql::TIssues issues;
        Send(ParentActor, new NTransferPrivate::TEvWriteCompleeted(Ydb::StatusIds::SUCCESS, std::move(issues)));
        PassAway();
    }

    void ReplyErrorAndDie(const TString& error) {
        NYql::TIssues issues;
        issues.AddIssue(error);
        ReplyErrorAndDie(std::move(issues));
    }

    void ReplyErrorAndDie(NYql::TIssues&& issues) {
        Send(ParentActor, new NTransferPrivate::TEvWriteCompleeted(Ydb::StatusIds::INTERNAL_ERROR, std::move(issues)));
        PassAway();
    }


private:
    const TActorId ParentActor;
    const TScheme Scheme;
    // Table path -> Data
    std::unordered_map<TString, std::shared_ptr<arrow::RecordBatch>> Data;

    ui64 Cookie = 0;
    // Cookie -> Table path
    std::unordered_map<ui64, TString> CookieMapping;

    size_t Retry = 0;
};

}

class TColumnTableState : public ITableKindState {
public:
    TColumnTableState(
        const TActorId& selfId,
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate>& result
    )
        : ITableKindState(selfId, result)
    {
        NavigateResult.reset(result.Release());
        Path = JoinPath(NavigateResult->ResultSet.front().Path);
    }

    NKqp::IDataBatcherPtr CreateDataBatcher() override {
        return NKqp::CreateColumnDataBatcher(Scheme.ColumnsMetadata, Scheme.WriteIndex, nullptr, GetScheme().ReadIndex);
    }

    bool Flush() override {
        if (Batchers.empty() || !BatchSize()) {
            return false;
        }

        std::unordered_map<TString, std::shared_ptr<arrow::RecordBatch>> tableData;

        for (auto& [tablePath, batcher] : Batchers)  {
            NKqp::IDataBatchPtr batch = batcher->Build();

            auto data = reinterpret_pointer_cast<arrow::RecordBatch>(batch->ExtractBatch());
            Y_VERIFY(data);

            tableData[tablePath] = data;
        }

        TActivationContext::AsActorContext().RegisterWithSameMailbox(
            new TTableUploader(SelfId, GetScheme(), std::move(tableData))
        );

        return true;
    }

private:
    std::shared_ptr<const NSchemeCache::TSchemeCacheNavigate> NavigateResult;
    TString Path;

    std::shared_ptr<arrow::RecordBatch> Data;
};

std::unique_ptr<ITableKindState> CreateColumnTableState(const TActorId& selfId, TAutoPtr<NSchemeCache::TSchemeCacheNavigate>& result) {
    return std::make_unique<TColumnTableState>(selfId, result);
}

}
