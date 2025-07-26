#include "upload_rows.h"
#include "upload_rows_common_impl.h"

#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr {
namespace NTxProxy {

class TUploadRowsInternal : public TUploadRowsBase<NKikimrServices::TActivity::UPLOAD_ROWS_INTERNAL> {
public:
    TUploadRowsInternal(
        TActorId sender,
        const TString& table,
        std::shared_ptr<TVector<std::pair<TString, Ydb::Type>>> types,
        std::shared_ptr<TVector<std::pair<TSerializedCellVec, TString>>> rows,
        EUploadRowsMode mode,
        bool writeToPrivateTable,
        bool writeToIndexImplTable,
        ui64 cookie)
        : Sender(sender)
        , Table(table)
        , ColumnTypes(types)
        , Rows(rows)
        , Cookie(cookie)
    {
        AllowWriteToPrivateTable = writeToPrivateTable;
        AllowWriteToIndexImplTable = writeToIndexImplTable;

        switch (mode) {
            case EUploadRowsMode::Normal:
                // nothing
                break;
            case EUploadRowsMode::WriteToTableShadow:
                WriteToTableShadow = true;
                break;
            case EUploadRowsMode::UpsertIfExists:
                UpsertIfExists = true;
                break;
        }
    }

private:
    TString GetDatabase()override {
        return TString();
    }

    const TString& GetTable() override {
        return Table;
    }

    const TVector<std::pair<TSerializedCellVec, TString>>& GetRows() const override {
        return *Rows;
    }

    bool CheckAccess(TString&) override {
        return true;
    }

    void SendResult(const NActors::TActorContext& ctx, const Ydb::StatusIds::StatusCode& status) override {
        auto ev = new TEvTxUserProxy::TEvUploadRowsResponse(status, Issues);
        ctx.Send(Sender, ev, 0, Cookie);
    }

    void RaiseIssue(const NYql::TIssue& issue) override {
        Issues.AddIssue(issue);
    }

    bool ExtractRows(TString&) override {
        return true;
    }

    bool ExtractBatch(TString& errorMessage) override {
        errorMessage = "Not supported";
        return false;
    }

    TConclusion<TVector<std::pair<TString, Ydb::Type>>> GetRequestColumns() const override {
        return *ColumnTypes;
    }

private:
    const TActorId Sender;
    const TString Table;
    const std::shared_ptr<TVector<std::pair<TString, Ydb::Type>>> ColumnTypes;
    const std::shared_ptr<TVector<std::pair<TSerializedCellVec, TString>>> Rows;
    const ui64 Cookie;

    NYql::TIssues Issues;
};

IActor* CreateUploadRowsInternal(const TActorId& sender,
    const TString& table,
    std::shared_ptr<TVector<std::pair<TString, Ydb::Type>>> types,
    std::shared_ptr<TVector<std::pair<TSerializedCellVec, TString>>> rows,
    EUploadRowsMode mode,
    bool writeToPrivateTable,
    bool writeToIndexImplTable,
    ui64 cookie)
{
    return new TUploadRowsInternal(sender,
        table,
        types,
        rows,
        mode,
        writeToPrivateTable,
        writeToIndexImplTable,
        cookie);
}

} // namespace NTxProxy
} // namespace NKikimr
