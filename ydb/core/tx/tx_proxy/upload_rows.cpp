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
        bool writeToPrivateTable)
        : Sender(sender)
        , Table(table)
        , ColumnTypes(types)
        , Rows(rows)
    {
        AllowWriteToPrivateTable = writeToPrivateTable;

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
        ctx.Send(Sender, ev);
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

    TVector<std::pair<TString, Ydb::Type>> GetRequestColumns(TString& errorMessage) const override {
        Y_UNUSED(errorMessage);
        return *ColumnTypes;
    }

private:
    const TActorId Sender;
    const TString Table;
    const std::shared_ptr<TVector<std::pair<TString, Ydb::Type>>> ColumnTypes;
    const std::shared_ptr<TVector<std::pair<TSerializedCellVec, TString>>> Rows;

    NYql::TIssues Issues;
};

IActor* CreateUploadRowsInternal(const TActorId& sender,
    const TString& table,
    std::shared_ptr<TVector<std::pair<TString, Ydb::Type>>> types,
    std::shared_ptr<TVector<std::pair<TSerializedCellVec, TString>>> rows,
    EUploadRowsMode mode,
    bool writeToPrivateTable)
{
    return new TUploadRowsInternal(sender,
        table,
        types,
        rows,
        mode,
        writeToPrivateTable);
}

} // namespace NTxProxy
} // namespace NKikimr
