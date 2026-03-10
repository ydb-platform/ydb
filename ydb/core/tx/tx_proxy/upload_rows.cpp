#include "upload_rows.h"
#include "upload_rows_common_impl.h"

#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr {
namespace NTxProxy {

class TUploadRowsInternal : public TUploadRowsBase<NKikimrServices::TActivity::UPLOAD_ROWS_INTERNAL> {
public:
    TUploadRowsInternal(
        TActorId sender,
        const TString& database,
        const TString& table,
        std::shared_ptr<const TVector<std::pair<TString, Ydb::Type>>> types,
        std::shared_ptr<const TVector<std::pair<TSerializedCellVec, TString>>>&& rows,
        const TString& userSID,
        EUploadRowsMode mode,
        bool writeToPrivateTable,
        bool writeToIndexImplTable,
        ui64 cookie,
        TBackoff backoff)
        : TUploadRowsBase(std::move(rows), userSID)
        , Sender(sender)
        , Database(database)
        , Table(table)
        , ColumnTypes(types)
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

        Backoff = backoff;
    }

private:
    const TString& GetDatabase() const override {
        return Database;
    }

    const TString& GetTable() const override {
        return Table;
    }

    bool CheckAccess(TString&) override {
        return true;
    }

    void SendResult(const NActors::TActorContext& ctx, const Ydb::StatusIds::StatusCode& status) override {
        auto ev = new TEvTxUserProxy::TEvUploadRowsResponse(status, Issues);
        ctx.Send(Sender, ev, 0, Cookie);
    }

    bool ValidateTable(TString& errorMessage) override {
        if (GetTableKind() != NSchemeCache::TSchemeCacheNavigate::KindTable) {
            errorMessage = "Only the OLTP table is supported";
            return false;
        }
        return true;
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
    const TString Database;
    const TString Table;
    const std::shared_ptr<const TVector<std::pair<TString, Ydb::Type>>> ColumnTypes;
    const ui64 Cookie;

    NYql::TIssues Issues;
};

IActor* CreateUploadRowsInternal(const TActorId& sender,
    const TString& database,
    const TString& table,
    std::shared_ptr<const TVector<std::pair<TString, Ydb::Type>>> types,
    std::shared_ptr<const TVector<std::pair<TSerializedCellVec, TString>>> rows,
    EUploadRowsMode mode,
    bool writeToPrivateTable,
    bool writeToIndexImplTable,
    ui64 cookie,
    TBackoff backoff)
{
    return new TUploadRowsInternal(sender,
        database,
        table,
        types,
        std::move(rows),
        BUILTIN_ACL_NO_USER_SID,
        mode,
        writeToPrivateTable,
        writeToIndexImplTable,
        cookie,
        backoff);
}

} // namespace NTxProxy
} // namespace NKikimr
