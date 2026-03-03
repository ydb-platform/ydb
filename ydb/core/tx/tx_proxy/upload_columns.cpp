#include "upload_rows.h"
#include "upload_rows_common_impl.h"

#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr {
namespace NTxProxy {

class TUploadColumnsInternal : public TUploadRowsBase<NKikimrServices::TActivity::UPLOAD_ROWS_INTERNAL> {
public:
    TUploadColumnsInternal(
        TActorId sender,
        const TString& database,
        const TString& table,
        std::shared_ptr<TUploadTypes>& types,
        std::shared_ptr<arrow::RecordBatch>& data,
        ui64 cookie)
        : TUploadRowsBase(std::make_shared<TVector<std::pair<TSerializedCellVec, TString>>>(), BUILTIN_ACL_NO_USER_SID)
        , Sender(sender)
        , Database(database)
        , Table(table)
        , ColumnTypes(types)
        , Data(data)
        , Cookie(cookie)
    {
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
        if (GetTableKind() != NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
            errorMessage = "Only the OLAP table is supported";
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

    bool ExtractBatch(TString&) override {
        Batch = Data;
        return true;
    }

    TConclusion<TVector<std::pair<TString, Ydb::Type>>> GetRequestColumns() const override {
        return *ColumnTypes;
    }

private:
    const TActorId Sender;
    const TString Database;
    const TString Table;
    const std::shared_ptr<TVector<std::pair<TString, Ydb::Type>>> ColumnTypes;
    const std::shared_ptr<arrow::RecordBatch> Data;
    const ui64 Cookie;

    NYql::TIssues Issues;
};

IActor* CreateUploadColumnsInternal(const TActorId& sender,
                                    const TString& database,
                                    const TString& table,
                                    std::shared_ptr<TUploadTypes> types,
                                    std::shared_ptr<arrow::RecordBatch> data,
                                    ui64 cookie = 0) {
    return new TUploadColumnsInternal(sender, database, table, types, data, cookie);
}


} // namespace NTxProxy
} // namespace NKikimr
