#pragma once

#include <yt/yql/providers/yt/fmr/yt_service/interface/yql_yt_yt_service.h>

namespace NYql::NFmr {

namespace {

class TYtUploadedTablesMock: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TYtUploadedTablesMock>;

    ~TYtUploadedTablesMock() = default;

    void AddTable(const TYtTableRef& ytTable, const TString& tableContent) {
        TString key = GetTableKey(ytTable);
        UploadedTables_[key] = tableContent;
    }
    bool Contains(const TYtTableRef& ytTable) const {
        TString key = GetTableKey(ytTable);
        return UploadedTables_.find(key) != UploadedTables_.end();
    }
    const TString& GetTableContent(const TYtTableRef& ytTable) const {
        TString key = GetTableKey(ytTable);
        return UploadedTables_.at(key);
    }
    void Clear() {
        UploadedTables_.clear();
    }
    bool IsEmpty() const {
        return UploadedTables_.empty();
    }
    int UploadedTablesNum() const {
        return UploadedTables_.size();
    }

private:
    TString GetTableKey(const TYtTableRef& ytTable) const {
        return ytTable.Cluster + ":" + ytTable.Path;
    }
    std::unordered_map<TString, TString> UploadedTables_;
};


class TYtServiceMock: public NYql::NFmr::IYtService {
public:

    TYtServiceMock(TYtUploadedTablesMock::TPtr ytUploadedTablesMock)
        : YtUploadedTablesMock_(ytUploadedTablesMock)
    {
    }

    std::variant<THolder<TTempFileHandle>, TError> Download(const TYtTableRef& ytTable, ui64& rowsCount, const TClusterConnection& /*clusterConnection*/) override {
        if (!YtUploadedTablesMock_->Contains(ytTable)) {
            return TError("Table not found");
        }
        auto tmpFile = MakeHolder<TTempFileHandle>();
        TString tableContent = YtUploadedTablesMock_->GetTableContent(ytTable);
        rowsCount = RowsCount(tableContent);
        tmpFile->Write(tableContent.data(), tableContent.size());
        return tmpFile;
    }

    TMaybe<TError> Upload(const TYtTableRef& ytTable, IInputStream& tableContent, const TClusterConnection& /*clusterConnection*/) override {
        YtUploadedTablesMock_->AddTable(ytTable, tableContent.ReadAll());
        return Nothing();
    }

private:
    ui64 RowsCount(const TString& tableContent) {
        ui64 rowsCount = 0;
        for (auto ch : tableContent) {
            if (ch == '}') {
                rowsCount++;
            }
        }
        return rowsCount;
    }

private:
    TYtUploadedTablesMock::TPtr YtUploadedTablesMock_;
};

} // namespace

IYtService::TPtr MakeYtServiceMock(TYtUploadedTablesMock::TPtr ytUploadedTablesMock) {
    return MakeIntrusive<TYtServiceMock>(ytUploadedTablesMock);
}

TYtUploadedTablesMock::TPtr MakeYtUploadedTablesMock() {
    return MakeIntrusive<TYtUploadedTablesMock>();
}

} // namespace NYql::NFmr

// TODO - move this to .cpp file
