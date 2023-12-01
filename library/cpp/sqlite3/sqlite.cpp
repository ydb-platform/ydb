#include "sqlite.h"

#include <util/generic/singleton.h>
#include <util/generic/scope.h>

#include <cstdlib>

using namespace NSQLite;

namespace {
    struct TSQLiteInit {
        inline TSQLiteInit() {
            int ret = sqlite3_config(SQLITE_CONFIG_MULTITHREAD);

            if (ret != SQLITE_OK) {
                ythrow TSQLiteError(ret) << "init failure";
            }
        }

        static inline void Ensure() {
            Singleton<TSQLiteInit>();
        }
    };
}

namespace NSQLite {
    TSQLiteError::TSQLiteError(sqlite3* hndl)
        : ErrorCode(sqlite3_errcode(hndl))
    {
        *this << sqlite3_errmsg(hndl) << ". ";
    }

    TSQLiteError::TSQLiteError(int rc)
        : ErrorCode(rc)
    {
        *this << sqlite3_errstr(rc) << " (" << rc << "). ";
    }

    TSQLiteDB::TSQLiteDB(const TString& path) {
        TSQLiteInit::Ensure();

        sqlite3* db = nullptr;
        const int rc = sqlite3_open(path.data(), &db);

        H_.Reset(db);

        if (rc) {
            ythrow TSQLiteError(Handle()) << "can not init db " << path.Quote();
        }
    }

    TSQLiteDB::TSQLiteDB(const TString& path, int flags) {
        TSQLiteInit::Ensure();

        sqlite3* db = nullptr;
        const int rc = sqlite3_open_v2(path.data(), &db, flags, nullptr);

        H_.Reset(db);

        if (rc) {
            ythrow TSQLiteError(Handle()) << "can not init db " << path.Quote();
        }
    }

    sqlite3* TSQLiteDB::Handle() const noexcept {
        return H_.Get();
    }

    size_t TSQLiteDB::RowsAffected() const noexcept {
        return static_cast<size_t>(sqlite3_changes(H_.Get()));
    }

    TSQLiteStatement::TSQLiteStatement(TSQLiteDB& db, const TString& s)
        : S_(s)
    {
        if (!S_.empty() && S_[S_.size() - 1] != ';') {
            S_ += ';';
        }

        sqlite3_stmt* st = nullptr;
        const char* tail = nullptr;
        const int rc = sqlite3_prepare_v2(db.Handle(), S_.data(), S_.size() + 1, &st, &tail);

        H_.Reset(st);

        if (rc != SQLITE_OK) {
            ythrow TSQLiteError(db.Handle()) << "can not prepare " << S_.Quote();
        }
    }

    void TSQLiteStatement::Execute() {
        while (Step()) {
        }

        Reset();
    }

    TSQLiteStatement& TSQLiteStatement::Bind(size_t idx, i64 val) {
        sqlite3_bind_int64(Handle(), idx, val);
        return *this;
    }

    TSQLiteStatement& TSQLiteStatement::Bind(size_t idx, int val) {
        sqlite3_bind_int(Handle(), idx, val);
        return *this;
    }

    TSQLiteStatement& TSQLiteStatement::Bind(size_t idx) {
        sqlite3_bind_null(Handle(), idx);
        return *this;
    }

    TSQLiteStatement& TSQLiteStatement::Bind(size_t idx, double val) {
        sqlite3_bind_double(Handle(), idx, val);
        return *this;
    }

    void TSQLiteStatement::BindText(size_t idx, const char* text, size_t len, TFreeFunc func) {
        sqlite3_bind_text(Handle(), idx, text, len, func);
    }

    TSQLiteStatement& TSQLiteStatement::Bind(size_t idx, TStringBuf str) {
        BindText(idx, str.data(), str.size(), SQLITE_STATIC);
        return *this;
    }

    TSQLiteStatement& TSQLiteStatement::BindBlob(size_t idx, TStringBuf blob) {
        sqlite3_bind_blob(Handle(), idx, blob.data(), blob.size(), SQLITE_STATIC);
        return *this;
    }

    size_t TSQLiteStatement::BoundNamePosition(TStringBuf name) const noexcept {
        return sqlite3_bind_parameter_index(Handle(), name.data());
    }

    size_t TSQLiteStatement::BoundParameterCount() const noexcept {
        return sqlite3_bind_parameter_count(Handle());
    }

    const char* TSQLiteStatement::BoundParameterName(size_t idx) const noexcept {
        return sqlite3_bind_parameter_name(Handle(), idx);
    }

    sqlite3_stmt* TSQLiteStatement::Handle() const noexcept {
        return H_.Get();
    }

    bool TSQLiteStatement::Step() {
        const int rc = sqlite3_step(Handle());

        switch (rc) {
            case SQLITE_ROW:
                return true;

            case SQLITE_DONE:
                return false;

            default:
                break;
        }

        char* stmt = rc == SQLITE_CONSTRAINT ? sqlite3_expanded_sql(Handle()) : nullptr;
        Y_DEFER {
            if (stmt != nullptr) {
                sqlite3_free(reinterpret_cast<void*>(stmt));
                stmt = nullptr;
            }
        };
        if (stmt != nullptr) {
            ythrow TSQLiteError(rc) << "step failed: " << stmt;
        } else {
            ythrow TSQLiteError(rc) << "step failed";
        }
    }

    i64 TSQLiteStatement::ColumnInt64(size_t idx) {
        return sqlite3_column_int64(Handle(), idx);
    }

    double TSQLiteStatement::ColumnDouble(size_t idx) {
        return sqlite3_column_double(Handle(), idx);
    }

    TStringBuf TSQLiteStatement::ColumnText(size_t idx) {
        return reinterpret_cast<const char*>(sqlite3_column_text(Handle(), idx));
    }

    TStringBuf TSQLiteStatement::ColumnBlob(size_t idx) {
        const void* blob = sqlite3_column_blob(Handle(), idx);
        size_t size = sqlite3_column_bytes(Handle(), idx);
        return TStringBuf(static_cast<const char*>(blob), size);
    }

    void TSQLiteStatement::ColumnAccept(size_t idx, ISQLiteColumnVisitor& visitor) {
        const auto columnType = sqlite3_column_type(Handle(), idx);
        switch (columnType) {
        case SQLITE_INTEGER:
            visitor.OnColumnInt64(ColumnInt64(idx));
            break;
        case SQLITE_FLOAT:
            visitor.OnColumnDouble(ColumnDouble(idx));
            break;
        case SQLITE_TEXT:
            visitor.OnColumnText(ColumnText(idx));
            break;
        case SQLITE_BLOB:
            visitor.OnColumnBlob(ColumnBlob(idx));
            break;
        case SQLITE_NULL:
            visitor.OnColumnNull();
            break;
        }
    }

    size_t TSQLiteStatement::ColumnCount() const noexcept {
        return static_cast<size_t>(sqlite3_column_count(Handle()));
    }

    TStringBuf TSQLiteStatement::ColumnName(size_t idx) const noexcept {
        return sqlite3_column_name(Handle(), idx);
    }

    void TSQLiteStatement::Reset() {
        const int rc = sqlite3_reset(Handle());

        if (rc != SQLITE_OK) {
            ythrow TSQLiteError(rc) << "reset failed";
        }
    }

    void TSQLiteStatement::ResetHard() {
        (void)sqlite3_reset(Handle());
    }

    void TSQLiteStatement::ClearBindings() noexcept {
        // No error is documented.
        // sqlite3.c's code always returns SQLITE_OK.
        (void)sqlite3_clear_bindings(Handle());
    }

    TSQLiteTransaction::TSQLiteTransaction(TSQLiteDB& db)
        : Db(&db)
    {
        Execute("BEGIN TRANSACTION");
    }

    TSQLiteTransaction::~TSQLiteTransaction() {
        if (Db) {
            Rollback();
        }
    }

    void TSQLiteTransaction::Commit() {
        Execute("COMMIT TRANSACTION");
        Db = nullptr;
    }

    void TSQLiteTransaction::Rollback() {
        Execute("ROLLBACK TRANSACTION");
        Db = nullptr;
    }

    void TSQLiteTransaction::Execute(const TString& query) {
        Y_ENSURE(Db, "Transaction is already ended");
        TSQLiteStatement st(*Db, query);
        st.Execute();
    }

    TSimpleDB::TSimpleDB(const TString& path)
        : TSQLiteDB(path)
        , Start_(*this, "begin transaction")
        , End_(*this, "end transaction")
    {
    }

    void TSimpleDB::Execute(const TString& statement) {
        TSQLiteStatement(*this, statement).Execute();
    }

    void TSimpleDB::Acquire() {
        Start_.Execute();
    }

    void TSimpleDB::Release() {
        End_.Execute();
    }

}
