#pragma once

#include <util/generic/yexception.h>
#include <util/generic/ptr.h>

#include <contrib/libs/sqlite3/sqlite3.h>

namespace NSQLite {
    class TSQLiteError: public yexception {
    public:
        TSQLiteError(sqlite3* hndl);
        TSQLiteError(int rc);

        int GetErrorCode() const {
            return ErrorCode;
        }

    private:
        int ErrorCode;
    };

    template <class T, int (*Func)(T*)>
    struct TCFree {
        static void Destroy(T* t) {
            Func(t);
        }
    };

    class TSQLiteDB {
    public:
        TSQLiteDB(const TString& path, int flags);
        TSQLiteDB(const TString& path);

        sqlite3* Handle() const noexcept;
        size_t RowsAffected() const noexcept;

    private:
        THolder<sqlite3, TCFree<sqlite3, sqlite3_close>> H_;
    };

    class ISQLiteColumnVisitor {
    public:
        virtual ~ISQLiteColumnVisitor() = default;

        virtual void OnColumnInt64(i64 value) = 0;
        virtual void OnColumnDouble(double value) = 0;
        virtual void OnColumnText(TStringBuf value) = 0;
        virtual void OnColumnBlob(TStringBuf value) = 0;
        virtual void OnColumnNull() = 0;
    };

    class TSQLiteStatement {
    public:
        TSQLiteStatement(TSQLiteDB& db, const TString& s);

        void Execute();
        TSQLiteStatement& Bind(size_t idx, i64 val);
        TSQLiteStatement& Bind(size_t idx, int val);
        TSQLiteStatement& Bind(size_t idx);
        TSQLiteStatement& Bind(size_t idx, double val);
        TSQLiteStatement& Bind(size_t idx, TStringBuf str);
        TSQLiteStatement& BindBlob(size_t idx, TStringBuf blob);
        template <typename Value>
        TSQLiteStatement& Bind(TStringBuf name, Value val) {
            size_t idx = BoundNamePosition(name);
            Y_ASSERT(idx > 0);
            return Bind(idx, val);
        }
        TSQLiteStatement& BindBlob(TStringBuf name, TStringBuf blob) {
            size_t idx = BoundNamePosition(name);
            Y_ASSERT(idx > 0);
            return BindBlob(idx, blob);
        }
        TSQLiteStatement& Bind(TStringBuf name) {
            size_t idx = BoundNamePosition(name);
            Y_ASSERT(idx > 0);
            return Bind(idx);
        }
        size_t BoundNamePosition(TStringBuf name) const noexcept;
        size_t BoundParameterCount() const noexcept;
        const char* BoundParameterName(size_t idx) const noexcept;

        sqlite3_stmt* Handle() const noexcept;
        bool Step();
        i64 ColumnInt64(size_t idx);
        double ColumnDouble(size_t idx);
        TStringBuf ColumnText(size_t idx);
        TStringBuf ColumnBlob(size_t idx);
        void ColumnAccept(size_t idx, ISQLiteColumnVisitor& visitor);
        size_t ColumnCount() const noexcept;
        TStringBuf ColumnName(size_t idx) const noexcept;
        void Reset();
        // Ignore last error on this statement
        void ResetHard();
        void ClearBindings() noexcept;

    private:
        typedef void (*TFreeFunc)(void*);
        void BindText(size_t col, const char* text, size_t len, TFreeFunc func);

    private:
        TString S_;
        THolder<sqlite3_stmt, TCFree<sqlite3_stmt, sqlite3_finalize>> H_;
    };

    /**
     * Forces user to commit transaction explicitly, to not get exception in destructor (with all consequences of it).
     */
    class TSQLiteTransaction: private TNonCopyable {
    private:
        TSQLiteDB* Db;

    public:
        TSQLiteTransaction(TSQLiteDB& db);
        ~TSQLiteTransaction();

        void Commit();
        void Rollback();

    private:
        void Execute(const TString& query);
    };

    class TSimpleDB: public TSQLiteDB {
    public:
        TSimpleDB(const TString& path);

        void Execute(const TString& statement);
        void Acquire();
        void Release();

    private:
        TSQLiteStatement Start_;
        TSQLiteStatement End_;
    };
}
