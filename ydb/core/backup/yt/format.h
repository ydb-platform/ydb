#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>

namespace NKikimr::NBackup::NYt {

    struct TColumn {
        TString Name;
        Ydb::Type::PrimitiveTypeId Type;
        bool Nullable = false;
    };

    struct TSchema {
        Ydb::Table::CreateTableRequest Table;
        NKikimrSchemeOp::TTableDescription Description;
        TVector<TColumn> Columns;
        TVector<ui32> KeyColumns;
    };

    // Inclusive source row index range; row count is End - Start + 1.
    struct TPartRange {
        ui64 Start = 0;
        ui64 End = 0;
    };

    bool ParseMetadata(TStringBuf data, TSchema& schema, TString& error);
    bool BuildSchema(const NKikimrSchemeOp::TTableDescription& description, TSchema& schema, TString& error);
    bool ParsePartKey(TStringBuf prefix, TStringBuf key, TPartRange& range, TString& error);

    // Cells refer to Storage and remain valid only during the callback.
    struct TDecodedRow {
        TVector<TString> Storage;
        TVector<TCell> Cells;
    };

    class TRowDecoder {
    public:
        using TCallback = std::function<bool(const TDecodedRow&, TString&)>;

        TRowDecoder(const TSchema& schema, TCallback callback, size_t maxRowBytes = 16 * 1024 * 1024);
        ~TRowDecoder();
        bool Feed(TStringBuf data, TString& error);
        bool Finish(TString& error);

    private:
        class TImpl;
        THolder<TImpl> Impl_;
    };

    // A bounded probe of the first decoded row; full stream integrity is checked during restore.
    class TFirstKeyProbe {
    public:
        TFirstKeyProbe(const TSchema& schema, size_t maxBytes = 16 * 1024 * 1024);
        ~TFirstKeyProbe();
        bool Feed(TStringBuf compressed, bool last, TString& error);
        bool HasKey() const;
        const TString& GetKey() const;

    private:
        class TImpl;
        THolder<TImpl> Impl_;
    };

} // namespace NKikimr::NBackup::NYt
