#pragma once

#include "dump.h"

#include <ydb/public/lib/ydb_cli/dump/files/files.h>
#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <util/folder/path.h>
#include <util/generic/hash_set.h>
#include <util/stream/fwd.h>
#include <util/string/builder.h>

namespace NYdb {
namespace NDump {

extern const char DOC_API_TABLE_VERSION_ATTR[23];
extern const char DOC_API_REQUEST_TYPE[22];

namespace NPrivate {

class TBatch;

class TLocation {
    friend class TBatch;
    TStringBuf File;
    ui64 LineNo;

public:
    explicit TLocation(TStringBuf file, ui64 lineNo);
    void Out(IOutputStream& out) const;
};

class TLine {
    TString Data;
    TLocation Location;

public:
    explicit TLine(TString&& data, TStringBuf file, ui64 lineNo);
    explicit TLine(TString&& data, const TLocation& location);

    inline const TString& GetData() const {
        return Data;
    }

    inline const TLocation& GetLocation() const {
        return Location;
    }

    inline operator TString() const {
        return Data;
    }

    inline operator TStringBuf() const {
        return Data;
    }

    inline auto size() const {
        return Data.size();
    }
};

class TBatch {
    TStringBuilder Data;
    TVector<TLocation> Locations;

public:
    void Add(const TLine& line);
    TString GetLocation() const;

    inline const TString& GetData() const {
        return Data;
    }

    inline const TLocation& GetLocation(ui32 index) const {
        return Locations.at(index);
    }

    inline operator TString() const {
        return Data;
    }

    inline auto size() const {
        return Data.size();
    }
};

class IDataAccumulator {
public:
    enum EStatus {
        OK,
        FULL,
        ERROR,
    };

    virtual ~IDataAccumulator() = default;
    virtual EStatus Check(const TLine& line) const = 0;
    virtual void Feed(TLine&& line) = 0;
    virtual bool Ready(bool force = false) const = 0;
    virtual TBatch GetData(bool force = false) = 0;
};

class IDataWriter {
public:
    virtual ~IDataWriter() = default;
    virtual bool Push(TBatch&& data) = 0;
    virtual void Wait() = 0;
};

} // NPrivate

class TRestoreClient {
    TRestoreResult RestoreFolder(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, const THashSet<TString>& oldEntries);
    TRestoreResult RestoreEmptyDir(const TFsPath& fsPath, const TString &dbPath, const TRestoreSettings& settings, const THashSet<TString>& oldEntries);
    TRestoreResult RestoreTable(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, const THashSet<TString>& oldEntries);

    TRestoreResult CheckSchema(const TString& dbPath, const NTable::TTableDescription& desc);
    TRestoreResult RestoreData(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, const NTable::TTableDescription& desc);
    TRestoreResult RestoreIndexes(const TString& dbPath, const NTable::TTableDescription& desc);
    TRestoreResult RestorePermissions(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, const THashSet<TString>& oldEntries);

public:
    explicit TRestoreClient(const TDriver& driver, const std::shared_ptr<TLog>& log);

    TRestoreResult Restore(const TString& fsPath, const TString& dbPath, const TRestoreSettings& settings = {});

private:
    NImport::TImportClient ImportClient;
    NOperation::TOperationClient OperationClient;
    NScheme::TSchemeClient SchemeClient;
    NTable::TTableClient TableClient;
    std::shared_ptr<TLog> Log;

}; // TRestoreClient

} // NDump
} // NYdb
