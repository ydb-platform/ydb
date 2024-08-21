#pragma once

#include "ydb_command.h"
#include "ydb_common.h"
#include "ydb_service_table.h"

#include <ydb/public/lib/ydb_cli/common/examples.h>
#include <ydb/public/lib/ydb_cli/common/parseable_struct.h>

#include <library/cpp/regex/pcre/regexp.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandTools : public TClientCommandTree {
public:
    TCommandTools();
};

class TToolsCommand : public TYdbCommand {
public:
    TToolsCommand(
        const TString& name,
        const std::initializer_list<TString>& aliases = std::initializer_list<TString>(),
        const TString& description = TString()
    );

    virtual void Config(TConfig& config) override;
};

class TCommandDump : public TToolsCommand, public TCommandWithPath {
public:
    TCommandDump();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TVector<TRegExMatch> ExclusionPatterns;
    TString FilePath;
    bool IsSchemeOnly;
    bool AvoidCopy = false;
    bool SavePartialResult = false;
    TString ConsistencyLevel;
    bool PreservePoolKinds = false;
    bool Ordered = false;
    bool PreserveACL = false;
};

class TCommandRestore : public TToolsCommand, public TCommandWithPath {
public:
    TCommandRestore();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString FilePath;
    bool IsDryRun = false;
    bool RestoreData = true;
    bool RestoreIndexes = true;
    bool SkipDocumentTables = false;
    bool SavePartialResult = false;
    TString UploadBandwidth;
    TString UploadRps;
    TString RowsPerRequest;
    TString BytesPerRequest;
    TString RequestUnitsPerRequest;
    ui32 InFly;
    bool UseBulkUpsert = false;
    bool UseImportData = false;
};

class TCommandCopy : public TTableCommand {
public:
    TCommandCopy();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    struct TItemFields {
        TString Source;
        TString Destination;
    };
    DEFINE_PARSEABLE_STRUCT(TItem, TItemFields, Source, Destination);

    TVector<TItem> Items;
    TString DatabaseName;
};

class TCommandRename : public TTableCommand, public TCommandWithExamples {
public:
    TCommandRename();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    struct TItemFields {
        TString Source;
        TString Destination;
        bool Replace = false;
    };
    DEFINE_PARSEABLE_STRUCT(TItem, TItemFields, Source, Destination, Replace);

    TVector<TItem> Items;
    TString DatabaseName;
};

class TCommandPgConvert : public TToolsCommand {
public:
    TCommandPgConvert();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString Path;
    bool IgnoreUnsupported = false;
};

}
}
