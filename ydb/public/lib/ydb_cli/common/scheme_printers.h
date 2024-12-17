#pragma once

#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <library/cpp/json/json_writer.h>
#include <mutex>

namespace NYdb {
namespace NConsoleClient {

class ISchemePrinter {
public:
    struct TSettings {
        TString Path;
        bool Recursive;
        bool Multithread;
        bool FromNewLine;
        NScheme::TListDirectorySettings ListDirectorySettings;
        NTable::TDescribeTableSettings DescribeTableSettings;
    };

public:
    virtual ~ISchemePrinter() = default;
    virtual void Print() = 0;

protected:
    virtual void PrintDirectory(const TString& relativePath, const NScheme::TListDirectoryResult& entryResult) = 0;
    virtual void PrintEntry(const TString& relativePath, const NScheme::TSchemeEntry& entry) = 0;
};

class TSchemePrinterBase : public ISchemePrinter {
public:

    TSchemePrinterBase(const TDriver& driver, TSettings&& settings);
    void Print() override;

protected:
    NTable::TDescribeTableResult DescribeTable(const TString& relativePath);

private:
    NThreading::TFuture<void> PrintDirectoryRecursive(const TString& fullPath, const TString& relativePath);
    static bool IsDirectoryLike(const NScheme::TSchemeEntry& entry);

protected:
    NTable::TTableClient TableClient;
    NScheme::TSchemeClient SchemeClient;
    const TSettings Settings;
    std::mutex Lock;
};

class TDefaultSchemePrinter : public TSchemePrinterBase {
public:
    TDefaultSchemePrinter(const TDriver& driver, TSettings&& settings);

private:
    void PrintDirectory(const TString& relativePath, const NScheme::TListDirectoryResult& entryResult) override;
    void PrintEntry(const TString& relativePath, const NScheme::TSchemeEntry& entry) override;
};

class TTableSchemePrinter : public TSchemePrinterBase {
public:
    TTableSchemePrinter(const TDriver& driver, TSettings&& settings);
    void Print() override;

private:
    void PrintDirectory(const TString& relativePath, const NScheme::TListDirectoryResult& entryResult) override;
    void PrintEntry(const TString& relativePath, const NScheme::TSchemeEntry& entry) override;
    void PrintTable(const TString& relativePath, const NScheme::TSchemeEntry& entry);
    void PrintOther(const TString& relativePath, const NScheme::TSchemeEntry& entry);

private:
    TPrettyTable Table;
    bool Changed = false;
};

class TJsonSchemePrinter : public TSchemePrinterBase {
public:
    TJsonSchemePrinter(const TDriver& driver, TSettings&& settings, bool advanced);
    void Print() override;

private:
    void PrintDirectory(const TString& relativePath, const NScheme::TListDirectoryResult& entryResult) override;
    void PrintEntry(const TString& relativePath, const NScheme::TSchemeEntry& entry) override;
    void PrintTable(const TString& relativePath, const NScheme::TSchemeEntry& entry);
    void PrintOther(const TString& relativePath, const NScheme::TSchemeEntry& entry);
    void PrintCommonInfo(const TString& relativePath, const NScheme::TSchemeEntry& entry);

private:
    bool Advanced;
    NJsonWriter::TBuf Writer;
    bool NeedToCloseList = false;
};

}
}
