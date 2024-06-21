#include "scheme_printers.h"
#include "print_utils.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/lib/ydb_cli/common/tabbed_table.h>
#include <library/cpp/colorizer/colors.h>

namespace NYdb {
namespace NConsoleClient {

TSchemePrinterBase::TSchemePrinterBase(const TDriver& driver, TSettings&& settings)
    : TableClient(driver)
    , SchemeClient(driver)
    , Settings(std::move(settings))
{}

void TSchemePrinterBase::Print() {
    PrintDirectoryRecursive(Settings.Path, "").GetValueSync();
}

bool TSchemePrinterBase::IsDirectoryLike(const NScheme::TSchemeEntry& entry) {
    return entry.Type == NScheme::ESchemeEntryType::Directory
        || entry.Type == NScheme::ESchemeEntryType::SubDomain
        || entry.Type == NScheme::ESchemeEntryType::ColumnStore;
}

NThreading::TFuture<void> TSchemePrinterBase::PrintDirectoryRecursive(const TString& fullPath, const TString& relativePath) {
    return SchemeClient.ListDirectory(
        fullPath,
        Settings.ListDirectorySettings
    ).Apply([this, fullPath, relativePath](const NScheme::TAsyncListDirectoryResult& resultFuture) {
        const auto& result = resultFuture.GetValueSync();
        ThrowOnError(result);

        if (relativePath || IsDirectoryLike(result.GetEntry())) {
            std::lock_guard g(Lock);
            PrintDirectory(relativePath, result);
        } else {
            std::lock_guard g(Lock);
            PrintEntry(relativePath, result.GetEntry());
        }

        TVector<NThreading::TFuture<void>> childFutures;
        if (Settings.Recursive) {
            for (const auto& child : result.GetChildren()) {
                TString childRelativePath = relativePath + (relativePath ? "/" : "") + child.Name;
                TString childFullPath = fullPath + "/" + child.Name;
                if (IsDirectoryLike(child)) {
                    childFutures.push_back(PrintDirectoryRecursive(childFullPath, childRelativePath));
                    if (!Settings.Multithread) {
                        childFutures.back().Wait();
                        childFutures.back().TryRethrow();
                    }
                } else {
                    std::lock_guard g(Lock);
                    PrintEntry(childRelativePath, child);
                }
            }
        }
        return NThreading::WaitExceptionOrAll(childFutures);
    });
}

NTable::TDescribeTableResult TSchemePrinterBase::DescribeTable(const TString& relativePath) {
    NTable::TCreateSessionResult sessionResult = TableClient.GetSession(
        NTable::TCreateSessionSettings()
    ).GetValueSync();
    ThrowOnError(sessionResult);

    NTable::TDescribeTableResult tableResult = sessionResult.GetSession().DescribeTable(
        Settings.Path + (relativePath ? ("/" + relativePath) : ""),
        Settings.DescribeTableSettings
    ).GetValueSync();
    ThrowOnError(tableResult);
    return tableResult;
}

TDefaultSchemePrinter::TDefaultSchemePrinter(const TDriver& driver, TSettings&& settings)
    : TSchemePrinterBase(driver, std::move(settings))
{}

void TDefaultSchemePrinter::PrintDirectory(
    const TString& relativePath,
    const NScheme::TListDirectoryResult& entryResult)
{
    TVector<NScheme::TSchemeEntry> children = entryResult.GetChildren();
    NScheme::TSchemeEntry entry = entryResult.GetEntry();

    if (Settings.Recursive) {
        if (relativePath) {
            Cout << Endl;
        }
        Cout << (relativePath ? relativePath : "./") << ":" << Endl;
    }
    if (children.size()) {
        if (Settings.FromNewLine) {
            NColorizer::TColors colors = NColorizer::AutoColors(Cout);
            for (const auto& child : children) {
                PrintSchemeEntry(Cout, child, colors);
                Cout << Endl;
            }
        }
        else {
            TAdaptiveTabbedTable table(children);
            Cout << table;
        }
    }
}

void TDefaultSchemePrinter::PrintEntry(const TString& relativePath, const NScheme::TSchemeEntry& entry) {
    if (!relativePath) {
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        PrintSchemeEntry(Cout, entry, colors);
        Cout << Endl;
    }
}

TTableSchemePrinter::TTableSchemePrinter(const TDriver& driver, TSettings&& settings)
    : TSchemePrinterBase(driver, std::move(settings))
    , Table(
        { "Type", "Owner", "Size", "Created", "Modified", "Name" },
        TPrettyTableConfig().WithoutRowDelimiters()
    )
{}

void TTableSchemePrinter::Print() {
    TSchemePrinterBase::Print();
    if (Changed) {
        Cout << Table;
    }
}

void TTableSchemePrinter::PrintDirectory(
    const TString& relativePath,
    const NScheme::TListDirectoryResult& entryResult)
{
    if (relativePath) {
        PrintOther(relativePath, entryResult.GetEntry());
    } else {
        // Do not print target directory itself
        if (!Settings.Recursive) {
            for (const auto& child : entryResult.GetChildren()) {
                PrintEntry(child.Name, child);
            }
        }
    }
}

void TTableSchemePrinter::PrintEntry(const TString& relativePath, const NScheme::TSchemeEntry& entry) {
    if (entry.Type == NScheme::ESchemeEntryType::Table) {
        PrintTable(relativePath, entry);
    } else {
        PrintOther(relativePath, entry);
    }
}

void TTableSchemePrinter::PrintTable(const TString& relativePath, const NScheme::TSchemeEntry& entry) {
    auto tableResult = DescribeTable(relativePath);
    auto tableDescription = tableResult.GetTableDescription();

    // Empty relative path in case of a single non-directory object in Path
    TString actualRelativePath = relativePath ? relativePath : entry.Name;

    Table.AddRow()
        .Column(0, EntryTypeToString(entry.Type))
        .Column(1, entry.Owner)
        .Column(2, PrettySize(tableDescription.GetTableSize()))
        .Column(3, FormatTime(tableDescription.GetCreationTime()))
        .Column(4, FormatTime(tableDescription.GetModificationTime()))
        .Column(5, actualRelativePath);
    Changed = true;
}

void TTableSchemePrinter::PrintOther(const TString& relativePath, const NScheme::TSchemeEntry& entry) {
    // Empty relative path in case of a single non-directory object in Path
    TString actualRelativePath = relativePath ? relativePath : entry.Name;
    Table.AddRow()
        .Column(0, EntryTypeToString(entry.Type))
        .Column(1, entry.Owner)
        .Column(2, "")
        .Column(3, "")
        .Column(4, "")
        .Column(5, actualRelativePath);
    Changed = true;
}

TJsonSchemePrinter::TJsonSchemePrinter(const TDriver& driver, TSettings&& settings, bool advanced)
    : TSchemePrinterBase(driver, std::move(settings))
    , Advanced(advanced)
    , Writer(NJsonWriter::HEM_UNSAFE)
{
    Writer.SetIndentSpaces(2);
}

void TJsonSchemePrinter::Print() {
    TSchemePrinterBase::Print();
    if (NeedToCloseList) {
        Writer.EndList();
        Cout << Writer.Str() << Endl;
    }
}

void TJsonSchemePrinter::PrintDirectory(const TString& relativePath, const NScheme::TListDirectoryResult& entryResult) {
    if (relativePath) {
        PrintOther(relativePath, entryResult.GetEntry());
    } else {
        // Do not print target directory itself
        Writer.BeginList();
        if (Settings.Recursive) {
            NeedToCloseList = true;
        } else {
            for (const auto& child : entryResult.GetChildren()) {
                PrintEntry(child.Name, child);
            }
            Writer.EndList();
            Cout << Writer.Str() << Endl;
        }
    }
}

void TJsonSchemePrinter::PrintEntry(const TString& relativePath, const NScheme::TSchemeEntry& entry) {
    if (Advanced && entry.Type == NScheme::ESchemeEntryType::Table) {
        PrintTable(relativePath, entry);
    } else {
        PrintOther(relativePath, entry);
    }
}

void TJsonSchemePrinter::PrintTable(const TString& relativePath, const NScheme::TSchemeEntry& entry) {
    auto tableResult = DescribeTable(relativePath);
    auto tableDescription = tableResult.GetTableDescription();

    Writer.BeginObject();
    PrintCommonInfo(relativePath, entry);
    Writer.WriteKey("size");
    Writer.WriteULongLong(tableDescription.GetTableSize());
    Writer.WriteKey("created");
    Writer.WriteULongLong(tableDescription.GetCreationTime().MilliSeconds());
    Writer.WriteKey("modified");
    Writer.WriteULongLong(tableDescription.GetModificationTime().MilliSeconds());
    Writer.EndObject();
    if (!relativePath) {
        Cout << Writer.Str() << Endl;
    }
}

void TJsonSchemePrinter::PrintOther(const TString& relativePath, const NScheme::TSchemeEntry& entry) {
    Writer.BeginObject();
    PrintCommonInfo(relativePath, entry);
    Writer.EndObject();
    if (!relativePath) {
        Cout << Writer.Str() << Endl;
    }
}

void TJsonSchemePrinter::PrintCommonInfo(const TString& relativePath, const NScheme::TSchemeEntry& entry) {
    // Empty relative path in case of a single non-directory object in Path
    TString actualRelativePath = relativePath ? relativePath : entry.Name;
    Writer.WriteKey("type");
    Writer.WriteString(EntryTypeToString(entry.Type));
    Writer.WriteKey("path");
    Writer.WriteString(actualRelativePath);
    if (Advanced) {
        Writer.WriteKey("owner");
        Writer.WriteString(entry.Owner);
    }
}

}
}
