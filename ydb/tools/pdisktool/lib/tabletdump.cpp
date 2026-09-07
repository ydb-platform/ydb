#include "tabletdump.h"

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/flat_iterator.h>

#include <util/folder/path.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

#include <cctype>

namespace NKikimr::NPDiskTool {

namespace {

// A damaged part can put an iterator into a loop that never reaches the end; this is where the dump
// gives up on a table and says so.
constexpr ui64 MaxRowsPerTable = 1ull << 34;

bool NeedsQuoting(const TString& value, char delimiter) {
    for (const char c : value) {
        const unsigned char byte = static_cast<unsigned char>(c);
        if (c == delimiter || c == '"' || byte < 0x20 || byte == 0x7f) {
            return true;
        }
    }
    return false;
}

TString TableFileName(ui32 id, const TString& name, bool csv) {
    TStringBuilder out;
    out << id;
    if (name) {
        out << '_';
        for (const char c : name) {
            out << (isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '-' ? c : '_');
        }
    }
    out << (csv ? ".csv" : ".tsv");
    return out;
}

TString RenderCell(const NTable::TRowState& row, ui32 pos, NScheme::TTypeInfo type) {
    const auto& cell = row.Get(pos);
    if (cell.IsNull()) {
        return {}; // both a null and an absent cell come out as an empty field
    }
    TString out;
    DbgPrintValue(out, cell, type);
    return out;
}

const char* RowOpName(NTable::ERowOp op) {
    switch (op) {
        case NTable::ERowOp::Absent: return "absent";
        case NTable::ERowOp::Upsert: return "upsert";
        case NTable::ERowOp::Erase: return "erase";
        case NTable::ERowOp::Reset: return "reset";
    }
    return "?";
}

// Columns in the order they are written: the key columns first, in key order, then the rest by column
// id, so a table always reads the same way regardless of the order it was altered in.
TVector<const NTable::TColumn*> ColumnOrder(const NTable::TScheme::TTableInfo& info) {
    TVector<const NTable::TColumn*> columns;
    THashSet<ui32> taken;

    for (const ui32 tag : info.KeyColumns) {
        if (const auto* column = info.Columns.FindPtr(tag); column && taken.insert(tag).second) {
            columns.push_back(column);
        }
    }

    TVector<ui32> rest;
    for (const auto& it : info.Columns) {
        if (!taken.contains(it.first)) {
            rest.push_back(it.first);
        }
    }
    Sort(rest);
    for (const ui32 tag : rest) {
        columns.push_back(info.Columns.FindPtr(tag));
    }
    return columns;
}

void WriteDescription(IOutputStream& out, const NTable::TScheme& scheme, const TDumpOptions& options,
        const TVector<TTableDump>& tables)
{
    out << "# Tables recovered from the tablet log, one file per table." << Endl;
    out << "# Fields are separated by " << (options.Csv ? "commas" : "tabs")
        << "; a field holding the delimiter, a quote, a newline or a control byte is wrapped in"
        << " double quotes with inner quotes doubled." << Endl;
    out << "# A null or absent cell is an empty field. Values of binary types are C-escaped." << Endl;
    if (options.IncludeErased) {
        out << "# The first column of every file is the row operation; erased rows are kept." << Endl;
    }
    out << Endl;

    THashMap<ui32, const TTableDump*> written;
    for (const auto& one : tables) {
        written[one.Table] = &one;
    }

    TVector<ui32> ids;
    for (const auto& it : scheme.Tables) {
        ids.push_back(it.first);
    }
    Sort(ids);

    for (const ui32 id : ids) {
        const auto& info = scheme.Tables.at(id);
        out << "table " << id << " " << info.Name << Endl;
        if (const auto* dump = written.Value(id, nullptr)) {
            out << "  file " << dump->File << ", " << dump->Rows << " row(s)";
            if (dump->Erased) {
                out << ", " << dump->Erased << " erased";
            }
            if (!dump->Complete) {
                out << ", TRUNCATED: some data is not in the input";
            }
            out << Endl;
        } else if (!options.TablesOnly) {
            out << "  no file: the table could not be read" << Endl;
        }

        const auto columns = ColumnOrder(info);
        for (size_t pos = 0; pos < columns.size(); ++pos) {
            const auto* column = columns[pos];
            out << "  column " << pos << " id " << column->Id << " " << column->Name
                << " " << NScheme::TypeName(column->PType, column->PTypeMod);
            if (const auto order = column->GetCorrectKeyOrder()) {
                out << " key " << *order;
            }
            if (column->NotNull) {
                out << " not-null";
            }
            out << Endl;
        }
        out << Endl;
    }
}

} // namespace

TString QuoteField(const TString& value, char delimiter) {
    if (!NeedsQuoting(value, delimiter)) {
        return value;
    }
    TString out;
    out.reserve(value.size() + 8);
    out.append('"');
    for (const char c : value) {
        if (c == '"') {
            out.append('"');
        }
        out.append(c);
    }
    out.append('"');
    return out;
}

bool DumpTablet(NTable::TDatabase& db, NTable::IPages& env, const TDumpOptions& options,
        TIssueLog& issues, TDumpStats& stats)
{
    const char delimiter = options.Csv ? ',' : '\t';

    TFsPath dir(options.Output);
    try {
        dir.MkDirs();
    } catch (...) {
        issues.Error("tablet-dump", TStringBuilder() << "Cannot create the output directory "
            << options.Output << ": " << CurrentExceptionMessage());
        return false;
    }

    const auto& scheme = db.GetScheme();

    if (!options.TablesOnly) {
        // The stamp only orders this read against the log that was replayed, and everything is already
        // in place, so anything above the tail will do.
        try {
            db.Begin(NTable::TTxStamp(Max<ui32>(), 0), env);
        } catch (...) {
            issues.Error("tablet-dump", TStringBuilder() << "Cannot start reading the recovered"
                << " database: " << CurrentExceptionMessage());
            return false;
        }

        TVector<ui32> ids;
        for (const auto& it : scheme.Tables) {
            ids.push_back(it.first);
        }
        Sort(ids);

        for (const ui32 id : ids) {
            const auto& info = scheme.Tables.at(id);
            const auto columns = ColumnOrder(info);
            if (columns.empty()) {
                issues.Warning("tablet-dump", TStringBuilder() << "Table " << id << " " << info.Name
                    << " has no columns, so it is skipped");
                continue;
            }

            TTableDump dump;
            dump.Table = id;
            dump.Name = info.Name;
            dump.File = TableFileName(id, info.Name, options.Csv);

            TVector<NTable::TTag> tags;
            tags.reserve(columns.size());
            for (const auto* column : columns) {
                tags.push_back(column->Id);
            }

            const TString path = (dir / dump.File).GetPath();
            try {
                TFileOutput file(path);
                TString line;

                auto field = [&](const TString& value) {
                    if (line) {
                        line.append(delimiter);
                    }
                    line.append(QuoteField(value, delimiter));
                };

                if (options.IncludeErased) {
                    field("op");
                }
                for (const auto* column : columns) {
                    field(column->Name);
                }
                line.append('\n');
                file.Write(line);
                dump.Bytes += line.size();

                auto it = db.IterateRange(id, {}, tags, TRowVersion::Max());
                const auto mode = options.IncludeErased ? NTable::ENext::All : NTable::ENext::Data;

                for (;;) {
                    if (dump.Rows + dump.Erased >= MaxRowsPerTable) {
                        dump.Complete = false;
                        break;
                    }
                    const auto ready = it->Next(mode);
                    if (ready == NTable::EReady::Gone) {
                        break;
                    }
                    if (ready == NTable::EReady::Page) {
                        // A retry would change nothing: the page this row needs is not in the input and
                        // the iterator has no way to step over it.
                        dump.Complete = false;
                        break;
                    }

                    const auto& row = it->Row();
                    const bool erased = row.GetRowState() == NTable::ERowOp::Erase;

                    line.clear();
                    if (options.IncludeErased) {
                        field(RowOpName(row.GetRowState()));
                    }
                    for (size_t pos = 0; pos < columns.size(); ++pos) {
                        field(RenderCell(row, pos, columns[pos]->PType));
                    }
                    line.append('\n');
                    file.Write(line);

                    dump.Bytes += line.size();
                    erased ? ++dump.Erased : ++dump.Rows;
                }

                file.Finish();
            } catch (...) {
                // A row that cannot be rendered, or a part that turns out to be damaged deeper than the
                // loader could see, costs the rest of this table and nothing else.
                dump.Complete = false;
                issues.Warning("tablet-dump", TStringBuilder() << "Table " << id << " " << info.Name
                    << " could not be read to the end: " << CurrentExceptionMessage());
            }

            if (!dump.Complete) {
                ++stats.Incomplete;
                issues.Warning("tablet-dump", TStringBuilder() << "Table " << id << " " << info.Name
                    << " is truncated after " << dump.Rows << " row(s) because the data that follows"
                    << " is not in the input");
            }

            stats.Rows += dump.Rows;
            stats.Tables.push_back(std::move(dump));
        }
    }

    stats.Description = (dir / "tables.txt").GetPath();
    try {
        TFileOutput file(stats.Description);
        WriteDescription(file, scheme, options, stats.Tables);
        file.Finish();
    } catch (...) {
        issues.Error("tablet-dump", TStringBuilder() << "Cannot write " << stats.Description << ": "
            << CurrentExceptionMessage());
        return false;
    }

    return true;
}

} // namespace NKikimr::NPDiskTool
