#pragma once

#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/split.h>

namespace NYdb {
namespace NConsoleClient {

struct TPrettyTableConfig {
    bool Header = true;
    bool DelimRows = true;
    size_t Width = 0; // auto

    TPrettyTableConfig& WithoutHeader() {
        Header = false;
        return *this;
    }

    TPrettyTableConfig& WithoutRowDelimiters() {
        DelimRows = false;
        return *this;
    }

    TPrettyTableConfig& MaxWidth(size_t width) {
        Width = width;
        return *this;
    }
};

class TPrettyTable {
public:
    class TRow {
        friend class TPrettyTable;

        explicit TRow(const TVector<TString>& columnNames) { 
            for (const auto& name : columnNames) {
                Columns.push_back({name});
            }
        }

        explicit TRow(size_t nColumns);

    public:
        template <typename T>
        TRow& Column(size_t index, const T& data) {
            TString lines = TStringBuilder() << data;

            for (auto& line : StringSplitter(lines).Split('\n')) {
                if (line.Empty()) {
                    continue;
                }

                Columns[index].emplace_back(std::move(line));
            }

            return *this;
        }

        TRow& FreeText(TString text) {
            Text = std::move(text);
            return *this;
        }

    private:
        size_t ColumnWidth(size_t columnIndex) const;
        bool PrintColumns(IOutputStream& o, const TVector<size_t>& widths, size_t lineNumber) const;
        bool HasFreeText() const;
        void PrintFreeText(IOutputStream& o, size_t width) const;

    private:
        TVector<TVector<TString>> Columns;
        TString Text;

    };

public:
    explicit TPrettyTable(const TVector<TString>& columnNames, const TPrettyTableConfig& config = TPrettyTableConfig()) 
        : Columns(columnNames.size())
        , Config(config)
    {
        if (Config.Header) {
            Rows.push_back(TRow(columnNames));
        }
    }

    TRow& AddRow();
    void Print(IOutputStream& o) const;

private:
    TVector<size_t> CalcWidths() const;

private:
    const size_t Columns;
    const TPrettyTableConfig Config;
    TVector<TRow> Rows;

};

}
}

template <>
inline void Out<NYdb::NConsoleClient::TPrettyTable>(IOutputStream& o, const NYdb::NConsoleClient::TPrettyTable& x) {
    return x.Print(o);
}
