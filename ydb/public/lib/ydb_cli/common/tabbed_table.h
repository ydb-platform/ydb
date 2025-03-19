#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

namespace NYdb {
namespace NConsoleClient {

class TAdaptiveTabbedTable {
public:
    TAdaptiveTabbedTable(const std::vector<NScheme::TSchemeEntry>& entries);
    void Print(IOutputStream& o) const;

private:
    struct TColumnInfo {
        bool ValidLen = true;
        TVector<size_t> LengthValues;
        size_t LineLength = 0;
        size_t ColumnWidth = 0;
    };

    void InitializeColumnInfo(size_t maxCols, size_t minColumnWidth);
    void CalculateColumns();

    const std::vector<NScheme::TSchemeEntry>& Entries;
    TVector<TColumnInfo> ColumnInfo;
    size_t ColumnCount;
};

}
}

template <>
inline void Out<NYdb::NConsoleClient::TAdaptiveTabbedTable>(
    IOutputStream& o,
    const NYdb::NConsoleClient::TAdaptiveTabbedTable& x
) {
    return x.Print(o);
}
