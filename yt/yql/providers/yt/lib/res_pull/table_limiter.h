#pragma once

#include <util/system/defaults.h>

namespace NYql {

struct TRecordsRange;

class TTableLimiter {
public:
    TTableLimiter(const TRecordsRange& range);

    bool NextTable(ui64 recordCount);
    void NextDynamicTable();
    inline ui64 GetTableStart() const {
        return TableStart;
    }
    inline ui64 GetTableEnd() const {
        return TableEnd;
    }
    ui64 GetTableZEnd() const;
    inline bool Exceed() const {
        return Current >= End;
    }
    inline explicit operator bool() const {
        return !Exceed();
    }
    void Skip(ui64 recordCount);
private:
    ui64 Start;
    ui64 End;
    ui64 Current;
    ui64 TableStart;
    ui64 TableEnd;
};

} // NYql
