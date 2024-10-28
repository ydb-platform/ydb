#include "util.h"

#include <util/generic/map.h>
#include <util/generic/singleton.h>
#include <util/generic/yexception.h>
#include <util/generic/ymath.h>
#include <util/string/cast.h>

#include <ctype.h>

namespace NYdb {

namespace NBackup {

struct TLog {
    std::shared_ptr<::TLog> Log;

    TLog()
        : Log(std::make_shared<::TLog>(CreateLogBackend("cerr")))
    {}
};

void SetLog(const std::shared_ptr<::TLog>& log) {
    Singleton<TLog>()->Log = log;
}

const std::shared_ptr<::TLog>& GetLog() {
    return Singleton<TLog>()->Log;
}

} // NBackup

TString RelPathFromAbsolute(TString db, TString path) {
    if (!db.StartsWith('/')) {
        db.prepend('/');
    }

    if (db.EndsWith('/')) {
        db.pop_back();
    }

    TString info = TStringBuilder() << "db# " << db.Quote() << " path# " << path.Quote();

    if (!path.StartsWith("/")) {
        throw yexception() << "path should be absolute, " << info;
    }

    if (!path.StartsWith(db)) {
        throw yexception() << "the path should starts with a name of the database, " << info;
    }

    db.push_back('/');
    path = path.erase(0, Min(path.size(), db.size()));
    return path ? path : "/";
}

namespace {

template<typename T>
static constexpr T pow(T x, size_t p) {
    T res = 1;
    for (size_t i = 0; i < p; ++i) {
        res *= x;
    }
    return res;
}

TMap<TStringBuf, ui64> SizeSuffix {
    {"", 1},
    {"K", Power<ui64>(1000, 1)},
    {"M", Power<ui64>(1000, 2)},
    {"G", Power<ui64>(1000, 3)},
    {"T", Power<ui64>(1000, 4)},
    {"Ki", Power<ui64>(1024, 1)},
    {"KiB", Power<ui64>(1024, 1)},
    {"Mi", Power<ui64>(1024, 2)},
    {"MiB", Power<ui64>(1024, 2)},
    {"Gi", Power<ui64>(1024, 3)},
    {"GiB", Power<ui64>(1024, 3)},
    {"Ti", Power<ui64>(1024, 4)},
    {"TiB", Power<ui64>(1024, 4)},
};

}

ui64 SizeFromString(TStringBuf s) {
    size_t pos = s.size();
    while (pos > 0 && !isdigit(s[pos - 1])) {
        --pos;
    }

    TStringBuf suffix;
    TStringBuf number;
    s.SplitAt(pos, number, suffix);
    auto it = SizeSuffix.find(suffix);
    Y_ENSURE(it != SizeSuffix.end(), "Cannot parse string, unknown suffix# " << TString{suffix}.Quote());
    return FromString<ui64>(number) * it->second;
}

}
