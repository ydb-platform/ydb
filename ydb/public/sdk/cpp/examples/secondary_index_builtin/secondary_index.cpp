#include "secondary_index.h"

#include <util/folder/pathsplit.h>

TCommand Parse(const char * stringCmd) {

    if (!strcmp(stringCmd, "create")) {
        return TCommand::CREATE;
    } else if (!strcmp(stringCmd, "insert")) {
        return TCommand::INSERT;
    } else if (!strcmp(stringCmd, "select")) {
        return TCommand::SELECT;
    } else if (!strcmp(stringCmd, "drop")) {
        return TCommand::DROP;
    } else if (!strcmp(stringCmd, "selectjoin")) {
        return TCommand::SELECT_JOIN;
    } else {
        return TCommand::NONE;
    }

    return TCommand::NONE;
}

TString JoinPath(const TString& prefix, const TString& path) {
    if (prefix.empty()) {
        return path;
    }

    TPathSplitUnix  prefixPathSplit(prefix);
    prefixPathSplit.AppendComponent(path);

    return prefixPathSplit.Reconstruct();
}


void ParseSelectSeries(TVector<TSeries>& parseResult, TResultSetParser&& parser) {
    parseResult.clear();
    while (parser.TryNextRow()) {
        auto& series = parseResult.emplace_back();
        series.SeriesId = *parser.ColumnParser(0).GetOptionalUint64();
        series.Title = *parser.ColumnParser(1).GetOptionalUtf8();
        series.Info = *parser.ColumnParser(2).GetOptionalUtf8();
        series.ReleaseDate = *parser.ColumnParser(3).GetOptionalDatetime();
        series.Views = *parser.ColumnParser(4).GetOptionalUint64();
        series.UploadedUserId = *parser.ColumnParser(5).GetOptionalUint64();
    }
}


