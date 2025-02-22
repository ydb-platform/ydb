#include "secondary_index.h"

#include <filesystem>

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

std::string JoinPath(const std::string& prefix, const std::string& path) {
    if (prefix.empty()) {
        return path;
    }

    std::filesystem::path prefixPathSplit(prefix);
    prefixPathSplit /= path;

    return prefixPathSplit;
}


void ParseSelectSeries(std::vector<TSeries>& parseResult, TResultSetParser&& parser) {
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


