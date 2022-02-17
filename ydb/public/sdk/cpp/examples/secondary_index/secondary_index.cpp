#include "secondary_index.h"

#include <util/folder/pathsplit.h>

TString GetCmdList() {
    return "create_tables, drop_tables, update_views, list, generate, delete";
}

ECmd ParseCmd(const char* cmd) {
    if (!strcmp(cmd, "create_tables")) {
        return ECmd::CREATE_TABLES;
    }
    if (!strcmp(cmd, "drop_tables")) {
        return ECmd::DROP_TABLES;
    }
    if (!strcmp(cmd, "update_views")) {
        return ECmd::UPDATE_VIEWS;
    }
    if (!strcmp(cmd, "list")) {
        return ECmd::LIST_SERIES;
    }
    if (!strcmp(cmd, "generate")) {
        return ECmd::GENERATE_SERIES;
    }
    if (!strcmp(cmd, "delete")) {
        return ECmd::DELETE_SERIES;
    }
    return ECmd::NONE;
}

TString JoinPath(const TString& prefix, const TString& path) {
    if (prefix.empty()) {
        return path;
    }

    TPathSplitUnix prefixPathSplit(prefix);
    prefixPathSplit.AppendComponent(path);

    return prefixPathSplit.Reconstruct();
}
