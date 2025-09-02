#include "secondary_index.h"

#include <filesystem>

std::string GetCmdList() {
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

std::string JoinPath(const std::string& prefix, const std::string& path) {
    if (prefix.empty()) {
        return path;
    }

    std::filesystem::path prefixPathSplit(prefix);
    prefixPathSplit /= path;

    return prefixPathSplit;
}
