#pragma once

#include <util/generic/string.h>
#include <library/cpp/logger/priority.h>

struct TOptions {
    TString Endpoint;
    TString Database;
    TString TopicPath;
    TString ConsumerName;
    bool UseSecureConnection = false;
    TString TablePath;
    ELogPriority LogPriority = TLOG_WARNING;

    TOptions(int argc, const char* argv[]);
};
