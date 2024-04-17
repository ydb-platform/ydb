#pragma once

#include "cli.h"
#include <ydb/core/driver_lib/cli_base/cli_cmds.h>

namespace NKikimr {
namespace NDriverClient {

class TClientCommandAdmin : public TClientCommandTree {
public:
    TClientCommandAdmin();
};

class TClientCommandBlobStorage : public TClientCommandTree {
public:
    TClientCommandBlobStorage();
};

class TClientCommandDebug : public TClientCommandTree {
public:
    TClientCommandDebug();
};

class TClientCommandTablet : public TClientCommandTree {
public:
    TClientCommandTablet();
};

class TClientCommandNode : public TClientCommandTree {
public:
    TClientCommandNode();
};

class TClientCommandTenant : public TClientCommandTree {
public:
    TClientCommandTenant();
};

class TClientCommandConsole : public TClientCommandTree {
public:
    TClientCommandConsole();
};

class TClientCommandCms : public TClientCommandTree {
public:
    TClientCommandCms();
};

class TClientCommandConfig : public TClientCommandTree {
public:
    TClientCommandConfig();
};

}
}
