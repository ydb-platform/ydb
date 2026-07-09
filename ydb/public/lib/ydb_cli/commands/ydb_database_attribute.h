#pragma once

#include "ydb_command.h"

#include <util/generic/set.h>
#include <util/generic/map.h>
#include <util/generic/string.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandDatabaseAttribute : public TClientCommandTree {
public:
    TCommandDatabaseAttribute();
};

class TCommandDatabaseAttributeGet : public TYdbReadOnlyCommand {
public:
    TCommandDatabaseAttributeGet();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;
};

class TCommandDatabaseAttributeSet : public TYdbCommand {
public:
    TCommandDatabaseAttributeSet();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TMap<TString, TString> Attributes;
};

class TCommandDatabaseAttributeDel : public TYdbCommand {
public:
    TCommandDatabaseAttributeDel();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TSet<TString> Attributes;
};

}
}
