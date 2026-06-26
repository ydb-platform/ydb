#pragma once

#include "ydb_command.h"

#include <util/generic/set.h>
#include <util/generic/map.h>
#include <util/generic/string.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandDatabaseUserAttribute : public TClientCommandTree {
public:
    TCommandDatabaseUserAttribute();
};

class TCommandDatabaseUserAttributeGet : public TYdbReadOnlyCommand {
public:
    TCommandDatabaseUserAttributeGet();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;
};

class TCommandDatabaseUserAttributeSet : public TYdbCommand {
public:
    TCommandDatabaseUserAttributeSet();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TMap<TString, TString> Attributes;
};

class TCommandDatabaseUserAttributeDel : public TYdbCommand {
public:
    TCommandDatabaseUserAttributeDel();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TSet<TString> Attributes;
};

}
}
