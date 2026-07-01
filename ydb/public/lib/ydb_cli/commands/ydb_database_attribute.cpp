#include "ydb_database_attribute.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/cms/cms.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <memory>

namespace NYdb {
namespace NConsoleClient {

TCommandDatabaseAttribute::TCommandDatabaseAttribute()
    : TClientCommandTree("attribute", {"attr"}, "Database attributes operations")
{
    AddCommand(std::make_unique<TCommandDatabaseAttributeGet>());
    AddCommand(std::make_unique<TCommandDatabaseAttributeSet>());
    AddCommand(std::make_unique<TCommandDatabaseAttributeDel>());
}

TCommandDatabaseAttributeGet::TCommandDatabaseAttributeGet()
    : TYdbReadOnlyCommand("get", {"list"}, "Get attributes")
{}

void TCommandDatabaseAttributeGet::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsNum(0);
}

void TCommandDatabaseAttributeGet::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandDatabaseAttributeGet::Run(TConfig& config) {
    NCms::TCmsClient client(CreateDriver(config));
    auto result = client.GetDatabaseStatus(config.Database).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    const auto& attrs = result.GetAttributes();
    for (const auto& attr : attrs) {
        Cout << attr.first << ": " << attr.second << Endl;
    }

    return EXIT_SUCCESS;
}

TCommandDatabaseAttributeSet::TCommandDatabaseAttributeSet()
    : TYdbCommand("set", {}, "Set attribute(s)")
{}

void TCommandDatabaseAttributeSet::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsMin(1);
    SetFreeArgTitle(0, "<ATTRIBUTE>", "NAME=VALUE");
}

void TCommandDatabaseAttributeSet::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    for (size_t i = 0; i < config.ParseResult->GetFreeArgCount(); ++i) {
        TString attr = config.ParseResult->GetFreeArgs()[i];
        TVector<TString> items = StringSplitter(attr).Split('=').ToList<TString>();

        if (items.size() != 2) {
            throw TMisuseException()
                << "Bad format in attribute '" + attr + "'. NAME=VALUE expected";
        }

        Attributes[items.at(0)] = items.at(1);
    }
}

int TCommandDatabaseAttributeSet::Run(TConfig& config) {
    NCms::TCmsClient client(CreateDriver(config));

    NYdb::NCms::TAttributes attributes;
    for (const auto& kv : Attributes) {
        attributes.emplace(kv.first, kv.second);
    }

    auto settings = NCms::TAlterDatabaseSettings().AlterAttributes(attributes);
    auto status = client.AlterDatabase(config.Database, settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(status);
    return EXIT_SUCCESS;
}

TCommandDatabaseAttributeDel::TCommandDatabaseAttributeDel()
    : TYdbCommand("delete", {"del", "remove", "rm"}, "Delete attribute(s)")
{}

void TCommandDatabaseAttributeDel::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsMin(1);
    SetFreeArgTitle(0, "<ATTRIBUTE>", "NAME");
}

void TCommandDatabaseAttributeDel::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    for (size_t i = 0; i < config.ParseResult->GetFreeArgCount(); ++i) {
        TString key = config.ParseResult->GetFreeArgs()[i];

        Attributes.insert(key);
    }
}

int TCommandDatabaseAttributeDel::Run(TConfig& config) {
    NCms::TCmsClient client(CreateDriver(config));

    NYdb::NCms::TAttributes attributes;
    for (const auto& kv : Attributes) {
        attributes.emplace(kv, "");
    }

    auto settings = NCms::TAlterDatabaseSettings().AlterAttributes(attributes);
    auto status = client.AlterDatabase(config.Database, settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(status);
    return EXIT_SUCCESS;
}

}
}
