#include "ydb_database_user_attribute.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/cms/cms.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <memory>

namespace NYdb {
namespace NConsoleClient {

TCommandDatabaseUserAttribute::TCommandDatabaseUserAttribute()
    : TClientCommandTree("user-attribute", { "ua" }, "User attribute operations")
{
    AddCommand(std::make_unique<TCommandDatabaseUserAttributeGet>());
    AddCommand(std::make_unique<TCommandDatabaseUserAttributeSet>());
    AddCommand(std::make_unique<TCommandDatabaseUserAttributeDel>());
}

TCommandDatabaseUserAttributeGet::TCommandDatabaseUserAttributeGet()
    : TYdbReadOnlyCommand("get", {}, "Get user attributes")
{}

void TCommandDatabaseUserAttributeGet::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsNum(0);
}

void TCommandDatabaseUserAttributeGet::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandDatabaseUserAttributeGet::Run(TConfig& config) {
    NCms::TCmsClient client(CreateDriver(config));
    auto result = client.GetDatabaseStatus(config.Database).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    const auto& attrs = result.GetUserAttributes();
    for (const auto& attr : attrs) {
        Cout << attr.first << ": " << attr.second << Endl;
    }

    return EXIT_SUCCESS;
}

TCommandDatabaseUserAttributeSet::TCommandDatabaseUserAttributeSet()
    : TYdbCommand("set", {}, "Set user attribute(s)")
{}

void TCommandDatabaseUserAttributeSet::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsMin(1);
    SetFreeArgTitle(0, "<ATTRIBUTE>", "NAME=VALUE");
}

void TCommandDatabaseUserAttributeSet::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    for (size_t i = 0; i < config.ParseResult->GetFreeArgCount(); ++i) {
        TString attr = config.ParseResult->GetFreeArgs()[i];
        TVector<TString> items = StringSplitter(attr).Split('=').ToList<TString>();

        if (items.size() != 2) {
            throw TMisuseException()
                << "Bad format in attribute '" + attr + "'";
        }

        Attributes[items.at(0)] = items.at(1);
    }
}

int TCommandDatabaseUserAttributeSet::Run(TConfig& config) {
    NCms::TCmsClient client(CreateDriver(config));

    NYdb::NCms::TUserAttributes attributes;
    for (const auto& kv : Attributes) {
        attributes.emplace(kv.first, kv.second);
    }

    auto settings = NCms::TAlterDatabaseSettings().AlterAttributes(attributes);
    auto status = client.AlterDatabase(config.Database, settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(status);
    return EXIT_SUCCESS;
}

TCommandDatabaseUserAttributeDel::TCommandDatabaseUserAttributeDel()
    : TYdbCommand("del", {}, "Delete user attribute(s)")
{}

void TCommandDatabaseUserAttributeDel::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsMin(1);
    SetFreeArgTitle(0, "<ATTRIBUTE>", "NAME");
}

void TCommandDatabaseUserAttributeDel::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    for (size_t i = 0; i < config.ParseResult->GetFreeArgCount(); ++i) {
        TString key = config.ParseResult->GetFreeArgs()[i];

        Attributes.insert(key);
    }
}

int TCommandDatabaseUserAttributeDel::Run(TConfig& config) {
    NCms::TCmsClient client(CreateDriver(config));

    NYdb::NCms::TUserAttributes attributes;
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
