#pragma once

#include "tool_interface.h"

#include <ydb/public/lib/ydb_cli/common/colors.h>

namespace NYdb::NConsoleClient::NAi {

class TToolBase : public ITool {
protected:
    inline const static NColorizer::TColors Colors = NConsoleClient::AutoColors(Cout);

public:
    TToolBase(const NJson::TJsonValue& parametersSchema, const TString& description);

    const NJson::TJsonValue& GetParametersSchema() const final;

    const TString& GetDescription() const final;

    TResponse Execute(const NJson::TJsonValue& parameters) final;

protected:
    virtual void ParseParameters(const NJson::TJsonValue& parameters) = 0;

    virtual bool AskPermissions() = 0;

    virtual TResponse DoExecute() = 0;

private:
    const NJson::TJsonValue ParametersSchema;
    const TString Description;
};

class TDatabaseToolBase : public TToolBase {
    using TBase = TToolBase;

public:
    TDatabaseToolBase(const TString& database, const NJson::TJsonValue& parametersSchema, const TString& description);

protected:
    TString CanonizePath(const TString& path) const;

    const TString Database;
};

} // namespace NYdb::NConsoleClient::NAi
