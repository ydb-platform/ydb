#pragma once
#include "linter.h"

namespace NYql {
namespace NFastCheck {

class ICheckRunner {
public:
    virtual ~ICheckRunner() = default;

    virtual TString GetCheckName() const = 0;
    virtual TCheckResponse Run(const TChecksRequest& request) = 0;
};

class ICheckRunnerFactory {
public:
    virtual ~ICheckRunnerFactory() = default;

    virtual std::unique_ptr<ICheckRunner> MakeRunner(const TString& checkName) const = 0;
};

std::unique_ptr<ICheckRunner> MakeLexerRunner();
std::unique_ptr<ICheckRunner> MakeParserRunner();
std::unique_ptr<ICheckRunner> MakeTranslatorRunner();
std::unique_ptr<ICheckRunner> MakeFormatRunner();

}
}
