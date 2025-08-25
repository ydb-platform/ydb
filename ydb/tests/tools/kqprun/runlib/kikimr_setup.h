#pragma once

#include "settings.h"

#include <library/cpp/logger/backend.h>

#include <ydb/core/testlib/test_client.h>

namespace NKikimrRun {

class TKikimrSetupBase {
public:
    TKikimrSetupBase() = default;

    TAutoPtr<TLogBackend> CreateLogBackend(const TServerSettings& settings) const;

    NKikimr::Tests::TServerSettings GetServerSettings(const TServerSettings& settings, ui32 grpcPort, bool verbose);

private:
    void SetLoggerSettings(const TServerSettings& settings, NKikimr::Tests::TServerSettings& serverSettings) const;

    void SetFunctionRegistry(const TServerSettings& settings, NKikimr::Tests::TServerSettings& serverSettings) const;

protected:
    TPortManager PortManager;
};

}  // namespace NKikimrRun
