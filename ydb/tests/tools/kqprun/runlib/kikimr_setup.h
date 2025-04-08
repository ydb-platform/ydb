#pragma once

#include "settings.h"

#include <library/cpp/logger/backend.h>

#include <ydb/core/testlib/test_client.h>

namespace NKikimrRun {

class TKikimrSetupBase {
public:
    TKikimrSetupBase(const TServerSettings& settings);

    TAutoPtr<TLogBackend> CreateLogBackend() const;

    NKikimr::Tests::TServerSettings GetServerSettings(ui32 grpcPort, bool verbose);

private:
    void SetLoggerSettings(NKikimr::Tests::TServerSettings& serverSettings) const;

    void SetFunctionRegistry(NKikimr::Tests::TServerSettings& serverSettings) const;

protected:
    TPortManager PortManager;

private:
    const TServerSettings& Settings;
};

}  // namespace NKikimrRun
