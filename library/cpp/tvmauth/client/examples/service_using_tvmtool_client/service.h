#pragma once

#include <library/cpp/http/io/stream.h>

#include <util/generic/ptr.h>

#include <unordered_set>

namespace NTvmAuth {
    class TTvmClient;
}

namespace NExample {
    struct TConfig {
        using TAllowedTvmIds = std::unordered_set<ui32>;

        TAllowedTvmIds AllowedTvmIds;
    };

    class TSomeService {
    public:
        TSomeService(const TConfig& cfg);
        ~TSomeService();

        void HandleRequest(THttpInput& in, THttpOutput& out);

    private:
        TString GetDataFromBackendC(const TString& userTicket);

    private:
        // WARNING: См. Здесь
        TConfig Config_;
        THolder<NTvmAuth::TTvmClient> Tvm_;
    };
}
