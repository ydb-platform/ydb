#include "auth.h"

#include <ydb/core/base/appdata.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/services/scheme_secret/resolver.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <sys/stat.h>
#include <util/stream/file.h>
#include <util/thread/pool.h>

#include <atomic>
#include <fcntl.h>
#include <unistd.h>

namespace NFq::NWasmServices {
namespace {
// Node-wide admission remains held until physical completion, even if the
// query detaches. Cancellation cannot create an unbounded orphan queue.
std::atomic<ui32> PendingSecrets{ 0 };

struct TRequest {
    std::atomic<bool> Cancelled{ false };
    NAsync::ITransport::TCompletion Completion;
    TInstant Deadline;

    ~TRequest() {
        --PendingSecrets;
    }

    void Reply(TString value = {}) {
        if (Cancelled.exchange(true)) {
            return;
        }
        const bool ready = Deadline > TInstant::Now() && !value.empty() && value.size() <= 4096;
        Completion(ready ? NAsync::EOperationStatus::Ready : NAsync::EOperationStatus::Failed, ready ? std::move(value) : TString());
    }
};

TThreadPool& FilePool() {
    struct TPool {
        TThreadPool Pool{ "WasmAuthFile" };

        TPool() {
            Pool.Start(2, 128);
        }
    };

    static TPool pool;
    return pool.Pool;
}

TString ReadSecret(const TString& file, const TString& reference) {
    // O_NONBLOCK plus fstat prevents accidentally blocking on a FIFO/device.
    const int fd = open(file.c_str(), O_RDONLY | O_NONBLOCK | O_CLOEXEC);
    Y_ENSURE(fd >= 0, "Secret source unavailable");
    TFile input(fd);
    struct stat st;
    Y_ENSURE(fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size <= (1 << 20), "Invalid secret source");
    NJson::TJsonValue json;
    // Bound the actual read too, including a concurrent file growth.
    TString bytes;
    bytes.resize((1 << 20) + 1);
    bytes.resize(TFileInput(input).Load(bytes.begin(), bytes.size()));
    Y_ENSURE(bytes.size() <= (1 << 20), "Secret source exceeds limit");
    Y_ENSURE(NJson::ReadJsonTree(bytes, &json, true) && json.IsMap(), "Invalid secret source");
    const auto it = json.GetMap().find(reference);
    Y_ENSURE(it != json.GetMap().end() && it->second.IsString(), "Secret unavailable");
    return it->second.GetString();
}
}   // namespace

TAuthSecretResolver CreateAuthSecretResolver(const NConfig::TWasmServicesConfig& config, const THashMap<TString, TString>& secureParams) {
    const TString file = config.GetAuthSecretsFile();
    TIntrusiveConstPtr<NACLib::TUserToken> token;
    TString database;
    auto* system = NActors::TActivationContext::ActorSystem();
    if (file.empty()) {
        const auto identity = secureParams.find(TString(AuthIdentityKey));
        const auto db = secureParams.find(TString(AuthDatabaseKey));
        Y_ENSURE(identity != secureParams.end() && db != secureParams.end() && !db->second.empty(),
            "Auth secret resolver requires execution identity and database");
        token = MakeIntrusive<NACLib::TUserToken>(Base64Decode(identity->second));
        Y_ENSURE(!token->GetUserSID().empty(), "Auth secret resolver requires an authenticated identity");
        database = db->second;
    } else {
        FilePool();
    }
    return [file, token, database, system](const TString& reference, TInstant deadline, auto completion) -> NAsync::ITransport::TCancel {
        if (PendingSecrets.fetch_add(1) >= 128) {
            --PendingSecrets;
            completion(NAsync::EOperationStatus::Failed, {});
            return {};
        }
        auto request = std::make_shared<TRequest>();
        request->Completion = std::move(completion);
        request->Deadline = deadline;
        if (!file.empty()) {
            if (!FilePool().AddFunc([request, file, reference] {
                    if (request->Cancelled || request->Deadline <= TInstant::Now()) {
                        request->Reply();
                        return;
                    }
                    try {
                        request->Reply(ReadSecret(file, reference));
                    } catch (...) {
                        request->Reply();
                    }
                })) {
                request->Reply();
            }
        } else {
            try {
                Y_ENSURE(reference.StartsWith('/'), "WASM auth requires schema secret paths");
                Y_ENSURE(NKikimr::NSecret::UseSchemaSecrets(NKikimr::AppData(system)->FeatureFlags, reference), "Schema secrets are disabled");
                NKikimr::NSecret::DescribeSecret(
                    { reference }, token, database, system, { .RetryPolicy = NKikimr::NSecret::MakeShortRetryPolicy() })
                    .Subscribe([request](const auto& future) {
                        try {
                            const auto& value = future.GetValue();
                            if (value.Status == Ydb::StatusIds::SUCCESS && value.SecretValues.size() == 1) {
                                request->Reply(value.SecretValues[0]);
                            } else {
                                request->Reply();
                            }
                        } catch (...) {
                            request->Reply();
                        }
                    });
            } catch (...) {
                request->Reply();
            }
        }
        // Runtime also guards against callback/cancel races with weak operation state.
        return [weak = std::weak_ptr<TRequest>(request)] {
            if (auto request = weak.lock()) {
                request->Cancelled = true;
            }
        };
    };
}
}   // namespace NFq::NWasmServices
