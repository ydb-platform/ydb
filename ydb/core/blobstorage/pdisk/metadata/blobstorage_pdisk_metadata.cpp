#include "blobstorage_pdisk_metadata.h"
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_actorsystem_creator.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/openssl/crypto/sha.h>
#include <future>

namespace NKikimr {
namespace NPDisk {

namespace {
    constexpr ui32 kNodeIdForMetadata = 1;
    constexpr ui32 kPDiskIdForMetadata = 1;
    constexpr ui32 kMetaTimeoutSeconds = 10;
    
    static NPDisk::TMainKey GetEffectiveMainKey(const NPDisk::TMainKey& mainKey) {
        NPDisk::TMainKey result = mainKey;
        if (!result.IsInitialized || result.Keys.empty()) {
            result.Initialize();
        }
        return result;
    }
    
    static TIntrusivePtr<TPDiskConfig> MakeMetadataPDiskConfig(const TString& path, ui32 pdiskId, bool readOnly, TStringStream& details) {
        std::optional<NPDisk::TDriveData> driveData = NPDisk::GetDriveData(path, &details);
        const NPDisk::EDeviceType deviceType = driveData ? driveData->DeviceType : NPDisk::DEVICE_TYPE_ROT;
        const ui64 category = static_cast<ui64>(TPDiskCategory(deviceType, 0).GetRaw());
    
        auto cfg = MakeIntrusive<TPDiskConfig>(path, static_cast<ui64>(0), static_cast<ui32>(pdiskId), category);
        cfg->ReadOnly = readOnly;
        cfg->MetadataOnly = true;
        return cfg;
    }
    
    static NActors::TActorId RegisterPDiskActor(NActors::TActorSystem* sys,
                                               const TIntrusivePtr<TPDiskConfig>& cfg,
                                               const NPDisk::TMainKey& mainKey,
                                               const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
        IActor* pdiskActorImpl = CreatePDisk(cfg, mainKey, counters);
        const NActors::TActorId pdiskActor = sys->Register(pdiskActorImpl);
        const NActors::TActorId pdiskActorId = MakeBlobStoragePDiskID(/*nodeId*/kNodeIdForMetadata, /*pdiskId*/kPDiskIdForMetadata);
        sys->RegisterLocalService(pdiskActorId, pdiskActor);
        return pdiskActorId;
    }
    
    template <typename TFut>
    static inline void WaitOrThrow(TFut& fut, int timeoutSeconds, const TString& details, const char* action) {
        if (fut.wait_for(std::chrono::seconds(timeoutSeconds)) != std::future_status::ready) {
            ythrow yexception()
                << "PDisk metadata " << action << " timeout after " << timeoutSeconds << "s\n"
                << "Details: " << details;
        }
    }
    
} // namespace

NKikimrBlobStorage::TPDiskMetadataRecord ReadPDiskMetadata(const TString& path, const NPDisk::TMainKey& mainKey) {
    TActorSystemCreator creator;
    auto* sys = creator.GetActorSystem();
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();

    TStringStream details;

    auto pdiskCfg = MakeMetadataPDiskConfig(path, kPDiskIdForMetadata, /*readOnly*/true, details);

    const NPDisk::TMainKey effectiveMainKey = GetEffectiveMainKey(mainKey);

    const NActors::TActorId pdiskActorId = RegisterPDiskActor(sys, pdiskCfg, effectiveMainKey, counters);

    NKikimrBlobStorage::TPDiskMetadataRecord out;

    class TMetadataReader : public NActors::TActorBootstrapped<TMetadataReader> {
        const NActors::TActorId PDiskActorId;
        NKikimrBlobStorage::TPDiskMetadataRecord& Out;
        std::promise<bool>& Done;
        TString& Error;

        static const char* OutcomeToStr(NPDisk::EPDiskMetadataOutcome oc) {
            switch (oc) {
                case NPDisk::EPDiskMetadataOutcome::OK: return "OK";
                case NPDisk::EPDiskMetadataOutcome::NO_METADATA: return "NO_METADATA";
                case NPDisk::EPDiskMetadataOutcome::ERROR: return "ERROR";
            }
            return "UNKNOWN";
        }
    public:
        TMetadataReader(NActors::TActorId pdiskActorId,
                        NKikimrBlobStorage::TPDiskMetadataRecord& out,
                        std::promise<bool>& done,
                        TString& error)
            : PDiskActorId(pdiskActorId)
            , Out(out)
            , Done(done)
            , Error(error)
        {}

        void Bootstrap() {
            Send(PDiskActorId, new NPDisk::TEvReadMetadata());
            Become(&TThis::StateFunc, TDuration::Seconds(kMetaTimeoutSeconds), new TEvents::TEvWakeup);
        }

        void Handle(NPDisk::TEvReadMetadataResult::TPtr ev) {
            auto* msg = ev->Get();
            bool ok = false;
            switch (msg->Outcome) {
                case NPDisk::EPDiskMetadataOutcome::OK: {
                    TRope rope(std::move(msg->Metadata));
                    TRopeStream stream(rope.begin(), rope.size());
                    ok = Out.ParseFromZeroCopyStream(&stream);
                    if (!ok) {
                        Error = "PARSE_FAILED";
                    }
                    break;
                }
                case NPDisk::EPDiskMetadataOutcome::NO_METADATA: {
                    Error = OutcomeToStr(msg->Outcome);
                    ok = false;
                    break;
                }
                case NPDisk::EPDiskMetadataOutcome::ERROR: {
                    Error = OutcomeToStr(msg->Outcome);
                    ok = false;
                    break;
                }
            }
            Done.set_value(ok);
            PassAway();
        }

        void HandleWakeup() {
            Error = "TIMEOUT";
            Done.set_value(false);
            PassAway();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(NPDisk::TEvReadMetadataResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        )
    };

    std::promise<bool> done;
    auto fut = done.get_future();
    TString error;
    sys->Register(new TMetadataReader(pdiskActorId, out, done, error));

    WaitOrThrow(fut, 10, details.Str(), "read");
    bool ok = fut.get();
    if (!ok) {
        ythrow yexception()
            << "PDisk metadata read failed: " << (!error.empty() ? error : TString("ERROR")) << "\n"
            << "Details: " << details.Str();
    }
    return out;
}

static void UpdateStorageConfigFingerprint(NKikimrBlobStorage::TStorageConfig* config) {
    if (!config) {
        return;
    }
    config->ClearFingerprint();
    TString s;
    const bool success = config->SerializeToString(&s);
    Y_ABORT_UNLESS(success);
    auto digest = NOpenSsl::NSha1::Calc(s.data(), s.size());
    config->SetFingerprint(digest.data(), digest.size());
}

void WritePDiskMetadata(const TString& path, const NKikimrBlobStorage::TPDiskMetadataRecord& record, const NPDisk::TMainKey& mainKey) {
    NKikimrBlobStorage::TPDiskMetadataRecord adjustedRecord(record);

    const NPDisk::TMainKey effectiveMainKey = GetEffectiveMainKey(mainKey);

    NKikimrBlobStorage::TPDiskMetadataRecord previous;
    bool havePrevious = false;
    try {
        previous = ReadPDiskMetadata(path, effectiveMainKey);
        havePrevious = true;
    } catch (...) {
        havePrevious = false;
    }

    if (adjustedRecord.HasCommittedStorageConfig()) {
        auto* cfg = adjustedRecord.MutableCommittedStorageConfig();
        if (!cfg->HasPrevConfig() && havePrevious) {
            if (previous.HasCommittedStorageConfig()) {
                cfg->MutablePrevConfig()->CopyFrom(previous.GetCommittedStorageConfig());
            }
        }
        UpdateStorageConfigFingerprint(cfg);
    }

    TActorSystemCreator creator;
    auto* sys = creator.GetActorSystem();
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();

    TStringStream details;

    auto pdiskCfg = MakeMetadataPDiskConfig(path, kPDiskIdForMetadata, /*readOnly*/false, details);

    const NActors::TActorId pdiskActorId = RegisterPDiskActor(sys, pdiskCfg, effectiveMainKey, counters);

    class TWriter : public NActors::TActorBootstrapped<TWriter> {
        const NActors::TActorId PDiskActorId;
        const NKikimrBlobStorage::TPDiskMetadataRecord& Record;
        std::promise<bool>& Done;
    public:
        TWriter(NActors::TActorId pdiskActorId,
                const NKikimrBlobStorage::TPDiskMetadataRecord& record,
                std::promise<bool>& done)
            : PDiskActorId(pdiskActorId)
            , Record(record)
            , Done(done)
        {}

        void Bootstrap() {
            TString data;
            if (!Record.SerializeToString(&data)) {
                Done.set_value(false);
                PassAway();
                return;
            }
            Send(PDiskActorId, new NPDisk::TEvWriteMetadata(TRcBuf(std::move(data))));
            Become(&TThis::StateFunc, TDuration::Seconds(kMetaTimeoutSeconds), new TEvents::TEvWakeup);
        }

        void Handle(NPDisk::TEvWriteMetadataResult::TPtr ev) {
            bool ok = (ev->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);
            Done.set_value(ok);
            PassAway();
        }

        void HandleWakeup() {
            Done.set_value(false);
            PassAway();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(NPDisk::TEvWriteMetadataResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        )
    };

    std::promise<bool> done;
    auto fut = done.get_future();
    sys->Register(new TWriter(pdiskActorId, adjustedRecord, done));

    WaitOrThrow(fut, 10, details.Str(), "write");
    if (!fut.get()) {
        ythrow yexception()
            << "PDisk metadata write failed\n"
            << "Details: " << details.Str();
    }
}

} // NPDisk
} // NKikimr
