#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/events/get_object.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/client/ClientConfiguration.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/threading/Executor.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/auth/AWSCredentials.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/GetObjectRequest.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/queue.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/ptr.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/stream/input.h>
#include <util/system/datetime.h>
#include <util/system/env.h>
#include <util/system/sanitizers.h>
#include <util/folder/path.h>
#include <util/string/printf.h>

using namespace NActors;
using namespace NKikimr::NWrappers;
using namespace NKikimr::NWrappers::NExternalStorage;

struct TConfig {
    TString Bucket;
    TString Endpoint;
    TString Region = "ru-central1";
    TString Scheme = "https";
    bool VerifySSL = true;
    bool PathStyle = false;
    ui32 Threads = 32;
    ui32 Concurrency = 32;
    ui64 SizeMiBPerFile = 512;
    TString KeysFile = "keys.txt";
    ui64 RangeOffsetBytes = 0;     // custom start offset
    ui64 RangeSizeBytes = 0;       // custom size; if 0, use SizeMiBPerFile
    TString SaveDir;               // empty => don't write
    TString AccessKey;
    TString SecretKey;
};

static THolder<TActorSystemSetup> BuildActorSystemSetup(ui32 threads) {
    auto setup = MakeHolder<TActorSystemSetup>();
    setup->NodeId = 1;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
    setup->Executors[0] = new TBasicExecutorPool(0, threads, 50);
    setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 0));
    return setup;
}

class TS3BenchActor final : public TActorBootstrapped<TS3BenchActor> {
    struct TEvStart : public TEventLocal<TEvStart, EventSpaceBegin(NActors::TEvents::ES_PRIVATE)> {};
public:
    TS3BenchActor(const TActorId& s3Wrapper,
                  TVector<TString>&& keys,
                  const TString& bucket,
                  ui32 concurrency,
                  ui64 sizeBytes,
                  ui64 startOffsetBytes,
                  const NThreading::TPromise<void>& donePromise,
                  const TString& saveDir)
        : S3Wrapper(s3Wrapper)
        , Keys(std::move(keys))
        , Bucket(bucket)
        , MaxInFlight(concurrency)
        , RangeSizeBytes(sizeBytes)
        , StartOffsetBytes(startOffsetBytes)
        , SaveDir(saveDir)
        , Done(donePromise)
    {}

    void Bootstrap() {
        StartTime = TInstant::Now();
        Send(SelfId(), new TEvStart());
        Become(&TThis::StateWork);
    }

    STRICT_STFUNC(StateWork,
        cFunc(TEvents::TSystem::PoisonPill, PassAway);
        cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        hFunc(TEvStart, HandleStart);
        hFunc(TEvGetObjectResponse, HandleGetObject);
    )

private:
    static TString SanitizeFileName(const TString& key) {
        TString out(key);
        for (char& c : out) {
            if (c == '/' || c == '\\') {
                c = '_';
            }
        }

        return out;
    }

    void HandleStart(TEvStart::TPtr&) {
        for (; InFlight < MaxInFlight && NextIndex < Keys.size(); ++InFlight, ++NextIndex) {
            SendNext(Keys[NextIndex]);
        }

        CheckDone();
    }

    void HandleGetObject(TEvGetObjectResponse::TPtr& ev) {
        --InFlight;
        ++Completed;
        if (ev->Get()->IsSuccess()) {
            BytesRead += ev->Get()->Body.size();
            if (SaveDir) {
                TString name = ev->Get()->Key ? *ev->Get()->Key : ToString(Completed);
                TString path = TFsPath(SaveDir) / SanitizeFileName(name);
                try {
                    TFileOutput out(path);
                    out.Write(ev->Get()->Body);
                    out.Finish();
                } catch (...) {
                    ++Errors;
                }
            }
        } else {
            if (/* verbose */ false) {
            }

            ++Errors;
        }

        if (NextIndex < Keys.size()) {
            ++InFlight;
            SendNext(Keys[NextIndex++]);
        }

        CheckDone();
    }

    void HandleWakeup() {
        CheckDone();
    }

    void SendNext(const TString& key) {
        Aws::S3::Model::GetObjectRequest req;
        req.WithBucket(Bucket.c_str());
        req.WithKey(key.c_str());
        const ui64 start = StartOffsetBytes;
        const ui64 length = RangeSizeBytes ? RangeSizeBytes : 1;
        const ui64 end = start + length - 1;
        req.WithRange(Sprintf("bytes=%" PRIu64 "-%" PRIu64, start, end).c_str());
        Send(S3Wrapper, new TEvGetObjectRequest(req));
    }

    void CheckDone() {
        if (Completed == Keys.size() && InFlight == 0) {
            const TInstant end = TInstant::Now();
            const ui64 dtSec = Max<ui64>((end - StartTime).Seconds(), 1);
            const double mib = double(BytesRead) / (1024.0 * 1024.0);
            const double speedMiB = mib / double(dtSec);
            Cout << "Speed: " << Sprintf("%.6f", speedMiB) << " MiB/s" << Endl;
            Cout << "Time: " << dtSec << " s  (files=" << Keys.size()
                 << ", conc=" << MaxInFlight << ", errors=" << Errors << ")" << Endl;
            Done.SetValue();
        }
    }

private:
    const TActorId S3Wrapper;
    TVector<TString> Keys;
    const TString Bucket;
    const ui32 MaxInFlight;
    const ui64 RangeSizeBytes;
    const ui64 StartOffsetBytes;
    const TString SaveDir;

    ui64 BytesRead = 0;
    ui32 InFlight = 0;
    ui64 Completed = 0;
    ui64 Errors = 0;
    ui64 NextIndex = 0;
    TInstant StartTime;
    NThreading::TPromise<void> Done;
};

static TVector<TString> ReadKeys(const TString& path) {
    TVector<TString> keys;
    TFileInput in(path);
    TString line;
    while (in.ReadLine(line)) {
        if (!line.empty()) {
            keys.push_back(line);
        }
    }

    return keys;
}

i32 main(i32 argc, const char** argv) {
    TConfig cfg;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption("bucket", "S3 bucket").Required().StoreResult(&cfg.Bucket);
    opts.AddLongOption("endpoint", "S3 endpoint (e.g. https://storage.yandexcloud.net)").Required().StoreResult(&cfg.Endpoint);
    opts.AddLongOption("region", "S3 region").StoreResult(&cfg.Region);
    opts.AddLongOption("scheme", "http or https").StoreResult(&cfg.Scheme);
    opts.AddLongOption("verify-ssl", "verify SSL (true/false)").StoreResult(&cfg.VerifySSL);
    opts.AddLongOption("threads", "S3 client threads/max connections").StoreResult(&cfg.Threads);
    opts.AddLongOption("concurrency", "parallel GetObject requests").StoreResult(&cfg.Concurrency);
    opts.AddLongOption("size-mib-per-file", "MiB read per object (range from 0)").StoreResult(&cfg.SizeMiBPerFile);
    opts.AddLongOption("keys", "path to keys.txt").StoreResult(&cfg.KeysFile);
    opts.AddLongOption("path-style", "use path-style addressing (disable virtual addressing)").NoArgument().SetFlag(&cfg.PathStyle);
    opts.AddLongOption("range-offset-bytes", "read range start offset in bytes").StoreResult(&cfg.RangeOffsetBytes);
    opts.AddLongOption("range-size-bytes", "read range size in bytes (overrides size-mib-per-file)").StoreResult(&cfg.RangeSizeBytes);
    opts.AddLongOption("save-dir", "directory to save downloaded data").StoreResult(&cfg.SaveDir);
    opts.AddLongOption("access-key", "AWS access key").StoreResult(&cfg.AccessKey);
    opts.AddLongOption("secret-key", "AWS secret key").StoreResult(&cfg.SecretKey);

    NLastGetopt::TOptsParseResult pr(&opts, argc, argv);

    if (!cfg.AccessKey) {
        cfg.AccessKey = GetEnv("AWS_ACCESS_KEY_ID");
    }

    if (!cfg.SecretKey) {
        cfg.SecretKey = GetEnv("AWS_SECRET_ACCESS_KEY");
    }

    auto keys = ReadKeys(cfg.KeysFile);
    if (keys.empty()) {
        Cerr << "keys file is empty: " << cfg.KeysFile << Endl;
        return 1;
    }

    if (cfg.SaveDir) {
        TFsPath outDir(cfg.SaveDir);
        if (!outDir.Exists()) {
            outDir.MkDirs();
        }
    }

    Aws::SDKOptions awsOptions;
    Aws::InitAPI(awsOptions);

    i32 exitCode = 0;
    try {
        NKikimrSchemeOp::TS3Settings settings;
        settings.SetBucket(cfg.Bucket);
        settings.SetEndpoint(cfg.Endpoint);
        settings.SetRegion(cfg.Region);
        settings.SetAccessKey(cfg.AccessKey);
        settings.SetSecretKey(cfg.SecretKey);
        settings.SetVerifySSL(cfg.VerifySSL);
        settings.SetUseVirtualAddressing(!cfg.PathStyle);
        settings.SetExecutorThreadsCount(cfg.Threads);
        settings.SetMaxConnectionsCount(cfg.Threads);
        settings.SetScheme(cfg.Scheme == "http"
            ? NKikimrSchemeOp::TS3Settings::HTTP
            : NKikimrSchemeOp::TS3Settings::HTTPS);

        auto storageCfg = IExternalStorageConfig::Construct(NKikimrConfig::TAwsClientConfig(), settings);
        auto storageOperator = storageCfg->ConstructStorageOperator(/*verbose*/true);

        auto setup = BuildActorSystemSetup(/*threads*/ Max<ui32>(1u, cfg.Threads / 2u));
        THolder<TActorSystem> system(new TActorSystem(setup));
        system->Start();

        const TActorId wrapperId = system->Register(CreateStorageWrapper(storageOperator));
        const ui64 sizeBytes = cfg.RangeSizeBytes ? cfg.RangeSizeBytes : (cfg.SizeMiBPerFile * 1024ull * 1024ull);
        auto done = NThreading::NewPromise<void>();
        auto future = done.GetFuture();
        const TActorId bench = system->Register(new TS3BenchActor(wrapperId, std::move(keys), cfg.Bucket, cfg.Concurrency, sizeBytes, cfg.RangeOffsetBytes, done, cfg.SaveDir));
        Y_UNUSED(bench);

        future.Wait();
        system->Stop();
        system->Cleanup();
    } catch (...) {
        Cerr << "Unhandled exception" << Endl;
        exitCode = 2;
    }

    Aws::ShutdownAPI(awsOptions);
    return exitCode;
}
