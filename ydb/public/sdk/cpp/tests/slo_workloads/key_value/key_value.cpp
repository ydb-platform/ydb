#include "key_value.h"

#include <util/stream/file.h>
#include <util/system/getpid.h>
#include <util/thread/pool.h>

using namespace NYdb;

const std::string TableName = "key_value";

NYdb::TValue BuildValueFromRecord(const TKeyValueRecordData& recordData) {
    NYdb::TValueBuilder value;
    value.BeginStruct();
    value.AddMember("object_id_key").Uint32(GetHash(recordData.ObjectId));
    value.AddMember("object_id").Uint32(recordData.ObjectId);
    value.AddMember("timestamp").Uint64(recordData.Timestamp);
    value.AddMember("payload").Utf8(recordData.Payload);
    value.EndStruct();
    return value.Build();
}

int DoCreate(TDatabaseOptions& dbOptions, int argc, char** argv) {
    TCreateOptions createOptions{ {dbOptions} };
    if (!ParseOptionsCreate(argc, argv, createOptions)) {
        return EXIT_FAILURE;
    }

    createOptions.CommonOptions.MaxInfly = createOptions.CommonOptions.MaxInputThreads;

    int result = CreateTable(dbOptions);
    if (result) {
        return result;
    }

    std::uint32_t maxId = GetTableStats(dbOptions, TableName).MaxId;

    createOptions.CommonOptions.ReactionTime = TDuration::Seconds(20);

    Cout << TInstant::Now().ToRfc822StringLocal() << " Uploading initial content... do 'kill -USR1 " << GetPID()
        << "' for progress details or Ctrl/Cmd+C to interrupt" << Endl;

    std::shared_ptr<TJobContainer>& jobs = *Singleton<std::shared_ptr<TJobContainer>>();
    TJobGC gc(jobs);
    jobs = std::make_shared<TJobContainer>();

    jobs->Add(new TGenerateInitialContentJob(createOptions, maxId));

    SetUpInteraction();

    jobs->Start();
    jobs->Wait();
    jobs->ShowProgress();

    return EXIT_SUCCESS;
}

int DoRun(TDatabaseOptions& dbOptions, int argc, char** argv) {
    TRunOptions runOptions{ {dbOptions} };
    if (!ParseOptionsRun(argc, argv, runOptions)) {
        return EXIT_FAILURE;
    }

    Cout << TInstant::Now().ToRfc822StringLocal() << " Creating and initializing jobs..." << Endl;

    std::uint32_t maxId = GetTableStats(dbOptions, TableName).MaxId;

    std::shared_ptr<TJobContainer>& jobs = *Singleton<std::shared_ptr<TJobContainer>>();
    TJobGC gc(jobs);
    jobs = std::make_shared<TJobContainer>();

    if (!runOptions.DontRunA) {
        runOptions.CommonOptions.Rps = runOptions.Read_rps;
        runOptions.CommonOptions.ReactionTime = TDuration::MilliSeconds(runOptions.CommonOptions.A_ReactionTime);
        jobs->Add(new TReadJob(runOptions.CommonOptions, maxId));
    }
    if (!runOptions.DontRunB) {
        runOptions.CommonOptions.Rps = runOptions.Write_rps;
        runOptions.CommonOptions.ReactionTime = DefaultReactionTime;
        jobs->Add(new TWriteJob(runOptions.CommonOptions, maxId));
    }

    TInstant start = TInstant::Now();
    TInstant deadline = start + TDuration::Seconds(runOptions.CommonOptions.SecondsToRun);

    jobs->Start(deadline);

    SetUpInteraction();
    Cout << "Jobs launched. Do 'kill -USR1 " << GetPID()
        << "' for progress details or 'kill -INT " << GetPID() << "' (Ctrl/Cmd + C) to interrupt" << Endl
        << "           Start time: " << start.ToRfc822StringLocal() << Endl
        << "Estimated finish time: " << deadline.ToRfc822StringLocal() << Endl;

    jobs->Wait();

    Cout << "All jobs finished: " << TInstant::Now().ToRfc822StringLocal() << Endl;

    jobs->ShowProgress();

    return EXIT_SUCCESS;
}

int DoCleanup(TDatabaseOptions& dbOptions, int argc) {
    if (argc > 1) {
        Cerr << "Unexpected arguments after cleanup" << Endl;
        return EXIT_FAILURE;
    }
    return DropTable(dbOptions);
}
