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
    Y_UNUSED(dbOptions);
    Y_UNUSED(argc);
    Y_UNUSED(argv);
    Cerr << "The run command has not been implemented for this scenario yet" << Endl;
    return EXIT_SUCCESS;
}

int DoCleanup(TDatabaseOptions& dbOptions, int argc) {
    if (argc > 1) {
        Cerr << "Unexpected arguments after cleanup" << Endl;
        return EXIT_FAILURE;
    }
    return DropTable(dbOptions);
}
