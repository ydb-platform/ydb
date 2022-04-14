#include <queue>

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/string/join.h>
#include <util/string/vector.h>

#include <google/protobuf/message.h>

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>

using namespace NPersQueue;


int main() {


    // [BEGIN create pqlib]
    // Step 1. First create logger. It could be your logger inherited from ILogger interface or one from lib.
    TIntrusivePtr<ILogger> logger = new TCerrLogger(7);

    // Step 2. Create settings of main TPQLib object.
    TPQLibSettings pqLibSettings;

    // Number of threads for processing gRPC events.
    pqLibSettings.ThreadsCount = 1;
    // Number of threads for compression/decomression actions.
    pqLibSettings.CompressionPoolThreads = 3;
    // Default logger. Used inside consumers and producers if no logger provided.
    pqLibSettings.DefaultLogger = logger;

    // Step 3. Create TPQLib object.
    TPQLib pq(pqLibSettings);
    // [END create pqlib]

    // [BEGIN create producer]
    // Step 4. Initialize producer parameters

    // Topics to read.
    TString topic = "topic_path";
    // Partitions group. 0 means any.
    ui32 group = 0;

    // Source identifier of messages group.
    TString sourceId = "source_id";

    // OAuth token.
    TString oauthToken = "oauth_token";

    // IAM JWT key
    TString jwtKey = "<jwt json key>";

    // TVM parameters.
    TString tvmSecret = "tvm_secret";
    ui32 tvmClientId = 200000;
    ui32 tvmDstId = 2001147; // logbroker main prestable
    tvmDstId = 2001059; // logbroker main production

    // Logbroker endpoint.
    TString endpoint = "logbroker.yandex.net";

    // Step 5. Put producer parameters into settings.
    TProducerSettings settings;
    // Logbroker endpoint.
    settings.Server.Address = endpoint;
    {
        // Working with logbroker in Yandex.Cloud:
        // Fill database for Yandex.Cloud Logbroker.
        settings.Server.Database = "/Root";
        // Enable TLS.
        settings.Server.EnableSecureConnection("");
    }
    // Topic path.
    settings.Topic = topic;
    // SourceId of messages group.
    settings.SourceId = sourceId;
    // Partition group to write to.
    settings.PartitionGroup = group;
    // ReconnectOnFailure will  gurantee that producer will retry writes in case of failre.
    settings.ReconnectOnFailure = true; //retries on server errors
    // Codec describes how to compress data.
    settings.Codec = NPersQueueCommon::ECodec::GZIP;

    // One must set CredentialsProvider for authentification. There could be OAuth and TVM providers.
    // Create OAuth provider from OAuth ticket.
    settings.CredentialsProvider = CreateOAuthCredentialsProvider(oauthToken);
    // Or create TVM provider from TVM settings.
    settings.CredentialsProvider = CreateTVMCredentialsProvider(tvmSecret, tvmClientId, tvmDstId, logger);
    // Or create IAM provider from jwt-key
    settings.CredentialsProvider = CreateIAMJwtParamsCredentialsForwarder(jwtKey, logger);

    // Step 6. Create producer object. PQLib object must be valid during all lifetime of producer, otherwise writing to producer will fail but this is not UB.
    auto producer = pq.CreateProducer(settings, logger);

    // Step 7. Start producer.
    auto future = producer->Start();

    // Step 8. Wait initialization and check it result.
    future.Wait();

    if (future.GetValue().Response.HasError()) {
        return 1;
    }
    // [END create producer]

    // [BEGIN write message]

    // Step 9. Single write sample.
    // Step 9.1. Form message.
    // Creation timestamp.
    TInstant timestamp = TInstant::Now();
    // Message payload.
    TString payload = "abacaba";

    TData data{payload, timestamp};
    // Corresponding messages's sequence number.
    ui64 seqNo = 1;

    // Step 9.2. Write message. Until result future is signalled - message is stored inside PQLib memory. Future will be signalled with message payload and result of writing.
    auto result = producer->Write(seqNo, data);

    // Step 9.3 Check writing result.
    // Check all results.
    result.Wait();
    Y_VERIFY(!result.GetValue().Response.HasError());

    // Only when future is destroyed no memory is used by message.

    // [END write message]

    // [BEGIN write messages with inflight]

    // Step 10. Writing loop with in-flight.
    // Max in-flight border, parameter.
    ui32 maxInflyCount = 10;
    // Queue for messages in-flight.
    std::queue<NThreading::TFuture<TProducerCommitResponse>> v;

    while(true) {
        // Creation timestamp.
        TInstant timestamp = TInstant::Now();
        // Message payload.
        TString payload = "abacaba";
        TData data{payload, timestamp};

        // SeqNo must increase for consecutive messages.
        ++seqNo;

        // Step 10.1. Write message.
        auto result = producer->Write(seqNo, data);
        // Step 10.2. Put future inside queue.
        v.push(result);
        // Step 10.3. If in-flight size is too big.
        if (v.size() > maxInflyCount) {
            // Step 10.4. Wait for first message in queue processing result, check it.
            v.front().Wait();
            Y_VERIFY(!v.front().GetValue().Response.HasError());
            // Step 10.5. Drop first processed message. Only after memory of this message will be freed.
            v.pop();
        }
    }

    // Step 10.6. After processing loop one must wait for all not yet processed messages.

    while (!v.empty()) {
        v.front().Wait();
        Y_VERIFY(!v.front().GetValue().Response.HasError());
        v.pop();
    }

    // [END write messages with inflight]

    return 0;
}
