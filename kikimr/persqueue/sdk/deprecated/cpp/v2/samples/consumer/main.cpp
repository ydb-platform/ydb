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

    pqLibSettings.DefaultLogger = logger;

    // Step 3. Create TPQLib object.
    TPQLib pq(pqLibSettings);
    // [END create pqlib]

    // [BEGIN create consumer]
    // Step 4. Parameters used in sample.

    // List of topics to read.
    TVector<TString> topics = {"topic_path"};

    // Path of consumer that will read topics.
    TString consumerPath = "consumer_path";

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
    TString endpoint = "man.logbroker.yandex.net";

    // Simple single read.

    // Step 5. Create settings of consumer object.
    TConsumerSettings settings;
    // Fill consumer settings. See types.h for more information.
    // Settings without defaults, must be set always.
    settings.Server.Address = endpoint;
    {
        // Working with logbroker in Yandex.Cloud:
        // Fill database for Yandex.Cloud Logbroker.
        settings.Server.Database = "/Root";
        // Enable TLS.
        settings.Server.EnableSecureConnection("");
    }
    settings.Topics = topics;
    settings.ClientId = consumerPath;

    // One must set CredentialsProvider for authentification. There could be OAuth and TVM providers.
    // Create OAuth provider from OAuth ticket.
    settings.CredentialsProvider = CreateOAuthCredentialsProvider(oauthToken);
    // Or create TVM provider from TVM settings.
    settings.CredentialsProvider = CreateTVMCredentialsProvider(tvmSecret, tvmClientId, tvmDstId, logger);
    // Or create IAM provider from jwt-key
    settings.CredentialsProvider = CreateIAMJwtParamsCredentialsForwarder(jwtKey, logger);

    // Step 6. Create consumer object. PQLib object must be valid during all lifetime of consumer, otherwise reading from consumer will fail but this is not UB.
    auto consumer = pq.CreateConsumer(settings, logger);

    // Step 7. Start consuming data.
    auto future = consumer->Start();

    // Step 9. Wait initialization and check it result.
    future.Wait();

    if (future.GetValue().Response.HasError()) {
        return 1;
    }
    // [END create consumer]

    // [BEGIN read messages]
    // Step 10. Simple single read.
    // Step 10.1. Request message from consumer.
    auto msg = consumer->GetNextMessage();

    // Step 10.2. Wait for message.
    msg.Wait();

    auto value  = msg.GetValue();

    // Step 10.3. Check result.
    switch (value.Type) {
        // Step 10.3.1. Result is error - consumer is in incorrect state now. You can log error and recreate everything starting from step 5.
        case EMT_ERROR:
            return 1;
        // Step 10.3.2. Read data.
        case EMT_DATA: {
            // Process received messages. Inside will be several batches.
            for (const auto& t : value.Response.data().message_batch()) {
                const TString topic = t.topic();
                const ui32 partition = t.partition();
                Y_UNUSED(topic);
                Y_UNUSED(partition);
                // There are several messages from partition of topic inside one batch.
                for (const auto& m : t.message()) {
                    const ui64 offset = m.offset();
                    const TString& data = m.data();
                    // Each message has offset in partition, payload data (decompressed or as is) and metadata.
                    Y_UNUSED(offset);
                    Y_UNUSED(data);
                }
            }

            // Step 10.3.3. Each processed data batch from step 10.3.2 must be committed once all data inside is processed.

            // This must be done using cookie from data batch.
            auto cookie = msg.GetValue().Response.GetData().GetCookie();
            // Commit cookie['s]. Acknowledge from server will be received inside one of next messages from consumer later.
            consumer->Commit({cookie});
            break;
        }
        // Step 10.3.4. Commit acknowledge. Can be used for logging and debugging purposes. (Can't be first message, only possible if one makes several
        // consecutive reads.
        case EMT_COMMIT:
            break;
        // Handle other cases - only for compiling, there could not be any.
        default:
            break;

    }
    // [END read messages]

    // [BEGIN advanced read messages]

    // Point that consumer will perform advanced reads.
    settings.UseLockSession = true;

    {
        // Step 11. Create consumer object. PQLib object must be valid during all lifetime of consumer, otherwise reading from consumer will fail but this is not UB.
        auto consumer = pq.CreateConsumer(settings, logger);

        // Step 12. Start consuming data.
        auto future = consumer->Start();

        // Step 14. Wait initialization and check it result.
        future.Wait();

        if (future.GetValue().Response.HasError()) {
            return 1;
        }

        // Step 14. Event loop.
        while (true) {
            // Step 14.1. Request message from consumer.
            auto msg = consumer->GetNextMessage();

            // Step 14.2. Wait for message.
            msg.Wait();

            const auto& value = msg.GetValue();

            // Step 14.3. Check result.
            switch (value.Type) {
                // Step 14.3.1. Result is error: consumer is in incorrect state now. You can log error and recreate everything starting from step 11.
                case EMT_ERROR:
                    return 1;
                // Step 14.3.2. Lock request from server.
                case EMT_LOCK: {
                    const TString& topic = value.Response.lock().topic();
                    const ui32 partition = value.Response.lock().partition();
                    // Server is ready to send data from corresponding partition of topic. Topic is legacy topic name = rt3.<cluster>--<converted topic path>
                    Y_UNUSED(topic);
                    Y_UNUSED(partition);
                    // One must react on this event with setting promise ReadyToRead right now or later. Only after this client will start receiving data from this partition.
                    msg.GetValue().ReadyToRead.SetValue(TLockInfo{0 /*readOffset*/, 0 /*commitOffset*/, false /*verifyReadOffset*/});
                    break;
                }
                // Step 14.3.3. Release request from server.
                case EMT_RELEASE: {
                    const TString& topic = value.Response.release().topic();
                    const ui32 partition = value.Response.release().partition();
                    Y_UNUSED(topic);
                    Y_UNUSED(partition);
                    // // Server will not send any data from partition of topic until next Lock request is confirmed by client.
                   break;
                  }
                // Step 14.3.4. Read data.
                case EMT_DATA: {
                    // Process received messages. Inside will be several batches. The same as in the simple read.
                    for (const auto& t : value.Response.data().message_batch()) {
                        const TString topic = t.topic();
                        const ui32 partition = t.partition();
                        Y_UNUSED(topic);
                        Y_UNUSED(partition);
                        // There are several messages from partition of topic inside one batch.
                        for (const auto& m : t.message()) {
                            const ui64 offset = m.offset();
                            const TString& data = m.data();
                            // Each message has offset in partition, payload data (decompressed or as is) and metadata.
                            Y_UNUSED(offset);
                            Y_UNUSED(data);
                        }
                    }

                    // Step 14.3.5. Each processed data batch from step 14.3.4 should be committed after all data inside is processed.

                    // This must be done using cookie from data batch.
                    auto cookie = msg.GetValue().Response.GetData().GetCookie();
                    // Commit cookie['s]. Acknowledge from server will be received inside message from consumer later.
                    consumer->Commit({cookie});
                    break;
                }
                // Step 14.3.6. Commit acknowledge. Can be used for logging and debugging purposes.
                case EMT_STATUS:
                case EMT_COMMIT:
                    break;
            } // switch()
        } // while()
    }

    // [END advanced read messages]

    // Step 15. End of working. Nothing special needs to be done - desctuction of objects only.

    return 0;
}
