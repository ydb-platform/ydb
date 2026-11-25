import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.AsyncWriter;
import tech.ydb.topic.write.Message;

public final class RediscoveryProbe {
    private static final Duration RETRY_WINDOW = Duration.ofSeconds(8);

    @SuppressWarnings("deprecation") // SchemeClient uses the transport's default request timeout.
    public static void main(String[] args) throws Exception {
        String mode = System.getenv().getOrDefault("YDB_PROBE", "scheme");
        String endpoint = Objects.requireNonNull(System.getenv("YDB_ENDPOINT"), "YDB_ENDPOINT");
        String database = Objects.requireNonNull(System.getenv("YDB_DATABASE"), "YDB_DATABASE");

        GrpcTransportBuilder builder = GrpcTransport.forConnectionString("grpc://" + endpoint + database);
        if ("1".equals(System.getenv("YDB_LONG_POLL_DISCOVERY"))) {
            // The same probe also builds against the released, unary-only artifact.
            builder.getClass().getMethod("withLongPollDiscovery").invoke(builder);
        }
        try (GrpcTransport transport = builder.withDiscoveryTimeout(RETRY_WINDOW)
                .withReadTimeout(Duration.ofSeconds(2))
                .build();
                SchemeClient scheme = SchemeClient.newClient(transport).build()) {
            if (mode.equals("topic-write-on-demand") || mode.equals("topic-read-on-demand")) {
                topicProbe(transport, mode);
                return;
            }
            if (!mode.equals("scheme")) {
                throw new IllegalArgumentException("Unsupported YDB_PROBE " + mode);
            }
            long retryDeadline = System.nanoTime() + RETRY_WINDOW.toNanos();
            RuntimeException lastError = null;
            long remaining;
            while ((remaining = retryDeadline - System.nanoTime()) > 0) {
                try {
                    scheme.listDirectory(database).orTimeout(remaining, TimeUnit.NANOSECONDS).join().getValue();
                    System.out.println("OK");
                    retryDeadline = System.nanoTime() + RETRY_WINDOW.toNanos();
                } catch (RuntimeException error) {
                    lastError = error;
                    System.err.println(error);
                }
                Thread.sleep(200);
            }
            throw new IllegalStateException("ListDirectory did not succeed within the retry window", lastError);
        }
    }

    private static void topicProbe(GrpcTransport transport, String mode) throws Exception {
        String path = Objects.requireNonNull(System.getenv("YDB_PROBE_TOPIC"), "YDB_PROBE_TOPIC");
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        try (TopicClient client = TopicClient.newClient(transport).build()) {
            if (mode.equals("topic-write-on-demand")) {
                AsyncWriter writer = client.createAsyncWriter(WriterSettings.newBuilder()
                        .setTopicPath(path).setProducerId("probe").build());
                writer.init().get(20, TimeUnit.SECONDS);
                for (String line; (line = input.readLine()) != null;) {
                    writer.send(Message.of(line.getBytes(StandardCharsets.UTF_8))).get(20, TimeUnit.SECONDS);
                    System.out.println("OK " + line);
                }
                writer.shutdown().get(20, TimeUnit.SECONDS);
            } else {
                SyncReader reader = client.createSyncReader(ReaderSettings.newBuilder().setConsumerName("probe")
                        .addTopic(TopicReadSettings.newBuilder().setPath(path).build()).build());
                reader.init();
                while (input.readLine() != null) {
                    tech.ydb.topic.read.Message message = reader.receive(20, TimeUnit.SECONDS);
                    System.out.println("OK " + new String(message.getData(), StandardCharsets.UTF_8));
                }
                reader.shutdown();
            }
        }
    }
}
