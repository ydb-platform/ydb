import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.scheme.SchemeClient;

public final class RediscoveryProbe {
    private static final Duration RETRY_WINDOW = Duration.ofSeconds(8);

    @SuppressWarnings("deprecation") // SchemeClient uses the transport's default request timeout.
    public static void main(String[] args) throws Exception {
        String mode = System.getenv().getOrDefault("YDB_PROBE", "scheme");
        if (!mode.equals("scheme")) {
            throw new IllegalArgumentException("Unsupported YDB_PROBE " + mode);
        }
        String endpoint = Objects.requireNonNull(System.getenv("YDB_ENDPOINT"), "YDB_ENDPOINT");
        String database = Objects.requireNonNull(System.getenv("YDB_DATABASE"), "YDB_DATABASE");

        try (GrpcTransport transport = GrpcTransport.forConnectionString("grpc://" + endpoint + database)
                .withDiscoveryTimeout(RETRY_WINDOW)
                .withReadTimeout(Duration.ofSeconds(2))
                .build();
                SchemeClient scheme = SchemeClient.newClient(transport).build()) {
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
}
