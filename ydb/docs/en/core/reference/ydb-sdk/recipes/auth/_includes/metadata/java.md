```java
public void work(String connectionString) {
    AuthProvider authProvider = CloudAuthProvider.newAuthProvider(
        ComputeEngineCredentialProvider.builder()
            .build()
    );

    GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
            .withAuthProvider(authProvider)
            .build();

    TableClient tableClient = TableClient
        .newClient(GrpcTableRpc.ownTransport(transport))
        .build());

    doWork(tableClient);

    tableClient.close();
}
```
