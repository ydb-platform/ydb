```java
public void work(String connectionString, Path saKeyPath) {
    AuthProvider authProvider = CloudAuthProvider.newAuthProvider(
        ApiKeyCredentialProvider.builder()
            .fromFile(saKeyPath)
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
