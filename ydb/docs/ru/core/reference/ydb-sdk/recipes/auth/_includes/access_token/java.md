```java
public void work(String connectionString, String accessToken) {
    AuthProvider authProvider = CloudAuthProvider.newAuthProvider(
        IamTokenCredentialProvider.builder()
            .token(accessToken)
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
