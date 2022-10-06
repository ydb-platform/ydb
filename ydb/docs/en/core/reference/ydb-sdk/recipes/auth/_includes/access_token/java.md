```java
public void work(String connectionString, String accessToken) {
    AuthProvider authProvider = new TokenAuthProvider(accessToken);

    GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
            .withAuthProvider(authProvider)
            .build());
    
    TableClient tableClient = TableClient.newClient(transport).build();

    doWork(tableClient);

    tableClient.close();
    transport.close();
}
```
