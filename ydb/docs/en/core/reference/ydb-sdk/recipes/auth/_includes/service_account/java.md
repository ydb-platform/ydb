```java
public void work(String connectionString, String saKeyPath) {
    AuthProvider authProvider = CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyPath);

    GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
            .withAuthProvider(authProvider)
            .build());
    
    TableClient tableClient = TableClient.newClient(transport).build();

    doWork(tableClient);

    tableClient.close();
    transport.close();
}
```
