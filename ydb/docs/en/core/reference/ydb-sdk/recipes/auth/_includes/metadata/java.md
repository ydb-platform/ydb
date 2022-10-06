```java
public void work(String connectionString) {
    AuthProvider authProvider = CloudAuthHelper.getMetadataAuthProvider(); 

    GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
            .withAuthProvider(authProvider)
            .build());
    
    TableClient tableClient = TableClient.newClient(transport).build();

    doWork(tableClient);

    tableClient.close();
    transport.close();
}
```
