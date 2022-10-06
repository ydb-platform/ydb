```java
this.tableClient = TableClient.newClient(transport)
        // 10 - minimum number of active sessions to keep in the pool during the cleanup
        // 500 - maximum number of sessions in the pool
        .sessionPoolSize(10, 500)
        .build();
```