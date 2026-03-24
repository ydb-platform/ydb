```typescript
import { Driver } from '@ydbjs/core';
import { ServiceAccountCredentialsProvider } from '@ydbjs/auth-yandex-cloud';

const driver = new Driver(connectionString, {
  credentialsProvider: ServiceAccountCredentialsProvider.fromFile('./authorized_key.json'),
});
await driver.ready();
```
