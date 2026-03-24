```typescript
import { Driver } from '@ydbjs/core';
import { ServiceAccountCredentialsProvider } from '@ydbjs/auth-yandex-cloud';

const driver = new Driver(connectionString, {
  credentialsProvider: new ServiceAccountCredentialsProvider({
    id: 'serviceAccountId',
    privateKey: '-----BEGIN PRIVATE KEY-----\n...',
    keyId: 'accessKeyId',
  }),
});
await driver.ready();
```
