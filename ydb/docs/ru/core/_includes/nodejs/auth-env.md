```typescript
import { Driver } from '@ydbjs/core';
import { EnvironCredentialsProvider } from '@ydbjs/auth/environ';

const cs = process.env.YDB_CONNECTION_STRING!;
const creds = new EnvironCredentialsProvider(cs);

const driver = new Driver(cs, {
  credentialsProvider: creds,
  secureOptions: creds.secureOptions,
});
await driver.ready();
```
