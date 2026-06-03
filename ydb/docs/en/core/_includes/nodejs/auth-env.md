```typescript
import { Driver } from "@ydbjs/core";
import { EnvironCredentialsProvider } from "@ydbjs/auth/environ";

const creds = new EnvironCredentialsProvider("grpc://localhost:2136/local");

const driver = new Driver("grpc://localhost:2136/local", {
  credentialsProvider: creds,
  secureOptions: creds.secureOptions,
});

await driver.ready();
```
