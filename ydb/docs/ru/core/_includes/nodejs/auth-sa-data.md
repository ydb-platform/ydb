```typescript
import { Driver, IamAuthService } from 'ydb-sdk';
import { IIamCredentials } from 'ydb-sdk/build/cjs/src/credentials';

export async function connect(endpoint: string, database: string) {
    const saCredentials: IIamCredentials = {
        serviceAccountId: 'serviceAccountId',
        accessKeyId: 'accessKeyId',
        privateKey: Buffer.from('-----BEGIN PRIVATE KEY-----\nyJ1yFwJq...'),
        iamEndpoint: 'iam.api.cloud.yandex.net:443',
    };
    const authService = new IamAuthService(saCredentials);
    const driver = new Driver({endpoint, database, authService});
    const timeout = 10000;
    if (!await driver.ready(timeout)) {
        console.log(`Driver has not become ready in ${timeout}ms!`);
        process.exit(1);
    }
    console.log('Driver connected')
    return driver
}
```
