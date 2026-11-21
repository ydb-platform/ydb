import { setTimeout as delay } from 'node:timers/promises';
import { StatusIds_StatusCode } from '@ydbjs/api/operation';
import { SchemeServiceDefinition } from '@ydbjs/api/scheme';
import { Driver } from '@ydbjs/core';

const { YDB_ENDPOINT: endpoint, YDB_DATABASE: database, YDB_PROBE: mode = 'scheme' } = process.env;
if (mode !== 'scheme') {
    throw new Error(`Unsupported YDB_PROBE ${mode}`);
}
if (!endpoint || !database) {
    throw new Error('YDB_ENDPOINT and YDB_DATABASE must be set');
}

const retryWindowMs = 8_000;
const driver = new Driver(`grpc://${endpoint}${database}`);
try {
    await driver.ready(AbortSignal.timeout(retryWindowMs));
    const scheme = driver.createClient(SchemeServiceDefinition);
    let retryDeadline = performance.now() + retryWindowMs;
    let lastError;
    let remaining;
    while ((remaining = Math.floor(retryDeadline - performance.now())) > 0) {
        try {
            const { operation } = await scheme.listDirectory(
                { path: database }, { signal: AbortSignal.timeout(Math.min(2_000, remaining)) },
            );
            if (!operation?.ready || operation.status !== StatusIds_StatusCode.SUCCESS) {
                throw new Error(`ListDirectory failed (status ${operation?.status})`);
            }
            console.log('OK');
            retryDeadline = performance.now() + retryWindowMs;
        } catch (error) {
            lastError = error;
            console.error(error.message);
        }
        await delay(200);
    }
    throw new Error('ListDirectory did not succeed within the retry window', { cause: lastError });
} finally {
    driver.close();
}
