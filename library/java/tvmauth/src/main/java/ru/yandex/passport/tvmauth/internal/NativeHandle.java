package ru.yandex.passport.tvmauth.internal;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class NativeHandle implements AutoCloseable {
    protected final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
    protected long nativeHandle;

    @Override
    public void close() {
        rwlock.writeLock().lock();
        try {
            disposeHandle(nativeHandle);
            this.nativeHandle = 0;
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    protected abstract void disposeHandle(long handle);

    protected long getNativeHandle() {
        if (nativeHandle == 0) {
            throw new IllegalStateException("Instance is already destroyed");
        }
        return nativeHandle;
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void finalize() {
        this.close();
    }
}
