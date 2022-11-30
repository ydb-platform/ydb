package ru.yandex.passport.tvmauth.deprecated;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.passport.tvmauth.CheckedServiceTicket;
import ru.yandex.passport.tvmauth.internal.JniUtils;
import ru.yandex.passport.tvmauth.internal.NativeHandle;

/**
 * WARNING: it is low level API: first of all try use NativeTvmClient.
 */
public final class ServiceContext extends NativeHandle {
    /**
     * Create service context.
     * Service contexts are used to store TVM keys and parse service tickets.
     * Service contexts are used to make signature for TVM-API.
     * @param tvmId
     * @param secretBase64
     */
    public ServiceContext(int tvmId, @Nullable String secretBase64, @Nullable String tvmKeysResponse) {
        if (tvmKeysResponse == null && secretBase64 == null) {
            throw new IllegalArgumentException("secretBase64 and tvmKeysResponse cannot be both null-reference");
        }
        this.nativeHandle = factory(tvmId, secretBase64, tvmKeysResponse);
    }

    /**
     * Create service context.
     * Service contexts are used to make signature for TVM-API.
     */
    public ServiceContext(@Nonnull String secretBase64, int tvmId) {
        this(tvmId, secretBase64, null);
    }

    /**
     * Create service context.
     * Service contexts are used to store TVM keys and parse service tickets.
     */
    public ServiceContext(int tvmId, @Nonnull String tvmKeysResponse) {
        this(tvmId, null, tvmKeysResponse);
    }

    @Nonnull
    public static ServiceContext create(int tvmId, @Nullable String secretBase64, @Nullable String tvmKeysResponse) {
        return new ServiceContext(tvmId, secretBase64, tvmKeysResponse);
    }
    @Nonnull
    public static ServiceContext create(@Nonnull String secretBase64, int tvmId) {
        return new ServiceContext(secretBase64, tvmId);
    }
    @Nonnull
    public static ServiceContext create(int tvmId, @Nonnull String tvmKeysResponse) {
        return new ServiceContext(tvmId, tvmKeysResponse);
    }

    /**
     * Parse and validate service ticket body then create CheckedServiceTicket object.
     */
    @Nonnull
    public CheckedServiceTicket check(@Nonnull String ticketBody) {
        rwlock.readLock().lock();
        try {
            return checkNative(getNativeHandle(), ticketBody);
        } finally {
            rwlock.readLock().unlock();
        }
    }

    /**
     * Sign params for TVM API
     */
    @Nonnull
    public String signCgiParamsForTvm(@Nonnull String ts, @Nonnull String dst, @Nonnull String scopes) {
        rwlock.readLock().lock();
        try {
            return signCgiParamsForTvmNative(getNativeHandle(), ts, dst, scopes);
        } finally {
            rwlock.readLock().unlock();
        }
    }
    public String signCgiParamsForTvm(String ts, String dst) {
        return signCgiParamsForTvm(ts, dst, "");
    }

    @Override
    protected void disposeHandle(long handle) {
        dispose(handle);
    }

    private static native long factory(int tvmId, String secretBase64, String tvmKeysResponse);
    private static native void dispose(long nativeHandle);
    private static native CheckedServiceTicket checkNative(long nativeHandle, String ticketBody);
    private static native String signCgiParamsForTvmNative(long nativeHandle, String ts, String dst, String scopes);

    static {
        JniUtils.loadLibrary();
    }
}
