package ru.yandex.passport.tvmauth.deprecated;

import javax.annotation.Nonnull;

import ru.yandex.passport.tvmauth.BlackboxEnv;
import ru.yandex.passport.tvmauth.CheckedUserTicket;
import ru.yandex.passport.tvmauth.internal.JniUtils;
import ru.yandex.passport.tvmauth.internal.NativeHandle;

/**
 * WARNING: it is low level API: first of all try use NativeTvmClient.
 */
public final class UserContext extends NativeHandle {
    /**
     * Create user context.
     */
    public UserContext(@Nonnull BlackboxEnv env, @Nonnull String tvmKeysResponse) {
        this.nativeHandle = factory(env.ordinal(), tvmKeysResponse);
    }
    @Nonnull
    public static UserContext create(@Nonnull BlackboxEnv env, @Nonnull String tvmKeysResponse) {
        return new UserContext(env, tvmKeysResponse);
    }

    /**
     * Parse and validate user ticket body then create CheckedUserTicket object.
     */
    @Nonnull
    public CheckedUserTicket check(@Nonnull String ticketBody) {
        rwlock.readLock().lock();
        try {
            return checkNative(getNativeHandle(), ticketBody);
        } finally {
            rwlock.readLock().unlock();
        }
    }

    @Override
    protected void disposeHandle(long handle) {
        dispose(handle);
    }

    private static native long factory(int env, String tvmKeysResponse);
    private static native void dispose(long nativeHandle);
    private static native CheckedUserTicket checkNative(long nativeHandle, String ticketBody);

    static {
        JniUtils.loadLibrary();
    }
}
