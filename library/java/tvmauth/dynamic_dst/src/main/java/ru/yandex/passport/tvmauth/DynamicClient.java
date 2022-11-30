package ru.yandex.passport.tvmauth;

import java.util.Optional;

import javax.annotation.Nonnull;

import ru.yandex.passport.tvmauth.internal.LogFetcher;

/**
 * It is designed for very special case:
 * https://st.yandex-team.ru/PASSP-29336
 */
public final class DynamicClient extends NativeTvmClient {
    private long dynNativeHandle;

    class NativeHandles {
        long dyn;
        long common;
    }

    public DynamicClient(@Nonnull TvmApiSettings settings) {
        LogFetcher log = new LogFetcher();
        try {
            NativeHandles handles = factoryDynamicClientNative(settings.getHandle(), log.getHandle());
            this.dynNativeHandle = handles.dyn;
            setNativeHandle(handles.common);
        } finally {
            log.run();
        }

        this.startThread(log);
    }
    @Nonnull
    public static DynamicClient create(@Nonnull TvmApiSettings settings) {
        return new DynamicClient(settings);
    }

    public void addDsts(@Nonnull int[] dsts) {
        StringBuilder b = new StringBuilder(10 * dsts.length);
        for (int d : dsts) {
            b.append(d).append(";");
        }

        rwlock.readLock().lock();

        try {
            getNativeHandle();
            addDstsNative(dynNativeHandle, b.toString());
        } finally {
            rwlock.readLock().unlock();
        }
    }

    Optional<String> getOptionalServiceTicketFor(int tvmId) {
        rwlock.readLock().lock();

        try {
            getNativeHandle();
            return Optional.ofNullable(getOptionalServiceTicketForTvmIdNative(dynNativeHandle, tvmId));
        } finally {
            rwlock.readLock().unlock();
        }
    }

    private static native NativeHandles factoryDynamicClientNative(long settings, long logger);
    private static native void addDstsNative(long nativeHandle, String dsts);
    private static native String getOptionalServiceTicketForTvmIdNative(long nativeHandle, int tvmId);
}
