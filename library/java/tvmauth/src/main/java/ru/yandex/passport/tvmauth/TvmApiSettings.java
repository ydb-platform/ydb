package ru.yandex.passport.tvmauth;

import java.util.Map;

import javax.annotation.Nonnull;

import ru.yandex.passport.tvmauth.internal.JniUtils;
import ru.yandex.passport.tvmauth.internal.NativeHandle;

/**
 * Settings for TVM client. Uses https://tvm-api.yandex.net to get state.
 * At least one of them is required:
 *     enableServiceTicketChecking()
 *     enableUserTicketChecking()
 *     enableServiceTicketsFetchOptions()
 */
public final class TvmApiSettings extends NativeHandle {

    public TvmApiSettings() {
        this.nativeHandle = factory();
    }
    @Nonnull
    public static TvmApiSettings create() {
        return new TvmApiSettings();
    }

    /**
     * Required by enableServiceTicketChecking() and enableServiceTicketsFetchOptions()
     * @param tvmId
     * @return modified this
     */
    @Nonnull
    public TvmApiSettings setSelfTvmId(int tvmId) {
        rwlock.readLock().lock();

        try {
            setSelfTvmIdNative(getNativeHandle(), tvmId);
        } finally {
            rwlock.readLock().unlock();
        }

        return this;
    }

    /**
     * Prerequires setSelfTvmId().
     * This option enables fetching of public keys for signature checking.
     * @return modified this
     */
    @Nonnull
    public TvmApiSettings enableServiceTicketChecking() {
        rwlock.readLock().lock();

        try {
            enableServiceTicketCheckingNative(getNativeHandle());
        } finally {
            rwlock.readLock().unlock();
        }

        return this;
    }

    /**
     * This option enables checking of UserTickets
     *   and enables fetching of public keys for signature checking.
     * @return modified this
     */
    @Nonnull
    public TvmApiSettings enableUserTicketChecking(@Nonnull BlackboxEnv env) {
        rwlock.readLock().lock();

        try {
            enableUserTicketCheckingNative(getNativeHandle(), env.ordinal());
        } finally {
            rwlock.readLock().unlock();
        }

        return this;
    }

    /**
     * Set path to directory for disk cache
     * Requires read/write permissions. Checks permissions
     * WARNING: The same directory can be used only:
     *            - for TVM clients with the same settings
     *          OR
     *            - for new client replacing previous - with another config.
     *          System user must be the same for processes with these clients inside.
     *          Implementation doesn't provide other scenarios.
     * @param dir
     * @return modified this
     */
    @Nonnull
    public TvmApiSettings setDiskCacheDir(@Nonnull String dir) {
        rwlock.readLock().lock();

        try {
            setDiskCacheDirNative(getNativeHandle(), dir);
        } finally {
            rwlock.readLock().unlock();
        }

        return this;
    }

    /**
     * Alias is internal name of destination in your code. It allows not to bring destination's
     *  tvm_id to each calling point. Useful for several environments: prod/test/etc.
     * @example:
     *      static final String MY_BACKEND = "my backend";
     *      ...
     *      // init
     *      HashMap<String,Integer> map = new HashMap<String,Integer>();
     *      map.put(MY_BACKEND, config.getBackendId());
     *      s.enableServiceTicketsFetchOptions(config.getSelfSecret, m);
     *      ...
     *      // per request
     *      String t = tvmClient.setServiceTicketFor(MY_BACKEND);
     *
     * Set options for fetching Service Tickets from tvm-api
     *  to allow you send them to your backends.
     *
     * WARNING: It is not way to provide authorization for incoming ServiceTickets!
     *          It is way only to send your ServiceTickets to your backend!
     *
     * @param selfSecret
     * @param dsts is map: alias -> tvmId
     */
    @Nonnull
    public TvmApiSettings enableServiceTicketsFetchOptions(@Nonnull String selfSecret,
                                                           @Nonnull Map<String, Integer> dsts) {
        StringBuilder b = new StringBuilder(10 * dsts.size());
        for (Map.Entry<String, Integer> d : dsts.entrySet()) {
            b.append(d.getKey()).append(":").append(d.getValue()).append(";");
        }

        rwlock.readLock().lock();

        try {
            enableServiceTicketsFetchOptionsWithAliases(getNativeHandle(), selfSecret, b.toString());
        } finally {
            rwlock.readLock().unlock();
        }

        return this;
    }

    /**
     * Set options for fetching Service Tickets from tvm-api
     *  to allow you send them to your backends.
     *
     * WARNING: It is not way to provide authorization for incoming ServiceTickets!
     *          It is way only to send your ServiceTickets to your backend!
     *
     * @param selfSecret
     * @param dsts is array of tvmIds
     */
    @Nonnull
    public TvmApiSettings enableServiceTicketsFetchOptions(@Nonnull String selfSecret,
                                                           int[] dsts) {
        StringBuilder b = new StringBuilder(10 * dsts.length);
        for (int d : dsts) {
            b.append(d).append(";");
        }

        rwlock.readLock().lock();

        try {
            enableServiceTicketsFetchOptionsWithTvmIds(getNativeHandle(), selfSecret, b.toString());
        } finally {
            rwlock.readLock().unlock();
        }

        return this;
    }

    @Nonnull
    public TvmApiSettings fetchRolesForIdmSystemSlug(@Nonnull String slug) {
        rwlock.readLock().lock();

        try {
            fetchRolesForIdmSystemSlugNative(getNativeHandle(), slug);
        } finally {
            rwlock.readLock().unlock();
        }

        return this;
    }

    /**
     * By default client checks src from ServiceTicket or default uid from UserTicket -
     *   to prevent you from forgetting to check it yourself.
     * It does binary checks only:
     *   ticket gets status NoRoles, if there is no role for src or default uid.
     * You need to check roles on your own if you have a non-binary role system or
     *     you have disabled ShouldCheckSrc/ShouldCheckDefaultUid
     *
     * You may need to disable this check in the following cases:
     *   - You use GetRoles() to provide verbose message (with revision).
     *     Double check may be inconsistent:
     *       binary check inside client uses revision of roles X - i.e. src 100500 has no role,
     *       exact check in your code uses revision of roles Y -  i.e. src 100500 has some roles.
     *
     * @param value
     */
    @Nonnull
    public TvmApiSettings shouldCheckSrc(boolean value) {
        rwlock.readLock().lock();

        try {
            shouldCheckSrcNative(getNativeHandle(), value);
        } finally {
            rwlock.readLock().unlock();
        }

        return this;
    }

    /**
     * Look at shouldCheckSrc()
     * @param value
     */
    @Nonnull
    public TvmApiSettings shouldCheckDefaultUid(boolean value) {
        rwlock.readLock().lock();

        try {
            shouldCheckDefaultUidNative(getNativeHandle(), value);
        } finally {
            rwlock.readLock().unlock();
        }

        return this;
    }

    /*!
     * Should be used in tests for mocking tvmapi
     */
    @Nonnull
    public TvmApiSettings setTvmPortForLocalhost(int port) {
        return setTvmHostPort("localhost", port);
    }

    @Nonnull
    public TvmApiSettings setTvmHostPort(@Nonnull String host, int port) {
        rwlock.readLock().lock();

        try {
            setTvmHostPortNative(getNativeHandle(), host, port);
        } finally {
            rwlock.readLock().unlock();
        }

        return this;
    }

    @Nonnull
    public TvmApiSettings setTiroleConnectionParams(@Nonnull String host, int port, int tvmid) {
        rwlock.readLock().lock();

        try {
            setTiroleConnectionParamsNative(getNativeHandle(), host, port, tvmid);
        } finally {
            rwlock.readLock().unlock();
        }

        return this;
    }

    public long getHandle() {
        rwlock.readLock().lock();
        try {
            return getNativeHandle();
        } finally {
            rwlock.readLock().unlock();
        }
    }

    @Override
    protected void disposeHandle(long handle) {
        dispose(handle);
    }

    private static native long factory();
    private static native void dispose(long nativeHandle);
    private static native void setSelfTvmIdNative(long nativeHandle, int tvmId);
    private static native void enableServiceTicketCheckingNative(long nativeHandle);
    private static native void enableUserTicketCheckingNative(long nativeHandle, int env);
    private static native void setDiskCacheDirNative(long nativeHandle, String dir);
    private static native void enableServiceTicketsFetchOptionsWithAliases(long nativeHandle,
                                                                           String selfSecret,
                                                                           String dsts);
    private static native void enableServiceTicketsFetchOptionsWithTvmIds(long nativeHandle,
                                                                          String selfSecret,
                                                                          String dsts);
    private static native void fetchRolesForIdmSystemSlugNative(long nativeHandle, String slug);
    private static native void shouldCheckSrcNative(long nativeHandle, boolean value);
    private static native void shouldCheckDefaultUidNative(long nativeHandle, boolean value);
    private static native void setTvmHostPortNative(long nativeHandle, String host, int port);
    private static native void setTiroleConnectionParamsNative(long nativeHandle, String host, int port, int tvmid);

    static {
        JniUtils.loadLibrary();
    }
}
