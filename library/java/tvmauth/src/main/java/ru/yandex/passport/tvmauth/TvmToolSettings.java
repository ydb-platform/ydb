package ru.yandex.passport.tvmauth;

import javax.annotation.Nonnull;

import ru.yandex.passport.tvmauth.internal.JniUtils;
import ru.yandex.passport.tvmauth.internal.NativeHandle;

/**
 * Uses local http-interface to get state: http://localhost/tvm/.
 * This interface can be provided with tvmtool (local daemon) or Qloud/YP (local http api in container).
 * See more: https://wiki.yandex-team.ru/passport/tvm2/qloud/.
 *
 * Most part of settings will be fetched from tvmtool on start of client.
 * You need to use aliases for TVM-clients (src and dst) which you specified in tvmtool or Qloud/YP interface
 */
public final class TvmToolSettings extends NativeHandle {
    /**
     * Sets default values:
     * - hostname == "localhost"
     * - port detected with env["DEPLOY_TVM_TOOL_URL"] (provided with Yandex.Deploy),
     *      otherwise port == 1 (it is ok for Qloud).
     * - authToken: env["TVMTOOL_LOCAL_AUTHTOKEN"] (provided with Yandex.Deploy),
     *      otherwise env["QLOUD_TVM_TOKEN"] (provided with Qloud)
     *
     * AuthToken is protection from SSRF.
     *
     * @param selfAias - alias for your TVM client, which you specified in tvmtool or YD interface
     */
    public TvmToolSettings(@Nonnull String selfAlias) {
        this.nativeHandle = factory(selfAlias);
    }
    @Nonnull
    public static TvmToolSettings create(@Nonnull String selfAlias) {
        return new TvmToolSettings(selfAlias);
    }

    /**
     * Look at comment for ctor
     * @param port
     */
    @Nonnull
    public TvmToolSettings setPort(int port) {
        rwlock.readLock().lock();

        try {
            setPortNative(getNativeHandle(), port);
        } finally {
            rwlock.readLock().unlock();
        }

        return this;
    }

    /**
     * Default value: hostname == "localhost"
     * @param hostname
     */
    @Nonnull
    public TvmToolSettings setHostname(@Nonnull String hostname) {
        rwlock.readLock().lock();

        try {
            setHostnameNative(getNativeHandle(), hostname);
        } finally {
            rwlock.readLock().unlock();
        }

        return this;
    }

    /**
     * Look at comment for ctor
     * @param token
     */
    @Nonnull
    public TvmToolSettings setAuthToken(@Nonnull String authToken) {
        rwlock.readLock().lock();

        try {
            setAuthTokenNative(getNativeHandle(), authToken);
        } finally {
            rwlock.readLock().unlock();
        }

        return this;
    }

    /*!
      * Blackbox environment is provided by tvmtool for client.
      * You can override it for your purpose with limitations:
      *   (env from tvmtool) -> (override)
      *  - Prod/ProdYateam -> Prod/ProdYateam
      *  - Test/TestYateam -> Test/TestYateam
      *  - Stress -> Stress
      *
      * You can contact tvm-dev@yandex-team.ru if limitations are too strict
      * @param env
      */
    @Nonnull
    public TvmToolSettings overrideBlackboxEnv(@Nonnull BlackboxEnv env) {
        rwlock.readLock().lock();

        try {
            overrideBlackboxEnv(getNativeHandle(), env.ordinal());
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
    public TvmToolSettings shouldCheckSrc(boolean value) {
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
    public TvmToolSettings shouldCheckDefaultUid(boolean value) {
        rwlock.readLock().lock();

        try {
            shouldCheckDefaultUidNative(getNativeHandle(), value);
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

    private static native long factory(String selfAlias);
    private static native void dispose(long nativeHandle);
    private static native void setPortNative(long nativeHandle, int port);
    private static native void setHostnameNative(long nativeHandle, String hostname);
    private static native void setAuthTokenNative(long nativeHandle, String authToken);
    private static native void overrideBlackboxEnv(long nativeHandle, int env);
    private static native void shouldCheckSrcNative(long nativeHandle, boolean value);
    private static native void shouldCheckDefaultUidNative(long nativeHandle, boolean value);

    static {
        JniUtils.loadLibrary();
    }
}
