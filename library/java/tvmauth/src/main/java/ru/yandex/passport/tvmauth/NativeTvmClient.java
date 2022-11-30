package ru.yandex.passport.tvmauth;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;

import ru.yandex.passport.tvmauth.internal.JniUtils;
import ru.yandex.passport.tvmauth.internal.LogFetcher;
import ru.yandex.passport.tvmauth.internal.NativeHandle;
import ru.yandex.passport.tvmauth.roles.Roles;
import ru.yandex.passport.tvmauth.roles.RolesParser;

/**
 * Long lived thread-safe object for interacting with TVM. Each client starts system thread.
 *
 * In 99% cases NativeTvmClient shoud be created at service startup and live for the whole process lifetime.
 * If your case in this 1% and you need to RESTART client, you should to use method 'stop()' for old client.
 *
 * If you don't like method 'finalize()', you can destroy object manaully with method 'close()':
 * after that method 'finalize()' will be no-op.
 */
public class NativeTvmClient extends NativeHandle implements TvmClient {
    private Thread thread;
    private Task task;
    private Roles roles;
    private ReentrantReadWriteLock rolesLock = new ReentrantReadWriteLock();

    /**
     * Uses local http-interface to get state: http://localhost/tvm/.
     * This interface can be provided with tvmtool (local daemon) or Qloud/YP (local http api in container).
     * See more: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/.
     *
     * Logs stored in org.slf4j.Logger
     *
     * Starts thread for updating of in-memory cache in background
     * @param settings
     */
    public NativeTvmClient(@Nonnull TvmToolSettings settings) {
        LogFetcher log = new LogFetcher();
        try {
            this.nativeHandle = factoryTvmTool(settings.getHandle(), log.getHandle());
        } finally {
            log.run();
        }

        this.startThread(log);
    }

    /**
     * Uses general way to get state: https://tvm-api.yandex.net.
     * It is not recommended for Qloud/YP.
     *
     * Logs stored in org.slf4j.Logger
     *
     * Starts thread for updating of in-memory cache in background
     * Reads cache from disk if specified
     * @param settings
     */
    public NativeTvmClient(@Nonnull TvmApiSettings settings) {
        LogFetcher log = new LogFetcher();
        try {
            this.nativeHandle = factoryTvmApi(settings.getHandle(), log.getHandle());
        } finally {
            log.run();
        }

        this.startThread(log);
    }

    /**
      * For DynamicClient
      */
    protected NativeTvmClient() {
    }

    @Nonnull
    public static NativeTvmClient create(@Nonnull TvmToolSettings settings) {
        return new NativeTvmClient(settings);
    }
    @Nonnull
    public static NativeTvmClient create(@Nonnull TvmApiSettings settings) {
        return new NativeTvmClient(settings);
    }

    /**
     * You should trigger your monitoring if status is not Ok.
     * It will be unable to operate if status is Error.
     * Description: https://a.yandex-team.ru/arc/trunk/arcadia/library/java/tvmauth/client/README.md#NativeTvmClient
     * @return Current status of client.
     */
    @Override
    @Nonnull
    public ClientStatus getStatus() {
        rwlock.readLock().lock();
        try {
            return getStatusNative(getNativeHandle());
        } finally {
            rwlock.readLock().unlock();
        }
    }

    /**
     * Requires fetching options (from TClientSettings or Qloud/YP/tvmtool settings)
     * Can throw exception if cache is invalid or wrong config
     * @param dst - alias specified in settings or tvmtool
     */
    @Override
    @Nonnull
    public String getServiceTicketFor(@Nonnull String alias) {
        rwlock.readLock().lock();
        try {
            return getServiceTicketForAlias(getNativeHandle(), alias);
        } finally {
            rwlock.readLock().unlock();
        }
    }

    /**
     * Requires fetching options (from TClientSettings or Qloud/YP/tvmtool settings)
     * Can throw exception if cache is invalid or wrong config
     * @param dst - tvmId specified in settings or tvmtool
     */
    @Override
    @Nonnull
    public String getServiceTicketFor(int tvmId) {
        rwlock.readLock().lock();
        try {
            return getServiceTicketForTvmId(getNativeHandle(), tvmId);
        } finally {
            rwlock.readLock().unlock();
        }
    }

    /**
     * For TTvmApi::TClientSettings: checking must be enabled in TClientSettings
     * Can throw exception if checking was not enabled in settings
     * @param ticketBody
     */
    @Override
    @Nonnull
    public CheckedServiceTicket checkServiceTicket(@Nonnull String ticketBody) {
        rwlock.readLock().lock();
        try {
            return checkServiceTicketNative(getNativeHandle(), ticketBody);
        } finally {
            rwlock.readLock().unlock();
        }
    }

    /**
     * Requires blackbox environment (from TClientSettings or Qloud/YP/tvmtool settings)
     * Can throw exception if checking was not enabled in settings
     * @param ticketBody
     */
    @Override
    @Nonnull
    public CheckedUserTicket checkUserTicket(@Nonnull String ticketBody) {
        rwlock.readLock().lock();
        try {
            return checkUserTicketNative(getNativeHandle(), ticketBody);
        } finally {
            rwlock.readLock().unlock();
        }
    }

    /**
     * Requires blackbox environment (from TClientSettings or Qloud/YP/tvmtool settings)
     * Can throw exception if checking was not enabled in settings
     * @param ticketBody
     */
    @Nonnull
    public CheckedUserTicket checkUserTicket(@Nonnull String ticketBody, @Nonnull BlackboxEnv overridedBbEnv) {
        rwlock.readLock().lock();
        try {
            return checkUserTicketNativeWithOverridedEnv(getNativeHandle(), ticketBody, overridedBbEnv.ordinal());
        } finally {
            rwlock.readLock().unlock();
        }
    }

    @Nonnull
    public Roles getRoles() {
        rwlock.readLock().lock();
        try {
            rolesLock.readLock().lock();
            Roles currentRoles = roles;
            rolesLock.readLock().unlock();

            String currentRevision = null;
            if (currentRoles != null) {
                currentRevision = currentRoles.getMeta().getRevision();
            }

            String newRoles = getRolesNative(getNativeHandle(), currentRevision);
            if (newRoles == null) {
                return currentRoles;
            }

            rolesLock.writeLock().lock();
            try {
                // already updated
                if (currentRoles != roles) {
                    return roles;
                }

                roles = RolesParser.parse(newRoles);
                return roles;
            } finally {
                rolesLock.writeLock().unlock();
            }

        } finally {
            rwlock.readLock().unlock();
        }
    }

    /**
      * First call will delete object. Next calls will be no-op.
      */
    @Override
    public void close() {
        rwlock.writeLock().lock();

        try {
            if (this.task != null) {
                this.task.terminate();
                this.thread.interrupt();
            }

            dispose(nativeHandle);
            this.nativeHandle = 0;

            if (this.task != null) {
                this.task.doJob();
                this.task.destroy();
                this.task = null;
            }
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    protected void startThread(LogFetcher log) {
        this.task = new Task(log);
        this.thread = new Thread(task, "NativeTvmClient");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    protected void setNativeHandle(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    @Override
    protected void disposeHandle(long handle) {
        dispose(handle);
    }

    private static native long factoryTvmApi(long settings, long logger);
    private static native long factoryTvmTool(long settings, long logger);
    private static native void dispose(long nativeHandle);
    private static native ClientStatus getStatusNative(long nativeHandle);
    private static native String getServiceTicketForAlias(long nativeHandle, String alias);
    private static native String getServiceTicketForTvmId(long nativeHandle, int tvmId);
    private static native CheckedServiceTicket checkServiceTicketNative(long nativeHandle, String ticketBody);
    private static native CheckedUserTicket checkUserTicketNative(long nativeHandle, String ticketBody);
    private static native CheckedUserTicket checkUserTicketNativeWithOverridedEnv(long nativeHandle,
                                                                                  String ticketBody,
                                                                                  int env);
    private static native String getRolesNative(long nativeHandle, String revision);

    static {
        JniUtils.loadLibrary();
    }
}

class Task implements Runnable {
    private final LogFetcher log;
    private volatile boolean running = true;

    Task(LogFetcher log) {
        this.log = log;
    }

    @Override
    public void run() {
        while (running) {
            try {
                Thread.sleep(500L);
                this.doJob();
            } catch (InterruptedException e) {
                running = false;
            }
        }
    }

    public void doJob() {
        this.log.run();
    }

    public void terminate() {
        this.running = false;
    }

    public void destroy() {
        this.log.close();
    }
}
