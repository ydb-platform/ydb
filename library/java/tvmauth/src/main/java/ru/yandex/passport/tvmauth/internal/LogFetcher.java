package ru.yandex.passport.tvmauth.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.passport.tvmauth.TvmClient;

public class LogFetcher extends NativeHandle {
    class Message {
        int lvl;
        String msg;
    }

    private Logger logger;

    public LogFetcher() {
        this.logger = LoggerFactory.getLogger(TvmClient.class);
        this.nativeHandle = factory();
    }

    public void run() {
        rwlock.readLock().lock();

        if (nativeHandle == 0) {
            return;
        }

        Message[] msgs = null;
        try {
            msgs = fetch(getNativeHandle());
            if (msgs == null) {
                return;
            }

            for (Message m : msgs) {
                log(m.lvl, m.msg);
            }
        } catch (Exception e) {
            logger.error("Got error on processing log messages: {}", e);
        } finally {
            rwlock.readLock().unlock();
        }
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

    private void log(int lvl, String msg) {
        switch (lvl) {
        case 0:
        case 1:
        case 2:
        case 3:
            logger.error("{}", msg);
            break;
        case 4:
        case 5:
            logger.warn("{}", msg);
            break;
        case 6:
            logger.info("{}", msg);
            break;
        case 7:
            logger.debug("{}", msg);
            break;
        default: break;
        }
    }

    private static native long factory();
    private static native void dispose(long nativeHandle);
    private static native Message[] fetch(long nativeHandle);

    static {
        JniUtils.loadLibrary();
    }
}
