package ru.yandex.passport.tvmauth;

import javax.annotation.Nonnull;

import ru.yandex.passport.tvmauth.internal.JniUtils;

public final class Version {
    private Version() {
    }

    @Nonnull
    public static native String get();

    static {
        JniUtils.loadLibrary();
    }
}
