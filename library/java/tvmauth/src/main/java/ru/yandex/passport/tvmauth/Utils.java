package ru.yandex.passport.tvmauth;

import javax.annotation.Nonnull;

import ru.yandex.passport.tvmauth.internal.JniUtils;

public class Utils {
    private Utils() {
    }

    /**
     * Remove signature from ticket string - rest part can be parsed later with `tvmknife parse_ticket ...`
     * @return safe for logging part of ticket
     */
    @Nonnull
    public static native String removeTicketSignature(@Nonnull String ticketBody);

    static {
        JniUtils.loadLibrary();
    }
}
