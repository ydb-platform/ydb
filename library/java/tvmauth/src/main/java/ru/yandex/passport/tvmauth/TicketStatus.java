package ru.yandex.passport.tvmauth;

/**
 * TicketStatus mean result of ticket check
 */
public enum TicketStatus {
    OK,
    EXPIRED,
    INVALID_BLACKBOX_ENV,
    INVALID_DST,
    INVALID_TICKET_TYPE,
    MALFORMED,
    MISSING_KEY,
    SIGN_BROKEN,
    UNSUPPORTED_VERSION,
    NO_ROLES,
}
