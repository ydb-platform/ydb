package ru.yandex.passport.tvmauth;

import java.util.Arrays;

import javax.annotation.Nonnull;

import ru.yandex.passport.tvmauth.exception.NotAllowedException;

public class CheckedUserTicket {
    private TicketStatus status;
    private String debugInfo;
    private String[] scopes;
    private long defaultUid;
    private long[] uids;
    private BlackboxEnv env;

    // TODO: PASSP-30786
    // Drop it
    public CheckedUserTicket(TicketStatus status, String debugInfo, String[] scopes, long defaultUid, long[] uids) {
        this.status = status;
        this.debugInfo = debugInfo;
        this.scopes = scopes;
        this.defaultUid = defaultUid;
        this.uids = uids;
    }

    public boolean booleanValue() {
        return status == TicketStatus.OK;
    }

    @Nonnull
    public String debugInfo() {
        return debugInfo;
    }

    public boolean hasScope(@Nonnull String scopeName) {
        checkStatus();
        return Arrays.binarySearch(scopes, scopeName) >= 0;
    }

    /**
     * @return default user - may be 0
     */
    public long getDefaultUid() {
        checkStatus();
        return defaultUid;
    }

    /**
     * Never empty
     * @return array of scopes inherited from credential
     */
    @Nonnull
    public String[] getScopes() {
        checkStatus();
        return scopes;
    }

    /**
     * Never empty
     * @return array of valid users
     */
    @Nonnull
    public long[] getUids() {
        checkStatus();
        return uids;
    }

    @Nonnull
    public BlackboxEnv getEnv() {
        checkStatus();
        return env;
    }

    @Nonnull
    public TicketStatus getStatus() {
        return status;
    }

    private void checkStatus() {
        if (!this.booleanValue()) {
            throw new NotAllowedException("Ticket is not valid");
        }
    }
}
