package ru.yandex.passport.tvmauth;

import javax.annotation.Nonnull;

import ru.yandex.passport.tvmauth.exception.NotAllowedException;

public class CheckedServiceTicket {
    private TicketStatus status;
    private String debugInfo;
    private int src;
    private long issuerUid;

    // TODO: PASSP-30786
    // Drop it
    public CheckedServiceTicket(TicketStatus status, String debugInfo, int src, long issuerUid) {
        this.status = status;
        this.debugInfo = debugInfo;
        this.src = src;
        this.issuerUid = issuerUid;
    }

    @Nonnull
    public String debugInfo() {
        return debugInfo;
    }

    /**
    * @return uid of developer, who got CheckedServiceTicket with grant_type=sshkey. Maybe 0 if issuer uid is absent
    */
    public long getIssuerUid() {
        checkStatus();
        return issuerUid;
    }

    public boolean booleanValue() {
        return status == TicketStatus.OK;
    }

    /**
     * You should check src with your ACL
     * @return tvmId of request source
     */
    public int getSrc() {
        checkStatus();
        return src;
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
