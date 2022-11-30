package ru.yandex.passport.tvmauth;

import ru.yandex.passport.tvmauth.roles.Roles;

public interface TvmClient extends AutoCloseable {

    ClientStatus getStatus();
    String getServiceTicketFor(String alias);
    String getServiceTicketFor(int tvmId);
    CheckedServiceTicket checkServiceTicket(String ticketBody);
    CheckedUserTicket checkUserTicket(String ticketBody);
    CheckedUserTicket checkUserTicket(String ticketBody, BlackboxEnv overridedBbEnv);
    Roles getRoles();

    @Override
    void close();
}
