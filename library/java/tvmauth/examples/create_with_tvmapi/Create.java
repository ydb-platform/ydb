package ru.yandex.example;

import java.util.HashMap;

import ru.yandex.passport.tvmauth.BlackboxEnv;
import ru.yandex.passport.tvmauth.NativeTvmClient;
import ru.yandex.passport.tvmauth.TvmApiSettings;
import ru.yandex.passport.tvmauth.TvmClient;

public class Create {
    public static TvmClient createClientForCheckingAllTicketsAndFetchingServiceTickets() {
        TvmApiSettings settings = new TvmApiSettings();

        settings.setSelfTvmId(11);
        HashMap<String,Integer> map = new HashMap<String,Integer>();
        map.put("bb", 224);
        map.put("datasync", 2000060);
        settings.enableServiceTicketsFetchOptions("qwe", map);
        settings.enableServiceTicketChecking();
        settings.enableUserTicketChecking(BlackboxEnv.TEST);
        settings.setDiskCacheDir("/var/cache/my_service/tvm/");

        NativeTvmClient c = new NativeTvmClient(settings);

        // c.checkServiceTicket("some service ticket")
        // c.checkUserTicket("some user ticket")
        // c.getServiceTicketFor("bb")
        // c.getServiceTicketFor(224)

        return c;
    }

    public static TvmClient createClientForCheckingAllTickets() {
        TvmApiSettings settings = new TvmApiSettings();

        settings.setSelfTvmId(11);
        settings.enableServiceTicketChecking();
        settings.enableUserTicketChecking(BlackboxEnv.TEST);
        settings.setDiskCacheDir("/var/cache/my_service/tvm/");

        NativeTvmClient c = new NativeTvmClient(settings);

        // c.checkServiceTicket("some service ticket")
        // c.checkUserTicket("some user ticket")

        return c;
    }

    public static TvmClient createClientForFetchingServiceTickets() {
        TvmApiSettings settings = new TvmApiSettings();

        settings.setSelfTvmId(11);
        HashMap<String,Integer> map = new HashMap<String,Integer>();
        map.put("bb", 224);
        map.put("datasync", 2000060);
        settings.enableServiceTicketsFetchOptions("qwe", map);
        settings.setDiskCacheDir("/var/cache/my_service/tvm/");

        NativeTvmClient c = new NativeTvmClient(settings);

        // c.getServiceTicketFor("bb")
        // c.getServiceTicketFor(224)

        return c;
    }

    public static TvmClient createClientForCheckingServiceTickets() {
        TvmApiSettings settings = new TvmApiSettings();

        settings.setSelfTvmId(11);
        settings.enableServiceTicketChecking();
        settings.setDiskCacheDir("/var/cache/my_service/tvm/");

        NativeTvmClient c = new NativeTvmClient(settings);

        // c.checkServiceTicket("some service ticket")

        return c;
    }

    public static TvmClient createClientForCheckingServiceTicketsWithRoles() {
        TvmApiSettings settings = new TvmApiSettings();

        settings.setSelfTvmId(11);
        settings.enableServiceTicketsFetchOptions("AAAAAAAAAAAAAAAAAAAAAA", new int[]{});
        settings.enableServiceTicketChecking();
        settings.fetchRolesForIdmSystemSlug("passporttestservice");
        settings.setDiskCacheDir("/var/cache/my_service/tvm/");

        NativeTvmClient c = new NativeTvmClient(settings);

        // CheckedServiceTicket t = c.checkServiceTicket("some service ticket")
        // ... = c.getRoles().getRolesForService(t)

        return c;
    }

    public static TvmClient createClientForCheckingAllTicketsWithRoles() {
        TvmApiSettings settings = new TvmApiSettings();

        settings.setSelfTvmId(11);
        settings.enableServiceTicketsFetchOptions("AAAAAAAAAAAAAAAAAAAAAA", new int[]{});
        settings.enableServiceTicketChecking();
        settings.enableUserTicketChecking(BlackboxEnv.PROD_YATEAM);
        settings.fetchRolesForIdmSystemSlug("passporttestservice");
        settings.setDiskCacheDir("/var/cache/my_service/tvm/");

        NativeTvmClient c = new NativeTvmClient(settings);

        // CheckedUserTicket t = c.checkUserTicket("some user ticket")
        // ... = c.getRoles().getRolesForUser(t)

        return c;
    }
}
