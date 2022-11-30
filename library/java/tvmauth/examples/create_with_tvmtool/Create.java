package ru.yandex.example;

import ru.yandex.passport.tvmauth.NativeTvmClient;
import ru.yandex.passport.tvmauth.TvmClient;
import ru.yandex.passport.tvmauth.TvmToolSettings;

public class Create {
    // Possibility of using functions depends on config of tvmtool
    //    checkServiceTicket
    //    checkUserTicket
    //    getServiceTicketFor

    public static TvmClient createClientInQloudOrYandexDeploy() {
        TvmToolSettings settings = TvmToolSettings.create("my_service");

        NativeTvmClient c = new NativeTvmClient(settings);

        return c;
    }

    public static TvmClient createClientForDevOrTests() {
        TvmToolSettings settings = TvmToolSettings.create("my_service");
        settings.setPort(18080);
        settings.setAuthToken("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");

        NativeTvmClient c = new NativeTvmClient(settings);

        return c;
    }
}
