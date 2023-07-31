#pragma once

#include <library/cpp/case_insensitive_string/case_insensitive_string.h>


namespace robotstxtcfg {
    // robots.txt agents and identifiers

    enum EBots : ui32 {
        id_anybot = 0,
        id_yandexbot = 1,
        id_yandexmediabot = 2,
        id_yandeximagesbot = 3,
        id_googlebot = 4,
        id_yandexbotmirr = 5,
        id_yahooslurp = 6,
        id_msnbot = 7,
        id_yandexcatalogbot = 8,
        id_yandexdirectbot = 9,
        id_yandexblogsbot = 10,
        id_yandexnewsbot = 11,
        id_yandexpagechk = 12,
        id_yandexmetrikabot = 13,
        id_yandexbrowser = 14,
        id_yandexmarketbot = 15,
        id_yandexcalendarbot = 16,
        id_yandexwebmasterbot = 17,
        id_yandexvideobot = 18,
        id_yandeximageresizerbot = 19,
        id_yandexadnetbot = 20,
        id_yandexpartnerbot = 21,
        id_yandexdirectdbot = 22,
        id_yandextravelbot = 23,
        id_yandexmobilebot = 24,
        id_yandexrcabot = 25,
        id_yandexdirectdynbot = 26,
        id_yandexmobilebot_ed = 27,
        id_yandexaccessibilitybot = 28,
        id_baidubot = 29,
        id_yandexscreenshotbot = 30,
        id_yandexmetrikayabs = 31,
        id_yandexvideoparserbot = 32,
        id_yandexnewsbot4 = 33,
        id_yandexmarketbot2 = 34,
        id_yandexmedianabot = 35,
        id_yandexsearchshopbot = 36,
        id_yandexontodbbot = 37,
        id_yandexontodbapibot = 38,
        id_yandexampbot = 39,
        id_yandexvideohosting = 40,
        id_yandexmediaselling = 41,
        id_yandexverticals = 42,
        id_yandexturbobot = 43,
        id_yandexzenbot = 44,
        id_yandextrackerbot = 45,
        id_yandexmetrikabot4 = 46,
        id_yandexmobilescreenshotbot = 47,
        id_yandexfaviconsbot = 48,
        id_yandexrenderresourcesbot = 49,
        id_yandexactivity = 50,
        max_botid
    };

    static const ui32 id_defbot = id_yandexbot;

    struct TBotInfo {
        TCaseInsensitiveStringBuf ReqPrefix;
        TCaseInsensitiveStringBuf FullName;
        TStringBuf FromField = {};
        TStringBuf UserAgent = {};
        TStringBuf RotorUserAgent = {};
        bool ExplicitDisallow = false;
    };

    static constexpr TStringBuf UserAgentFrom("support@search.yandex.ru");

    static constexpr TBotInfo BotInfoArr[] = {
        {"*", "*"},
        {"Yandex", "YandexBot/3.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},
        {"Yandex", "YandexMedia/3.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexMedia/3.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexMedia/3.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},
        {"Yandex", "YandexImages/3.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},
        {"Google", "GoogleBot"},
        {"Yandex", "YandexBot/3.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexBot/3.0; MirrorDetector; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexBot/3.0; MirrorDetector; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},
        {"Slurp", "Slurp"},
        {"msn", "msnbot"},
        {"Yandex", "YandexCatalog/3.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexCatalog/3.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexCatalog/3.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},
        {"YaDirectFetcher", "YaDirectFetcher/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YaDirectFetcher/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YaDirectFetcher/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},

        {"Yandex", "YandexBlogs/0.99", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexBlogs/0.99; robot; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexBlogs/0.99; robot; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},
        {"Yandex", "YandexNews/3.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexNews/3.0; robot; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexNews/3.0; robot; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},
        {"Yandex", "YandexPagechecker/2.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexPagechecker/2.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexPagechecker/2.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},
        {"Yandex", "YandexMetrika/3.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexMetrika/3.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexMetrika/3.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},
        {"Yandex", "YandexBrowser/1.0", UserAgentFrom,
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/536.5 (KHTML, like Gecko) YaBrowser/1.0.1084.5402 Chrome/19.0.1084.5409 Safari/536.5",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/536.5 (KHTML, like Gecko) YaBrowser/1.0.1084.5402 Chrome/19.0.1084.5409 Safari/536.5",
            false},
        {"Yandex", "YandexMarket/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexMarket/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexMarket/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},
        {"YandexCalendar", "YandexCalendar/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexCalendar/1.0 +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexCalendar/1.0 +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"Yandex", "YandexWebmaster/2.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexWebmaster/2.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexWebmaster/2.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},
        {"Yandex", "YandexVideo/3.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexVideo/3.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexVideo/3.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},
        {"Yandex", "YandexImageResizer/2.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexImageResizer/2.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexImageResizer/2.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},

        {"YandexDirect", "YandexDirect/3.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexDirect/3.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexDirect/3.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexPartner", "YandexPartner/3.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexPartner/3.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexPartner/3.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YaDirectFetcher", "YaDirectFetcher/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YaDirectFetcher/1.0; Dyatel; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YaDirectFetcher/1.0; Dyatel; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"Yandex", "YandexTravel/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexTravel/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexTravel/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},
        {"Yandex", "YandexBot/3.0", UserAgentFrom,
            "Mozilla/5.0 (iPhone; CPU iPhone OS 8_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12B411 Safari/600.1.4 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 8_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12B411 Safari/600.1.4 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
            false},
        {"YandexRCA", "YandexRCA/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexRCA/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexRCA/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexDirectDyn", "YandexDirectDyn/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexDirectDyn/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexDirectDyn/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexMobileBot", "YandexMobileBot/3.0", UserAgentFrom,
            "Mozilla/5.0 (iPhone; CPU iPhone OS 15_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Mobile/15E148 Safari/604.1 (compatible; YandexMobileBot/3.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 15_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Mobile/15E148 Safari/604.1 (compatible; YandexMobileBot/3.0; +http://yandex.com/bots)",
            true},
        {"YandexAccessibilityBot", "YandexAccessibilityBot/3.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexAccessibilityBot/3.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexAccessibilityBot/3.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"Baidu", "Baiduspider"},

        {"YandexScreenshotBot", "YandexScreenshotBot/3.0", UserAgentFrom,
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36 (compatible; YandexScreenshotBot/3.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36 (compatible; YandexScreenshotBot/3.0; +http://yandex.com/bots)",
            true},
        {"YandexMetrika", "YandexMetrika/2.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexMetrika/2.0; +http://yandex.com/bots yabs01)",
            "Mozilla/5.0 (compatible; YandexMetrika/2.0; +http://yandex.com/bots yabs01) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexVideoParser", "YandexVideoParser/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexVideoParser/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexVideoParser/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"Yandex", "YandexNews/4.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexNews/4.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexNews/4.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexMarket", "YandexMarket/2.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexMarket/2.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexMarket/2.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexMedianaBot", "YandexMedianaBot/1.0", UserAgentFrom,
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36 (compatible; YandexMedianaBot/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36 (compatible; YandexMedianaBot/1.0; +http://yandex.com/bots)",
            true},
        {"YandexSearchShop", "YandexSearchShop/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexSearchShop/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexSearchShop/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"Yandex", "YandexOntoDB/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexOntoDB/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexOntoDB/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            false},
        {"YandexOntoDBAPI", "YandexOntoDBAPI/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexOntoDBAPI/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexOntoDBAPI/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"Yandex-AMPHTML", "Yandex-AMPHTML", UserAgentFrom,
            "Mozilla/5.0 (compatible; Yandex-AMPHTML; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; Yandex-AMPHTML; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},

        {"YandexVideoHosting", "YandexVideoHosting/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexVideoHosting/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexVideoHosting/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexMediaSelling", "YandexMediaSelling/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexMediaSelling/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexMediaSelling/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexVerticals", "YandexVerticals/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexVerticals/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexVerticals/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexTurbo", "YandexTurbo/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexTurbo/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexTurbo/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexZenRss", "YandexZenRss/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexZenRss/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexZenRss/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexTracker", "YandexTracker/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexTracker/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexTracker/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexMetrika", "YandexMetrika/4.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexMetrika/4.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexMetrika/4.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexMobileScreenShotBot", "YandexMobileScreenShotBot/1.0", UserAgentFrom,
            "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/11.0 Mobile/12B411 Safari/600.1.4 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/11.0 Mobile/12B411 Safari/600.1.4 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
            true},
        {"YandexFavicons", "YandexFavicons/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexFavicons/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexFavicons/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexRenderResourcesBot", "YandexRenderResourcesBot/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexRenderResourcesBot/1.0; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexRenderResourcesBot/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true},
        {"YandexActivity", "YandexActivity/1.0", UserAgentFrom,
            "Mozilla/5.0 (compatible; YandexActivity; robot; +http://yandex.com/bots)",
            "Mozilla/5.0 (compatible; YandexActivity; robot; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0",
            true}
    };

    static_assert(std::size(BotInfoArr) == max_botid);

    constexpr auto GetReqPrefix(ui32 botId) {
        return BotInfoArr[botId].ReqPrefix;
    }

    constexpr auto GetFullName(ui32 botId) {
        return BotInfoArr[botId].FullName;
    }

    constexpr auto GetFromField(ui32 botId) {
        return BotInfoArr[botId].FromField;
    }

    constexpr auto GetUserAgent(ui32 botId) {
        return BotInfoArr[botId].UserAgent;
    }

    constexpr auto GetRotorUserAgent(ui32 botId) {
        return BotInfoArr[botId].RotorUserAgent;
    }

    constexpr bool IsExplicitDisallow(ui32 botId) {
        return BotInfoArr[botId].ExplicitDisallow;
    }

    constexpr bool IsYandexBotId(ui32 botId) {
        return !BotInfoArr[botId].UserAgent.empty();
    }

} // namespace robotstxtcfg
