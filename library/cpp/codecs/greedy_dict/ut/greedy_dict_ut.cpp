#include "gd_builder.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/string_utils/relaxed_escaper/relaxed_escaper.h>
#include <util/string/printf.h>
#include <util/generic/ymath.h>

class TGreedyDictTest: public TTestBase {
    UNIT_TEST_SUITE(TGreedyDictTest);
    UNIT_TEST(TestEntrySet)
    UNIT_TEST(TestBuilder0)
    UNIT_TEST(TestBuilder)
    UNIT_TEST_SUITE_END();

    void TestEntrySet() {
        using namespace NGreedyDict;

        {
            TEntrySet d;

            d.InitWithAlpha();

            for (TEntrySet::const_iterator it = d.begin(); it != d.end(); ++it) {
                UNIT_ASSERT_C(!it->HasPrefix(), Sprintf("%u -> %u", it->Number, it->NearestPrefix));
                UNIT_ASSERT_VALUES_EQUAL(it->Number, (ui32)(it - d.begin()));
            }

            UNIT_ASSERT_VALUES_EQUAL(d.size(), 256u);
            TStringBuf s = "aaabbb";
            UNIT_ASSERT_VALUES_EQUAL(d.FindPrefix(s)->Str, "a");
            UNIT_ASSERT_VALUES_EQUAL(s, "aabbb");
            UNIT_ASSERT_VALUES_EQUAL(d.FindPrefix(s)->Str, "a");
            UNIT_ASSERT_VALUES_EQUAL(s, "abbb");
            UNIT_ASSERT_VALUES_EQUAL(d.FindPrefix(s)->Str, "a");
            UNIT_ASSERT_VALUES_EQUAL(s, "bbb");
            UNIT_ASSERT_VALUES_EQUAL(d.FindPrefix(s)->Str, "b");
            UNIT_ASSERT_VALUES_EQUAL(s, "bb");
            UNIT_ASSERT_VALUES_EQUAL(d.FindPrefix(s)->Str, "b");
            UNIT_ASSERT_VALUES_EQUAL(s, "b");
            UNIT_ASSERT_VALUES_EQUAL(d.FindPrefix(s)->Str, "b");
            UNIT_ASSERT_VALUES_EQUAL(s, "");
            s = TStringBuf("", 1);
            UNIT_ASSERT_VALUES_EQUAL(d.FindPrefix(s)->Str, TStringBuf("", 1));
            UNIT_ASSERT_VALUES_EQUAL(s, "");
            s = "\xFF";
            UNIT_ASSERT_VALUES_EQUAL(d.FindPrefix(s)->Str, "\xFF");
            UNIT_ASSERT_VALUES_EQUAL(s, "");
        }
        {
            TEntrySet d;
            d.Add("a");
            d.Add("b");
            d.Add("b", "a");
            d.BuildHierarchy();

            UNIT_ASSERT_VALUES_EQUAL(d.size(), 3u);

            TStringBuf s = "bab";
            UNIT_ASSERT_VALUES_EQUAL(d.FindPrefix(s)->Str, "ba");
            UNIT_ASSERT_VALUES_EQUAL(s, "b");
        }
        {
            TEntrySet d;

            d.Add("a");
            d.Add("aa");
            d.Add("aaa");
            d.Add("aab");
            d.Add("b");
            d.Add("ba");

            d.BuildHierarchy();

            UNIT_ASSERT_VALUES_EQUAL(d.size(), 6u);
            {
                TStringBuf s = "aaaaa";
                const TEntry* e = d.FindPrefix(s);
                UNIT_ASSERT_VALUES_EQUAL(e->Str, "aaa");
                UNIT_ASSERT_VALUES_EQUAL(e->Number, 2u);
                UNIT_ASSERT_VALUES_EQUAL(e->NearestPrefix, 1);
                UNIT_ASSERT_VALUES_EQUAL(s, "aa");
            }

            {
                TStringBuf s = "a";
                const TEntry* e = d.FindPrefix(s);
                UNIT_ASSERT_VALUES_EQUAL(e->Str, "a");
                UNIT_ASSERT_VALUES_EQUAL(e->Number, 0u);
                UNIT_ASSERT_VALUES_EQUAL(e->NearestPrefix, -1);
                UNIT_ASSERT_VALUES_EQUAL(s, "");
            }

            {
                TStringBuf s = "bab";
                const TEntry* e = d.FindPrefix(s);
                UNIT_ASSERT_VALUES_EQUAL(e->Str, "ba");
                UNIT_ASSERT_VALUES_EQUAL(e->Number, 5u);
                UNIT_ASSERT_VALUES_EQUAL(e->NearestPrefix, 4);
                UNIT_ASSERT_VALUES_EQUAL(s, "b");
            }

            {
                TStringBuf s = "bba";
                const TEntry* e = d.FindPrefix(s);
                UNIT_ASSERT_VALUES_EQUAL(e->Str, "b");
                UNIT_ASSERT_VALUES_EQUAL(e->Number, 4u);
                UNIT_ASSERT_VALUES_EQUAL(e->NearestPrefix, -1);
                UNIT_ASSERT_VALUES_EQUAL(s, "ba");
            }
        }
    }

    void TestBuilder0() {
        using namespace NGreedyDict;
        ui32 a = 1, b = 11;
        ui64 ab = TDictBuilder::Compose(a, b);
        UNIT_ASSERT(TDictBuilder::IsCompound(ab));
        UNIT_ASSERT_VALUES_EQUAL(TDictBuilder::Prev(ab), a);
        UNIT_ASSERT_VALUES_EQUAL(TDictBuilder::Next(ab), b);
    }

    void FillData(NGreedyDict::TStringBufs& data) {
        static const char* urls[] = {"http://53.ru/car/motors/foreign/opel/tigra/", "http://abakan.24au.ru/tender/85904/", "http://anm15.gulaig.com/", "http://avto-parts.com/mercedes-benz/mercedes-benz-w220-1998-2005/category-442/category-443/", "http://ballooncousin.co.uk/", "http://benzol.ru/equipment/?id=1211&parent=514", "http://blazingseorank.com/blazing-seo-rank-free-website-analysis-to-increase-rank-and-traffic-450.html", "http://blogblaugrana.contadorwebmasters.com/", "http://bristolhash.org.uk/bh3cntct.php", "http://broker.borovichi.ru/category/item/3/1/0/8/28/257", "http://canoncompactcamerax.blogspot.com/", "http://classifieds.smashits.com/p,107881,email-to-friend.htm", "http://conferences.ksde.org/Portals/132/FallAssessment/SAVETHEDAY-FA09.pdf", "http://eway.vn/raovat/325-dien-tu-gia-dung/337-dieu-hoa/98041-b1-sua-may-lanh-quan-binh-tan-sua-may-lanh-quan-binh-chanh-hh-979676119-toan-quoc.html", "http://gallery.e2bn.org/asset73204_8-.html", "http://goplay.nsw.gov.au/activities-for-kids/by/historic-houses-trust/?startdate=2012-07-10", "http://grichards19067.multiply.com/", "http://hotkovo.egent.ru/user/89262269084/", "http://howimetyourself.com/?redirect_to=http://gomiso.com/m/suits/seasons/2/episodes/2", "http://islamqa.com/hi/ref/9014/DEAD%20PEOPLE%20GOOD%20DEEDS", "http://lapras.rutube.ru/", "http://nceluiko.ya.ru/", "http://nyanyanyanyaa.beon.ru/", "http://ozbo.com/Leaf-River-DV-7SS-7-0-MP-Game-Camera-K1-32541.html", "http://sbantom.ru/catalog/chasy/632753.html", "http://shopingoff.com/index.php?option=com_virtuemart&Itemid=65&category_id=&page=shop.browse&manufacturer_id=122&limit=32&limitstart=96", "http://shopingoff.com/katalog-odezhdy/manufacturer/62-christian-audigier.html?limit=32&start=448", "https://webwinkel.ah.nl/process?fh_location=//ecommerce/nl_NL/categories%3C%7Becommerce_shoc1%7D/it_show_product_code_1384%3E%7B10%3B20%7D/pr_startdate%3C20120519/pr_enddate%3E20120519/pr_ltc_allowed%3E%7Bbowi%7D/categories%3C%7Becommerce_shoc1_1al%7D/categories%3C%7Becommerce_shoc1_1al_1ahal%7D&&action=albert_noscript.modules.build", "http://top100.rambler.ru/navi/?theme=208/210/371&rgn=17", "http://volgogradskaya-oblast.extra-m.ru/classifieds/rabota/vakansii/banki-investicii/901467/", "http://wikien4.appspot.com/wiki/Warburg_hypothesis", "http://wola_baranowska.kamerzysta24.com.pl/", "http://www.10dot0dot0dot1.com/", "http://www.anima-redux.ru/index.php?key=gifts+teenage+girls", "http://www.aquaticabyseaworld.com/Calendar.aspx/CP/CP/CP/sp-us/CP/CP/ParkMap/Tickets/Weather.aspx", "http://www.autousa.com/360spin/2012_cadillac_ctssportwagon_3.6awdpremiumcollection.htm", "http://www.booking.com/city/gb/paignton-aireborough.html?inac=0&lang=pl", "http://www.booking.com/city/it/vodo-cadore.en.html", "http://www.booking.com/district/us/new-york/rockefeller-center.html&lang=no", "http://www.booking.com/hotel/bg/crown-fort-club.lv.html", "http://www.booking.com/hotel/ca/gouverneur-rimouski.ar.html", "http://www.booking.com/hotel/ch/l-auberge-du-chalet-a-gobet.fi.html", "http://www.booking.com/hotel/de/mark-garni.ru.html?aid=337384;label=yandex-hotel-mark-garni-68157-%7Bparam1%7D", "http://www.booking.com/hotel/de/mercure-goldschmieding-castrop-rauxel.ro.html", "http://www.booking.com/hotel/de/zollenspieker-fahrhaus.fr.html", "http://www.booking.com/hotel/es/jardin-metropolitano.ca.html", "http://www.booking.com/hotel/fr/clim.fr.html", "http://www.booking.com/hotel/fr/radisson-sas-toulouse-airport.et.html", "http://www.booking.com/hotel/gb/stgileshotel.ro.html?srfid=68c7fe42a03653a8796c84435c5299e4X16?tab=4", "http://www.booking.com/hotel/gr/rodos-park-suites.ru.html", "http://www.booking.com/hotel/id/le-grande-suites-bali.ru.html", "http://www.booking.com/hotel/it/mozart.it.html?aid=321655", "http://www.booking.com/hotel/ni/bahia-del-sol-villas.ru.html?dcid=1;dva=0", "http://www.booking.com/hotel/nl/cpschiphol.ro.html.ro.html?tab=4", "http://www.booking.com/hotel/th/laem-din.en-gb.html", "http://www.booking.com/hotel/th/tinidee-ranong.en.html", "http://www.booking.com/hotel/us/best-western-plus-merrimack-valley.hu.html", "http://www.booking.com/hotel/vn/tan-hai-long.km.html", "http://www.booking.com/landmark/au/royal-brisbane-women-s-hospital.vi.html", "http://www.booking.com/landmark/hk/nam-cheong-station.html&lang=id", "http://www.booking.com/landmark/it/spanish-steps.ca.html", "http://www.booking.com/landmark/sg/asian-civilisations-museum.html&lang=fi", "http://www.booking.com/place/fi-1376029.pt.html", "http://www.booking.com/place/tn257337.pl.html", "http://www.booking.com/region/ca/niagarafalls.ar.html&selected_currency=PLN", "http://www.booking.com/region/mx/queretaro.pt-pt.html&selected_currency=AUD", "http://www.booking.com/searchresults.en.html?city=20063074", "http://www.booking.com/searchresults.et.html?checkin=;checkout=;city=-394632", "http://www.booking.com/searchresults.lv.html?region=3936", "http://www.cevredanismanlari.com/index.php/component/k2/index.php/mevzuat/genel-yazlar/item/dosyalar/index.php?option=com_k2&view=item&id=16:iso-14001-%C3%A7evre-y%C3%B6netim-sistemi&Itemid=132&limitstart=107120", "http://www.dh-wholesaler.com/MENS-POLO-RACING-TEE-RL-p-417.html", "http://www.employabilityonline.net/", "http://www.esso.inc.ru/board/tools.php?event=profile&pname=Invinerrq", "http://www.filesurgery.ru/searchfw/kids_clothes-3.html", "http://www.furnitureandcarpetsource.com/Item.aspx?ItemID=-2107311899&ItemNum=53-T3048", "http://www.gets.cn/product/Gold-Sand-Lampwork-Glass-Beads--Flat-round--28x28x13mm_p260717.html", "http://www.gets.cn/wholesale-Sterling-Silver-Pendant-Findings-3577_S--L-Star-P-1.html?view=1&by=1", "http://www.homeandgardenadvice.com/diy/Mortgages_Loans_and_Financing/9221.html", "http://www.hongkongairport.com/eng/index.html/passenger/passenger/transport/to-from-airport/business/about-the-airport/transport/shopping/entertainment/t2/passenger/interactive-map.html", "http://www.hongkongairport.com/eng/index.html/shopping/insideshopping/all/passenger/transfer-transit/all/airline-information/shopping/entertainment/t2/business/about-the-airport/welcome.html", "http://www.hongkongairport.com/eng/index.html/transport/business/about-the-airport/transport/business/airport-authority/passenger/shopping/dining/all/dining.html", "http://www.idedge.com/index.cfm/fuseaction/category.display/category_id/298/index.cfm", "http://www.istanbulburda.com/aramalar.php", "http://www.jewelryinthenet.com/ads/AdDetail.aspx?AdID=1-0311002490689&stid=22-0111001020877", "http://www.johnnydepp.ru/forum/index.php?showtopic=1629&mode=linearplus&view=findpost&p=186977", "http://www.johnnydepp.ru/forum/index.php?showtopic=476&st=60&p=87379&", "http://www.joseleano.com/joomla/index.php/audio", "http://www.kaplicarehberi.com/tag/sakar-ilicali-kaplicalari/feed", "http://www.khaber.com.tr/arama.html?key=%C3%A7avdar", "http://www.kiz-oyunlari1.com/1783/4437/4363/1056/4170/Bump-Copter2-.html", "http://www.kiz-oyunlari1.com/3752/2612/4175/1166/3649/1047/Angelina-Oyunu.html", "http://www.kiz-oyunlari1.com/4266/3630/3665/3286/4121/301/3274/Sinir-Sinekler-.html", "http://www.kuldiga.lv/index.php?f=8&cat=371", "http://www.kuldiga.lv/index.php/img/index.php?l=lv&art_id=1836&show_c=&cat=85", "http://www.patronessa.ru/remontiruemsya/kuzovnie30raboti.html", "http://www.rapdict.org/Nu_Money?title=Talk:Nu_Money&action=edit", "http://www.serafin-phu.tabor24.com/?page=8", "http://www.shoes-store.org/brand1/Kids/Minnetonka.html", "http://www.shoes-store.org/shoes-store.xml", "http://www.way2allah.com/khotab-download-34695.htm"};
        data.clear();
        data.insert(data.begin(), urls, urls + Y_ARRAY_SIZE(urls));
    }

    typedef THashMap<TStringBuf, NGreedyDict::TEntry> TDict;

    TAutoPtr<NGreedyDict::TEntrySet> DoTestBuilder(const NGreedyDict::TBuildSettings& s,
                                                   TDict& res) {
        using namespace NGreedyDict;

        TStringBufs data;
        FillData(data);

        TDictBuilder b(s);
        b.SetInput(data);
        b.Build(256 + 128);

        TEntrySet& set = b.EntrySet();

        for (const auto& it : set) {
            if (it.Score) {
                res[it.Str] = it;
            }
        }

        return b.ReleaseEntrySet();
    }

    void DoAssertEntry(TStringBuf entry, ui32 number, i32 parent, float score, const TDict& dict) {
        TDict::const_iterator it = dict.find(entry);
        UNIT_ASSERT_C(it != dict.end(), entry);
        UNIT_ASSERT_VALUES_EQUAL_C(it->second.Number, number, entry);
        UNIT_ASSERT_VALUES_EQUAL_C(it->second.NearestPrefix, parent, entry);
        UNIT_ASSERT_VALUES_EQUAL_C(round(it->second.Score * 10000), round(score * 10000), entry);
    }

    void TestBuilder() {
        TAutoPtr<NGreedyDict::TEntrySet> set;
        THashMap<TStringBuf, NGreedyDict::TEntry> res;
        NGreedyDict::TBuildSettings s;
        set = DoTestBuilder(s, res);

        UNIT_ASSERT_VALUES_EQUAL(set->size(), 295u);
        UNIT_ASSERT_VALUES_EQUAL(res.size(), 110u);

        DoAssertEntry("%", 37, -1, 0.00375193, res);
        DoAssertEntry("%7", 38, 37, 0.00513299, res);
        DoAssertEntry("&", 39, -1, 0.00794527, res);
        DoAssertEntry("+", 44, -1, 0.000441404, res);
        DoAssertEntry(",", 45, -1, 0.000441404, res);
        DoAssertEntry("-", 46, -1, 0.0417126, res);
        DoAssertEntry(".", 47, -1, 0.0196425, res);
        DoAssertEntry(".com/", 48, 47, 0.0374482, res);
        DoAssertEntry(".html", 49, 47, 0.0496577, res);
        DoAssertEntry(".html?", 50, 49, 0.0153908, res);
        DoAssertEntry(".php", 51, 47, 0.0123585, res);
        DoAssertEntry(".ru/", 52, 47, 0.0150027, res);
        DoAssertEntry("/", 53, -1, 0.0452439, res);
        DoAssertEntry("/index", 54, 53, 0.0158905, res);
        DoAssertEntry("0", 55, -1, 0.00816597, res);
        DoAssertEntry("1", 56, -1, 0.0167733, res);
        DoAssertEntry("10", 57, 56, 0.00530474, res);
        DoAssertEntry("2", 58, -1, 0.0101523, res);
        DoAssertEntry("20", 59, 58, 0.00674234, res);
        DoAssertEntry("3", 60, -1, 0.01258, res);
        DoAssertEntry("32", 61, 60, 0.00490697, res);
        DoAssertEntry("4", 62, -1, 0.00993158, res);
        DoAssertEntry("5", 63, -1, 0.00617965, res);
        DoAssertEntry("6", 64, -1, 0.00971088, res);
        DoAssertEntry("7", 65, -1, 0.0101523, res);
        DoAssertEntry("8", 66, -1, 0.00728316, res);
        DoAssertEntry("9", 67, -1, 0.00728316, res);
        DoAssertEntry(":", 68, -1, 0.000662106, res);
        DoAssertEntry(";", 69, -1, 0.000882807, res);
        DoAssertEntry("=", 71, -1, 0.01258, res);
        DoAssertEntry("?", 73, -1, 0.00397263, res);
        DoAssertEntry("A", 75, -1, 0.00264842, res);
        DoAssertEntry("B", 76, -1, 0.00220702, res);
        DoAssertEntry("C", 77, -1, 0.00353123, res);
        DoAssertEntry("D", 78, -1, 0.00375193, res);
        DoAssertEntry("E", 79, -1, 0.00286912, res);
        DoAssertEntry("F", 80, -1, 0.00110351, res);
        DoAssertEntry("G", 81, -1, 0.00110351, res);
        DoAssertEntry("H", 82, -1, 0.000220702, res);
        DoAssertEntry("I", 83, -1, 0.00198632, res);
        DoAssertEntry("K", 85, -1, 0.000441404, res);
        DoAssertEntry("L", 86, -1, 0.00198632, res);
        DoAssertEntry("M", 87, -1, 0.00154491, res);
        DoAssertEntry("N", 88, -1, 0.00154491, res);
        DoAssertEntry("O", 89, -1, 0.00132421, res);
        DoAssertEntry("P", 90, -1, 0.00308983, res);
        DoAssertEntry("R", 92, -1, 0.000662106, res);
        DoAssertEntry("S", 93, -1, 0.00264842, res);
        DoAssertEntry("T", 94, -1, 0.00110351, res);
        DoAssertEntry("U", 95, -1, 0.000220702, res);
        DoAssertEntry("V", 96, -1, 0.000441404, res);
        DoAssertEntry("W", 97, -1, 0.000441404, res);
        DoAssertEntry("X", 98, -1, 0.000220702, res);
        DoAssertEntry("Y", 99, -1, 0.000220702, res);
        DoAssertEntry("_", 105, -1, 0.00904877, res);
        DoAssertEntry("a", 107, -1, 0.0505407, res);
        DoAssertEntry("an", 108, 107, 0.018273, res);
        DoAssertEntry("ar", 109, 107, 0.0169385, res);
        DoAssertEntry("b", 110, -1, 0.0156698, res);
        DoAssertEntry("c", 111, -1, 0.018539, res);
        DoAssertEntry("cat", 112, 111, 0.00846732, res);
        DoAssertEntry("ch", 113, 111, 0.00644872, res);
        DoAssertEntry("com", 114, 111, 0.00724235, res);
        DoAssertEntry("ct", 115, 111, 0.00605729, res);
        DoAssertEntry("d", 116, -1, 0.020746, res);
        DoAssertEntry("di", 117, 116, 0.00730659, res);
        DoAssertEntry("e", 118, -1, 0.0624586, res);
        DoAssertEntry("en", 119, 118, 0.0108999, res);
        DoAssertEntry("ent", 120, 119, 0.00616002, res);
        DoAssertEntry("f", 121, -1, 0.00860737, res);
        DoAssertEntry("fi", 122, 121, 0.00423196, res);
        DoAssertEntry("g", 123, -1, 0.0180975, res);
        DoAssertEntry("go", 124, 123, 0.00601862, res);
        DoAssertEntry("h", 125, -1, 0.010373, res);
        DoAssertEntry("ho", 126, 125, 0.00570298, res);
        DoAssertEntry("http://", 127, 125, 0.0494372, res);
        DoAssertEntry("http://www.", 128, 127, 0.0849702, res);
        DoAssertEntry("http://www.booking.com/", 129, 128, 0.071066, res);
        DoAssertEntry("http://www.booking.com/hotel/", 130, 129, 0.121607, res);
        DoAssertEntry("i", 131, -1, 0.0258221, res);
        DoAssertEntry("id=", 132, 131, 0.00725369, res);
        DoAssertEntry("im", 133, 131, 0.00373318, res);
        DoAssertEntry("in", 134, 131, 0.013625, res);
        DoAssertEntry("ing", 135, 134, 0.00795491, res);
        DoAssertEntry("ion", 136, 131, 0.00796149, res);
        DoAssertEntry("it", 137, 131, 0.00953416, res);
        DoAssertEntry("j", 138, -1, 0.00132421, res);
        DoAssertEntry("k", 139, -1, 0.0134628, res);
        DoAssertEntry("l", 140, -1, 0.0381814, res);
        DoAssertEntry("m", 141, -1, 0.0174354, res);
        DoAssertEntry("mer", 142, 141, 0.00711846, res);
        DoAssertEntry("n", 143, -1, 0.0132421, res);
        DoAssertEntry("o", 144, -1, 0.0302362, res);
        DoAssertEntry("on", 145, 144, 0.00802271, res);
        DoAssertEntry("ou", 146, 144, 0.00414545, res);
        DoAssertEntry("p", 147, -1, 0.0225116, res);
        DoAssertEntry("port", 148, 147, 0.0123532, res);
        DoAssertEntry("q", 149, -1, 0.00176561, res);
        DoAssertEntry("r", 150, -1, 0.0401677, res);
        DoAssertEntry("ran", 151, 150, 0.00686918, res);
        DoAssertEntry("s", 152, -1, 0.0487751, res);
        DoAssertEntry("sho", 153, 152, 0.0113876, res);
        DoAssertEntry("t", 154, -1, 0.0379607, res);
        DoAssertEntry("u", 155, -1, 0.0211874, res);
        DoAssertEntry("v", 156, -1, 0.00595895, res);
        DoAssertEntry("vi", 157, 156, 0.00480673, res);
        DoAssertEntry("w", 158, -1, 0.00816597, res);
        DoAssertEntry("x", 159, -1, 0.00375193, res);
        DoAssertEntry("y", 160, -1, 0.0130214, res);
        DoAssertEntry("z", 161, -1, 0.00353123, res);
    }
};

UNIT_TEST_SUITE_REGISTRATION(TGreedyDictTest);
