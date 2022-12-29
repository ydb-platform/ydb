#include <library/cpp/codecs/delta_codec.h>
#include <library/cpp/codecs/huffman_codec.h>
#include <library/cpp/codecs/pfor_codec.h>
#include <library/cpp/codecs/solar_codec.h>
#include <library/cpp/codecs/zstd_dict_codec.h>
#include <library/cpp/codecs/comptable_codec.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/buffer.h>
#include <util/string/util.h>
#include <util/string/hex.h>
#include <library/cpp/string_utils/relaxed_escaper/relaxed_escaper.h>

namespace {
    const char* TextValues[] = {
        "! сентября газета",
        "!(возмездие это)!",
        "!(материнский капитал)",
        "!(пермь березники)",
        "!биография | !жизнь / + розинг | зворыгин & изобретение | телевидение | электронно лучевая трубка",
        "!овсиенко николай павлович",
        "!путин",
        "\"i'm on you\" p. diddy тимати клип",
        "\"билайн\" представит собственный планшет",
        "\"в особо крупном размере\"",
        "\"викиликс\" джулиан ассанж",
        "\"вимм билль данн",
        "\"газэнергосеть астрахань",
        "\"газэнергосеть астрахань\"",
        "\"домодедово\" ту-154",
        "\"жилина\" \"спартак\" видео",
        "\"зелёнsq шершнm\"",
        "\"зелёного шершня\"",
        "\"золотой граммофон\" марины яблоковой",
        "\"золотой граммофон-2010\"",
        "\"калинниковы\"",
        "\"манчестер юнайтед\" (англия) \"валенсия\" (испания) 1:1 (0:1)",
        "\"маркер\"",
        "\"моника\" засыпает москву снегом",
        "\"моника\" снегопад",
        "\"о безопасности\",",
        "\"памятку\" для пассажиров воздушных международных рейсов",
        "\"петровский парк\" и \"ходынское поле\"",
        "\"путинская\" трава",
        "\"пятерочка\"купила \"копейку\"",
        "\"пятёрочка\" и \"копейка\" объединились",
        "\"реал\" \"осер\" 4:0",
        "\"речь мутко\"",
        "\"российский лес 2010\"",
        "\"ростехинвентаризация федеральное бти\" рубцов",
        "\"саня останется с нами\",",
        "\"следопыт\" реалити шоу",
        "\"слышишь\" молодые авторы",
        "\"стадион\"",
        "\"ходынское поле\" метро",
        "\"хроники нарнии\"",
        "\"чистая вода\"",
        "\"школа деда мороза\"",
        "# asus -1394",
        "# сторонники wikileaks",
        "#106#",
        "#11",
        "#8 какой цвет",
        "#если клиент",
        "$ 13,79",
        "$ xnj ,s dct ,skb ljdjkmys !!!",
        "$ в день",
        "$ диск компьютера",
        "$.ajax",
        "$125 000",
        "$курс",
        "% в си",
        "% влады",
        "% годовых",
        "% женщин и % мужчин в россии",
        "% занятости персонала",
        "% инфляции 2010",
        "% инфляции в 2010 г.",
        "% налога",
        "% налогов в 2010г.",
        "% общего количества",
        "% от числа",
        "% по налогу на прибыль организации",
        "%24",
        "%академия%",
        "%комарова%татьяна",
        "& в 1с",
        "&& (+не существует | !такой проблемы)",
        "&gt;&gt;&gt;скачать | download c cs strikez.clan.su&lt;&lt;&lt;",
        "&gt;hbq nbityrjd",
        "&lt; какой знак",
        "&lt; лицей | &lt; техническая школа# &lt; история#&lt; лицей сегодня#&lt; перечень профессий#&lt; руководство лицея#&lt; прием учащихся#&lt; контакты#&lt; схема проезда#&lt; фотогалереяистория создания лицея и основные этапы путиулица купчинская дом 28",
        "&lt;&lt;link&gt;&gt;",
        "&lt;/storage&gt;",
        "&lt;bfnkjy",
        "&lt;bktntd",
        "&lt;cr",
        "&lt;ddr3&gt;",
        "&lt;e[ufknthcrbq abyfycjdsq",
        "&lt;fcctqys",
        "&lt;fhcf",
        "&lt;fhctkjyf he,by",
        "&lt;firbhbz",
        "&lt;fyr djphj;ltybt",
        "&lt;fyr vjcrds",
        "&lt;fyr резерв",
        "&lt;fyufkjh",
        "&lt;index&gt;",
        "&lt;jkmifz jrhe;yfz rbtd",
        "&lt;kbpytws",
        "&lt;megafon&gt; интернет",
        "&lt;thtpybrb gthvcrbq rhfq",
        "&lt;tkjxrf",
        "&lt;беларусь это мы",
        "&lt;бокс, версия ibf",
        "designer tree svc",
        "seriesg810",
        "doll makers",
        "rotten.com",
        "evening gowns",
        "discover",
        "south carolina escorts",
        "forkliftjobsinhousron",
        "mailbox",
        "alexis",
        "espn.com mlb",
        "gypsy.chat.2k",
        "the man in the mirror",
        "azteca",
        "sebastian telfair - jamel thomas",
        "kirby",
        "java",
        "trike motorcycles",
        "piasecki helicopter",
        "wicca binding spells",
        "pier park panama city beach .com",
        "continente europeo",
        "asswatchers.com",
        "asswatchers.com",
        "easton stealth stiff flex cnt adult baseball bat - 3",
        "facesofdeath",
        "video of 9 11",
        "profileedit.myspace.com",
        "georgia snakes",
        "yahoo.com",
        "google",
        "http wwwclassicindustries .corvettes-roadsters.com",
        "arington training stable",
        "find bred of dog",
        "southpark contact tables for myspace",
        "symptoms of laryngitis",
        "suzuki stickers",
        "avianca",
        "radio shack",
        "dominican republic pictures",
        "recent",
        "mapquest",
        "http myspace .com",
        "research chemicals supplies",
        "winn dixie.com",
        "drivers 20guide.com",
        "dylan whitley north carolina",
        "google com",
        "order wild horses cigarettes",
        "yahoocom",
        "fl runners",
        "aol companion install",
        "nbc.comdond 59595 6",
        "directv.com",
        "motorsports insurance",
        "cartoonnetwork",
        "pop warner-victorville",
        "black iorn spars",
        "goog",
        "the suns",
        "ebay",
        "pop warner",
        "philadelphia cream cheese",
        "oklahoma",
        "doudleday books.com",
        "javascript download",
        "city of nacogdoches",
        "sfyl",
        "myspace.com",
        "baptism pictures",
        "games",
        "depredadores sexuales",
        "mycl.cravelyrics.com",
        "become a bone marrow donner",
        "vintage copies",
        "ford dealership",
        "candystand",
        "smarthairypussyom",
        "yahoo.com",
        "vanderbilt.edu",
        "ebay",
        "grouper",
        "mys",
        "myrsa and birth defects",
        "hatteras rentals",
        "female escorts",
        "ja rule",
        "meat bluesheet",
        "yahoo",
        "american disability act court cases",
        "clearview cinemas",
        "hard69.com",
        "make a living will for free",
        "fat asses",
        "flashback concert in atlanta ga",
        "fucking",
        "flat abdomen exercises",
        "big brother facial",
        "german dictionary",
        "black dick",
        "ebonymovies",
        "airsoft rifles",
        "best fishing days calander",
        "tattoo",
        "impressions",
        "cs.com",
        "northwest airlines reservations",
        "halo 3",
        "wallbaums",
        "chat room listings",
        "waterbury ct warrants",
        "pictures of chad michael murry",
        "yahoo",
        "install wallpaper",
        "halo 3",
        "clits and tits",
        "prothsmouth general circuit courts",
        "old hawthorne columbia",
        "jess lee photos",
        "no deposit casino bonus",
        "bbc gladiator dressed to kill",
        "anemagazine.com",
        "lyrics unfaithful",
        "gold bars found",
        "art.comhttp",
        "free unlock key",
        "man o war lost a race",
        "blue cross and blue shield",
        "phenergan",
        "myspace.com",
        "http www.constitutional court.com",
        "monster trucks",
        "the breeze fort myers fla.newspaper",
        "davis origin name",
        "upper deck.com",
        "arizona",
        "akira lane",
        "ebaumsworld",
        "union pacific jobs",
        "google.cm",
        "free bigt girls nudes",
        "abcnews.com",
        "tootse.com",
        "az lyrics",
        "freddy",
        "georgia.com",
        "johncombest.com",
        "nelly",
        "gussi mane",
        "university of illinois",
        "oregan valcano's",
        "mythbusters",
        "sailormoon hentai",
        "international cub tractor",
        "desert sky movie green valley az",
        "evite",
        "nelly nud epics",
        "penndot.com",
        "first banks",
        "psp manual",
        "google",
        "jackieaudet hotmail.com",
        "internet",
        "shootinggames",
        "shootinggames",
        "montana western rendezvous of art",
        "hello kitty layouts",
        "yahoo",
        "translation",
        "glenn scott attorney",
        "hallofshame",
        "capitolone.com",
        "recipe for popovers",
        "pictures of demons",
        "barnes and nobles.com",
        "rbd",
        "hart and hunnington tattoo shop",
        "janepowellmovies.com",
        "ged schools in the military",
        "kelis",
        "hvacagent",
        "neat home organizer television show",
        "2719 24-2-crime and courts",
        "fsu",
        "torpedo bomber games",
        "love poems",
        "polly pocket'toys",
        "yweatherahoo.com",
        "jungle gin",
        "flemington new jersey real estate",
        "milf hunter stories",
        "budget.com",
        "chopperstyle",
        "keno player",
        "up skirt",
        "dogs",
        "beerballers",
        "phat white butt",
        "phat white butt",
        "va licensing for interpeters for the deaf",
        "white page phone book maiden north carolina",
        "controlled 20solutions 20corp.com",
        "friedman jewelery",
        "kelis",
        "curtains",
        "curtains",
        "fuck me harder",
        "naked girls",
        "southwest airlines boarding pass",
        "mailbox",
        "1976 mavrick",
        "adult diapers",
        "horse nasal discharge",
        "charles ludlam",
        "google",
        "himnos en espanol",
        "quarter horses for sale in nebraska",
        "cosmo",
        "hi",
        "mattel",
        "aouto 20trader.com",
        "sunsetter awnings",
        "bl.cfm",
        "at",
        "tattoo designs",
        "bubs",
        "yahoo",
        "free live gay cam chats",
        "antibiotics",
        "upgrade",
        "aessuccess.org",
        "yahoo",
        "boobdex",
        "the jackle",
        "plus size lingerie magazines for home",
        "lehigh valley little league",
        "ancient trade coins",
        "pillsbury",
        "colorado springs",
        "canada aviation jobs",
        "free guitar tablature",
        "kids aol",
        "capitol community colage",
        "kevin thomas bermuda",
        "missouri lotto",
        "homedepotsportscomplex.com",
        "dr. franklin schneier",
        "williamsburg va. hotels",
        "aim",
        "morningbuzz",
        "probusines.com",
        "wwwalbasoul.com",
        "w.runehints.com",
        "yahoo.com",
        "yahoo.com",
        "yahoo.com",
        "fantasy 5",
        "xxx rape",
        "hawaiian gift baskets",
        "madonna.com",
        "myspace contact tables",
        "white cock",
        "safe space",
        "drinks",
        "o rly",
        "dsl",
        "wwww.uncc.edu",
        "wwww.uncc.edu",
        "wwww.uncc.edu",
        "online overseas checkt.westernunion.com",
        "angina",
        "heba technologies",
        "hebrew ancient coins",
        "games",
        "recent",
        "international male.com",
        "sex pics",
        "paul wall layouts for myspace",
        "health",
        "wire lamp shade frames",
        "windows",
        "top business colleges",
        "mary jo eustace",
        "attored",
        "oklahoma indian legal services",
        "6arab",
        "santo nino",
        "10.1.0.199",
        "http www.myspace.com daffydonn07",
        "marine electrical",
        "sandy creek cabins weekend new york",
        "onionbutts",
        "tucson classifieds",
        "new york times",
        "recently deleted screen names",
        "goldeneagle.net",
        "fta support forums",
        "low protein in bloos",
        "datring",
        "lilwayne",
        "free billiards games",
        "yahoo",
        "ako",
        "a.tribalfusion.c script language",
        "dustin davis",
        "cooking",
        "yahoo.com",
        "universal studios",
        "adult chat",
        "santa monica flea market",
        "carpevino.us",
        "wine vinyard in stewertstown pa",
        "y",
        "craigslist",
        "ups.com",
        "1-866-347-3292",
        "renegade boats",
        "renegade boats",
        "sunset state beach caping",
        "artofstanlync.org",
        "heart-i want make love to you video",
        "triangles around the world",
        "mycl.cravelyrics.com",
        "in the bible what type of persons were forced to walk around in public and say unclean unclean",
        "providence water fire",
        "googlecom",
        "yahoo.com",
        "b.g",
        "website de rebelde",
        "stoplinks",
        "allison 2000 transmission",
        "thepriceanduseofgasoline.com",
        "chamillinaire",
        "veryspecialhomescom",
        "crashbandicoot",
        "a short sex story",
        "yahoo.com",
        "music now",
        "east carolina university",
        "vandalism in new york",
        "the bainde soleil company",
        "dicaprio movies",
        "xxx dvds",
        "visual basic scripting support",
        "english bulldogs",
        "travelocity.com",
        "website for asstr.org",
        "hypnotic slave training",
        "pogo",
        "university at buffalo addmissions",
        "screen name services",
        "superdrol",
        "art institute",
        "online business cards",
        "aolfinancial",
        "upgrade shop",
        "anderson abrasive",
        "weatherchannel.com",
        "recent",
        "ebay",
        "diagram and xray of a normal shouldercheck out surgicalpoker.comfor more sports medicine and orthopedic information and images check out emedx.com by dr. allan mishranormal diagram normal x-ray",
        "95 mustang gt chips",
        "gold grills",
        "hap housing in portland or",
        "car sales",
        "swimming with dolphins",
        "jennifer lopez nude",
        "wwwdubcnn.com",
        "dominicks pizza",
        "fl studio",
        "http blackplanet .com",
        "http blackplanet .com",
        "http blackplanet .com",
        "A$AP Rocky",
        "benie mac",
        "fujifilm.com",
        "aol dialup setup",
        "metal fabrication tools",
        "internet",
        "buy my painting",
        "pulaski va classifieds",
        "w.coj.net",
        "postopia.com",
        "no medical records hydrocodone",
        "auto completes for deal or no deal contest",
        "http www. big monster dicks .com",
        "invacare wheelchairs",
        "musicdownload.com",
        "president bush",
        "heavy equipment",
        "inmate information",
        "allina.com",
        "megan law.gov",
        "wwwl.eharmony.com",
        "jobs in colombiaoqx0nq",
        "beastsex",
        "ferguisson",
        "heart-i wanna make love to you vedio",
        "west georgia university",
        "west georgia university",
        "hsn",
        "bb&t",
        "midas realty",
        "yahoo",
        "mytrip.com",
        "donna texas mcdonalds",
        "free picture of our lady",
        "bubs",
        "taken chemo for 5 month's cancer can still be seen on ct scan",
        "porn 20video 20clips",
        "lake monsters",
        "freedj mix vibes",
        "myspace.coim",
        "la joya school district tx",
        "colorado bungee jumping",
        "yahoo",
        "google.com",
        "lafayette co vampire grave",
        "ice cube",
        "internet",
        "tccd.edu",
        "google",
        "people",
        "instructions on putting together a filing cabinet",
        "click.babycenter.com",
        "90minut",
        "ramien noodles",
        "lilwayne",
        "danni virgin",
        "nice sexy girls.com",
        "guttural pouch",
        "free male masturbating",
        "good",
        "rotton 20dot.com",
        "fox sports",
        "seth rogen",
        "desb.mspaceads.com",
        "betjc.com",
        "pictures of quebec",
        "gold in quartz",
        "evergreen college",
        "runescape",
        "gastons white river resort",
        "sunset beach santa cruz",
        "auto parts",
        "travelocity",
        "myspace.com",
        "laptops",
        "beyaonce and j",
        "free gay ebony knights webcams",
        "google",
        "derek watson",
        "alice in wonderland tshirts",
        "hippa p rivacy act",
        "down payment mortgage",
        "believe it or not",
        "mys",
        "datatreca",
        "onesuite",
        "names",
        "lil john",
        "scales of justice cuff links",
        "localsales.com",
        "alametris denise lipsey",
        "adam for adam",
        "flip flops crochet",
        "arbors",
        "heb hospital",
        "myspae.com",
        "midevil breast torture",
        "askjeeves",
        "assparade",
        ".comhttp",
        "weekly hotels reston virginia",
        "noiceinparadise.com",
        "pre diabetic diet",
        "h.i.m.com",
        "myspace",
        "myspace",
        "wwww.sex.lp.cpm",
        "mcso mugshots",
        "roush",
        "wellfargo",
        "lilwayne",
        "hopecherie",
        "frontgate.com",
        "barbados registration department",
        "american pitbull",
        "free pc full flight simulation game downloads",
        "google",
        "vaginal secretion grey stuff",
        "myspace layouts",
        "kanye west",
        "walmart",
        "pain in hip and leg",
        "tenneesseeaquarium.com",
        "suncom.com",
        "alysseandrachelwerehere",
        "pimiclo",
        "starmagazine.com",
        "classifieds",
        "mount rushmore in dakota",
        "sams",
        "disney com",
        "beastyality",
        "chief joseph paintings",
        "henry scott",
        "paris hilton",
        "kb903235",
        "autotrader",
        "irish traveller",
        "ajcobs.com",
        "art of stanlync.org",
        "fox news",
        "freeporn",
        "depo provera",
        "air france",
        "talk city active chats",
        "codes for the gamecube game resident evil 4",
        "good food to eat for sugar diabetes",
        "warpmymind",
        "arc jacksonville fl",
        "7fwww.sendspace.com",
        "j blackfoot",
        "mcso madison street jail inmate",
        "macys",
        "eduscapes",
        "free picture of our lady",
        "http www.eastman.org",
        "minneapolisstartribune localnews",
        "minneapolisstartribune localnews",
        "tennessee",
        "foodtown",
        "anti virous download",
        "http www.mdland rec.net",
        "ed edd eddy",
        "maryjbilge",
        "shipping services",
        "baseball videogames",
        "egyption ancient coins",
        "internet",
        "what is sodomy",
        "international cub lowboy",
        "mary j. bilge",
        "scenic backgrounds",
        "google.com",
        "rosettalangueges.com",
        "titanpoker.net",
        "titie show",
        "edelen realtor",
        "lil cim",
        "china.com",
        "boost mobile",
        "nc eipa",
        "people's 20pharmacy 20guide 20to",
        "costco",
        "charles schultz drawings",
        "nicisterling",
        "a picture of author stephen crane",
        "yahoo.com",
        "sponge bob myspace layouts",
        "g",
        "calendar creator",
        "careerbuilder.com",
        "cool tex for web pages",
        "yahoo.com",
        "mcdougal littel",
        "sign on",
        "superman",
        "radio",
        "lajollaindians.com",
        "mike tyson died",
        "pink panther",
        "lolita newgroups",
        "nude girls",
        "galveston 20texas",
        "gerlach meat co.",
        "thetakeover2006.com",
        "yahoo",
        "simpsons movie",
        "saxy",
        "yahoo",
        "21st century realty",
        "new zealand",
        "dogs",
        "weather",
        "free porn sex",
        "bugs bunny parties",
        "mortal kombat 2 fatalities",
        "sea life park hawaii",
        "songs for middle school choir",
        "rocky mountain jeep",
        "householdbank.com",
        "birdville isd",
        "brutal dildo",
        "brutal dildo",
        "free live gay cam chats",
        "wonder woman",
        "ebay com",
        "myspace.com",
        "boost mobile",
        "desktop themes sex",
        "myspace.com",
        "myspace.com",
        "maroon chevy auto dealership",
        "beyonce",
        "cleopatra vii",
        "accountcentralonline.com",
        "juvenile",
        "the game cock",
        "pics of ashland city tennessee",
        "coherent deos",
        "microwsoft wireless connection",
        "best buy",
        "southwest airlines",
        "southwest airlines",
        "pogo games",
        "family court record room in brooklyn newyork",
        "60.ufc.net",
        "us mint",
        "people",
        "firstcitycreditunion",
        "washington mutual careers",
        "beyonce",
        "tab energy drink",
        "http vemmabuilder.com",
        "new york state lottery",
        "yahoo",
        "tmobile",
        "yellow pages.com",
        "az.central.com",
        "pasco auto salvage",
        "im help",
        "home based businesses",
        "studyisland",
        "bible study from king james on 1 corinthians chapter 6 verses 18- 20",
        "bellevue-ne",
        "msn.com",
        "aolsignupfree",
        "the simsons",
        "nevada",
        "forsyth central high school",
        "road state college",
        "does my child have adhd",
        "les tucanesde tijuana",
        "yahoo.com",
        "mexican pharmacy hyrocodone",
        "ford motor co year end sales",
        "google.com",
        "google.com",
        "person.com",
        "marylyn monroe",
        "nfl",
        "the hun.net",
        "nkena anderson",
        "free netscape download",
        "top fifty colleges",
        "wil.",
        "memphis tennessee",
        "yahoo mail",
        "corrections officer of juveniles",
        "jada pinkett smith",
        "mapquest.com",
        "apartments",
        "msn.com",
        "msn.com",
        "wasco state prison",
        "solitaire",
        "http",
        "freeport seaman center",
        "futbol soccer",
        "screen names",
        "kmov.com",
        "survey.otxresearch.com",
        "facial shaves",
        "gle",
        "flw.com",
        "seasportboats.com",
        "toysrus.com",
        "animated sexy graphics",
        "colombia",
        "unitarian univeralist association",
        "fr",
        "google video.com",
        "660-342-1072",
        "suzan-lori parks",
        "male facial",
        "william bouguereau first kiss how much it is worth",
        "streetfighter",
        "nick.com",
        "wonder woman",
        "pentagram",
        "mcafee virus protection",
        "diary",
        "037f34742140a5f761ad51d95180b4f8",
        "free porn",
        "no deposit casino bonus",
        "spongebob the movie myspace layouts",
        "on line banking",
        "equestrian properties for sale",
        "kazaa free muisc download",
        "gay truckers",
        "24",
        "pay-pal",
        "www yahoo.com",
        "phatazz.white hoes",
        "planets of the universe",
        "free movies",
        "budget rentals special",
        "yahoogames",
        "talaat pasha",
        "mariah carey song lyrics don't forget about us",
        "futbol soccer",
        "msn groups",
        "martha steward",
        "martha steward",
        "soap opera scoops cbs",
        "cingular",
        "stuwie",
        "womengiving blowjobs",
        "hear dancing queen by abba",
        "love song",
        "fhsaa.org",
        "any dvd",
        "any dvd",
        "gallery.brookeskye.com",
        "gibson ranch",
        "wachovia com",
        "kzg golf information",
        "skylight curtains",
        "c",
        "123freeweblayouts.com",
        "yahoo.com",
        "allie.com",
        "ghosts of bingham cemetery",
        "resume maker",
        "resume maker",
        "resume maker",
        "lymphomatoid papulosis",
        "sez.com",
    };
}

class TCodecsTest: public TTestBase {
    UNIT_TEST_SUITE(TCodecsTest);
    UNIT_TEST(TestPipeline)
    UNIT_TEST(TestDelta)
    UNIT_TEST(TestHuffman)
    UNIT_TEST(TestZStdDict)
    UNIT_TEST(TestCompTable)
    UNIT_TEST(TestHuffmanLearnByFreqs)
    UNIT_TEST(TestSolar)
    UNIT_TEST(TestPFor)
    UNIT_TEST(TestRegistry)

    UNIT_TEST_SUITE_END();

private:
    TString PrintError(TStringBuf learn, TStringBuf test, TStringBuf codec, ui32 i) {
        TString s;
        TStringOutput sout(s);
        sout << codec << ": " << i << ", "
             << "\n";
        sout << HexEncode(learn.data(), learn.size()); //NEscJ::EscapeJ<true>(learn, sout);
        sout << " != \n";
        sout << HexEncode(test.data(), test.size()); //NEscJ::EscapeJ<true>(test, sout);

        if (s.Size() > 1536) {
            TString res = s.substr(0, 512);
            res.append("...<skipped ").append(ToString(s.size() - 1024)).append(">...");
            res.append(s.substr(s.size() - 512));
        }

        return s;
    }

    TStringBuf AsStrBuf(const TBuffer& b) {
        return TStringBuf(b.data(), b.size());
    }

    template <typename TCodec, bool testsaveload>
    void TestCodec(const TVector<TBuffer>& inlearn = TVector<TBuffer>(), const TVector<TBuffer>& in = TVector<TBuffer>(), NCodecs::TCodecPtr c = new TCodec) {
        using namespace NCodecs;

        TBuffer buff;

        {
            TVector<TBuffer> out;

            c->Learn(inlearn.begin(), inlearn.end());

            if (testsaveload) {
                {
                    TBufferOutput bout(buff);
                    ICodec::Store(&bout, c);
                }

                {
                    TBufferInput bin(buff);
                    c = ICodec::Restore(&bin);
                    UNIT_ASSERT(c->AlreadyTrained());
                }
            }

            {
                for (ui32 i = 0; i < inlearn.size(); ++i) {
                    out.emplace_back();
                    c->Encode(AsStrBuf(inlearn[i]), out[i]);
                }

                TBuffer vecl;
                for (ui32 i = 0; i < out.size(); ++i) {
                    vecl.Clear();
                    c->Decode(AsStrBuf(out[i]), vecl);

                    UNIT_ASSERT_EQUAL_C(AsStrBuf(inlearn[i]), AsStrBuf(vecl),
                                        PrintError(TStringBuf(inlearn[i].data(), inlearn[i].size()),
                                                   TStringBuf(vecl.data(), vecl.size()), c->GetName(), i));
                }
            }
        }

        {
            if (testsaveload) {
                TBufferInput bin(buff);
                c = ICodec::Restore(&bin);
            }

            TBuffer out, in1;
            for (ui32 i = 0; i < in.size(); ++i) {
                out.Clear();
                in1.Clear();
                c->Encode(AsStrBuf(in[i]), out);
                c->Decode(AsStrBuf(out), in1);
                UNIT_ASSERT_EQUAL_C(AsStrBuf(in[i]), AsStrBuf(in1),
                                    PrintError(TStringBuf(in[i].data(), in[i].size()),
                                               TStringBuf(in1.data(), in1.size()), c->GetName(), i));
            }
        }
    }

    template <class T>
    void AppendTo(TBuffer& b, T t) {
        b.Append((char*)&t, sizeof(t));
    }

    void TestDelta() {
        using namespace NCodecs;
        TVector<TBuffer> d;

        // 1. common case
        d.emplace_back();
        AppendTo(d.back(), 1ULL);
        AppendTo(d.back(), 10ULL);
        AppendTo(d.back(), 100ULL);
        AppendTo(d.back(), 1000ULL);
        AppendTo(d.back(), 10000ULL);
        AppendTo(d.back(), 100000ULL);

        // 2. delta overflow
        d.emplace_back();
        AppendTo(d.back(), 1ULL);
        AppendTo(d.back(), 10ULL);
        AppendTo(d.back(), 100ULL);
        AppendTo(d.back(), 1000ULL);
        AppendTo(d.back(), (ui64)-100LL);
        AppendTo(d.back(), (ui64)-10ULL);

        // 3. bad sorting
        d.emplace_back();
        AppendTo(d.back(), 1ULL);
        AppendTo(d.back(), 10ULL);
        AppendTo(d.back(), 1000ULL);
        AppendTo(d.back(), 100ULL);
        AppendTo(d.back(), 10000ULL);
        AppendTo(d.back(), 100000ULL);

        // all bad
        d.emplace_back();
        AppendTo(d.back(), -1LL);
        AppendTo(d.back(), -1LL);
        AppendTo(d.back(), -1LL);
        AppendTo(d.back(), -1LL);

        TestCodec<TDeltaCodec<ui64, true>, false>(d);
        TestCodec<TDeltaCodec<ui64, false>, false>(d);
    }

    void TestPFor() {
        using namespace NCodecs;
        {
            TVector<TBuffer> d;
            d.emplace_back();
            AppendTo(d.back(), -1LL);
            AppendTo(d.back(), -1LL);
            AppendTo(d.back(), -1LL);
            AppendTo(d.back(), -1LL);
            d.emplace_back();
            AppendTo(d.back(), 0LL);
            AppendTo(d.back(), 1LL);
            AppendTo(d.back(), 2LL);
            AppendTo(d.back(), 1LL);
            AppendTo(d.back(), 0LL);
            AppendTo(d.back(), 1LL);
            AppendTo(d.back(), 2LL);
            d.emplace_back();
            AppendTo(d.back(), 0LL);
            AppendTo(d.back(), 1LL);
            AppendTo(d.back(), 2LL);
            AppendTo(d.back(), 1LL);
            AppendTo(d.back(), -1LL);
            AppendTo(d.back(), 0LL);
            AppendTo(d.back(), 1LL);
            AppendTo(d.back(), 2LL);
            d.emplace_back();
            AppendTo(d.back(), 0LL);
            AppendTo(d.back(), -1LL);
            AppendTo(d.back(), -2LL);
            AppendTo(d.back(), -1LL);
            AppendTo(d.back(), -2LL);
            AppendTo(d.back(), -1LL);
            AppendTo(d.back(), 0LL);
            AppendTo(d.back(), -1LL);
            AppendTo(d.back(), -2LL);

            TestCodec<TPForCodec<ui64>, false>(d);
            TestCodec<TPForCodec<ui64, true>, true>(d);
        }
        {
            TVector<TBuffer> d;
            d.emplace_back();
            AppendTo(d.back(), -1);
            AppendTo(d.back(), -1);
            AppendTo(d.back(), -1);
            AppendTo(d.back(), -1);
            d.emplace_back();
            AppendTo(d.back(), 0);
            AppendTo(d.back(), 1);
            AppendTo(d.back(), 2);
            AppendTo(d.back(), 1);
            AppendTo(d.back(), -1);
            AppendTo(d.back(), 0);
            AppendTo(d.back(), 1);
            AppendTo(d.back(), 2);
            d.emplace_back();
            AppendTo(d.back(), 0);
            AppendTo(d.back(), -1);
            AppendTo(d.back(), -2);
            AppendTo(d.back(), -1);
            AppendTo(d.back(), -2);
            AppendTo(d.back(), -1);
            AppendTo(d.back(), 0);
            AppendTo(d.back(), -1);
            AppendTo(d.back(), -2);

            TestCodec<TPForCodec<ui32>, false>(d);
            TestCodec<TPForCodec<ui32, true>, false>(d);
        }
        {
            TVector<TBuffer> d;
            d.emplace_back();
            for (auto& textValue : TextValues) {
                AppendTo(d.back(), (ui32)strlen(textValue));
            }

            TestCodec<TPForCodec<ui32>, false>(d);
            TestCodec<TPForCodec<ui32, true>, false>(d);
        }
        {
            TVector<TBuffer> d;
            d.emplace_back();
            for (auto& textValue : TextValues) {
                AppendTo(d.back(), (ui64)strlen(textValue));
            }

            TestCodec<TPForCodec<ui64>, false>(d);
            TestCodec<TPForCodec<ui64, true>, false>(d);
        }
    }

    template <class TCodec>
    void DoTestSimpleCodec() {
        using namespace NCodecs;
        {
            TVector<TBuffer> learn;

            for (auto& textValue : TextValues) {
                learn.emplace_back(textValue, strlen(textValue));
            }

            TestCodec<TCodec, true>(learn);
        }
        {
            TestCodec<TCodec, true>();
        }

        {
            TVector<TBuffer> learn;
            learn.emplace_back();
            learn.back().Append('a');

            TVector<TBuffer> test;
            test.emplace_back();
            for (ui32 i = 0; i < 256; ++i) {
                test.back().Append((ui8)i);
            }

            TestCodec<TCodec, true>(learn, test);
        }

        {
            TVector<TBuffer> learn;
            learn.emplace_back();
            for (ui32 i = 0; i < 256; ++i) {
                for (ui32 j = 0; j < i; ++j) {
                    learn.back().Append((ui8)i);
                }
            }

            TVector<TBuffer> test;
            test.emplace_back();
            for (ui32 i = 0; i < 256; ++i) {
                test.back().Append((ui8)i);
            }

            TestCodec<TCodec, true>(learn, test);
        }

        {
            TVector<TBuffer> learn;
            learn.emplace_back();
            for (ui32 i = 0; i < 128; ++i) {
                for (ui32 j = 0; j < i; ++j) {
                    learn.back().Append((ui8)i);
                }
            }

            TVector<TBuffer> test;
            test.emplace_back();
            for (ui32 i = 128; i < 256; ++i) {
                test.back().Append((ui8)i);
            }

            TestCodec<TCodec, true>(learn, test);
        }
    }

    void TestHuffman() {
        DoTestSimpleCodec<NCodecs::THuffmanCodec>();
    }

    void TestZStdDict() {
        using namespace NCodecs;
        {
            TVector<TBuffer> learn;

            for (auto& textValue : TextValues) {
                learn.emplace_back(textValue, strlen(textValue));
            }

            TestCodec<TZStdDictCodec, true>(learn);
        }

    }

    void TestCompTable() {
        DoTestSimpleCodec<NCodecs::TCompTableCodec>();
    }

    void TestHuffmanLearnByFreqs() {
        using namespace NCodecs;

        TVector<TBuffer> data;

        for (auto& textValue : TextValues) {
            data.emplace_back(textValue, strlen(textValue));
        }

        TVector<TBuffer> outLearn;

        {
            THuffmanCodec codec;
            static_cast<ICodec&>(codec).Learn(data.begin(), data.end());

            for (ui32 i = 0; i < data.size(); ++i) {
                outLearn.emplace_back();
                codec.Encode(AsStrBuf(data[i]), outLearn[i]);
            }
        }

        TVector<TBuffer> outLearnByFreqs;

        {
            THuffmanCodec codec;
            std::pair<char, ui64> freqs[256];

            for (size_t i = 0; i < Y_ARRAY_SIZE(freqs); ++i) {
                freqs[i].first = (char)i;
                freqs[i].second = 0;
            }

            for (auto& textValue : TextValues) {
                size_t len = strlen(textValue);
                for (size_t j = 0; j < len; ++j) {
                    ++freqs[(ui32)(0xFF & textValue[j])].second;
                }
            }

            codec.LearnByFreqs(TArrayRef<std::pair<char, ui64>>(freqs, Y_ARRAY_SIZE(freqs)));

            for (ui32 i = 0; i < data.size(); ++i) {
                outLearnByFreqs.emplace_back();
                codec.Encode(AsStrBuf(data[i]), outLearnByFreqs[i]);
            }
        }

        UNIT_ASSERT_EQUAL(outLearn.size(), outLearnByFreqs.size());
        const size_t sz = outLearn.size();
        for (size_t n = 0; n < sz; ++n) {
            UNIT_ASSERT_EQUAL(AsStrBuf(outLearn[n]), AsStrBuf(outLearnByFreqs[n]));
        }
    }

    void TestSolar() {
        using namespace NCodecs;
        {
            TVector<TBuffer> learn;

            for (auto& textValue : TextValues) {
                learn.emplace_back(textValue, strlen(textValue));
            }

            TestCodec<TSolarCodec, true>(learn, TVector<TBuffer>(), new TSolarCodec(512, 8));
            TestCodec<TAdaptiveSolarCodec, false>(learn, TVector<TBuffer>(), new TAdaptiveSolarCodec(512, 8));
            TestCodec<TAdaptiveSolarCodec, true>(learn, TVector<TBuffer>(), new TAdaptiveSolarCodec(512, 8));
            TestCodec<TSolarCodecShortInt, true>(learn, TVector<TBuffer>(), new TSolarCodecShortInt(512, 8));
        }
        {
            TestCodec<TSolarCodec, true>(TVector<TBuffer>(), TVector<TBuffer>(), new TSolarCodec(512, 8));
            TestCodec<TAdaptiveSolarCodec, false>(TVector<TBuffer>(), TVector<TBuffer>(), new TAdaptiveSolarCodec(512, 8));
            TestCodec<TAdaptiveSolarCodec, true>(TVector<TBuffer>(), TVector<TBuffer>(), new TAdaptiveSolarCodec(512, 8));
            TestCodec<TSolarCodecShortInt, true>(TVector<TBuffer>(), TVector<TBuffer>(), new TSolarCodecShortInt(512, 8));
        }

        {
            TVector<TBuffer> learn;
            learn.emplace_back();
            learn.back().Append('a');

            TVector<TBuffer> test;
            test.emplace_back();
            for (ui32 i = 0; i < 256; ++i) {
                test.back().Append((ui8)i);
            }

            TestCodec<TSolarCodec, true>(learn, test, new TSolarCodec(512, 8));
            TestCodec<TAdaptiveSolarCodec, false>(learn, test, new TAdaptiveSolarCodec(512, 8));
            TestCodec<TAdaptiveSolarCodec, true>(learn, test, new TAdaptiveSolarCodec(512, 8));
            TestCodec<TSolarCodecShortInt, true>(learn, test, new TSolarCodecShortInt(512, 8));
        }

        {
            TVector<TBuffer> learn;
            learn.emplace_back();
            for (ui32 i = 0; i < 256; ++i) {
                for (ui32 j = 0; j < i; ++j) {
                    learn.back().Append((ui8)i);
                }
            }

            TVector<TBuffer> test;
            test.emplace_back();
            for (ui32 i = 0; i < 256; ++i) {
                test.back().Append((ui8)i);
            }

            TestCodec<TSolarCodec, true>(learn, test, new TSolarCodec(512, 8));
            TestCodec<TAdaptiveSolarCodec, false>(learn, test, new TAdaptiveSolarCodec(512, 8));
            TestCodec<TAdaptiveSolarCodec, true>(learn, test, new TAdaptiveSolarCodec(512, 8));
            TestCodec<TSolarCodecShortInt, true>(learn, test, new TSolarCodecShortInt(512, 8));
        }
    }

    void TestPipeline() {
        using namespace NCodecs;
        {
            TVector<TBuffer> learn;
            learn.emplace_back();
            for (ui32 i = 0; i < 256; ++i) {
                for (i32 j = i; j >= 0; --j) {
                    learn.back().Append((ui8)j);
                }
            }

            TVector<TBuffer> test;
            test.emplace_back();
            for (ui32 i = 0; i < 256; ++i) {
                test.back().Append((ui8)i);
            }

            TestCodec<TPipelineCodec, true>(learn, test,
                                            new TPipelineCodec(new TSolarCodec(512, 8), new TSolarCodec(512, 8), new THuffmanCodec));
        }
        {
            TVector<TBuffer> d;
            d.emplace_back();
            for (ui32 i = 0; i < 256; ++i) {
                for (i32 j = i; j >= 0; --j) {
                    d.back().Append(i * i);
                }
            }

            TestCodec<TPipelineCodec, false>(d, TVector<TBuffer>(),
                                             new TPipelineCodec(new TDeltaCodec<ui32, false>, new TPForCodec<ui32>));
        }
    }

    void TestRegistry() {
        using namespace NCodecs;
        TVector<TString> vs = ICodec::GetCodecsList();
        for (const auto& v : vs) {
            TCodecPtr p = ICodec::GetInstance(v);
            if (v == "none") {
                UNIT_ASSERT(!p);
                continue;
            }
            UNIT_ASSERT_C(!!p, v);
            UNIT_ASSERT_C(TStringBuf(v).Head(3) == TStringBuf(p->GetName()).Head(3), v + " " + p->GetName());
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TCodecsTest)
