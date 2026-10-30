/* Pango Language Sample Table
 *
 * Each entry is of the form:
 *
 * LANGUAGE(
 * 	 code	|* Name *|,
 * 	 SOURCE,
 * 	 "Sample text for the language."
 * 	 |* Translation of the sample text to English *|
 * 	)
 *
 * Where code is the ISO639-1, ISO639-2, or ISO639-3 code for the language,
 * the first one that exists.  Name is the name of the language in English.
 *
 * Source is where the sample text comes from.  One of:
 *
 * WP-PANG
 * 	Wikipedia's List of Pangrams in Other Languages
 * 	http://en.wikipedia.org/wiki/List_of_pangrams#Other_languages
 * 	Fetched on 2008-08-19
 *
 * WP-SFD
 * 	Wikipedia's Sample Font Displays in Other Languages
 * 	http://en.wikipedia.org/wiki/Sample_Font_Displays_In_Other_Languages
 * 	Fetched on 2008-08-19
 *
 * WP
 *      Wikipedia, Article about the language
 *      Fetched on 2020-09-08
 *
 * GLASS
 * 	Kermit project's "I Can Eat Glass" list, also available in pango-view/
 * 	http://www.columbia.edu/kermit/utf8.html#glass
 * 	Fetched on 2008-08-19, updates on 2020-09-08
 *
 * KERMIT
 * 	Kermit project's Quick-Brown-Fox equivalents for other languages
 * 	http://www.columbia.edu/kermit/utf8.html#quickbrownfox
 * 	Fetched on 2008-08-19
 *
 * GSPECI
 * 	gnome-specimen's translations
 * 	http://svn.gnome.org/viewvc/gnome-specimen/trunk/po/
 * 	Fetched on 2008-08-19
 *
 * MISC
 * 	Miscellaneous
 *
 * The sample text may be a pangram, but is not necessarily.  It is chosen to
 * be demonstrative of normal text in the language, as well as exposing font
 * feature requirements unique to the language.  It should be suitable for use
 * as sample text in a font selection dialog.
 *
 * Needless to say, the list MUST be sorted on the language code.
 */
/* Sacrificial define to make introspection happy. */
#ifndef LANGUAGE
#define LANGUAGE(x, y, z)
#endif
LANGUAGE(
	 af	/* Afrikaans */,
	 GLASS,
	 "Ek kan glas eet, maar dit doen my nie skade nie."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 ar	/* Arabic */,
	 WP-PANG,
	 "نص حكيم له سر قاطع وذو شأن عظيم مكتوب على ثوب أخضر ومغلف بجلد أزرق."
	 /* A wise text which has an absolute secret and great importance, written on a green tissue and covered with blue leather. */
	)
LANGUAGE(
	 arn	/* Mapudungun */,
	 WP-PANG,
	 "Gvxam mincetu apocikvyeh: ñizol ce mamvj ka raq kuse bafkeh mew."
	 /* Tale under the full moon: the chief chemamull and the clay old woman at the lake/sea. */
	)
LANGUAGE(
	 as	/* Assamese */,
	 WP,
	 "সুপ্ৰভাত আপোনাক লগ পাই ভাল লাগিল।"
	 /* Good morning, nice to meet you. */
	)
LANGUAGE(
	 bar	/* Bavarian */,
	 GLASS,
	 "I koh Glos esa, und es duard ma ned wei."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 bg	/* Bulgarian */,
	 WP-SFD,
	 "Под южно дърво, цъфтящо в синьо, бягаше малко пухкаво зайче."
	 /* A little fluffy young rabbit ran under a southern tree blooming in blue */
	)
LANGUAGE(
	 bi	/* Bislama */,
	 GLASS,
	 "Mi save kakae glas, hemi no save katem mi."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 bn	/* Bengali */,
	 GLASS,
	 "আমি কাঁচ খেতে পারি, তাতে আমার কোনো ক্ষতি হয় না।"
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 bo	/* Tibetan */,
	 GLASS,
	 "ཤེལ་སྒོ་ཟ་ནས་ང་ན་གི་མ་རེད།"
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 bs	/* Bosnian */,
	 WP-PANG,
	 "Fin džip, gluh jež i čvrst konjić dođoše bez moljca."
	 /* A nice jeep, a deaf hedgehog and a tough horse came without a moth. */
	)
LANGUAGE(
	 ca	/* Catalan */,
	 WP-PANG,
	 "Jove xef, porti whisky amb quinze glaçons d'hidrogen, coi!"
	 /* Young chef, bring whisky with fifteen hydrogen ice cubes, damn! */
	)
LANGUAGE(
	 ch	/* Chamorro */,
	 GLASS,
	 "Siña yo' chumocho krestat, ti ha na'lalamen yo'."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 cs	/* Czech */,
	 WP-SFD,
	 "Příliš žluťoučký kůň úpěl ďábelské ódy."
	 /* A too yellow horse moaned devil odes. */
	)
LANGUAGE(
	 cy	/* Welsh */,
	 GLASS,
	 "Dw i'n gallu bwyta gwydr, 'dyw e ddim yn gwneud dolur i mi."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 da	/* Danish */,
	 WP-SFD,
	 "Quizdeltagerne spiste jordbær med fløde, mens cirkusklovnen Walther spillede på xylofon."
	 /* The quiz contestants ate strawberries with cream while Walther the clown was playing the xylophone. */
	)
LANGUAGE(
	 de	/* German */,
	 WP-SFD,
	 "Zwölf Boxkämpfer jagen Viktor quer über den großen Sylter Deich."
	 /* Twelve boxing fighters drive Viktor over the great. */
	)
LANGUAGE(
	 doi	/* Dogri */,
	 WP,
	 "अ'ऊं बमार आँ, मिगी डॉक्टर दी लोड़ ऐ।"
	 /* I'm sick, I need a doctor. */
	)
LANGUAGE(
         dv     /* Maldivian */,
         WP,
         "މާއްދާ 1 – ހުރިހާ އިންސާނުން ވެސް އުފަންވަނީ، ދަރަޖަ އާއި ޙައްޤު ތަކުގައި މިނިވަންކަމާއި ހަމަހަމަކަން ލިބިގެންވާ ބައެއްގެ ގޮތުގައެވެ."
         /* Beginning of UDHR */
        )
LANGUAGE(
	 el	/* Greek */,
	 WP-SFD,
	 "Θέλει αρετή και τόλμη η ελευθερία. (Ανδρέας Κάλβος)"
	 /* Liberty requires virtue and mettle. (Andreas Kalvos) */
	)
LANGUAGE(
	 en	/* English */,
	 GSPECI,
	 "The quick brown fox jumps over the lazy dog."
	)
LANGUAGE(
	 enm	/* Middle English */,
	 GLASS,
	 "Ich canne glas eten and hit hirtiþ me nouȝt."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 eo	/* Esperanto */,
	 WP-SFD,
	 "Eĥoŝanĝo ĉiuĵaŭde."
	 /* Change of echo every Thursday. */
	)
LANGUAGE(
	 es	/* Spanish */,
	 WP-PANG,
	 "Jovencillo emponzoñado de whisky: ¡qué figurota exhibe!"
	 /* Whisky-intoxicated youngster — what a figure he's showing! */
	)
LANGUAGE(
	 et	/* Estonian */,
	 WP-SFD,
	 "See väike mölder jõuab rongile hüpata."
	 /* This small miller is able to jump on the train. */
	)
LANGUAGE(
	 eu	/* Basque */,
	 GLASS,
	 "Kristala jan dezaket, ez dit minik ematen."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 fa	/* Persian */,
	 MISC	/* Behdad Esfahbod (#548730) */,
	 "«الا یا اَیُّها السّاقی! اَدِرْ کَأساً وَ ناوِلْها!» که عشق آسان نمود اوّل، ولی افتاد مشکل‌ها!"
	)
LANGUAGE(
	 fi	/* Finnish */,
	 WP-SFD,
	 "Viekas kettu punaturkki laiskan koiran takaa kurkki."
	 /* The cunning red-coated fox peeped from behind the lazy dog. */
	)
LANGUAGE(
	 fr	/* French */,
	 MISC	/* Vincent Untz (#549520) http://fr.wikipedia.org/wiki/Pangramme */,
	 "Voix ambiguë d'un cœur qui, au zéphyr, préfère les jattes de kiwis."
	 /* Ambiguous voice of a heart that, in the wind, prefers bowls of kiwis. */
	)
LANGUAGE(
	 fro	/* Old French */,
	 GLASS,
	 "Je puis mangier del voirre. Ne me nuit."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 ga	/* Irish */,
	 WP-PANG,
	 "Chuaigh bé mhórshách le dlúthspád fíorfhinn trí hata mo dhea-phorcáin bhig."
	 /* A maiden of large appetite with an intensely white, dense spade went through the hat of my good little porker. */
	)
LANGUAGE(
	 gd	/* Scottish Gaelic */,
	 GLASS,
	 "S urrainn dhomh gloinne ithe; cha ghoirtich i mi."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 gl	/* Galician */,
	 GLASS,
	 "Eu podo xantar cristais e non cortarme."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 got	/* Gothic */,
	 GLASS,
	 "𐌼𐌰𐌲 𐌲𐌻𐌴𐍃 𐌹̈𐍄𐌰𐌽, 𐌽𐌹 𐌼𐌹𐍃 𐍅𐌿 𐌽𐌳𐌰𐌽 𐌱𐍂𐌹𐌲𐌲𐌹𐌸."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 gu	/* Gujarati */,
	 GLASS,
	 "હું કાચ ખાઇ શકુ છુ અને તેનાથી મને દર્દ નથી થતુ."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 gv	/* Manx Gaelic */,
	 GLASS,
	 "Foddym gee glonney agh cha jean eh gortaghey mee."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 haw	/* Hawaiian */,
	 GLASS,
	 "Hiki iaʻu ke ʻai i ke aniani; ʻaʻole nō lā au e ʻeha."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 he	/* Hebrew */,
	 WP-SFD,
	 "דג סקרן שט לו בים זך אך לפתע פגש חבורה נחמדה שצצה כך."
	 /* A curious fish sailed a clear sea, and suddenly found nice company that just popped up. */
	)
LANGUAGE(
	 hi	/* Hindi */,
	 MISC	/* G Karunakar (#549532) */,
	 "नहीं नजर किसी की बुरी नहीं किसी का मुँह काला जो करे सो उपर वाला"
	 /* its not in the sight or the face, but its all in god's grace. */
	)
LANGUAGE(
	 hr	/* Croatian */,
	 MISC,
	 "Deblji krojač: zgužvah smeđ filc u tanjušni džepić."
	 /* A fatter taylor: I’ve crumpled a brown felt in a slim pocket. */
	)
LANGUAGE(
	 hu	/* Hungarian */,
	 WP-SFD,
	 "Egy hűtlen vejét fülöncsípő, dühös mexikói úr Wesselényinél mázol Quitóban."
	 /* An angry Mexican man, who caught his faithless son-in-law, is painting Wesselényi's house in Quito. */
	)
LANGUAGE(
	 hy	/* Armenian */,
	 GLASS,
	 "Կրնամ ապակի ուտել և ինծի անհանգիստ չըներ։"
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 is	/* Icelandic */,
	 WP-PANG,
	 "Kæmi ný öxi hér ykist þjófum nú bæði víl og ádrepa"
	 /* If a new axe were here, thieves would feel increasing deterrence and punishment. */
	)
LANGUAGE(
	 it	/* Italian */,
	 WP-SFD,
	 "Ma la volpe, col suo balzo, ha raggiunto il quieto Fido."
	 /* But the fox, with its jump, reached the calm dog */
	)
LANGUAGE(
	 ja	/* Japanese */,
	 KERMIT,
	 "いろはにほへと ちりぬるを 色は匂へど 散りぬるを"
	)
LANGUAGE(
	 jam	/* Jamaican Creole English */,
	 KERMIT,
	 "Chruu, a kwik di kwik brong fox a jomp huova di liezi daag de, yu no siit?"
	)
LANGUAGE(
	 jbo	/* Lojban */,
	 WP-PANG,
	 ".o'i mu xagji sofybakni cu zvati le purdi"
	 /* Watch out, five hungry Soviet-cows are in the garden! */
	)
LANGUAGE(
	 jv	/* Javanese */,
	 GLASS,
	 "Aku isa mangan beling tanpa lara."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 ka	/* Georgian */,
	 GLASS,
	 "მინას ვჭამ და არა მტკივა."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
         km     /* Khmer */,
         GLASS,
         "ខ្ញុំអាចញុំកញ្ចក់បាន ដោយគ្មានបញ្ហារ"
	 /* I can eat glass and it doesn't hurt me. */
        )
LANGUAGE(
	 kn	/* Kannada */,
	 GLASS,
	 "ನಾನು ಗಾಜನ್ನು ತಿನ್ನಬಲ್ಲೆ ಮತ್ತು ಅದರಿಂದ ನನಗೆ ನೋವಾಗುವುದಿಲ್ಲ."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 ko	/* Korean */,
	 WP-SFD,
	 "다람쥐 헌 쳇바퀴에 타고파"
	 /* I Wanna ride on the chipmunk's old hamster wheel. */
	)
LANGUAGE(
	 ks	/* Kashmiri */,
	 WP,
	 "ब’ छस न’ ज़ानान, तोहय हेकिवा दुबार’ वॅनिथ ।"
	 /* I don't understand, Please say that again. */
	)
LANGUAGE(
	 kw	/* Cornish */,
	 GLASS,
	 "Mý a yl dybry gwéder hag éf ny wra ow ankenya."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 la	/* Latin */,
	 WP-PANG,
	 "Sic surgens, dux, zelotypos quam karus haberis"
	)
LANGUAGE(
         lo     /* Lao */,
         GLASS,
         "ຂອ້ຍກິນແກ້ວໄດ້ໂດຍທີ່ມັນບໍ່ໄດ້ເຮັດໃຫ້ຂອ້ຍເຈັບ"
	 /* I can eat glass and it doesn't hurt me. */
        )
LANGUAGE(
	 lt	/* Lithuanian */,
	 WP-PANG,
	 "Įlinkdama fechtuotojo špaga sublykčiojusi pragręžė apvalų arbūzą."
	 /* Incurving fencer sword sparkled and perforated a round watermelon. */
	)
LANGUAGE(
	 lv	/* Latvian */,
	 WP-SFD,
	 "Sarkanās jūrascūciņas peld pa jūru."
	 /* Red seapigs swim in the sea. */
	)
LANGUAGE(
	 mai	/* Maithili */,
	 WP,
	 "हम अहाँ के बिना कुछ नै छिय"
	 /* I am nothing without you */
	)
LANGUAGE(
	 map	/* Marquesan */,
	 GLASS,
	 "E koʻana e kai i te karahi, mea ʻā, ʻaʻe hauhau."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 mk	/* Macedonian */,
	 GLASS,
	 "Можам да јадам стакло, а не ме штета."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 ml	/* Malayalam */,
	 GLASS,
	 "വേദനയില്ലാതെ കുപ്പിചില്ലു് എനിയ്ക്കു് കഴിയ്ക്കാം."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 mn	/* Mongolian */,
	 GLASS,
	 "ᠪᠢ ᠰᠢᠯᠢ ᠢᠳᠡᠶᠦ ᠴᠢᠳᠠᠨᠠ ᠂ ᠨᠠᠳᠤᠷ ᠬᠣᠤᠷᠠᠳᠠᠢ ᠪᠢᠰᠢ"
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 mr	/* Marathi */,
	 GLASS,
	 "मी काच खाऊ शकतो, मला ते दुखत नाही."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 ms	/* Malay */,
	 GLASS,
	 "Saya boleh makan kaca dan ia tidak mencederakan saya."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
         my     /* Burmese */,
         WP,
         "ဘာသာပြန်နှင့် စာပေပြုစုရေး ကော်မရှင်"
         /* Literary and Translation Commission */
        )
LANGUAGE(
	 nap	/* Neapolitan */,
	 GLASS,
	 "M' pozz magna' o'vetr, e nun m' fa mal."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 nb	/* Norwegian Bokmål */,
	 GSPECI,
	 "Vår sære Zulu fra badeøya spilte jo whist og quickstep i min taxi."
	)
LANGUAGE(
	 ne	/* Nepali */,
	 WP,
	 "के तपाईँ मसँग नाच्न चाहनुहुन्छ ?"
	 /* Would you like to dance with me? */
	)
LANGUAGE(
	 nl	/* Dutch */,
	 WP-SFD,
	 "Pa's wijze lynx bezag vroom het fikse aquaduct."
	 /* Dad's wise lynx piously regarded the substantial aqueduct. */
	)
LANGUAGE(
	 nn	/* Norwegian Nynorsk */,
	 GLASS,
	 "Eg kan eta glas utan å skada meg."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 no	/* Norwegian Bokmål */,
	 GSPECI,
	 "Vår sære Zulu fra badeøya spilte jo whist og quickstep i min taxi."
	)
LANGUAGE(
	 nv	/* Navajo */,
	 GLASS,
	 "Tsésǫʼ yishą́ągo bííníshghah dóó doo shił neezgai da."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 oc	/* Occitan */,
	 GLASS,
	 "Pòdi manjar de veire, me nafrariá pas."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 or	/* Oriya */,
	 GLASS,
	 "ମୁଁ କାଚ ଖାଇପାରେ ଏବଂ ତାହା ମୋର କ୍ଷତି କରିନଥାଏ।."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 pa	/* Punjabi */,
	 GLASS,
	 "ਮੈਂ ਗਲਾਸ ਖਾ ਸਕਦਾ ਹਾਂ ਅਤੇ ਇਸ ਨਾਲ ਮੈਨੂੰ ਕੋਈ ਤਕਲੀਫ ਨਹੀਂ."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 pcd	/* Picard */,
	 GLASS,
	 "Ch'peux mingi du verre, cha m'foé mie n'ma."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 pl	/* Polish */,
	 WP-SFD,
	 "Pchnąć w tę łódź jeża lub ośm skrzyń fig."
	 /* Push into this boat a hedgehog or eight boxes of figs. */
	)
LANGUAGE(
	 pt	/* Portuguese */,
	 WP-SFD,
	 "Vejam a bruxa da raposa Salta-Pocinhas e o cão feliz que dorme regalado."
	 /* Watch the witch of the Jump-Puddles fox and the happy dog that sleeps delighted. */
	)
LANGUAGE(
	 pt-br	/* Brazilian Portuguese */,
	 WP-PANG,
	 "À noite, vovô Kowalsky vê o ímã cair no pé do pingüim queixoso e vovó põe açúcar no chá de tâmaras do jabuti feliz."
	 /* At night, grandpa Kowalsky sees the magnet falling in the complaining penguin's foot and grandma puts sugar in the happy tortoise's date tea.*/
	)
LANGUAGE(
	 ro	/* Romanian */,
	 MISC	/* Misu Moldovan (#552993) */,
	 "Fumegând hipnotic sașiul azvârle mreje în bălți."
	 /* Hypnotically smoking, the cross-eyed man throws fishing nets into ponds. */
	)
LANGUAGE(
	 ru	/* Russian */,
	 WP-PANG,
	 "В чащах юга жил бы цитрус? Да, но фальшивый экземпляр!"
	 /* Would a citrus live in the bushes of the south? Yes, but only a fake one! */
	)
LANGUAGE(
	 sa	/* Sanskrit */,
	 GLASS,
	 "काचं शक्नोम्यत्तुम् । नोपहिनस्ति माम् ॥"
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 scn	/* Sicilian */,
	 GLASS,
	 "Puotsu mangiari u vitru, nun mi fa mali."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 sd	/* Sindhi */,
	 WP,
	 "توهان ملي ڏاڍو سٺو لڳو"
	 /* Pleased to meet you */
	)
LANGUAGE(
         si     /* Sinhalese */,
         WP,
         "මනොපුබ්‌බඞ්‌ගමා ධම්‌මා, මනොසෙට්‌ඨා මනොමයා; මනසා චෙ පදුට්‌ඨෙන, භාසති වා කරොති වා; තතො නං දුක්‌ඛමන්‌වෙති, චක්‌කංව වහතො පදං."
        )
LANGUAGE(
	 sk	/* Slovak */,
	 KERMIT,
	 "Starý kôň na hŕbe kníh žuje tíško povädnuté ruže, na stĺpe sa ďateľ učí kvákať novú ódu o živote."
	)
LANGUAGE(
	 sl	/* Slovenian */,
	 WP-PANG,
	 "Šerif bo za vajo spet kuhal domače žgance."
	 /* For an exercise, sheriff will again make home-made mush. */
	)
LANGUAGE(
	 sq	/* Albanian */,
	 GLASS,
	 "Unë mund të ha qelq dhe nuk më gjen gjë."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 sr	/* Serbian (Cyrillic) */,
	 WP-SFD,
	 "Чешће цeђење мрeжастим џаком побољшава фертилизацију генских хибрида."
	 /* More frequent filtering through the reticular bag improves fertilization of genetic hybrids. */
	)
#if 0
LANGUAGE(
	 sr-sr@latin	/* Serbian (Latin) */,
	 WP-SFD,
	 "Češće ceđenje mrežastim džakom poboljšava fertilizaciju genskih hibrida."
	 /* More frequent filtering through the reticular bag improves fertilization of genetic hybrids. */
	)
#endif
LANGUAGE(
	 sv	/* Swedish */,
	 WP-SFD,
	 "Flygande bäckasiner söka strax hwila på mjuka tuvor."
	 /* Flying snipes soon look to rest on soft grass beds. */
	)
LANGUAGE(
	 swg	/* Swabian */,
	 GLASS,
	 "I kå Glas frässa, ond des macht mr nix!"
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 ta	/* Tamil */,
	 GLASS,
	 "நான் கண்ணாடி சாப்பிடுவேன், அதனால் எனக்கு ஒரு கேடும் வராது."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 te	/* Telugu */,
	 GLASS,
	 "నేను గాజు తినగలను అయినా నాకు యేమీ కాదు."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 th	/* Thai */,
	 WP-SFD,
	 "เป็นมนุษย์สุดประเสริฐเลิศคุณค่า - กว่าบรรดาฝูงสัตว์เดรัจฉาน - จงฝ่าฟันพัฒนาวิชาการ อย่าล้างผลาญฤๅเข่นฆ่าบีฑาใคร - ไม่ถือโทษโกรธแช่งซัดฮึดฮัดด่า - หัดอภัยเหมือนกีฬาอัชฌาสัย - ปฏิบัติประพฤติกฎกำหนดใจ - พูดจาให้จ๊ะ ๆ จ๋า ๆ น่าฟังเอยฯ"
	 /* Being a man is worthy - Beyond senseless animal - Begin educate thyself - Begone from killing and trouble - Bear not thy grudge, damn and, curse - Bestow forgiving and sporting - Befit with rules - Benign speech speak thou */
	)
LANGUAGE(
	 tl	/* Tagalog */,
	 GLASS,
	 "Kaya kong kumain nang bubog at hindi ako masaktan."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 tr	/* Turkish */,
	 WP-PANG,
	 "Pijamalı hasta yağız şoföre çabucak güvendi."
	 /* The patient in pajamas trusted the swarthy driver quickly. */
	)
LANGUAGE(
	 tw	/* Twi */,
	 GLASS,
	 "Metumi awe tumpan, ɜnyɜ me hwee."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 uk	/* Ukrainian */,
	 WP-PANG,
	 "Чуєш їх, доцю, га? Кумедна ж ти, прощайся без ґольфів!"
	 /* Daughter, do you hear them, eh? Oh, you are funny! Say good-bye without knee-length socks. */
	)
LANGUAGE(
	 ur	/* Urdu */,
	 GLASS,
	 "میں کانچ کھا سکتا ہوں اور مجھے تکلیف نہیں ہوتی ۔"
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 vec	/* Venetian */,
	 GLASS,
	 "Mi posso magnare el vetro, no'l me fa mae."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 vi	/* Vietnamese */,
	 GSPECI,
	 "Con sói nâu nhảy qua con chó lười."
	)
LANGUAGE(
	 wa	/* Walloon */,
	 GLASS,
	 "Dji pou magnî do vêre, çoula m' freut nén må."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 yi	/* Yiddish */,
	 GLASS,
	 "איך קען עסן גלאָז און עס טוט מיר נישט װײ."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 yo	/* Yoruba */,
	 GLASS,
	 "Mo lè je̩ dígí, kò ní pa mí lára."
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 zh-cn	/* Chinese Simplified */,
	 GLASS,
	 "我能吞下玻璃而不伤身体。"
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 zh-mo	/* Chinese Traditional */,
	 GLASS,
	 "我能吞下玻璃而不傷身體。"
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 zh-sg	/* Chinese Simplified */,
	 GLASS,
	 "我能吞下玻璃而不伤身体。"
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 zh-tw	/* Chinese Traditional */,
	 GLASS,
	 "我能吞下玻璃而不傷身體。"
	 /* I can eat glass and it doesn't hurt me. */
	)
LANGUAGE(
	 zlm	/* Malay */,
	 GLASS,
	 "Saya boleh makan kaca dan ia tidak mencederakan saya."
	 /* I can eat glass and it doesn't hurt me. */
	)
