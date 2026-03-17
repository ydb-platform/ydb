"""A Catalog of 115 Bright Stars

Data sources:

Hipparcos Catalog
https://cdsarc.u-strasbg.fr/ftp/cats/I/239/hip_main.dat

IAU Catalog of Star Names (IAU-CSN)
http://www.pas.rochester.edu/~emamajek/WGSN/IAU-CSN.txt

Proper motion has been applied to the Hipparcos catalog's 1991.25 epoch
positions to generate the 2000.0 positions that PyEphem needs, using the
high-precision Skyfield astronomy library for Python.  See the script
`bin/rebuild-star-data` in the PyEphem project repository for the
details of how the star positions are generated and formatted.  It also
includes variant names for several stars that you might want to consult:

https://github.com/brandon-rhodes/pyephem/blob/master/bin/rebuild-star-data

The 57 navigational stars are drawn from:

Her Majesty's Nautical Almanac Office Publications ('Almanac'):
    NP314-18 "The Nautical Almanac" (2017).
    DP330 "NavPac and Compact Data" (2015).
    NP303(AP3270) "Rapid Sight Reduction Tables for Navigation" (2012).

"""

db = """\
Acamar,f|S|A4,2.97102074|-53.53,-40.30467239|25.71,2.88
Achernar,f|S|B3,1.62856849|88.02,-57.23675744|-40.08,0.45
Acrux,f|S|B0,12.44330439|-35.37,-63.09909168|-14.73,0.77
Adara,f|S|B2,6.97709679|2.63,-28.97208374|2.29,1.5
Adhara,f|S|B2,6.97709679|2.63,-28.97208374|2.29,1.5
Agena,f|S|B1,14.06372347|-33.96,-60.37303932|-25.06,0.61
Albereo,f|S|K3,19.51202239|-7.09,27.95968112|-5.63,3.05
Albireo,f|S|K3,19.51202239|-7.09,27.95968112|-5.63,3.05
Alcaid,f|S|B3,13.79234379|-121.23,49.31326512|-15.56,1.85
Alcor,f|S|A5,13.42042721|120.35,54.98795774|-16.94,3.99
Alcyone,f|S|B7,3.79141014|19.35,24.10513714|-43.11,2.85
Aldebaran,f|S|K5,4.59867740|62.78,16.50930138|-189.36,0.87
Alderamin,f|S|A7,21.30965876|149.91,62.58557256|48.27,2.45
Alfirk,f|S|B2,21.47766587|12.6,70.56071602|8.73,3.23
Algenib,f|S|B2,0.22059801|4.7,15.18359590|-8.24,2.83
Algieba,f|S|K0,10.33287623|310.77,19.84148875|-152.88,2.01
Algol,f|S|B8,3.13614765|2.39,40.95564766|-1.44,2.09
Alhena,f|S|A0,6.62852808|-2.04,16.39925217|-66.92,1.93
Alioth,f|S|A0,12.90048595|111.74,55.95982123|-8.99,1.76
Alkaid,f|S|B3,13.79234379|-121.23,49.31326512|-15.56,1.85
Almach,f|S|B8,2.06498696|43.08,42.32972472|-50.85,2.1
Alnair,f|S|B7,22.13721819|127.6,-46.96097539|-147.91,1.73
Alnilam,f|S|B0,5.60355929|1.49,-1.20191983|-1.06,1.69
Alnitak,f|S|O9,5.67931309|3.99,-1.94257224|2.54,1.74
Alphard,f|S|K3,9.45978980|-14.49,-8.65860253|33.25,1.99
Alphecca,f|S|A0,15.57813004|120.38,26.71469307|-89.44,2.22
Alpheratz,f|S|B9,0.13979405|135.68,29.09043197|-162.95,2.07
Alshain,f|S|G8,19.92188706|46.35,6.40676348|-481.32,3.71
Altair,f|S|A7,19.84638864|536.82,8.86832203|385.54,0.76
Ankaa,f|S|K0,0.43806972|232.76,-42.30598144|-353.64,2.4
Antares,f|S|M1,16.49012803|-10.16,-26.43200250|-23.21,1.06
Arcturus,f|S|K2,14.26102001|-1093.45,19.18241038|-1999.4,-0.05
Arkab Posterior,f|S|F2,19.38698247|92.78,-44.79977847|-53.73,4.27
Arkab Prior,f|S|B9,19.37730347|7.31,-44.45896465|-22.43,3.96
Arneb,f|S|F0,5.54550442|3.27,-17.82228853|1.54,2.58
Atlas,f|S|B8,3.81937293|17.77,24.05341547|-44.7,3.62
Atria,f|S|K2,16.81108191|17.85,-69.02771505|-32.92,1.91
Avior,f|S|K3,8.37523211|-25.34,-59.50948307|22.72,1.86
Bellatrix,f|S|B2,5.41885085|-8.75,6.34970223|-13.28,1.64
Betelgeuse,f|S|M2,5.91952924|27.33,7.40706274|10.86,0.45
Canopus,f|S|F0,6.39919718|19.99,-52.69566045|23.67,-0.62
Capella,f|S|M1,5.27815528|75.52,45.99799106|-427.13,0.08
Caph,f|S|F2,0.15296808|523.39,59.14977950|-180.42,2.28
Castor,f|S|A2,7.57662855|-206.33,31.88827631|-148.18,1.58
Cebalrai,f|S|K2,17.72454254|-40.67,4.56730283|158.8,2.76
Deneb,f|S|A2,20.69053187|1.56,45.28033800|1.55,1.25
Denebola,f|S|A3,11.81766043|-499.02,14.57206038|-113.78,2.14
Diphda,f|S|K0,0.72649196|232.79,-17.98660457|32.71,2.04
Dubhe,f|S|F7,11.06213019|-136.46,61.75103324|-35.25,1.81
Electra,f|S|B6,3.74792703|21.55,24.11333922|-44.92,3.72
Elnath,f|S|B7,5.43819816|23.28,28.60745000|-174.22,1.65
Eltanin,f|S|K5,17.94343608|-8.52,51.48889500|-23.05,2.24
Enif,f|S|K2,21.73643281|30.02,9.87501126|1.38,2.38
Etamin,f|S|K5,17.94343608|-8.52,51.48889500|-23.05,2.24
Fomalhaut,f|S|A3,22.96084626|329.22,-29.62223601|-164.22,1.17
Formalhaut,f|S|A3,22.96084626|329.22,-29.62223601|-164.22,1.17
Gacrux,f|S|M4,12.51943314|27.94,-57.11321175|-264.33,1.59
Gienah Corvi,f|S|B8,12.26343617|-159.58,-17.54192948|22.31,2.58
Gienah,f|S|B8,12.26343617|-159.58,-17.54192948|22.31,2.58
Hadar,f|S|B1,14.06372347|-33.96,-60.37303932|-25.06,0.61
Hamal,f|S|K2,2.11955753|190.73,23.46242310|-145.77,2.01
Izar,f|S|A0,14.74978270|-50.65,27.07422246|20.0,2.35
Kaus Australis,f|S|B9,18.40286620|-39.61,-34.38461611|-124.05,1.79
Kochab,f|S|K4,14.84509068|-32.29,74.15550496|11.91,2.07
Maia,f|S|B8,3.76377962|21.09,24.36774851|-45.03,3.87
Markab,f|S|B9,23.07934827|61.1,15.20526441|-42.56,2.49
Megrez,f|S|A3,12.25710003|103.56,57.03261698|7.81,3.32
Menkalinan,f|S|A2,5.99214525|-56.41,44.94743277|-0.88,1.9
Menkar,f|S|M2,3.03799227|-11.81,4.08973396|-78.76,2.54
Menkent,f|S|K0,14.11137457|-519.29,-36.36995451|-517.87,2.06
Merak,f|S|A1,11.03068799|81.66,56.38242685|33.74,2.34
Merope,f|S|B6,3.77210384|21.17,23.94835835|-42.67,4.14
Miaplacidus,f|S|A2,9.21999318|-157.66,-69.71720776|108.91,1.67
Mimosa,f|S|B0,12.79535087|-48.24,-59.68876364|-12.82,1.25
Minkar,f|S|K2,12.16874463|-71.52,-22.61976647|10.55,3.02
Mintaka,f|S|O9,5.53344464|1.67,-0.29909204|0.56,2.25
Mirach,f|S|M0,1.16220100|175.59,35.62055768|-112.23,2.07
Mirfak,f|S|F5,3.40538065|24.11,49.86117958|-26.01,1.79
Mirzam,f|S|B1,6.37832924|-3.45,-17.95591772|-0.47,1.98
Mizar,f|S|A2,13.39876192|121.23,54.92536183|-22.01,2.23
Naos,f|S|O5,8.05973519|-30.82,-40.00314770|16.77,2.21
Nihal,f|S|G5,5.47075644|-5.03,-20.75944096|-85.92,2.81
Nunki,f|S|B2,18.92109048|13.87,-26.29672225|-52.65,2.05
Peacock,f|S|B2,20.42746051|7.71,-56.73509009|-86.15,1.94
Phecda,f|S|A0,11.89717984|107.76,53.69476015|11.16,2.41
Polaris,f|S|F7,2.53030100|44.22,89.26410949|-11.74,1.97
Pollux,f|S|K0,7.75526397|-625.69,28.02619865|-45.95,1.16
Procyon,f|S|F5,7.65503283|-716.57,5.22499314|-1034.58,0.4
Rasalgethi,f|S|M5,17.24412734|-6.71,14.39033282|32.78,2.78
Rasalhague,f|S|A5,17.58224183|110.08,12.56003481|-222.61,2.08
Regulus,f|S|B7,10.13953074|-249.4,11.96720709|4.91,1.36
Rigel,f|S|B8,5.24229787|1.87,-8.20164055|-0.56,0.18
Rigil Kentaurus,f|S|G2,14.66013779|-3678.19,-60.83397588|481.84,-0.01
Rukbat,f|S|B8,19.39810458|32.67,-40.61593992|-120.81,3.96
Sabik,f|S|A2,17.17296871|41.16,-15.72491023|97.65,2.43
Sadalmelik,f|S|G2,22.09639881|17.9,-0.31985069|-9.93,2.95
Sadr,f|S|F8,20.37047275|2.43,40.25667924|-0.93,2.23
Saiph,f|S|B0,5.79594135|1.55,-9.66960477|-1.2,2.07
Scheat,f|S|M2,23.06290487|187.76,28.08278908|137.61,2.44
Schedar,f|S|K0,0.67512237|50.36,56.53733107|-32.17,2.24
Shaula,f|S|B1,17.56014444|-8.9,-37.10382115|-29.95,1.62
Sheliak,f|S|A8,18.83466519|1.1,33.36266704|-4.46,3.52
Sirius,f|S|A0,6.75247697|-546.01,-16.71611569|-1223.08,-1.44
Sirrah,f|S|B9,0.13979405|135.68,29.09043197|-162.95,2.07
Spica,f|S|B1,13.41988313|-42.5,-11.16132203|-31.73,0.98
Suhail,f|S|K4,9.13326624|-23.21,-43.43258935|14.28,2.23
Sulafat,f|S|B9,18.98239518|-2.76,32.68955742|1.77,3.25
Tarazed,f|S|K3,19.77099430|15.72,10.61326121|-3.08,2.72
Taygeta,f|S|B6,3.75347069|19.35,24.46727760|-41.63,4.3
Thuban,f|S|A0,14.07315271|-56.52,64.37585053|17.19,3.67
Unukalhai,f|S|K2,15.73779857|134.66,6.42562701|44.14,2.63
Vega,f|S|A0,18.61564903|201.02,38.78369185|287.46,0.03
Vindemiatrix,f|S|G8,13.03627697|-275.05,10.95915039|19.96,2.85
Wezen,f|S|F8,7.13985674|-2.75,-26.39319967|3.33,1.83
Zaurak,f|S|M1,3.96715732|60.51,-13.50851532|-111.34,2.97
Zubenelgenubi,f|S|A3,14.84797587|-105.69,-16.04177819|-69.0,2.75
"""

stars = {}

def build_stars():
    from ephem import readdb
    for line in db.splitlines():
        star = readdb(line)
        stars[star.name] = star

build_stars()
del build_stars

def star(name, *args, **kwargs):
    star = stars[name].copy()
    if args or kwargs:
        star.compute(*args, **kwargs)
    return star

# The 57 navigational stars by number
STAR_NUMBER_NAME = {
    7 : "Acamar",
    5 : "Achernar",
    30 : "Acrux",
    19 : "Adhara",
    10 : "Aldebaran",
    32 : "Alioth",
    34 : "Alkaid",
    55 : "Alnair",
    15 : "Alnilam",
    25 : "Alphard",
    41 : "Alphecca",
    1 : "Alpheratz",
    51 : "Altair",
    2 : "Ankaa",
    42 : "Antares",
    37 : "Arcturus",
    43 : "Atria",
    22 : "Avior",
    13 : "Bellatrix",
    16 : "Betelgeuse",
    17 : "Canopus",
    12 : "Capella",
    53 : "Deneb",
    28 : "Denebola",
    4 : "Diphda",
    27 : "Dubhe",
    14 : "Elnath",
    47 : "Eltanin",
    54 : "Enif",
    56 : "Formalhaut",
    31 : "Gacrux",
    29 : "Gienah",
    35 : "Hadar",
    6 : "Hamal",
    48 : "Kaus Australis",
    40 : "Kochab",
    57 : "Markab",
    8 : "Menkar",
    36 : "Menkent",
    24 : "Miaplacidus",
    9 : "Mirfak",
    50 : "Nunki",
    52 : "Peacock",
    21 : "Pollux",
    20 : "Procyon",
    46 : "Rasalhague",
    26 : "Regulus",
    11 : "Rigel",
    38 : "Rigil Kentaurus",
    44 : "Sabik",
    3 : "Schedar",
    45 : "Shaula",
    18 : "Sirius",
    33 : "Spica",
    23 : "Suhail" ,
    49 : "Vega",
    39 : "Zubenelgenubi",
}

# The 57 navigational stars reverse lookup, {name : number, ...}
STAR_NAME_NUMBER = {}
for k, v in STAR_NUMBER_NAME.items():
    assert v not in STAR_NAME_NUMBER
    STAR_NAME_NUMBER[v] = k
