PROGRAM(pyres_pack)

SET(PYRES_ENCODINGS contrib/tools/python3/Lib/encodings)

IF (NOT USE_ARCADIA_PYTHON)
    PYTHON3_ADDINCL()

    PEERDIR(
        contrib/tools/python3
    )
ELSEIF (USE_PYTHON3_PREV)
    PEERDIR(
        contrib/tools/python3_prev
    )
    ADDINCL(
        contrib/tools/python3_prev/Include
    )
    SET(PYRES_ENCODINGS contrib/tools/python3_prev/Lib/encodings)
ELSE()
    PEERDIR(
        contrib/tools/python3
    )
    ADDINCL(
        contrib/tools/python3/Include
    )
ENDIF()

PEERDIR(
    library/cpp/archive
    library/cpp/resource
)

SRCS(main.cpp)

# The encodings package, frozen into the tool (see TFrozenEncodings in main.cpp): with it the embedded
# interpreter has the codec registry and the import system, so no python program is needed as a
# fallback compiler. Compiled with stage0pycc and embedded with ARCHIVE, not with python resources:
# the tool must not depend on the machinery it implements.
RUN_PROGRAM(
    library/python/runtime_py3/stage0pycc
        mod=${PYRES_ENCODINGS}/__init__.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/__init__.py encodings.pyc
        mod=${PYRES_ENCODINGS}/aliases.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/aliases.py encodings.aliases.pyc
        mod=${PYRES_ENCODINGS}/ascii.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/ascii.py encodings.ascii.pyc
        mod=${PYRES_ENCODINGS}/base64_codec.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/base64_codec.py encodings.base64_codec.pyc
        mod=${PYRES_ENCODINGS}/big5.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/big5.py encodings.big5.pyc
        mod=${PYRES_ENCODINGS}/big5hkscs.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/big5hkscs.py encodings.big5hkscs.pyc
        mod=${PYRES_ENCODINGS}/bz2_codec.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/bz2_codec.py encodings.bz2_codec.pyc
        mod=${PYRES_ENCODINGS}/charmap.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/charmap.py encodings.charmap.pyc
        mod=${PYRES_ENCODINGS}/cp037.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp037.py encodings.cp037.pyc
        mod=${PYRES_ENCODINGS}/cp1006.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1006.py encodings.cp1006.pyc
        mod=${PYRES_ENCODINGS}/cp1026.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1026.py encodings.cp1026.pyc
        mod=${PYRES_ENCODINGS}/cp1125.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1125.py encodings.cp1125.pyc
        mod=${PYRES_ENCODINGS}/cp1140.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1140.py encodings.cp1140.pyc
        mod=${PYRES_ENCODINGS}/cp1250.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1250.py encodings.cp1250.pyc
        mod=${PYRES_ENCODINGS}/cp1251.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1251.py encodings.cp1251.pyc
        mod=${PYRES_ENCODINGS}/cp1252.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1252.py encodings.cp1252.pyc
        mod=${PYRES_ENCODINGS}/cp1253.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1253.py encodings.cp1253.pyc
        mod=${PYRES_ENCODINGS}/cp1254.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1254.py encodings.cp1254.pyc
        mod=${PYRES_ENCODINGS}/cp1255.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1255.py encodings.cp1255.pyc
        mod=${PYRES_ENCODINGS}/cp1256.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1256.py encodings.cp1256.pyc
        mod=${PYRES_ENCODINGS}/cp1257.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1257.py encodings.cp1257.pyc
        mod=${PYRES_ENCODINGS}/cp1258.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1258.py encodings.cp1258.pyc
        mod=${PYRES_ENCODINGS}/cp273.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp273.py encodings.cp273.pyc
        mod=${PYRES_ENCODINGS}/cp424.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp424.py encodings.cp424.pyc
        mod=${PYRES_ENCODINGS}/cp437.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp437.py encodings.cp437.pyc
        mod=${PYRES_ENCODINGS}/cp500.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp500.py encodings.cp500.pyc
        mod=${PYRES_ENCODINGS}/cp720.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp720.py encodings.cp720.pyc
        mod=${PYRES_ENCODINGS}/cp737.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp737.py encodings.cp737.pyc
        mod=${PYRES_ENCODINGS}/cp775.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp775.py encodings.cp775.pyc
        mod=${PYRES_ENCODINGS}/cp850.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp850.py encodings.cp850.pyc
        mod=${PYRES_ENCODINGS}/cp852.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp852.py encodings.cp852.pyc
        mod=${PYRES_ENCODINGS}/cp855.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp855.py encodings.cp855.pyc
        mod=${PYRES_ENCODINGS}/cp856.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp856.py encodings.cp856.pyc
        mod=${PYRES_ENCODINGS}/cp857.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp857.py encodings.cp857.pyc
        mod=${PYRES_ENCODINGS}/cp858.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp858.py encodings.cp858.pyc
        mod=${PYRES_ENCODINGS}/cp860.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp860.py encodings.cp860.pyc
        mod=${PYRES_ENCODINGS}/cp861.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp861.py encodings.cp861.pyc
        mod=${PYRES_ENCODINGS}/cp862.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp862.py encodings.cp862.pyc
        mod=${PYRES_ENCODINGS}/cp863.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp863.py encodings.cp863.pyc
        mod=${PYRES_ENCODINGS}/cp864.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp864.py encodings.cp864.pyc
        mod=${PYRES_ENCODINGS}/cp865.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp865.py encodings.cp865.pyc
        mod=${PYRES_ENCODINGS}/cp866.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp866.py encodings.cp866.pyc
        mod=${PYRES_ENCODINGS}/cp869.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp869.py encodings.cp869.pyc
        mod=${PYRES_ENCODINGS}/cp874.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp874.py encodings.cp874.pyc
        mod=${PYRES_ENCODINGS}/cp875.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp875.py encodings.cp875.pyc
        mod=${PYRES_ENCODINGS}/cp932.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp932.py encodings.cp932.pyc
        mod=${PYRES_ENCODINGS}/cp949.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp949.py encodings.cp949.pyc
        mod=${PYRES_ENCODINGS}/cp950.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp950.py encodings.cp950.pyc
        mod=${PYRES_ENCODINGS}/euc_jis_2004.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/euc_jis_2004.py encodings.euc_jis_2004.pyc
        mod=${PYRES_ENCODINGS}/euc_jisx0213.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/euc_jisx0213.py encodings.euc_jisx0213.pyc
        mod=${PYRES_ENCODINGS}/euc_jp.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/euc_jp.py encodings.euc_jp.pyc
        mod=${PYRES_ENCODINGS}/euc_kr.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/euc_kr.py encodings.euc_kr.pyc
        mod=${PYRES_ENCODINGS}/gb18030.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/gb18030.py encodings.gb18030.pyc
        mod=${PYRES_ENCODINGS}/gb2312.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/gb2312.py encodings.gb2312.pyc
        mod=${PYRES_ENCODINGS}/gbk.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/gbk.py encodings.gbk.pyc
        mod=${PYRES_ENCODINGS}/hex_codec.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/hex_codec.py encodings.hex_codec.pyc
        mod=${PYRES_ENCODINGS}/hp_roman8.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/hp_roman8.py encodings.hp_roman8.pyc
        mod=${PYRES_ENCODINGS}/hz.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/hz.py encodings.hz.pyc
        mod=${PYRES_ENCODINGS}/idna.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/idna.py encodings.idna.pyc
        mod=${PYRES_ENCODINGS}/iso2022_jp.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso2022_jp.py encodings.iso2022_jp.pyc
        mod=${PYRES_ENCODINGS}/iso2022_jp_1.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso2022_jp_1.py encodings.iso2022_jp_1.pyc
        mod=${PYRES_ENCODINGS}/iso2022_jp_2.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso2022_jp_2.py encodings.iso2022_jp_2.pyc
        mod=${PYRES_ENCODINGS}/iso2022_jp_2004.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso2022_jp_2004.py encodings.iso2022_jp_2004.pyc
        mod=${PYRES_ENCODINGS}/iso2022_jp_3.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso2022_jp_3.py encodings.iso2022_jp_3.pyc
        mod=${PYRES_ENCODINGS}/iso2022_jp_ext.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso2022_jp_ext.py encodings.iso2022_jp_ext.pyc
        mod=${PYRES_ENCODINGS}/iso2022_kr.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso2022_kr.py encodings.iso2022_kr.pyc
        mod=${PYRES_ENCODINGS}/iso8859_1.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_1.py encodings.iso8859_1.pyc
        mod=${PYRES_ENCODINGS}/iso8859_10.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_10.py encodings.iso8859_10.pyc
        mod=${PYRES_ENCODINGS}/iso8859_11.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_11.py encodings.iso8859_11.pyc
        mod=${PYRES_ENCODINGS}/iso8859_13.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_13.py encodings.iso8859_13.pyc
        mod=${PYRES_ENCODINGS}/iso8859_14.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_14.py encodings.iso8859_14.pyc
        mod=${PYRES_ENCODINGS}/iso8859_15.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_15.py encodings.iso8859_15.pyc
        mod=${PYRES_ENCODINGS}/iso8859_16.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_16.py encodings.iso8859_16.pyc
        mod=${PYRES_ENCODINGS}/iso8859_2.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_2.py encodings.iso8859_2.pyc
        mod=${PYRES_ENCODINGS}/iso8859_3.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_3.py encodings.iso8859_3.pyc
        mod=${PYRES_ENCODINGS}/iso8859_4.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_4.py encodings.iso8859_4.pyc
        mod=${PYRES_ENCODINGS}/iso8859_5.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_5.py encodings.iso8859_5.pyc
        mod=${PYRES_ENCODINGS}/iso8859_6.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_6.py encodings.iso8859_6.pyc
        mod=${PYRES_ENCODINGS}/iso8859_7.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_7.py encodings.iso8859_7.pyc
        mod=${PYRES_ENCODINGS}/iso8859_8.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_8.py encodings.iso8859_8.pyc
        mod=${PYRES_ENCODINGS}/iso8859_9.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_9.py encodings.iso8859_9.pyc
        mod=${PYRES_ENCODINGS}/johab.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/johab.py encodings.johab.pyc
        mod=${PYRES_ENCODINGS}/koi8_r.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/koi8_r.py encodings.koi8_r.pyc
        mod=${PYRES_ENCODINGS}/koi8_t.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/koi8_t.py encodings.koi8_t.pyc
        mod=${PYRES_ENCODINGS}/koi8_u.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/koi8_u.py encodings.koi8_u.pyc
        mod=${PYRES_ENCODINGS}/kz1048.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/kz1048.py encodings.kz1048.pyc
        mod=${PYRES_ENCODINGS}/latin_1.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/latin_1.py encodings.latin_1.pyc
        mod=${PYRES_ENCODINGS}/mac_arabic.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_arabic.py encodings.mac_arabic.pyc
        mod=${PYRES_ENCODINGS}/mac_croatian.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_croatian.py encodings.mac_croatian.pyc
        mod=${PYRES_ENCODINGS}/mac_cyrillic.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_cyrillic.py encodings.mac_cyrillic.pyc
        mod=${PYRES_ENCODINGS}/mac_farsi.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_farsi.py encodings.mac_farsi.pyc
        mod=${PYRES_ENCODINGS}/mac_greek.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_greek.py encodings.mac_greek.pyc
        mod=${PYRES_ENCODINGS}/mac_iceland.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_iceland.py encodings.mac_iceland.pyc
        mod=${PYRES_ENCODINGS}/mac_latin2.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_latin2.py encodings.mac_latin2.pyc
        mod=${PYRES_ENCODINGS}/mac_roman.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_roman.py encodings.mac_roman.pyc
        mod=${PYRES_ENCODINGS}/mac_romanian.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_romanian.py encodings.mac_romanian.pyc
        mod=${PYRES_ENCODINGS}/mac_turkish.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_turkish.py encodings.mac_turkish.pyc
        mod=${PYRES_ENCODINGS}/mbcs.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mbcs.py encodings.mbcs.pyc
        mod=${PYRES_ENCODINGS}/oem.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/oem.py encodings.oem.pyc
        mod=${PYRES_ENCODINGS}/palmos.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/palmos.py encodings.palmos.pyc
        mod=${PYRES_ENCODINGS}/ptcp154.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/ptcp154.py encodings.ptcp154.pyc
        mod=${PYRES_ENCODINGS}/punycode.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/punycode.py encodings.punycode.pyc
        mod=${PYRES_ENCODINGS}/quopri_codec.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/quopri_codec.py encodings.quopri_codec.pyc
        mod=${PYRES_ENCODINGS}/raw_unicode_escape.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/raw_unicode_escape.py encodings.raw_unicode_escape.pyc
        mod=${PYRES_ENCODINGS}/rot_13.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/rot_13.py encodings.rot_13.pyc
        mod=${PYRES_ENCODINGS}/shift_jis.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/shift_jis.py encodings.shift_jis.pyc
        mod=${PYRES_ENCODINGS}/shift_jis_2004.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/shift_jis_2004.py encodings.shift_jis_2004.pyc
        mod=${PYRES_ENCODINGS}/shift_jisx0213.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/shift_jisx0213.py encodings.shift_jisx0213.pyc
        mod=${PYRES_ENCODINGS}/tis_620.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/tis_620.py encodings.tis_620.pyc
        mod=${PYRES_ENCODINGS}/undefined.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/undefined.py encodings.undefined.pyc
        mod=${PYRES_ENCODINGS}/unicode_escape.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/unicode_escape.py encodings.unicode_escape.pyc
        mod=${PYRES_ENCODINGS}/utf_16.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_16.py encodings.utf_16.pyc
        mod=${PYRES_ENCODINGS}/utf_16_be.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_16_be.py encodings.utf_16_be.pyc
        mod=${PYRES_ENCODINGS}/utf_16_le.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_16_le.py encodings.utf_16_le.pyc
        mod=${PYRES_ENCODINGS}/utf_32.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_32.py encodings.utf_32.pyc
        mod=${PYRES_ENCODINGS}/utf_32_be.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_32_be.py encodings.utf_32_be.pyc
        mod=${PYRES_ENCODINGS}/utf_32_le.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_32_le.py encodings.utf_32_le.pyc
        mod=${PYRES_ENCODINGS}/utf_7.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_7.py encodings.utf_7.pyc
        mod=${PYRES_ENCODINGS}/utf_8.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_8.py encodings.utf_8.pyc
        mod=${PYRES_ENCODINGS}/utf_8_sig.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_8_sig.py encodings.utf_8_sig.pyc
        mod=${PYRES_ENCODINGS}/uu_codec.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/uu_codec.py encodings.uu_codec.pyc
        mod=${PYRES_ENCODINGS}/zlib_codec.py ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/zlib_codec.py encodings.zlib_codec.pyc
    IN
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/__init__.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/aliases.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/ascii.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/base64_codec.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/big5.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/big5hkscs.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/bz2_codec.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/charmap.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp037.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1006.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1026.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1125.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1140.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1250.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1251.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1252.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1253.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1254.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1255.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1256.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1257.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp1258.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp273.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp424.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp437.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp500.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp720.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp737.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp775.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp850.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp852.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp855.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp856.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp857.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp858.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp860.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp861.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp862.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp863.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp864.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp865.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp866.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp869.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp874.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp875.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp932.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp949.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/cp950.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/euc_jis_2004.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/euc_jisx0213.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/euc_jp.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/euc_kr.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/gb18030.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/gb2312.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/gbk.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/hex_codec.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/hp_roman8.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/hz.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/idna.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso2022_jp.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso2022_jp_1.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso2022_jp_2.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso2022_jp_2004.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso2022_jp_3.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso2022_jp_ext.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso2022_kr.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_1.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_10.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_11.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_13.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_14.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_15.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_16.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_2.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_3.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_4.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_5.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_6.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_7.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_8.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/iso8859_9.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/johab.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/koi8_r.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/koi8_t.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/koi8_u.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/kz1048.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/latin_1.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_arabic.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_croatian.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_cyrillic.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_farsi.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_greek.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_iceland.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_latin2.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_roman.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_romanian.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mac_turkish.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/mbcs.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/oem.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/palmos.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/ptcp154.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/punycode.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/quopri_codec.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/raw_unicode_escape.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/rot_13.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/shift_jis.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/shift_jis_2004.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/shift_jisx0213.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/tis_620.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/undefined.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/unicode_escape.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_16.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_16_be.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_16_le.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_32.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_32_be.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_32_le.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_7.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_8.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/utf_8_sig.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/uu_codec.py
        ${ARCADIA_ROOT}/${PYRES_ENCODINGS}/zlib_codec.py
    OUT_NOAUTO
        encodings.pyc
        encodings.aliases.pyc
        encodings.ascii.pyc
        encodings.base64_codec.pyc
        encodings.big5.pyc
        encodings.big5hkscs.pyc
        encodings.bz2_codec.pyc
        encodings.charmap.pyc
        encodings.cp037.pyc
        encodings.cp1006.pyc
        encodings.cp1026.pyc
        encodings.cp1125.pyc
        encodings.cp1140.pyc
        encodings.cp1250.pyc
        encodings.cp1251.pyc
        encodings.cp1252.pyc
        encodings.cp1253.pyc
        encodings.cp1254.pyc
        encodings.cp1255.pyc
        encodings.cp1256.pyc
        encodings.cp1257.pyc
        encodings.cp1258.pyc
        encodings.cp273.pyc
        encodings.cp424.pyc
        encodings.cp437.pyc
        encodings.cp500.pyc
        encodings.cp720.pyc
        encodings.cp737.pyc
        encodings.cp775.pyc
        encodings.cp850.pyc
        encodings.cp852.pyc
        encodings.cp855.pyc
        encodings.cp856.pyc
        encodings.cp857.pyc
        encodings.cp858.pyc
        encodings.cp860.pyc
        encodings.cp861.pyc
        encodings.cp862.pyc
        encodings.cp863.pyc
        encodings.cp864.pyc
        encodings.cp865.pyc
        encodings.cp866.pyc
        encodings.cp869.pyc
        encodings.cp874.pyc
        encodings.cp875.pyc
        encodings.cp932.pyc
        encodings.cp949.pyc
        encodings.cp950.pyc
        encodings.euc_jis_2004.pyc
        encodings.euc_jisx0213.pyc
        encodings.euc_jp.pyc
        encodings.euc_kr.pyc
        encodings.gb18030.pyc
        encodings.gb2312.pyc
        encodings.gbk.pyc
        encodings.hex_codec.pyc
        encodings.hp_roman8.pyc
        encodings.hz.pyc
        encodings.idna.pyc
        encodings.iso2022_jp.pyc
        encodings.iso2022_jp_1.pyc
        encodings.iso2022_jp_2.pyc
        encodings.iso2022_jp_2004.pyc
        encodings.iso2022_jp_3.pyc
        encodings.iso2022_jp_ext.pyc
        encodings.iso2022_kr.pyc
        encodings.iso8859_1.pyc
        encodings.iso8859_10.pyc
        encodings.iso8859_11.pyc
        encodings.iso8859_13.pyc
        encodings.iso8859_14.pyc
        encodings.iso8859_15.pyc
        encodings.iso8859_16.pyc
        encodings.iso8859_2.pyc
        encodings.iso8859_3.pyc
        encodings.iso8859_4.pyc
        encodings.iso8859_5.pyc
        encodings.iso8859_6.pyc
        encodings.iso8859_7.pyc
        encodings.iso8859_8.pyc
        encodings.iso8859_9.pyc
        encodings.johab.pyc
        encodings.koi8_r.pyc
        encodings.koi8_t.pyc
        encodings.koi8_u.pyc
        encodings.kz1048.pyc
        encodings.latin_1.pyc
        encodings.mac_arabic.pyc
        encodings.mac_croatian.pyc
        encodings.mac_cyrillic.pyc
        encodings.mac_farsi.pyc
        encodings.mac_greek.pyc
        encodings.mac_iceland.pyc
        encodings.mac_latin2.pyc
        encodings.mac_roman.pyc
        encodings.mac_romanian.pyc
        encodings.mac_turkish.pyc
        encodings.mbcs.pyc
        encodings.oem.pyc
        encodings.palmos.pyc
        encodings.ptcp154.pyc
        encodings.punycode.pyc
        encodings.quopri_codec.pyc
        encodings.raw_unicode_escape.pyc
        encodings.rot_13.pyc
        encodings.shift_jis.pyc
        encodings.shift_jis_2004.pyc
        encodings.shift_jisx0213.pyc
        encodings.tis_620.pyc
        encodings.undefined.pyc
        encodings.unicode_escape.pyc
        encodings.utf_16.pyc
        encodings.utf_16_be.pyc
        encodings.utf_16_le.pyc
        encodings.utf_32.pyc
        encodings.utf_32_be.pyc
        encodings.utf_32_le.pyc
        encodings.utf_7.pyc
        encodings.utf_8.pyc
        encodings.utf_8_sig.pyc
        encodings.uu_codec.pyc
        encodings.zlib_codec.pyc
    ENV PYTHONHASHSEED=0
)

ARCHIVE(
    NAME pyres_encodings.inc
    DONTCOMPRESS
        encodings.pyc
        encodings.aliases.pyc
        encodings.ascii.pyc
        encodings.base64_codec.pyc
        encodings.big5.pyc
        encodings.big5hkscs.pyc
        encodings.bz2_codec.pyc
        encodings.charmap.pyc
        encodings.cp037.pyc
        encodings.cp1006.pyc
        encodings.cp1026.pyc
        encodings.cp1125.pyc
        encodings.cp1140.pyc
        encodings.cp1250.pyc
        encodings.cp1251.pyc
        encodings.cp1252.pyc
        encodings.cp1253.pyc
        encodings.cp1254.pyc
        encodings.cp1255.pyc
        encodings.cp1256.pyc
        encodings.cp1257.pyc
        encodings.cp1258.pyc
        encodings.cp273.pyc
        encodings.cp424.pyc
        encodings.cp437.pyc
        encodings.cp500.pyc
        encodings.cp720.pyc
        encodings.cp737.pyc
        encodings.cp775.pyc
        encodings.cp850.pyc
        encodings.cp852.pyc
        encodings.cp855.pyc
        encodings.cp856.pyc
        encodings.cp857.pyc
        encodings.cp858.pyc
        encodings.cp860.pyc
        encodings.cp861.pyc
        encodings.cp862.pyc
        encodings.cp863.pyc
        encodings.cp864.pyc
        encodings.cp865.pyc
        encodings.cp866.pyc
        encodings.cp869.pyc
        encodings.cp874.pyc
        encodings.cp875.pyc
        encodings.cp932.pyc
        encodings.cp949.pyc
        encodings.cp950.pyc
        encodings.euc_jis_2004.pyc
        encodings.euc_jisx0213.pyc
        encodings.euc_jp.pyc
        encodings.euc_kr.pyc
        encodings.gb18030.pyc
        encodings.gb2312.pyc
        encodings.gbk.pyc
        encodings.hex_codec.pyc
        encodings.hp_roman8.pyc
        encodings.hz.pyc
        encodings.idna.pyc
        encodings.iso2022_jp.pyc
        encodings.iso2022_jp_1.pyc
        encodings.iso2022_jp_2.pyc
        encodings.iso2022_jp_2004.pyc
        encodings.iso2022_jp_3.pyc
        encodings.iso2022_jp_ext.pyc
        encodings.iso2022_kr.pyc
        encodings.iso8859_1.pyc
        encodings.iso8859_10.pyc
        encodings.iso8859_11.pyc
        encodings.iso8859_13.pyc
        encodings.iso8859_14.pyc
        encodings.iso8859_15.pyc
        encodings.iso8859_16.pyc
        encodings.iso8859_2.pyc
        encodings.iso8859_3.pyc
        encodings.iso8859_4.pyc
        encodings.iso8859_5.pyc
        encodings.iso8859_6.pyc
        encodings.iso8859_7.pyc
        encodings.iso8859_8.pyc
        encodings.iso8859_9.pyc
        encodings.johab.pyc
        encodings.koi8_r.pyc
        encodings.koi8_t.pyc
        encodings.koi8_u.pyc
        encodings.kz1048.pyc
        encodings.latin_1.pyc
        encodings.mac_arabic.pyc
        encodings.mac_croatian.pyc
        encodings.mac_cyrillic.pyc
        encodings.mac_farsi.pyc
        encodings.mac_greek.pyc
        encodings.mac_iceland.pyc
        encodings.mac_latin2.pyc
        encodings.mac_roman.pyc
        encodings.mac_romanian.pyc
        encodings.mac_turkish.pyc
        encodings.mbcs.pyc
        encodings.oem.pyc
        encodings.palmos.pyc
        encodings.ptcp154.pyc
        encodings.punycode.pyc
        encodings.quopri_codec.pyc
        encodings.raw_unicode_escape.pyc
        encodings.rot_13.pyc
        encodings.shift_jis.pyc
        encodings.shift_jis_2004.pyc
        encodings.shift_jisx0213.pyc
        encodings.tis_620.pyc
        encodings.undefined.pyc
        encodings.unicode_escape.pyc
        encodings.utf_16.pyc
        encodings.utf_16_be.pyc
        encodings.utf_16_le.pyc
        encodings.utf_32.pyc
        encodings.utf_32_be.pyc
        encodings.utf_32_le.pyc
        encodings.utf_7.pyc
        encodings.utf_8.pyc
        encodings.utf_8_sig.pyc
        encodings.uu_codec.pyc
        encodings.zlib_codec.pyc
)

END()
