from devtools.yamaker.project import GNUMakeNixProject

readstat = GNUMakeNixProject(
    nixattr="readstat",
    owners=["g:pythia", "g:cpp-contrib"],
    arcdir="contrib/libs/readstat",
    disable_includes=[
        # if HAVE_CSVREADER
        "read_csv/read_csv.h",
        # if HAVE_ZLIB
        "zlib.h",
        "readstat_zsav_compress.h",
        "readstat_zsav_read.h",
        "readstat_zsav_write.h",
        # if HAVE_XLSXWRITER
        "write/mod_xlsx.h",
    ],
)
