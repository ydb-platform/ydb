#include <string.h>
#if defined(_MSC_VER)
#   define strncasecmp _strnicmp
#   define strcasecmp _stricmp
#else
#   include <strings.h>
#endif

#include "file_format.h"
#include "../../readstat.h"

rs_format_e readstat_format(const char *filename) {
    if (filename == NULL)
        return RS_FORMAT_UNKNOWN;

    size_t len = strlen(filename);
    if (len < sizeof(".dta")-1)
        return RS_FORMAT_UNKNOWN;

    if (strncasecmp(filename + len - 4, ".dta", 4) == 0)
        return RS_FORMAT_DTA;

    if (strncasecmp(filename + len - 4, ".dct", 4) == 0)
        return RS_FORMAT_STATA_DICTIONARY;

    if (strncasecmp(filename + len - 4, ".por", 4) == 0)
        return RS_FORMAT_POR;

    if (strncasecmp(filename + len - 4, ".sas", 4) == 0)
        return RS_FORMAT_SAS_COMMANDS;

    if (strncasecmp(filename + len - 4, ".sps", 4) == 0)
        return RS_FORMAT_SPSS_COMMANDS;

    if (strncasecmp(filename + len - 4, ".sav", 4) == 0)
        return RS_FORMAT_SAV;

#if HAVE_CSVREADER
    if (strncasecmp(filename + len - 4, ".csv", 4) == 0)
        return RS_FORMAT_CSV;
#endif

    if (strncasecmp(filename + len - 4, ".xpt", 4) == 0)
        return RS_FORMAT_XPORT;

    if (len < sizeof(".json")-1)
        return RS_FORMAT_UNKNOWN;
    
    if (strncasecmp(filename + len - 5, ".json", 5) == 0)
        return RS_FORMAT_JSON;

    if (strncasecmp(filename + len - 5, ".zsav", 5) == 0)
        return RS_FORMAT_ZSAV;

    if (len < sizeof(".sas7bdat")-1)
        return RS_FORMAT_UNKNOWN;

    if (strncasecmp(filename + len - 9, ".sas7bdat", 9) == 0)
        return RS_FORMAT_SAS_DATA;

    if (strncasecmp(filename + len - 9, ".sas7bcat", 9) == 0)
        return RS_FORMAT_SAS_CATALOG;

    return RS_FORMAT_UNKNOWN;
}

const char *readstat_format_name(rs_format_e format) {
    if (format == RS_FORMAT_DTA)
        return "Stata binary file (DTA)";

    if (format == RS_FORMAT_STATA_DICTIONARY)
        return "Stata dictionary file (DCT)";

    if (format == RS_FORMAT_SAV)
        return "SPSS binary file (SAV)";

    if (format == RS_FORMAT_ZSAV)
        return "SPSS compressed binary file (ZSAV)";

    if (format == RS_FORMAT_POR)
        return "SPSS portable file (POR)";

    if (format == RS_FORMAT_SPSS_COMMANDS)
        return "SPSS command file";

    if (format == RS_FORMAT_SAS_DATA)
        return "SAS data file (SAS7BDAT)";

    if (format == RS_FORMAT_SAS_CATALOG)
        return "SAS catalog file (SAS7BCAT)";

    if (format == RS_FORMAT_SAS_COMMANDS)
        return "SAS command file";
    
    if (format == RS_FORMAT_CSV)
        return "CSV";

    if (format == RS_FORMAT_XPORT)
        return "SAS transport file (XPORT)";

    return "Unknown";
}

int is_catalog(const char *filename) {
    return (readstat_format(filename) == RS_FORMAT_SAS_CATALOG);
}

int is_json(const char *filename) {
    return (readstat_format(filename) == RS_FORMAT_JSON);
}

int is_dictionary(const char *filename) {
    return (readstat_format(filename) == RS_FORMAT_STATA_DICTIONARY ||
            readstat_format(filename) == RS_FORMAT_SAS_COMMANDS ||
            readstat_format(filename) == RS_FORMAT_SPSS_COMMANDS);
}

int can_read(const char *filename) {
    return (readstat_format(filename) != RS_FORMAT_UNKNOWN);
}
