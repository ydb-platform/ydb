/* 
 * Legal Notice 
 * 
 * This document and associated source code (the "Work") is a part of a 
 * benchmark specification maintained by the TPC. 
 * 
 * The TPC reserves all right, title, and interest to the Work as provided 
 * under U.S. and international laws, including without limitation all patent 
 * and trademark rights therein. 
 * 
 * No Warranty 
 * 
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION 
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE 
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER 
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY, 
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES, 
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR 
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF 
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE. 
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT, 
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT 
 *     WITH REGARD TO THE WORK. 
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO 
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE 
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS 
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT, 
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT 
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD 
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES. 
 * 
 * Contributors:
 * Gradient Systems
 */ 
#ifndef QGEN_PARAMS_H
#define QGEN_PARAMS_H

#include "r_params.h"
#include "release.h"
#define MAX_PARAM    21
#ifdef DECLARER

option_t options[] =
{
/* General Parmeters */
{"PROG",        OPT_STR|OPT_HIDE|OPT_SET,    0, "DO NOT MODIFY" , NULL, "qgen2"}, 
{"FILE",        OPT_STR,            1, "read parameters from file <s>", read_file, ""}, 
{"VERBOSE",        OPT_FLG,            2, "enable verbose output", NULL, "N"}, 
{"HELP",        OPT_FLG,            3, "display this message", usage, "N"},
{"DISTRIBUTIONS",    OPT_STR|OPT_ADV,4, "read distributions from file <s>", NULL, "tpcds.idx"}, 
{"OUTPUT_DIR",    OPT_STR,            5, "write query streams into directory <s>", NULL, "."}, 
#ifndef WIN32
{"PATH_SEP",    OPT_STR|OPT_ADV,    6, "use <s> to separate path elements", NULL, "/"}, 
#else
{"PATH_SEP",    OPT_STR|OPT_ADV,    6, "use <s> to separate path elements", NULL, "\\\\"}, 
#endif
{"DUMP",        OPT_FLG|OPT_ADV|OPT_HIDE,    7,"dump templates as parsed", NULL, "N"}, 
{"YYDEBUG",        OPT_FLG|OPT_ADV|OPT_HIDE,    8,"debug the grammar", NULL, "N"}, 
{"QUIET",        OPT_FLG,            9, "suppress all output (for scripting)", NULL, "N"}, 
{"STREAMS",        OPT_INT,            10, "generate <n> query streams/versions", NULL, "1"}, 
{"INPUT",        OPT_STR,            11, "read template names from <s>", NULL, ""}, 
{"SCALE",        OPT_INT,            12, "assume a database of <n> GB", NULL, "1"}, 
{"RNGSEED",        OPT_INT|OPT_ADV,    13, "seed the RNG with <n>", NULL, "19620718"}, 
{"RELEASE",        OPT_FLG|OPT_ADV,    14, "display QGEN release info", printReleaseInfo, ""}, 
{"TEMPLATE",    OPT_STR|OPT_ADV,    15, "build queries from template <s> ONLY", NULL, ""}, 
{"COUNT",        OPT_INT|OPT_ADV,    16, "generate <n> versions per stream (used with TEMPLATE)", NULL, "1"}, 
{"DEBUG",        OPT_FLG|OPT_ADV,    17,    "minor debugging outptut", NULL, "N"},
{"LOG",            OPT_STR,            18,    "write parameter log to <s>", NULL, ""},
{"FILTER",        OPT_FLG|OPT_ADV,    19,    "write generated queries to stdout", NULL, "N"},
{"QUALIFY",        OPT_FLG,            20,    "generate qualification queries in ascending order", NULL, "N"},
{"DIALECT",        OPT_STR|OPT_ADV,    21, "include query dialect defintions found in <s>.tpl", NULL, "ansi"},
{"DIRECTORY",    OPT_STR|OPT_ADV,    22, "look in <s> for templates", NULL, ""},
{NULL}
};

char *params[MAX_PARAM + 2];
#else
extern option_t options[];
extern char *params[];
extern char *szTableNames[];
#endif
#endif
