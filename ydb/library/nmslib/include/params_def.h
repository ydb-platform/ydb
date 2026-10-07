/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#ifndef PARAMS_DEF_H
#define PARAMS_DEF_H

#include <string>

// Definition of most command-line parameters
const std::string HELP_PARAM_OPT                 = "help,h";
const std::string HELP_PARAM_MSG                 = "produce help message";

const std::string SPACE_TYPE_PARAM_OPT           = "spaceType,s";
const std::string SPACE_TYPE_PARAM_MSG           = "space type, e.g., l1, l2, lp";

const std::string DIST_TYPE_PARAM_OPT            = "distType";
const std::string DIST_TYPE_PARAM_MSG            = "distance value type: int, float, double";

const std::string DATA_FILE_PARAM_OPT            = "dataFile,i";
const std::string DATA_FILE_PARAM_MSG            = "input data file";

const std::string MAX_NUM_DATA_PARAM_OPT         = "maxNumData,D";
const std::string MAX_NUM_DATA_PARAM_MSG         = "if non-zero, only the first maxNumData elements are used";
const unsigned MAX_NUM_DATA_PARAM_DEFAULT        = 0;

const std::string QUERY_FILE_PARAM_OPT           = "queryFile,q";
const std::string QUERY_FILE_PARAM_MSG           = "query file";
const std::string QUERY_FILE_PARAM_DEFAULT       = "";

const std::string LOAD_INDEX_PARAM_OPT           = "loadIndex,L";
const std::string LOAD_INDEX_PARAM_MSG           = "a location to load the index from ";
const std::string LOAD_INDEX_PARAM_DEFAULT       = "";

const std::string SAVE_INDEX_PARAM_OPT           = "saveIndex,S";
const std::string SAVE_INDEX_PARAM_MSG           = "a location to save the index to ";
const std::string SAVE_INDEX_PARAM_DEFAULT       = "";

const std::string CACHE_PREFIX_GS_PARAM_OPT      = "cachePrefixGS,g";
const std::string CACHE_PREFIX_GS_PARAM_MSG      = "a prefix of gold standard cache files";
const std::string CACHE_PREFIX_GS_PARAM_DEFAULT  = "";

const std::string MAX_CACHE_GS_QTY_PARAM_OPT     = "maxCacheGSRelativeQty,";
const std::string MAX_CACHE_GS_QTY_PARAM_MSG     = "a maximum number of gold standard entries to compute/cache, note that it is relative to the number of result entries.";
const float MAX_CACHE_GS_QTY_PARAM_DEFAULT       = 10.0;

const std::string RECALL_ONLY_PARAM_OPT          = "recallOnly";
const std::string RECALL_ONLY_PARAM_MSG          = "if tset, only recall is computed, but not other effectiveness metrics";
const bool RECALL_ONLY_PARAM_DEFAULT             = false;

const std::string LOG_FILE_PARAM_OPT             = "logFile,l";
const std::string LOG_FILE_PARAM_MSG             = "log file";
const std::string LOG_FILE_PARAM_DEFAULT         = "";

const std::string MAX_NUM_QUERY_PARAM_OPT        = "maxNumQuery,Q";
const std::string MAX_NUM_QUERY_PARAM_MSG        = "if non-zero, use maxNumQuery query elements (required in the case of bootstrapping)";
const unsigned MAX_NUM_QUERY_PARAM_DEFAULT       = 0;

const std::string TEST_SET_QTY_PARAM_OPT         = "testSetQty,b";
const std::string TEST_SET_QTY_PARAM_MSG         = "# of test sets obtained by bootstrapping; ignored if queryFile is specified";
const unsigned TEST_SET_QTY_PARAM_DEFAULT        = 0;

const std::string KNN_PARAM_OPT                  = "knn,k";
const std::string KNN_PARAM_MSG                  = "comma-separated values of K for the k-NN search";

const std::string RANGE_PARAM_OPT                = "range,r";
const std::string RANGE_PARAM_MSG                = "comma-separated radii for the range searches";

const std::string EPS_PARAM_OPT                  = "eps";
const std::string EPS_PARAM_MSG                  = "the parameter for the eps-approximate k-NN search.";
const double EPS_PARAM_DEFAULT                   = 0.0;

const std::string QUERY_TIME_PARAMS_PARAM_OPT    = "queryTimeParams,t";
const std::string QUERY_TIME_PARAMS_PARAM_MSG    = "query-time method(s) parameters in the format: param1=value1,param2=value2,...,paramK=valueK";

const std::string INDEX_TIME_PARAMS_PARAM_OPT    = "createIndex,c";
const std::string INDEX_TIME_PARAMS_PARAM_MSG    = "index-time method(s) parameters in the format: param1=value1,param2=value2,...,paramK=valueK";

const std::string METHOD_PARAM_OPT               = "method,m";
const std::string METHOD_PARAM_MSG               = "method/index name";
const std::string METHOD_PARAM_DEFAULT           = "";

const std::string THREAD_TEST_QTY_PARAM_OPT      = "threadTestQty";
const std::string THREAD_TEST_QTY_PARAM_MSG      = "# of threads during querying";
const unsigned THREAD_TEST_QTY_PARAM_DEFAULT     = 1;

const std::string OUT_FILE_PREFIX_PARAM_OPT      = "outFilePrefix,o";
const std::string OUT_FILE_PREFIX_PARAM_MSG      = "output file prefix";
const std::string OUT_FILE_PREFIX_PARAM_DEFAULT  = "";

const std::string APPEND_TO_RES_FILE_PARAM_OPT   = "appendToResFile,a";
const std::string APPEND_TO_RES_FILE_PARAM_MSG   = "do not override information in results files, append new data";

const std::string NO_PROGRESS_PARAM_OPT          = "noProgressBar";
const std::string NO_PROGRESS_PARAM_MSG          = "suppress displaying (mostly indexing) progress bars (for some methods)";

// Server/client parameters

const std::string DEBUG_PARAM_OPT                = "debug,D";
const std::string DEBUG_PARAM_MSG                = "Print debug messages?";

const std::string PORT_PARAM_OPT                 = "port,p";
const std::string PORT_PARAM_MSG                 = "TCP/IP port number";

const std::string ADDR_PARAM_OPT                 = "addr,a";
const std::string ADDR_PARAM_MSG                 = "TCP/IP server address";

const std::string THREAD_PARAM_OPT               = "threadQty";
const std::string THREAD_PARAM_MSG               = "A number of server threads";

const std::string RET_EXT_ID_PARAM_OPT           = "retExternId,e";
const std::string RET_EXT_ID_PARAM_MSG           = "Return external IDs?";

const std::string RET_OBJ_PARAM_OPT              = "retObj,o";
const std::string RET_OBJ_PARAM_MSG              = "Return string representation of found objects?";

#endif
