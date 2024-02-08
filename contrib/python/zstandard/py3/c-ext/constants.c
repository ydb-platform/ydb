/**
 * Copyright (c) 2016-present, Gregory Szorc
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include "python-zstandard.h"

extern PyObject *ZstdError;

static char frame_header[] = {
    '\x28',
    '\xb5',
    '\x2f',
    '\xfd',
};

void constants_module_init(PyObject *mod) {
    PyObject *version;
    PyObject *zstdVersion;
    PyObject *frameHeader;

    version = PyUnicode_FromString(PYTHON_ZSTANDARD_VERSION);
    PyModule_AddObject(mod, "__version__", version);

    ZstdError = PyErr_NewException("zstd.ZstdError", NULL, NULL);
    PyModule_AddObject(mod, "ZstdError", ZstdError);

    PyModule_AddIntConstant(mod, "FLUSH_BLOCK", 0);
    PyModule_AddIntConstant(mod, "FLUSH_FRAME", 1);

    PyModule_AddIntConstant(mod, "COMPRESSOBJ_FLUSH_FINISH",
                            compressorobj_flush_finish);
    PyModule_AddIntConstant(mod, "COMPRESSOBJ_FLUSH_BLOCK",
                            compressorobj_flush_block);

    /* For now, the version is a simple tuple instead of a dedicated type. */
    zstdVersion = PyTuple_New(3);
    PyTuple_SetItem(zstdVersion, 0, PyLong_FromLong(ZSTD_VERSION_MAJOR));
    PyTuple_SetItem(zstdVersion, 1, PyLong_FromLong(ZSTD_VERSION_MINOR));
    PyTuple_SetItem(zstdVersion, 2, PyLong_FromLong(ZSTD_VERSION_RELEASE));
    PyModule_AddObject(mod, "ZSTD_VERSION", zstdVersion);

    frameHeader = PyBytes_FromStringAndSize(frame_header, sizeof(frame_header));
    if (frameHeader) {
        PyModule_AddObject(mod, "FRAME_HEADER", frameHeader);
    }
    else {
        PyErr_Format(PyExc_ValueError, "could not create frame header object");
    }

    PyModule_AddObject(mod, "CONTENTSIZE_UNKNOWN",
                       PyLong_FromUnsignedLongLong(ZSTD_CONTENTSIZE_UNKNOWN));
    PyModule_AddObject(mod, "CONTENTSIZE_ERROR",
                       PyLong_FromUnsignedLongLong(ZSTD_CONTENTSIZE_ERROR));

    PyModule_AddIntConstant(mod, "MAX_COMPRESSION_LEVEL", ZSTD_maxCLevel());
    PyModule_AddIntConstant(mod, "COMPRESSION_RECOMMENDED_INPUT_SIZE",
                            (long)ZSTD_CStreamInSize());
    PyModule_AddIntConstant(mod, "COMPRESSION_RECOMMENDED_OUTPUT_SIZE",
                            (long)ZSTD_CStreamOutSize());
    PyModule_AddIntConstant(mod, "DECOMPRESSION_RECOMMENDED_INPUT_SIZE",
                            (long)ZSTD_DStreamInSize());
    PyModule_AddIntConstant(mod, "DECOMPRESSION_RECOMMENDED_OUTPUT_SIZE",
                            (long)ZSTD_DStreamOutSize());

    PyModule_AddIntConstant(mod, "MAGIC_NUMBER", ZSTD_MAGICNUMBER);
    PyModule_AddIntConstant(mod, "BLOCKSIZELOG_MAX", ZSTD_BLOCKSIZELOG_MAX);
    PyModule_AddIntConstant(mod, "BLOCKSIZE_MAX", ZSTD_BLOCKSIZE_MAX);
    PyModule_AddIntConstant(mod, "WINDOWLOG_MIN", ZSTD_WINDOWLOG_MIN);
    PyModule_AddIntConstant(mod, "WINDOWLOG_MAX", ZSTD_WINDOWLOG_MAX);
    PyModule_AddIntConstant(mod, "CHAINLOG_MIN", ZSTD_CHAINLOG_MIN);
    PyModule_AddIntConstant(mod, "CHAINLOG_MAX", ZSTD_CHAINLOG_MAX);
    PyModule_AddIntConstant(mod, "HASHLOG_MIN", ZSTD_HASHLOG_MIN);
    PyModule_AddIntConstant(mod, "HASHLOG_MAX", ZSTD_HASHLOG_MAX);
    PyModule_AddIntConstant(mod, "SEARCHLOG_MIN", ZSTD_SEARCHLOG_MIN);
    PyModule_AddIntConstant(mod, "SEARCHLOG_MAX", ZSTD_SEARCHLOG_MAX);
    PyModule_AddIntConstant(mod, "MINMATCH_MIN", ZSTD_MINMATCH_MIN);
    PyModule_AddIntConstant(mod, "MINMATCH_MAX", ZSTD_MINMATCH_MAX);
    /* TODO SEARCHLENGTH_* is deprecated. */
    PyModule_AddIntConstant(mod, "SEARCHLENGTH_MIN", ZSTD_MINMATCH_MIN);
    PyModule_AddIntConstant(mod, "SEARCHLENGTH_MAX", ZSTD_MINMATCH_MAX);
    PyModule_AddIntConstant(mod, "TARGETLENGTH_MIN", ZSTD_TARGETLENGTH_MIN);
    PyModule_AddIntConstant(mod, "TARGETLENGTH_MAX", ZSTD_TARGETLENGTH_MAX);
    PyModule_AddIntConstant(mod, "LDM_MINMATCH_MIN", ZSTD_LDM_MINMATCH_MIN);
    PyModule_AddIntConstant(mod, "LDM_MINMATCH_MAX", ZSTD_LDM_MINMATCH_MAX);
    PyModule_AddIntConstant(mod, "LDM_BUCKETSIZELOG_MAX",
                            ZSTD_LDM_BUCKETSIZELOG_MAX);

    PyModule_AddIntConstant(mod, "STRATEGY_FAST", ZSTD_fast);
    PyModule_AddIntConstant(mod, "STRATEGY_DFAST", ZSTD_dfast);
    PyModule_AddIntConstant(mod, "STRATEGY_GREEDY", ZSTD_greedy);
    PyModule_AddIntConstant(mod, "STRATEGY_LAZY", ZSTD_lazy);
    PyModule_AddIntConstant(mod, "STRATEGY_LAZY2", ZSTD_lazy2);
    PyModule_AddIntConstant(mod, "STRATEGY_BTLAZY2", ZSTD_btlazy2);
    PyModule_AddIntConstant(mod, "STRATEGY_BTOPT", ZSTD_btopt);
    PyModule_AddIntConstant(mod, "STRATEGY_BTULTRA", ZSTD_btultra);
    PyModule_AddIntConstant(mod, "STRATEGY_BTULTRA2", ZSTD_btultra2);

    PyModule_AddIntConstant(mod, "DICT_TYPE_AUTO", ZSTD_dct_auto);
    PyModule_AddIntConstant(mod, "DICT_TYPE_RAWCONTENT", ZSTD_dct_rawContent);
    PyModule_AddIntConstant(mod, "DICT_TYPE_FULLDICT", ZSTD_dct_fullDict);

    PyModule_AddIntConstant(mod, "FORMAT_ZSTD1", ZSTD_f_zstd1);
    PyModule_AddIntConstant(mod, "FORMAT_ZSTD1_MAGICLESS",
                            ZSTD_f_zstd1_magicless);
}
