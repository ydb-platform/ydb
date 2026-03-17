/* typecast_array.c - array typecasters
 *
 * Copyright (C) 2005-2019 Federico Di Gregorio <fog@debian.org>
 * Copyright (C) 2020-2021 The Psycopg Team
 *
 * This file is part of psycopg.
 *
 * psycopg2 is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link this program with the OpenSSL library (or with
 * modified versions of OpenSSL that use the same license as OpenSSL),
 * and distribute linked combinations including the two.
 *
 * You must obey the GNU Lesser General Public License in all respects for
 * all of the code used other than OpenSSL.
 *
 * psycopg2 is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 */

#define MAX_DIMENSIONS 16

/** typecast_array_cleanup - remove the horrible [...]= stuff **/

static int
typecast_array_cleanup(const char **str, Py_ssize_t *len)
{
    Py_ssize_t i, depth = 1;

    if ((*str)[0] != '[') return -1;

    for (i=1 ; depth > 0 && i < *len ; i++) {
        if ((*str)[i] == '[')
            depth += 1;
        else if ((*str)[i] == ']')
            depth -= 1;
    }
    if ((*str)[i] != '=') return -1;

    *str = &((*str)[i+1]);
    *len = *len - i - 1;
    return 0;
}

/** typecast_array_scan - scan a string looking for array items **/

#define ASCAN_ERROR -1
#define ASCAN_EOF    0
#define ASCAN_BEGIN  1
#define ASCAN_END    2
#define ASCAN_TOKEN  3
#define ASCAN_QUOTED 4

static int
typecast_array_tokenize(const char *str, Py_ssize_t strlength,
                        Py_ssize_t *pos, char** token,
                        Py_ssize_t *length, int *quotes)
{
    /* FORTRAN glory */
    Py_ssize_t i, l;
    int q, b, res;

    Dprintf("typecast_array_tokenize: '%s', "
            FORMAT_CODE_PY_SSIZE_T "/" FORMAT_CODE_PY_SSIZE_T,
            &str[*pos], *pos, strlength);

    /* we always get called with pos pointing at the start of a token, so a
       fast check is enough for ASCAN_EOF, ASCAN_BEGIN and ASCAN_END */
    if (*pos == strlength) {
        return ASCAN_EOF;
    }
    else if (str[*pos] == '{') {
        *pos += 1;
        return ASCAN_BEGIN;
    }
    else if (str[*pos] == '}') {
        *pos += 1;
        if (str[*pos] == ',')
            *pos += 1;
        return ASCAN_END;
    }

    /* now we start looking for the first unquoted ',' or '}', the only two
       tokens that can limit an array element */
    q = 0; /* if q is odd we're inside quotes */
    b = 0; /* if b is 1 we just encountered a backslash */
    res = ASCAN_TOKEN;

    for (i = *pos ; i < strlength ; i++) {
        switch (str[i]) {
        case '"':
            if (b == 0)
                q += 1;
            else
                b = 0;
            break;

        case '\\':
            res = ASCAN_QUOTED;
            if (b == 0)
                b = 1;
            else
                /* we're backslashing a backslash */
                b = 0;
            break;

        case '}':
        case ',':
            if (b == 0 && ((q&1) == 0))
                goto tokenize;
            break;

        default:
            /* reset the backslash counter */
            b = 0;
            break;
        }
    }

 tokenize:
    /* remove initial quoting character and calculate raw length */
    *quotes = 0;
    l = i - *pos;
    if (str[*pos] == '"') {
        *pos += 1;
        l -= 2;
        *quotes = 1;
    }

    if (res == ASCAN_QUOTED) {
        const char *j, *jj;
        char *buffer = PyMem_Malloc(l+1);
        if (buffer == NULL) {
            PyErr_NoMemory();
            return ASCAN_ERROR;
        }

        *token = buffer;

        for (j = str + *pos, jj = j + l; j < jj; ++j) {
            if (*j == '\\') { ++j; }
            *(buffer++) = *j;
        }

        *buffer = '\0';
        /* The variable that was used to indicate the size of buffer is of type
         * Py_ssize_t, so a subsegment of buffer couldn't possibly exceed
         * PY_SSIZE_T_MAX: */
        *length = (Py_ssize_t) (buffer - *token);
    }
    else {
        *token = (char *)&str[*pos];
        *length = l;
    }

    *pos = i;

    /* skip the comma and set position to the start of next token */
    if (str[i] == ',') *pos += 1;

    return res;
}

RAISES_NEG static int
typecast_array_scan(const char *str, Py_ssize_t strlength,
                    PyObject *curs, PyObject *base, PyObject *array)
{
    int state, quotes = 0;
    Py_ssize_t length = 0, pos = 0;
    char *token;

    PyObject *stack[MAX_DIMENSIONS];
    size_t stack_index = 0;

    while (1) {
        token = NULL;
        state = typecast_array_tokenize(str, strlength,
                                    &pos, &token, &length, &quotes);
        Dprintf("typecast_array_scan: state = %d,"
                " length = " FORMAT_CODE_PY_SSIZE_T ", token = '%s'",
                state, length, token);
        if (state == ASCAN_TOKEN || state == ASCAN_QUOTED) {
            PyObject *obj;
            if (!quotes && length == 4
                && (token[0] == 'n' || token[0] == 'N')
                && (token[1] == 'u' || token[1] == 'U')
                && (token[2] == 'l' || token[2] == 'L')
                && (token[3] == 'l' || token[3] == 'L'))
            {
                obj = typecast_cast(base, NULL, 0, curs);
            } else {
                obj = typecast_cast(base, token, length, curs);
            }

            /* before anything else we free the memory */
            if (state == ASCAN_QUOTED) PyMem_Free(token);
            if (obj == NULL) return -1;

            PyList_Append(array, obj);
            Py_DECREF(obj);
        }

        else if (state == ASCAN_BEGIN) {
            PyObject *sub = PyList_New(0);
            if (sub == NULL) return -1;

            PyList_Append(array, sub);
            Py_DECREF(sub);

            if (stack_index == MAX_DIMENSIONS) {
                PyErr_SetString(DataError, "excessive array dimensions");
                return -1;
            }

            stack[stack_index++] = array;
            array = sub;
        }

        else if (state == ASCAN_ERROR) {
            return -1;
        }

        else if (state == ASCAN_END) {
            if (stack_index == 0) {
                PyErr_SetString(DataError, "unbalanced braces in array");
                return -1;
            }
            array = stack[--stack_index];
        }

        else if (state ==  ASCAN_EOF)
            break;
    }

    return 0;
}


/** GENERIC - a generic typecaster that can be used when no special actions
    have to be taken on the single items **/

static PyObject *
typecast_GENERIC_ARRAY_cast(const char *str, Py_ssize_t len, PyObject *curs)
{
    PyObject *obj = NULL;
    PyObject *base = ((typecastObject*)((cursorObject*)curs)->caster)->bcast;

    Dprintf("typecast_GENERIC_ARRAY_cast: str = '%s',"
            " len = " FORMAT_CODE_PY_SSIZE_T, str, len);

    if (str == NULL) { Py_RETURN_NONE; }
    if (str[0] == '[')
        typecast_array_cleanup(&str, &len);
    if (str[0] != '{') {
        PyErr_SetString(DataError, "array does not start with '{'");
        return NULL;
    }
    if (str[1] == '\0') {
        PyErr_SetString(DataError, "malformed array: '{'");
        return NULL;
    }

    Dprintf("typecast_GENERIC_ARRAY_cast: str = '%s',"
            " len = " FORMAT_CODE_PY_SSIZE_T, str, len);

    if (!(obj = PyList_New(0))) { return NULL; }

    /* scan the array skipping the first level of {} */
    if (typecast_array_scan(&str[1], len-2, curs, base, obj) < 0) {
        Py_CLEAR(obj);
    }

    return obj;
}

/** almost all the basic array typecasters are derived from GENERIC **/

#define typecast_LONGINTEGERARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_INTEGERARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_FLOATARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_DECIMALARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_STRINGARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_UNICODEARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_BYTESARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_BOOLEANARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_DATETIMEARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_DATETIMETZARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_DATEARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_TIMEARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_INTERVALARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_BINARYARRAY_cast typecast_GENERIC_ARRAY_cast
#define typecast_ROWIDARRAY_cast typecast_GENERIC_ARRAY_cast
