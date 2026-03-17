#ifndef _TEXTENC_H
#define _TEXTENC_H

enum {
    BYTEORDER_LE = -1,
    BYTEORDER_NATIVE = 0,
    BYTEORDER_BE = 1,

    OPTENC_NONE    = 0,         // No optimized encoding - use the named encoding
    OPTENC_RAW     = 1,         // In Python 2, pass bytes directly to string - no decoder
    OPTENC_UTF8    = 2,
    OPTENC_UTF16   = 3,         // "Native", so check for BOM and default to BE
    OPTENC_UTF16BE = 4,
    OPTENC_UTF16LE = 5,
    OPTENC_LATIN1  = 6,
    OPTENC_UTF32   = 7,
    OPTENC_UTF32LE = 8,
    OPTENC_UTF32BE = 9,

#if PY_MAJOR_VERSION < 3
    TO_UNICODE = 1,
    TO_STR     = 2
#endif
};

#ifdef WORDS_BIGENDIAN
# define OPTENC_UTF16NE OPTENC_UTF16BE
# define ENCSTR_UTF16NE "utf-16be"
#else
# define OPTENC_UTF16NE OPTENC_UTF16LE
# define ENCSTR_UTF16NE "utf-16le"
#endif

typedef unsigned short ODBCCHAR;
// I'm not sure why, but unixODBC seems to define SQLWCHAR as wchar_t even with
// the size is incorrect.  So we might get 4-byte SQLWCHAR on 64-bit Linux even
// though it requires 2-byte characters.  We have to define our own type to
// operate on.

enum {
    ODBCCHAR_SIZE = 2
};

struct TextEnc
{
    // Holds encoding information for reading or writing text.  Since some drivers / databases
    // are not easy to configure efficiently, a separate instance of this structure is
    // configured for:
    //
    // * reading SQL_CHAR
    // * reading SQL_WCHAR
    // * writing unicode strings
    // * writing non-unicode strings (Python 2.7 only)

#if PY_MAJOR_VERSION < 3
    int to;
    // The type of object to return if reading from the database: str or unicode.
#endif

    int optenc;
    // Set to one of the OPTENC constants to indicate whether an optimized encoding is to be
    // used or a custom one.  If OPTENC_NONE, no optimized encoding is set and `name` should be
    // used.

    const char* name;
    // The name of the encoding.  This must be freed using `free`.

    SQLSMALLINT ctype;
    // The C type to use, SQL_C_CHAR or SQL_C_WCHAR.  Normally this matches the SQL type of the
    // column (SQL_C_CHAR is used for SQL_CHAR, etc.).  At least one database reports it has
    // SQL_WCHAR data even when configured for UTF-8 which is better suited for SQL_C_CHAR.

    PyObject* Encode(PyObject*) const;
    // Given a string (unicode or str for 2.7), return a bytes object encoded.  This is used
    // for encoding a Python object for passing to a function expecting SQLCHAR* or SQLWCHAR*.
};


struct SQLWChar
{
    // Encodes a Python string to a SQLWCHAR pointer.  This should eventually replace the
    // SQLWchar structure.
    //
    // Note: This does *not* increment the refcount!

    // IMPORTANT: I've made the conscious decision *not* to determine the character count.  If
    // we only had to follow the ODBC specification, it would simply be the number of
    // characters in the string and would be the bytelen / 2.  The problem is drivers that
    // don't follow the specification and expect things like UTF-8.  What length do these
    // drivers expect?  Very, very likely they want the number of *bytes*, not the actual
    // number of characters.  I'm simply going to null terminate and pass SQL_NTS.
    //
    // This is a performance penalty when using utf16 since we have to copy the string just to
    // add the null terminator bytes, but we don't use it very often.  If this becomes a
    // bottleneck, we'll have to revisit this design.

    SQLWCHAR* psz;
    bool isNone;

    Object bytes;
    // A temporary object holding the decoded bytes if we can't use a pointer into the original
    // object.

    SQLWChar(PyObject* src, const char* szEncoding)
    {
        TextEnc enc;
        enc.name = szEncoding;
        enc.ctype = SQL_C_WCHAR;
        enc.optenc = (strcmp(szEncoding, "raw") == 0) ? OPTENC_RAW : OPTENC_NONE;
        init(src, enc);
    }

    SQLWChar(PyObject* src, const TextEnc* penc)
    {
        init(src, *penc);
    }

    SQLWChar(PyObject* src, const TextEnc& enc)
    {
        init(src, enc);
    }

    bool isValidOrNone()
    {
        // Returns true if this object is a valid string *or* None.
        return isNone || (psz != 0);
    }

    bool isValid()
    {
        return psz != 0;
    }

private:
    void init(PyObject* src, const TextEnc& enc);

    SQLWChar(const SQLWChar&) {}
    void operator=(const SQLWChar&) {}
};


PyObject* TextBufferToObject(const TextEnc& enc, const byte* p, Py_ssize_t len);
// Convert a text buffer to a Python object using the given encoding.
//
// The buffer can be a SQLCHAR array or SQLWCHAR array.  The text encoding
// should match it.

#endif // _TEXTENC_H
