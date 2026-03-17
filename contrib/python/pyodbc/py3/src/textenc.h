#ifndef _TEXTENC_H
#define _TEXTENC_H

enum {
    BYTEORDER_LE = -1,
    BYTEORDER_NATIVE = 0,
    BYTEORDER_BE = 1,

    OPTENC_NONE    = 0,         // No optimized encoding - use the named encoding
    OPTENC_UTF8    = 1,
    OPTENC_UTF16   = 2,         // "Native", so check for BOM and default to BE
    OPTENC_UTF16BE = 3,
    OPTENC_UTF16LE = 4,
    OPTENC_LATIN1  = 5,
    OPTENC_UTF32   = 6,
    OPTENC_UTF32LE = 7,
    OPTENC_UTF32BE = 8,
};

#ifdef WORDS_BIGENDIAN
# define OPTENC_UTF16NE OPTENC_UTF16BE
# define ENCSTR_UTF16NE "utf-16be"
#else
# define OPTENC_UTF16NE OPTENC_UTF16LE
# define ENCSTR_UTF16NE "utf-16le"
#endif

struct TextEnc
{
    // Holds encoding information for reading or writing text.  Since some drivers / databases
    // are not easy to configure efficiently, a separate instance of this structure is
    // configured for:
    //
    // * reading SQL_CHAR
    // * reading SQL_WCHAR
    // * writing unicode strings
    // * reading metadata like column names
    //
    // I would have expected the metadata to follow the SQLCHAR / SQLWCHAR based on whether the
    // ANSI or wide API was called, but it does not.

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
    // Given a string, return a bytes object encoded.  This is used for encoding a Python
    // object for passing to a function expecting SQLCHAR* or SQLWCHAR*.
};


class SQLWChar
{
    // A convenience object that encodes a Unicode string to a given encoding.  It can be cast
    // to a SQLWCHAR* to return the pointer.
    //
    // This is designed to be created on the stack, perform the conversion, and cleanup any
    // temporary objects in the destructor.
    //
    // The SQLWCHAR pointer is *only* valid during the lifetime of this object.  It may point
    // into a temporary `bytes` object that is deleted by the constructor.

public:
    SQLWChar()
    {
        psz = 0;
        isNone = true;
    }

    SQLWChar(PyObject* src, const char* szEncoding)
    {
        psz = 0;
        isNone = true;
        set(src, szEncoding);
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

    void set(PyObject* src, const char* szEncoding) {
        bytes.Attach(0);  // free old, if any
        psz = 0;
        isNone = true;

        TextEnc enc;
        enc.name = szEncoding;
        enc.ctype = SQL_C_WCHAR;
        enc.optenc = OPTENC_NONE;
        init(src, enc);
    }

    SQLWCHAR* get() { return psz; }

    operator SQLWCHAR*() { return psz; }

private:
    SQLWCHAR* psz;
    bool isNone;

    Object bytes;
    // A temporary object holding the decoded bytes if we can't use a pointer into the original
    // object.

    void init(PyObject* src, const TextEnc& enc);

    SQLWChar(const SQLWChar&) {}
    void operator=(const SQLWChar&) {}
};


PyObject* TextBufferToObject(const TextEnc& enc, const byte* p, Py_ssize_t len);
// Convert a text buffer to a Python object using the given encoding.
//
// - pbData :: The buffer, which is an array of SQLCHAR or SQLWCHAR.  We treat it as bytes here
//   since the encoding `enc` tells us how to treat it.
// - cbData :: The length of `pbData` in *bytes*.

#endif // _TEXTENC_H
