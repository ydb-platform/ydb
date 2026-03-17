
#include "pyodbc.h"
#include "wrapper.h"
#include "textenc.h"

void SQLWChar::init(PyObject* src, const TextEnc& enc)
{
    // Initialization code common to all of the constructors.
    //
    // Convert `src` to SQLWCHAR.

    static PyObject* nulls = NULL;

    if (src == 0 || src == Py_None)
    {
        psz = 0;
        isNone = true;
        return;
    }

    isNone = false;

    // If there are optimized encodings that don't require a temporary object, use them.
    if (enc.optenc == OPTENC_UTF8 && PyUnicode_Check(src))
    {
        psz = (SQLWCHAR*)PyUnicode_AsUTF8(src);
        return;
    }

    PyObject* pb = 0;

    if (!pb && PyUnicode_Check(src))
        pb = PyUnicode_AsEncodedString(src, enc.name, "strict");

    if (pb)
    {
        // Careful: Some encodings don't return bytes.
        if (!PyBytes_Check(pb))
        {
            // REVIEW: Error or just return null?
            psz = 0;
            Py_DECREF(pb);
            return;
        }
        
        if(!nulls)
            nulls = PyBytes_FromStringAndSize("\0\0\0\0", 4);

        PyBytes_Concat(&pb, nulls);
        if (!pb)
        {
            psz = 0;
            return;
        }
    } else {
        // If the encoding failed (possibly due to "strict"), it will generate an exception, but
        // we're going to continue.
        PyErr_Clear();
        psz = 0;
    }

    if (pb) {
        bytes.Attach(pb);
        psz = (SQLWCHAR*)PyBytes_AS_STRING(pb);
    }
}


PyObject* TextEnc::Encode(PyObject* obj) const
{
    PyObject* bytes = PyCodec_Encode(obj, name, "strict");

    if (bytes && PyErr_Occurred())
    {
        // REVIEW: Issue #206.  I am not sure what is going on here, but PyCodec_Encode
        // sometimes returns bytes but *also* sets an exception saying "'ascii' codec can't
        // encode characters...".  I assume the ascii is from my sys encoding, but it seems to
        // be a superfluous error.  Since Cursor.fetchall() looks for exceptions this extraneous
        // error causes us to throw an exception.
        //
        // I'm putting in a work around but we should track down the root cause and report it
        // to the Python project if it is not ours.

        PyErr_Clear();
    }

    return bytes;
}



PyObject* TextBufferToObject(const TextEnc& enc, const byte* pbData, Py_ssize_t cbData)
{
    // cbData
    //   The length of data in bytes (cb == 'count of bytes').

    // NB: In each branch we make a check for a zero length string and handle it specially
    // since PyUnicode_Decode may (will?) fail if we pass a zero-length string.  Issue #172
    // first pointed this out with shift_jis.  I'm not sure if it is a fault in the
    // implementation of this codec or if others will have it also.

    //  PyObject* str;

    if (cbData == 0)
        return PyUnicode_FromStringAndSize("", 0);

    switch (enc.optenc)
    {
        case OPTENC_UTF8:
            return PyUnicode_DecodeUTF8((char*)pbData, cbData, "strict");

        case OPTENC_UTF16: {
            int byteorder = BYTEORDER_NATIVE;
            return PyUnicode_DecodeUTF16((char*)pbData, cbData, "strict", &byteorder);
        }

        case OPTENC_UTF16LE: {
            int byteorder = BYTEORDER_LE;
            return PyUnicode_DecodeUTF16((char*)pbData, cbData, "strict", &byteorder);
        }

        case OPTENC_UTF16BE: {
            int byteorder = BYTEORDER_BE;
            return PyUnicode_DecodeUTF16((char*)pbData, cbData, "strict", &byteorder);
        }

        case OPTENC_LATIN1:
            return PyUnicode_DecodeLatin1((char*)pbData, cbData, "strict");
    }

    // The user set an encoding by name.
    return PyUnicode_Decode((char*)pbData, cbData, enc.name, "strict");
}
