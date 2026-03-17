
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
#if PY_MAJOR_VERSION < 3
    if (enc.optenc == OPTENC_RAW && PyString_Check(src))
    {
        psz = (SQLWCHAR*)PyString_AS_STRING(src);
        return;
    }
#endif

#if PY_MAJOR_VERSION >= 3
    if (enc.optenc == OPTENC_UTF8 && PyUnicode_Check(src))
    {
        psz = (SQLWCHAR*)PyUnicode_AsUTF8(src);
        return;
    }
#endif

    PyObject* pb = 0;

#if PY_MAJOR_VERSION == 2
    if (PyBytes_Check(src))
    {
      // If this is Python 2, the string could already be encoded as bytes.  If the encoding is
      // different than what we want, we have to decode to Unicode and then re-encode.


      PyObject* u = PyString_AsDecodedObject(src, 0, "strict");
      if (u)
        src = u;
    }
#endif

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
#if PY_MAJOR_VERSION < 3
    if (optenc == OPTENC_RAW || PyBytes_Size(obj) == 0)
    {
        Py_INCREF(obj);
        return obj;
    }
#endif

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


#if PY_MAJOR_VERSION < 3
PyObject* EncodeStr(PyObject* str, const TextEnc& enc)
{
    if (enc.optenc == OPTENC_RAW || PyBytes_Size(str) == 0)
    {
        // No conversion.
        Py_INCREF(str);
        return str;
    }
    else
    {
        // Encode the text with the user's encoding.
        Object encoded(PyCodec_Encode(str, enc.name, "strict"));
        if (!encoded)
            return 0;

        if (!PyBytes_CheckExact(encoded))
        {
            // Not all encodings return bytes.
            PyErr_Format(PyExc_TypeError, "Unicode read encoding '%s' returned unexpected data type: %s",
                         enc.name, encoded.Get()->ob_type->tp_name);
            return 0;
        }

        return encoded.Detach();
    }
}
#endif

PyObject* TextBufferToObject(const TextEnc& enc, const byte* pbData, Py_ssize_t cbData)
{
    // cbData
    //   The length of data in bytes (cb == 'count of bytes').

    // NB: In each branch we make a check for a zero length string and handle it specially
    // since PyUnicode_Decode may (will?) fail if we pass a zero-length string.  Issue #172
    // first pointed this out with shift_jis.  I'm not sure if it is a fault in the
    // implementation of this codec or if others will have it also.

    PyObject* str;

#if PY_MAJOR_VERSION < 3
    // The Unicode paths use the same code.
    if (enc.to == TO_UNICODE)
    {
#endif
        if (cbData == 0)
        {
            str = PyUnicode_FromStringAndSize("", 0);
        }
        else
        {
            int byteorder = 0;
            switch (enc.optenc)
            {
            case OPTENC_UTF8:
                str = PyUnicode_DecodeUTF8((char*)pbData, cbData, "strict");
                break;
            case OPTENC_UTF16:
                byteorder = BYTEORDER_NATIVE;
                str = PyUnicode_DecodeUTF16((char*)pbData, cbData, "strict", &byteorder);
                break;
            case OPTENC_UTF16LE:
                byteorder = BYTEORDER_LE;
                str = PyUnicode_DecodeUTF16((char*)pbData, cbData, "strict", &byteorder);
                break;
            case OPTENC_UTF16BE:
                byteorder = BYTEORDER_BE;
                str = PyUnicode_DecodeUTF16((char*)pbData, cbData, "strict", &byteorder);
                break;
            case OPTENC_LATIN1:
                str = PyUnicode_DecodeLatin1((char*)pbData, cbData, "strict");
                break;
            default:
                // The user set an encoding by name.
                str = PyUnicode_Decode((char*)pbData, cbData, enc.name, "strict");
                break;
            }
        }
#if PY_MAJOR_VERSION < 3
    }
    else if (cbData == 0)
    {
        str = PyString_FromStringAndSize("", 0);
    }
    else if (enc.optenc == OPTENC_RAW)
    {
        // No conversion.
        str = PyString_FromStringAndSize((char*)pbData, cbData);
    }
    else
    {
        // The user has requested a string object.  Unfortunately we don't have
        // str versions of all of the optimized functions.
        const char* encoding;
        switch (enc.optenc)
        {
        case OPTENC_UTF8:
            encoding = "utf-8";
            break;
        case OPTENC_UTF16:
            encoding = "utf-16";
            break;
        case OPTENC_UTF16LE:
            encoding = "utf-16-le";
            break;
        case OPTENC_UTF16BE:
            encoding = "utf-16-be";
            break;
        case OPTENC_LATIN1:
            encoding = "latin-1";
            break;
        default:
            encoding = enc.name;
        }

        str = PyString_Decode((char*)pbData, cbData, encoding, "strict");
    }
#endif

    return str;
}
