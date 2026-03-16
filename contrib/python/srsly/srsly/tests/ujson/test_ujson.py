import decimal
import json
import math
import sys
import unittest
import pytest
from io import StringIO
from pathlib import Path
from srsly import ujson


json_unicode = json.dumps


class UltraJSONTests(unittest.TestCase):
    def test_encodeDecimal(self):
        sut = decimal.Decimal("1337.1337")
        encoded = ujson.encode(sut)
        decoded = ujson.decode(encoded)
        self.assertEqual(decoded, 1337.1337)

    def test_encodeStringConversion(self):
        input = "A string \\ / \b \f \n \r \t </script> &"
        not_html_encoded = '"A string \\\\ \\/ \\b \\f \\n \\r \\t <\\/script> &"'
        html_encoded = (
            '"A string \\\\ \\/ \\b \\f \\n \\r \\t \\u003c\\/script\\u003e \\u0026"'
        )
        not_slashes_escaped = '"A string \\\\ / \\b \\f \\n \\r \\t </script> &"'

        def helper(expected_output, **encode_kwargs):
            output = ujson.encode(input, **encode_kwargs)
            self.assertEqual(output, expected_output)
            if encode_kwargs.get("escape_forward_slashes", True):
                self.assertEqual(input, json.loads(output))
                self.assertEqual(input, ujson.decode(output))

        # Default behavior assumes encode_html_chars=False.
        helper(not_html_encoded, ensure_ascii=True)
        helper(not_html_encoded, ensure_ascii=False)

        # Make sure explicit encode_html_chars=False works.
        helper(not_html_encoded, ensure_ascii=True, encode_html_chars=False)
        helper(not_html_encoded, ensure_ascii=False, encode_html_chars=False)

        # Make sure explicit encode_html_chars=True does the encoding.
        helper(html_encoded, ensure_ascii=True, encode_html_chars=True)
        helper(html_encoded, ensure_ascii=False, encode_html_chars=True)

        # Do escape forward slashes if disabled.
        helper(not_slashes_escaped, escape_forward_slashes=False)

    def testWriteEscapedString(self):
        self.assertEqual(
            "\"\\u003cimg src='\\u0026amp;'\\/\\u003e\"",
            ujson.dumps("<img src='&amp;'/>", encode_html_chars=True),
        )

    def test_doubleLongIssue(self):
        sut = {"a": -4342969734183514}
        encoded = json.dumps(sut)
        decoded = json.loads(encoded)
        self.assertEqual(sut, decoded)
        encoded = ujson.encode(sut)
        decoded = ujson.decode(encoded)
        self.assertEqual(sut, decoded)

    def test_doubleLongDecimalIssue(self):
        sut = {"a": -12345678901234.56789012}
        encoded = json.dumps(sut)
        decoded = json.loads(encoded)
        self.assertEqual(sut, decoded)
        encoded = ujson.encode(sut)
        decoded = ujson.decode(encoded)
        self.assertEqual(sut, decoded)

    def test_encodeDecodeLongDecimal(self):
        sut = {"a": -528656961.4399388}
        encoded = ujson.dumps(sut)
        ujson.decode(encoded)

    def test_decimalDecodeTest(self):
        sut = {"a": 4.56}
        encoded = ujson.encode(sut)
        decoded = ujson.decode(encoded)
        self.assertAlmostEqual(sut[u"a"], decoded[u"a"])

    def test_encodeDictWithUnicodeKeys(self):
        input = {
            "key1": "value1",
            "key1": "value1",
            "key1": "value1",
            "key1": "value1",
            "key1": "value1",
            "key1": "value1",
        }
        ujson.encode(input)

        input = {
            "بن": "value1",
            "بن": "value1",
            "بن": "value1",
            "بن": "value1",
            "بن": "value1",
            "بن": "value1",
            "بن": "value1",
        }
        ujson.encode(input)

    def test_encodeDoubleConversion(self):
        input = math.pi
        output = ujson.encode(input)
        self.assertEqual(round(input, 5), round(json.loads(output), 5))
        self.assertEqual(round(input, 5), round(ujson.decode(output), 5))

    def test_encodeWithDecimal(self):
        input = 1.0
        output = ujson.encode(input)
        self.assertEqual(output, "1.0")

    def test_encodeDoubleNegConversion(self):
        input = -math.pi
        output = ujson.encode(input)

        self.assertEqual(round(input, 5), round(json.loads(output), 5))
        self.assertEqual(round(input, 5), round(ujson.decode(output), 5))

    def test_encodeArrayOfNestedArrays(self):
        input = [[[[]]]] * 20
        output = ujson.encode(input)
        self.assertEqual(input, json.loads(output))
        # self.assertEqual(output, json.dumps(input))
        self.assertEqual(input, ujson.decode(output))

    def test_encodeArrayOfDoubles(self):
        input = [31337.31337, 31337.31337, 31337.31337, 31337.31337] * 10
        output = ujson.encode(input)
        self.assertEqual(input, json.loads(output))
        # self.assertEqual(output, json.dumps(input))
        self.assertEqual(input, ujson.decode(output))

    def test_encodeStringConversion2(self):
        input = "A string \\ / \b \f \n \r \t"
        output = ujson.encode(input)
        self.assertEqual(input, json.loads(output))
        self.assertEqual(output, '"A string \\\\ \\/ \\b \\f \\n \\r \\t"')
        self.assertEqual(input, ujson.decode(output))

    def test_decodeUnicodeConversion(self):
        pass

    def test_encodeUnicodeConversion1(self):
        input = "Räksmörgås اسامة بن محمد بن عوض بن لادن"
        enc = ujson.encode(input)
        dec = ujson.decode(enc)
        self.assertEqual(enc, json_unicode(input))
        self.assertEqual(dec, json.loads(enc))

    def test_encodeControlEscaping(self):
        input = "\x19"
        enc = ujson.encode(input)
        dec = ujson.decode(enc)
        self.assertEqual(input, dec)
        self.assertEqual(enc, json_unicode(input))

    def test_encodeUnicodeConversion2(self):
        input = "\xe6\x97\xa5\xd1\x88"
        enc = ujson.encode(input)
        dec = ujson.decode(enc)
        self.assertEqual(enc, json_unicode(input))
        self.assertEqual(dec, json.loads(enc))

    def test_encodeUnicodeSurrogatePair(self):
        input = "\xf0\x90\x8d\x86"
        enc = ujson.encode(input)
        dec = ujson.decode(enc)

        self.assertEqual(enc, json_unicode(input))
        self.assertEqual(dec, json.loads(enc))

    def test_encodeUnicode4BytesUTF8(self):
        input = "\xf0\x91\x80\xb0TRAILINGNORMAL"
        enc = ujson.encode(input)
        dec = ujson.decode(enc)

        self.assertEqual(enc, json_unicode(input))
        self.assertEqual(dec, json.loads(enc))

    def test_encodeUnicode4BytesUTF8Highest(self):
        input = "\xf3\xbf\xbf\xbfTRAILINGNORMAL"
        enc = ujson.encode(input)
        dec = ujson.decode(enc)

        self.assertEqual(enc, json_unicode(input))
        self.assertEqual(dec, json.loads(enc))

    # Characters outside of Basic Multilingual Plane(larger than
    # 16 bits) are represented as \UXXXXXXXX in python but should be encoded
    # as \uXXXX\uXXXX in json.
    def testEncodeUnicodeBMP(self):
        s = "\U0001f42e\U0001f42e\U0001F42D\U0001F42D"  # 🐮🐮🐭🐭
        encoded = ujson.dumps(s)
        encoded_json = json.dumps(s)

        if len(s) == 4:
            self.assertEqual(len(encoded), len(s) * 12 + 2)
        else:
            self.assertEqual(len(encoded), len(s) * 6 + 2)

        self.assertEqual(encoded, encoded_json)
        decoded = ujson.loads(encoded)
        self.assertEqual(s, decoded)

        # ujson outputs an UTF-8 encoded str object
        encoded = ujson.dumps(s, ensure_ascii=False)
        # json outputs an unicode object
        encoded_json = json.dumps(s, ensure_ascii=False)
        self.assertEqual(len(encoded), len(s) + 2)  # original length + quotes
        self.assertEqual(encoded, encoded_json)
        decoded = ujson.loads(encoded)
        self.assertEqual(s, decoded)

    def testEncodeSymbols(self):
        s = "\u273f\u2661\u273f"  # ✿♡✿
        encoded = ujson.dumps(s)
        encoded_json = json.dumps(s)
        self.assertEqual(len(encoded), len(s) * 6 + 2)  # 6 characters + quotes
        self.assertEqual(encoded, encoded_json)
        decoded = ujson.loads(encoded)
        self.assertEqual(s, decoded)

        # ujson outputs an UTF-8 encoded str object
        encoded = ujson.dumps(s, ensure_ascii=False)
        # json outputs an unicode object
        encoded_json = json.dumps(s, ensure_ascii=False)
        self.assertEqual(len(encoded), len(s) + 2)  # original length + quotes
        self.assertEqual(encoded, encoded_json)
        decoded = ujson.loads(encoded)
        self.assertEqual(s, decoded)

    def test_encodeArrayInArray(self):
        input = [[[[]]]]
        output = ujson.encode(input)

        self.assertEqual(input, json.loads(output))
        self.assertEqual(output, json.dumps(input))
        self.assertEqual(input, ujson.decode(output))

    def test_encodeIntConversion(self):
        input = 31337
        output = ujson.encode(input)
        self.assertEqual(input, json.loads(output))
        self.assertEqual(output, json.dumps(input))
        self.assertEqual(input, ujson.decode(output))

    def test_encodeIntNegConversion(self):
        input = -31337
        output = ujson.encode(input)
        self.assertEqual(input, json.loads(output))
        self.assertEqual(output, json.dumps(input))
        self.assertEqual(input, ujson.decode(output))

    def test_encodeLongNegConversion(self):
        input = -9223372036854775808
        output = ujson.encode(input)

        json.loads(output)
        ujson.decode(output)

        self.assertEqual(input, json.loads(output))
        self.assertEqual(output, json.dumps(input))
        self.assertEqual(input, ujson.decode(output))

    def test_encodeListConversion(self):
        input = [1, 2, 3, 4]
        output = ujson.encode(input)
        self.assertEqual(input, json.loads(output))
        self.assertEqual(input, ujson.decode(output))

    def test_encodeDictConversion(self):
        input = {"k1": 1, "k2": 2, "k3": 3, "k4": 4}
        output = ujson.encode(input)
        self.assertEqual(input, json.loads(output))
        self.assertEqual(input, ujson.decode(output))
        self.assertEqual(input, ujson.decode(output))

    def test_encodeNoneConversion(self):
        input = None
        output = ujson.encode(input)
        self.assertEqual(input, json.loads(output))
        self.assertEqual(output, json.dumps(input))
        self.assertEqual(input, ujson.decode(output))

    def test_encodeTrueConversion(self):
        input = True
        output = ujson.encode(input)
        self.assertEqual(input, json.loads(output))
        self.assertEqual(output, json.dumps(input))
        self.assertEqual(input, ujson.decode(output))

    def test_encodeFalseConversion(self):
        input = False
        output = ujson.encode(input)
        self.assertEqual(input, json.loads(output))
        self.assertEqual(output, json.dumps(input))
        self.assertEqual(input, ujson.decode(output))

    def test_encodeToUTF8(self):
        input = b"\xe6\x97\xa5\xd1\x88"
        input = input.decode("utf-8")
        enc = ujson.encode(input, ensure_ascii=False)
        dec = ujson.decode(enc)
        self.assertEqual(enc, json.dumps(input, ensure_ascii=False))
        self.assertEqual(dec, json.loads(enc))

    def test_decodeFromUnicode(self):
        input = '{"obj": 31337}'
        dec1 = ujson.decode(input)
        dec2 = ujson.decode(str(input))
        self.assertEqual(dec1, dec2)

    def test_encodeRecursionMax(self):
        # 8 is the max recursion depth
        class O2:
            member = 0

            def toDict(self):
                return {"member": self.member}

        class O1:
            member = 0

            def toDict(self):
                return {"member": self.member}

        input = O1()
        input.member = O2()
        input.member.member = input
        self.assertRaises(OverflowError, ujson.encode, input)

    def test_encodeDoubleNan(self):
        input = float("nan")
        self.assertRaises(OverflowError, ujson.encode, input)

    def test_encodeDoubleInf(self):
        input = float("inf")
        self.assertRaises(OverflowError, ujson.encode, input)

    def test_encodeDoubleNegInf(self):
        input = -float("inf")
        self.assertRaises(OverflowError, ujson.encode, input)

    def test_encodeOrderedDict(self):
        from collections import OrderedDict

        input = OrderedDict([(1, 1), (0, 0), (8, 8), (2, 2)])
        self.assertEqual('{"1":1,"0":0,"8":8,"2":2}', ujson.encode(input))

    def test_decodeJibberish(self):
        input = "fdsa sda v9sa fdsa"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeBrokenArrayStart(self):
        input = "["
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeBrokenObjectStart(self):
        input = "{"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeBrokenArrayEnd(self):
        input = "]"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeArrayDepthTooBig(self):
        input = "[" * (1024 * 1024)
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeBrokenObjectEnd(self):
        input = "}"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeObjectTrailingCommaFail(self):
        input = '{"one":1,}'
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeObjectDepthTooBig(self):
        input = "{" * (1024 * 1024)
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeStringUnterminated(self):
        input = '"TESTING'
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeStringUntermEscapeSequence(self):
        input = '"TESTING\\"'
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeStringBadEscape(self):
        input = '"TESTING\\"'
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeTrueBroken(self):
        input = "tru"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeFalseBroken(self):
        input = "fa"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeNullBroken(self):
        input = "n"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeBrokenDictKeyTypeLeakTest(self):
        input = '{{1337:""}}'
        for x in range(1000):
            self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeBrokenDictLeakTest(self):
        input = '{{"key":"}'
        for x in range(1000):
            self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeBrokenListLeakTest(self):
        input = "[[[true"
        for x in range(1000):
            self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeDictWithNoKey(self):
        input = "{{{{31337}}}}"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeDictWithNoColonOrValue(self):
        input = '{{{{"key"}}}}'
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeDictWithNoValue(self):
        input = '{{{{"key":}}}}'
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeNumericIntPos(self):
        input = "31337"
        self.assertEqual(31337, ujson.decode(input))

    def test_decodeNumericIntNeg(self):
        input = "-31337"
        self.assertEqual(-31337, ujson.decode(input))

    def test_encodeUnicode4BytesUTF8Fail(self):
        input = b"\xfd\xbf\xbf\xbf\xbf\xbf"
        self.assertRaises(OverflowError, ujson.encode, input)

    def test_encodeNullCharacter(self):
        input = "31337 \x00 1337"
        output = ujson.encode(input)
        self.assertEqual(input, json.loads(output))
        self.assertEqual(output, json.dumps(input))
        self.assertEqual(input, ujson.decode(output))

        input = "\x00"
        output = ujson.encode(input)
        self.assertEqual(input, json.loads(output))
        self.assertEqual(output, json.dumps(input))
        self.assertEqual(input, ujson.decode(output))

        self.assertEqual('"  \\u0000\\r\\n "', ujson.dumps("  \u0000\r\n "))

    def test_decodeNullCharacter(self):
        input = '"31337 \\u0000 31337"'
        self.assertEqual(ujson.decode(input), json.loads(input))

    def test_encodeListLongConversion(self):
        input = [
            9223372036854775807,
            9223372036854775807,
            9223372036854775807,
            9223372036854775807,
            9223372036854775807,
            9223372036854775807,
        ]
        output = ujson.encode(input)
        self.assertEqual(input, json.loads(output))
        self.assertEqual(input, ujson.decode(output))

    def test_encodeListLongUnsignedConversion(self):
        input = [18446744073709551615, 18446744073709551615, 18446744073709551615]
        output = ujson.encode(input)

        self.assertEqual(input, json.loads(output))
        self.assertEqual(input, ujson.decode(output))

    def test_encodeLongConversion(self):
        input = 9223372036854775807
        output = ujson.encode(input)
        self.assertEqual(input, json.loads(output))
        self.assertEqual(output, json.dumps(input))
        self.assertEqual(input, ujson.decode(output))

    def test_encodeLongUnsignedConversion(self):
        input = 18446744073709551615
        output = ujson.encode(input)

        self.assertEqual(input, json.loads(output))
        self.assertEqual(output, json.dumps(input))
        self.assertEqual(input, ujson.decode(output))

    def test_numericIntExp(self):
        input = "1337E40"
        output = ujson.decode(input)
        self.assertEqual(output, json.loads(input))

    def test_numericIntFrcExp(self):
        input = "1.337E40"
        output = ujson.decode(input)
        self.assertEqual(output, json.loads(input))

    def test_decodeNumericIntExpEPLUS(self):
        input = "1337E+9"
        output = ujson.decode(input)
        self.assertEqual(output, json.loads(input))

    def test_decodeNumericIntExpePLUS(self):
        input = "1.337e+40"
        output = ujson.decode(input)
        self.assertEqual(output, json.loads(input))

    def test_decodeNumericIntExpE(self):
        input = "1337E40"
        output = ujson.decode(input)
        self.assertEqual(output, json.loads(input))

    def test_decodeNumericIntExpe(self):
        input = "1337e40"
        output = ujson.decode(input)
        self.assertEqual(output, json.loads(input))

    def test_decodeNumericIntExpEMinus(self):
        input = "1.337E-4"
        output = ujson.decode(input)
        self.assertEqual(output, json.loads(input))

    def test_decodeNumericIntExpeMinus(self):
        input = "1.337e-4"
        output = ujson.decode(input)
        self.assertEqual(output, json.loads(input))

    def test_dumpToFile(self):
        f = StringIO()
        ujson.dump([1, 2, 3], f)
        self.assertEqual("[1,2,3]", f.getvalue())

    def test_dumpToFileLikeObject(self):
        class filelike:
            def __init__(self):
                self.bytes = ""

            def write(self, bytes):
                self.bytes += bytes

        f = filelike()
        ujson.dump([1, 2, 3], f)
        self.assertEqual("[1,2,3]", f.bytes)

    def test_dumpFileArgsError(self):
        self.assertRaises(TypeError, ujson.dump, [], "")

    def test_loadFile(self):
        f = StringIO("[1,2,3,4]")
        self.assertEqual([1, 2, 3, 4], ujson.load(f))

    def test_loadFileLikeObject(self):
        class filelike:
            def read(self):
                try:
                    self.end
                except AttributeError:
                    self.end = True
                    return "[1,2,3,4]"

        f = filelike()
        self.assertEqual([1, 2, 3, 4], ujson.load(f))

    def test_loadFileArgsError(self):
        self.assertRaises(TypeError, ujson.load, "[]")

    def test_encodeNumericOverflow(self):
        self.assertRaises(OverflowError, ujson.encode, 12839128391289382193812939)

    def test_decodeNumberWith32bitSignBit(self):
        # Test that numbers that fit within 32 bits but would have the
        # sign bit set (2**31 <= x < 2**32) are decoded properly.
        docs = (
            '{"id": 3590016419}',
            '{"id": %s}' % 2 ** 31,
            '{"id": %s}' % 2 ** 32,
            '{"id": %s}' % ((2 ** 32) - 1),
        )
        results = (3590016419, 2 ** 31, 2 ** 32, 2 ** 32 - 1)
        for doc, result in zip(docs, results):
            self.assertEqual(ujson.decode(doc)["id"], result)

    def test_encodeBigEscape(self):
        for x in range(10):
            base = "\u00e5".encode("utf-8")
            input = base * 1024 * 1024 * 2
            ujson.encode(input)

    def test_decodeBigEscape(self):
        for x in range(10):
            base = "\u00e5".encode("utf-8")
            quote = '"'.encode()
            input = quote + (base * 1024 * 1024 * 2) + quote
            ujson.decode(input)

    def test_toDict(self):
        d = {"key": 31337}

        class DictTest:
            def toDict(self):
                return d

            def __json__(self):
                return '"json defined"'  # Fallback and shouldn't be called.

        o = DictTest()
        output = ujson.encode(o)
        dec = ujson.decode(output)
        self.assertEqual(dec, d)

    def test_object_with_json(self):
        # If __json__ returns a string, then that string
        # will be used as a raw JSON snippet in the object.
        output_text = "this is the correct output"

        class JSONTest:
            def __json__(self):
                return '"' + output_text + '"'

        d = {u"key": JSONTest()}
        output = ujson.encode(d)
        dec = ujson.decode(output)
        self.assertEqual(dec, {u"key": output_text})

    def test_object_with_json_unicode(self):
        # If __json__ returns a string, then that string
        # will be used as a raw JSON snippet in the object.
        output_text = u"this is the correct output"

        class JSONTest:
            def __json__(self):
                return u'"' + output_text + u'"'

        d = {u"key": JSONTest()}
        output = ujson.encode(d)
        dec = ujson.decode(output)
        self.assertEqual(dec, {u"key": output_text})

    def test_object_with_complex_json(self):
        # If __json__ returns a string, then that string
        # will be used as a raw JSON snippet in the object.
        obj = {u"foo": [u"bar", u"baz"]}

        class JSONTest:
            def __json__(self):
                return ujson.encode(obj)

        d = {u"key": JSONTest()}
        output = ujson.encode(d)
        dec = ujson.decode(output)
        self.assertEqual(dec, {u"key": obj})

    def test_object_with_json_type_error(self):
        # __json__ must return a string, otherwise it should raise an error.
        for return_value in (None, 1234, 12.34, True, {}):

            class JSONTest:
                def __json__(self):
                    return return_value

            d = {u"key": JSONTest()}
            self.assertRaises(TypeError, ujson.encode, d)

    def test_object_with_json_attribute_error(self):
        # If __json__ raises an error, make sure python actually raises it.
        class JSONTest:
            def __json__(self):
                raise AttributeError

        d = {u"key": JSONTest()}
        self.assertRaises(AttributeError, ujson.encode, d)

    def test_decodeArrayTrailingCommaFail(self):
        input = "[31337,]"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeArrayLeadingCommaFail(self):
        input = "[,31337]"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeArrayOnlyCommaFail(self):
        input = "[,]"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeArrayUnmatchedBracketFail(self):
        input = "[]]"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeArrayEmpty(self):
        input = "[]"
        obj = ujson.decode(input)
        self.assertEqual([], obj)

    def test_decodeArrayOneItem(self):
        input = "[31337]"
        ujson.decode(input)

    def test_decodeLongUnsignedValue(self):
        input = "18446744073709551615"
        ujson.decode(input)

    def test_decodeBigValue(self):
        input = "9223372036854775807"
        ujson.decode(input)

    def test_decodeSmallValue(self):
        input = "-9223372036854775808"
        ujson.decode(input)

    def test_decodeTooBigValue(self):
        input = "18446744073709551616"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeTooSmallValue(self):
        input = "-90223372036854775809"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeVeryTooBigValue(self):
        input = "18446744073709551616"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeVeryTooSmallValue(self):
        input = "-90223372036854775809"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeWithTrailingWhitespaces(self):
        input = "{}\n\t "
        ujson.decode(input)

    def test_decodeWithTrailingNonWhitespaces(self):
        input = "{}\n\t a"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeArrayWithBigInt(self):
        input = "[18446744073709551616]"
        self.assertRaises(ValueError, ujson.decode, input)

    def test_decodeFloatingPointAdditionalTests(self):
        self.assertAlmostEqual(-1.1234567893, ujson.loads("-1.1234567893"))
        self.assertAlmostEqual(-1.234567893, ujson.loads("-1.234567893"))
        self.assertAlmostEqual(-1.34567893, ujson.loads("-1.34567893"))
        self.assertAlmostEqual(-1.4567893, ujson.loads("-1.4567893"))
        self.assertAlmostEqual(-1.567893, ujson.loads("-1.567893"))
        self.assertAlmostEqual(-1.67893, ujson.loads("-1.67893"))
        self.assertAlmostEqual(-1.7894, ujson.loads("-1.7894"))
        self.assertAlmostEqual(-1.893, ujson.loads("-1.893"))
        self.assertAlmostEqual(-1.3, ujson.loads("-1.3"))

        self.assertAlmostEqual(1.1234567893, ujson.loads("1.1234567893"))
        self.assertAlmostEqual(1.234567893, ujson.loads("1.234567893"))
        self.assertAlmostEqual(1.34567893, ujson.loads("1.34567893"))
        self.assertAlmostEqual(1.4567893, ujson.loads("1.4567893"))
        self.assertAlmostEqual(1.567893, ujson.loads("1.567893"))
        self.assertAlmostEqual(1.67893, ujson.loads("1.67893"))
        self.assertAlmostEqual(1.7894, ujson.loads("1.7894"))
        self.assertAlmostEqual(1.893, ujson.loads("1.893"))
        self.assertAlmostEqual(1.3, ujson.loads("1.3"))

    def test_ReadBadObjectSyntax(self):
        input = '{"age", 44}'
        self.assertRaises(ValueError, ujson.decode, input)

    def test_ReadTrue(self):
        self.assertEqual(True, ujson.loads("true"))

    def test_ReadFalse(self):
        self.assertEqual(False, ujson.loads("false"))

    def test_ReadNull(self):
        self.assertEqual(None, ujson.loads("null"))

    def test_WriteTrue(self):
        self.assertEqual("true", ujson.dumps(True))

    def test_WriteFalse(self):
        self.assertEqual("false", ujson.dumps(False))

    def test_WriteNull(self):
        self.assertEqual("null", ujson.dumps(None))

    def test_ReadArrayOfSymbols(self):
        self.assertEqual([True, False, None], ujson.loads(" [ true, false,null] "))

    def test_WriteArrayOfSymbolsFromList(self):
        self.assertEqual("[true,false,null]", ujson.dumps([True, False, None]))

    def test_WriteArrayOfSymbolsFromTuple(self):
        self.assertEqual("[true,false,null]", ujson.dumps((True, False, None)))

    def test_encodingInvalidUnicodeCharacter(self):
        s = "\udc7f"
        self.assertRaises(UnicodeEncodeError, ujson.dumps, s)

    def test_sortKeys(self):
        data = {"a": 1, "c": 1, "b": 1, "e": 1, "f": 1, "d": 1}
        sortedKeys = ujson.dumps(data, sort_keys=True)
        self.assertEqual(sortedKeys, '{"a":1,"b":1,"c":1,"d":1,"e":1,"f":1}')

    @unittest.skipIf(not hasattr(sys, 'getrefcount') == True, reason="test requires sys.refcount")
    def test_does_not_leak_dictionary_values(self):
        import gc

        gc.collect()
        value = ["abc"]
        data = {"1": value}
        ref_count = sys.getrefcount(value)
        ujson.dumps(data)
        self.assertEqual(ref_count, sys.getrefcount(value))

    @unittest.skipIf(not hasattr(sys, 'getrefcount') == True, reason="test requires sys.refcount")
    def test_does_not_leak_dictionary_keys(self):
        import gc

        gc.collect()
        key1 = "1"
        key2 = "1"
        value1 = ["abc"]
        value2 = [1, 2, 3]
        data = {key1: value1, key2: value2}
        ref_count1 = sys.getrefcount(key1)
        ref_count2 = sys.getrefcount(key2)
        ujson.dumps(data)
        self.assertEqual(ref_count1, sys.getrefcount(key1))
        self.assertEqual(ref_count2, sys.getrefcount(key2))

    @unittest.skipIf(not hasattr(sys, 'getrefcount') == True, reason="test requires sys.refcount")
    def test_does_not_leak_dictionary_string_key(self):
        import gc

        gc.collect()
        key1 = "1"
        value1 = 1
        data = {key1: value1}
        ref_count1 = sys.getrefcount(key1)
        ujson.dumps(data)
        self.assertEqual(ref_count1, sys.getrefcount(key1))

    @unittest.skipIf(not hasattr(sys, 'getrefcount') == True, reason="test requires sys.refcount")
    def test_does_not_leak_dictionary_tuple_key(self):
        import gc

        gc.collect()
        key1 = ("a",)
        value1 = 1
        data = {key1: value1}
        ref_count1 = sys.getrefcount(key1)
        ujson.dumps(data)
        self.assertEqual(ref_count1, sys.getrefcount(key1))

    @unittest.skipIf(not hasattr(sys, 'getrefcount') == True, reason="test requires sys.refcount")
    def test_does_not_leak_dictionary_bytes_key(self):
        import gc

        gc.collect()
        key1 = b"1"
        value1 = 1
        data = {key1: value1}
        ref_count1 = sys.getrefcount(key1)
        ujson.dumps(data)
        self.assertEqual(ref_count1, sys.getrefcount(key1))

    @unittest.skipIf(not hasattr(sys, 'getrefcount') == True, reason="test requires sys.refcount")
    def test_does_not_leak_dictionary_None_key(self):
        import gc

        gc.collect()
        key1 = None
        value1 = 1
        data = {key1: value1}
        ref_count1 = sys.getrefcount(key1)
        ujson.dumps(data)
        self.assertEqual(ref_count1, sys.getrefcount(key1))


"""
def test_decodeNumericIntFrcOverflow(self):
input = "X.Y"
raise NotImplementedError("Implement this test!")


def test_decodeStringUnicodeEscape(self):
input = "\u3131"
raise NotImplementedError("Implement this test!")

def test_decodeStringUnicodeBrokenEscape(self):
input = "\u3131"
raise NotImplementedError("Implement this test!")

def test_decodeStringUnicodeInvalidEscape(self):
input = "\u3131"
raise NotImplementedError("Implement this test!")

def test_decodeStringUTF8(self):
input = "someutfcharacters"
raise NotImplementedError("Implement this test!")

"""

if __name__ == "__main__":
    unittest.main()

"""
# Use this to look for memory leaks
if __name__ == '__main__':
    from guppy import hpy
    hp = hpy()
    hp.setrelheap()
    while True:
        try:
            unittest.main()
        except SystemExit:
            pass
        heap = hp.heapu()
        print(heap)
"""


@pytest.mark.parametrize("indent", list(range(65537, 65542)))
def test_dump_huge_indent(indent):
    ujson.encode({"a": True}, indent=indent)


@pytest.mark.parametrize("first_length", list(range(2, 7)))
@pytest.mark.parametrize("second_length", list(range(10919, 10924)))
def test_dump_long_string(first_length, second_length):
    ujson.dumps(["a" * first_length, "\x00" * second_length])


def test_dump_indented_nested_list():
    a = _a = []
    for i in range(20):
        _a.append(list(range(i)))
        _a = _a[-1]
        ujson.dumps(a, indent=i)


@pytest.mark.parametrize("indent", [0, 1, 2, 4, 5, 8, 49])
def test_issue_334(indent):
    import yatest.common as yc
    path = Path(yc.source_path(__file__)).with_name("334-reproducer.json")
    a = ujson.loads(path.read_bytes())
    ujson.dumps(a, indent=indent)


@pytest.mark.parametrize(
    "test_input, expected",
    [
        # Normal cases
        (r'"\uD83D\uDCA9"', "\U0001F4A9"),
        (r'"a\uD83D\uDCA9b"', "a\U0001F4A9b"),
        # Unpaired surrogates
        (r'"\uD800"', "\uD800"),
        (r'"a\uD800b"', "a\uD800b"),
        (r'"\uDEAD"', "\uDEAD"),
        (r'"a\uDEADb"', "a\uDEADb"),
        (r'"\uD83D\uD83D\uDCA9"', "\uD83D\U0001F4A9"),
        (r'"\uDCA9\uD83D\uDCA9"', "\uDCA9\U0001F4A9"),
        (r'"\uD83D\uDCA9\uD83D"', "\U0001F4A9\uD83D"),
        (r'"\uD83D\uDCA9\uDCA9"', "\U0001F4A9\uDCA9"),
        (r'"\uD83D \uDCA9"', "\uD83D \uDCA9"),
        # No decoding of actual surrogate characters (rather than escaped ones)
        ('"\uD800"', "\uD800"),
        ('"\uDEAD"', "\uDEAD"),
        ('"\uD800a\uDEAD"', "\uD800a\uDEAD"),
        ('"\uD83D\uDCA9"', "\uD83D\uDCA9"),
    ],
)
def _test_decode_surrogate_characters(test_input, expected):  # disable test, problems with encode bad utf char in Arcadia
    assert ujson.loads(test_input) == expected
    assert ujson.loads(test_input.encode("utf-8", "surrogatepass")) == expected

    # Ensure that this matches stdlib's behaviour
    assert json.loads(test_input) == expected
