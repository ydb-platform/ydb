# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

# Copyright (C) 2003-2007, 2009-2011 Nominum, Inc.
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose with or without fee is hereby granted,
# provided that the above copyright notice and this permission notice
# appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND NOMINUM DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL NOMINUM BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
# OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import unittest

import dns.exception
import dns.tokenizer

Token = dns.tokenizer.Token

class TokenizerTestCase(unittest.TestCase):

    def testStr(self):
        tok = dns.tokenizer.Tokenizer('foo')
        token = tok.get()
        self.assertEqual(token, Token(dns.tokenizer.IDENTIFIER, 'foo'))

    def testQuotedString1(self):
        tok = dns.tokenizer.Tokenizer(r'"foo"')
        token = tok.get()
        self.assertEqual(token, Token(dns.tokenizer.QUOTED_STRING, 'foo'))

    def testQuotedString2(self):
        tok = dns.tokenizer.Tokenizer(r'""')
        token = tok.get()
        self.assertEqual(token, Token(dns.tokenizer.QUOTED_STRING, ''))

    def testQuotedString3(self):
        tok = dns.tokenizer.Tokenizer(r'"\"foo\""')
        token = tok.get()
        self.assertEqual(token, Token(dns.tokenizer.QUOTED_STRING, '\\"foo\\"'))

    def testQuotedString4(self):
        tok = dns.tokenizer.Tokenizer(r'"foo\010bar"')
        token = tok.get()
        self.assertEqual(token, Token(dns.tokenizer.QUOTED_STRING,
                                      'foo\\010bar'))

    def testQuotedString5(self):
        with self.assertRaises(dns.exception.UnexpectedEnd):
            tok = dns.tokenizer.Tokenizer(r'"foo')
            tok.get()

    def testQuotedString6(self):
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer(r'"foo\01')
            tok.get()

    def testQuotedString7(self):
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('"foo\nbar"')
            tok.get()

    def testEmpty1(self):
        tok = dns.tokenizer.Tokenizer('')
        token = tok.get()
        self.assertTrue(token.is_eof())

    def testEmpty2(self):
        tok = dns.tokenizer.Tokenizer('')
        token1 = tok.get()
        token2 = tok.get()
        self.assertTrue(token1.is_eof() and token2.is_eof())

    def testEOL(self):
        tok = dns.tokenizer.Tokenizer('\n')
        token1 = tok.get()
        token2 = tok.get()
        self.assertTrue(token1.is_eol() and token2.is_eof())

    def testWS1(self):
        tok = dns.tokenizer.Tokenizer(' \n')
        token1 = tok.get()
        self.assertTrue(token1.is_eol())

    def testWS2(self):
        tok = dns.tokenizer.Tokenizer(' \n')
        token1 = tok.get(want_leading=True)
        self.assertTrue(token1.is_whitespace())

    def testComment1(self):
        tok = dns.tokenizer.Tokenizer(' ;foo\n')
        token1 = tok.get()
        self.assertTrue(token1.is_eol())

    def testComment2(self):
        tok = dns.tokenizer.Tokenizer(' ;foo\n')
        token1 = tok.get(want_comment=True)
        token2 = tok.get()
        self.assertEqual(token1, Token(dns.tokenizer.COMMENT, 'foo'))
        self.assertTrue(token2.is_eol())

    def testComment3(self):
        tok = dns.tokenizer.Tokenizer(' ;foo bar\n')
        token1 = tok.get(want_comment=True)
        token2 = tok.get()
        self.assertEqual(token1, Token(dns.tokenizer.COMMENT, 'foo bar'))
        self.assertTrue(token2.is_eol())

    def testMultiline1(self):
        tok = dns.tokenizer.Tokenizer('( foo\n\n bar\n)')
        tokens = list(iter(tok))
        self.assertEqual(tokens, [Token(dns.tokenizer.IDENTIFIER, 'foo'),
                                  Token(dns.tokenizer.IDENTIFIER, 'bar')])

    def testMultiline2(self):
        tok = dns.tokenizer.Tokenizer('( foo\n\n bar\n)\n')
        tokens = list(iter(tok))
        self.assertEqual(tokens, [Token(dns.tokenizer.IDENTIFIER, 'foo'),
                                  Token(dns.tokenizer.IDENTIFIER, 'bar'),
                                  Token(dns.tokenizer.EOL, '\n')])

    def testMultiline3(self):
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('foo)')
            list(iter(tok))

    def testMultiline4(self):
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('((foo)')
            list(iter(tok))

    def testUnget1(self):
        tok = dns.tokenizer.Tokenizer('foo')
        t1 = tok.get()
        tok.unget(t1)
        t2 = tok.get()
        self.assertEqual(t1, t2)
        self.assertEqual(t1.ttype, dns.tokenizer.IDENTIFIER)
        self.assertEqual(t1.value, 'foo')

    def testUnget2(self):
        with self.assertRaises(dns.tokenizer.UngetBufferFull):
            tok = dns.tokenizer.Tokenizer('foo')
            t1 = tok.get()
            tok.unget(t1)
            tok.unget(t1)

    def testGetEOL1(self):
        tok = dns.tokenizer.Tokenizer('\n')
        t = tok.get_eol()
        self.assertEqual(t, '\n')

    def testGetEOL2(self):
        tok = dns.tokenizer.Tokenizer('')
        t = tok.get_eol()
        self.assertEqual(t, '')

    def testEscapedDelimiter1(self):
        tok = dns.tokenizer.Tokenizer(r'ch\ ld')
        t = tok.get()
        self.assertEqual(t.ttype, dns.tokenizer.IDENTIFIER)
        self.assertEqual(t.value, r'ch\ ld')

    def testEscapedDelimiter2(self):
        tok = dns.tokenizer.Tokenizer(r'ch\032ld')
        t = tok.get()
        self.assertEqual(t.ttype, dns.tokenizer.IDENTIFIER)
        self.assertEqual(t.value, r'ch\032ld')

    def testEscapedDelimiter3(self):
        tok = dns.tokenizer.Tokenizer(r'ch\ild')
        t = tok.get()
        self.assertEqual(t.ttype, dns.tokenizer.IDENTIFIER)
        self.assertEqual(t.value, r'ch\ild')

    def testEscapedDelimiter1u(self):
        tok = dns.tokenizer.Tokenizer(r'ch\ ld')
        t = tok.get().unescape()
        self.assertEqual(t.ttype, dns.tokenizer.IDENTIFIER)
        self.assertEqual(t.value, r'ch ld')

    def testEscapedDelimiter2u(self):
        tok = dns.tokenizer.Tokenizer(r'ch\032ld')
        t = tok.get().unescape()
        self.assertEqual(t.ttype, dns.tokenizer.IDENTIFIER)
        self.assertEqual(t.value, 'ch ld')

    def testEscapedDelimiter3u(self):
        tok = dns.tokenizer.Tokenizer(r'ch\ild')
        t = tok.get().unescape()
        self.assertEqual(t.ttype, dns.tokenizer.IDENTIFIER)
        self.assertEqual(t.value, r'child')

    def testGetUInt(self):
        tok = dns.tokenizer.Tokenizer('1234')
        v = tok.get_int()
        self.assertEqual(v, 1234)
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('"1234"')
            tok.get_int()
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('q1234')
            tok.get_int()
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('281474976710656')
            tok.get_uint48()
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('4294967296')
            tok.get_uint32()
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('65536')
            tok.get_uint16()
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('256')
            tok.get_uint8()
        # Even though it is badly named get_int(), it's really get_unit!
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('-1234')
            tok.get_int()
        # get_uint16 can do other bases too, and has a custom error
        # for base 8.
        tok = dns.tokenizer.Tokenizer('177777')
        self.assertEqual(tok.get_uint16(base=8), 65535)
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('200000')
            tok.get_uint16(base=8)

    def testGetString(self):
        tok = dns.tokenizer.Tokenizer('foo')
        v = tok.get_string()
        self.assertEqual(v, 'foo')
        tok = dns.tokenizer.Tokenizer('"foo"')
        v = tok.get_string()
        self.assertEqual(v, 'foo')
        tok = dns.tokenizer.Tokenizer('abcdefghij')
        v = tok.get_string(max_length=10)
        self.assertEqual(v, 'abcdefghij')
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('abcdefghij')
            tok.get_string(max_length=9)
        tok = dns.tokenizer.Tokenizer('')
        with self.assertRaises(dns.exception.SyntaxError):
            tok.get_string()

    def testMultiLineWithComment(self):
        tok = dns.tokenizer.Tokenizer('( ; abc\n)')
        tok.get_eol()
        # Nothing to assert here, as we're testing tok.get_eol() does NOT
        # raise.

    def testEOLAfterComment(self):
        tok = dns.tokenizer.Tokenizer('; abc\n')
        t = tok.get()
        self.assertTrue(t.is_eol())

    def testEOFAfterComment(self):
        tok = dns.tokenizer.Tokenizer('; abc')
        t = tok.get()
        self.assertTrue(t.is_eof())

    def testMultiLineWithEOFAfterComment(self):
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('( ; abc')
            tok.get_eol()

    def testEscapeUnexpectedEnd(self):
        with self.assertRaises(dns.exception.UnexpectedEnd):
            tok = dns.tokenizer.Tokenizer('\\')
            tok.get()

    def testEscapeBounds(self):
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('\\256')
            tok.get().unescape()
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('\\256')
            tok.get().unescape_to_bytes()

    def testGetUngetRegetComment(self):
        tok = dns.tokenizer.Tokenizer(';comment')
        t1 = tok.get(want_comment=True)
        tok.unget(t1)
        t2 = tok.get(want_comment=True)
        self.assertEqual(t1, t2)

    def testBadAsName(self):
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('"not an identifier"')
            t = tok.get()
            tok.as_name(t)

    def testBadGetTTL(self):
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('"not an identifier"')
            tok.get_ttl()

    def testBadGetEOL(self):
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('"not an identifier"')
            tok.get_eol_as_token()

    def testDanglingEscapes(self):
        for text in ['"\\"', '"\\0"', '"\\00"', '"\\00a"']:
            with self.assertRaises(dns.exception.SyntaxError):
                tok = dns.tokenizer.Tokenizer(text)
                tok.get().unescape()
            with self.assertRaises(dns.exception.SyntaxError):
                tok = dns.tokenizer.Tokenizer(text)
                tok.get().unescape_to_bytes()

    def testTokenMisc(self):
        t1 = dns.tokenizer.Token(dns.tokenizer.IDENTIFIER, 'hi')
        t2 = dns.tokenizer.Token(dns.tokenizer.IDENTIFIER, 'hi')
        t3 = dns.tokenizer.Token(dns.tokenizer.IDENTIFIER, 'there')
        self.assertEqual(t1, t2)
        self.assertFalse(t1 == 'hi')  # not NotEqual because we want to use ==
        self.assertNotEqual(t1, 'hi')
        self.assertNotEqual(t1, t3)
        self.assertEqual(str(t1), '3 "hi"')

    def testBadConcatenateRemaining(self):
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('a b "not an identifier" c')
            tok.concatenate_remaining_identifiers()

    def testStdinFilename(self):
        tok = dns.tokenizer.Tokenizer()
        self.assertEqual(tok.filename, '<stdin>')

    def testBytesLiteral(self):
        tok = dns.tokenizer.Tokenizer(b'this is input')
        self.assertEqual(tok.get().value, 'this')
        self.assertEqual(tok.filename, '<string>')
        tok = dns.tokenizer.Tokenizer(b'this is input', 'myfilename')
        self.assertEqual(tok.filename, 'myfilename')

    def testUngetBranches(self):
        tok = dns.tokenizer.Tokenizer(b'    this is input')
        t = tok.get(want_leading=True)
        tok.unget(t)
        t = tok.get(want_leading=True)
        self.assertEqual(t.ttype, dns.tokenizer.WHITESPACE)
        tok.unget(t)
        t = tok.get()
        self.assertEqual(t.ttype, dns.tokenizer.IDENTIFIER)
        self.assertEqual(t.value, 'this')
        tok = dns.tokenizer.Tokenizer(b';    this is input\n')
        t = tok.get(want_comment=True)
        tok.unget(t)
        t = tok.get(want_comment=True)
        self.assertEqual(t.ttype, dns.tokenizer.COMMENT)
        tok.unget(t)
        t = tok.get()
        self.assertEqual(t.ttype, dns.tokenizer.EOL)

if __name__ == '__main__':
    unittest.main()
