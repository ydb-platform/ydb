# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os
import unittest
from w3lib.url import (is_url, safe_url_string, safe_download_url,
    url_query_parameter, add_or_replace_parameter, url_query_cleaner,
    file_uri_to_path, parse_data_uri, path_to_file_uri, any_to_uri,
    urljoin_rfc, canonicalize_url, parse_url, add_or_replace_parameters)
from six.moves.urllib.parse import urlparse


class UrlTests(unittest.TestCase):

    def test_safe_url_string(self):
        # Motoko Kusanagi (Cyborg from Ghost in the Shell)
        motoko = u'\u8349\u8599 \u7d20\u5b50'
        self.assertEqual(safe_url_string(motoko),  # note the %20 for space
                        '%E8%8D%89%E8%96%99%20%E7%B4%A0%E5%AD%90')
        self.assertEqual(safe_url_string(motoko),
                         safe_url_string(safe_url_string(motoko)))
        self.assertEqual(safe_url_string(u'©'), # copyright symbol
                         '%C2%A9')
        # page-encoding does not affect URL path
        self.assertEqual(safe_url_string(u'©', 'iso-8859-1'),
                         '%C2%A9')
        # path_encoding does
        self.assertEqual(safe_url_string(u'©', path_encoding='iso-8859-1'),
                         '%A9')
        self.assertEqual(safe_url_string("http://www.example.org/"),
                        'http://www.example.org/')

        alessi = u'/ecommerce/oggetto/Te \xf2/tea-strainer/1273'

        self.assertEqual(safe_url_string(alessi),
                         '/ecommerce/oggetto/Te%20%C3%B2/tea-strainer/1273')

        self.assertEqual(safe_url_string("http://www.example.com/test?p(29)url(http://www.another.net/page)"),
                                         "http://www.example.com/test?p(29)url(http://www.another.net/page)")
        self.assertEqual(safe_url_string("http://www.example.com/Brochures_&_Paint_Cards&PageSize=200"),
                                         "http://www.example.com/Brochures_&_Paint_Cards&PageSize=200")

        # page-encoding does not affect URL path
        # we still end up UTF-8 encoding characters before percent-escaping
        safeurl = safe_url_string(u"http://www.example.com/£")
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%C2%A3")

        safeurl = safe_url_string(u"http://www.example.com/£", encoding='utf-8')
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%C2%A3")

        safeurl = safe_url_string(u"http://www.example.com/£", encoding='latin-1')
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%C2%A3")

        safeurl = safe_url_string(u"http://www.example.com/£", path_encoding='latin-1')
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%A3")

        self.assertTrue(isinstance(safe_url_string(b'http://example.com/'), str))

    def test_safe_url_string_remove_ascii_tab_and_newlines(self):
        self.assertEqual(safe_url_string("http://example.com/test\n.html"),
                                         "http://example.com/test.html")
        self.assertEqual(safe_url_string("http://example.com/test\t.html"),
                                         "http://example.com/test.html")
        self.assertEqual(safe_url_string("http://example.com/test\r.html"),
                                         "http://example.com/test.html")
        self.assertEqual(safe_url_string("http://example.com/test\r.html\n"),
                                         "http://example.com/test.html")
        self.assertEqual(safe_url_string("http://example.com/test\r\n.html\t"),
                                         "http://example.com/test.html")
        self.assertEqual(safe_url_string("http://example.com/test\a\n.html"),
                                         "http://example.com/test%07.html")

    def test_safe_url_string_unsafe_chars(self):
        safeurl = safe_url_string(r"http://localhost:8001/unwise{,},|,\,^,[,],`?|=[]&[]=|")
        self.assertEqual(safeurl, r"http://localhost:8001/unwise%7B,%7D,|,%5C,%5E,[,],%60?|=[]&[]=|")
        
    def test_safe_url_string_quote_path(self):
        safeurl = safe_url_string(u'http://google.com/"hello"', quote_path=True)
        self.assertEqual(safeurl, u'http://google.com/%22hello%22')
        
        safeurl = safe_url_string(u'http://google.com/"hello"', quote_path=False)
        self.assertEqual(safeurl, u'http://google.com/"hello"')
        
        safeurl = safe_url_string(u'http://google.com/"hello"')
        self.assertEqual(safeurl, u'http://google.com/%22hello%22')
        

    def test_safe_url_string_with_query(self):
        safeurl = safe_url_string(u"http://www.example.com/£?unit=µ")
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%C2%A3?unit=%C2%B5")

        safeurl = safe_url_string(u"http://www.example.com/£?unit=µ", encoding='utf-8')
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%C2%A3?unit=%C2%B5")

        safeurl = safe_url_string(u"http://www.example.com/£?unit=µ", encoding='latin-1')
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%C2%A3?unit=%B5")

        safeurl = safe_url_string(u"http://www.example.com/£?unit=µ", path_encoding='latin-1')
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%A3?unit=%C2%B5")

        safeurl = safe_url_string(u"http://www.example.com/£?unit=µ", encoding='latin-1', path_encoding='latin-1')
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%A3?unit=%B5")

    def test_safe_url_string_misc(self):
        # mixing Unicode and percent-escaped sequences
        safeurl = safe_url_string(u"http://www.example.com/£?unit=%C2%B5")
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%C2%A3?unit=%C2%B5")

        safeurl = safe_url_string(u"http://www.example.com/%C2%A3?unit=µ")
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%C2%A3?unit=%C2%B5")

    def test_safe_url_string_bytes_input(self):
        safeurl = safe_url_string(b"http://www.example.com/")
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/")

        # bytes input is assumed to be UTF-8
        safeurl = safe_url_string(b"http://www.example.com/\xc2\xb5")
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%C2%B5")

        # page-encoding encoded bytes still end up as UTF-8 sequences in path
        safeurl = safe_url_string(b"http://www.example.com/\xb5", encoding='latin1')
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%C2%B5")

        safeurl = safe_url_string(b"http://www.example.com/\xa3?unit=\xb5", encoding='latin1')
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%C2%A3?unit=%B5")

    def test_safe_url_string_bytes_input_nonutf8(self):
        # latin1
        safeurl = safe_url_string(b"http://www.example.com/\xa3?unit=\xb5")
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%A3?unit=%B5")

        # cp1251
        # >>> u'Россия'.encode('cp1251')
        # '\xd0\xee\xf1\xf1\xe8\xff'
        safeurl = safe_url_string(b"http://www.example.com/country/\xd0\xee\xf1\xf1\xe8\xff")
        self.assertTrue(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/country/%D0%EE%F1%F1%E8%FF")

    def test_safe_url_idna(self):
        # adapted from:
        # https://ssl.icu-project.org/icu-bin/idnbrowser
        # http://unicode.org/faq/idn.html
        # + various others
        websites = (
            (u'http://www.färgbolaget.nu/färgbolaget', 'http://www.xn--frgbolaget-q5a.nu/f%C3%A4rgbolaget'),
            (u'http://www.räksmörgås.se/?räksmörgås=yes', 'http://www.xn--rksmrgs-5wao1o.se/?r%C3%A4ksm%C3%B6rg%C3%A5s=yes'),
            (u'http://www.brændendekærlighed.com/brændende/kærlighed', 'http://www.xn--brndendekrlighed-vobh.com/br%C3%A6ndende/k%C3%A6rlighed'),
            (u'http://www.예비교사.com', 'http://www.xn--9d0bm53a3xbzui.com'),
            (u'http://理容ナカムラ.com', 'http://xn--lck1c3crb1723bpq4a.com'),
            (u'http://あーるいん.com', 'http://xn--l8je6s7a45b.com'),

            # --- real websites ---

            # in practice, this redirect (301) to http://www.buecher.de/?q=b%C3%BCcher
            (u'http://www.bücher.de/?q=bücher', 'http://www.xn--bcher-kva.de/?q=b%C3%BCcher'),

            # Japanese
            (u'http://はじめよう.みんな/?query=サ&maxResults=5', 'http://xn--p8j9a0d9c9a.xn--q9jyb4c/?query=%E3%82%B5&maxResults=5'),

            # Russian
            (u'http://кто.рф/', 'http://xn--j1ail.xn--p1ai/'),
            (u'http://кто.рф/index.php?domain=Что', 'http://xn--j1ail.xn--p1ai/index.php?domain=%D0%A7%D1%82%D0%BE'),

            # Korean
            (u'http://내도메인.한국/', 'http://xn--220b31d95hq8o.xn--3e0b707e/'),
            (u'http://맨체스터시티축구단.한국/', 'http://xn--2e0b17htvgtvj9haj53ccob62ni8d.xn--3e0b707e/'),

            # Arabic
            (u'http://nic.شبكة', 'http://nic.xn--ngbc5azd'),

            # Chinese
            (u'https://www.贷款.在线', 'https://www.xn--0kwr83e.xn--3ds443g'),
            (u'https://www2.xn--0kwr83e.在线', 'https://www2.xn--0kwr83e.xn--3ds443g'),
            (u'https://www3.贷款.xn--3ds443g', 'https://www3.xn--0kwr83e.xn--3ds443g'),
        )
        for idn_input, safe_result in websites:
            safeurl = safe_url_string(idn_input)
            self.assertEqual(safeurl, safe_result)

        # make sure the safe URL is unchanged when made safe a 2nd time
        for _, safe_result in websites:
            safeurl = safe_url_string(safe_result)
            self.assertEqual(safeurl, safe_result)

    def test_safe_url_idna_encoding_failure(self):
        # missing DNS label
        self.assertEqual(
            safe_url_string(u"http://.example.com/résumé?q=résumé"),
            "http://.example.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9")

        # DNS label too long
        self.assertEqual(
            safe_url_string(
                u"http://www.{label}.com/résumé?q=résumé".format(
                    label=u"example"*11)),
            "http://www.{label}.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9".format(
                    label=u"example"*11))

    def test_safe_url_port_number(self):
        self.assertEqual(
            safe_url_string(u"http://www.example.com:80/résumé?q=résumé"),
            "http://www.example.com:80/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9")
        self.assertEqual(
            safe_url_string(u"http://www.example.com:/résumé?q=résumé"),
            "http://www.example.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9")

    def test_safe_download_url(self):
        self.assertEqual(safe_download_url('http://www.example.org'),
                         'http://www.example.org/')
        self.assertEqual(safe_download_url('http://www.example.org/../'),
                         'http://www.example.org/')
        self.assertEqual(safe_download_url('http://www.example.org/../../images/../image'),
                         'http://www.example.org/image')
        self.assertEqual(safe_download_url('http://www.example.org/dir/'),
                         'http://www.example.org/dir/')
        self.assertEqual(safe_download_url(b'http://www.example.org/dir/'),
                         'http://www.example.org/dir/')

        # Encoding related tests
        self.assertEqual(safe_download_url(b'http://www.example.org?\xa3',
                         encoding='latin-1', path_encoding='latin-1'),
                         'http://www.example.org/?%A3')
        self.assertEqual(safe_download_url(b'http://www.example.org?\xc2\xa3',
                         encoding='utf-8', path_encoding='utf-8'),
                         'http://www.example.org/?%C2%A3')
        self.assertEqual(safe_download_url(b'http://www.example.org/\xc2\xa3?\xc2\xa3',
                         encoding='utf-8', path_encoding='latin-1'),
                         'http://www.example.org/%A3?%C2%A3')

    def test_is_url(self):
        self.assertTrue(is_url('http://www.example.org'))
        self.assertTrue(is_url('https://www.example.org'))
        self.assertTrue(is_url('file:///some/path'))
        self.assertFalse(is_url('foo://bar'))
        self.assertFalse(is_url('foo--bar'))

    def test_url_query_parameter(self):
        self.assertEqual(url_query_parameter("product.html?id=200&foo=bar", "id"),
                         '200')
        self.assertEqual(url_query_parameter("product.html?id=200&foo=bar", "notthere", "mydefault"),
                         'mydefault')
        self.assertEqual(url_query_parameter("product.html?id=", "id"),
                         None)
        self.assertEqual(url_query_parameter("product.html?id=", "id", keep_blank_values=1),
                         '')

    def test_url_query_parameter_2(self):
        """
        This problem was seen several times in the feeds. Sometime affiliate URLs contains
        nested encoded affiliate URL with direct URL as parameters. For example:
        aff_url1 = 'http://www.tkqlhce.com/click-2590032-10294381?url=http%3A%2F%2Fwww.argos.co.uk%2Fwebapp%2Fwcs%2Fstores%2Fservlet%2FArgosCreateReferral%3FstoreId%3D10001%26langId%3D-1%26referrer%3DCOJUN%26params%3Dadref%253DGarden+and+DIY-%3EGarden+furniture-%3EChildren%26%2339%3Bs+garden+furniture%26referredURL%3Dhttp%3A%2F%2Fwww.argos.co.uk%2Fwebapp%2Fwcs%2Fstores%2Fservlet%2FProductDisplay%253FstoreId%253D10001%2526catalogId%253D1500001501%2526productId%253D1500357023%2526langId%253D-1'
        the typical code to extract needed URL from it is:
        aff_url2 = url_query_parameter(aff_url1, 'url')
        after this aff2_url is:
        'http://www.argos.co.uk/webapp/wcs/stores/servlet/ArgosCreateReferral?storeId=10001&langId=-1&referrer=COJUN&params=adref%3DGarden and DIY->Garden furniture->Children&#39;s gardenfurniture&referredURL=http://www.argos.co.uk/webapp/wcs/stores/servlet/ProductDisplay%3FstoreId%3D10001%26catalogId%3D1500001501%26productId%3D1500357023%26langId%3D-1'
        the direct URL extraction is
        url = url_query_parameter(aff_url2, 'referredURL')
        but this will not work, because aff_url2 contains &#39; (comma sign encoded in the feed)
        and the URL extraction will fail, current workaround was made in the spider,
        just a replace for &#39; to %27
        """
        return # FIXME: this test should pass but currently doesnt
        # correct case
        aff_url1 = "http://www.anrdoezrs.net/click-2590032-10294381?url=http%3A%2F%2Fwww.argos.co.uk%2Fwebapp%2Fwcs%2Fstores%2Fservlet%2FArgosCreateReferral%3FstoreId%3D10001%26langId%3D-1%26referrer%3DCOJUN%26params%3Dadref%253DGarden+and+DIY-%3EGarden+furniture-%3EGarden+table+and+chair+sets%26referredURL%3Dhttp%3A%2F%2Fwww.argos.co.uk%2Fwebapp%2Fwcs%2Fstores%2Fservlet%2FProductDisplay%253FstoreId%253D10001%2526catalogId%253D1500001501%2526productId%253D1500357199%2526langId%253D-1"
        aff_url2 = url_query_parameter(aff_url1, 'url')
        self.assertEqual(aff_url2, "http://www.argos.co.uk/webapp/wcs/stores/servlet/ArgosCreateReferral?storeId=10001&langId=-1&referrer=COJUN&params=adref%3DGarden and DIY->Garden furniture->Garden table and chair sets&referredURL=http://www.argos.co.uk/webapp/wcs/stores/servlet/ProductDisplay%3FstoreId%3D10001%26catalogId%3D1500001501%26productId%3D1500357199%26langId%3D-1")
        prod_url = url_query_parameter(aff_url2, 'referredURL')
        self.assertEqual(prod_url, "http://www.argos.co.uk/webapp/wcs/stores/servlet/ProductDisplay?storeId=10001&catalogId=1500001501&productId=1500357199&langId=-1")
        # weird case
        aff_url1 = "http://www.tkqlhce.com/click-2590032-10294381?url=http%3A%2F%2Fwww.argos.co.uk%2Fwebapp%2Fwcs%2Fstores%2Fservlet%2FArgosCreateReferral%3FstoreId%3D10001%26langId%3D-1%26referrer%3DCOJUN%26params%3Dadref%253DGarden+and+DIY-%3EGarden+furniture-%3EChildren%26%2339%3Bs+garden+furniture%26referredURL%3Dhttp%3A%2F%2Fwww.argos.co.uk%2Fwebapp%2Fwcs%2Fstores%2Fservlet%2FProductDisplay%253FstoreId%253D10001%2526catalogId%253D1500001501%2526productId%253D1500357023%2526langId%253D-1"
        aff_url2 = url_query_parameter(aff_url1, 'url')
        self.assertEqual(aff_url2, "http://www.argos.co.uk/webapp/wcs/stores/servlet/ArgosCreateReferral?storeId=10001&langId=-1&referrer=COJUN&params=adref%3DGarden and DIY->Garden furniture->Children&#39;s garden furniture&referredURL=http://www.argos.co.uk/webapp/wcs/stores/servlet/ProductDisplay%3FstoreId%3D10001%26catalogId%3D1500001501%26productId%3D1500357023%26langId%3D-1")
        prod_url = url_query_parameter(aff_url2, 'referredURL')
        # fails, prod_url is None now
        self.assertEqual(prod_url, "http://www.argos.co.uk/webapp/wcs/stores/servlet/ProductDisplay?storeId=10001&catalogId=1500001501&productId=1500357023&langId=-1")

    def test_add_or_replace_parameter(self):
        url = 'http://domain/test'
        self.assertEqual(add_or_replace_parameter(url, 'arg', 'v'),
                         'http://domain/test?arg=v')
        url = 'http://domain/test?arg1=v1&arg2=v2&arg3=v3'
        self.assertEqual(add_or_replace_parameter(url, 'arg4', 'v4'),
                         'http://domain/test?arg1=v1&arg2=v2&arg3=v3&arg4=v4')
        self.assertEqual(add_or_replace_parameter(url, 'arg3', 'nv3'),
                         'http://domain/test?arg1=v1&arg2=v2&arg3=nv3')

        url = 'http://domain/test?arg1=v1;arg2=v2'
        self.assertEqual(add_or_replace_parameter(url, 'arg1', 'v3'),
                         'http://domain/test?arg1=v3&arg2=v2')

        self.assertEqual(add_or_replace_parameter("http://domain/moreInfo.asp?prodID=", 'prodID', '20'),
                         'http://domain/moreInfo.asp?prodID=20')
        url = 'http://rmc-offers.co.uk/productlist.asp?BCat=2%2C60&CatID=60'
        self.assertEqual(add_or_replace_parameter(url, 'BCat', 'newvalue'),
                         'http://rmc-offers.co.uk/productlist.asp?BCat=newvalue&CatID=60')
        url = 'http://rmc-offers.co.uk/productlist.asp?BCat=2,60&CatID=60'
        self.assertEqual(add_or_replace_parameter(url, 'BCat', 'newvalue'),
                         'http://rmc-offers.co.uk/productlist.asp?BCat=newvalue&CatID=60')
        url = 'http://rmc-offers.co.uk/productlist.asp?'
        self.assertEqual(add_or_replace_parameter(url, 'BCat', 'newvalue'),
                         'http://rmc-offers.co.uk/productlist.asp?BCat=newvalue')

        url = "http://example.com/?version=1&pageurl=http%3A%2F%2Fwww.example.com%2Ftest%2F%23fragment%3Dy&param2=value2"
        self.assertEqual(add_or_replace_parameter(url, 'version', '2'),
                         'http://example.com/?version=2&pageurl=http%3A%2F%2Fwww.example.com%2Ftest%2F%23fragment%3Dy&param2=value2')
        self.assertEqual(add_or_replace_parameter(url, 'pageurl', 'test'),
                         'http://example.com/?version=1&pageurl=test&param2=value2')

        url = 'http://domain/test?arg1=v1&arg2=v2&arg1=v3'
        self.assertEqual(add_or_replace_parameter(url, 'arg4', 'v4'),
                         'http://domain/test?arg1=v1&arg2=v2&arg1=v3&arg4=v4')
        self.assertEqual(add_or_replace_parameter(url, 'arg1', 'v3'),
                         'http://domain/test?arg1=v3&arg2=v2')

    def test_add_or_replace_parameters(self):
        url = 'http://domain/test'
        self.assertEqual(add_or_replace_parameters(url, {'arg': 'v'}),
                         'http://domain/test?arg=v')
        url = 'http://domain/test?arg1=v1&arg2=v2&arg3=v3'
        self.assertEqual(add_or_replace_parameters(url, {'arg4': 'v4'}),
                         'http://domain/test?arg1=v1&arg2=v2&arg3=v3&arg4=v4')
        self.assertEqual(add_or_replace_parameters(url, {'arg4': 'v4', 'arg3': 'v3new'}),
                         'http://domain/test?arg1=v1&arg2=v2&arg3=v3new&arg4=v4')
        url = 'http://domain/test?arg1=v1&arg2=v2&arg1=v3'
        self.assertEqual(add_or_replace_parameters(url, {'arg4': 'v4'}),
                         'http://domain/test?arg1=v1&arg2=v2&arg1=v3&arg4=v4')
        self.assertEqual(add_or_replace_parameters(url, {'arg1': 'v3'}),
                         'http://domain/test?arg1=v3&arg2=v2')

    def test_add_or_replace_parameters_does_not_change_input_param(self):
        url = 'http://domain/test?arg=original'
        input_param = {'arg': 'value'}
        new_url = add_or_replace_parameters(url, input_param)  # noqa
        self.assertEqual(input_param, {'arg': 'value'})

    def test_url_query_cleaner(self):
        self.assertEqual('product.html',
                url_query_cleaner("product.html?"))
        self.assertEqual('product.html',
                url_query_cleaner("product.html?&"))
        self.assertEqual('product.html?id=200',
                url_query_cleaner("product.html?id=200&foo=bar&name=wired", ['id']))
        self.assertEqual('product.html?id=200',
                url_query_cleaner("product.html?&id=200&&foo=bar&name=wired", ['id']))
        self.assertEqual('product.html',
                url_query_cleaner("product.html?foo=bar&name=wired", ['id']))
        self.assertEqual('product.html?id=200&name=wired',
                url_query_cleaner("product.html?id=200&foo=bar&name=wired", ['id', 'name']))
        self.assertEqual('product.html?id',
                url_query_cleaner("product.html?id&other=3&novalue=", ['id']))
        # default is to remove duplicate keys
        self.assertEqual('product.html?d=1',
                url_query_cleaner("product.html?d=1&e=b&d=2&d=3&other=other", ['d']))
        # unique=False disables duplicate keys filtering
        self.assertEqual('product.html?d=1&d=2&d=3',
                url_query_cleaner("product.html?d=1&e=b&d=2&d=3&other=other", ['d'], unique=False))
        self.assertEqual('product.html?id=200&foo=bar',
                url_query_cleaner("product.html?id=200&foo=bar&name=wired#id20", ['id', 'foo']))
        self.assertEqual('product.html?foo=bar&name=wired',
                url_query_cleaner("product.html?id=200&foo=bar&name=wired", ['id'], remove=True))
        self.assertEqual('product.html?name=wired',
                url_query_cleaner("product.html?id=2&foo=bar&name=wired", ['id', 'foo'], remove=True))
        self.assertEqual('product.html?foo=bar&name=wired',
                url_query_cleaner("product.html?id=2&foo=bar&name=wired", ['id', 'footo'], remove=True))
        self.assertEqual('product.html',
                url_query_cleaner("product.html", ['id'], remove=True))
        self.assertEqual('product.html',
                url_query_cleaner("product.html?&", ['id'], remove=True))
        self.assertEqual('product.html?foo=bar',
                url_query_cleaner("product.html?foo=bar&name=wired", 'foo'))
        self.assertEqual('product.html?foobar=wired',
                url_query_cleaner("product.html?foo=bar&foobar=wired", 'foobar'))

    def test_url_query_cleaner_keep_fragments(self):
        self.assertEqual('product.html?id=200#foo',
                url_query_cleaner("product.html?id=200&foo=bar&name=wired#foo",
                                  ['id'],
                                  keep_fragments=True))

    def test_path_to_file_uri(self):
        if os.name == 'nt':
            self.assertEqual(path_to_file_uri(r"C:\\windows\clock.avi"),
                             "file:///C:/windows/clock.avi")
        else:
            self.assertEqual(path_to_file_uri("/some/path.txt"),
                             "file:///some/path.txt")

        fn = "test.txt"
        x = path_to_file_uri(fn)
        self.assertTrue(x.startswith('file:///'))
        self.assertEqual(file_uri_to_path(x).lower(), os.path.abspath(fn).lower())

    def test_file_uri_to_path(self):
        if os.name == 'nt':
            self.assertEqual(file_uri_to_path("file:///C:/windows/clock.avi"),
                             r"C:\\windows\clock.avi")
            uri = "file:///C:/windows/clock.avi"
            uri2 = path_to_file_uri(file_uri_to_path(uri))
            self.assertEqual(uri, uri2)
        else:
            self.assertEqual(file_uri_to_path("file:///path/to/test.txt"),
                             "/path/to/test.txt")
            self.assertEqual(file_uri_to_path("/path/to/test.txt"),
                             "/path/to/test.txt")
            uri = "file:///path/to/test.txt"
            uri2 = path_to_file_uri(file_uri_to_path(uri))
            self.assertEqual(uri, uri2)

        self.assertEqual(file_uri_to_path("test.txt"),
                         "test.txt")

    def test_any_to_uri(self):
        if os.name == 'nt':
            self.assertEqual(any_to_uri(r"C:\\windows\clock.avi"),
                             "file:///C:/windows/clock.avi")
        else:
            self.assertEqual(any_to_uri("/some/path.txt"),
                             "file:///some/path.txt")
        self.assertEqual(any_to_uri("file:///some/path.txt"),
                         "file:///some/path.txt")
        self.assertEqual(any_to_uri("http://www.example.com/some/path.txt"),
                         "http://www.example.com/some/path.txt")

    def test_urljoin_rfc_deprecated(self):
        jurl = urljoin_rfc("http://www.example.com/", "/test")
        self.assertEqual(jurl, b"http://www.example.com/test")


class CanonicalizeUrlTest(unittest.TestCase):

    def test_canonicalize_url(self):
        # simplest case
        self.assertEqual(canonicalize_url("http://www.example.com/"),
                                          "http://www.example.com/")

    def test_return_str(self):
        assert isinstance(canonicalize_url(u"http://www.example.com"), str)
        assert isinstance(canonicalize_url(b"http://www.example.com"), str)

    def test_append_missing_path(self):
        self.assertEqual(canonicalize_url("http://www.example.com"),
                                          "http://www.example.com/")

    def test_typical_usage(self):
        self.assertEqual(canonicalize_url("http://www.example.com/do?a=1&b=2&c=3"),
                                          "http://www.example.com/do?a=1&b=2&c=3")
        self.assertEqual(canonicalize_url("http://www.example.com/do?c=1&b=2&a=3"),
                                          "http://www.example.com/do?a=3&b=2&c=1")
        self.assertEqual(canonicalize_url("http://www.example.com/do?&a=1"),
                                          "http://www.example.com/do?a=1")

    def test_port_number(self):
        self.assertEqual(canonicalize_url("http://www.example.com:8888/do?a=1&b=2&c=3"),
                                          "http://www.example.com:8888/do?a=1&b=2&c=3")
        # trailing empty ports are removed
        self.assertEqual(canonicalize_url("http://www.example.com:/do?a=1&b=2&c=3"),
                                          "http://www.example.com/do?a=1&b=2&c=3")

    def test_sorting(self):
        self.assertEqual(canonicalize_url("http://www.example.com/do?c=3&b=5&b=2&a=50"),
                                          "http://www.example.com/do?a=50&b=2&b=5&c=3")

    def test_keep_blank_values(self):
        self.assertEqual(canonicalize_url("http://www.example.com/do?b=&a=2", keep_blank_values=False),
                                          "http://www.example.com/do?a=2")
        self.assertEqual(canonicalize_url("http://www.example.com/do?b=&a=2"),
                                          "http://www.example.com/do?a=2&b=")
        self.assertEqual(canonicalize_url("http://www.example.com/do?b=&c&a=2", keep_blank_values=False),
                                          "http://www.example.com/do?a=2")
        self.assertEqual(canonicalize_url("http://www.example.com/do?b=&c&a=2"),
                                          "http://www.example.com/do?a=2&b=&c=")

        self.assertEqual(canonicalize_url(u'http://www.example.com/do?1750,4'),
                                           'http://www.example.com/do?1750%2C4=')

    def test_spaces(self):
        self.assertEqual(canonicalize_url("http://www.example.com/do?q=a space&a=1"),
                                          "http://www.example.com/do?a=1&q=a+space")
        self.assertEqual(canonicalize_url("http://www.example.com/do?q=a+space&a=1"),
                                          "http://www.example.com/do?a=1&q=a+space")
        self.assertEqual(canonicalize_url("http://www.example.com/do?q=a%20space&a=1"),
                                          "http://www.example.com/do?a=1&q=a+space")

    def test_canonicalize_url_unicode_path(self):
        self.assertEqual(canonicalize_url(u"http://www.example.com/résumé"),
                                          "http://www.example.com/r%C3%A9sum%C3%A9")

    def test_canonicalize_url_unicode_query_string(self):
        # default encoding for path and query is UTF-8
        self.assertEqual(canonicalize_url(u"http://www.example.com/résumé?q=résumé"),
                                          "http://www.example.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9")

        # passed encoding will affect query string
        self.assertEqual(canonicalize_url(u"http://www.example.com/résumé?q=résumé", encoding='latin1'),
                                          "http://www.example.com/r%C3%A9sum%C3%A9?q=r%E9sum%E9")

        self.assertEqual(canonicalize_url(u"http://www.example.com/résumé?country=Россия", encoding='cp1251'),
                                          "http://www.example.com/r%C3%A9sum%C3%A9?country=%D0%EE%F1%F1%E8%FF")

    def test_canonicalize_url_unicode_query_string_wrong_encoding(self):
        # trying to encode with wrong encoding
        # fallback to UTF-8
        self.assertEqual(canonicalize_url(u"http://www.example.com/résumé?currency=€", encoding='latin1'),
                                          "http://www.example.com/r%C3%A9sum%C3%A9?currency=%E2%82%AC")

        self.assertEqual(canonicalize_url(u"http://www.example.com/résumé?country=Россия", encoding='latin1'),
                                          "http://www.example.com/r%C3%A9sum%C3%A9?country=%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D1%8F")

    def test_normalize_percent_encoding_in_paths(self):
        self.assertEqual(canonicalize_url("http://www.example.com/r%c3%a9sum%c3%a9"),
                                          "http://www.example.com/r%C3%A9sum%C3%A9")

        # non-UTF8 encoded sequences: they should be kept untouched, only upper-cased
        # 'latin1'-encoded sequence in path
        self.assertEqual(canonicalize_url("http://www.example.com/a%a3do"),
                                          "http://www.example.com/a%A3do")

        # 'latin1'-encoded path, UTF-8 encoded query string
        self.assertEqual(canonicalize_url("http://www.example.com/a%a3do?q=r%c3%a9sum%c3%a9"),
                                          "http://www.example.com/a%A3do?q=r%C3%A9sum%C3%A9")

        # 'latin1'-encoded path and query string
        self.assertEqual(canonicalize_url("http://www.example.com/a%a3do?q=r%e9sum%e9"),
                                          "http://www.example.com/a%A3do?q=r%E9sum%E9")

    def test_normalize_percent_encoding_in_query_arguments(self):
        self.assertEqual(canonicalize_url("http://www.example.com/do?k=b%a3"),
                                          "http://www.example.com/do?k=b%A3")

        self.assertEqual(canonicalize_url("http://www.example.com/do?k=r%c3%a9sum%c3%a9"),
                                          "http://www.example.com/do?k=r%C3%A9sum%C3%A9")

    def test_non_ascii_percent_encoding_in_paths(self):
        self.assertEqual(canonicalize_url("http://www.example.com/a do?a=1"),
                                          "http://www.example.com/a%20do?a=1"),
        self.assertEqual(canonicalize_url("http://www.example.com/a %20do?a=1"),
                                          "http://www.example.com/a%20%20do?a=1"),
        self.assertEqual(canonicalize_url(u"http://www.example.com/a do£.html?a=1"),
                                          "http://www.example.com/a%20do%C2%A3.html?a=1")
        self.assertEqual(canonicalize_url(b"http://www.example.com/a do\xc2\xa3.html?a=1"),
                                          "http://www.example.com/a%20do%C2%A3.html?a=1")

    def test_non_ascii_percent_encoding_in_query_arguments(self):
        self.assertEqual(canonicalize_url(u"http://www.example.com/do?price=£500&a=5&z=3"),
                                          u"http://www.example.com/do?a=5&price=%C2%A3500&z=3")
        self.assertEqual(canonicalize_url(b"http://www.example.com/do?price=\xc2\xa3500&a=5&z=3"),
                                          "http://www.example.com/do?a=5&price=%C2%A3500&z=3")
        self.assertEqual(canonicalize_url(b"http://www.example.com/do?price(\xc2\xa3)=500&a=1"),
                                          "http://www.example.com/do?a=1&price%28%C2%A3%29=500")

    def test_urls_with_auth_and_ports(self):
        self.assertEqual(canonicalize_url(u"http://user:pass@www.example.com:81/do?now=1"),
                                          u"http://user:pass@www.example.com:81/do?now=1")

    def test_remove_fragments(self):
        self.assertEqual(canonicalize_url(u"http://user:pass@www.example.com/do?a=1#frag"),
                                          u"http://user:pass@www.example.com/do?a=1")
        self.assertEqual(canonicalize_url(u"http://user:pass@www.example.com/do?a=1#frag", keep_fragments=True),
                                          u"http://user:pass@www.example.com/do?a=1#frag")

    def test_dont_convert_safe_characters(self):
        # dont convert safe characters to percent encoding representation
        self.assertEqual(canonicalize_url(
            "http://www.simplybedrooms.com/White-Bedroom-Furniture/Bedroom-Mirror:-Josephine-Cheval-Mirror.html"),
            "http://www.simplybedrooms.com/White-Bedroom-Furniture/Bedroom-Mirror:-Josephine-Cheval-Mirror.html")

    def test_safe_characters_unicode(self):
        # urllib.quote uses a mapping cache of encoded characters. when parsing
        # an already percent-encoded url, it will fail if that url was not
        # percent-encoded as utf-8, that's why canonicalize_url must always
        # convert the urls to string. the following test asserts that
        # functionality.
        self.assertEqual(canonicalize_url(u'http://www.example.com/caf%E9-con-leche.htm'),
                                           'http://www.example.com/caf%E9-con-leche.htm')

    def test_domains_are_case_insensitive(self):
        self.assertEqual(canonicalize_url("http://www.EXAMPLE.com/"),
                                          "http://www.example.com/")

    def test_canonicalize_idns(self):
        self.assertEqual(canonicalize_url(u'http://www.bücher.de?q=bücher'),
                                           'http://www.xn--bcher-kva.de/?q=b%C3%BCcher')
        # Japanese (+ reordering query parameters)
        self.assertEqual(canonicalize_url(u'http://はじめよう.みんな/?query=サ&maxResults=5'),
                                           'http://xn--p8j9a0d9c9a.xn--q9jyb4c/?maxResults=5&query=%E3%82%B5')

    def test_quoted_slash_and_question_sign(self):
        self.assertEqual(canonicalize_url("http://foo.com/AC%2FDC+rocks%3f/?yeah=1"),
                         "http://foo.com/AC%2FDC+rocks%3F/?yeah=1")
        self.assertEqual(canonicalize_url("http://foo.com/AC%2FDC/"),
                         "http://foo.com/AC%2FDC/")

    def test_canonicalize_urlparsed(self):
        # canonicalize_url() can be passed an already urlparse'd URL
        self.assertEqual(canonicalize_url(urlparse(u"http://www.example.com/résumé?q=résumé")),
                                          "http://www.example.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9")
        self.assertEqual(canonicalize_url(urlparse('http://www.example.com/caf%e9-con-leche.htm')),
                                          'http://www.example.com/caf%E9-con-leche.htm')
        self.assertEqual(canonicalize_url(urlparse("http://www.example.com/a%a3do?q=r%c3%a9sum%c3%a9")),
                                          "http://www.example.com/a%A3do?q=r%C3%A9sum%C3%A9")

    def test_canonicalize_parse_url(self):
        # parse_url() wraps urlparse and is used in link extractors
        self.assertEqual(canonicalize_url(parse_url(u"http://www.example.com/résumé?q=résumé")),
                                          "http://www.example.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9")
        self.assertEqual(canonicalize_url(parse_url('http://www.example.com/caf%e9-con-leche.htm')),
                                          'http://www.example.com/caf%E9-con-leche.htm')
        self.assertEqual(canonicalize_url(parse_url("http://www.example.com/a%a3do?q=r%c3%a9sum%c3%a9")),
                                          "http://www.example.com/a%A3do?q=r%C3%A9sum%C3%A9")

    def test_canonicalize_url_idempotence(self):
        for url, enc in [(u'http://www.bücher.de/résumé?q=résumé', 'utf8'),
                         (u'http://www.example.com/résumé?q=résumé', 'latin1'),
                         (u'http://www.example.com/résumé?country=Россия', 'cp1251'),
                         #(u'http://はじめよう.みんな/?query=サ&maxResults=5', 'iso2022jp')
        ]:
            canonicalized = canonicalize_url(url, encoding=enc)

            # if we canonicalize again, we ge the same result
            self.assertEqual(canonicalize_url(canonicalized, encoding=enc), canonicalized)

            # without encoding, already canonicalized URL is canonicalized identically
            self.assertEqual(canonicalize_url(canonicalized), canonicalized)

    def test_canonicalize_url_idna_exceptions(self):
        # missing DNS label
        self.assertEqual(
            canonicalize_url(u"http://.example.com/résumé?q=résumé"),
            "http://.example.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9")

        # DNS label too long
        self.assertEqual(
            canonicalize_url(
                u"http://www.{label}.com/résumé?q=résumé".format(
                    label=u"example"*11)),
            "http://www.{label}.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9".format(
                    label=u"example"*11))


class DataURITests(unittest.TestCase):

    def test_default_mediatype_charset(self):
        result = parse_data_uri("data:,A%20brief%20note")
        self.assertEqual(result.media_type, "text/plain")
        self.assertEqual(result.media_type_parameters, {"charset": "US-ASCII"})
        self.assertEqual(result.data, b"A brief note")

    def test_text_uri(self):
        result = parse_data_uri(u"data:,A%20brief%20note")
        self.assertEqual(result.data, b"A brief note")

    def test_bytes_uri(self):
        result = parse_data_uri(b"data:,A%20brief%20note")
        self.assertEqual(result.data, b"A brief note")

    def test_unicode_uri(self):
        result = parse_data_uri(u"data:,é")
        self.assertEqual(result.data, u"é".encode('utf-8'))

    def test_default_mediatype(self):
        result = parse_data_uri("data:;charset=iso-8859-7,%be%d3%be")
        self.assertEqual(result.media_type, "text/plain")
        self.assertEqual(result.media_type_parameters,
                         {"charset": "iso-8859-7"})
        self.assertEqual(result.data, b"\xbe\xd3\xbe")

    def test_text_charset(self):
        result = parse_data_uri("data:text/plain;charset=iso-8859-7,%be%d3%be")
        self.assertEqual(result.media_type, "text/plain")
        self.assertEqual(result.media_type_parameters,
                         {"charset": "iso-8859-7"})
        self.assertEqual(result.data, b"\xbe\xd3\xbe")

    def test_mediatype_parameters(self):
        result = parse_data_uri('data:text/plain;'
                                'foo=%22foo;bar%5C%22%22;'
                                'charset=utf-8;'
                                'bar=%22foo;%5C%22foo%20;/%20,%22,'
                                '%CE%8E%CE%A3%CE%8E')

        self.assertEqual(result.media_type, "text/plain")
        self.assertEqual(result.media_type_parameters,
                         {"charset": "utf-8",
                          "foo": 'foo;bar"',
                          "bar": 'foo;"foo ;/ ,'})
        self.assertEqual(result.data, b"\xce\x8e\xce\xa3\xce\x8e")

    def test_base64(self):
        result = parse_data_uri("data:text/plain;base64,"
                                "SGVsbG8sIHdvcmxkLg%3D%3D")
        self.assertEqual(result.media_type, "text/plain")
        self.assertEqual(result.data, b"Hello, world.")

    def test_base64_spaces(self):
        result = parse_data_uri("data:text/plain;base64,SGVsb%20G8sIH%0A%20%20"
                                "dvcm%20%20%20xk%20Lg%3D%0A%3D")
        self.assertEqual(result.media_type, "text/plain")
        self.assertEqual(result.data, b"Hello, world.")

        result = parse_data_uri("data:text/plain;base64,SGVsb G8sIH\n  "
                                "dvcm   xk Lg%3D\n%3D")
        self.assertEqual(result.media_type, "text/plain")
        self.assertEqual(result.data, b"Hello, world.")

    def test_wrong_base64_param(self):
        with self.assertRaises(ValueError):
            parse_data_uri("data:text/plain;baes64,SGVsbG8sIHdvcmxkLg%3D%3D")

    def test_missing_comma(self):
        with self.assertRaises(ValueError):
            parse_data_uri("data:A%20brief%20note")

    def test_missing_scheme(self):
        with self.assertRaises(ValueError):
            parse_data_uri("text/plain,A%20brief%20note")

    def test_wrong_scheme(self):
        with self.assertRaises(ValueError):
            parse_data_uri("http://example.com/")

    def test_scheme_case_insensitive(self):
        result = parse_data_uri("DATA:,A%20brief%20note")
        self.assertEqual(result.data, b"A brief note")
        result = parse_data_uri("DaTa:,A%20brief%20note")
        self.assertEqual(result.data, b"A brief note")


if __name__ == "__main__":
    unittest.main()

