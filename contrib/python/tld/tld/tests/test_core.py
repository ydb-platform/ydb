# -*- coding: utf-8 -*-

import copy
import logging
import unittest
from os.path import abspath, join, dirname
from tempfile import gettempdir
from typing import Type
from urllib.parse import SplitResult, urlsplit

from faker import Faker

from tld import defaults
from tld.base import BaseTLDSourceParser, Registry
from tld.conf import get_setting, reset_settings, set_setting
from tld.exceptions import (
    TldBadUrl,
    TldDomainNotFound,
    TldImproperlyConfigured,
    TldIOError,
)
from tld.helpers import project_dir
from tld.utils import (
    BaseMozillaTLDSourceParser,
    MozillaTLDSourceParser,
    get_fld,
    get_tld,
    get_tld_names,
    get_tld_names_container,
    is_tld,
    parse_tld,
    reset_tld_names,
    update_tld_names,
    update_tld_names_cli,
)
from .base import internet_available_only, log_info

import yatest.common as yc
TEST_DIR = abspath(dirname(yc.source_path(__file__)))
TLD_NAMES_LOCAL_PATH_CUSTOM = join(TEST_DIR, "res", "effective_tld_names_custom.dat.txt")

__author__ = "Artur Barseghyan"
__copyright__ = "2013-2025 Artur Barseghyan"
__license__ = "MPL-1.1 OR GPL-2.0-only OR LGPL-2.1-or-later"
__all__ = ("TestCore",)

LOGGER = logging.getLogger(__name__)


class TestCore(unittest.TestCase):
    """Core tld functionality tests."""

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = gettempdir()

    def setUp(self):
        """Set up."""
        self.good_patterns = [
            {
                "url": "http://www.google.co.uk",
                "fld": "google.co.uk",
                "subdomain": "www",
                "domain": "google",
                "suffix": "co.uk",
                "tld": "co.uk",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "http://www.v2.google.co.uk",
                "fld": "google.co.uk",
                "subdomain": "www.v2",
                "domain": "google",
                "suffix": "co.uk",
                "tld": "co.uk",
                "kwargs": {"fail_silently": True},
            },
            # No longer valid
            # {
            #    'url': 'http://www.me.congresodelalengua3.ar',
            #    'tld': 'me.congresodelalengua3.ar',
            #    'subdomain': 'www',
            #    'domain': 'me',
            #    'suffix': 'congresodelalengua3.ar',
            # },
            {
                "url": "http://хром.гугл.рф",
                "fld": "гугл.рф",
                "subdomain": "хром",
                "domain": "гугл",
                "suffix": "рф",
                "tld": "рф",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "http://www.google.co.uk:8001/lorem-ipsum/",
                "fld": "google.co.uk",
                "subdomain": "www",
                "domain": "google",
                "suffix": "co.uk",
                "tld": "co.uk",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "http://www.me.cloudfront.net",
                "fld": "me.cloudfront.net",
                "subdomain": "www",
                "domain": "me",
                "suffix": "cloudfront.net",
                "tld": "cloudfront.net",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "http://www.v2.forum.tech.google.co.uk:8001/"
                "lorem-ipsum/",
                "fld": "google.co.uk",
                "subdomain": "www.v2.forum.tech",
                "domain": "google",
                "suffix": "co.uk",
                "tld": "co.uk",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "https://pantheon.io/",
                "fld": "pantheon.io",
                "subdomain": "",
                "domain": "pantheon",
                "suffix": "io",
                "tld": "io",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "v2.www.google.com",
                "fld": "google.com",
                "subdomain": "v2.www",
                "domain": "google",
                "suffix": "com",
                "tld": "com",
                "kwargs": {"fail_silently": True, "fix_protocol": True},
            },
            {
                "url": "//v2.www.google.com",
                "fld": "google.com",
                "subdomain": "v2.www",
                "domain": "google",
                "suffix": "com",
                "tld": "com",
                "kwargs": {"fail_silently": True, "fix_protocol": True},
            },
            {
                "url": "http://foo@bar.com",
                "fld": "bar.com",
                "subdomain": "",
                "domain": "bar",
                "suffix": "com",
                "tld": "com",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "http://user:foo@bar.com",
                "fld": "bar.com",
                "subdomain": "",
                "domain": "bar",
                "suffix": "com",
                "tld": "com",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "https://faguoren.xn--fiqs8s",
                "fld": "faguoren.xn--fiqs8s",
                "subdomain": "",
                "domain": "faguoren",
                "suffix": "xn--fiqs8s",
                "tld": "xn--fiqs8s",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "blogs.lemonde.paris",
                "fld": "lemonde.paris",
                "subdomain": "blogs",
                "domain": "lemonde",
                "suffix": "paris",
                "tld": "paris",
                "kwargs": {"fail_silently": True, "fix_protocol": True},
            },
            {
                "url": "axel.brighton.ac.uk",
                "fld": "brighton.ac.uk",
                "subdomain": "axel",
                "domain": "brighton",
                "suffix": "ac.uk",
                "tld": "ac.uk",
                "kwargs": {"fail_silently": True, "fix_protocol": True},
            },
            {
                "url": "m.fr.blogspot.com.au",
                "fld": "blogspot.com.au",
                "subdomain": "m.fr",
                "domain": "blogspot",
                "suffix": "com.au",
                "tld": "com.au",
                "kwargs": {"fail_silently": True, "fix_protocol": True},
            },
            {
                "url": "help.www.福岡.jp",
                "fld": "www.福岡.jp",
                "subdomain": "help",
                "domain": "www",
                "suffix": "福岡.jp",
                "tld": "福岡.jp",
                "kwargs": {"fail_silently": True, "fix_protocol": True},
            },
            {
                "url": "syria.arabic.variant.سوريا",
                "fld": "variant.سوريا",
                "subdomain": "syria.arabic",
                "domain": "variant",
                "suffix": "سوريا",
                "tld": "سوريا",
                "kwargs": {"fail_silently": True, "fix_protocol": True},
            },
            {
                "url": "http://www.help.kawasaki.jp",
                "fld": "www.help.kawasaki.jp",
                "subdomain": "",
                "domain": "www",
                "suffix": "help.kawasaki.jp",
                "tld": "help.kawasaki.jp",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "http://www.city.kawasaki.jp",
                "fld": "city.kawasaki.jp",
                "subdomain": "www",
                "domain": "city",
                "suffix": "kawasaki.jp",
                "tld": "kawasaki.jp",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "http://fedoraproject.org",
                "fld": "fedoraproject.org",
                "subdomain": "",
                "domain": "fedoraproject",
                "suffix": "org",
                "tld": "org",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "http://www.cloud.fedoraproject.org",
                "fld": "www.cloud.fedoraproject.org",
                "subdomain": "",
                "domain": "www",
                "suffix": "cloud.fedoraproject.org",
                "tld": "cloud.fedoraproject.org",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "https://www.john.app.os.fedoraproject.org",
                "fld": "john.app.os.fedoraproject.org",
                "subdomain": "www",
                "domain": "john",
                "suffix": "app.os.fedoraproject.org",
                "tld": "app.os.fedoraproject.org",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "ftp://www.xn--mxail5aa.xn--11b4c3d",
                "fld": "xn--mxail5aa.xn--11b4c3d",
                "subdomain": "www",
                "domain": "xn--mxail5aa",
                "suffix": "xn--11b4c3d",
                "tld": "xn--11b4c3d",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "http://cloud.fedoraproject.org",
                "fld": "cloud.fedoraproject.org",
                "subdomain": "",
                "domain": "cloud.fedoraproject.org",
                "suffix": "cloud.fedoraproject.org",
                "tld": "cloud.fedoraproject.org",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "github.io",
                "fld": "github.io",
                "subdomain": "",
                "domain": "github.io",
                "suffix": "github.io",
                "tld": "github.io",
                "kwargs": {"fail_silently": True, "fix_protocol": True},
            },
            {
                "url": urlsplit("http://lemonde.fr/article.html"),
                "fld": "lemonde.fr",
                "subdomain": "",
                "domain": "lemonde",
                "suffix": "fr",
                "tld": "fr",
                "kwargs": {"fail_silently": True},
            },
            {
                "url": "https://github.com....../barseghyanartur/tld/",
                "fld": "github.com",
                "subdomain": "",
                "domain": "github",
                "suffix": "com",
                "tld": "com",
                "kwargs": {"fail_silently": True},
            },
        ]

        self.bad_patterns = {
            "v2.www.google.com": {
                "exception": TldBadUrl,
            },
            "/index.php?a=1&b=2": {
                "exception": TldBadUrl,
            },
            "http://www.tld.doesnotexist": {
                "exception": TldDomainNotFound,
            },
            "https://2001:0db8:0000:85a3:0000:0000:ac1f:8001": {
                "exception": TldDomainNotFound,
            },
            "http://192.169.1.1": {
                "exception": TldDomainNotFound,
            },
            "http://localhost:8080": {
                "exception": TldDomainNotFound,
            },
            "https://localhost": {
                "exception": TldDomainNotFound,
            },
            "https://localhost2": {
                "exception": TldImproperlyConfigured,
                "kwargs": {"search_public": False, "search_private": False},
            },
        }

        self.invalid_tlds = {
            "v2.www.google.com",
            "tld.doesnotexist",
            "2001:0db8:0000:85a3:0000:0000:ac1f",
            "192.169.1.1",
            "localhost",
            "google.com",
        }

        self.tld_names_local_path_custom = TLD_NAMES_LOCAL_PATH_CUSTOM
        self.good_patterns_custom_parser = [
            {
                "url": "http://www.foreverchild",
                "fld": "www.foreverchild",
                "subdomain": "",
                "domain": "www",
                "suffix": "foreverchild",
                "tld": "foreverchild",
                "kwargs": {
                    "fail_silently": True,
                    # 'parser_class': self.get_custom_parser_class(),
                },
            },
            {
                "url": "http://www.v2.foreverchild",
                "fld": "v2.foreverchild",
                "subdomain": "www",
                "domain": "v2",
                "suffix": "foreverchild",
                "tld": "foreverchild",
                "kwargs": {
                    "fail_silently": True,
                    # 'parser_class': self.get_custom_parser_class(),
                },
            },
        ]
        reset_settings()

    def tearDown(self):
        """Tear down."""
        reset_settings()
        Registry.reset()

    @property
    def good_url(self):
        return self.good_patterns[0]["url"]

    @property
    def bad_url(self):
        return list(self.bad_patterns.keys())[0]

    def get_custom_parser_class(
        self,
        uid: str = "custom_mozilla",
        source_url: str = None,
        local_path: str = TLD_NAMES_LOCAL_PATH_CUSTOM,
    ) -> Type[BaseTLDSourceParser]:
        # Define a custom TLD source parser class
        parser_class = type(
            "CustomMozillaTLDSourceParser",
            (BaseMozillaTLDSourceParser,),
            {
                "uid": uid,
                "source_url": source_url,
                "local_path": local_path,
            },
        )
        return parser_class

    @log_info
    def test_0_tld_names_loaded(self):
        """Test if tld names are loaded."""
        get_fld("http://www.google.co.uk")
        from tld.utils import tld_names

        res = len(tld_names) > 0
        self.assertTrue(res)
        return res

    @internet_available_only
    @log_info
    def test_1_update_tld_names(self):
        """Test updating the tld names (re-fetch mozilla source)."""
        res = update_tld_names(fail_silently=False)
        self.assertTrue(res)
        return res

    @log_info
    def test_2_fld_good_patterns_pass(self):
        """Test good URL patterns."""
        res = []
        for data in self.good_patterns:
            _res = get_fld(data["url"], **data["kwargs"])
            self.assertEqual(_res, data["fld"])
            res.append(_res)
        return res

    @log_info
    def test_3_fld_bad_patterns_pass(self):
        """Test bad URL patterns."""
        res = []
        for url, params in self.bad_patterns.items():
            _res = get_fld(url, fail_silently=True)
            self.assertEqual(_res, None)
            res.append(_res)
        return res

    @log_info
    def test_4_override_settings(self):
        """Testing settings override."""

        def override_settings():
            """Override settings."""
            return get_setting("DEBUG")

        self.assertEqual(defaults.DEBUG, override_settings())

        set_setting("DEBUG", True)

        self.assertEqual(True, override_settings())

        return override_settings()

    @log_info
    def test_5_tld_good_patterns_pass_parsed_object(self):
        """Test good URL patterns."""
        res = []
        for data in self.good_patterns:
            kwargs = copy.copy(data["kwargs"])
            kwargs["as_object"] = True
            _res = get_tld(data["url"], **kwargs)
            self.assertEqual(_res.tld, data["tld"])
            self.assertEqual(_res.subdomain, data["subdomain"])
            self.assertEqual(_res.domain, data["domain"])
            self.assertEqual(_res.suffix, data["suffix"])
            self.assertEqual(_res.fld, data["fld"])

            self.assertEqual(
                str(_res).encode("utf8"), data["tld"].encode("utf8")
            )

            self.assertEqual(
                _res.__dict__,
                {
                    "tld": _res.tld,
                    "domain": _res.domain,
                    "subdomain": _res.subdomain,
                    "fld": _res.fld,
                    "parsed_url": _res.parsed_url,
                },
            )

            res.append(_res)
        return res

    @unittest.skip("")
    @log_info
    def test_6_override_full_names_path(self):
        default = project_dir("dummy.txt")
        override_base = "/tmp/test"
        set_setting("NAMES_LOCAL_PATH_PARENT", override_base)
        modified = project_dir("dummy.txt")
        self.assertNotEqual(default, modified)
        self.assertEqual(modified, abspath("/tmp/test/dummy.txt"))

    @log_info
    def test_7_public_private(self):
        res = get_fld(
            "http://silly.cc.ua",
            fail_silently=True,
            search_private=False,
            parser_class=MozillaTLDSourceParser,
        )
        self.assertEqual(res, None)

        res = get_fld(
            "http://silly.cc.ua", fail_silently=True, search_private=False
        )
        self.assertEqual(res, "cc.ua")

        res = get_fld(
            "http://silly.cc.ua", fail_silently=True, search_private=True
        )
        self.assertEqual(res, "silly.cc.ua")

        res = get_fld(
            "mercy.compute.amazonaws.com",
            fail_silently=True,
            search_private=False,
            fix_protocol=True,
            parser_class=MozillaTLDSourceParser,
        )
        self.assertEqual(res, None)

        res = get_fld(
            "mercy.compute.amazonaws.com",
            fail_silently=True,
            search_private=False,
            fix_protocol=True,
        )
        self.assertEqual(res, "amazonaws.com")

        res = get_fld(
            "http://whatever.com", fail_silently=True, search_public=False
        )
        self.assertEqual(res, None)

    @log_info
    def test_8_fld_bad_patterns_exceptions(self):
        """Test exceptions."""
        res = []
        for url, params in self.bad_patterns.items():
            kwargs = params["kwargs"] if "kwargs" in params else {}
            kwargs["fail_silently"] = False
            with self.assertRaises(params["exception"]):
                _res = get_fld(url, **kwargs)
                res.append(_res)
        return res

    @log_info
    def test_9_tld_good_patterns_pass(self):
        """Test `get_tld` good URL patterns."""
        res = []
        for data in self.good_patterns:
            _res = get_tld(data["url"], **data["kwargs"])
            self.assertEqual(_res, data["tld"])
            res.append(_res)
        return res

    @log_info
    def test_10_tld_bad_patterns_pass(self):
        """Test `get_tld` bad URL patterns."""
        res = []
        for url, params in self.bad_patterns.items():
            _res = get_tld(url, fail_silently=True)
            self.assertEqual(_res, None)
            res.append(_res)
        return res

    @log_info
    def test_11_parse_tld_good_patterns(self):
        """Test `parse_tld` good URL patterns."""
        res = []
        for data in self.good_patterns:
            _res = parse_tld(data["url"], **data["kwargs"])
            self.assertEqual(
                _res, (data["tld"], data["domain"], data["subdomain"])
            )
            res.append(_res)
        return res

    @log_info
    def test_12_is_tld_good_patterns(self):
        """Test `is_tld` good URL patterns."""
        for data in self.good_patterns:
            self.assertTrue(is_tld(data["tld"]))

    @log_info
    def test_13_is_tld_bad_patterns(self):
        """Test `is_tld` bad URL patterns."""
        for _tld in self.invalid_tlds:
            self.assertFalse(is_tld(_tld))

    @log_info
    def test_14_fail_update_tld_names(self):
        """Test fail `update_tld_names`."""
        parser_class = self.get_custom_parser_class(
            uid="custom_mozilla_2", source_url="i-do-not-exist"
        )
        # Assert raise TldIOError on wrong NAMES_SOURCE_URL
        with self.assertRaises(TldIOError):
            update_tld_names(fail_silently=False, parser_uid=parser_class.uid)

        # Assert return False on wrong NAMES_SOURCE_URL
        self.assertFalse(
            update_tld_names(fail_silently=True, parser_uid=parser_class.uid)
        )

    @log_info
    def test_15_fail_get_tld_names(self):
        """Test fail `update_tld_names`."""
        parser_class = self.get_custom_parser_class(
            uid="custom_mozilla_3",
            source_url="i-do-not-exist",
            local_path="/srv/tests/res/effective_tld_names_custom_3.dat.txt",
        )
        reset_tld_names()
        # Assert raise TldIOError on wrong NAMES_SOURCE_URL
        for params in self.good_patterns:
            kwargs = {"url": params["url"]}
            kwargs.update(params["kwargs"])
            kwargs["fail_silently"] = False
            kwargs["parser_class"] = parser_class
            with self.assertRaises(TldIOError):
                get_tld(**kwargs)

    @log_info
    def test_16_fail_get_fld_wrong_kwargs(self):
        """Test fail `get_fld` with wrong kwargs."""
        with self.assertRaises(TldImproperlyConfigured):
            get_fld(self.good_url, as_object=True)

    @log_info
    def test_17_fail_parse_tld(self):
        """Test fail `parse_tld`.

        Assert raise TldIOError on wrong `NAMES_SOURCE_URL` for `parse_tld`.
        """
        parser_class = self.get_custom_parser_class(source_url="i-do-not-exist")
        parsed_tld = parse_tld(
            self.bad_url, fail_silently=False, parser_class=parser_class
        )
        self.assertEqual(parsed_tld, (None, None, None))

    @log_info
    def test_18_get_tld_names_and_reset_tld_names(self):
        """Test fail `get_tld_names` and repair using `reset_tld_names`."""
        tmp_filename = join(gettempdir(), f"{Faker().uuid4()}.dat.txt")
        parser_class = self.get_custom_parser_class(
            source_url="i-do-not-exist", local_path=tmp_filename
        )
        reset_tld_names()

        with self.subTest("Assert raise TldIOError"):
            # Assert raise TldIOError on wrong NAMES_SOURCE_URL for
            # `get_tld_names`
            with self.assertRaises(TldIOError):
                get_tld_names(fail_silently=False, parser_class=parser_class)

        tmp_filename = join(gettempdir(), f"{Faker().uuid4()}.dat.txt")
        parser_class_2 = self.get_custom_parser_class(
            source_url="i-do-not-exist-2", local_path=tmp_filename
        )
        reset_tld_names()

        with self.subTest("Assert get None"):
            # Assert get None on wrong `NAMES_SOURCE_URL` for `get_tld_names`
            self.assertIsNone(
                get_tld_names(fail_silently=True, parser_class=parser_class_2)
            )

    @internet_available_only
    @log_info
    def test_19_update_tld_names_cli(self):
        """Test the return code of the CLI version of `update_tld_names`."""
        reset_tld_names()
        res = update_tld_names_cli()
        self.assertEqual(res, 0)

    @log_info
    def test_20_parse_tld_custom_tld_names_good_patterns(self):
        """Test `parse_tld` good URL patterns for custom tld names."""
        res = []

        for data in self.good_patterns_custom_parser:
            kwargs = copy.copy(data["kwargs"])
            kwargs["parser_class"] = self.get_custom_parser_class()
            _res = parse_tld(data["url"], **kwargs)
            self.assertEqual(
                _res, (data["tld"], data["domain"], data["subdomain"])
            )
            res.append(_res)
        return res

    @log_info
    def test_21_tld_custom_tld_names_good_patterns_pass_parsed_object(self):
        """Test `get_tld` good URL patterns for custom tld names."""
        res = []
        for data in self.good_patterns_custom_parser:
            kwargs = copy.copy(data["kwargs"])
            kwargs.update(
                {
                    "as_object": True,
                    "parser_class": self.get_custom_parser_class(),
                }
            )
            _res = get_tld(data["url"], **kwargs)
            self.assertEqual(_res.tld, data["tld"])
            self.assertEqual(_res.subdomain, data["subdomain"])
            self.assertEqual(_res.domain, data["domain"])
            self.assertEqual(_res.suffix, data["suffix"])
            self.assertEqual(_res.fld, data["fld"])

            self.assertEqual(
                str(_res).encode("utf8"), data["tld"].encode("utf8")
            )

            self.assertEqual(
                _res.__dict__,
                {
                    "tld": _res.tld,
                    "domain": _res.domain,
                    "subdomain": _res.subdomain,
                    "fld": _res.fld,
                    "parsed_url": _res.parsed_url,
                },
            )

            res.append(_res)
        return res

    @log_info
    def test_22_reset_tld_names_for_custom_parser(self):
        """Test `reset_tld_names` for `tld_names_local_path`."""
        res = []
        parser_class = self.get_custom_parser_class()
        for data in self.good_patterns_custom_parser:
            kwargs = copy.copy(data["kwargs"])
            kwargs.update(
                {
                    "as_object": True,
                    "parser_class": self.get_custom_parser_class(),
                }
            )
            _res = get_tld(data["url"], **kwargs)
            self.assertEqual(_res.tld, data["tld"])
            self.assertEqual(_res.subdomain, data["subdomain"])
            self.assertEqual(_res.domain, data["domain"])
            self.assertEqual(_res.suffix, data["suffix"])
            self.assertEqual(_res.fld, data["fld"])

            self.assertEqual(
                str(_res).encode("utf8"), data["tld"].encode("utf8")
            )

            self.assertEqual(
                _res.__dict__,
                {
                    "tld": _res.tld,
                    "domain": _res.domain,
                    "subdomain": _res.subdomain,
                    "fld": _res.fld,
                    "parsed_url": _res.parsed_url,
                },
            )

            res.append(_res)

        tld_names = get_tld_names_container()
        self.assertIn(parser_class.local_path, tld_names)
        reset_tld_names(parser_class.local_path)
        self.assertNotIn(parser_class.local_path, tld_names)

        return res

    @log_info
    def test_23_fail_define_custom_parser_class_without_uid(self):
        """Test fail define custom parser class without `uid`."""

        class CustomParser(BaseTLDSourceParser):
            pass

        class AnotherCustomParser(BaseTLDSourceParser):

            uid = "another-custom-parser"

        # Assert raise TldImproperlyConfigured
        with self.assertRaises(TldImproperlyConfigured):
            CustomParser.get_tld_names()

        # Assert raise NotImplementedError
        with self.assertRaises(NotImplementedError):
            AnotherCustomParser.get_tld_names()

    @log_info
    def test_24_len_trie_nodes(self):
        """Test len of the trie nodes."""
        get_tld("http://delusionalinsanity.com")
        tld_names = get_tld_names_container()
        self.assertGreater(len(tld_names[MozillaTLDSourceParser.local_path]), 0)

    @log_info
    def test_25_get_tld_names_no_arguments(self):
        """Test len of the trie nodes."""
        tld_names = get_tld_names()
        self.assertGreater(len(tld_names), 0)

    @log_info
    def test_26_case(self):
        res = get_tld(
            "https://MyDomain.com/AsDrFt?QUeRY=12aA",
            fail_silently=True,
            search_private=False,
            as_object=True,
        )
        self.assertEqual(res.tld, "com")
        self.assertEqual(res.domain, "mydomain")
        self.assertEqual(res.subdomain, "")
        self.assertEqual(res.fld, "mydomain.com")
        self.assertEqual(
            res.parsed_url,
            SplitResult(
                scheme="https",
                netloc="MyDomain.com",
                path="/AsDrFt",
                query="QUeRY=12aA",
                fragment="",
            ),
        )

    @log_info
    def test_27_tld_fail_silently_pass(self):
        """Test `get_tld` bad URL patterns that would raise exception
        if `fail_silently` isn't `True`.
        """
        res = []
        bad_url = ["https://user:password[@host.com", "https://user[@host.com"]
        for url in bad_url:
            _res = get_tld(url, fail_silently=True)
            self.assertEqual(_res, None)
            res.append(_res)
        return res
