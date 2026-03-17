import unittest

from pamqp import commands

# Tag uri examples from https://en.wikipedia.org/wiki/Tag_URI_scheme
TAG_URIS = [
    'tag:timothy@hpl.hp.com,2001:web/externalHome',
    'tag:sandro@w3.org,2004-05:Sandro',
    'tag:my-ids.com,2001-09-15:TimKindberg:presentations:UBath2004-05-19',
    'tag:blogger.com,1999:blog-555',
    'tag:yaml.org,2002:int#section1'
]


class TagUriScheme(unittest.TestCase):

    def test_tag_uri_scheme_tag1(self):
        commands.Exchange.Declare(exchange=TAG_URIS[0])

    def test_tag_uri_scheme_tag2(self):
        commands.Exchange.Declare(exchange=TAG_URIS[1])

    def test_tag_uri_scheme_tag3(self):
        commands.Exchange.Declare(exchange=TAG_URIS[2])

    def test_tag_uri_scheme_tag4(self):
        commands.Exchange.Declare(exchange=TAG_URIS[3])
