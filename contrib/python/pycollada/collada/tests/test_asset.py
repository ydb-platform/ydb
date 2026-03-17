import datetime

import collada
import unittest
from collada.xmlutil import etree

fromstring = etree.fromstring
tostring = etree.tostring


class TestAsset(unittest.TestCase):

    def setUp(self):
        self.dummy = collada.Collada(validate_output=True)

    def test_asset_contributor(self):
        contributor = collada.asset.Contributor()
        self.assertIsNone(contributor.author)
        self.assertIsNone(contributor.authoring_tool)
        self.assertIsNone(contributor.comments)
        self.assertIsNone(contributor.copyright)
        self.assertIsNone(contributor.source_data)

        contributor.save()
        contributor = collada.asset.Contributor.load(self.dummy, {}, fromstring(tostring(contributor.xmlnode)))
        self.assertIsNone(contributor.author)
        self.assertIsNone(contributor.authoring_tool)
        self.assertIsNone(contributor.comments)
        self.assertIsNone(contributor.copyright)
        self.assertIsNone(contributor.source_data)

        contributor.author = "author1"
        contributor.authoring_tool = "tool2"
        contributor.comments = "comments3"
        contributor.copyright = "copyright4"
        contributor.source_data = "data5"

        contributor.save()
        contributor = collada.asset.Contributor.load(self.dummy, {}, fromstring(tostring(contributor.xmlnode)))
        self.assertEqual(contributor.author, "author1")
        self.assertEqual(contributor.authoring_tool, "tool2")
        self.assertEqual(contributor.comments, "comments3")
        self.assertEqual(contributor.copyright, "copyright4")
        self.assertEqual(contributor.source_data, "data5")

    def test_asset(self):
        asset = collada.asset.Asset()

        self.assertIsNone(asset.title)
        self.assertIsNone(asset.subject)
        self.assertIsNone(asset.revision)
        self.assertIsNone(asset.keywords)
        self.assertIsNone(asset.unitname)
        self.assertIsNone(asset.unitmeter)
        self.assertEqual(asset.contributors, [])
        self.assertEqual(asset.upaxis, collada.asset.UP_AXIS.Y_UP)
        self.assertIsInstance(asset.created, datetime.datetime)
        self.assertIsInstance(asset.modified, datetime.datetime)

        asset.save()
        asset = collada.asset.Asset.load(self.dummy, {}, fromstring(tostring(asset.xmlnode)))

        self.assertIsNone(asset.title)
        self.assertIsNone(asset.subject)
        self.assertIsNone(asset.revision)
        self.assertIsNone(asset.keywords)
        self.assertIsNone(asset.unitname)
        self.assertIsNone(asset.unitmeter)
        self.assertEqual(asset.contributors, [])
        self.assertEqual(asset.upaxis, collada.asset.UP_AXIS.Y_UP)
        self.assertIsInstance(asset.created, datetime.datetime)
        self.assertIsInstance(asset.modified, datetime.datetime)

        asset.title = 'title1'
        asset.subject = 'subject2'
        asset.revision = 'revision3'
        asset.keywords = 'keywords4'
        asset.unitname = 'feet'
        asset.unitmeter = 3.1
        contrib1 = collada.asset.Contributor(author="jeff")
        contrib2 = collada.asset.Contributor(author="bob")
        asset.contributors = [contrib1, contrib2]
        asset.upaxis = collada.asset.UP_AXIS.Z_UP
        time1 = datetime.datetime.now()
        asset.created = time1
        time2 = datetime.datetime.now() + datetime.timedelta(hours=5)
        asset.modified = time2

        asset.save()
        asset = collada.asset.Asset.load(self.dummy, {}, fromstring(tostring(asset.xmlnode)))
        self.assertEqual(asset.title, 'title1')
        self.assertEqual(asset.subject, 'subject2')
        self.assertEqual(asset.revision, 'revision3')
        self.assertEqual(asset.keywords, 'keywords4')
        self.assertEqual(asset.unitname, 'feet')
        self.assertEqual(asset.unitmeter, 3.1)
        self.assertEqual(asset.upaxis, collada.asset.UP_AXIS.Z_UP)
        self.assertEqual(asset.created, time1)
        self.assertEqual(asset.modified, time2)
        self.assertEqual(len(asset.contributors), 2)


if __name__ == '__main__':
    unittest.main()
