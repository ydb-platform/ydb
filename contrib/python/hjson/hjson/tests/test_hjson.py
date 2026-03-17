from __future__ import with_statement

import os
import sys
import subprocess
import tempfile
import codecs # dump

from unittest import TestCase

import hjson

class TestAssets(TestCase):

    def __init__(self, *args, **kwargs):
        super(TestAssets, self).__init__(*args, **kwargs)
        self.assetsDir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "assets")
        self.assets = self.load('testlist.txt', False).split('\n')
        self.maxDiff = None
        self.verma, self.vermi = sys.version_info[0:2]

    def load(self, name, cr):
        name = os.path.join(self.assetsDir, name)
        with open(name, 'rb') as f:
            text = f.read().decode('utf-8')
            text = text.replace('\r', '')
            if cr: text = text.replace('\n', '\r\n')
            return text

    def check(self, name, file, inputCr):
        text = self.load(file, inputCr)
        shouldFail = name[0:4] == "fail"

        try:
            data = hjson.loads(text)
            self.assertFalse(shouldFail, file)

            text1 = hjson.dumpsJSON(data)
            hjson1 = hjson.dumps(data, ensure_ascii=False);
            result = hjson.loads(self.load(name + "_result.json", inputCr))
            text2 = hjson.dumpsJSON(result)
            hjson2 = self.load(name + "_result.hjson", False)

            # dbg
            # with open(name + "_dbg1.txt", "w") as tmp: tmp.write(hjson1.encode("utf-8"))
            # with open(name + "_dbg2.txt", "w") as tmp: tmp.write(hjson2.encode("utf-8"))
            # with codecs.open(name + "_dbg3.txt", 'w', 'utf-8') as tmp: hjson.dump(data, tmp)

            if self.verma>2 or self.vermi>6:
                # final check fails on py2.6 because of string formatting issues
                self.assertEqual(text2, text1, file)
                self.assertEqual(hjson2, hjson1, file)

        except hjson.HjsonDecodeError as e:
            if not shouldFail:
                self.fail("raised error on parsing %s: %r" % (file, e))

    def test_files(self):
        for file in self.assets:
            name, sep, ext = file.partition("_test.")
            if name.startswith("stringify/quotes") or \
                name.startswith("extra/"): continue # ignore/not supported

            self.check(name, file, True)
            self.check(name, file, False)
