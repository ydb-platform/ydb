"""Parser integration tests."""

import os
import unittest
import datetime as dt

from trollsift.parser import Parser


class TestParser(unittest.TestCase):
    def setUp(self):
        self.fmt = "/somedir/{directory}/hrpt_{platform:4s}{platnum:2s}" + "_{time:%Y%m%d_%H%M}_{orbit:05d}.l1b"
        self.string = "/somedir/otherdir/hrpt_noaa16_20140210_1004_69022.l1b"
        self.data = {
            "directory": "otherdir",
            "platform": "noaa",
            "platnum": "16",
            "time": dt.datetime(2014, 2, 10, 10, 4),
            "orbit": 69022,
        }
        self.p = Parser(self.fmt)

    def test_parse(self):
        # Run
        result = self.p.parse(self.string)
        # Assert
        self.assertDictEqual(result, self.data)

    def test_cache_clear(self):
        """Test we can clear the internal cache properly"""
        from trollsift.parser import purge, regex_format

        # Run
        result = self.p.parse(self.string)
        # Assert
        self.assertDictEqual(result, self.data)
        assert regex_format.cache_info()[-1] != 0
        purge()
        assert regex_format.cache_info()[-1] == 0

    def test_compose(self):
        # Run
        result = self.p.compose(self.data)
        # Assert
        self.assertEqual(result, self.string)

    def test_validate(self):
        # These cases are True
        self.assertTrue(self.p.validate("/somedir/avhrr/2014/hrpt_noaa19_20140212_1412_12345.l1b"))
        # These cases are False
        self.assertFalse(self.p.validate("/somedir/bla/bla/hrpt_noaa19_20140212__1412_00000.l1b"))

    def assertDictEqual(self, a, b):
        for key in a:
            self.assertTrue(key in b)
            self.assertEqual(a[key], b[key])

        self.assertEqual(len(a), len(b))

    def assertItemsEqual(self, a, b):
        for i in range(len(a)):
            if isinstance(a[i], dict):
                self.assertDictEqual(a[i], b[i])
            else:
                self.assertEqual(a[i], b[i])
        self.assertEqual(len(a), len(b))


class TestParserVariousFormats(unittest.TestCase):
    def test_parse_viirs_sdr(self):
        fmt = (
            "SVI01_{platform_shortname}_d{start_time:%Y%m%d_t%H%M%S%f}_"
            "e{end_time:%H%M%S%f}_b{orbit:5d}_c{creation_time:%Y%m%d%H%M%S%f}_{source}.h5"
        )
        filename = "SVI01_npp_d20120225_t1801245_e1802487_b01708_c20120226002130255476_noaa_ops.h5"
        data = {
            "platform_shortname": "npp",
            "start_time": dt.datetime(2012, 2, 25, 18, 1, 24, 500000),
            "orbit": 1708,
            "end_time": dt.datetime(1900, 1, 1, 18, 2, 48, 700000),
            "source": "noaa_ops",
            "creation_time": dt.datetime(2012, 2, 26, 0, 21, 30, 255476),
        }
        p = Parser(fmt)
        result = p.parse(filename)
        self.assertDictEqual(result, data)

    def test_parse_iasi_l2(self):
        fmt = (
            "W_XX-EUMETSAT-{reception_location},{instrument},{long_platform_id}+{processing_location}_"
            "C_EUMS_{processing_time:%Y%m%d%H%M%S}_IASI_PW3_02_{platform_id}_{start_time:%Y%m%d-%H%M%S}Z_"
            "{end_time:%Y%m%d.%H%M%S}Z.hdf"
        )
        filename = (
            "W_XX-EUMETSAT-kan,iasi,metopb+kan_C_EUMS_20170920103559_IASI_PW3_02_"
            "M01_20170920-102217Z_20170920.102912Z.hdf"
        )
        data = {
            "reception_location": "kan",
            "instrument": "iasi",
            "long_platform_id": "metopb",
            "processing_location": "kan",
            "processing_time": dt.datetime(2017, 9, 20, 10, 35, 59),
            "platform_id": "M01",
            "start_time": dt.datetime(2017, 9, 20, 10, 22, 17),
            "end_time": dt.datetime(2017, 9, 20, 10, 29, 12),
        }
        p = Parser(fmt)
        result = p.parse(filename)
        self.assertDictEqual(result, data)

    def test_parse_olci_l1b(self):
        fmt = os.path.join(
            "{mission_id:3s}_OL_1_{datatype_id:_<6s}_{start_time:%Y%m%dT%H%M%S}_"
            "{end_time:%Y%m%dT%H%M%S}_{creation_time:%Y%m%dT%H%M%S}_{duration:4d}_"
            "{cycle:3d}_{relative_orbit:3d}_{frame:4d}_{centre:3s}_{platform_mode:1s}_"
            "{timeliness:2s}_{collection:3s}.SEN3",
            "{dataset_name}_radiance.nc",
        )
        # made up:
        filename = os.path.join(
            "S3A_OL_1_EFR____20180916T090539_20180916T090839_20180916T090539_0001_001_001_0001_CEN_M_AA_AAA.SEN3",
            "Oa21_radiance.nc",
        )
        data = {
            "mission_id": "S3A",
            "datatype_id": "EFR",
            "start_time": dt.datetime(2018, 9, 16, 9, 5, 39),
            "end_time": dt.datetime(2018, 9, 16, 9, 8, 39),
            "creation_time": dt.datetime(2018, 9, 16, 9, 5, 39),
            "duration": 1,
            "cycle": 1,
            "relative_orbit": 1,
            "frame": 1,
            "centre": "CEN",
            "platform_mode": "M",
            "timeliness": "AA",
            "collection": "AAA",
            "dataset_name": "Oa21",
        }
        p = Parser(fmt)
        result = p.parse(filename)
        self.assertDictEqual(result, data)

    def test_parse_duplicate_fields(self):
        """Test parsing a pattern that has duplicate fields."""
        fmt = "{version_number:1s}/filename_with_version_number_{version_number:1s}.tif"
        filename = "1/filename_with_version_number_1.tif"
        p = Parser(fmt)
        result = p.parse(filename)
        self.assertEqual(result["version_number"], "1")
