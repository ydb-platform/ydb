#
# (C) Copyright 2017- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#

"""
Tests of the ecCodes Python3 bindings
"""

import math
import os.path

import numpy as np
import pytest

import eccodes

SAMPLE_DATA_FOLDER = os.path.join(os.path.dirname(__file__), "sample-data")
TEST_GRIB_TIGGE_DATA = os.path.join(SAMPLE_DATA_FOLDER, "tiggelam_cnmc_sfc.grib2")
TEST_GRIB_ERA5_DATA = os.path.join(SAMPLE_DATA_FOLDER, "era5-levels-members.grib")


def get_sample_fullpath(sample):
    samples_path = eccodes.codes_samples_path()
    if not os.path.isdir(samples_path):
        return None
    return os.path.join(samples_path, sample)


# ---------------------------------------------
# PRODUCT ANY
# ---------------------------------------------
def test_codes_definition_path():
    df = eccodes.codes_definition_path()
    print(f"\n\teccodes.codes_definition_path returned {df}")
    assert df is not None


def test_codes_samples_path():
    sp = eccodes.codes_samples_path()
    print(f"\n\teccodes.codes_samples_path returned {sp}")
    assert sp is not None


def test_codes_set_debug():
    if eccodes.codes_get_api_version(int) < 23800:
        pytest.skip("ecCodes version too old")
    eccodes.codes_set_debug(-1)
    eccodes.codes_set_debug(0)


def test_codes_set_data_quality_checks():
    if eccodes.codes_get_api_version(int) < 24100:
        pytest.skip("ecCodes version too old")
    eccodes.codes_set_data_quality_checks(2)
    eccodes.codes_set_data_quality_checks(1)
    eccodes.codes_set_data_quality_checks(0)


def test_codes_set_definitions_path():
    eccodes.codes_set_definitions_path(eccodes.codes_definition_path())


def test_codes_set_samples_path():
    eccodes.codes_set_samples_path(eccodes.codes_samples_path())


def test_api_version():
    vs = eccodes.codes_get_api_version()
    assert isinstance(vs, str)
    assert len(vs) > 0
    assert vs == eccodes.codes_get_api_version(str)
    vi = eccodes.codes_get_api_version(int)
    assert isinstance(vi, int)
    assert vi > 20000
    print(vi)


def test_version_info():
    vinfo = eccodes.codes_get_version_info()
    print("ecCodes version information: ", vinfo)
    assert len(vinfo) == 2


def test_codes_get_features():
    if eccodes.codes_get_api_version(int) < 23800:
        pytest.skip("ecCodes version too old")

    features = eccodes.codes_get_features(eccodes.CODES_FEATURES_ALL)
    print(f"\n\tAll features = {features}")
    features = eccodes.codes_get_features(eccodes.CODES_FEATURES_DISABLED)
    print(f"\tDisabled features = {features}")
    features = eccodes.codes_get_features(eccodes.CODES_FEATURES_ENABLED)
    print(f"\tEnabled features = {features}")


def test_codes_is_defined():
    gid = eccodes.codes_grib_new_from_samples("sh_sfc_grib1")
    assert eccodes.codes_is_defined(gid, "JS")


def test_codes_get_native_type():
    gid = eccodes.codes_grib_new_from_samples("GRIB2")
    assert eccodes.codes_get_native_type(gid, "date") is int
    assert eccodes.codes_get_native_type(gid, "referenceValue") is float
    assert eccodes.codes_get_native_type(gid, "stepType") is str
    assert eccodes.codes_get_native_type(gid, "section_1") is None
    with pytest.raises(eccodes.NullHandleError):
        eccodes.codes_get_native_type(0, "aKey")  # NULL handle


def test_new_from_file():
    fpath = get_sample_fullpath("GRIB2.tmpl")
    if fpath is None:
        return
    with open(fpath, "rb") as f:
        msgid = eccodes.codes_new_from_file(f, eccodes.CODES_PRODUCT_GRIB)
        assert msgid
    fpath = get_sample_fullpath("BUFR4.tmpl")
    with open(fpath, "rb") as f:
        msgid = eccodes.codes_new_from_file(f, eccodes.CODES_PRODUCT_BUFR)
        assert msgid
    fpath = get_sample_fullpath("clusters_grib1.tmpl")
    with open(fpath, "rb") as f:
        msgid = eccodes.codes_new_from_file(f, eccodes.CODES_PRODUCT_ANY)
        assert msgid
    with open(fpath, "rb") as f:
        msgid = eccodes.codes_new_from_file(f, eccodes.CODES_PRODUCT_GTS)
        assert msgid is None
    with pytest.raises(ValueError):
        with open(fpath, "rb") as f:
            eccodes.codes_new_from_file(f, 1024)


def test_any_read():
    fpath = get_sample_fullpath("GRIB1.tmpl")
    if fpath is None:
        return
    with open(fpath, "rb") as f:
        msgid = eccodes.codes_any_new_from_file(f)
        assert eccodes.codes_get(msgid, "edition") == 1
        assert eccodes.codes_get(msgid, "identifier") == "GRIB"
        assert eccodes.codes_get(msgid, "identifier", str) == "GRIB"
        eccodes.codes_release(msgid)

    fpath = get_sample_fullpath("BUFR3.tmpl")
    with open(fpath, "rb") as f:
        msgid = eccodes.codes_any_new_from_file(f)
        assert eccodes.codes_get(msgid, "edition") == 3
        assert eccodes.codes_get(msgid, "identifier") == "BUFR"
        eccodes.codes_release(msgid)


def test_count_in_file():
    with open(TEST_GRIB_TIGGE_DATA, "rb") as f:
        assert eccodes.codes_count_in_file(f) == 7
    with open(TEST_GRIB_ERA5_DATA, "rb") as f:
        assert eccodes.codes_count_in_file(f) == 160
    fpath = get_sample_fullpath("GRIB1.tmpl")
    if fpath is None:
        return
    with open(fpath, "rb") as f:
        assert eccodes.codes_count_in_file(f) == 1


def test_new_from_message():
    gid = eccodes.codes_grib_new_from_samples("sh_sfc_grib1")
    message = eccodes.codes_get_message(gid)
    eccodes.codes_release(gid)
    assert len(message) == 9358
    newgid = eccodes.codes_new_from_message(message)
    assert eccodes.codes_get(newgid, "packingType") == "spectral_complex"
    assert eccodes.codes_get(newgid, "gridType") == "sh"
    eccodes.codes_release(newgid)

    # This time read from a string rather than a file
    # metar_str = "METAR LQMO 022350Z 09003KT 6000 FEW010 SCT035 BKN060 08/08 Q1003="
    # newgid = eccodes.codes_new_from_message(metar_str)
    # cccc = eccodes.codes_get(newgid, "CCCC")
    # assert cccc == "LQMO"
    # eccodes.codes_release(newgid)


def test_new_from_message_memoryview():
    # ECC-2081
    fpath = get_sample_fullpath("gg_sfc_grib1.tmpl")
    if fpath is None:
        return
    with open(fpath, "rb") as f:
        data = f.read()
        mv = memoryview(data)
        assert len(mv) == 26860
        newgid = eccodes.codes_new_from_message(mv)
        assert eccodes.codes_get(newgid, "packingType") == "grid_simple"
        assert eccodes.codes_get(newgid, "gridType") == "reduced_gg"
        assert eccodes.codes_get(newgid, "totalLength") == 26860
        eccodes.codes_release(newgid)


def test_gts_header():
    eccodes.codes_gts_header(True)
    eccodes.codes_gts_header(False)


def test_extract_offsets():
    offsets = eccodes.codes_extract_offsets(
        TEST_GRIB_TIGGE_DATA, eccodes.CODES_PRODUCT_ANY, is_strict=True
    )
    offsets_list = list(offsets)
    expected = [0, 432, 864, 1296, 1728, 2160, 2616]
    assert offsets_list == expected


def test_extract_offsets_sizes():
    if eccodes.codes_get_api_version(int) < 23400:
        pytest.skip("ecCodes version too old")

    offsets_sizes = eccodes.codes_extract_offsets_sizes(
        TEST_GRIB_TIGGE_DATA, eccodes.CODES_PRODUCT_GRIB, is_strict=True
    )
    result = list(offsets_sizes)
    expected = [
        (0, 432),
        (432, 432),
        (864, 432),
        (1296, 432),
        (1728, 432),
        (2160, 456),
        (2616, 456),
    ]
    assert result == expected


def test_any_new_from_samples():
    msgid = eccodes.codes_new_from_samples(
        "reduced_gg_ml_grib2", eccodes.CODES_PRODUCT_ANY
    )
    assert eccodes.codes_get(msgid, "identifier") == "GRIB"
    eccodes.codes_release(msgid)
    msgid = eccodes.codes_new_from_samples("BUFR4", eccodes.CODES_PRODUCT_ANY)
    assert eccodes.codes_get(msgid, "identifier") == "BUFR"
    eccodes.codes_release(msgid)

    msgid = eccodes.codes_any_new_from_samples("diag.tmpl")
    assert eccodes.codes_get(msgid, "identifier") == "DIAG"
    eccodes.codes_release(msgid)


# ---------------------------------------------
# PRODUCT GRIB
# ---------------------------------------------
def test_grib_read():
    gid = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib1")
    assert eccodes.codes_get(gid, "Ni") == 16
    assert eccodes.codes_get(gid, "Nj") == 31
    assert eccodes.codes_get(gid, "const") == 1
    assert eccodes.codes_get(gid, "centre", str) == "ecmf"
    assert eccodes.codes_get(gid, "packingType", str) == "grid_simple"
    assert eccodes.codes_get(gid, "gridType", str) == "regular_ll"
    eccodes.codes_release(gid)
    gid = eccodes.codes_grib_new_from_samples("sh_ml_grib2")
    assert eccodes.codes_get(gid, "const") == 0
    assert eccodes.codes_get(gid, "gridType", str) == "sh"
    assert eccodes.codes_get(gid, "typeOfLevel", str) == "hybrid"
    assert eccodes.codes_get_long(gid, "avg") == 185
    eccodes.codes_release(gid)


def test_grib_set_string():
    gid = eccodes.codes_grib_new_from_samples("regular_gg_sfc_grib2")
    eccodes.codes_set(gid, "gridType", "reduced_gg")
    eccodes.codes_release(gid)


def test_grib_set_error():
    gid = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib1")
    with pytest.raises(TypeError):
        eccodes.codes_set_long(gid, "centre", "kwbc")
    with pytest.raises(TypeError):
        eccodes.codes_set_double(gid, "centre", "kwbc")
    with pytest.raises(eccodes.CodesInternalError):
        eccodes.codes_set(gid, "centre", [])


def test_grib_write(tmpdir):
    gid = eccodes.codes_grib_new_from_samples("GRIB2")
    eccodes.codes_set(gid, "backgroundProcess", 44)
    output = tmpdir.join("test_grib_write.grib")
    with open(str(output), "wb") as fout:
        eccodes.codes_write(gid, fout)
    eccodes.codes_release(gid)


def test_grib_codes_set_missing():
    gid = eccodes.codes_grib_new_from_samples("reduced_rotated_gg_ml_grib2")
    eccodes.codes_set(gid, "typeOfFirstFixedSurface", "sfc")
    eccodes.codes_set_missing(gid, "scaleFactorOfFirstFixedSurface")
    eccodes.codes_set_missing(gid, "scaledValueOfFirstFixedSurface")
    assert eccodes.codes_is_missing(gid, "scaleFactorOfFirstFixedSurface")


def test_grib_set_key_vals():
    gid = eccodes.codes_grib_new_from_samples("GRIB2")
    # String
    eccodes.codes_set_key_vals(gid, "shortName=z,dataDate=20201112")
    assert eccodes.codes_get(gid, "shortName", str) == "z"
    assert eccodes.codes_get(gid, "date", int) == 20201112
    # List
    eccodes.codes_set_key_vals(gid, ["shortName=2t", "dataDate=20191010"])
    assert eccodes.codes_get(gid, "shortName", str) == "2t"
    assert eccodes.codes_get(gid, "date", int) == 20191010
    # Dictionary
    eccodes.codes_set_key_vals(gid, {"shortName": "msl", "dataDate": 20181010})
    assert eccodes.codes_get(gid, "shortName", str) == "msl"
    assert eccodes.codes_get(gid, "date", int) == 20181010
    eccodes.codes_release(gid)


def test_grib_get_error():
    gid = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    # Note: AssertionError can be raised when type checks are enabled
    with pytest.raises((ValueError, AssertionError)):
        eccodes.codes_get(gid, None)


def test_grib_get_array():
    gid = eccodes.codes_grib_new_from_samples("reduced_gg_pl_160_grib2")
    pl = eccodes.codes_get_array(gid, "pl")
    assert pl[0] == 18
    pli = eccodes.codes_get_array(gid, "pl", int)
    assert np.array_equal(pl, pli)
    pls = eccodes.codes_get_array(gid, "centre", str)
    assert pls == ["ecmf"]
    dvals = eccodes.codes_get_array(gid, "values")
    assert len(dvals) == 138346
    assert type(dvals[0]) is np.float64
    eccodes.codes_release(gid)


def test_grib_get_array_single_precision():
    if eccodes.codes_get_api_version(int) < 23100:
        pytest.skip("ecCodes version too old")

    gid = eccodes.codes_grib_new_from_samples("reduced_gg_pl_160_grib2")

    dvals = eccodes.codes_get_array(gid, "values", ktype=float)
    assert type(dvals[0]) is np.float64
    fvals = eccodes.codes_get_array(gid, "values", ktype=np.float32)
    assert type(fvals[0]) is np.float32
    fvals = eccodes.codes_get_float_array(gid, "values")
    assert type(fvals[0]) is np.float32
    dvals = eccodes.codes_get_values(gid)
    assert type(dvals[0]) is np.float64
    fvals = eccodes.codes_get_values(gid, np.float32)
    assert type(fvals[0]) is np.float32

    eccodes.codes_release(gid)


def test_grib_gaussian_latitudes():
    orders = [256, 1280]
    # Latitude of the first element (nearest the north pole)
    expected_lats = [89.731148, 89.946187]
    for _order, _lat in zip(orders, expected_lats):
        lats = eccodes.codes_get_gaussian_latitudes(_order)
        assert len(lats) == 2 * _order
        assert math.isclose(lats[0], _lat, abs_tol=0.00001)


def test_grib_latitudes_longitudes():
    gid = eccodes.codes_grib_new_from_samples("reduced_gg_pl_160_grib2")
    lats = eccodes.codes_get_array(gid, "latitudes")
    assert len(lats) == 138346
    assert math.isclose(lats[0], 89.570089, abs_tol=0.00001)
    lats = eccodes.codes_get_array(gid, "distinctLatitudes")
    assert len(lats) == 320
    assert math.isclose(lats[0], 89.570089, abs_tol=0.00001)
    assert math.isclose(lats[319], -89.570089, abs_tol=0.00001)

    lons = eccodes.codes_get_array(gid, "longitudes")
    assert len(lons) == 138346
    assert math.isclose(lons[138345], 340.00, abs_tol=0.00001)
    lons = eccodes.codes_get_array(gid, "distinctLongitudes")
    assert len(lons) == 4762
    assert math.isclose(lons[4761], 359.4375, abs_tol=0.00001)


def test_grib_get_message_size():
    gid = eccodes.codes_grib_new_from_samples("GRIB2")
    assert eccodes.codes_get_message_size(gid) == 179


def test_grib_get_message_offset():
    gid = eccodes.codes_grib_new_from_samples("GRIB2")
    assert eccodes.codes_get_message_offset(gid) == 0


def test_grib_get_key_offset():
    gid = eccodes.codes_grib_new_from_samples("GRIB2")
    assert eccodes.codes_get_offset(gid, "identifier") == 0
    assert eccodes.codes_get_offset(gid, "discipline") == 6
    assert eccodes.codes_get_offset(gid, "offsetSection1") == 16
    assert eccodes.codes_get_offset(gid, "7777") == 175


def test_grib_clone():
    gid = eccodes.codes_grib_new_from_samples("GRIB2")
    clone = eccodes.codes_clone(gid)
    assert gid
    assert clone
    assert eccodes.codes_get(clone, "identifier") == "GRIB"
    assert eccodes.codes_get(clone, "totalLength") == 179
    eccodes.codes_release(gid)
    eccodes.codes_release(clone)


def test_grib_clone_headers_only():
    if eccodes.codes_get_api_version(int) < 23400:
        pytest.skip("ecCodes version too old")

    with open(TEST_GRIB_ERA5_DATA, "rb") as f:
        msgid1 = eccodes.codes_grib_new_from_file(f)
        msgid2 = eccodes.codes_clone(msgid1, headers_only=True)
        msg1_size = eccodes.codes_get_message_size(msgid1)
        msg2_size = eccodes.codes_get_message_size(msgid2)
        assert msg1_size > msg2_size
        assert eccodes.codes_get(msgid1, "totalLength") == 14752
        assert eccodes.codes_get(msgid2, "totalLength") == 112
        assert eccodes.codes_get(msgid1, "bitsPerValue") == 16
        assert eccodes.codes_get(msgid2, "bitsPerValue") == 0
        eccodes.codes_release(msgid1)
        eccodes.codes_release(msgid2)


def test_grib_keys_iterator():
    gid = eccodes.codes_grib_new_from_samples("reduced_gg_pl_1280_grib1")
    iterid = eccodes.codes_keys_iterator_new(gid, "ls")
    count = 0
    while eccodes.codes_keys_iterator_next(iterid):
        keyname = eccodes.codes_keys_iterator_get_name(iterid)
        keyval = eccodes.codes_get_string(gid, keyname)
        assert len(keyval) > 0
        count += 1
    assert count == 10
    eccodes.codes_keys_iterator_rewind(iterid)
    eccodes.codes_keys_iterator_delete(iterid)
    eccodes.codes_release(gid)


def test_grib_keys_iterator_skip():
    gid = eccodes.codes_grib_new_from_samples("reduced_gg_pl_1280_grib1")
    iterid = eccodes.codes_keys_iterator_new(gid, "ls")
    count = 0
    eccodes.codes_skip_computed(iterid)
    # codes_skip_coded(iterid)
    eccodes.codes_skip_edition_specific(iterid)
    eccodes.codes_skip_duplicates(iterid)
    eccodes.codes_skip_read_only(iterid)
    eccodes.codes_skip_function(iterid)
    while eccodes.codes_keys_iterator_next(iterid):
        keyname = eccodes.codes_keys_iterator_get_name(iterid)
        keyval = eccodes.codes_get_string(gid, keyname)
        assert len(keyval) > 0
        count += 1
    # centre, level and dataType
    assert count == 3
    eccodes.codes_keys_iterator_delete(iterid)

    iterid = eccodes.codes_keys_iterator_new(gid)
    count = 0
    eccodes.codes_skip_coded(iterid)
    while eccodes.codes_keys_iterator_next(iterid):
        count += 1
    assert count > 140
    eccodes.codes_keys_iterator_delete(iterid)
    eccodes.codes_release(gid)


def test_grib_get_data():
    gid = eccodes.codes_grib_new_from_samples("GRIB1")
    ggd = eccodes.codes_grib_get_data(gid)
    assert len(ggd) == 65160
    eccodes.codes_release(gid)
    gid = eccodes.codes_grib_new_from_samples("reduced_gg_pl_32_grib2")
    ggd = eccodes.codes_grib_get_data(gid)
    assert len(ggd) == 6114
    elem1 = ggd[0]  # This is a 'Bunch' derived off dict
    assert "lat" in elem1.keys()
    assert "lon" in elem1.keys()
    assert "value" in elem1.keys()
    eccodes.codes_release(gid)


def test_grib_get_double_element():
    gid = eccodes.codes_grib_new_from_samples("gg_sfc_grib2")
    elem = eccodes.codes_get_double_element(gid, "values", 1)
    assert math.isclose(elem, 259.9865, abs_tol=0.001)
    eccodes.codes_release(gid)


def test_grib_get_double_elements():
    gid = eccodes.codes_grib_new_from_samples("gg_sfc_grib1")
    values = eccodes.codes_get_values(gid)
    num_vals = len(values)
    indexes = [0, int(num_vals / 2), num_vals - 1]
    elems = eccodes.codes_get_double_elements(gid, "values", indexes)
    assert math.isclose(elems[0], 259.6935, abs_tol=0.001)
    assert math.isclose(elems[1], 299.9064, abs_tol=0.001)
    assert math.isclose(elems[2], 218.8146, abs_tol=0.001)
    elems2 = eccodes.codes_get_elements(gid, "values", indexes)
    assert elems == elems2


def test_grib_get_values():
    gid = eccodes.codes_grib_new_from_samples("gg_sfc_grib2")
    with pytest.raises(TypeError):
        eccodes.codes_get_values(gid, str)
    eccodes.codes_release(gid)


def test_grib_geoiterator():
    gid = eccodes.codes_grib_new_from_samples("reduced_gg_pl_256_grib2")
    iterid = eccodes.codes_grib_iterator_new(gid, 0)
    i = 0
    while 1:
        result = eccodes.codes_grib_iterator_next(iterid)
        if not result:
            break
        [lat, lon, value] = result
        assert -90.0 < lat < 90.00
        assert 0.0 <= lon < 360.0
        # assert math.isclose(value, 1.0, abs_tol=0.001)
        i += 1
    assert i == 348528
    eccodes.codes_grib_iterator_delete(iterid)
    eccodes.codes_release(gid)


def test_grib_nearest():
    gid = eccodes.codes_grib_new_from_samples("reduced_gg_ml_grib2")
    lat, lon = 30, -20
    nearest = eccodes.codes_grib_find_nearest(gid, lat, lon)[0]
    assert nearest.index == 1770
    lat, lon = 10, 0
    nearest = eccodes.codes_grib_find_nearest(gid, lat, lon)[0]
    assert nearest.index == 2545
    lat, lon = 10, 20
    nearest = eccodes.codes_grib_find_nearest(gid, lat, lon, False, 4)
    expected_indexes = (2553, 2552, 2425, 2424)
    returned_indexes = (
        nearest[0].index,
        nearest[1].index,
        nearest[2].index,
        nearest[3].index,
    )
    assert sorted(expected_indexes) == sorted(returned_indexes)
    # Cannot do more than 4 nearest neighbours
    with pytest.raises(ValueError):
        eccodes.codes_grib_find_nearest(gid, lat, lon, False, 5)
    eccodes.codes_release(gid)


def test_grib_nearest_multiple():
    gid = eccodes.codes_new_from_samples(
        "reduced_gg_ml_grib2", eccodes.CODES_PRODUCT_GRIB
    )
    inlats = (30, 13)
    inlons = (-20, 234)
    is_lsm = False
    nearest = eccodes.codes_grib_find_nearest_multiple(gid, is_lsm, inlats, inlons)
    eccodes.codes_release(gid)
    assert nearest[0].index == 1770
    assert nearest[1].index == 2500
    # Error condition
    with pytest.raises(ValueError):
        eccodes.codes_grib_find_nearest_multiple(gid, is_lsm, (1, 2), (1, 2, 3))


def test_grib_ecc_1042():
    # Issue ECC-1042: Python3 interface writes integer arrays incorrectly
    gid = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")

    # Trying write with inferred dtype
    write_vals = np.array([1, 2, 3])
    eccodes.codes_set_values(gid, write_vals)
    read_vals = eccodes.codes_get_values(gid)
    length = len(read_vals)
    assert read_vals[0] == 1
    assert read_vals[length - 1] == 3

    # Trying write with explicit dtype
    write_vals = np.array(
        [
            1,
            2,
            3,
        ],
        dtype=float,
    )
    eccodes.codes_set_values(gid, write_vals)
    read_vals = eccodes.codes_get_values(gid)
    assert read_vals[0] == 1
    assert read_vals[length - 1] == 3

    eccodes.codes_release(gid)


def test_grib_ecc_1007():
    # Issue ECC-1007: Python3 interface cannot write large arrays
    gid = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    numvals = 1501 * 1501
    values = np.zeros((numvals,))
    values[0] = 12  # Make sure it's not a constant field
    eccodes.codes_set_values(gid, values)
    maxv = eccodes.codes_get(gid, "max")
    minv = eccodes.codes_get(gid, "min")
    assert minv == 0
    assert maxv == 12
    eccodes.codes_release(gid)


def test_grib_set_bitmap():
    gid = eccodes.codes_grib_new_from_samples("GRIB2")
    # Note: np.Infinity was removed in the NumPy 2.0 release. Use np.inf instead
    missing = np.inf
    eccodes.codes_set(gid, "bitmapPresent", 1)
    eccodes.codes_set(gid, "missingValue", missing)
    # Grid with 100 points 2 of which are missing
    values = np.ones((100,))
    values[2] = missing
    values[4] = missing
    eccodes.codes_set_values(gid, values)
    assert eccodes.codes_get(gid, "numberOfDataPoints") == 100
    assert eccodes.codes_get(gid, "numberOfCodedValues") == 98
    assert eccodes.codes_get(gid, "numberOfMissing") == 2
    eccodes.codes_get_array(gid, "bitmap")
    eccodes.codes_release(gid)


def test_grib_set_float_array():
    gid = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    for ftype in (float, np.float16, np.float32, np.float64):
        values = np.ones((100000,), ftype)
        eccodes.codes_set_array(gid, "values", values)
        assert (eccodes.codes_get_values(gid) == 1.0).all()
        eccodes.codes_set_values(gid, values)
        assert (eccodes.codes_get_values(gid) == 1.0).all()


def test_grib_set_2d_array():
    gid = eccodes.codes_grib_new_from_samples("GRIB2")
    num_vals = eccodes.codes_get(gid, "numberOfValues")
    assert num_vals == 496
    vals2d = np.array([[11, 2, 3], [4, 15, -6]], np.float64)
    eccodes.codes_set_double_array(gid, "values", vals2d)
    num_vals = eccodes.codes_get(gid, "numberOfValues")
    assert num_vals == 6
    vals = eccodes.codes_get_double_array(gid, "values")
    assert vals[0] == 11.0
    assert vals[1] == 2.0
    assert vals[4] == 15.0
    assert vals[5] == -6.0


def test_grib_set_np_int64():
    gid = eccodes.codes_grib_new_from_samples("regular_gg_sfc_grib2")
    eccodes.codes_set(gid, "productDefinitionTemplateNumber", 1)
    eccodes.codes_set(gid, "number", np.int64(17))
    assert eccodes.codes_get_long(gid, "number") == 17
    eccodes.codes_set_long(gid, "number", np.int64(16))
    assert eccodes.codes_get_long(gid, "number") == 16
    eccodes.codes_release(gid)


def test_gribex_mode():
    eccodes.codes_gribex_mode_on()
    eccodes.codes_gribex_mode_off()


def test_grib_new_from_samples_error():
    with pytest.raises(eccodes.FileNotFoundError):
        eccodes.codes_new_from_samples("nonExistentSample", eccodes.CODES_PRODUCT_GRIB)


def test_grib_new_from_file_error(tmp_path):
    # Note: AssertionError can be raised when type checks are enabled
    with pytest.raises((TypeError, AssertionError)):
        eccodes.codes_grib_new_from_file(None)
    p = tmp_path / "afile.txt"
    p.write_text("GRIBxxxx")
    with open(p, "rb") as f:
        with pytest.raises(eccodes.UnsupportedEditionError):
            eccodes.codes_grib_new_from_file(f)


def test_grib_index_new_from_file(tmpdir):
    fpath = get_sample_fullpath("GRIB1.tmpl")
    if fpath is None:
        return
    index_keys = ["shortName", "level", "number", "step", "referenceValue"]
    iid = eccodes.codes_index_new_from_file(fpath, index_keys)
    index_file = str(tmpdir.join("temp.grib.index"))
    eccodes.codes_index_write(iid, index_file)

    key = "level"
    assert eccodes.codes_index_get_size(iid, key) == 1

    # Cannot get the native type of a key from an index
    # so right now the default is str.
    assert eccodes.codes_index_get(iid, key) == ("500",)
    assert eccodes.codes_index_get(iid, key, int) == (500,)
    assert eccodes.codes_index_get_long(iid, key) == (500,)

    key = "referenceValue"
    refVal = 47485.4
    assert eccodes.codes_index_get_double(iid, key) == (refVal,)
    assert eccodes.codes_index_get(iid, key, float) == (refVal,)

    eccodes.codes_index_select(iid, "level", 500)
    eccodes.codes_index_select(iid, "shortName", "z")
    eccodes.codes_index_select(iid, "number", 0)
    eccodes.codes_index_select(iid, "step", 0)
    eccodes.codes_index_select(iid, "referenceValue", refVal)
    gid = eccodes.codes_new_from_index(iid)
    assert eccodes.codes_get(gid, "edition") == 1
    assert eccodes.codes_get(gid, "totalLength") == 107
    eccodes.codes_release(gid)

    eccodes.codes_index_release(iid)

    iid2 = eccodes.codes_index_read(index_file)
    assert eccodes.codes_index_get(iid2, "shortName") == ("z",)
    eccodes.codes_index_release(iid2)


def test_grib_multi_support_reset_file():
    try:
        # TODO: read an actual multi-field GRIB here
        fpath = get_sample_fullpath("GRIB2.tmpl")
        if fpath is None:
            return
        eccodes.codes_grib_multi_support_on()
        with open(fpath, "rb") as f:
            eccodes.codes_grib_multi_support_reset_file(f)
    finally:
        eccodes.codes_grib_multi_support_off()


def test_grib_multi_field_write(tmpdir):
    # Note: eccodes.codes_grib_multi_new() calls codes_grib_multi_support_on()
    #       hence the 'finally' block
    try:
        gid = eccodes.codes_grib_new_from_samples("GRIB2")
        mgid = eccodes.codes_grib_multi_new()
        section_num = 4
        for step in range(12, 132, 12):
            eccodes.codes_set(gid, "step", step)
            eccodes.codes_grib_multi_append(gid, section_num, mgid)
        output = tmpdir.join("test_grib_multi_field_write.grib2")
        with open(str(output), "wb") as fout:
            eccodes.codes_grib_multi_write(mgid, fout)
        eccodes.codes_grib_multi_release(mgid)
        eccodes.codes_release(gid)
    finally:
        eccodes.codes_grib_multi_support_off()


def test_grib_uuid_get_set():
    # ECC-1167
    gid = eccodes.codes_grib_new_from_samples("GRIB2")
    eccodes.codes_set(gid, "gridType", "unstructured_grid")
    key = "uuidOfHGrid"
    ntype = eccodes.codes_get_native_type(gid, key)
    assert ntype is bytes

    uuid = eccodes.codes_get_string(gid, key)
    assert uuid == "00000000000000000000000000000000"
    eccodes.codes_set_string(gid, key, "DEfdBEef10203040b00b1e50001100FF")
    uuid = eccodes.codes_get(gid, key, str)
    assert uuid == "defdbeef10203040b00b1e50001100ff"
    uuid = eccodes.codes_get(gid, key)
    assert uuid == "defdbeef10203040b00b1e50001100ff"

    uuid_arr = eccodes.codes_get_array(gid, key)
    assert uuid_arr == ["defdbeef10203040b00b1e50001100ff"]
    eccodes.codes_release(gid)


def test_grib_dump(tmp_path):
    gid = eccodes.codes_grib_new_from_samples("GRIB2")
    p = tmp_path / "dump_grib.txt"
    with open(p, "w") as fout:
        eccodes.codes_dump(gid, fout)
        eccodes.codes_dump(gid, fout, "debug")
    eccodes.codes_release(gid)


def test_grib_copy_namespace():
    gid1 = eccodes.codes_grib_new_from_samples("GRIB2")
    gid2 = eccodes.codes_grib_new_from_samples("reduced_gg_pl_32_grib2")
    eccodes.codes_copy_namespace(gid1, "ls", gid2)


# ---------------------------------------------
# PRODUCT BUFR
# ---------------------------------------------
def test_bufr_read_write(tmpdir):
    bid = eccodes.codes_new_from_samples("BUFR4", eccodes.CODES_PRODUCT_BUFR)
    eccodes.codes_set(bid, "unpack", 1)
    assert eccodes.codes_get(bid, "typicalYear") == 2012
    assert eccodes.codes_get(bid, "centre", str) == "ecmf"
    eccodes.codes_set(bid, "totalSunshine", 13)
    eccodes.codes_set(bid, "pack", 1)
    output = tmpdir.join("test_bufr_write.bufr")
    with open(str(output), "wb") as fout:
        eccodes.codes_write(bid, fout)
    assert eccodes.codes_get(bid, "totalSunshine") == 13
    eccodes.codes_release(bid)


def test_bufr_encode(tmpdir):
    ibufr = eccodes.codes_bufr_new_from_samples("BUFR3_local_satellite")
    eccodes.codes_set_array(ibufr, "inputDelayedDescriptorReplicationFactor", (4,))
    eccodes.codes_set(ibufr, "masterTableNumber", 0)
    eccodes.codes_set(ibufr, "bufrHeaderSubCentre", 0)
    eccodes.codes_set(ibufr, "bufrHeaderCentre", 98)
    eccodes.codes_set(ibufr, "updateSequenceNumber", 0)
    eccodes.codes_set(ibufr, "dataCategory", 12)
    eccodes.codes_set(ibufr, "dataSubCategory", 139)
    eccodes.codes_set(ibufr, "masterTablesVersionNumber", 13)
    eccodes.codes_set(ibufr, "localTablesVersionNumber", 1)
    eccodes.codes_set(ibufr, "numberOfSubsets", 492)
    eccodes.codes_set(ibufr, "localNumberOfObservations", 492)
    eccodes.codes_set(ibufr, "satelliteID", 4)
    eccodes.codes_set(ibufr, "observedData", 1)
    eccodes.codes_set(ibufr, "compressedData", 1)
    eccodes.codes_set(ibufr, "unexpandedDescriptors", 312061)
    eccodes.codes_set(ibufr, "pixelSizeOnHorizontal1", 1.25e04)
    eccodes.codes_set(ibufr, "orbitNumber", 31330)
    eccodes.codes_set(ibufr, "#1#beamIdentifier", 1)
    eccodes.codes_set(
        ibufr, "#4#likelihoodComputedForSolution", eccodes.CODES_MISSING_DOUBLE
    )
    eccodes.codes_set(ibufr, "pack", 1)
    output = tmpdir.join("test_bufr_encode.bufr")
    with open(str(output), "wb") as fout:
        eccodes.codes_write(ibufr, fout)
    eccodes.codes_release(ibufr)


def test_bufr_set_float():
    ibufr = eccodes.codes_bufr_new_from_samples("BUFR4")
    eccodes.codes_set(ibufr, "unpack", 1)
    eccodes.codes_set(ibufr, "totalPrecipitationPast24Hours", np.float32(1.26e04))
    eccodes.codes_set(ibufr, "totalPrecipitationPast24Hours", np.float16(1.27e04))
    eccodes.codes_release(ibufr)


def test_bufr_set_string_array():
    ibufr = eccodes.codes_bufr_new_from_samples("BUFR3_local_satellite")
    eccodes.codes_set(ibufr, "numberOfSubsets", 3)
    eccodes.codes_set(ibufr, "unexpandedDescriptors", 307022)
    inputVals = ("ARD2-LPTR", "EPFL-LPTR", "BOU2-LPTR")
    eccodes.codes_set_array(ibufr, "stationOrSiteName", inputVals)
    eccodes.codes_set(ibufr, "pack", 1)
    outputVals = eccodes.codes_get_string_array(ibufr, "stationOrSiteName")
    assert len(outputVals) == 3
    assert outputVals[0] == "ARD2-LPTR"
    assert outputVals[1] == "EPFL-LPTR"
    assert outputVals[2] == "BOU2-LPTR"
    eccodes.codes_release(ibufr)


def test_bufr_keys_iterator():
    bid = eccodes.codes_bufr_new_from_samples("BUFR3_local_satellite")
    # Header keys only
    iterid = eccodes.codes_bufr_keys_iterator_new(bid)
    count = 0
    while eccodes.codes_bufr_keys_iterator_next(iterid):
        keyname = eccodes.codes_bufr_keys_iterator_get_name(iterid)
        assert "#" not in keyname
        count += 1
    # assert count == 54

    eccodes.codes_set(bid, "unpack", 1)
    eccodes.codes_bufr_keys_iterator_rewind(iterid)
    count = 0
    while eccodes.codes_bufr_keys_iterator_next(iterid):
        keyname = eccodes.codes_bufr_keys_iterator_get_name(iterid)
        count += 1
    # assert count == 157
    eccodes.codes_bufr_keys_iterator_rewind(iterid)
    eccodes.codes_bufr_keys_iterator_delete(iterid)
    eccodes.codes_release(bid)


def test_bufr_codes_is_missing():
    bid = eccodes.codes_bufr_new_from_samples("BUFR4_local")
    eccodes.codes_set(bid, "unpack", 1)
    assert eccodes.codes_is_missing(bid, "heightOfBarometerAboveMeanSeaLevel") == 1
    assert eccodes.codes_is_missing(bid, "blockNumber") == 1
    assert eccodes.codes_is_missing(bid, "stationOrSiteName") == 1
    assert eccodes.codes_is_missing(bid, "unexpandedDescriptors") == 0
    assert eccodes.codes_is_missing(bid, "ident") == 0

    eccodes.codes_set(bid, "stationOrSiteName", "Barca")
    eccodes.codes_set(bid, "pack", 1)

    assert eccodes.codes_is_missing(bid, "stationOrSiteName") == 0

    eccodes.codes_release(bid)


def test_bufr_multi_element_constant_arrays():
    eccodes.codes_bufr_multi_element_constant_arrays_off()
    bid = eccodes.codes_bufr_new_from_samples("BUFR3_local_satellite")
    eccodes.codes_set(bid, "unpack", 1)
    assert eccodes.codes_get_size(bid, "satelliteIdentifier") == 1
    eccodes.codes_release(bid)

    eccodes.codes_bufr_multi_element_constant_arrays_on()
    bid = eccodes.codes_bufr_new_from_samples("BUFR3_local_satellite")
    eccodes.codes_set(bid, "unpack", 1)
    numSubsets = eccodes.codes_get(bid, "numberOfSubsets")
    assert eccodes.codes_get_size(bid, "satelliteIdentifier") == numSubsets
    eccodes.codes_release(bid)


def test_bufr_new_from_samples_error():
    with pytest.raises(eccodes.FileNotFoundError):
        eccodes.codes_new_from_samples("nonExistentSample", eccodes.CODES_PRODUCT_BUFR)
    with pytest.raises(ValueError):
        eccodes.codes_new_from_samples("BUFR3_local", 1024)


def test_bufr_get_message_size():
    gid = eccodes.codes_bufr_new_from_samples("BUFR3_local")
    assert eccodes.codes_get_message_size(gid) == 279


def test_bufr_get_message_offset():
    gid = eccodes.codes_bufr_new_from_samples("BUFR3_local")
    assert eccodes.codes_get_message_offset(gid) == 0


def test_codes_bufr_key_is_header():
    bid = eccodes.codes_bufr_new_from_samples("BUFR4_local_satellite")
    assert eccodes.codes_bufr_key_is_header(bid, "edition")
    assert eccodes.codes_bufr_key_is_header(bid, "satelliteID")
    assert eccodes.codes_bufr_key_is_header(bid, "unexpandedDescriptors")

    with pytest.raises(eccodes.KeyValueNotFoundError):
        eccodes.codes_bufr_key_is_header(bid, "satelliteSensorIndicator")

    eccodes.codes_set(bid, "unpack", 1)
    assert not eccodes.codes_bufr_key_is_header(bid, "satelliteSensorIndicator")
    assert not eccodes.codes_bufr_key_is_header(bid, "#6#brightnessTemperature")


def test_codes_bufr_key_is_coordinate():
    if eccodes.codes_get_api_version(int) < 23100:
        pytest.skip("ecCodes version too old")

    bid = eccodes.codes_bufr_new_from_samples("BUFR4")
    assert not eccodes.codes_bufr_key_is_coordinate(bid, "edition")

    with pytest.raises(eccodes.KeyValueNotFoundError):
        eccodes.codes_bufr_key_is_coordinate(bid, "latitude")

    eccodes.codes_set(bid, "unpack", 1)
    assert eccodes.codes_bufr_key_is_coordinate(bid, "latitude")
    assert eccodes.codes_bufr_key_is_coordinate(bid, "#14#timePeriod")
    assert not eccodes.codes_bufr_key_is_coordinate(bid, "dewpointTemperature")

    eccodes.codes_release(bid)


def test_bufr_extract_headers():
    fpath = get_sample_fullpath("BUFR4_local.tmpl")
    if fpath is None:
        return
    headers = list(eccodes.codes_bufr_extract_headers(fpath))
    # Sample file contains just one message
    assert len(headers) == 1
    header = headers[0]
    assert header["edition"] == 4
    assert header["internationalDataSubCategory"] == 255
    assert header["masterTablesVersionNumber"] == 24
    assert header["ident"].strip() == "91334"
    assert header["rdbtimeSecond"] == 19
    assert math.isclose(header["localLongitude"], 151.83)


def test_bufr_dump(tmp_path):
    bid = eccodes.codes_bufr_new_from_samples("BUFR4")
    eccodes.codes_set(bid, "unpack", 1)
    p = tmp_path / "dump_bufr.txt"
    with open(p, "w") as fout:
        eccodes.codes_dump(bid, fout, "json")
    eccodes.codes_release(bid)


def test_bufr_copy_data():
    bid1 = eccodes.codes_bufr_new_from_samples("BUFR4_local")
    bid2 = eccodes.codes_bufr_new_from_samples("BUFR4")
    bid3 = eccodes.codes_bufr_copy_data(bid1, bid2)
    assert bid3


# ---------------------------------------------
# Experimental features
# ---------------------------------------------
def test_grib_nearest2():
    if "codes_grib_nearest_new" not in dir(eccodes):
        pytest.skip("codes_grib_nearest_new absent")
    gid = eccodes.codes_grib_new_from_samples("gg_sfc_grib2")
    lat, lon = 40, 20
    flags = eccodes.CODES_GRIB_NEAREST_SAME_GRID | eccodes.CODES_GRIB_NEAREST_SAME_POINT
    nid = eccodes.codes_grib_nearest_new(gid)
    assert nid > 0
    nearest = eccodes.codes_grib_nearest_find(nid, gid, lat, lon, flags)
    assert len(nearest) == 4
    expected_indexes = (2679, 2678, 2517, 2516)
    returned_indexes = (
        nearest[0].index,
        nearest[1].index,
        nearest[2].index,
        nearest[3].index,
    )
    assert sorted(expected_indexes) == sorted(returned_indexes)
    assert math.isclose(nearest[0].value, 295.22085, abs_tol=0.0001)
    assert math.isclose(nearest[2].distance, 24.16520, abs_tol=0.0001)

    # Error conditions
    with pytest.raises(eccodes.FunctionNotImplementedError):
        eccodes.codes_grib_nearest_find(nid, gid, lat, lon, flags, is_lsm=True)
    with pytest.raises(eccodes.FunctionNotImplementedError):
        eccodes.codes_grib_nearest_find(nid, gid, lat, lon, flags, False, npoints=5)

    eccodes.codes_release(gid)
    eccodes.codes_grib_nearest_delete(nid)
