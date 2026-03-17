import os.path

import numpy as np
import pytest

# flake8: noqa


# from eccodes import messages

SAMPLE_DATA_FOLDER = os.path.join(os.path.dirname(__file__), "sample-data")
TEST_DATA = os.path.join(SAMPLE_DATA_FOLDER, "era5-levels-members.grib")


def _test_Message_read():
    with open(TEST_DATA) as file:
        res1 = messages.Message.from_file(file)

    assert res1.message_get("paramId") == 129
    assert res1["paramId"] == 129
    assert list(res1)[0] == "globalDomain"
    assert list(res1.message_grib_keys("time"))[0] == "dataDate"
    assert "paramId" in res1
    assert len(res1) > 100

    with pytest.raises(KeyError):
        res1["non-existent-key"]

    assert res1.message_get("non-existent-key", default=1) == 1

    res2 = messages.Message.from_message(res1)
    for (k2, v2), (k1, v1) in zip(res2.items(), res1.items()):
        assert k2 == k1
        if isinstance(v2, np.ndarray) or isinstance(v1, np.ndarray):
            assert np.allclose(v2, v1)
        else:
            assert v2 == v1

    with open(TEST_DATA) as file:
        with pytest.raises(EOFError):
            while True:
                messages.Message.from_file(file)


def _test_Message_write(tmpdir):
    res = messages.Message.from_sample_name("regular_ll_pl_grib2")
    assert res["gridType"] == "regular_ll"

    res.message_set("Ni", 20)
    assert res["Ni"] == 20

    res["iDirectionIncrementInDegrees"] = 1.0
    assert res["iDirectionIncrementInDegrees"] == 1.0

    res.message_set("gridType", "reduced_gg")
    assert res["gridType"] == "reduced_gg"

    res["pl"] = [2.0, 3.0]
    assert np.allclose(res["pl"], [2.0, 3.0])

    # warn on errors
    res["centreDescription"] = "DUMMY"
    assert res["centreDescription"] != "DUMMY"
    res["edition"] = -1
    assert res["edition"] != -1

    # ignore errors
    res.errors = "ignore"
    res["centreDescription"] = "DUMMY"
    assert res["centreDescription"] != "DUMMY"

    # raise errors
    res.errors = "raise"
    with pytest.raises(KeyError):
        res["centreDescription"] = "DUMMY"

    with pytest.raises(NotImplementedError):
        del res["gridType"]

    out = tmpdir.join("test.grib")
    with open(str(out), "wb") as file:
        res.write(file)


def _test_ComputedKeysMessage_read():
    computed_keys = {
        "ref_time": (lambda m: str(m["dataDate"]) + str(m["dataTime"]), None),
        "error_key": (lambda m: 1 / 0, None),
        "centre": (lambda m: -1, lambda m, v: None),
    }
    with open(TEST_DATA) as file:
        res = messages.ComputedKeysMessage.from_file(file, computed_keys=computed_keys)

    assert res["paramId"] == 129
    assert res["ref_time"] == "201701010"
    assert len(res) > 100
    assert res["centre"] == -1

    with pytest.raises(ZeroDivisionError):
        res["error_key"]


def _test_ComputedKeysMessage_write():
    computed_keys = {
        "ref_time": (lambda m: "%s%04d" % (m["dataDate"], m["dataTime"]), None),
        "error_key": (lambda m: 1 / 0, None),
        "centre": (lambda m: -1, lambda m, v: None),
    }
    res = messages.ComputedKeysMessage.from_sample_name(
        "regular_ll_pl_grib2", computed_keys=computed_keys
    )
    res["dataDate"] = 20180101
    res["dataTime"] = 0
    assert res["ref_time"] == "201801010000"

    res["centre"] = 1


def _test_compat_create_exclusive(tmpdir):
    test_file = tmpdir.join("file.grib.idx")

    try:
        with messages.compat_create_exclusive(str(test_file)):
            raise RuntimeError("Test remove")
    except RuntimeError:
        pass

    with messages.compat_create_exclusive(str(test_file)) as file:
        file.write(b"Hi!")

    with pytest.raises(OSError):
        with messages.compat_create_exclusive(str(test_file)) as file:
            file.write(b"Hi!")


def _test_FileIndex():
    res = messages.FileIndex.from_filestream(
        messages.FileStream(TEST_DATA), ["paramId"]
    )
    assert res["paramId"] == [129, 130]
    assert len(res) == 1
    assert list(res) == ["paramId"]
    assert res.first()

    with pytest.raises(ValueError):
        res.getone("paramId")

    with pytest.raises(KeyError):
        res["non-existent-key"]

    subres = res.subindex(paramId=130)

    assert subres.get("paramId") == [130]
    assert subres.getone("paramId") == 130
    assert len(subres) == 1


def _test_FileIndex_from_indexpath_or_filestream(tmpdir):
    grib_file = tmpdir.join("file.grib")

    with open(TEST_DATA, "rb") as file:
        grib_file.write_binary(file.read())

    # create index file
    res = messages.FileIndex.from_indexpath_or_filestream(
        messages.FileStream(str(grib_file)), ["paramId"]
    )
    assert isinstance(res, messages.FileIndex)

    # read index file
    res = messages.FileIndex.from_indexpath_or_filestream(
        messages.FileStream(str(grib_file)), ["paramId"]
    )
    assert isinstance(res, messages.FileIndex)

    # do not read nor create the index file
    res = messages.FileIndex.from_indexpath_or_filestream(
        messages.FileStream(str(grib_file)), ["paramId"], indexpath=""
    )
    assert isinstance(res, messages.FileIndex)

    # can't create nor read index file
    res = messages.FileIndex.from_indexpath_or_filestream(
        messages.FileStream(str(grib_file)),
        ["paramId"],
        indexpath=str(tmpdir.join("non-existent-folder").join("non-existent-file")),
    )
    assert isinstance(res, messages.FileIndex)

    # trigger mtime check
    grib_file.remove()
    with open(TEST_DATA, "rb") as file:
        grib_file.write_binary(file.read())

    res = messages.FileIndex.from_indexpath_or_filestream(
        messages.FileStream(str(grib_file)), ["paramId"]
    )
    assert isinstance(res, messages.FileIndex)


def _test_FileIndex_errors():
    class MyMessage(messages.ComputedKeysMessage):
        computed_keys = {"error_key": lambda m: 1 / 0}

    stream = messages.FileStream(TEST_DATA, message_class=MyMessage)
    res = messages.FileIndex.from_filestream(stream, ["paramId", "error_key"])
    assert res["paramId"] == [129, 130]
    assert len(res) == 2
    assert list(res) == ["paramId", "error_key"]
    assert res["error_key"] == ["undef"]


def _test_FileStream():
    res = messages.FileStream(TEST_DATA)
    leader = res.first()
    assert len(leader) > 100
    assert sum(1 for _ in res) == leader["count"]
    assert len(res.index(["paramId"])) == 1

    # __file__ is not a GRIB, but contains the "GRIB" string, so it is a very tricky corner case
    res = messages.FileStream(str(__file__))
    with pytest.raises(EOFError):
        res.first()

    res = messages.FileStream(str(__file__), errors="ignore")
    with pytest.raises(EOFError):
        res.first()

    # res = messages.FileStream(str(__file__), errors='raise')
    # with pytest.raises(bindings.EcCodesError):
    #     res.first()
