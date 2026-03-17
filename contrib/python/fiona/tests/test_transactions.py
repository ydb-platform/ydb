from collections import defaultdict
import logging
import os
import pytest
from random import uniform, randint

import fiona
from fiona.model import Feature
import fiona.ogrext

from .conftest import requires_gdal2

has_gpkg = "GPKG" in fiona.supported_drivers.keys()


def create_records(count):
    for n in range(count):
        record = {
            "geometry": {
                "type": "Point",
                "coordinates": [uniform(-180, 180), uniform(-90, 90)],
            },
            "properties": {"value": randint(0, 1000)},
        }
        yield Feature.from_dict(**record)


class DebugHandler(logging.Handler):
    def __init__(self, pattern):
        logging.Handler.__init__(self)
        self.pattern = pattern
        self.history = defaultdict(lambda: 0)

    def emit(self, record):
        if self.pattern in record.msg:
            self.history[record.msg] += 1


log = logging.getLogger()


@requires_gdal2
@pytest.mark.skipif(not has_gpkg, reason="Requires geopackage driver")
class TestTransaction:
    def setup_method(self):
        self.handler = DebugHandler(pattern="transaction")
        self.handler.setLevel(logging.DEBUG)
        log.setLevel(logging.DEBUG)
        log.addHandler(self.handler)

    def teardown_method(self):
        log.removeHandler(self.handler)

    def test_transaction(self, tmpdir):
        """
        Test transaction start/commit is called the expected number of times,
        and that the default transaction size can be overloaded. The test uses
        a custom logging handler to listen for the debug messages produced
        when the transaction is started/committed.
        """
        num_records = 250
        transaction_size = 100

        assert fiona.ogrext.DEFAULT_TRANSACTION_SIZE == 20000
        fiona.ogrext.DEFAULT_TRANSACTION_SIZE = transaction_size
        assert fiona.ogrext.DEFAULT_TRANSACTION_SIZE == transaction_size

        path = str(tmpdir.join("output.gpkg"))

        schema = {"geometry": "Point", "properties": {"value": "int"}}

        with fiona.open(path, "w", driver="GPKG", schema=schema) as dst:
            dst.writerecords(create_records(num_records))

        assert self.handler.history["Starting transaction (initial)"] == 1
        assert (
            self.handler.history["Starting transaction (intermediate)"]
            == num_records // transaction_size
        )
        assert (
            self.handler.history["Committing transaction (intermediate)"]
            == num_records // transaction_size
        )
        assert self.handler.history["Committing transaction (final)"] == 1

        with fiona.open(path, "r") as src:
            assert len(src) == num_records
