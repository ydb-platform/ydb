"""Tests of skipped field log message filtering"""

import logging
import os

import fiona
from fiona.logutils import LogFiltering, FieldSkipLogFilter


def test_filtering(caplog):
    """Test that ordinary log messages pass"""
    logger = logging.getLogger()
    with LogFiltering(logger, FieldSkipLogFilter()):
        logger.warning("Attention!")
        logger.warning("Skipping field 1")
        logger.warning("Skipping field 2")
        logger.warning("Danger!")
        logger.warning("Skipping field 1")

    assert len(caplog.records) == 4
    assert caplog.records[0].getMessage() == "Attention!"
    assert caplog.records[1].getMessage() == "Skipping field 1"
    assert caplog.records[2].getMessage() == "Skipping field 2"
    assert caplog.records[3].getMessage() == "Danger!"


def test_skipping_slice(caplog, data_dir):
    """Collection filters out all but one warning message"""
    with fiona.open(os.path.join(data_dir, "issue627.geojson")) as src:
        results = list(src)
    assert len(results) == 3
    assert not any(['skip_me' in f['properties'] for f in results])
    assert len([rec for rec in caplog.records if rec.getMessage().startswith('Skipping')]) == 1


def test_skipping_list(caplog, data_dir):
    """Collection filters out all but one warning message"""
    with fiona.open(os.path.join(data_dir, "issue627.geojson")) as src:
        results = list(src)
    assert len(results) == 3
    assert not any(['skip_me' in f['properties'] for f in results])
    assert len([rec for rec in caplog.records if rec.getMessage().startswith('Skipping')]) == 1


def test_log_filter_exception(caplog):
    """FieldSkipLogFilter handles exceptions from log.exception()."""
    logger = logging.getLogger()
    with LogFiltering(logger, FieldSkipLogFilter()):
        logger.exception(ValueError("Oh no"))

    assert len(caplog.records) == 1
    assert caplog.records[0].getMessage() == "Oh no"
