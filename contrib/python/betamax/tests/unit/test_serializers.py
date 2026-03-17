"""Tests for serializers."""
import os
import unittest

import pytest

from betamax.serializers import base
from betamax.serializers import json_serializer
from betamax.serializers import proxy


class TestJSONSerializer(unittest.TestCase):
    """Tests around the JSONSerializer default."""

    def setUp(self):
        """Fixture setup."""
        self.cassette_dir = 'fake_dir'
        self.cassette_name = 'cassette_name'

    def test_generate_cassette_name(self):
        """Verify the behaviour of generate_cassette_name."""
        assert (os.path.join('fake_dir', 'cassette_name.json') ==
                json_serializer.JSONSerializer.generate_cassette_name(
                    self.cassette_dir,
                    self.cassette_name,
                ))

    def test_generate_cassette_name_with_instance(self):
        """Verify generate_cassette_name works on an instance too."""
        serializer = json_serializer.JSONSerializer()
        assert (os.path.join('fake_dir', 'cassette_name.json') ==
                serializer.generate_cassette_name(self.cassette_dir,
                                                  self.cassette_name))


class Serializer(base.BaseSerializer):
    """Serializer to test NotImplementedError exceptions."""

    name = 'test'


class BytesSerializer(base.BaseSerializer):
    """Serializer to test stored_as_binary."""

    name = 'bytes-test'
    stored_as_binary = True

    # NOTE(sigmavirus24): These bytes, when decoded, result in a
    # UnicodeDecodeError
    serialized_bytes = b"hi \xAD"

    def serialize(self, *args):
        """Return the problematic bytes."""
        return self.serialized_bytes

    def deserialize(self, *args):
        """Return the problematic bytes."""
        return self.serialized_bytes


class TestBaseSerializer(unittest.TestCase):
    """Tests around BaseSerializer behaviour."""

    def test_serialize_is_an_interface(self):
        """Verify we handle unimplemented methods."""
        serializer = Serializer()
        with pytest.raises(NotImplementedError):
            serializer.serialize({})

    def test_deserialize_is_an_interface(self):
        """Verify we handle unimplemented methods."""
        serializer = Serializer()
        with pytest.raises(NotImplementedError):
            serializer.deserialize('path')

    def test_requires_a_name(self):
        """Verify we handle unimplemented methods."""
        with pytest.raises(ValueError):
            base.BaseSerializer()


class TestBinarySerializers(unittest.TestCase):
    """Verify the behaviour of stored_as_binary=True."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        serializer = BytesSerializer()
        self.cassette_path = 'test_cassette.test'
        self.proxy = proxy.SerializerProxy(
            serializer,
            self.cassette_path,
            allow_serialization=True,
        )

    def test_serialize(self):
        """Verify we use the right mode with open()."""
        mode = self.proxy.corrected_file_mode('w')
        assert mode == 'wb'

    def test_deserialize(self):
        """Verify we use the right mode with open()."""
        mode = self.proxy.corrected_file_mode('r')
        assert mode == 'rb'


class TestTextSerializer(unittest.TestCase):
    """Verify the default behaviour of stored_as_binary."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        serializer = Serializer()
        self.cassette_path = 'test_cassette.test'
        self.proxy = proxy.SerializerProxy(
            serializer,
            self.cassette_path,
            allow_serialization=True,
        )

    def test_serialize(self):
        """Verify we use the right mode with open()."""
        mode = self.proxy.corrected_file_mode('w')
        assert mode == 'w'

    def test_deserialize(self):
        """Verify we use the right mode with open()."""
        mode = self.proxy.corrected_file_mode('r')
        assert mode == 'r'
