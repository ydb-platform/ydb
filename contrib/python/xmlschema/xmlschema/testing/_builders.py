#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
# mypy: ignore-errors
import pdb
import os
import ast
import pickle
import re
import time
import logging
import tempfile
import warnings
from importlib import util as importlib_util
from xml.etree import ElementTree

try:
    import lxml.etree as lxml_etree
except ImportError:
    lxml_etree = None
    lxml_etree_element = None
else:
    lxml_etree_element = lxml_etree.Element

import xmlschema
from xmlschema import XMLSchemaBase, XMLSchema11, XMLSchemaValidationError, \
    XMLSchemaParseError, UnorderedConverter, ParkerConverter, BadgerFishConverter, \
    AbderaConverter, JsonMLConverter, ColumnarConverter, GDataConverter
from xmlschema.names import XSD_IMPORT
from xmlschema.utils.qnames import local_name
from xmlschema.utils.etree import etree_tostring
from xmlschema.resources import fetch_namespaces
from xmlschema.validators import XsdType, Xsd11ComplexType
from xmlschema.dataobjects import DataElementConverter, DataBindingConverter, DataElement
from xmlschema.loaders import LocationSchemaLoader, SafeSchemaLoader

try:
    from xmlschema.extras.codegen import PythonGenerator
except ImportError:
    PythonGenerator = None

from ._helpers import iter_nested_items, etree_elements_assert_equal
from ._test_case_classes import XsdValidatorTestCase
from ._observers import SchemaObserver


OBJ_ID_PATTERN = re.compile(r" at 0x[0-9a-fA-F]+")


def make_schema_test_class(test_file, test_args, test_num, schema_class, check_with_lxml):
    """
    Creates a schema test class.

    :param test_file: the schema test file path.
    :param test_args: line arguments for test case.
    :param test_num: a positive integer number associated with the test case.
    :param schema_class: the schema class to use.
    :param check_with_lxml: if `True` compare with lxml XMLSchema class, reporting anomalies. \
    Works only for XSD 1.0 tests.
    """
    xsd_file = os.path.relpath(test_file)

    # Extract schema test arguments
    expected_errors = test_args.errors
    expected_warnings = test_args.warnings
    inspect = test_args.inspect
    locations = test_args.locations
    defuse = test_args.defuse
    no_pickle = test_args.no_pickle
    skip_location_loader = test_args.skip_location_loader
    debug_mode = test_args.debug
    codegen = test_args.codegen
    loglevel = logging.DEBUG if debug_mode else None

    class TestSchema(XsdValidatorTestCase):

        @classmethod
        def setUpClass(cls):
            cls.schema_class = schema_class
            cls.errors = []
            cls.longMessage = True

            if debug_mode:
                print("\n##\n## Testing %r schema in debug mode.\n##" % xsd_file)
                pdb.set_trace()

        def check_xsd_file(self):
            if expected_errors > 0:
                schema = schema_class(xsd_file, validation='lax', locations=locations,
                                      defuse=defuse, loglevel=loglevel)
            else:
                schema = schema_class(xsd_file, locations=locations,
                                      defuse=defuse, loglevel=loglevel)
            self.errors.extend(schema.maps.all_errors)

            if inspect:
                components_ids = {id(c) for c in schema.maps.iter_components()}
                components_ids.update(id(c) for c in schema.meta_schema.iter_components())
                missing = [
                    c for c in SchemaObserver.components if id(c) not in components_ids
                ]
                if missing:
                    raise ValueError("schema missing %d components: %r" % (len(missing), missing))

            # Pickling test (skip inspected schema classes test)
            if not inspect and not no_pickle:
                obj = pickle.dumps(schema)
                deserialized_schema = pickle.loads(obj)
                self.assertTrue(isinstance(deserialized_schema, XMLSchemaBase), msg=xsd_file)
                self.assertEqual(schema.built, deserialized_schema.built, msg=xsd_file)

            # XPath node tree tests
            if not inspect and not self.errors:
                xpath_root = schema.xpath_node
                element_nodes = [x for x in xpath_root.iter() if hasattr(x, 'elem')]
                descendants = [x for x in xpath_root.iter_descendants('descendant-or-self')]
                self.assertTrue(x in descendants for x in element_nodes)

                context_xsd_elements = [e.value for e in element_nodes]
                for xsd_element in schema.iter():
                    # Context elements can include elements of other schemas (by element ref)
                    self.assertIn(xsd_element, context_xsd_elements, msg=xsd_file)

            # Checks on XSD types
            for xsd_type in schema.maps.iter_components(xsd_classes=XsdType):
                self.assertIn(
                    xsd_type.content_type_label, {'empty', 'simple', 'element-only', 'mixed'},
                    msg=xsd_file
                )

            # Check that the schema is valid also with XSD 1.1 validator
            if not expected_errors and schema_class.XSD_VERSION == '1.0':
                try:
                    XMLSchema11(xsd_file, locations=locations, defuse=defuse, loglevel=loglevel)
                except XMLSchemaParseError as err:
                    if not isinstance(err.validator, Xsd11ComplexType) or \
                            "is simple or has a simple content" not in str(err):
                        raise  # Not a case of forbidden complex content extension

                    schema = schema_class(xsd_file, validation='lax', locations=locations,
                                          defuse=defuse, loglevel=loglevel)
                    for error in schema.all_errors:
                        if not isinstance(err.validator, Xsd11ComplexType) or \
                                "is simple or has a simple content" not in str(err):
                            raise error

            # Test alternative schema loaders
            if not expected_errors and not skip_location_loader:
                other = schema_class(
                    xsd_file, loader_class=SafeSchemaLoader, locations=locations,
                    defuse=defuse, loglevel=loglevel
                )
                urls = set(s.url for s in schema.maps.schemas)
                other_urls = set(s.url for s in other.maps.schemas)
                self.assertTrue(urls.issubset(other_urls), msg=xsd_file)

                if not skip_location_loader:
                    other = schema_class(
                        xsd_file, loader_class=LocationSchemaLoader, locations=locations,
                        defuse=defuse, loglevel=loglevel
                    )
                    urls = set(s.url for s in schema.maps.schemas)
                    other_urls = set(s.url for s in other.maps.schemas)
                    self.assertTrue(urls.issubset(other_urls), msg=xsd_file)

            # Check XML bindings module only for schemas that do not have errors
            if codegen and PythonGenerator is not None and not self.errors and \
                    all('schemaLocation' in e.attrib for e in schema.root if e.tag == XSD_IMPORT):

                generator = PythonGenerator(schema)
                with tempfile.TemporaryDirectory() as tempdir:
                    cwd = os.getcwd()
                    try:
                        schema.export(tempdir, save_remote=True)
                        os.chdir(tempdir)
                        generator.render_to_files('bindings.py.jinja')

                        spec = importlib_util.spec_from_file_location(tempdir, 'bindings.py')
                        module = importlib_util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                    finally:
                        os.chdir(cwd)

        def check_xsd_file_with_lxml(self, xmlschema_time):
            start_time = time.time()
            lxs = lxml_etree.parse(xsd_file)
            try:
                lxml_etree.XMLSchema(lxs.getroot())
            except lxml_etree.XMLSchemaParseError as err:
                if not self.errors:
                    print("\nSchema error with lxml.etree.XMLSchema for file {!r} ({}): {}".format(
                        xsd_file, self.__class__.__name__, str(err)
                    ))
            else:
                if self.errors:
                    msg = "\nUnrecognized errors with lxml.etree.XMLSchema for file {!r} ({}): {}"
                    print(msg.format(
                        xsd_file, self.__class__.__name__,
                        '\n++++++\n'.join([str(e) for e in self.errors])
                    ))
                lxml_schema_time = time.time() - start_time
                if lxml_schema_time >= xmlschema_time:
                    msg = "\nSlower lxml.etree.XMLSchema ({:.3f}s VS {:.3f}s) with file {!r} ({})"
                    print(msg.format(
                        lxml_schema_time, xmlschema_time, xsd_file, self.__class__.__name__
                    ))

        def test_xsd_file(self):
            if inspect:
                SchemaObserver.clear()
            del self.errors[:]

            start_time = time.time()
            if expected_warnings > 0:
                with warnings.catch_warnings(record=True) as include_import_warnings:
                    warnings.simplefilter("always")
                    self.check_xsd_file()
                    self.assertEqual(len(include_import_warnings), expected_warnings, msg=xsd_file)
            else:
                self.check_xsd_file()

            # Check with lxml.etree.XMLSchema class
            if check_with_lxml and lxml_etree is not None:
                self.check_xsd_file_with_lxml(xmlschema_time=time.time() - start_time)
            self.check_errors(xsd_file, expected_errors)

    TestSchema.__name__ = TestSchema.__qualname__ = str(f'TestSchema{test_num:03}')
    return TestSchema


def make_validation_test_class(test_file, test_args, test_num, schema_class, check_with_lxml):
    """
    Creates a test class for checking xml instance validation.

    :param test_file: the XML test file path.
    :param test_args: line arguments for test case.
    :param test_num: a positive integer number associated with the test case.
    :param schema_class: the schema class to use.
    :param check_with_lxml: if `True` compare with lxml XMLSchema class, reporting anomalies. \
    Works only for XSD 1.0 tests.
    """
    xml_file = os.path.relpath(test_file)
    msg_tmpl = '%s: {0}:\n\n{1}' % xml_file

    # Extract schema test arguments
    expected_errors = test_args.errors
    expected_warnings = test_args.warnings
    inspect = test_args.inspect
    locations = test_args.locations
    defuse = test_args.defuse
    validation_only = test_args.validation_only
    no_pickle = test_args.no_pickle
    lax_encode = test_args.lax_encode
    debug_mode = test_args.debug
    codegen = test_args.codegen

    class TestValidator(XsdValidatorTestCase):

        @classmethod
        def setUpClass(cls):
            # Builds schema instance using 'lax' validation mode
            # to accepts also schemas with not crashing errors.
            cls.schema_class = schema_class
            source, _locations = xmlschema.fetch_schema_locations(xml_file, locations)
            cls.schema = schema_class(source, validation='lax', locations=_locations, defuse=defuse)
            if check_with_lxml and lxml_etree is not None:
                cls.lxml_schema = lxml_etree.parse(source)

            cls.errors = []
            cls.chunks = []
            cls.longMessage = True

            if debug_mode:
                print("\n##\n## Testing %r validation in debug mode.\n##" % xml_file)
                pdb.set_trace()

        def check_decode_encode(self, root, converter=None, etree_element_class=None, **kwargs):
            lossy = converter in (ParkerConverter, AbderaConverter, ColumnarConverter)
            losslessly = converter is JsonMLConverter
            unordered = converter not in (AbderaConverter, JsonMLConverter) or \
                kwargs.get('unordered', False)
            if self.schema.validity != 'valid' and 'validation' not in kwargs:
                kwargs['validation'] = 'lax'

            decoded_data1 = self.schema.decode(root, converter=converter, **kwargs)
            if isinstance(decoded_data1, tuple):
                decoded_data1 = decoded_data1[0]  # When validation='lax'

            for _ in iter_nested_items(decoded_data1):
                pass

            try:
                elem1 = self.schema.encode(
                    decoded_data1, path=root.tag, converter=converter,
                    etree_element_class=etree_element_class, **kwargs
                )
            except XMLSchemaValidationError as err:
                raise AssertionError(msg_tmpl.format("error during re-encoding", str(err)))

            if isinstance(elem1, tuple):
                # When validation='lax'
                if converter is not ParkerConverter and converter is not ColumnarConverter:
                    for e in elem1[1]:
                        self.check_namespace_prefixes(str(e))
                elem1 = elem1[0]

            # Checks if the encoded element is of the same type of the root element
            self.assertFalse(hasattr(root, 'nsmap') ^ hasattr(elem1, 'nsmap'))

            # Checks the encoded element to not contains reserved namespace prefixes
            if 'namespaces' in kwargs:
                self.check_namespace_prefixes(
                    etree_tostring(elem1, namespaces=kwargs['namespaces'])
                )

            # Main check: compare original a re-encoded tree
            try:
                etree_elements_assert_equal(root, elem1, strict=False, unordered=unordered)
            except AssertionError as err:
                # If the check fails retry only if the converter is lossy (e.g. ParkerConverter)
                # or if the XML case has defaults taken from the schema or some part of data
                # decoding is skipped by schema wildcards (set the specific argument in testfiles).
                if lax_encode:
                    # can't ensure encode equivalence on this case,
                    # for example if the test case use defaults.
                    pass
                elif lossy or unordered:
                    # can't check encode equivalence if the converter
                    # is lossy or if it is not fully ordered.
                    pass
                elif losslessly:
                    if debug_mode:
                        pdb.set_trace()
                    raise AssertionError(
                        msg_tmpl.format("encoded tree differs from original", str(err))
                    )
                else:
                    # Lossy or augmenting cases are checked with another decoding/encoding pass
                    decoded_data2 = self.schema.decode(elem1, converter=converter, **kwargs)
                    if isinstance(decoded_data2, tuple):
                        decoded_data2 = decoded_data2[0]

                    try:
                        self.assertEqual(decoded_data1, decoded_data2, msg=xml_file)
                    except AssertionError:
                        if debug_mode:
                            pdb.set_trace()
                        raise

                    elem2 = self.schema.encode(
                        decoded_data2, path=root.tag, converter=converter,
                        etree_element_class=etree_element_class, **kwargs
                    )
                    if isinstance(elem2, tuple):
                        elem2 = elem2[0]

                    try:
                        etree_elements_assert_equal(
                            elem1, elem2, strict=False, unordered=unordered
                        )
                    except AssertionError as err:
                        if debug_mode:
                            pdb.set_trace()
                        raise AssertionError(
                            msg_tmpl.format("encoded tree differs after second pass", str(err))
                        )

        def check_json_serialization(self, root, converter=None,
                                     etree_element_class=None, **kwargs):
            lossy = converter in (ParkerConverter, AbderaConverter, ColumnarConverter)
            unordered = converter not in (AbderaConverter, JsonMLConverter) or \
                kwargs.get('unordered', False)

            if self.schema.validity != 'valid' and 'validation' not in kwargs:
                kwargs['validation'] = 'lax'

            # Use str instead of float in order to preserve original data
            kwargs['decimal_type'] = str

            json_data1 = xmlschema.to_json(root, schema=self.schema, converter=converter, **kwargs)
            if isinstance(json_data1, tuple):
                json_data1 = json_data1[0]

            elem1 = xmlschema.from_json(
                json_data1, schema=self.schema, path=root.tag, converter=converter,
                etree_element_class=etree_element_class, **kwargs
            )
            if isinstance(elem1, tuple):
                elem1 = elem1[0]

            if lax_encode:
                kwargs['validation'] = kwargs.get('validation', 'lax')

            json_data2 = xmlschema.to_json(
                elem1, schema=self.schema, converter=converter, **kwargs
            )
            if isinstance(json_data2, tuple):
                json_data2 = json_data2[0]

            if json_data2 != json_data1 and (lax_encode or lossy or unordered):
                # Can't ensure decode equivalence if the test case use defaults,
                # white spaces are replaced/collapsed or the converter is lossy
                # or the decoding is unordered.
                return

            self.assertEqual(json_data2, json_data1, msg=xml_file)

        def check_decoding_with_element_tree(self):
            del self.errors[:]
            del self.chunks[:]

            def do_decoding():
                for obj in self.schema.iter_decode(xml_file):
                    if isinstance(obj, (xmlschema.XMLSchemaDecodeError,
                                        xmlschema.XMLSchemaValidationError)):
                        self.errors.append(obj)
                    else:
                        self.chunks.append(obj)

            if expected_warnings == 0:
                do_decoding()
            else:
                with warnings.catch_warnings(record=True) as include_import_warnings:
                    warnings.simplefilter("always")
                    do_decoding()
                    self.assertEqual(len(include_import_warnings), expected_warnings, msg=xml_file)

            self.check_errors(xml_file, expected_errors)

            if not self.chunks:
                raise ValueError("No decoded object returned!!")
            elif len(self.chunks) > 1:
                raise ValueError("Too many ({}) decoded objects returned: {}".format(
                    len(self.chunks), self.chunks)
                )
            elif not self.errors:
                try:
                    skip_decoded_data = self.schema.decode(xml_file, validation='skip')
                    self.assertEqual(skip_decoded_data, self.chunks[0], msg=xml_file)
                except AssertionError:
                    if not lax_encode:
                        raise

        def check_schema_serialization(self):
            # Repeat with serialized-deserialized schema (only for Python 3)
            serialized_schema = pickle.dumps(self.schema)

            deserialized_schema = pickle.loads(serialized_schema)
            deserialized_errors = []
            deserialized_chunks = []

            for obj in deserialized_schema.iter_decode(xml_file):
                if isinstance(obj, xmlschema.XMLSchemaValidationError):
                    deserialized_errors.append(obj)
                else:
                    deserialized_chunks.append(obj)

            self.assertEqual(len(deserialized_errors), len(self.errors), msg=xml_file)
            self.assertEqual(deserialized_chunks, self.chunks, msg=xml_file)

        def check_decode_api(self):
            # Compare with the decode API and other validation modes
            strict_decoded_data = self.schema.decode(xml_file)
            lax_decoded_data = self.schema.decode(xml_file, validation='lax')
            skip_decoded_data = self.schema.decode(xml_file, validation='skip')

            self.assertEqual(strict_decoded_data, self.chunks[0], msg=xml_file)
            self.assertEqual(lax_decoded_data[0], self.chunks[0], msg=xml_file)
            self.assertEqual(skip_decoded_data, self.chunks[0], msg=xml_file)

        def check_data_conversion_with_element_tree(self):
            root = ElementTree.parse(xml_file).getroot()
            namespaces = fetch_namespaces(xml_file)  # need a collapsed nsmap
            options = {'namespaces': namespaces, 'xmlns_processing': 'none'}

            self.check_decode_encode(root, cdata_prefix='#', **options)  # Default converter
            self.check_decode_encode(root, UnorderedConverter, cdata_prefix='#', **options)
            self.check_decode_encode(root, ParkerConverter, validation='lax', **options)
            self.check_decode_encode(root, ParkerConverter, validation='skip', **options)
            self.check_decode_encode(root, BadgerFishConverter, **options)
            self.check_decode_encode(root, GDataConverter, **options)
            self.check_decode_encode(root, AbderaConverter, **options)
            self.check_decode_encode(root, JsonMLConverter, **options)
            self.check_decode_encode(root, ColumnarConverter, validation='lax', **options)

            self.check_decode_encode(root, DataElementConverter, **options)
            self.check_decode_encode(root, DataBindingConverter, **options)
            self.schema.maps.clear_bindings()

            self.check_json_serialization(root, cdata_prefix='#', **options)
            self.check_json_serialization(root, UnorderedConverter, **options)
            self.check_json_serialization(root, ParkerConverter, validation='lax', **options)
            self.check_json_serialization(root, ParkerConverter, validation='skip', **options)
            self.check_json_serialization(root, BadgerFishConverter, **options)
            self.check_json_serialization(root, GDataConverter, **options)
            self.check_json_serialization(root, AbderaConverter, **options)
            self.check_json_serialization(root, JsonMLConverter, **options)
            self.check_json_serialization(root, ColumnarConverter, validation='lax', **options)

            self.check_decode_to_objects(root)
            self.check_decode_to_objects(root, with_bindings=True)
            self.schema.maps.clear_bindings()

        def check_decode_to_objects(self, root, with_bindings=False):
            validation = 'lax' if self.schema.validity != 'valid' else 'strict'

            data_element = self.schema.to_objects(xml_file, with_bindings, validation=validation)
            if validation == 'lax':
                self.assertIsInstance(data_element, tuple)
                data_element = data_element[0]

            self.assertIsInstance(data_element, DataElement)
            self.assertEqual(data_element.tag, root.tag)

            if not with_bindings:
                self.assertIs(data_element.__class__, DataElement)
            else:
                self.assertEqual(data_element.tag, root.tag)
                self.assertTrue(data_element.__class__.__name__.endswith('Binding'))

        def check_data_conversion_with_lxml(self):
            xml_tree = lxml_etree.parse(xml_file)

            lxml_errors = []
            lxml_decoded_chunks = []
            for obj in self.schema.iter_decode(xml_tree):
                if isinstance(obj, xmlschema.XMLSchemaValidationError):
                    lxml_errors.append(obj)
                else:
                    lxml_decoded_chunks.append(obj)

            self.assertEqual(lxml_decoded_chunks, self.chunks, msg=xml_file)
            self.assertEqual(len(lxml_errors), len(self.errors), msg=xml_file)

            if not lxml_errors:
                root = xml_tree.getroot()

                options = {
                    'etree_element_class': lxml_etree_element,
                }
                self.check_decode_encode(root, cdata_prefix='#', **options)  # Default converter
                self.check_decode_encode(root, UnorderedConverter, cdata_prefix='#', **options)
                self.check_decode_encode(root, BadgerFishConverter, **options)
                self.check_decode_encode(root, GDataConverter, **options)
                self.check_decode_encode(root, JsonMLConverter, **options)

                # Tests with converters that loss namespace information and JSON
                # serialization: need to provide a full namespace map and don't
                # update that map.
                namespaces = fetch_namespaces(xml_file, root_only=False)
                if namespaces.get(''):
                    # Add a not empty prefix for encoding to avoid the use of reserved prefix ns0
                    namespaces['tns0'] = namespaces['']

                options = {
                    'etree_element_class': lxml_etree_element,
                    'namespaces': namespaces,
                    'xmlns_processing': 'none'
                }
                self.check_decode_encode(root, ParkerConverter, validation='lax', **options)
                self.check_decode_encode(root, ParkerConverter, validation='skip', **options)
                self.check_decode_encode(root, AbderaConverter, **options)

                self.check_json_serialization(root, cdata_prefix='#', **options)
                self.check_json_serialization(root, UnorderedConverter, **options)
                self.check_json_serialization(root, ParkerConverter, validation='lax', **options)
                self.check_json_serialization(root, ParkerConverter, validation='skip', **options)
                self.check_json_serialization(root, BadgerFishConverter, **options)
                self.check_json_serialization(root, GDataConverter, **options)
                self.check_json_serialization(root, AbderaConverter, **options)
                self.check_json_serialization(root, JsonMLConverter, **options)

        def check_with_lxml_iterparse(self):
            iterparse = lxml_etree.iterparse

            lxml_errors = []
            lxml_decoded_chunks = []
            for obj in self.schema.iter_decode(xml_file, iterparse=iterparse):
                if isinstance(obj, xmlschema.XMLSchemaValidationError):
                    lxml_errors.append(obj)
                else:
                    lxml_decoded_chunks.append(obj)

            self.assertEqual(lxml_decoded_chunks, self.chunks, msg=xml_file)
            self.assertEqual(len(lxml_errors), len(self.errors), msg=xml_file)

            if not lxml_errors:
                self.assertTrue(self.schema.is_valid(xml_file), msg=xml_file)
            else:
                self.assertFalse(self.schema.is_valid(xml_file), msg=xml_file)

        def check_validate_and_is_valid_api(self):
            if expected_errors:
                self.assertFalse(self.schema.is_valid(xml_file), msg=xml_file)
                with self.assertRaises(XMLSchemaValidationError, msg=xml_file):
                    self.schema.validate(xml_file)
            else:
                self.assertTrue(self.schema.is_valid(xml_file), msg=xml_file)
                self.assertIsNone(self.schema.validate(xml_file), msg=xml_file)

        def check_iter_errors(self):
            def compare_error_reasons(reason, other_reason):
                if ' at 0x' in reason:
                    self.assertEqual(
                        OBJ_ID_PATTERN.sub(' at 0xff', reason),
                        OBJ_ID_PATTERN.sub(' at 0xff', other_reason),
                        msg=xml_file
                    )
                else:
                    self.assertEqual(reason, other_reason, msg=xml_file)

            errors = list(self.schema.iter_errors(xml_file))
            for e in errors:
                self.assertIsInstance(e.reason, str, msg=xml_file)
            self.assertEqual(len(errors), expected_errors, msg=xml_file)

            module_api_errors = list(xmlschema.iter_errors(xml_file, schema=self.schema))
            for e, api_error in zip(errors, module_api_errors):
                compare_error_reasons(e.reason, api_error.reason)
            self.assertEqual(len(errors), len(module_api_errors), msg=xml_file)

            lazy_errors = list(xmlschema.iter_errors(xml_file, schema=self.schema, lazy=True))
            for e, lazy_error in zip(errors, lazy_errors):
                compare_error_reasons(e.reason, lazy_error.reason)
            self.assertEqual(len(errors), len(lazy_errors), msg=xml_file)

            # TODO: Test also lazy validation with lazy=2.
            #  This needs two fixes in XPath:
            #   1) find has to retrieve also element substitutes
            #   2) multiple XSD type match on tokens that have wildcard parent (eg. /root/*/name)

        def check_lxml_validation(self):
            try:
                schema = lxml_etree.XMLSchema(self.lxml_schema.getroot())
            except lxml_etree.XMLSchemaParseError:
                print("\nSkip lxml.etree.XMLSchema validation test for {!r} ({})".
                      format(xml_file, TestValidator.__name__, ))
            else:
                xml_tree = lxml_etree.parse(xml_file)
                if self.errors:
                    self.assertFalse(schema.validate(xml_tree), msg=xml_file)
                else:
                    self.assertTrue(schema.validate(xml_tree), msg=xml_file)

        def check_validation_with_generated_code(self):
            generator = PythonGenerator(self.schema)

            python_module = generator.render('bindings.py.jinja')[0]
            ast_module = ast.parse(python_module)
            self.assertIsInstance(ast_module, ast.Module)

            with tempfile.TemporaryDirectory() as tempdir:
                module_name = '{}.py'.format(self.schema.name.rstrip('.xsd'))
                cwd: str = os.getcwd()

                try:
                    self.schema.export(tempdir, save_remote=True)
                    os.chdir(tempdir)
                    with open(module_name, 'w') as fp:
                        fp.write(python_module)

                    spec = importlib_util.spec_from_file_location(tempdir, module_name)
                    module = importlib_util.module_from_spec(spec)
                    spec.loader.exec_module(module)

                    xml_root = ElementTree.parse(os.path.join(cwd, xml_file)).getroot()
                    bindings = [x for x in filter(lambda x: x.endswith('Binding'), dir(module))]
                    if len(bindings) == 1:
                        class_name = bindings[0]
                    else:
                        class_name = '{}Binding'.format(
                            local_name(xml_root.tag).title().replace('_', ''))

                    binding_class = getattr(module, class_name)
                    xml_data = binding_class.fromsource(os.path.join(cwd, xml_file))
                    self.assertEqual(xml_data.tag, xml_root.tag)
                finally:
                    os.chdir(cwd)

        def test_xml_document_validation(self):
            if not validation_only:
                self.check_decoding_with_element_tree()
                if not inspect and not no_pickle:
                    self.check_schema_serialization()

                if not self.errors:
                    self.check_data_conversion_with_element_tree()

                if lxml_etree is not None:
                    self.check_data_conversion_with_lxml()
                    self.check_with_lxml_iterparse()

            self.check_iter_errors()
            self.check_validate_and_is_valid_api()
            if check_with_lxml and lxml_etree is not None:
                self.check_lxml_validation()

            # Test validation with XML data bindings only for instances and
            # schemas that do not have errors and imports without locations
            if not validation_only and codegen and PythonGenerator is not None and \
                    not self.errors and not self.schema.all_errors and \
                    all('schemaLocation' in e.attrib
                        for e in self.schema.root
                        if e.tag == XSD_IMPORT):

                self.check_validation_with_generated_code()

    TestValidator.__name__ = TestValidator.__qualname__ = f'TestValidator{test_num:03}'
    return TestValidator
