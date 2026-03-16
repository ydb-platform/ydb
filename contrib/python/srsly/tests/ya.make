PY3TEST()

PEERDIR(
    contrib/python/mock
    contrib/python/numpy
    contrib/python/srsly
)

DATA(
    arcadia/contrib/python/srsly/srsly/tests/ujson
)

SRCDIR(contrib/python/srsly)

TEST_SRCS(
    srsly/tests/cloudpickle/cloudpickle_file_test.py
    # srsly/tests/cloudpickle/cloudpickle_test.py
    srsly/tests/cloudpickle/__init__.py
    # srsly/tests/cloudpickle/testutils.py
    srsly/tests/__init__.py
    srsly/tests/msgpack/__init__.py
    srsly/tests/msgpack/test_buffer.py
    srsly/tests/msgpack/test_case.py
    srsly/tests/msgpack/test_except.py
    srsly/tests/msgpack/test_extension.py
    srsly/tests/msgpack/test_format.py
    srsly/tests/msgpack/test_limits.py
    srsly/tests/msgpack/test_memoryview.py
    srsly/tests/msgpack/test_newspec.py
    srsly/tests/msgpack/test_numpy.py
    srsly/tests/msgpack/test_pack.py
    srsly/tests/msgpack/test_read_size.py
    srsly/tests/msgpack/test_seq.py
    srsly/tests/msgpack/test_sequnpack.py
    srsly/tests/msgpack/test_stricttype.py
    srsly/tests/msgpack/test_subtype.py
    srsly/tests/msgpack/test_unpack.py
    srsly/tests/ruamel_yaml/__init__.py
    srsly/tests/ruamel_yaml/roundtrip.py
    # srsly/tests/ruamel_yaml/test_add_xxx.py
    srsly/tests/ruamel_yaml/test_a_dedent.py
    srsly/tests/ruamel_yaml/test_anchor.py
    # srsly/tests/ruamel_yaml/test_api_change.py
    srsly/tests/ruamel_yaml/test_appliance.py
    srsly/tests/ruamel_yaml/test_class_register.py
    srsly/tests/ruamel_yaml/test_collections.py
    srsly/tests/ruamel_yaml/test_comment_manipulation.py
    srsly/tests/ruamel_yaml/test_comments.py
    srsly/tests/ruamel_yaml/test_contextmanager.py
    srsly/tests/ruamel_yaml/test_copy.py
    srsly/tests/ruamel_yaml/test_datetime.py
    srsly/tests/ruamel_yaml/test_deprecation.py
    srsly/tests/ruamel_yaml/test_documents.py
    srsly/tests/ruamel_yaml/test_fail.py
    srsly/tests/ruamel_yaml/test_float.py
    srsly/tests/ruamel_yaml/test_flowsequencekey.py
    srsly/tests/ruamel_yaml/test_indentation.py
    srsly/tests/ruamel_yaml/test_int.py
    # srsly/tests/ruamel_yaml/test_issues.py
    srsly/tests/ruamel_yaml/test_json_numbers.py
    srsly/tests/ruamel_yaml/test_line_col.py
    srsly/tests/ruamel_yaml/test_literal.py
    srsly/tests/ruamel_yaml/test_none.py
    srsly/tests/ruamel_yaml/test_numpy.py
    srsly/tests/ruamel_yaml/test_program_config.py
    srsly/tests/ruamel_yaml/test_spec_examples.py
    srsly/tests/ruamel_yaml/test_string.py
    srsly/tests/ruamel_yaml/test_tag.py
    srsly/tests/ruamel_yaml/test_version.py
    srsly/tests/ruamel_yaml/test_yamlfile.py
    # srsly/tests/ruamel_yaml/test_yamlobject.py
    srsly/tests/ruamel_yaml/test_z_check_debug_leftovers.py
    srsly/tests/ruamel_yaml/test_z_data.py
    srsly/tests/test_json_api.py
    srsly/tests/test_msgpack_api.py
    srsly/tests/test_pickle_api.py
    srsly/tests/test_yaml_api.py
    srsly/tests/ujson/__init__.py
    srsly/tests/ujson/test_ujson.py
    srsly/tests/util.py
)

NO_LINT()

END()
