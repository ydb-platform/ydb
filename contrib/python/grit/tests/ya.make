PY3TEST()

PEERDIR(
    contrib/python/grit
    contrib/python/six
)

NO_LINT()
NO_CHECK_IMPORTS()

DATA(
    arcadia/contrib/python/grit/grit/testdata
)

TEST_SRCS(
    contrib/python/grit/grit/format/android_xml_unittest.py
    contrib/python/grit/grit/format/c_format_unittest.py
    contrib/python/grit/grit/format/chrome_messages_json_unittest.py
    contrib/python/grit/grit/format/data_pack_unittest.py
    contrib/python/grit/grit/format/gen_predetermined_ids_unittest.py
    contrib/python/grit/grit/format/gzip_string_unittest.py
    contrib/python/grit/grit/format/html_inline_unittest.py
    contrib/python/grit/grit/format/policy_templates_json_unittest.py
    contrib/python/grit/grit/format/rc_header_unittest.py
    contrib/python/grit/grit/gather/admin_template_unittest.py
    contrib/python/grit/grit/gather/chrome_html_unittest.py
    contrib/python/grit/grit/gather/chrome_scaled_image_unittest.py
    contrib/python/grit/grit/gather/policy_json_unittest.py
    contrib/python/grit/grit/gather/tr_html_unittest.py
    contrib/python/grit/grit/gather/txt_unittest.py
    contrib/python/grit/grit/grd_reader_unittest.py
    contrib/python/grit/grit/grit_runner_unittest.py
    contrib/python/grit/grit/lazy_re_unittest.py
    contrib/python/grit/grit/node/base_unittest.py
    contrib/python/grit/grit/node/include_unittest.py
    contrib/python/grit/grit/node/message_unittest.py
    contrib/python/grit/grit/node/node_io_unittest.py
    contrib/python/grit/grit/pseudo_unittest.py
    contrib/python/grit/grit/shortcuts_unittest.py
    contrib/python/grit/grit/tclib_unittest.py
    contrib/python/grit/grit/tool/android2grd_unittest.py
    contrib/python/grit/grit/tool/build_unittest.py
    contrib/python/grit/grit/tool/buildinfo_unittest.py
    contrib/python/grit/grit/tool/diff_structures_unittest.py
    contrib/python/grit/grit/tool/newgrd_unittest.py
    contrib/python/grit/grit/tool/postprocess_unittest.py
    contrib/python/grit/grit/tool/preprocess_unittest.py
    contrib/python/grit/grit/tool/rc2grd_unittest.py
    contrib/python/grit/grit/util_unittest.py
    contrib/python/grit/grit/xtb_reader_unittest.py
)

END()
