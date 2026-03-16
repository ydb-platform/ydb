PY3TEST()

PEERDIR(
    contrib/python/chardet
    contrib/python/cssutils
    contrib/python/mock
    contrib/python/jaraco.test
)

SRCDIR(
    contrib/python/cssutils/cssutils/tests
)

TEST_SRCS(
    __init__.py
    basetest.py
    conftest.py
    test_codec.py
    test_csscharsetrule.py
    test_csscomment.py
    test_cssfontfacerule.py
    test_cssimportrule.py
    test_cssmediarule.py
    test_cssnamespacerule.py
    test_csspagerule.py
    test_cssproperties.py
    test_cssrule.py
    test_cssrulelist.py
    test_cssstyledeclaration.py
    test_cssstylerule.py
    test_cssstylesheet.py
    test_cssunknownrule.py
    test_cssutils.py
    test_cssutilsimport.py
    test_cssvalue.py
    test_cssvariablesdeclaration.py
    test_cssvariablesrule.py
    test_domimplementation.py
    test_encutils.py
    test_errorhandler.py
    test_helper.py
    test_marginrule.py
    test_medialist.py
    test_mediaquery.py
    test_parse.py
    test_prodparser.py
    test_profiles.py
    test_properties.py
    test_property.py
    test_scripts_csscombine.py
    test_selector.py
    test_selectorlist.py
    test_serialize.py
    test_settings.py
    test_stylesheet.py
    test_tokenize2.py
    test_util.py
    test_value.py
    test_x.py
)

DATA(
    arcadia/contrib/python/cssutils
)

NO_LINT()

END()
