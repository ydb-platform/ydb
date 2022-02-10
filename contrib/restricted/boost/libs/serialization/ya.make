LIBRARY()

LICENSE(BSL-1.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc)

SRCS(
    src/archive_exception.cpp
    src/basic_archive.cpp
    src/basic_iarchive.cpp
    src/basic_iserializer.cpp
    src/basic_oarchive.cpp
    src/basic_oserializer.cpp
    src/basic_pointer_iserializer.cpp
    src/basic_pointer_oserializer.cpp
    src/basic_serializer_map.cpp
    src/basic_text_iprimitive.cpp
    src/basic_text_oprimitive.cpp
    src/basic_text_wiprimitive.cpp
    src/basic_text_woprimitive.cpp
    src/basic_xml_archive.cpp
    src/binary_iarchive.cpp
    src/binary_oarchive.cpp
    src/codecvt_null.cpp
    src/extended_type_info.cpp
    src/extended_type_info_no_rtti.cpp
    src/extended_type_info_typeid.cpp
    src/polymorphic_iarchive.cpp
    src/polymorphic_oarchive.cpp
    src/stl_port.cpp
    src/text_iarchive.cpp
    src/text_oarchive.cpp
    src/text_wiarchive.cpp
    src/text_woarchive.cpp
    src/utf8_codecvt_facet.cpp
    src/void_cast.cpp
    src/xml_archive_exception.cpp
    src/xml_grammar.cpp
    src/xml_iarchive.cpp
    src/xml_oarchive.cpp
    src/xml_wgrammar.cpp
    src/xml_wiarchive.cpp
    src/xml_woarchive.cpp
)

END()
