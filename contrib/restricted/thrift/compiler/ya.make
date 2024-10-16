PROGRAM()

VERSION(0.10.0)

LICENSE(
    Apache-2.0 AND
    GPL-3.0-or-later WITH Bison-exception-2.2
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

SUBSCRIBER(g:contrib)

PEERDIR(
    contrib/restricted/thrift
)

ADDINCL(
    contrib/restricted/thrift/compiler
)

NO_UTIL()

NO_COMPILER_WARNINGS()

SRCS(
    thrift/audit/t_audit.cpp
    thrift/common.cc
    thrift/generate/t_as3_generator.cc
    #thrift/generate/t_c_glib_generator.cc
    thrift/generate/t_cl_generator.cc
    thrift/generate/t_cocoa_generator.cc
    thrift/generate/t_cpp_generator.cc
    thrift/generate/t_csharp_generator.cc
    thrift/generate/t_d_generator.cc
    thrift/generate/t_dart_generator.cc
    thrift/generate/t_delphi_generator.cc
    thrift/generate/t_erl_generator.cc
    thrift/generate/t_generator.cc
    thrift/generate/t_go_generator.cc
    thrift/generate/t_gv_generator.cc
    thrift/generate/t_haxe_generator.cc
    thrift/generate/t_hs_generator.cc
    thrift/generate/t_html_generator.cc
    thrift/generate/t_java_generator.cc
    thrift/generate/t_javame_generator.cc
    thrift/generate/t_js_generator.cc
    thrift/generate/t_json_generator.cc
    thrift/generate/t_lua_generator.cc
    #thrift/generate/t_netcore_generator.cc
    thrift/generate/t_ocaml_generator.cc
    thrift/generate/t_perl_generator.cc
    thrift/generate/t_php_generator.cc
    thrift/generate/t_py_generator.cc
    thrift/generate/t_rb_generator.cc
    thrift/generate/t_rs_generator.cc
    thrift/generate/t_st_generator.cc
    thrift/generate/t_swift_generator.cc
    thrift/generate/t_xml_generator.cc
    thrift/generate/t_xsd_generator.cc
    #thrift/logging.cc
    thrift/main.cc
    thrift/parse/parse.cc
    thrift/parse/t_typedef.cc
    #thrift/plugin/plugin.cc
    #thrift/plugin/plugin_output.cc
    thrift/thriftl.cc
    thrift/thrifty.cc
)

CXXFLAGS(-Wno-unused-variable)

END()
