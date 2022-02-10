LIBRARY(unit_test_framework)

WITHOUT_LICENSE_TEXTS()

LICENSE(BSL-1.0)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc) 

SRCDIR(${BOOST_ROOT}/libs/test/src)

SRCS(
    compiler_log_formatter.cpp
    debug.cpp
    decorator.cpp
    execution_monitor.cpp
    framework.cpp
    plain_report_formatter.cpp
    progress_monitor.cpp
    results_collector.cpp
    results_reporter.cpp
    test_framework_init_observer.cpp
    test_tools.cpp
    test_tree.cpp
    unit_test_log.cpp
    unit_test_main.cpp
    unit_test_monitor.cpp
    unit_test_parameters.cpp
    junit_log_formatter.cpp
    xml_log_formatter.cpp
    xml_report_formatter.cpp
)

END()
