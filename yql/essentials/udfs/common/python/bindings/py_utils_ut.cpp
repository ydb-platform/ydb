#include "py_utils.h"

#include <library/cpp/testing/unittest/registar.h>


using namespace NPython;

Y_UNIT_TEST_SUITE(TPyUtilsTest) {

    Y_UNIT_TEST(EncodingCookie) {
        UNIT_ASSERT(HasEncodingCookie("# -*- coding: latin-1 -*-"));
        UNIT_ASSERT(HasEncodingCookie("# -*- coding:latin-1 -*-"));
        UNIT_ASSERT(HasEncodingCookie("# -*- coding=latin-1 -*-"));
        UNIT_ASSERT(HasEncodingCookie("# -*- encoding: latin-1 -*-"));
        UNIT_ASSERT(HasEncodingCookie("# -*- encoding:latin-1 -*-"));
        UNIT_ASSERT(HasEncodingCookie("# -*- encoding=latin-1 -*-"));
        UNIT_ASSERT(HasEncodingCookie("# -*- coding: iso-8859-15 -*-"));
        UNIT_ASSERT(HasEncodingCookie("# -*- coding: ascii -*-"));
        UNIT_ASSERT(HasEncodingCookie(
                "# This Python file uses the following encoding: utf-8"));

        // encoding commend on second line
        UNIT_ASSERT(HasEncodingCookie(
                "#!/usr/local/bin/python\n"
                "# -*- coding: iso-8859-15 -*-\n"
                "print 'hello'"));

        // missing "coding:" prefix
        UNIT_ASSERT(false == HasEncodingCookie("# latin-1"));

        // encoding comment not on line 1 or 2
        UNIT_ASSERT(false == HasEncodingCookie(
                "#!/usr/local/bin/python\n"
                "#\n"
                "# -*- coding: latin-1 -*-\n"));
    }
}
