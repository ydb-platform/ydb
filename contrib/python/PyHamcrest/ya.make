PY23_LIBRARY()

LICENSE(BSD-3-Clause)

OWNER(g:python-contrib)
 
VERSION(1.9.0)

PEERDIR ( 
    contrib/python/six 
) 
 
SRCDIR(
    contrib/python/PyHamcrest/src
)

PY_SRCS(
    TOP_LEVEL

    hamcrest/core/compat.py
    hamcrest/core/assert_that.py
    hamcrest/core/matcher.py
    hamcrest/core/base_matcher.py
    hamcrest/core/selfdescribingvalue.py
    hamcrest/core/string_description.py
    hamcrest/core/core/isnot.py
    hamcrest/core/core/allof.py
    hamcrest/core/core/issame.py
    hamcrest/core/core/anyof.py
    hamcrest/core/core/isanything.py
    hamcrest/core/core/is_.py
    hamcrest/core/core/described_as.py
    hamcrest/core/core/raises.py
    hamcrest/core/core/isequal.py
    hamcrest/core/core/isnone.py
    hamcrest/core/core/isinstanceof.py
    hamcrest/core/core/__init__.py
    hamcrest/core/description.py
    hamcrest/core/selfdescribing.py
    hamcrest/core/base_description.py
    hamcrest/core/helpers/hasmethod.py
    hamcrest/core/helpers/wrap_matcher.py
    hamcrest/core/helpers/__init__.py
    hamcrest/core/__init__.py
    hamcrest/library/integration/match_equality.py
    hamcrest/library/integration/__init__.py
    hamcrest/library/number/ordering_comparison.py
    hamcrest/library/number/iscloseto.py
    hamcrest/library/number/__init__.py
    hamcrest/library/text/substringmatcher.py
    hamcrest/library/text/stringcontainsinorder.py
    hamcrest/library/text/isequal_ignoring_case.py
    hamcrest/library/text/stringstartswith.py
    hamcrest/library/text/stringendswith.py
    hamcrest/library/text/isequal_ignoring_whitespace.py
    hamcrest/library/text/stringcontains.py
    hamcrest/library/text/stringmatches.py
    hamcrest/library/text/__init__.py
    hamcrest/library/object/hasstring.py
    hamcrest/library/object/hasproperty.py
    hamcrest/library/object/haslength.py
    hamcrest/library/object/__init__.py
    hamcrest/library/collection/isdict_containingkey.py
    hamcrest/library/collection/issequence_onlycontaining.py
    hamcrest/library/collection/issequence_containing.py
    hamcrest/library/collection/issequence_containinginorder.py
    hamcrest/library/collection/isdict_containing.py
    hamcrest/library/collection/issequence_containinginanyorder.py
    hamcrest/library/collection/isin.py
    hamcrest/library/collection/isdict_containingvalue.py
    hamcrest/library/collection/is_empty.py
    hamcrest/library/collection/isdict_containingentries.py
    hamcrest/library/collection/__init__.py
    hamcrest/library/__init__.py
    hamcrest/__init__.py
)

NO_LINT()

END() 

RECURSE_FOR_TESTS(
    tests
)
