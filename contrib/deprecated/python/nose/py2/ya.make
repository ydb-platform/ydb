PY2_LIBRARY()

LICENSE(LGPL-2.1-or-later)

VERSION(1.3.7)

NO_LINT()

RESOURCE(nose/usage.txt nose/usage.txt)

PEERDIR(
    contrib/python/six
    library/python/resource
)

PY_SRCS(
    TOP_LEVEL
    nose/__init__.py
    nose/case.py
    nose/commands.py
    nose/config.py
    nose/core.py
    nose/exc.py
    nose/ext/__init__.py
    nose/ext/dtcompat.py
    nose/failure.py
    nose/importer.py
    nose/inspector.py
    nose/loader.py
    nose/plugins/__init__.py
    nose/plugins/allmodules.py
    nose/plugins/attrib.py
    nose/plugins/base.py
    nose/plugins/builtin.py
    nose/plugins/capture.py
    nose/plugins/collect.py
    nose/plugins/cover.py
    nose/plugins/debug.py
    nose/plugins/deprecated.py
    nose/plugins/doctests.py
    nose/plugins/errorclass.py
    nose/plugins/failuredetail.py
    nose/plugins/isolate.py
    nose/plugins/logcapture.py
    nose/plugins/manager.py
    nose/plugins/multiprocess.py
    nose/plugins/plugintest.py
    nose/plugins/prof.py
    nose/plugins/skip.py
    nose/plugins/testid.py
    nose/plugins/xunit.py
    nose/proxy.py
    nose/pyversion.py
    nose/result.py
    nose/selector.py
    nose/sphinx/__init__.py
    nose/sphinx/pluginopts.py
    nose/suite.py
    nose/tools/__init__.py
    nose/tools/nontrivial.py
    nose/tools/trivial.py
    nose/twistedtools.py
    nose/util.py
)

END()
