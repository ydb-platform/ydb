from devtools.yamaker.project import NixProject

apache_serf = NixProject(
    arcdir="contrib/libs/apache/serf",
    nixattr="serf",
    disable_includes=["zutil.h"],
)
