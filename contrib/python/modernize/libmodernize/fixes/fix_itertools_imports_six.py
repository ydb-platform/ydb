""" Fixer for imports of itertools.(imap|ifilter|izip|ifilterfalse) """

from __future__ import generator_stop

# Local imports
from fissix import fixer_base
from fissix.fixer_util import BlankLine, syms, token

import libmodernize

# This is a derived work of Lib/lib2to3/fixes/fix_itertools_imports.py. That file
# is under the copyright of the Python Software Foundation and licensed
# under the Python Software Foundation License 2.
#
# Copyright notice:
#
#     Copyright (c) 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010,
#     2011, 2012, 2013 Python Software Foundation. All rights reserved.


class FixItertoolsImportsSix(fixer_base.BaseFix):
    BM_compatible = True
    PATTERN = """
              import_from< 'from' 'itertools' 'import' imports=any >
              """ % (
        locals()
    )

    def transform(self, node, results):
        imports = results["imports"]
        if imports.type == syms.import_as_name or not imports.children:
            children = [imports]
        else:
            children = imports.children
        for child in children[::2]:
            if child.type == token.NAME:
                name_node = child
            elif child.type == token.STAR:
                # Just leave the import as is.
                return
            else:
                assert child.type == syms.import_as_name
                name_node = child.children[0]
            member_name = name_node.value
            if member_name in (
                "imap",
                "izip",
                "ifilter",
                "ifilterfalse",
                "izip_longest",
            ):
                child.value = None
                libmodernize.touch_import("six.moves", member_name[1:], node)
                child.remove()

        # Make sure the import statement is still sane
        children = imports.children[:] or [imports]
        remove_comma = True
        for child in children:
            if remove_comma and child.type == token.COMMA:
                child.remove()
            else:
                remove_comma ^= True

        while children and children[-1].type == token.COMMA:
            children.pop().remove()

        # If there are no imports left, just get rid of the entire statement
        if (
            not (imports.children or getattr(imports, "value", None))
            or imports.parent is None
        ):
            p = node.prefix
            node = BlankLine()
            node.prefix = p
            return node
