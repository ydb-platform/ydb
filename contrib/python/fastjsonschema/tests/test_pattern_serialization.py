import re

from fastjsonschema.generator import serialize_regexes


# Examples
ENTRYPOINT_PATTERN = r"[^\[\s=]([^=]*[^\s=])?"
PEP508_IDENTIFIER_PATTERN = r"([A-Z0-9]|[A-Z0-9][A-Z0-9._-]*[A-Z0-9])"
PEP440_VERSION = r"""
    v?
    (?:
        (?:(?P<epoch>[0-9]+)!)?                           # epoch
        (?P<release>[0-9]+(?:\.[0-9]+)*)                  # release segment
        (?P<pre>                                          # pre-release
            [-_\.]?
            (?P<pre_l>(a|b|c|rc|alpha|beta|pre|preview))
            [-_\.]?
            (?P<pre_n>[0-9]+)?
        )?
        (?P<post>                                         # post release
            (?:-(?P<post_n1>[0-9]+))
            |
            (?:
                [-_\.]?
                (?P<post_l>post|rev|r)
                [-_\.]?
                (?P<post_n2>[0-9]+)?
            )
        )?
        (?P<dev>                                          # dev release
            [-_\.]?
            (?P<dev_l>dev)
            [-_\.]?
            (?P<dev_n>[0-9]+)?
        )?
    )
    (?:\+(?P<local>[a-z0-9]+(?:[-_\.][a-z0-9]+)*))?       # local version
"""

EXAMPLES = {
    "unicode-identifier": re.compile(r"^(\w&[^0-9])\w*$", re.I),
    "entry-point": re.compile(f"^{ENTRYPOINT_PATTERN}$", re.I),
    "pep508-identifier": re.compile(f"^{PEP508_IDENTIFIER_PATTERN}$", re.I),
    # Regression tests:
    "issue-109": re.compile(r"^[ \r\n\t\S]+$"),
    # Some long regexes that would likely break with pprint:
    "pep-440": re.compile(r"^\s*" + PEP440_VERSION + r"\s*$", re.X | re.I)
}


def test_serialize_regexes():
    serialized = serialize_regexes(EXAMPLES)
    reconstructed = eval(serialized)
    for key, value in EXAMPLES.items():
        assert key in reconstructed
        evaluated = reconstructed[key]
        assert value.pattern == evaluated.pattern
        assert value.flags == evaluated.flags
        assert value == evaluated
