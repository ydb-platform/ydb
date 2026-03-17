import copy
import re
import types

from .ucre import build_re

# py>=37: re.Pattern, else: _sre.SRE_Pattern
RE_TYPE = type(re.compile(r""))


def _escape_re(string):
    return re.sub(r"([.?*+^$[\]\\(){}|-])", r"\\\1", string)


def _index_of(text, search_value):
    try:
        result = text.index(search_value)
    except ValueError:
        result = -1

    return result


class SchemaError(Exception):
    """Linkify schema error"""

    def __init__(self, name, val):
        message = "(LinkifyIt) Invalid schema '{}': '{}'".format(name, val)
        super().__init__(message)


class Match:
    """Match result.

    Attributes:
        schema (str): Prefix (protocol) for matched string.
        index (int): First position of matched string.
        last_index (int): Next position after matched string.
        raw (str): Matched string.
        text (str): Notmalized text of matched string.
        url (str): Normalized url of matched string.

    Args:
        linkifyit (:class:`linkify_it.main.LinkifyIt`) LinkifyIt object
        shift (int): text searh position
    """

    def __repr__(self):
        return "{}.{}({!r})".format(
            self.__class__.__module__, self.__class__.__name__, self.__dict__
        )

    def __init__(self, linkifyit, shift):
        start = linkifyit._index
        end = linkifyit._last_index
        text = linkifyit._text_cache[start:end]

        self.schema = linkifyit._schema.lower()
        self.index = start + shift
        self.last_index = end + shift
        self.raw = text
        self.text = text
        self.url = text


class LinkifyIt:
    """Creates new linkifier instance with optional additional schemas.

    By default understands:

    - ``http(s)://...`` , ``ftp://...``, ``mailto:...`` & ``//...`` links
    - "fuzzy" links and emails (example.com, foo@bar.com).

    ``schemas`` is an dict where each key/value describes protocol/rule:

    - **key** - link prefix (usually, protocol name with ``:`` at the end, ``skype:``
      for example). `linkify-it` makes shure that prefix is not preceeded with
      alphanumeric char. Only whitespaces and punctuation allowed.

    - **value** - rule to check tail after link prefix

      - *str* - just alias to existing rule
      - *dict*

        - *validate* - either a ``re.Pattern``, ``re str`` (start with ``^``, and don't
          include the link prefix itself), or a validator ``function`` which, given
          arguments *self*, *text* and *pos* returns the length of a match in *text*
          starting at index *pos*. *pos* is the index right after the link prefix.
        - *normalize* - optional function to normalize text & url of matched
          result (for example, for @twitter mentions).

    ``options`` is an dict:

    - **fuzzyLink** - recognige URL-s without ``http(s):`` prefix. Default ``True``.
    - **fuzzyIP** - allow IPs in fuzzy links above. Can conflict with some texts
      like version numbers. Default ``False``.
    - **fuzzyEmail** - recognize emails without ``mailto:`` prefix.
    - **---** - set `True` to terminate link with `---` (if it's considered as long
      dash).

    Args:
        schemas (dict): Optional. Additional schemas to validate (prefix/validator)
        options (dict): { fuzzy_link | fuzzy_email | fuzzy_ip: True | False }.
            Default: {"fuzzy_link": True, "fuzzy_email": True, "fuzzy_ip": False}.
    """

    def _validate_http(self, text, pos):
        tail = text[pos:]
        if not self.re.get("http"):
            # compile lazily, because "host"-containing variables can change on
            # tlds update.
            self.re["http"] = (
                "^\\/\\/"
                + self.re["src_auth"]
                + self.re["src_host_port_strict"]
                + self.re["src_path"]
            )

        founds = re.search(self.re["http"], tail, flags=re.IGNORECASE)
        if founds:
            return len(founds.group())

        return 0

    def _validate_double_slash(self, text, pos):
        tail = text[pos:]

        if not self.re.get("not_http"):
            # compile lazily, because "host"-containing variables can change on
            # tlds update.
            self.re["not_http"] = (
                "^"
                + self.re["src_auth"]
                + "(?:localhost|(?:(?:"
                + self.re["src_domain"]
                + ")\\.)+"
                + self.re["src_domain_root"]
                + ")"
                + self.re["src_port"]
                + self.re["src_host_terminator"]
                + self.re["src_path"]
            )

        founds = re.search(self.re["not_http"], tail, flags=re.IGNORECASE)
        if founds:
            if pos >= 3 and text[pos - 3] == ":":
                return 0

            if pos >= 3 and text[pos - 3] == "/":
                return 0

            return len(founds.group(0))

        return 0

    def _validate_mailto(self, text, pos):
        tail = text[pos:]

        if not self.re.get("mailto"):
            self.re["mailto"] = (
                "^" + self.re["src_email_name"] + "@" + self.re["src_host_strict"]
            )

        founds = re.search(self.re["mailto"], tail, flags=re.IGNORECASE)
        if founds:
            return len(founds.group(0))

        return 0

    def _reset_scan_cache(self):
        self._index = -1
        self._text_cache = ""

    def _create_validator(self, regex):
        def func(text, pos):
            tail = text[pos:]
            if isinstance(regex, str):
                founds = re.search(regex, tail, flags=re.IGNORECASE)
            else:
                # re.Pattern
                founds = re.search(regex, tail)

            if founds:
                return len(founds.group(0))

            return 0

        return func

    def _create_normalizer(self):
        def func(match):
            self.normalize(match)

        return func

    def _create_match(self, shift):
        match = Match(self, shift)
        self._compiled[match.schema]["normalize"](match)
        return match

    def __init__(self, schemas=None, options=None):
        self.default_options = {
            "fuzzy_link": True,
            "fuzzy_email": True,
            "fuzzy_ip": False,
        }

        self.default_schemas = {
            "http:": {"validate": self._validate_http},
            "https:": "http:",
            "ftp:": "http:",
            "//": {"validate": self._validate_double_slash},
            "mailto:": {"validate": self._validate_mailto},
        }

        # RE pattern for 2-character tlds (autogenerated by ./support/tlds_2char_gen.js)
        self.tlds_2ch_src_re = "a[cdefgilmnoqrstuwxz]|b[abdefghijmnorstvwyz]|c[acdfghiklmnoruvwxyz]|d[ejkmoz]|e[cegrstu]|f[ijkmor]|g[abdefghilmnpqrstuwy]|h[kmnrtu]|i[delmnoqrst]|j[emop]|k[eghimnprwyz]|l[abcikrstuvy]|m[acdeghklmnopqrstuvwxyz]|n[acefgilopruz]|om|p[aefghklmnrstwy]|qa|r[eosuw]|s[abcdeghijklmnortuvxyz]|t[cdfghjklmnortvwz]|u[agksyz]|v[aceginu]|w[fs]|y[et]|z[amw]"  # noqa: E501

        # DON'T try to make PRs with changes. Extend TLDs with LinkifyIt.tlds() instead
        self.tlds_default = "biz|com|edu|gov|net|org|pro|web|xxx|aero|asia|coop|info|museum|name|shop|рф".split(  # noqa: E501
            "|"
        )

        if options:
            self.default_options.update(options)
            self._opts = self.default_options
        else:
            self._opts = self.default_options

        # Cache last tested result. Used to skip repeating steps on next `match` call.
        self._index = -1
        self._last_index = -1  # Next scan position
        self._schema = ""
        self._text_cache = ""

        if schemas:
            self.default_schemas.update(schemas)
            self._schemas = self.default_schemas
        else:
            self._schemas = self.default_schemas

        self._compiled = {}

        self._tlds = self.tlds_default
        self._tlds_replaced = False

        self.re = {}

        self._compile()

    def _compile(self):
        """Schemas compiler. Build regexps."""

        # Load & clone RE patterns.
        self.re = build_re(self._opts)

        # Define dynamic patterns
        tlds = copy.deepcopy(self._tlds)

        self._on_compile()

        if not self._tlds_replaced:
            tlds.append(self.tlds_2ch_src_re)
        tlds.append(self.re["src_xn"])

        self.re["src_tlds"] = "|".join(tlds)

        def untpl(tpl):
            return tpl.replace("%TLDS%", self.re["src_tlds"])

        self.re["email_fuzzy"] = untpl(self.re["tpl_email_fuzzy"])

        self.re["link_fuzzy"] = untpl(self.re["tpl_link_fuzzy"])

        self.re["link_no_ip_fuzzy"] = untpl(self.re["tpl_link_no_ip_fuzzy"])

        self.re["host_fuzzy_test"] = untpl(self.re["tpl_host_fuzzy_test"])

        #
        # Compile each schema
        #

        aliases = []

        self._compiled = {}

        for name, val in self._schemas.items():
            # skip disabled methods
            if val is None:
                continue

            compiled = {"validate": None, "link": None}

            self._compiled[name] = compiled

            if isinstance(val, dict):
                if isinstance(val.get("validate"), RE_TYPE):
                    compiled["validate"] = self._create_validator(val.get("validate"))
                elif isinstance(val.get("validate"), str):
                    compiled["validate"] = self._create_validator(val.get("validate"))
                elif isinstance(val.get("validate"), types.MethodType):
                    compiled["validate"] = val.get("validate")
                # Add custom handler
                elif isinstance(val.get("validate"), types.FunctionType):
                    setattr(LinkifyIt, "func", val.get("validate"))
                    compiled["validate"] = self.func
                else:
                    raise SchemaError(name, val)

                if isinstance(val.get("normalize"), types.MethodType):
                    compiled["normalize"] = val.get("normalize")
                # Add custom handler
                elif isinstance(val.get("normalize"), types.FunctionType):
                    setattr(LinkifyIt, "func", val.get("normalize"))
                    compiled["normalize"] = self.func
                elif not val.get("normalize"):
                    compiled["normalize"] = self._create_normalizer()
                else:
                    raise SchemaError(name, val)

                continue

            if isinstance(val, str):
                aliases.append(name)
                continue

            raise SchemaError(name, val)

        #
        # Compile postponed aliases
        #
        for alias in aliases:
            if not self._compiled.get(self._schemas.get(alias)):
                continue

            self._compiled[alias]["validate"] = self._compiled[self._schemas[alias]][
                "validate"
            ]
            self._compiled[alias]["normalize"] = self._compiled[self._schemas[alias]][
                "normalize"
            ]

        # Fake record for guessed links
        self._compiled[""] = {"validate": None, "normalize": self._create_normalizer()}

        #
        # Build schema condition
        #
        slist = "|".join(
            [
                _escape_re(name)
                for name, val in self._compiled.items()
                if len(name) > 0 and val
            ]
        )

        re_schema_test = (
            "(^|(?!_)(?:[><\uff5c]|" + self.re["src_ZPCc"] + "))(" + slist + ")"
        )

        # (?!_) cause 1.5x slowdown
        self.re["schema_test"] = re_schema_test
        self.re["schema_search"] = re_schema_test
        self.re["schema_at_start"] = "^" + self.re["schema_search"]

        self.re["pretest"] = (
            "(" + re_schema_test + ")|(" + self.re["host_fuzzy_test"] + ")|@"
        )

        # Cleanup

        self._reset_scan_cache()

    def add(self, schema, definition):
        """Add new rule definition. (chainable)

        See :class:`linkify_it.main.LinkifyIt` init description for details.
        ``schema`` is a link prefix (``skype:``, for example), and ``definition``
        is a ``str`` to alias to another schema, or an ``dict`` with ``validate`` and
        optionally `normalize` definitions. To disable an existing rule, use
        ``.add(<schema>, None)``.

        Args:
            schema (str): rule name (fixed pattern prefix)
            definition (`str` or `re.Pattern`): schema definition

        Return:
            :class:`linkify_it.main.LinkifyIt`
        """
        self._schemas[schema] = definition
        self._compile()
        return self

    def set(self, options):
        """Override default options. (chainable)

        Missed properties will not be changed.

        Args:
            options (dict): ``keys``: [``fuzzy_link`` | ``fuzzy_email`` | ``fuzzy_ip``].
                ``values``: [``True`` | ``False``]

        Return:
            :class:`linkify_it.main.LinkifyIt`
        """
        self._opts.update(options)
        return self

    def test(self, text):
        """Searches linkifiable pattern and returns ``True`` on success or ``False``
        on fail.

        Args:
            text (str): text to search

        Returns:
            bool: ``True`` if a linkable pattern was found, otherwise it is ``False``.
        """
        self._text_cache = text
        self._index = -1

        if not len(text):
            return False

        if re.search(self.re["schema_test"], text, flags=re.IGNORECASE):
            regex = self.re["schema_search"]
            last_index = 0
            matched_iter = re.finditer(regex, text[last_index:], flags=re.IGNORECASE)
            for matched in matched_iter:
                last_index = matched.end(0)
                m = (matched.group(), matched.groups()[0], matched.groups()[1])
                length = self.test_schema_at(text, m[2], last_index)
                if length:
                    self._schema = m[2]
                    self._index = matched.start(0) + len(m[1])
                    self._last_index = matched.start(0) + len(m[0]) + length
                    break

        if self._opts.get("fuzzy_link") and self._compiled.get("http:"):
            # guess schemaless links
            matched_tld = re.search(
                self.re["host_fuzzy_test"], text, flags=re.IGNORECASE
            )
            if matched_tld:
                tld_pos = matched_tld.start(0)
            else:
                tld_pos = -1
            if tld_pos >= 0:
                # if tld is located after found link - no need to check fuzzy pattern
                if self._index < 0 or tld_pos < self._index:
                    if self._opts.get("fuzzy_ip"):
                        pattern = self.re["link_fuzzy"]
                    else:
                        pattern = self.re["link_no_ip_fuzzy"]

                    ml = re.search(pattern, text, flags=re.IGNORECASE)
                    if ml:
                        shift = ml.start(0) + len(ml.groups()[0])

                        if self._index < 0 or shift < self._index:
                            self._schema = ""
                            self._index = shift
                            self._last_index = ml.start(0) + len(ml.group())

        if self._opts.get("fuzzy_email") and self._compiled.get("mailto:"):
            # guess schemaless emails
            at_pos = _index_of(text, "@")
            if at_pos >= 0:
                # We can't skip this check, because this cases are possible:
                # 192.168.1.1@gmail.com, my.in@example.com
                me = re.search(self.re["email_fuzzy"], text, flags=re.IGNORECASE)
                if me:
                    shift = me.start(0) + len(me.groups()[0])
                    next_shift = me.start(0) + len(me.group())

                    if (
                        self._index < 0
                        or shift < self._index
                        or (shift == self._index and next_shift > self._last_index)
                    ):
                        self._schema = "mailto:"
                        self._index = shift
                        self._last_index = next_shift

        return self._index >= 0

    def pretest(self, text):
        """Very quick check, that can give false positives.

        Returns true if link MAY BE can exists. Can be used for speed optimization,
        when you need to check that link NOT exists.

        Args:
            text (str): text to search

        Returns:
            bool: ``True`` if a linkable pattern was found, otherwise it is ``False``.
        """
        if re.search(self.re["pretest"], text, flags=re.IGNORECASE):
            return True

        return False

    def test_schema_at(self, text, name, position):
        """Similar to :meth:`linkify_it.main.LinkifyIt.test` but checks only
        specific protocol tail exactly at given position.

        Args:
            text (str): text to scan
            name (str): rule (schema) name
            position (int): length of found pattern (0 on fail).

        Returns:
            int: text (str): text to search
        """
        # If not supported schema check requested - terminate
        if not self._compiled.get(name.lower()):
            return 0
        return self._compiled.get(name.lower()).get("validate")(text, position)

    def match(self, text):
        """Returns ``list`` of found link descriptions or ``None`` on fail.

        We strongly recommend to use :meth:`linkify_it.main.LinkifyIt.test`
        first, for best speed.

        Args:
            text (str): text to search

        Returns:
            ``list`` or ``None``: Result match description:
                * **schema** - link schema, can be empty for fuzzy links, or ``//``
                  for protocol-neutral  links.
                * **index** - offset of matched text
                * **last_index** - offset of matched text
                * **raw** - offset of matched text
                * **text** - normalized text
                * **url** - link, generated from matched text
        """
        shift = 0
        result = []

        # try to take previous element from cache, if .test() called before
        if self._index >= 0 and self._text_cache == text:
            result.append(self._create_match(shift))
            shift = self._last_index

        # Cut head if cache was used
        tail = text[shift:] if shift else text

        # Scan string until end reached
        while self.test(tail):
            result.append(self._create_match(shift))

            tail = tail[self._last_index :]
            shift += self._last_index

        if len(result):
            return result

        return None

    def match_at_start(self, text):
        """Returns fully-formed (not fuzzy) link if it starts at the beginning
        of the string, and null otherwise.

        Args:
            text (str): text to search

        Retuns:
            ``Match`` or ``None``
        """
        # Reset scan cache
        self._text_cache = text
        self._index = -1

        if not len(text):
            return None

        founds = re.search(self.re["schema_at_start"], text, flags=re.IGNORECASE)
        if not founds:
            return None

        m = (founds.group(), founds.groups()[0], founds.groups()[1])
        length = self.test_schema_at(text, m[2], len(m[0]))
        if not length:
            return None

        self._schema = m[2]
        self._index = founds.start(0) + len(m[1])
        self._last_index = founds.start(0) + len(m[0]) + length

        return self._create_match(0)

    def tlds(self, list_tlds, keep_old=False):
        """Load (or merge) new tlds list. (chainable)

        Those are user for fuzzy links (without prefix) to avoid false positives.
        By default this algorythm used:

        * hostname with any 2-letter root zones are ok.
        * biz|com|edu|gov|net|org|pro|web|xxx|aero|asia|coop|info|museum|name|shop|рф
          are ok.
        * encoded (`xn--...`) root zones are ok.

        If list is replaced, then exact match for 2-chars root zones will be checked.

        Args:
            list_tlds (list or str): ``list of tlds`` or ``tlds string``
            keep_old (bool): merge with current list if q`True`q (q`Falseq` by default)
        """
        _list = list_tlds if isinstance(list_tlds, list) else [list_tlds]

        if not keep_old:
            self._tlds = _list
            self._tlds_replaced = True
            self._compile()
            return self

        self._tlds.extend(_list)
        self._tlds = sorted(list(set(self._tlds)), reverse=True)

        self._compile()
        return self

    def normalize(self, match):
        """Default normalizer (if schema does not define it's own).

        Args:
            match (:class:`linkify_it.main.Match`): Match result
        """
        if not match.schema:
            match.url = "http://" + match.url

        if match.schema == "mailto:" and not re.search(
            "^mailto:", match.url, flags=re.IGNORECASE
        ):
            match.url = "mailto:" + match.url

    def _on_compile(self):
        """Override to modify basic RegExp-s."""
        pass
