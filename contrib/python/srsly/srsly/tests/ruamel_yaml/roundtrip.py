"""
helper routines for testing round trip of commented YAML data
"""
import sys
import textwrap
from pathlib import Path

enforce = object()


def dedent(data):
    try:
        position_of_first_newline = data.index("\n")
        for idx in range(position_of_first_newline):
            if not data[idx].isspace():
                raise ValueError
    except ValueError:
        pass
    else:
        data = data[position_of_first_newline + 1 :]
    return textwrap.dedent(data)


def round_trip_load(inp, preserve_quotes=None, version=None):
    import srsly.ruamel_yaml  # NOQA

    dinp = dedent(inp)
    return srsly.ruamel_yaml.load(
        dinp,
        Loader=srsly.ruamel_yaml.RoundTripLoader,
        preserve_quotes=preserve_quotes,
        version=version,
    )


def round_trip_load_all(inp, preserve_quotes=None, version=None):
    import srsly.ruamel_yaml  # NOQA

    dinp = dedent(inp)
    return srsly.ruamel_yaml.load_all(
        dinp,
        Loader=srsly.ruamel_yaml.RoundTripLoader,
        preserve_quotes=preserve_quotes,
        version=version,
    )


def round_trip_dump(
    data,
    stream=None,
    indent=None,
    block_seq_indent=None,
    top_level_colon_align=None,
    prefix_colon=None,
    explicit_start=None,
    explicit_end=None,
    version=None,
):
    import srsly.ruamel_yaml  # NOQA

    return srsly.ruamel_yaml.round_trip_dump(
        data,
        stream=stream,
        indent=indent,
        block_seq_indent=block_seq_indent,
        top_level_colon_align=top_level_colon_align,
        prefix_colon=prefix_colon,
        explicit_start=explicit_start,
        explicit_end=explicit_end,
        version=version,
    )


def diff(inp, outp, file_name="stdin"):
    import difflib

    inl = inp.splitlines(True)  # True for keepends
    outl = outp.splitlines(True)
    diff = difflib.unified_diff(inl, outl, file_name, "round trip YAML")
    # 2.6 difflib has trailing space on filename lines %-)
    strip_trailing_space = sys.version_info < (2, 7)
    for line in diff:
        if strip_trailing_space and line[:4] in ["--- ", "+++ "]:
            line = line.rstrip() + "\n"
        sys.stdout.write(line)


def round_trip(
    inp,
    outp=None,
    extra=None,
    intermediate=None,
    indent=None,
    block_seq_indent=None,
    top_level_colon_align=None,
    prefix_colon=None,
    preserve_quotes=None,
    explicit_start=None,
    explicit_end=None,
    version=None,
    dump_data=None,
):
    """
    inp:    input string to parse
    outp:   expected output (equals input if not specified)
    """
    if outp is None:
        outp = inp
    doutp = dedent(outp)
    if extra is not None:
        doutp += extra
    data = round_trip_load(inp, preserve_quotes=preserve_quotes)
    if dump_data:
        print("data", data)
    if intermediate is not None:
        if isinstance(intermediate, dict):
            for k, v in intermediate.items():
                if data[k] != v:
                    print("{0!r} <> {1!r}".format(data[k], v))
                    raise ValueError
    res = round_trip_dump(
        data,
        indent=indent,
        block_seq_indent=block_seq_indent,
        top_level_colon_align=top_level_colon_align,
        prefix_colon=prefix_colon,
        explicit_start=explicit_start,
        explicit_end=explicit_end,
        version=version,
    )
    if res != doutp:
        diff(doutp, res, "input string")
    print("\nroundtrip data:\n", res, sep="")
    assert res == doutp
    res = round_trip_dump(
        data,
        indent=indent,
        block_seq_indent=block_seq_indent,
        top_level_colon_align=top_level_colon_align,
        prefix_colon=prefix_colon,
        explicit_start=explicit_start,
        explicit_end=explicit_end,
        version=version,
    )
    print("roundtrip second round data:\n", res, sep="")
    assert res == doutp
    return data


def na_round_trip(
    inp,
    outp=None,
    extra=None,
    intermediate=None,
    indent=None,
    top_level_colon_align=None,
    prefix_colon=None,
    preserve_quotes=None,
    explicit_start=None,
    explicit_end=None,
    version=None,
    dump_data=None,
):
    """
    inp:    input string to parse
    outp:   expected output (equals input if not specified)
    """
    inp = dedent(inp)
    if outp is None:
        outp = inp
    if version is not None:
        version = version
    doutp = dedent(outp)
    if extra is not None:
        doutp += extra
    yaml = YAML()
    yaml.preserve_quotes = preserve_quotes
    yaml.scalar_after_indicator = False  # newline after every directives end
    data = yaml.load(inp)
    if dump_data:
        print("data", data)
    if intermediate is not None:
        if isinstance(intermediate, dict):
            for k, v in intermediate.items():
                if data[k] != v:
                    print("{0!r} <> {1!r}".format(data[k], v))
                    raise ValueError
    yaml.indent = indent
    yaml.top_level_colon_align = top_level_colon_align
    yaml.prefix_colon = prefix_colon
    yaml.explicit_start = explicit_start
    yaml.explicit_end = explicit_end
    res = yaml.dump(data, compare=doutp)
    return res


def YAML(**kw):
    import srsly.ruamel_yaml  # NOQA

    class MyYAML(srsly.ruamel_yaml.YAML):
        """auto dedent string parameters on load"""

        def load(self, stream):
            if isinstance(stream, str):
                if stream and stream[0] == "\n":
                    stream = stream[1:]
                stream = textwrap.dedent(stream)
            return srsly.ruamel_yaml.YAML.load(self, stream)

        def load_all(self, stream):
            if isinstance(stream, str):
                if stream and stream[0] == "\n":
                    stream = stream[1:]
                stream = textwrap.dedent(stream)
            for d in srsly.ruamel_yaml.YAML.load_all(self, stream):
                yield d

        def dump(self, data, **kw):
            from srsly.ruamel_yaml.compat import StringIO, BytesIO  # NOQA

            assert ("stream" in kw) ^ ("compare" in kw)
            if "stream" in kw:
                return srsly.ruamel_yaml.YAML.dump(data, **kw)
            lkw = kw.copy()
            expected = textwrap.dedent(lkw.pop("compare"))
            unordered_lines = lkw.pop("unordered_lines", False)
            if expected and expected[0] == "\n":
                expected = expected[1:]
            lkw["stream"] = st = StringIO()
            srsly.ruamel_yaml.YAML.dump(self, data, **lkw)
            res = st.getvalue()
            print(res)
            if unordered_lines:
                res = sorted(res.splitlines())
                expected = sorted(expected.splitlines())
            assert res == expected

        def round_trip(self, stream, **kw):
            from srsly.ruamel_yaml.compat import StringIO, BytesIO  # NOQA

            assert isinstance(stream, (srsly.ruamel_yaml.compat.text_type, str))
            lkw = kw.copy()
            if stream and stream[0] == "\n":
                stream = stream[1:]
            stream = textwrap.dedent(stream)
            data = srsly.ruamel_yaml.YAML.load(self, stream)
            outp = lkw.pop("outp", stream)
            lkw["stream"] = st = StringIO()
            srsly.ruamel_yaml.YAML.dump(self, data, **lkw)
            res = st.getvalue()
            if res != outp:
                diff(outp, res, "input string")
            assert res == outp

        def round_trip_all(self, stream, **kw):
            from srsly.ruamel_yaml.compat import StringIO, BytesIO  # NOQA

            assert isinstance(stream, (srsly.ruamel_yaml.compat.text_type, str))
            lkw = kw.copy()
            if stream and stream[0] == "\n":
                stream = stream[1:]
            stream = textwrap.dedent(stream)
            data = list(srsly.ruamel_yaml.YAML.load_all(self, stream))
            outp = lkw.pop("outp", stream)
            lkw["stream"] = st = StringIO()
            srsly.ruamel_yaml.YAML.dump_all(self, data, **lkw)
            res = st.getvalue()
            if res != outp:
                diff(outp, res, "input string")
            assert res == outp

    return MyYAML(**kw)


def save_and_run(program, base_dir=None, output=None, file_name=None, optimized=False):
    """
    safe and run a python program, thereby circumventing any restrictions on module level
    imports
    """
    from subprocess import check_output, STDOUT, CalledProcessError

    if not hasattr(base_dir, "hash"):
        base_dir = Path(str(base_dir))
    if file_name is None:
        file_name = "safe_and_run_tmp.py"
    file_name = base_dir / file_name
    file_name.write_text(dedent(program))

    try:
        cmd = [sys.executable]
        if optimized:
            cmd.append("-O")
        cmd.append(str(file_name))
        print("running:", *cmd)
        res = check_output(cmd, stderr=STDOUT, universal_newlines=True)
        if output is not None:
            if "__pypy__" in sys.builtin_module_names:
                res = res.splitlines(True)
                res = [line for line in res if "no version info" not in line]
                res = "".join(res)
            print("result:  ", res, end="")
            print("expected:", output, end="")
            assert res == output
    except CalledProcessError as exception:
        print("##### Running '{} {}' FAILED #####".format(sys.executable, file_name))
        print(exception.output)
        return exception.returncode
    return 0
