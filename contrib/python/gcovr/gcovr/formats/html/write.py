# -*- coding:utf-8 -*-

#  ************************** Copyrights and license ***************************
#
# This file is part of gcovr 8.6, a parsing and reporting tool for gcov.
# https://gcovr.com/en/8.6
#
# _____________________________________________________________________________
#
# Copyright (c) 2013-2026 the gcovr authors
# Copyright (c) 2013 Sandia Corporation.
# Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
# the U.S. Government retains certain rights in this software.
#
# This software is distributed under the 3-clause BSD License.
# For more information, see the README.rst file.
#
# ****************************************************************************

# cspell:ignore xmlcharrefreplace

import functools
import os
import re
from typing import Any, Callable, Iterable, Iterator

from jinja2 import (
    BaseLoader,
    Environment,
    ChoiceLoader,
    FileSystemLoader,
    FunctionLoader,
    PackageLoader,
    Template,
)
from markupsafe import Markup
import pygments
from pygments.filter import Filter
from pygments.formatters.html import HtmlFormatter
from pygments.lexer import Lexer
from pygments.lexers import get_lexer_for_filename
from pygments.token import _TokenType, Token
from pygments.style import Style
from pygments.styles.default import DefaultStyle

from ...data_model.container import CoverageContainer, CoverageContainerDirectory
from ...data_model.coverage import (
    DecisionCoverageConditional,
    DecisionCoverageSwitch,
    DecisionCoverageUncheckable,
    FileCoverage,
    LineCoverage,
    CoverageStat,
    DecisionCoverageStat,
)
from ...data_model.coverage_dict import FunctioncovKeyType
from ...exclusions.markers import _EXCLUDE_FLAG, get_markers_regex
from ...logging import LOGGER
from ...options import Options
from ...utils import (
    GZIP_SUFFIX,
    chdir,
    commonpath,
    force_unix_separator,
    get_md5_hexdigest,
    get_version_for_report,
    open_text_for_writing,
)


PYGMENTS_CSS_MARKER = "/* Comment.Preproc */"


# html_theme string is <theme_directory>.<color> or only <color> (if only color use default)
# examples: github.green github.blue or blue or green
def get_theme_name(html_theme: str) -> str:
    """Get the theme name without the color."""
    return html_theme.split(".")[0] if "." in html_theme else "default"


def get_theme_color(html_theme: str) -> str:
    """Get the theme color from the theme name."""
    return html_theme.split(".")[1] if "." in html_theme else html_theme


@functools.lru_cache(maxsize=1)
def templates(options: Options) -> Environment:
    """Get the Jinja2 environment for the templates."""
    # As default use the package loader
    loaders: list[BaseLoader] = []
    # If a directory is given files in the directory have higher precedence.
    if options.html_template_dir is not None:
        loaders.append(FileSystemLoader(options.html_template_dir))

    loaders += [
        PackageLoader(
            "gcovr.formats.html",
            package_path=get_theme_name(options.html_theme),
        ),
        PackageLoader(
            "gcovr.formats.html",
            package_path="common",
        ),
    ]

    return Environment(
        loader=ChoiceLoader(loaders),
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )


@functools.lru_cache(maxsize=1)
def user_templates() -> Environment:
    """Get the Jinja2 environment for the user templates."""

    def load_user_template(template: str) -> str | None:
        contents = None
        try:
            with open(template, "rb") as f:
                contents = f.read().decode("UTF-8")
        # This exception can only occur if the file gets inaccessible while gcovr is running.
        except FileNotFoundError:  # pragma: no cover
            pass

        return contents

    return Environment(
        loader=FunctionLoader(load_user_template),
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )


class CssRenderer:
    """Class for rendering the CSS template with Jinja2."""

    @staticmethod
    def __load_css_template(options: Options) -> Template:
        """Load the CSS template."""
        if options.html_css is not None:
            template_path = os.path.relpath(options.html_css)
            return user_templates().get_template(template_path)

        return templates(options).get_template("style.css")

    @staticmethod
    def render(options: Options) -> str:
        """Get the rendered CSS content."""
        template = CssRenderer.__load_css_template(options)
        return template.render(tab_size=options.html_tab_size)


class NullHighlighting:
    """Class if no syntax highlighting is available for the given file."""

    def get_css(self) -> str:
        """Get the empty CSS."""
        return ""

    @staticmethod
    def highlighter_for_file(_: str) -> Callable[[str], list[str]]:
        """Get the default highlighter which only returns the content as raw text lines."""
        return lambda code: [line.rstrip() for line in code.split("\n")]


class PygmentsHighlighting:
    """Class for syntax highlighting in report."""

    class DefaultStyle(Style):
        """GCOVR default style."""

        styles = dict(
            list(DefaultStyle.styles.items()) + [(Token.Comment.Special, "bold")]
        )

    class MarkerFilter(Filter):
        """A filter to highlight the marker keywords"""

        def __init__(self, markers_regex: re.Pattern[str], **options: Any):
            super().__init__(**options)
            self.markers_regex = markers_regex

        def filter(
            self, lexer: Lexer, stream: Iterable[tuple[_TokenType, str]]
        ) -> Iterator[tuple[_TokenType, str]]:
            for ttype, value in stream:
                if _EXCLUDE_FLAG in value:
                    last = 0
                    for match in self.markers_regex.finditer(value):
                        start, end = match.start(), match.end()
                        if start != last:
                            yield ttype, value[last:start]
                        yield Token.Comment.Special, value[start:end]
                        last = end
                    if last != len(value):
                        yield ttype, value[last:]
                else:
                    yield ttype, value

    def __init__(self, style: str, markers_regex: re.Pattern[str]) -> None:
        self.filter = PygmentsHighlighting.MarkerFilter(markers_regex)
        self.formatter = None
        try:
            self.formatter = HtmlFormatter(
                nowrap=True,
                style=PygmentsHighlighting.DefaultStyle
                if style == "default"
                else style,
            )
        except ImportError as e:  # pragma: no cover
            LOGGER.warning("No syntax highlighting available: %s", str(e))

    def get_css(self) -> str:
        """Get the CSS for the syntax highlighting."""
        if self.formatter is None:  # pragma: no cover
            return ""
        return (
            f"\n\n/* pygments syntax highlighting */\n{self.formatter.get_style_defs()}"  # type: ignore [no-untyped-call]
        )

    def highlighter_for_file(self, filename: str) -> Callable[[str], list[str]]:
        """Get the highlighter for the given filename."""
        if self.formatter is None:  # pragma: no cover
            return NullHighlighting.highlighter_for_file(filename)

        try:
            lexer = get_lexer_for_filename(filename, None, stripnl=False)
            lexer.add_filter(self.filter)
            formatter = self.formatter
            return lambda code: [
                Markup(line.rstrip())  # nosec
                for line in pygments.highlight(code, lexer, formatter).split("\n")
            ]
        except pygments.util.ClassNotFound:  # pragma: no cover
            return NullHighlighting.highlighter_for_file(filename)


@functools.lru_cache(maxsize=1)
def get_formatter(options: Options) -> PygmentsHighlighting | NullHighlighting:
    """Get the formatter for the selected theme."""
    if options.html_syntax_highlighting:
        highlight_style = (
            templates(options)
            .get_template(f"pygments.{get_theme_color(options.html_theme)}")
            .render()
        )
        return PygmentsHighlighting(
            highlight_style, get_markers_regex(options.exclude_pattern_prefix)
        )

    return NullHighlighting()


def coverage_to_class(
    coverage: float | None, medium_threshold: float, high_threshold: float
) -> str:
    """Get the coverage class depending on the threshold."""
    if coverage is None:
        return "coverage-unknown"
    if coverage == 0:
        return "coverage-none"
    if coverage < medium_threshold:
        return "coverage-low"
    if coverage < high_threshold:
        return "coverage-medium"
    return "coverage-high"


class RootInfo:
    """Class holding the information used in Jinja2 template."""

    def __init__(self, options: Options) -> None:
        self.sort_by = (
            "filename"
            if options.sort_key == "filename"
            else ("branches" if options.sort_branches else "lines")
        )
        self.sort_percent = options.sort_key != "uncovered-number"
        self.sorted = (
            "sorted-descending" if options.sort_reverse else "sorted-ascending"
        )
        self.medium_threshold = options.medium_threshold
        self.high_threshold = options.high_threshold
        self.medium_threshold_line = options.medium_threshold_line
        self.high_threshold_line = options.high_threshold_line
        self.medium_threshold_branch = options.medium_threshold_branch
        self.high_threshold_branch = options.high_threshold_branch
        self.link_function_list = (
            options.html_details or options.html_nested
        ) and not (options.html_single_page and options.html_static_report)
        self.relative_anchors = options.html_relative_anchors
        self.single_page = options.html_single_page
        self.static_report = options.html_static_report

        self.version = get_version_for_report()
        self.head = options.html_title
        self.date = options.timestamp.isoformat(sep=" ", timespec="seconds")
        self.encoding = options.html_encoding
        self.directory = ""
        self.branches = dict[str, Any]()
        self.conditions = dict[str, Any]()
        self.decisions = dict[str, Any]()
        self.calls = dict[str, Any]()
        self.functions = dict[str, Any]()
        self.lines = dict[str, Any]()
        self.navigation = dict[str, tuple[str | None, str | None]]()

    def set_directory(self, directory: str) -> None:
        """Set the directory for the report."""
        self.directory = directory

    def get_directory(self) -> str:
        """Get the directory for the report."""
        return (
            "." if self.directory == "" else force_unix_separator(str(self.directory))
        )

    def set_coverage(self, covdata: CoverageContainer) -> None:
        """Update this RootInfo with a summary of the CoverageContainer."""
        stats = covdata.stats
        self.lines = dict_from_stat(stats.line, self.line_coverage_class, 0.0)
        self.functions = dict_from_stat(stats.function, self.coverage_class)
        self.branches = dict_from_stat(stats.branch, self.branch_coverage_class)
        self.conditions = dict_from_stat(stats.condition, self.coverage_class)
        self.decisions = dict_from_stat(stats.decision, self.coverage_class)
        self.calls = dict_from_stat(stats.call, self.coverage_class)

    def line_coverage_class(self, coverage: float | None) -> str:
        """Get the coverage class for the line."""
        return coverage_to_class(
            coverage, self.medium_threshold_line, self.high_threshold_line
        )

    def branch_coverage_class(self, coverage: float | None) -> str:
        """Get the coverage class for the branch."""
        return coverage_to_class(
            coverage, self.medium_threshold_branch, self.high_threshold_branch
        )

    def coverage_class(self, coverage: float | None) -> str:
        """Get the coverage class for all other types."""
        return coverage_to_class(coverage, self.medium_threshold, self.high_threshold)


#
# Produce an HTML report
#
def write_report(
    covdata: CoverageContainer, output_file: str, options: Options
) -> None:
    """Write the HTML report"""
    css_data = CssRenderer.render(options).strip()
    medium_threshold = options.medium_threshold
    high_threshold = options.high_threshold
    medium_threshold_line = options.medium_threshold_line
    high_threshold_line = options.high_threshold_line
    medium_threshold_branch = options.medium_threshold_branch
    high_threshold_branch = options.high_threshold_branch
    show_calls = options.show_calls
    show_decision = options.show_decision

    data = dict[str, Any]()
    root_info = RootInfo(options)
    data["info"] = root_info

    data["SHOW_DECISION"] = show_decision
    data["SHOW_CALLS"] = show_calls
    data["SHOW_CONDITION_COVERAGE"] = any(
        filter(lambda filecov: filecov.condition_coverage().total > 0, covdata.values())  # type: ignore [arg-type]
    )
    data["USE_BLOCK_IDS"] = options.html_block_ids
    data["COVERAGE_MED"] = medium_threshold
    data["COVERAGE_HIGH"] = high_threshold
    data["LINE_COVERAGE_MED"] = medium_threshold_line
    data["LINE_COVERAGE_HIGH"] = high_threshold_line
    data["BRANCH_COVERAGE_MED"] = medium_threshold_branch
    data["BRANCH_COVERAGE_HIGH"] = high_threshold_branch

    self_contained = options.html_self_contained
    if self_contained is None:
        self_contained = (
            not (options.html_details or options.html_nested)
            or options.html_single_page
        )

    if output_file.endswith(os.sep):
        if options.html_single_page:
            output_file += "coverage_single_page.html"
        elif options.html_nested:
            output_file += "coverage_nested.html"
        elif options.html_details:
            output_file += "coverage_details.html"
        else:
            output_file += "coverage.html"

    if PYGMENTS_CSS_MARKER in css_data:
        LOGGER.info(
            "Skip adding of pygments styles since %r found in user stylesheet",
            PYGMENTS_CSS_MARKER,
        )
    else:
        css_data += get_formatter(options).get_css()

    if options.html_details or options.html_nested:
        data["ROOT_FNAME"] = os.path.basename(output_file)
        (output_prefix, output_suffix) = _get_prefix_and_suffix(output_file)
        functions_output_file = f"{output_prefix}.functions{output_suffix}"
        data["FUNCTIONS_FNAME"] = os.path.basename(functions_output_file)
        if options.html_single_page:
            # Remove the prefix to get shorter links
            data["FUNCTIONS_FNAME"] = data["FUNCTIONS_FNAME"].split(".", maxsplit=1)[1]

    javascript_data = (
        None
        if options.html_static_report
        else templates(options).get_template("gcovr.js").render(**data).strip()
    )

    if self_contained:
        data["css"] = css_data
        if javascript_data is not None:
            data["javascript"] = javascript_data
    else:
        css_output = os.path.splitext(output_file)[0] + ".css"
        with open_text_for_writing(css_output, encoding="utf-8") as fh_out:
            fh_out.write(css_data)
            fh_out.write("\n")

        if options.html_relative_anchors:
            css_link = os.path.basename(css_output)
        else:  # pragma: no cover  Can't be checked because of the reference compare
            css_link = css_output
        data["css_link"] = css_link

        if javascript_data is not None:
            javascript_output = os.path.splitext(output_file)[0] + ".js"
            with open_text_for_writing(javascript_output) as fh_out:
                fh_out.write(javascript_data)
                fh_out.write("\n")

            if options.html_relative_anchors:
                javascript_link = os.path.basename(javascript_output)
            else:  # pragma: no cover  Can't be checked because of the reference compare
                javascript_link = javascript_output
            data["javascript_link"] = javascript_link

    data["theme"] = get_theme_color(options.html_theme)

    root_info.set_coverage(covdata)
    if root_info.link_function_list and root_info.functions["total"] == 0:
        root_info.link_function_list = False

    # Generate the coverage output (on a per-package basis)
    # source_dirs = set()
    files = []
    filtered_fname = ""
    sorted_keys = covdata.sort_coverage(
        sort_key=options.sort_key,
        sort_reverse=options.sort_reverse,
        by_metric="branch" if options.sort_branches else "line",
        filename_uses_relative_pathname=True,
    )

    if options.html_nested:
        covdata.populate_directories(sorted_keys, options.root_filter)

    cdata_fname = dict[str, str]()
    cdata_sourcefile = dict[str, str | None]()
    for fname in sorted_keys + [v.dirname for v in covdata.directories]:
        filtered_fname = options.root_filter.sub("", fname)
        if filtered_fname != "":
            files.append(filtered_fname)
        cdata_fname[fname] = force_unix_separator(filtered_fname)
        if options.html_details or options.html_nested or options.html_single_page:
            if os.path.normpath(fname) == os.path.normpath(options.root_dir):
                cdata_sourcefile[fname] = (
                    output_file[: -len(GZIP_SUFFIX)]
                    if output_file.endswith(GZIP_SUFFIX)
                    else output_file
                )
            else:
                cdata_sourcefile[fname] = _make_short_source_filename(
                    output_file, filtered_fname.rstrip(os.sep)
                )
                if options.html_single_page and cdata_sourcefile[fname] is not None:
                    # Remove the prefix to get shorter links
                    cdata_sourcefile[fname] = str(cdata_sourcefile[fname]).split(
                        ".", maxsplit=1
                    )[1]
        else:
            cdata_sourcefile[fname] = None

    # Define the common root directory, which may differ from options.root_dir
    # when source files share a common prefix.
    root_directory = ""
    if len(files) > 1:
        common_dir = commonpath(files)
        if common_dir != "":
            root_directory = common_dir
    else:
        directory, _ = os.path.split(filtered_fname)
        if directory != "":
            root_directory = str(directory) + os.sep

    previous_fname: str | None = None
    previous_link_report: str | None = None
    for fname in sorted(cdata_sourcefile.keys()):
        root_info.navigation[fname] = (previous_link_report, None)
        link_report = cdata_sourcefile[fname]
        if link_report is not None:
            if root_info.relative_anchors or root_info.single_page:
                link_report = os.path.basename(link_report)
        if previous_fname is not None:
            root_info.navigation[previous_fname] = (
                root_info.navigation[previous_fname][0],
                link_report,
            )
        previous_fname = fname
        previous_link_report = link_report

    root_info.set_directory(force_unix_separator(root_directory))

    if options.html_single_page:
        write_single_page(
            options,
            root_info,
            output_file,
            covdata,
            sorted_keys,
            cdata_fname,
            cdata_sourcefile,
            data,
        )
    else:
        if options.html_nested and covdata.directories:
            write_directory_pages(
                options,
                root_info,
                output_file,
                covdata,
                cdata_fname,
                cdata_sourcefile,
                data,
            )
        else:
            write_root_page(
                options,
                root_info,
                output_file,
                covdata,
                cdata_fname,
                cdata_sourcefile,
                data,
                sorted_keys,
            )
            if not options.html_details:
                return

        write_source_pages(
            options,
            root_info,
            functions_output_file,
            covdata,
            cdata_fname,
            cdata_sourcefile,
            data,
        )


def write_root_page(
    options: Options,
    root_info: RootInfo,
    output_file: str,
    covdata: CoverageContainer,
    cdata_fname: dict[str, str],
    cdata_sourcefile: dict[str, Any],
    data: dict[str, Any],
    sorted_keys: list[str],
) -> None:
    """Generate the root HTML file that contains the high level report."""
    files = []
    for fname in sorted_keys:
        files.append(
            get_coverage_data(
                root_info, covdata[fname], cdata_sourcefile[fname], cdata_fname[fname]
            )
        )

    html_string = (
        templates(options)
        .get_template("directory_page.html")
        .render(**data, entries=files)
    )
    with open_text_for_writing(
        output_file, encoding=options.html_encoding, errors="xmlcharrefreplace"
    ) as fh:
        fh.write(html_string + "\n")


def write_source_pages(
    options: Options,
    root_info: RootInfo,
    functions_output_file: str,
    covdata: CoverageContainer,
    cdata_fname: dict[str, str],
    cdata_sourcefile: dict[str, Any],
    data: dict[str, Any],
) -> None:
    """Write a page for each source file."""
    error_no_files_not_found = 0
    all_functions = {}
    for fname, filecov in sorted(covdata.items()):
        file_data, functions, file_not_found = get_file_data(
            options, root_info, fname, cdata_fname, cdata_sourcefile, filecov
        )
        all_functions.update(functions)
        if file_not_found:
            error_no_files_not_found += 1

        html_string = (
            templates(options)
            .get_template("source_page.html")
            .render(**data, **file_data)
        )
        with open_text_for_writing(
            cdata_sourcefile[fname],
            encoding=options.html_encoding,
            errors="xmlcharrefreplace",
        ) as fh:
            fh.write(html_string + "\n")

    html_string = (
        templates(options)
        .get_template("functions_page.html")
        .render(
            **data,
            all_functions=[all_functions[k] for k in sorted(all_functions)],
        )
    )
    with open_text_for_writing(
        functions_output_file,
        encoding=options.html_encoding,
        errors="xmlcharrefreplace",
    ) as fh:
        fh.write(html_string + "\n")

    if error_no_files_not_found != 0:
        raise RuntimeError(f"{error_no_files_not_found} source file(s) not found.")


def write_directory_pages(
    options: Options,
    root_info: RootInfo,
    output_file: str,
    covdata: CoverageContainer,
    cdata_fname: dict[str, str],
    cdata_sourcefile: dict[str, Any],
    data: dict[str, Any],
) -> None:
    """Write a page for each directory."""
    # The first directory is the shortest one --> This is the root dir
    root_key = next(iter(sorted([d.dirname for d in covdata.directories])))

    directory_data = {}
    for dircov in covdata.directories:
        directory_data = get_directory_data(
            options, root_info, cdata_fname, cdata_sourcefile, dircov
        )
        html_string = (
            templates(options)
            .get_template("directory_page.html")
            .render(**data, **directory_data)
        )
        filename = None
        if dircov.dirname in [root_key, ""]:
            filename = output_file
        elif dircov.dirname in cdata_sourcefile:
            filename = cdata_sourcefile[dircov.dirname]
        else:
            LOGGER.warning(
                "There's a subdirectory %r that there's no source files within it",
                dircov.dirname,
            )

        if filename:
            with open_text_for_writing(
                filename, encoding=options.html_encoding, errors="xmlcharrefreplace"
            ) as fh:
                fh.write(html_string + "\n")


def write_single_page(
    options: Options,
    root_info: RootInfo,
    output_file: str,
    covdata: CoverageContainer,
    sorted_keys: list[str],
    cdata_fname: dict[str, str],
    cdata_sourcefile: dict[str, Any],
    data: dict[str, Any],
) -> None:
    """Write a single page HTML report."""
    error_no_files_not_found = 0
    files = []
    all_functions = {}
    for fname, filecov in sorted(covdata.items()):
        file_data, functions, file_not_found = get_file_data(
            options, root_info, fname, cdata_fname, cdata_sourcefile, filecov
        )
        all_functions.update(functions)
        if file_not_found:
            error_no_files_not_found += 1

        files.append(file_data)

    all_files = []
    for fname in sorted_keys:
        all_files.append(
            get_coverage_data(
                root_info, covdata[fname], cdata_sourcefile[fname], cdata_fname[fname]
            )
        )
    directories = list[dict[str, Any]]([{"entries": all_files}])
    if not root_info.static_report:
        for dircov in covdata.directories:
            directories.append(
                get_directory_data(
                    options, root_info, cdata_fname, cdata_sourcefile, dircov
                )
            )
    if len(directories) == 1:
        directories[0]["dirname"] = "/"  # We need this to have a correct id in HTML.

    html_string = (
        templates(options)
        .get_template("single_page.html")
        .render(
            **data,
            files=files,
            directories=directories,
            all_functions=[all_functions[k] for k in sorted(all_functions)],
        )
    )
    with open_text_for_writing(
        output_file,
        encoding=options.html_encoding,
        errors="xmlcharrefreplace",
    ) as fh:
        fh.write(html_string + "\n")

    if error_no_files_not_found != 0:
        raise RuntimeError(f"{error_no_files_not_found} source file(s) not found.")


def get_coverage_data(
    root_info: RootInfo,
    cdata: CoverageContainerDirectory | FileCoverage,
    link_report: str,
    cdata_fname: str,
    relative_path: str = "",
) -> dict[str, Any]:
    """Get the coverage data"""

    medium_threshold = root_info.medium_threshold
    high_threshold = root_info.high_threshold
    medium_threshold_line = root_info.medium_threshold_line
    high_threshold_line = root_info.high_threshold_line
    medium_threshold_branch = root_info.medium_threshold_branch
    high_threshold_branch = root_info.high_threshold_branch

    def coverage_class(coverage: float | None) -> str:
        return coverage_to_class(coverage, medium_threshold, high_threshold)

    def line_coverage_class(coverage: float | None) -> str:
        return coverage_to_class(coverage, medium_threshold_line, high_threshold_line)

    def branch_coverage_class(coverage: float | None) -> str:
        return coverage_to_class(
            coverage, medium_threshold_branch, high_threshold_branch
        )

    def sort_value(coverage_stats: CoverageStat | DecisionCoverageStat) -> str:
        return str(
            coverage_stats.percent_or("-")
            if root_info.sort_percent
            else coverage_stats.total - coverage_stats.covered
        )

    stats = cdata.stats

    is_file_with_lines = isinstance(cdata, FileCoverage) and cdata.has_lines()
    lines = {
        "total": stats.line.total_with_excluded,
        "exec": stats.line.covered,
        "excluded": stats.line.excluded,
        "coverage": stats.line.percent_or(100.0 if is_file_with_lines else "-"),
        "class": line_coverage_class(
            stats.line.percent_or(100.0 if is_file_with_lines else None)
        ),
        "sort": sort_value(stats.line),
    }

    branches = {
        "total": stats.branch.total_with_excluded,
        "exec": stats.branch.covered,
        "excluded": stats.branch.excluded,
        "coverage": stats.branch.percent_or("-"),
        "class": branch_coverage_class(stats.branch.percent),
        "sort": sort_value(stats.branch),
    }

    conditions = {
        "total": stats.condition.total_with_excluded,
        "exec": stats.condition.covered,
        "excluded": stats.condition.excluded,
        "coverage": stats.condition.percent_or("-"),
        "class": branch_coverage_class(stats.condition.percent),
        "sort": sort_value(stats.condition),
    }

    decisions = {
        "total": stats.decision.total,
        "exec": stats.decision.covered,
        "unchecked": stats.decision.uncheckable,
        "coverage": stats.decision.percent_or("-"),
        "class": coverage_class(stats.decision.percent),
        "sort": sort_value(stats.decision),
    }

    functions = {
        "total": stats.function.total_with_excluded,
        "exec": stats.function.covered,
        "excluded": stats.function.excluded,
        "coverage": stats.function.percent_or("-"),
        "class": coverage_class(stats.function.percent),
        "sort": sort_value(stats.function),
    }

    calls = {
        "total": stats.call.total_with_excluded,
        "exec": stats.call.covered,
        "excluded": stats.call.excluded,
        "coverage": stats.call.percent_or("-"),
        "class": coverage_class(stats.call.percent),
        "sort": sort_value(stats.call),
    }
    display_filename = force_unix_separator(
        os.path.relpath(
            os.path.realpath(cdata_fname),
            os.path.realpath(
                os.path.join(root_info.directory, relative_path)
                if relative_path
                else root_info.directory
            ),
        )
    )

    if link_report is not None:
        if root_info.relative_anchors or root_info.single_page:
            link_report = os.path.basename(link_report)

    return {
        "filename": display_filename,
        "link": link_report,
        "lines": lines,
        "branches": branches,
        "conditions": conditions,
        "decisions": decisions,
        "functions": functions,
        "calls": calls,
    }


def get_directory_data(
    options: Options,
    root_info: RootInfo,
    cdata_fname: dict[str, str],
    cdata_sourcefile: dict[str, str],
    covdata_dir: CoverageContainerDirectory,
) -> dict[str, Any]:
    """Get the data for a directory to generate the HTML"""
    relative_path = cdata_fname[covdata_dir.dirname]
    if relative_path == ".":
        relative_path = ""
    elif relative_path.startswith(root_info.directory):
        relative_path = relative_path[len(root_info.directory) :]
    directory_data = dict[str, Any](
        {
            "dirname": (
                cdata_sourcefile[covdata_dir.dirname]
                if cdata_fname[covdata_dir.dirname]
                else "/"
            ),
            "relative_path": relative_path,
        }
    )

    sorted_keys = covdata_dir.sort_coverage(
        sort_key=options.sort_key,
        sort_reverse=options.sort_reverse,
        by_metric="branch" if options.sort_branches else "line",
        filename_uses_relative_pathname=True,
    )

    files = []
    for key in sorted_keys:
        fname = covdata_dir[key].filename
        files.append(
            get_coverage_data(
                root_info,
                covdata_dir[key],
                cdata_sourcefile[fname],
                cdata_fname[fname],
                relative_path,
            )
        )

    directory_data["entries"] = files

    return directory_data


def get_file_data(
    options: Options,
    root_info: RootInfo,
    filename: str,
    cdata_fname: dict[str, str],
    cdata_sourcefile: dict[str, str],
    cdata: FileCoverage,
) -> tuple[dict[str, Any], dict[tuple[str, str, int], dict[str, Any]], bool]:
    """Get the data for a file to generate the HTML"""
    formatter = get_formatter(options)

    file_data = dict[str, Any](
        {
            "fname": filename,
            "filename": cdata_fname[filename],
            "html_filename": os.path.basename(cdata_sourcefile[filename]),
            "source_lines": [],
            "function_list": [],
        }
    )
    file_data.update(
        get_coverage_data(
            root_info, cdata, cdata_sourcefile[filename], cdata_fname[filename]
        )
    )
    functions = dict[tuple[FunctioncovKeyType, str, int], dict[str, Any]]()
    # Only use demangled names (containing a brace)
    for functioncov in cdata.functioncov(key=lambda functioncov: functioncov.key):
        for lineno in functioncov.linenos:
            f_data = dict[str, Any]()
            f_data["name"] = functioncov.name
            f_data["filename"] = cdata_fname[filename]
            f_data["html_filename"] = os.path.basename(cdata_sourcefile[filename])
            f_data["line"] = lineno
            f_data["count"] = functioncov.count[lineno]
            f_data["blocks"] = functioncov.blocks[lineno]
            f_data["excluded"] = functioncov.excluded[lineno]
            function_stats = cdata.filter_for_function(functioncov).stats
            f_data["line_coverage"] = function_stats.line.percent_or(100.0)
            f_data["branch_coverage"] = function_stats.branch.percent_or("-")
            f_data["condition_coverage"] = function_stats.condition.percent_or("-")

            file_data["function_list"].append(f_data)
            functions[
                (
                    functioncov.key,
                    str(f_data["filename"]),
                    int(f_data["line"]),
                )
            ] = f_data

    def get_linecovs(lineno: int) -> list[LineCoverage] | None:
        """Get a list of line coverage objects if available for the line."""
        linecovs = cdata.get_line(lineno)
        return None if linecovs is None else list(linecovs.linecov(sort=True))

    with chdir(options.root_dir):
        linecov_collections = list(cdata.lines())
        max_line_from_cdata = (
            linecov_collections[-1].lineno if linecov_collections else 0
        )
        file_not_found = True
        try:
            with open(
                filename,
                "r",
                encoding=options.source_encoding,
                errors="replace",
            ) as source_file:
                file_not_found = False
                lines = formatter.highlighter_for_file(filename)(source_file.read())
                lineno = 0
                for lineno, line in enumerate(lines, 1):
                    file_data["source_lines"].append(
                        source_row(
                            lineno,
                            line,
                            get_linecovs(lineno),
                            options.html_block_ids,
                        )
                    )
                if lineno < max_line_from_cdata:
                    LOGGER.warning(
                        "File %s has %d line(s) but coverage data has %d line(s).",
                        filename,
                        lineno,
                        max_line_from_cdata,
                    )
        except OSError as e:
            if filename.endswith("<stdin>"):
                file_not_found = False
                file_info = "!!! File from stdin !!!"
            else:
                file_info = f"!!! Can't read file: {e.strerror} !!!"
                LOGGER.warning("Can't read file: %s", e)
            # Python ranges are exclusive. We want to iterate over all lines, including
            # that last line. Thus, we have to add a +1 to include that line.
            for lineno in range(1, max_line_from_cdata + 1):
                file_data["source_lines"].append(
                    source_row(
                        lineno,
                        file_info if lineno == 1 else "",
                        get_linecovs(lineno),
                        options.html_block_ids,
                    )
                )

    file_data["lines"]["uncovered"] = len(
        [row for row in file_data["source_lines"] if row["covclass"] == "uncoveredLine"]
    )
    file_data["lines"]["partial"] = len(
        [
            row
            for row in file_data["source_lines"]
            if row["covclass"] == "partialCoveredLine"
        ]
    )

    return file_data, functions, file_not_found


def dict_from_stat(
    stat: CoverageStat | DecisionCoverageStat,
    coverage_class: Callable[[float | None], str],
    default: float | None = None,
) -> dict[str, Any]:
    """Get a dictionary from the stats."""
    coverage_default = "-" if default is None else default
    data = {
        "total": stat.total_with_excluded
        if isinstance(stat, CoverageStat)
        else stat.total,
        "exec": stat.covered,
        "excluded": stat.excluded if isinstance(stat, CoverageStat) else "-",
        "coverage": stat.percent_or(coverage_default),
        "class": coverage_class(stat.percent_or(default)),
    }

    if isinstance(stat, DecisionCoverageStat):
        data["unchecked"] = stat.uncheckable

    return data


def source_row(
    lineno: int,
    source: str,
    linecov_list: list[LineCoverage] | None,
    html_block_ids: bool,
) -> dict[str, Any]:
    """Get information for a row"""
    line_branches = None
    line_conditions = None
    line_decisions = None
    line_calls = None
    linecount: int | str = ""
    covclass = ""
    if linecov_list:
        if all(linecov.is_excluded for linecov in linecov_list):
            covclass = "excludedLine"
        elif all(linecov.is_uncovered for linecov in linecov_list):
            covclass = "uncoveredLine"
        else:
            line_branches = [
                source_row_branch(linecov)
                for linecov in linecov_list
                if linecov.has_reportable_branches
            ]
            covclass = (
                "coveredLine"
                if all(
                    line_branch["taken"] == line_branch["total"]
                    for line_branch in line_branches
                )
                else "partialCoveredLine"
            )
            line_conditions = [
                source_row_condition(linecov)
                for linecov in linecov_list
                if linecov.has_reportable_conditions
            ]
            line_decisions = [
                source_row_decision(linecov)
                for linecov in linecov_list
                if linecov.decision
            ]
            linecount = sum(linecov.count for linecov in linecov_list)
        line_calls = [
            source_row_call(linecov)
            for linecov in linecov_list
            if linecov.has_reportable_calls
        ]
    return {
        "lineno": lineno,
        "block_ids": []
        if linecov_list is None
        or not html_block_ids
        or all(linecov is None for linecov in linecov_list)
        else [linecov.block_ids for linecov in linecov_list if linecov.block_ids],
        "source": source,
        "covclass": covclass,
        "line_branches": line_branches,
        "line_conditions": line_conditions,
        "line_decisions": line_decisions,
        "line_calls": line_calls,
        "linecount": linecount,
    }


def source_row_branch(
    linecov: LineCoverage,
) -> dict[str, Any]:
    """Get branch information for a row"""
    items = list[dict[str, Any]]()
    for branchcov in [
        branchcov
        for branchcov in linecov.branches(sort=True)
        if branchcov.is_reportable
    ]:
        items.append(
            {
                "taken": branchcov.is_covered,
                "count": branchcov.count,
                "excluded": branchcov.is_excluded,
            }
        )
        if branchcov.branchno is not None:
            items[-1]["branchno"] = branchcov.branchno
        else:
            items[-1]["source_block_id"] = branchcov.source_block_id
            items[-1]["destination_block_id"] = branchcov.destination_block_id

    stats = linecov.branch_coverage()
    return {
        "function_name": linecov.report_function_name,
        "taken": stats.covered,
        "total": stats.total_with_excluded,
        "branches": items,
    }


def source_row_condition(
    linecov: LineCoverage,
) -> dict[str, Any]:
    """Get condition information for a row."""

    items = []

    conditioncov_list = list(
        conditioncov
        for conditioncov in linecov.conditions(sort=True)
        if conditioncov.is_reportable
    )
    for conditioncov in conditioncov_list:
        condition_prefix = (
            f"Condition {conditioncov.conditionno}"
            if len(conditioncov_list) > 1
            else ("" if conditioncov.count == 2 else "Condition ")
        )
        condition_separator = "." if len(conditioncov_list) > 1 else ""
        for index in range(0, conditioncov.count // 2):
            items.append(
                {
                    "prefix": f"{condition_prefix}{condition_separator}{index}: "
                    if conditioncov.count > 2
                    else (f"{condition_prefix}: " if condition_prefix else ""),
                    "not_covered_true": index in conditioncov.not_covered_true,
                    "not_covered_false": index in conditioncov.not_covered_false,
                    "excluded": conditioncov.is_excluded,
                }
            )
    stats = linecov.condition_coverage()
    return {
        "function_name": linecov.report_function_name,
        "count": stats.total_with_excluded,
        "covered": stats.covered,
        "condition": items,
    }


def source_row_decision(
    linecov: LineCoverage,
) -> dict[str, Any]:
    """Get decision information for a row"""

    items: list[dict[str, Any]] = []

    if isinstance(linecov.decision, DecisionCoverageUncheckable):
        items.append(
            {
                "uncheckable": True,
            }
        )
    elif isinstance(linecov.decision, DecisionCoverageConditional):
        items.append(
            {
                "uncheckable": False,
                "taken": linecov.decision.count_true > 0,
                "count": linecov.decision.count_true,
                "name": "true",
            }
        )
        items.append(
            {
                "uncheckable": False,
                "taken": linecov.decision.count_false > 0,
                "count": linecov.decision.count_false,
                "name": "false",
            }
        )
    elif isinstance(linecov.decision, DecisionCoverageSwitch):
        items.append(
            {
                "uncheckable": False,
                "taken": linecov.decision.count > 0,
                "count": linecov.decision.count,
                "name": "true",
            }
        )
    else:
        raise RuntimeError(f"Unknown decision type {linecov.decision!r}")

    return {
        "function_name": linecov.report_function_name,
        "taken": len([i for i in items if i.get("taken", False)]),
        "uncheckable": len([i for i in items if i["uncheckable"]]),
        "total": len(items),
        "decisions": items,
    }


def source_row_call(linecov: LineCoverage) -> dict[str, Any]:
    """Get call information for a source row."""
    items = []

    for callno, callcov in enumerate(
        callcov for callcov in linecov.calls(sort=True) if callcov.is_reportable
    ):
        items.append(
            {
                "name": callno,
                "invoked": callcov.is_covered,
                "excluded": callcov.is_excluded,
            }
        )

    stats = linecov.call_coverage()
    return {
        "function_name": linecov.report_function_name,
        "invoked": stats.covered,
        "total": stats.total_with_excluded,
        "calls": items,
    }


def _get_prefix_and_suffix(output_file: str) -> tuple[str, str]:
    """Split into prefix and suffix, ignoring the last suffix for GZIP."""
    (output_prefix, output_suffix) = os.path.splitext(os.path.abspath(output_file))
    if output_suffix == GZIP_SUFFIX:
        return _get_prefix_and_suffix(output_prefix)

    return (output_prefix, output_suffix)


def _make_short_source_filename(output_file: str, filename: str) -> str:
    r"""Make a short-ish file path for --html-detail output.

    Args:
        output_file (str): The output path.
        filename (str): Path from root to source code.
    """

    (output_prefix, output_suffix) = _get_prefix_and_suffix(output_file)
    filename = filename.replace(os.sep, "/").replace("<stdin>", "stdin")
    source_filename = (
        ".".join(
            (
                output_prefix,
                os.path.basename(filename),
                get_md5_hexdigest(filename.encode("UTF-8")),
            )
        )
        + output_suffix
    )
    return source_filename
