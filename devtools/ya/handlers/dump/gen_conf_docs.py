from __future__ import absolute_import, unicode_literals
import exts.yjson as json
import re
import os
import six

import core.config

import build.genconf
import build.ymake2

from yalibrary.vcs import vcsversion
from build.build_facade import gen_conf
from exts.strtobool import strtobool


class _Markdown:
    header = '#'
    alink = 'https://a.yandex-team.ru/arc/trunk/arcadia/'
    # Section nesting count
    scount = 2
    # Description nesting count
    dcount = 6
    # Avoid too many entries in toc for macros
    chunks_in_toc = 10
    # Checks description
    internal_pattern = re.compile(r'^\s*@?internal[.]?\s*$', flags=re.IGNORECASE)
    # Checks description
    deprecated_pattern = re.compile(r'^\s*@?deprecated[.]?\s*$', flags=re.IGNORECASE)
    # Checks header
    h_patterns = {
        'internal': re.compile('.*#.*internal.*', flags=re.IGNORECASE),
        'deprecated': re.compile('.*#.*deprecated.*', flags=re.IGNORECASE),
    }

    def __init__(self, arc_root, dump_all_descs, use_svn):
        self.descs = {'macros': {}, 'modules': {}, 'multimodules': {}, 'unknowns': {}}
        self.links = {}
        self.anchors = {}
        try:
            self.svn_revision, _ = vcsversion.repo_config(arc_root) if use_svn else (-1, '')
        except Exception:
            self.svn_revision = -1
        self.dump_all_descs = dump_all_descs

    def process_entry(self, file, entry):
        if 'kind' not in entry or entry['kind'] != 'node':
            return

        self._add_entry_optionally(file, entry)

    def dump(self):
        res = self._dump_toc()

        for type in ['multimodules', 'modules', 'macros', 'unknowns']:
            if self.descs[type]:
                res += _Markdown._format_section(type)

                for name in sorted(self.descs[type]):
                    for d in self.descs[type][name]['text']:
                        res += six.ensure_str(d)

        for type in ['multimodules', 'modules', 'macros', 'unknowns']:
            if self.descs[type]:
                for name in sorted(self.descs[type]):
                    link = self.descs[type][name]['link']
                    res += six.ensure_str(link)

        return res

    def _add_entry_optionally(self, file, entry):
        props = entry['properties']
        doc = {'name': props['name'], 'type': props['type'], 'file': file, 'line': entry['range']['line']}

        if 'comment' in props:
            doc['long desc'] = props['comment']
        if 'usage' in props:
            doc['usage'] = props['usage']

        doc['revision'] = self.svn_revision

        descs, link = self._format_entry(doc)

        dictionary = (
            self.descs[doc['type'] + 's']
            if doc['type'] in ['macro', 'module', 'multimodule']
            else self.descs['unknowns']
        )

        if not self.dump_all_descs and _Markdown._is_internal(descs[0]):
            return

        dictionary[doc['name']] = {'text': descs, 'link': link, 'src_data': doc}

    @classmethod
    def _is_internal(cls, header):
        return cls.h_patterns['internal'].match(header)

    def _dump_toc(self):
        res = '*Do not edit, this file is generated from comments to macros definitions using `ya dump conf-docs{all}`.*\n\n'.format(
            all=' --dump-all' if self.dump_all_descs else ''
        )
        res += '{markup} ya.make {all}commands\n\n'.format(
            markup=_Markdown.header, all='and core.conf ' if self.dump_all_descs else ''
        )
        res += (
            'General info: [How to write ya.make files](https://wiki.yandex-team.ru/yatool/HowToWriteYaMakeFiles)\n\n'
        )
        res += '{markup} Table of contents\n\n'.format(markup=_Markdown.header * 2)

        for type in ['multimodules', 'modules', 'macros', 'unknowns']:
            if self.descs[type]:
                res += _Markdown._format_toc_section(type)
                if type != 'macros':
                    for name in sorted(self.descs[type]):
                        res += _Markdown._format_toc_header(self.descs[type][name]['src_data'])
                else:
                    chunk_cnt = 0
                    first_macro = {}
                    last_macro = {}
                    for name in sorted(self.descs[type]):
                        if chunk_cnt == 0:
                            first_macro = self.descs[type][name]['src_data']
                        chunk_cnt += 1
                        last_macro = self.descs[type][name]['src_data']

                        if chunk_cnt == _Markdown.chunks_in_toc:
                            chunk_cnt = 0
                            res += _Markdown._format_toc_macro_header(first_macro, last_macro)

                    if chunk_cnt != 0:
                        res += _Markdown._format_toc_macro_header(first_macro, last_macro)
        return res

    @classmethod
    def _format_entry(cls, doc):
        descs = []

        lines, special_tags = _Markdown._format_desc(doc)
        descs.append(_Markdown._format_header(doc, special_tags))
        descs.append(lines)
        return (descs, _Markdown._format_link(doc))

    @classmethod
    def _format_toc_section(cls, type):
        return '{indent} * [{section}](#{anchor})\n'.format(
            indent=' ' * cls.scount, section=type.capitalize(), anchor=type
        )

    @classmethod
    def _format_section(cls, type):
        return '{markup} {text} <a name="{anchor}"></a>\n\n'.format(
            markup=cls.header * cls.scount, text=type.capitalize(), anchor=type
        )

    @staticmethod
    def _format_header_anchor(doc):
        return '<a name="{atype}_{aname}"></a>'.format(atype=doc['type'], aname=doc['name'])

    @classmethod
    def _format_toc_header(cls, doc):
        qual = doc['type'].capitalize() if doc['type'] in ['macro', 'module', 'multimodule'] else "Unknown"
        return '{indent} - {qual} [{name}](#{type}_{name})\n'.format(
            indent=' ' * cls.dcount, qual=qual, name=doc['name'], type=doc['type']
        )

    @classmethod
    def _format_toc_macro_header(cls, fdoc, ldoc):
        return '{indent} - Macros [{fname}](#{ftype}_{fname}) .. [{lname}](#{ltype}_{lname})\n'.format(
            indent=' ' * cls.dcount, fname=fdoc['name'], ftype=fdoc['type'], lname=ldoc['name'], ltype=ldoc['type']
        )

    # Also adds special tags 'internal' and 'deprecated'
    @classmethod
    def _format_header(cls, doc, special_tags):
        name = doc['name']
        usage = doc['usage'] if 'usage' in doc else ""
        usage = name if not usage else usage

        qual = doc['type'].capitalize() if doc['type'] in ['macro', 'module', 'multimodule'] else "Unknown"

        usage = _Markdown._remove_formatting(usage.rstrip().lstrip())
        name = _Markdown._remove_formatting(name)

        usage = usage.replace(name, '[' + name + "][]", 1)

        if special_tags:
            special = ''
            for tag in special_tags:
                if not cls.h_patterns[tag].match(usage):
                    special += ' ' + tag
            # Emphasis
            if usage.find("#") != -1:
                usage += special
            else:
                usage += ' #' + special[1:]

        # Emphasis
        if usage.find("#") != -1:
            usage = usage.replace('#', "_#", 1)
            usage += "_"

        return '{markup} {type} {usage} {anchor}\n'.format(
            markup=cls.header * cls.dcount, type=qual, usage=usage, anchor=_Markdown._format_header_anchor(doc)
        )

    # Prints verbatim. Strips unnecessary indent and escapes '_'/'*'.
    @classmethod
    def _format_desc(cls, doc):
        result = ""

        desc = ""
        if 'long desc' in doc:
            desc = doc['long desc'].rstrip()
        if not desc:
            desc = " Not documented yet.\n"

        lines = _Markdown._strip_blanks(desc.splitlines())
        lines = _Markdown._remove_formatting_markdown(lines)

        result += '\n'.join(lines) + "\n\n"

        special_tags = []
        if doc['name'].startswith('_') or any([cls.internal_pattern.match(x) for x in lines]):
            special_tags.append("internal")
        if any([cls.deprecated_pattern.match(x) for x in lines]):
            special_tags.append("deprecated")

        return (result, special_tags)

    @staticmethod
    def _format_link(doc):
        return ' [{tag_name}]: {baselink}{file}{rev}#L{line}\n'.format(
            tag_name=_Markdown._remove_formatting(doc['name']),
            baselink=_Markdown.alink,
            file=doc['file'],
            rev='?rev=' + str(doc['revision']) if doc['revision'] > 0 else '',
            line=doc['line'],
        )

    @staticmethod
    def _strip_blanks(lines):
        first = 0
        for line in lines:
            if not line.lstrip().rstrip():
                first += 1
            else:
                break
        last = 0
        for line in reversed(lines):
            if not line.lstrip().rstrip():
                last += 1
            else:
                break
        lines = lines[first : (len(lines) - last)]
        lines = [x.rstrip().expandtabs(4) for x in lines]
        indent = 10000
        for line in lines:
            if line:
                indent = min(indent, len(line) - len(line.lstrip()))

        if indent > 0:
            lines = [x.replace(' ' * indent, '', 1) for x in lines]

        return lines

    # Conditionally removed formatting.
    # Code blocks are not modified
    @staticmethod
    def _remove_formatting_markdown(lines):
        code = False
        new_paragraph = True
        backtick_block = False

        res = []
        for line in lines:
            if new_paragraph:
                if line.startswith('\t') or line.startswith(' ' * 4):
                    code = True

            if not line.startswith('\t') and not line.startswith(' ' * 4):
                code = False

            new_paragraph = not line

            if line.lstrip() == '```':
                backtick_block = not backtick_block

            res.append(line if code or backtick_block else _Markdown._remove_formatting(line))
        return res

    # Unconditionally removes formatting due to '_'/'*'
    @staticmethod
    def _remove_formatting(x):
        return x.replace("_", r"\_").replace("*", r"\*")


def _gen(
    custom_build_directory,
    build_type,
    build_targets,
    debug_options,
    flags=None,
    warn_mode=None,
    ymake_bin=None,
    platform=None,
    host_platform=None,
    target_platforms=None,
    **kwargs
):
    generation_conf = gen_conf(
        build_root=custom_build_directory,
        build_type=build_type,
        build_targets=build_targets,
        flags=flags,
        host_platform=host_platform,
        target_platforms=target_platforms,
    )
    res, evlog_dump = build.ymake2.ymake_dump(
        custom_build_directory=custom_build_directory,
        build_type=build_type,
        abs_targets=build_targets,
        debug_options=debug_options,
        warn_mode=warn_mode,
        flags=flags,
        ymake_bin=ymake_bin,
        platform=platform,
        grab_stderr=True,
        custom_conf=generation_conf,
        **kwargs
    )
    return res


def dump_mmm_docs(
    build_root,
    build_type,
    build_targets,
    debug_options,
    flags,
    dump_all_conf_docs=None,
    conf_docs_json=None,
    ymake_bin=None,
    platform=None,
    host_platform=None,
    target_platforms=None,
):
    json_dump_name = os.path.join(build_root, 'ymake.dump.ydx.json')
    arc_root = core.config.find_root_from(build_targets)
    null_ya_make = os.path.join(arc_root, 'build', 'docs', 'empty')

    if not conf_docs_json:
        if not os.path.exists(null_ya_make):
            raise "Empty project not found, dump conf-docs may work too long"

    res = _gen(
        custom_build_directory=build_root,
        build_type=build_type,
        # Override target
        build_targets=[null_ya_make] if not conf_docs_json else build_targets,
        debug_options=debug_options,
        flags=flags,
        ymake_bin=ymake_bin,
        platform=platform,
        host_platform=host_platform,
        target_platforms=target_platforms,
        yndex_file=json_dump_name,
    )

    if conf_docs_json:
        with open(json_dump_name, 'r') as jfile:
            res.stdout += jfile.read()
    else:
        with open(json_dump_name, 'r') as jfile:
            contents = jfile.read()
            jdata = json.loads(contents)
        no_svn = True if 'NO_SVN_DEPENDS' in flags and strtobool(flags['NO_SVN_DEPENDS']) else False
        doc = _Markdown(arc_root, dump_all_conf_docs, not no_svn)
        for efile in jdata:
            for entry in jdata[efile]:
                doc.process_entry(efile, entry)
        res.stdout += doc.dump()

    return res
