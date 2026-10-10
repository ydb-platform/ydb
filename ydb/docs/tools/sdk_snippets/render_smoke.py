"""Verify every SDK fragment in real documentation pages and both build formats."""

import re
import os
import json
import shutil
import subprocess
import sys
import tempfile
import textwrap
from collections import defaultdict
from html.parser import HTMLParser
from pathlib import Path

import yaml

from cli import STAGING_ROOT, directives, read_yaml, regions
from clean_output import clean


def canonical(value):
    return textwrap.dedent(value).expandtabs(4).strip('\n')


def fences(markdown):
    result = []
    lines = markdown.splitlines()
    opened = None
    body = []
    for line in lines:
        match = re.match(r'^([ \t]*)(`{3,})([^`]*)$', line)
        if opened is None:
            if match:
                opened = (match[1], match[2], match[3].strip())
                body = []
        elif match and match[2] == opened[1] and not match[3].strip():
            result.append((opened[2], canonical('\n'.join(body))))
            opened = None
        else:
            prefix = opened[0]
            body.append(line[len(prefix):] if line.startswith(prefix) else line)
    if opened:
        raise RuntimeError('Unclosed rendered code fence')
    return result


class CodeBlocks(HTMLParser):
    def __init__(self):
        super().__init__()
        self.blocks = []
        self.panels = []
        self.stack = []
        self.body = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'div':
            self.stack.append('yfm-tab-panel' in attrs.get('class', '').split())
        elif tag == 'pre':
            self.body = []
            self.panels.append(sum(self.stack))

    def handle_endtag(self, tag):
        if tag == 'div' and self.stack:
            self.stack.pop()
        elif tag == 'pre' and self.body is not None:
            self.blocks.append(canonical(''.join(self.body)))
            self.body = None

    def handle_data(self, data):
        if self.body is not None:
            self.body.append(data)


def compare(expected, actual, page, format):
    if len(expected) != len(actual):
        raise RuntimeError(f'{page}: {format} contains {len(actual)} blocks, expected {len(expected)}')
    for index, (before, after) in enumerate(zip(expected, actual), 1):
        if before != after:
            raise RuntimeError(f'{page}: {format} fragment {index} differs from its source\n'
                               f'Expected: {before!r}\nActual: {after!r}')


def main():
    cli = sys.argv[1]
    docs = Path(__file__).resolve().parents[2]
    lock = read_yaml(docs / 'sdk-snippets.lock.yaml')
    by_page = defaultdict(list)
    for page, line, path, attrs in directives(docs):
        by_page[page].append((line, path, attrs))
    source_regions = {}
    for sdk, entry in lock['sources'].items():
        for path in entry['files']:
            key = sdk + '/' + path
            source_regions[key] = regions((docs / STAGING_ROOT / key).read_text(), key)
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory).resolve()
        source = root / 'input'
        source.mkdir()
        config = {'allowCustomResources': True, 'strict': True,
                  'ignore': [STAGING_ROOT + '/**'],
                  'resources': {'style': ['_assets/nested-tabs.css']}}
        (source / '.yfm').write_text(yaml.safe_dump(config))
        shutil.copy(docs / '.yfmlint', source / '.yfmlint')
        (source / '_assets').mkdir()
        shutil.copy(docs / '_assets/nested-tabs.css', source / '_assets/nested-tabs.css')
        shutil.copytree(docs / STAGING_ROOT, source / STAGING_ROOT)
        expected_pages = {}
        items = []
        for index, (page, refs) in enumerate(by_page.items()):
            relative = f'pages/{index}.md'
            original = page.read_text().splitlines()
            resolved = original.copy()
            for line, path, attrs in refs:
                name = re.fullmatch(r'\[BEGIN (\w+)\]-\[END \1\]', attrs['lines'])
                if not name:
                    raise RuntimeError(f'{page}:{line}: expected one named region')
                content = textwrap.dedent(source_regions[path][name[1]])
                indent = re.match(r'[ \t]*', original[line - 1])[0]
                resolved[line - 1] = '\n'.join(indent + part for part in
                    [f"```{attrs['lang']}", *content.splitlines(), '```'])
            expected_pages[relative] = fences('\n'.join(resolved))
            # Keep the actual article structure. External links and unrelated
            # includes are outside this fixture's scope; SDK directives stay intact.
            content = '\n'.join(original).replace('{{ ydb-short-name }}', 'YDB')
            content = re.sub(r'\{%\s*include\b.*?%\}', 'This functionality is not supported.', content)
            content = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', content)
            target = source / relative
            target.parent.mkdir(exist_ok=True)
            target.write_text(content + '\n')
            items.append({'name': page.relative_to(docs).as_posix(), 'href': relative})
        (source / 'toc.yaml').write_text(yaml.safe_dump({'title': 'SDK snippets', 'items': items}))
        for format in ('html', 'md'):
            output = root / format
            subprocess.run([cli, 'build', '-i', str(source), '-o', str(output),
                            '--output-format', format, '--allow-custom-resources', '--strict'], check=True)
            clean(output)
            if os.environ.get('SDK_SNIPPETS_SMOKE_OUTPUT'):
                shutil.copytree(output, Path(os.environ['SDK_SNIPPETS_SMOKE_OUTPUT']) / format,
                                dirs_exist_ok=True)
            for relative, expected in expected_pages.items():
                target = output / Path(relative).with_suffix('.html' if format == 'html' else '.md')
                rendered = target.read_text()
                if '{% code' in rendered or '[BEGIN ' in rendered or '[END ' in rendered:
                    raise RuntimeError(f'{relative}: unexpanded directive or marker in {format}')
                if format == 'md':
                    compare(expected, fences(rendered), relative, format)
                else:
                    state = re.search(r'<script type="application/json" id="diplodoc-state">(.*?)</script>',
                                      rendered, re.S)
                    if state:
                        payload = state[1].replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
                        rendered = json.loads(payload)['data']['html']
                    parser = CodeBlocks()
                    parser.feed(rendered)
                    compare([code for _, code in expected], parser.blocks, relative, format)
                    if any(depth == 0 for depth in parser.panels):
                        raise RuntimeError(f'{relative}: a code block escaped its language tabs')
            if any(path.is_file() for path in (output / STAGING_ROOT).rglob('*')):
                raise RuntimeError('SDK staging files were published in ' + format)
    count = sum(len(blocks) for blocks in expected_pages.values())
    print(f'HTML and Markdown match all {count} fragments in {len(expected_pages)} real documentation pages')


if __name__ == '__main__':
    main()
