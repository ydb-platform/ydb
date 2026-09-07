import argparse
import codecs
import errno
import os
import re
import shutil
import sys

_TOC_YAML_RE = re.compile(r'^items\s*:', re.MULTILINE)
_TOC_HREF_RE = re.compile(r'\bhref\s*:\s*(\S+\.yaml)', re.MULTILINE)
_META_BLOCK_RE = re.compile(r'^meta\s*:\s*\n', re.MULTILINE)
_META_ANY_RE = re.compile(r'^meta\s*:', re.MULTILINE)

# Explicitly enable local imports
# Don't forget to add imported scripts to inputs of the calling command!
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import process_command_files as pcf  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--build-root', required=True)
    parser.add_argument('--dst-dir', required=True)
    parser.add_argument('--existing', choices=('skip', 'overwrite'), default='overwrite')
    parser.add_argument('--source-root', required=True)
    parser.add_argument('--src-dir', required=None)
    parser.add_argument('files', nargs='*')
    return parser.parse_args(pcf.get_args(sys.argv[1:]))


def makedirs(dirname):
    try:
        os.makedirs(dirname)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(dirname):
            pass
        else:
            raise


def collect_leading_pages(src_files):
    """Scan toc yaml files among src_files (those with top-level 'items:') and
    return a set of normalized absolute source paths referenced via 'href: *.yaml'."""
    leading_pages = set()
    for src in src_files:
        if not src.endswith('.yaml'):
            continue
        try:
            with open(src, 'rb') as f:
                content = f.read().decode('utf-8', errors='replace')
        except OSError:
            continue
        if not _TOC_YAML_RE.search(content):
            continue
        toc_dir = os.path.dirname(src)
        for m in _TOC_HREF_RE.finditer(content):
            href = m.group(1).strip('\'"')
            if href:
                leading_pages.add(os.path.normpath(os.path.join(toc_dir, href)))
    return leading_pages


def copy_file(src, dst, overwrite=False, orig_path=None, generated=False, leading_pages=None):
    if os.path.exists(dst) and not overwrite:
        return

    makedirs(os.path.dirname(dst))

    with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
        if (orig_path or generated) and src.endswith('.md'):
            out = b''
            buf = fsrc.readline()
            bom_length = len(codecs.BOM_UTF8)
            if buf[:bom_length] == codecs.BOM_UTF8:
                out += codecs.BOM_UTF8
                buf = buf[bom_length:]
            info = 'generated: true\n' if generated else 'vcsPath: {}\n'.format(orig_path)
            if buf.startswith(b'---') and b'\n' in buf[3:] and buf[3:].rstrip(b'\r\n') == b'':
                content = b''
                found = False
                while True:
                    line = fsrc.readline()
                    if len(line) == 0:
                        break
                    content += line
                    if line.startswith(b'---') and line[3:].rstrip(b'\r\n') == b'':
                        found = True
                        break
                out += buf
                if found:
                    out += info.encode('utf-8')
                out += content
            else:
                out += '---\n{}---\n'.format(info).encode('utf-8')
                out += buf
            fdst.write(out)
        elif orig_path and src.endswith('.yaml') and os.path.normpath(src) in (leading_pages or set()):
            raw = fsrc.read()
            bom = b''
            if raw[: len(codecs.BOM_UTF8)] == codecs.BOM_UTF8:
                bom = codecs.BOM_UTF8
                raw = raw[len(codecs.BOM_UTF8) :]
            content = raw.decode('utf-8')
            meta_block = _META_BLOCK_RE.search(content)
            if meta_block:
                rest = content[meta_block.end() :]
                indent_match = re.match(r'^([ \t]+)\S', rest)
                indent = indent_match.group(1) if indent_match else '  '
                already_has_vcspath = re.search(r'^\s+vcsPath\s*:', content, re.MULTILINE)
                if not re.match(r'^[ \t]+-', rest) and not already_has_vcspath:
                    vcs_line = '{}vcsPath: {}\n'.format(indent, orig_path)
                    content = content[: meta_block.end()] + vcs_line + content[meta_block.end() :]
                # else: meta is a sequence, or vcsPath already present — skip injection
            elif not _META_ANY_RE.search(content):
                meta_prefix = 'meta:\n  vcsPath: {}\n'.format(orig_path)
                if content.startswith('---\n') or content.startswith('---\r\n'):
                    sep_end = content.index('\n') + 1
                    content = content[:sep_end] + meta_prefix + content[sep_end:]
                else:
                    content = meta_prefix + content
            # else: flow-style meta (e.g. "meta: {noIndex: true}") — skip injection
            # to avoid a duplicate key that would break js-yaml
            fdst.write(bom + content.encode('utf-8'))
        shutil.copyfileobj(fsrc, fdst)


def main():
    args = parse_args()

    source_root = os.path.normpath(args.source_root) + os.path.sep
    build_root = os.path.normpath(args.build_root) + os.path.sep

    dst_dir = os.path.normpath(args.dst_dir)
    assert dst_dir.startswith(build_root)
    makedirs(dst_dir)

    src_dir = os.path.normpath(args.src_dir) + os.path.sep

    if src_dir.startswith(source_root):
        root = source_root
        is_from_source_root = True
    elif src_dir.startswith(build_root):
        root = build_root
        is_from_source_root = False
    else:
        assert False, 'src_dir [{}] should start with [{}] or [{}]'.format(src_dir, source_root, build_root)

    is_overwrite_existing = args.existing == 'overwrite'

    src_files = [os.path.normpath(os.path.join(src_dir, f)) for f in args.files]
    leading_pages = collect_leading_pages(src_files) if is_from_source_root else set()

    for src_file in src_files:
        dst_file = os.path.join(dst_dir, src_file[len(src_dir) :])
        if src_file == dst_file:
            continue
        rel_path = src_file[len(root) :] if is_from_source_root else None
        copy_file(src_file, dst_file, overwrite=is_overwrite_existing, orig_path=rel_path, leading_pages=leading_pages)


if __name__ == '__main__':
    main()
