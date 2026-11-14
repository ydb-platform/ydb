"""Prepare the opt-in WITH documentation overlay in a separate checkout-free tree."""

import argparse
import shutil
from pathlib import Path


def prepare(repo_root: Path, output: Path) -> None:
    repo_root = repo_root.resolve()
    output = output.resolve()
    if output == repo_root or repo_root in output.parents or output in repo_root.parents:
        raise ValueError('Output must be outside the source checkout')
    if output.exists():
        raise FileExistsError(f'Output already exists: {output}')
    docs = repo_root / 'ydb/docs'
    pages = {}
    for lang in ('ru', 'en'):
        common = (repo_root / f'yql/essentials/docs/{lang}/syntax/select/with.md').read_text()
        wrapper = (docs / f'tools/yql_overlay/overlays/{lang}/with.md').read_text()
        if wrapper.count('{% include [common](_yql_with.md) %}') != 1:
            raise ValueError(f'{lang}: wrapper must include the common page exactly once')
        # YDB documents EACH on the SELECT overview; concat.md is not published.
        # Keep this adaptation local: never edit the imported YQL source.
        common = common.replace('](#each)', '](index.md)')
        common = common.replace('](concat.md#each)', '](index.md)')
        pages[lang] = (common, wrapper)

    shutil.copytree(docs, output, ignore=shutil.ignore_patterns('__pycache__', '.DS_Store'))
    for lang, (common, wrapper) in pages.items():
        folder = output / lang / 'core/yql/reference/syntax/select'
        (folder / '_yql_with.md').write_text(common)
        (folder / 'with.md').write_text(wrapper)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root', type=Path, default=Path(__file__).resolve().parents[4])
    parser.add_argument('--output', type=Path, required=True, help='New directory outside the checkout')
    args = parser.parse_args()
    prepare(args.repo_root, args.output)
    print(f'Prepared RU/EN WITH overlay in {args.output}')


if __name__ == '__main__':
    main()
