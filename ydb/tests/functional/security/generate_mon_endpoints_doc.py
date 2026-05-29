#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate human-readable Markdown from test_mon_endpoints_auth canon data.

Reads canonical JSON (method -> path -> query -> token -> status) and produces
tables grouped by current access level.

Usage:
    python3 generate_mon_endpoints_doc.py
    python3 generate_mon_endpoints_doc.py --canon path/to/mon_endpoints_auth-enforce_user_token_enabled.json
    python3 generate_mon_endpoints_doc.py --output mon_endpoints_auth.md --all-queries
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[3]

DEFAULT_CANON = (
    SCRIPT_DIR
    / 'canondata'
    / 'test_mon_endpoints_auth.test_mon_endpoints_auth-enforce_user_token_enabled'
    / 'mon_endpoints_auth-enforce_user_token_enabled.json'
)

DEFAULT_OUTPUT = SCRIPT_DIR / 'mon_endpoints_auth.md'

VIEWER_CPP = REPO_ROOT / 'ydb/core/viewer/viewer.cpp'
AUDIT_DENYLIST_CPP = REPO_ROOT / 'ydb/core/mon/audit/audit_denylist.cpp'

TOKEN_ORDER = [
    ('__none__', 'public'),
    ('database@builtin', 'database'),
    ('viewer@builtin', 'viewer'),
    ('monitoring@builtin', 'monitoring'),
    ('root@builtin', 'admin'),
]

GROUPS = [
    ('public', 'Группа 1 — public (no token)'),
    ('database', 'Группа 2 — database'),
    ('viewer', 'Группа 3 — viewer'),
    ('monitoring', 'Группа 4 — monitoring'),
    ('unavailable', 'Группа 5 — unavailable'),
]

ACCESS_LEVEL_RANK = {
    'public': 0,
    'database': 1,
    'viewer': 2,
    'monitoring': 3,
    'admin': 4,
    'unavailable': 5,
}

LEVEL_LABELS = {
    'public': 'без токена',
    'database': 'database@builtin',
    'viewer': 'viewer@builtin',
    'monitoring': 'monitoring@builtin',
    'admin': 'root@builtin',
}

# Target access level overrides (planned policy, not always matching current HTTP behavior).
TARGET_LEVEL_OVERRIDES: dict[str, str] = {}

# Mon pages with AuthMode::Disabled in viewer.cpp (target: public without token).
PUBLIC_TARGET_PATHS = {
    '/viewer/capabilities',
    '/monitoring/',
}

# Paths that cannot be raised to monitoring_allowed_sids in the target model (stay at lower level).
CANNOT_RAISE_TO_MONITORING_PREFIXES = (
    '/viewer/acl',
    '/viewer/describe',
    '/scheme',
    '/query',
    '/operation',
    '/storage',
)

STATIC_PREFIXES = ('/static/', '/lwtrace/mon/static')
STATIC_EXACT = {
    '/jquery.tablesorter.js',
    '/jquery.tablesorter.css',
}

AUDIT_EXCEPTION_NO_LOG = {
    '/internal': 'псевдостатика; нет смысла логировать',
}

AUDIT_FORCE_LOG_PATHS = {
    '/viewer/acl',
    '/viewer/describe',
    '/viewer/json/acl',
    '/viewer/json/describe',
}


@dataclass(frozen=True)
class Row:
    endpoint: str
    method: str
    query: str
    current_level: str
    target_level: str
    audited: bool
    comment: str

    @property
    def group(self) -> str:
        if self.current_level == 'admin':
            return 'monitoring'
        return self.current_level


def _is_valid_access(code: int) -> bool:
    return 200 <= code < 300 or code == 400


def _uniform_statuses(status_by_token: dict[str, int]) -> bool:
    values = list(status_by_token.values())
    return bool(values) and len(set(values)) == 1


def current_access_level(status_by_token: dict[str, int]) -> str:
    if _uniform_statuses(status_by_token):
        code = next(iter(status_by_token.values()))
        if code in (404, 405):
            return 'public'
    for token, level in TOKEN_ORDER:
        code = status_by_token.get(token)
        if code is not None and _is_valid_access(code):
            return level
    return 'unavailable'


def _access_level_comment(status_by_token: dict[str, int], access_level: str) -> str:
    if access_level == 'unavailable':
        return 'нет валидного доступа (2xx или 400)'

    if _uniform_statuses(status_by_token):
        code = next(iter(status_by_token.values()))
        if code in (404, 405):
            return f'одинаковый {code} для всех токенов'

    token = next((token for token, level in TOKEN_ORDER if level == access_level), None)
    if token is None:
        return ''

    return f'{LEVEL_LABELS[access_level]}: {status_by_token[token]}'


def _parse_viewer_endpoint_access(viewer_cpp: Path) -> dict[str, str]:
    if not viewer_cpp.is_file():
        return {}
    text = viewer_cpp.read_text(encoding='utf-8')
    mapping = {
        'Administration': 'admin',
        'Viewer': 'viewer',
        'Database': 'database',
    }
    result: dict[str, str] = {}
    pattern = re.compile(
        r'\{"(/[^"]+)",\s*\{EViewerEndpointAccessType::(\w+)',
    )
    for path, access_type in pattern.findall(text):
        if access_type in mapping:
            result[path] = mapping[access_type]
    return result


def _parse_audit_denylist(cpp_path: Path) -> list[tuple[str, bool]]:
    if not cpp_path.is_file():
        return []
    text = cpp_path.read_text(encoding='utf-8')
    pattern = re.compile(
        r'\{\s*\.Path\s*=\s*"([^"]+)"(?:,\s*\.Recursive\s*=\s*(true|false))?\s*\}',
    )
    entries: list[tuple[str, bool]] = []
    for path, recursive in pattern.findall(text):
        entries.append((path, recursive == 'true'))
    return entries


def _path_in_denylist(path: str, denylist: list[tuple[str, bool]]) -> str | None:
    for pattern, recursive in denylist:
        if recursive:
            if path == pattern or path.startswith(pattern + '/'):
                return pattern
        elif path == pattern:
            return pattern
    return None


def _is_static(path: str) -> bool:
    if path in STATIC_EXACT:
        return True
    return any(path.startswith(prefix) for prefix in STATIC_PREFIXES)


def _target_access_level(path: str, viewer_access: dict[str, str]) -> str:
    if path in TARGET_LEVEL_OVERRIDES:
        return TARGET_LEVEL_OVERRIDES[path]
    if path in PUBLIC_TARGET_PATHS:
        return 'public'
    if path in viewer_access:
        return viewer_access[path]
    if path.startswith('/viewer/') or path == '/viewer':
        return 'database'
    if path.startswith('/actors/') or path in ('/cms', '/grpc', '/trace', '/nodetabmon', '/tablets', '/tablet'):
        return 'monitoring'
    if path.startswith('/memory/'):
        return 'monitoring'
    if path.startswith('/fq_diag/'):
        return 'monitoring'
    if path.startswith('/vdisk') or path.startswith('/pdisk'):
        return 'viewer'
    if path.startswith('/healthcheck'):
        return 'monitoring'
    if path.startswith('/counters'):
        return 'public'
    if path in ('/ping', '/status', '/ver', '/login', '/followercounters', '/labeledcounters'):
        return 'public'
    if path.startswith('/monitoring'):
        return 'public'
    if path.startswith('/internal'):
        return 'monitoring'
    if path.startswith('/node/'):
        return 'monitoring'
    if any(path.startswith(prefix) for prefix in CANNOT_RAISE_TO_MONITORING_PREFIXES):
        return 'database'
    return 'monitoring'


def _target_audited(method: str, path: str, target_level: str, denylist: list[tuple[str, bool]]) -> bool:
    path_only = path.split('?')[0]

    if method == 'OPTIONS':
        return False

    if method in ('POST', 'PUT', 'DELETE'):
        return True

    if path_only in AUDIT_FORCE_LOG_PATHS:
        return True

    if path_only in AUDIT_EXCEPTION_NO_LOG:
        return False

    if _is_static(path_only):
        return False

    deny_pattern = _path_in_denylist(path_only, denylist)
    if deny_pattern is not None:
        return False

    if target_level in ('monitoring', 'admin'):
        return True

    return True


def _format_endpoint(path: str, query: str) -> str:
    if not query:
        return path
    return f'{path}{query}'


def _load_canon(path: Path) -> dict:
    with path.open(encoding='utf-8') as f:
        return json.load(f)


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _iter_rows(
    canon: dict,
    *,
    all_queries: bool,
    viewer_access: dict[str, str],
    denylist: list[tuple[str, bool]],
) -> list[Row]:
    rows: list[Row] = []
    for method, paths in sorted(canon.items()):
        for path, queries in sorted(paths.items()):
            query_items = sorted(queries.items())
            if not all_queries:
                query_items = [
                    min(
                        query_items,
                        key=lambda item: (
                            ACCESS_LEVEL_RANK[current_access_level(item[1])],
                            item[0] != '',
                            item[0],
                        ),
                    )
                ]
            for query, status_by_token in query_items:
                current = current_access_level(status_by_token)
                target = _target_access_level(path, viewer_access)
                audited = _target_audited(method, path, target, denylist)
                rows.append(
                    Row(
                        endpoint=_format_endpoint(path, query),
                        method=method,
                        query=query,
                        current_level=current,
                        target_level=target,
                        audited=audited,
                        comment=_access_level_comment(status_by_token, current),
                    )
                )
    rows.sort(key=lambda r: (r.group, r.endpoint, r.method, r.query))
    return rows


def _audit_cell(audited: bool) -> str:
    return 'логируется' if audited else 'не логируется'


def _render_markdown(rows: list[Row], canon_path: Path) -> str:
    lines: list[str] = [
        '# Monitoring HTTP endpoints',
        '',
        'Документ сгенерирован скриптом `generate_mon_endpoints_doc.py` из канонических данных '
        '`test_mon_endpoints_auth` (режим `enforce_user_token_enabled`).',
        '',
        f'Источник: `{_display_path(canon_path)}`',
        '',
        '## Правила аудит-логирования (целевое состояние)',
        '',
        '- Все модифицирующие методы, кроме OPTIONS, аудируются.',
        '- Все запросы уровня `monitoring_allowed_sids` и `admin_allowed_sids` становятся аудируемыми, '
        'кроме статики.',
        '- Ограниченный набор эндпойнтов, у которых нельзя поднять уровень до `monitoring_allowed_sids`.',
        '- Обращения в `/viewer/acl`, `/viewer/describe` к объектам без схемных прав становятся аудируемыми '
        '(решение по внутреннему отказу, не по внешнему HTTP-коду).',
        '- Исключения: `/internal` (псевдостатика).',
        '',
    ]

    by_group: dict[str, list[Row]] = {key: [] for key, _ in GROUPS}
    for row in rows:
        by_group.setdefault(row.group, []).append(row)

    for group_key, group_title in GROUPS:
        group_rows = by_group.get(group_key, [])
        lines.append(f'## {group_title}')
        lines.append('')
        if not group_rows:
            lines.append('_Нет эндпойнтов._')
            lines.append('')
            continue
        lines.append(
            '| Endpoint | Метод | Текущий уровень | Аудит лог | Комментарий |'
        )
        lines.append('| --- | --- | --- | --- | --- |')
        for row in group_rows:
            lines.append(
                '| `{endpoint}` | {method} | {current} | {audit} | {comment} |'.format(
                    endpoint=row.endpoint.replace('|', '\\|'),
                    method=row.method,
                    current=row.current_level,
                    audit=_audit_cell(row.audited),
                    comment=row.comment.replace('|', '\\|'),
                )
            )
        lines.append('')

    return '\n'.join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--canon',
        type=Path,
        default=DEFAULT_CANON,
        help=f'Path to canonical JSON (default: {DEFAULT_CANON})',
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f'Output Markdown path (default: {DEFAULT_OUTPUT})',
    )
    parser.add_argument(
        '--all-queries',
        action='store_true',
        help='Include all query-string variants (default: only empty query)',
    )
    args = parser.parse_args()

    if not args.canon.is_file():
        raise SystemExit(f'Canon file not found: {args.canon}')

    canon = _load_canon(args.canon)
    viewer_access = _parse_viewer_endpoint_access(VIEWER_CPP)
    denylist = _parse_audit_denylist(AUDIT_DENYLIST_CPP)
    rows = _iter_rows(
        canon,
        all_queries=args.all_queries,
        viewer_access=viewer_access,
        denylist=denylist,
    )
    markdown = _render_markdown(rows, args.canon)
    args.output.write_text(markdown, encoding='utf-8')
    print(f'Wrote {len(rows)} rows to {args.output}')


if __name__ == '__main__':
    main()
