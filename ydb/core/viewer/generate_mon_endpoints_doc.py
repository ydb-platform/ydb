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
REPO_ROOT = SCRIPT_DIR.parents[2]

DEFAULT_CANON = (
    REPO_ROOT
    / 'ydb/tests/functional/security/canondata'
    / 'test_mon_endpoints_auth.test_mon_endpoints_auth-enforce_user_token_enabled'
    / 'mon_endpoints_auth-enforce_user_token_enabled.json'
)

DEFAULT_OUTPUT = SCRIPT_DIR / 'mon_endpoints_auth.md'

VIEWER_DIR = SCRIPT_DIR
VIEWER_CPP = VIEWER_DIR / 'viewer.cpp'
AUDIT_DENYLIST_CPP = REPO_ROOT / 'ydb/core/mon/audit/audit_denylist.cpp'

CHECK_ACCESS_PATTERN = re.compile(
    r'CheckAccess(Monitoring|Viewer|Administration)\s*\(',
)
ADD_HANDLER_PATTERN = re.compile(
    r'AddHandler\("([^"]+)"\s*,\s*new\s+T(?:Json|Http)Handler<(\w+)>',
)
CLASS_NAME_PATTERN = re.compile(r'\bclass\s+(T[A-Za-z0-9_]+)')

POLICY_CHECK_LABEL = {
    'monitoring': 'CheckAccessMonitoring',
    'viewer': 'CheckAccessViewer',
    'admin': 'CheckAccessAdministration',
}

TOKEN_ORDER = [
    ('__none__', 'anonymus'),
    ('user@builtin', 'public'),
    ('database@builtin', 'database'),
    ('viewer@builtin', 'viewer'),
    ('monitoring@builtin', 'monitoring'),
    ('root@builtin', 'admin'),
]

GROUPS = [
    ('anonymus', 'Anonymus (no token)'),
    ('public', 'Public'),
    ('database', 'Database'),
    ('viewer', 'Viewer'),
    ('monitoring', 'Monitoring'),
    ('admin', 'Admin'),
]

ACCESS_LEVEL_RANK = {
    'anonymus': 0,
    'public': 1,
    'database': 2,
    'viewer': 3,
    'monitoring': 4,
    'admin': 5,
    'unavailable': 6,
}

LEVEL_LABELS = {
    'anonymus': 'без токена',
    'public': 'user@builtin',
    'database': 'database@builtin',
    'viewer': 'viewer@builtin',
    'monitoring': 'monitoring@builtin',
    'admin': 'root@builtin',
}

@dataclass(frozen=True)
class Row:
    endpoint: str
    method: str
    query: str
    access_level: str
    audited: bool

    @property
    def group(self) -> str:
        return self.access_level


def _is_success(code: int) -> bool:
    return 200 <= code < 300


def _reaches_handler(code: int | None) -> bool:
    if code is None:
        return False
    return _is_success(code) or code == 400


def _uniform_statuses(status_by_token: dict[str, int]) -> bool:
    values = list(status_by_token.values())
    return bool(values) and len(set(values)) == 1


def _canon_success_level(status_by_token: dict[str, int]) -> str | None:
    for _token, level in TOKEN_ORDER:
        code = status_by_token.get(_token)
        if code is not None and _is_success(code):
            return level
    return None


def _canon_handler_reach_level(status_by_token: dict[str, int]) -> str | None:
    """Lowest token that reached handler logic (2xx or 400), not blocked at 401/403."""
    for _token, level in TOKEN_ORDER:
        code = status_by_token.get(_token)
        if _reaches_handler(code):
            return level
    return None


def _build_class_to_header(viewer_dir: Path) -> dict[str, Path]:
    mapping: dict[str, Path] = {}
    if not viewer_dir.is_dir():
        return mapping
    for header in viewer_dir.glob('*.h'):
        text = header.read_text(encoding='utf-8')
        for match in CLASS_NAME_PATTERN.finditer(text):
            mapping.setdefault(match.group(1), header)
    return mapping


def _strictest_policy_in_header(header: Path) -> str | None:
    text = header.read_text(encoding='utf-8')
    checks = CHECK_ACCESS_PATTERN.findall(text)
    if not checks:
        return None
    for check_name in ('Administration', 'Monitoring', 'Viewer'):
        if check_name in checks:
            return {
                'Administration': 'admin',
                'Monitoring': 'monitoring',
                'Viewer': 'viewer',
            }[check_name]
    return None


def _parse_handler_policy_levels(viewer_dir: Path) -> dict[str, str]:
    class_to_header = _build_class_to_header(viewer_dir)
    path_to_class: dict[str, str] = {}
    if viewer_dir.is_dir():
        for cpp in viewer_dir.glob('json_handlers*.cpp'):
            text = cpp.read_text(encoding='utf-8')
            for path, class_name in ADD_HANDLER_PATTERN.findall(text):
                path_to_class[path] = class_name

    policy: dict[str, str] = {}
    for path, class_name in path_to_class.items():
        header = class_to_header.get(class_name)
        if header is None:
            continue
        level = _strictest_policy_in_header(header)
        if level is not None:
            policy[path] = level

    # /healthcheck is registered as a mon page, same handler family as /viewer/healthcheck.
    if '/viewer/healthcheck' in policy:
        policy.setdefault('/healthcheck', policy['/viewer/healthcheck'])
    return policy


def resolve_access_level(
    path: str,
    status_by_token: dict[str, int],
    *,
    policy_levels: dict[str, str],
    viewer_access: dict[str, str],
) -> tuple[str, str]:
    """Return (access_level, comment).

    For anonymus/public/database endpoints, 400 still means the request passed
    the access check and reached handler validation. For viewer+ endpoints, 400
    can be a bad method/body/params before the protected action, so use handler
    policy when there is no successful response.
    """
    if _uniform_statuses(status_by_token):
        code = next(iter(status_by_token.values()))
        if code in (404, 405):
            return 'anonymus', f'одинаковый {code} для всех токенов'

    policy = viewer_access.get(path) or policy_levels.get(path)
    reach = _canon_handler_reach_level(status_by_token)

    if reach is not None and ACCESS_LEVEL_RANK.get(reach, 99) < ACCESS_LEVEL_RANK['viewer']:
        token = next(t for t, lvl in TOKEN_ORDER if lvl == reach)
        code = status_by_token[token]
        comment = f'{LEVEL_LABELS[reach]}: {code} — запрос дошёл до handler'
        if policy is not None and ACCESS_LEVEL_RANK.get(policy, 99) <= ACCESS_LEVEL_RANK[reach]:
            comment += f'; политика: {POLICY_CHECK_LABEL.get(policy, policy)}'
        return reach, comment

    success = _canon_success_level(status_by_token)
    if success is not None:
        token = next(t for t, lvl in TOKEN_ORDER if lvl == success)
        return success, f'{LEVEL_LABELS[success]}: {status_by_token[token]}'

    if policy is not None and ACCESS_LEVEL_RANK.get(policy, 99) >= ACCESS_LEVEL_RANK['viewer']:
        return policy, _policy_access_comment(policy, status_by_token, reach)

    if reach is not None:
        token = next(t for t, lvl in TOKEN_ORDER if lvl == reach)
        code = status_by_token[token]
        if code == 400:
            return reach, (
                f'{LEVEL_LABELS[reach]}: 400 — запрос дошёл до handler, '
                f'но ответ bad request'
            )
        return reach, f'{LEVEL_LABELS[reach]}: {code}'

    if policy is not None:
        return policy, _policy_access_comment(policy, status_by_token, reach)

    return 'unavailable', 'нет успешного доступа (2xx) и нет политики handler'


def _policy_access_comment(
    policy: str,
    status_by_token: dict[str, int],
    reach: str | None,
) -> str:
    label = POLICY_CHECK_LABEL.get(policy, policy)
    parts = [f'политика: {label}']
    canon_bits: list[str] = []
    for _token, level in TOKEN_ORDER:
        if ACCESS_LEVEL_RANK.get(level, 99) < ACCESS_LEVEL_RANK['viewer']:
            continue
        code = status_by_token.get(_token)
        if code is None:
            continue
        canon_bits.append(f'{LEVEL_LABELS[level]}: {code}')
    if canon_bits:
        parts.append('canon: ' + ', '.join(canon_bits))
    if reach is not None and reach != policy:
        parts.append(
            f'canon доходит до handler с {LEVEL_LABELS[reach]} (400 ≠ уровень доступа)'
        )
    return '; '.join(parts)


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


def _path_in_denylist(path: str, denylist: list[tuple[str, bool]]) -> bool:
    for pattern, recursive in denylist:
        if recursive:
            if path == pattern or path.startswith(pattern.rstrip('/') + '/'):
                return True
        elif path == pattern:
            return True
    return False


def _audited(method: str, path: str, denylist: list[tuple[str, bool]]) -> bool:
    path_only = path.split('?')[0]

    if method in ('POST', 'PUT', 'DELETE'):
        return True

    if method == 'OPTIONS':
        return False

    return not _path_in_denylist(path_only, denylist)


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
    policy_levels: dict[str, str],
    denylist: list[tuple[str, bool]],
) -> list[Row]:
    rows: list[Row] = []

    def _resolved_level(status_by_token: dict[str, int], endpoint_path: str) -> str:
        return resolve_access_level(
            endpoint_path,
            status_by_token,
            policy_levels=policy_levels,
            viewer_access=viewer_access,
        )[0]

    for method, paths in sorted(canon.items()):
        for path, queries in sorted(paths.items()):
            query_items = sorted(queries.items())
            if not all_queries:
                query_items = [
                    min(
                        query_items,
                        key=lambda item: (
                            ACCESS_LEVEL_RANK[_resolved_level(item[1], path)],
                            item[0] != '',
                            item[0],
                        ),
                    )
                ]
            for query, status_by_token in query_items:
                current, _comment = resolve_access_level(
                    path,
                    status_by_token,
                    policy_levels=policy_levels,
                    viewer_access=viewer_access,
                )
                if current == 'unavailable':
                    continue
                audited = _audited(method, path, denylist)
                rows.append(
                    Row(
                        endpoint=_format_endpoint(path, query),
                        method=method,
                        query=query,
                        access_level=current,
                        audited=audited,
                    )
                )
    rows.sort(key=lambda r: (r.group, r.endpoint, r.method, r.query))
    return rows


def _audit_cell(audited: bool) -> str:
    return 'логируется' if audited else 'не логируется'


def _format_denylist(denylist: list[tuple[str, bool]]) -> str:
    entries = [
        f'`{path}{"*" if recursive else ""}`'
        for path, recursive in denylist
    ]
    return ', '.join(entries)


def _render_markdown(rows: list[Row], canon_path: Path, denylist: list[tuple[str, bool]]) -> str:
    lines: list[str] = [
        '# Monitoring HTTP endpoints',
        '',
        'Документ сгенерирован скриптом `generate_mon_endpoints_doc.py` из канонических данных '
        '`test_mon_endpoints_auth` (режим `enforce_user_token_enabled`).',
        '',
        f'Источник: `{_display_path(canon_path)}`',
        '',
        '## Иерархия доступа',
        '',
        'Тестовая конфигурация использует встроенные SID: `database@builtin` в `database_allowed_sids`, '
        '`viewer@builtin` в `viewer_allowed_sids`, `monitoring@builtin` в `monitoring_allowed_sids`, '
        '`root@builtin` в `administration_allowed_sids`.',
        '',
        'Доступ наследуется сверху вниз: каждый следующий уровень включает права всех уровней ниже.',
        '',
        '```text',
        'administration_allowed_sids',
        '  -> monitoring_allowed_sids',
        '     -> viewer_allowed_sids',
        '        -> database_allowed_sids',
        '```',
        '',
        '## Аудит логгирование',
        '',
        'Аудит сейчас определяется denylist из `core/mon/audit/audit_denylist.cpp`:',
        '',
        '- `POST`, `PUT`, `DELETE` логируются всегда.',
        '- `OPTIONS` не логируется.',
        '- Остальные методы логируются, если URL не попал в denylist.',
        f'- Текущий denylist: {_format_denylist(denylist)}.',
        '',
        '## Как скрипт распределяет endpoints по уровням прав',
        '',
        'Скрипт читает canon `test_mon_endpoints_auth`: для каждого `method + path + query` там есть '
        'HTTP-статусы для запросов без токена и с токенами `user@builtin`, `database@builtin`, '
        '`viewer@builtin`, `monitoring@builtin`, `root@builtin`.',
        '',
        'Для каждого `method + path` по умолчанию выбирается один query-вариант: вариант с минимальным '
        'уровнем прав; при равенстве предпочитается пустой query, затем лексикографический порядок. '
        'Опция `--all-queries` выводит все query-варианты.',
        '',
        'Уровень прав endpoint определяется минимальным доступом, с которым запрос проходит access check:',
        '',
        '- Для уровней прав anonymus/public/database статус `2xx` или `400` '
        'означает, что запрос дошёл до handler; `400` здесь считается ошибкой валидации запроса после '
        'пройденного access check.',
        '- Для `viewer_allowed_sids`, `monitoring_allowed_sids` и `administration_allowed_sids` успешным '
        'доступом считается `2xx`. Если `2xx` нет, уровень прав берётся из политики handler или из регистрации '
        'viewer endpoint.',
        '- Endpoints, для которых нет успешного доступа и не найдена политика handler, в документ не выводятся.',
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
        lines.append('| Endpoint | Метод | Аудит лог |')
        lines.append('| --- | --- | --- |')
        for row in group_rows:
            lines.append(
                '| `{endpoint}` | {method} | {audit} |'.format(
                    endpoint=row.endpoint.replace('|', '\\|'),
                    method=row.method,
                    audit=_audit_cell(row.audited),
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
    policy_levels = _parse_handler_policy_levels(VIEWER_DIR)
    denylist = _parse_audit_denylist(AUDIT_DENYLIST_CPP)
    rows = _iter_rows(
        canon,
        all_queries=args.all_queries,
        viewer_access=viewer_access,
        policy_levels=policy_levels,
        denylist=denylist,
    )
    markdown = _render_markdown(rows, args.canon, denylist)
    args.output.write_text(markdown, encoding='utf-8')
    print(f'Wrote {len(rows)} rows to {args.output}')


if __name__ == '__main__':
    main()
