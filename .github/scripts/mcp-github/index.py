import json
import requests
import os
import base64
import io
import zipfile
from typing import Optional, Dict, Any, List, Tuple

REPO = "ydb-platform/ydb"
GITHUB_PAT = os.environ.get('GITHUB_PAT', '')

# Маппинг отображаемых имён workflow → имя файла (GitHub API принимает файл)
WORKFLOW_NAME_TO_FILE = {
    "Regression-run_Small_and_Medium": "regression_run_small_medium.yml",
    "Regression-run_Large": "regression_run_large.yml",
    "Regression-run_stress": "regression_run_stress.yml",
    "Regression-run_compatibility": "regression_run_compatibility.yml",
    "Regression-run": "regression_run.yml",
    "Regression-whitelist-run": "regression_whitelist_run.yml",
    "Whitelist-run": "regression_whitelist_run.yml",
}

# Маппинг build_preset → суффикс platform (ya-{branch}-x86-64{suffix})
BUILD_PRESET_TO_PLATFORM_SUFFIX = {
    "relwithdebinfo": "",
    "release-asan": "-asan",
    "release-tsan": "-tsan",
    "release-msan": "-msan",
    "asan": "-asan",
    "tsan": "-tsan",
    "msan": "-msan",
}

KNOWN_TOOLS = frozenset({
    "get_repo_file", "search_repo_files", "search_repo_content", "get_repo_tree",
    "get_file_info", "get_file_commits", "get_commit_diff", "get_pull_request",
    "get_pull_request_files", "search_pull_requests", "get_issue", "search_issues",
    "get_workflow_runs", "get_workflow_run_jobs", "compare_workflow_failures",
    "get_run_report", "compare_run_reports", "compare_with_previous_run",
    "get_regression_report", "get_run_errors_matrix",
})


def _load_tools_schema() -> Optional[List[Dict[str, Any]]]:
    """Загрузить tools из schema.json (схема для каждой функции)."""
    try:
        base = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(base, "schema.json")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        tools = data.get("tools")
        if tools and "version" in data:
            print(f"Loaded tools schema v{data['version']}, {len(tools)} tools")
        return tools
    except Exception as e:
        print(f"Failed to load schema.json: {e}")
        return None


def handler(event, context):
    """
    MCP (Model Context Protocol) сервер для работы с GitHub API.
    
    Поддерживаемые методы:
    - initialize: инициализация MCP соединения
    - tools/list: список доступных инструментов
    - tools/call: вызов инструмента
    """
    # Логирование для отладки
    print("=" * 60)
    print("MCP handler invoked")
    print(f"Event keys: {list(event.keys())}")
    # Определяем HTTP метод
    http_method = event.get('httpMethod', event.get('requestContext', {}).get('httpMethod', 'GET'))
    print(f"HTTP method: {http_method}")
    
    # Безопасная обработка body (с учетом base64 encoding в Yandex Cloud Functions)
    body = None
    body_str = None
    
    if 'body' in event and event['body']:
        try:
            body_str = event['body']
            print(f"Body type: {type(body_str)}, isBase64Encoded: {event.get('isBase64Encoded', False)}")
            # Декодируем base64, если указан флаг
            if event.get('isBase64Encoded', False):
                body_str = base64.b64decode(body_str).decode('utf-8')
                print("Decoded base64 body")
            
            # Если body уже строка, пытаемся распарсить JSON
            if isinstance(body_str, str):
                if body_str.strip():  # Проверяем, что строка не пустая
                    body = json.loads(body_str)
                    print(f"Parsed JSON body (first 300 chars): {json.dumps(body, ensure_ascii=False)[:300]}")
            elif isinstance(body_str, dict):
                # Если body уже словарь (иногда так бывает)
                body = body_str
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as e:
            # Логируем ошибку для отладки
            print(f"Error parsing body: {e}, body_str type: {type(body_str)}, value: {str(body_str)[:100]}")
            pass

    # Логируем query параметры (если есть)
    query_params = event.get('queryStringParameters') or {}
    if query_params:
        print(f"Query parameters: {query_params}")

    # Если body пустой, пробуем интерпретировать event как прямой вызов инструмента
    # Это бывает, когда инструмент вызывает функцию напрямую с параметрами.
    if not body and isinstance(event, dict):
        direct_keys = {
            'tool_name',
            'name',
            'arguments',
            'params',
            'path',
            'query',
            'extension',
            'recursive',
            'ref',
            'run_id',
            'job_id',
            'job_name',
            'suite',
            'platform',
            'report_url',
            'try_number',
            'base_url',
            'workflow',
            'branch',
            'test_branch',
            'status',
            'build_preset',
            'since',
            'until',
            'branches',
            'build_presets',
        }
        if any(key in event for key in direct_keys):
            # Не путаем HTTP path (например, "/sse") с путём файла
            event_path = event.get('path')
            if isinstance(event_path, str) and event_path.startswith('/'):
                print(f"Detected HTTP path '{event_path}', skipping direct tool inference")
            else:
                tool_name = event.get('tool_name') or event.get('name')
                if not tool_name and 'query' in event:
                    q = (event.get('query') or '').strip().lower().replace(' ', '_')
                    if q in KNOWN_TOOLS:
                        tool_name = q
                if not tool_name:
                    # run_id + suite/workflow → матрица «ветка × конфиг»; иначе run_id → jobs
                    if 'run_id' in event and ('suite' in event or 'workflow' in event) and 'job_name' not in event:
                        tool_name = 'get_run_errors_matrix'
                    elif 'run_id' in event and 'job_name' not in event:
                        tool_name = 'get_workflow_run_jobs'
                    elif 'report_url' in event or ('suite' in event and 'job_id' in event):
                        tool_name = 'get_run_report'
                    elif 'workflow' in event:
                        tool_name = 'get_regression_report'
                    elif event.get('recursive') or 'directory' in event or 'dir' in event:
                        tool_name = 'get_repo_tree'
                    elif 'query' in event:
                        tool_name = 'search_repo_files'
                    elif 'path' in event:
                        tool_name = 'get_repo_file'

                if tool_name:
                    arguments = {}
                    for key in (
                        'path', 'query', 'extension', 'recursive', 'ref',
                        'workflow', 'branch', 'test_branch', 'status', 'build_preset',
                        'since', 'until',
                        'run_id', 'job_id', 'job_name', 'suite', 'platform',
                        'report_url', 'try_number', 'base_url',
                        'branches', 'build_presets',
                    ):
                        if key in event and event.get(key) is not None:
                            arguments[key] = event.get(key)
                    if 'arguments' in event and isinstance(event['arguments'], dict):
                        arguments.update(event['arguments'])
                    if 'params' in event and isinstance(event['params'], dict):
                        arguments.update(event['params'])

                    print(f"Direct tool call inferred: tool_name={tool_name}, arguments={arguments}")
                    body = {
                        'jsonrpc': '2.0',
                        'method': 'tools/call',
                        'params': {
                            'name': tool_name,
                            'arguments': arguments
                        },
                        'id': event.get('id', 1)
                    }
                    print(f"Converted to MCP format (first 300 chars): {json.dumps(body, ensure_ascii=False)[:300]}")

    # Обработка прямых вызовов от Yandex Cloud Agent (не MCP формат)
    if body and not body.get('method') and not body.get('jsonrpc'):
        tool_name = None
        arguments = {}

        if 'tool_name' in body:
            tool_name = body.get('tool_name')
            arguments = body.get('arguments', {})
        elif 'name' in body:
            tool_name = body.get('name')
            arguments = body.get('params', body.get('arguments', {}))
        elif 'path' in body:
            tool_name = 'get_repo_file'
            arguments = {'path': body.get('path')}
            if 'ref' in body:
                arguments['ref'] = body.get('ref')

        if tool_name:
            print(f"Detected Yandex Agent format: tool_name={tool_name}, arguments={arguments}")
            body = {
                'jsonrpc': '2.0',
                'method': 'tools/call',
                'params': {
                    'name': tool_name,
                    'arguments': arguments
                },
                'id': body.get('id', 1)
            }
            print(f"Converted to MCP format (first 300 chars): {json.dumps(body, ensure_ascii=False)[:300]}")
    
    # Если body пустой, отвечаем на healthcheck
    if not body:
        print("Returning healthcheck (empty body)")
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'status': 'ok', 'mcp': True, 'repo': REPO})
        }

    if http_method == 'GET':
        print("Warning: GET with body inferred; continuing to handle as tool call")
    
    # Получаем метод и ID запроса
    method = body.get('method', '')
    request_id = body.get('id', 1)
    print(f"MCP method: {method}, request_id: {request_id}")
    
    # Обработка MCP методов
    if method == 'initialize':
        print("Handling initialize")
        return _handle_initialize(request_id)
    elif method == 'tools/list':
        print("Handling tools/list")
        return _handle_tools_list(request_id)
    elif method == 'tools/call':
        print("Handling tools/call")
        return _handle_tools_call(body, request_id)
    else:
        print(f"Unknown method: {method}")
        return _error_response(request_id, -32601, f'Method not found: {method}')
    
    return _error_response(request_id, -32600, 'Invalid Request')


def _handle_initialize(request_id: int) -> Dict[str, Any]:
    """Инициализация MCP соединения"""
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({
            'jsonrpc': '2.0',
            'result': {
                'protocolVersion': '2024-11-05',
                'capabilities': {
                    'tools': {}
                },
                'serverInfo': {
                    'name': 'mcp-github-ydb',
                    'version': '1.0.0'
                }
            },
            'id': request_id
        })
    }


def _handle_tools_list(request_id: int) -> Dict[str, Any]:
    """Возвращает список доступных инструментов. Из schema.json — схема для каждой функции; иначе fallback."""
    tools = _load_tools_schema()
    if not tools:
        tools = [
        {
            "name": "get_repo_file",
            "description": "Получить содержимое файла из YDB репозитория на GitHub",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Путь к файлу в репозитории (например, 'ydb/core/client/client.h')"
                    },
                    "ref": {
                        "type": "string",
                        "description": "Ветка, тег или commit SHA (опционально, по умолчанию 'main')"
                    }
                },
                "required": ["path"]
            }
        },
        {
            "name": "search_repo_files",
            "description": "Поиск файлов в репозитории по имени или пути",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Поисковый запрос (имя файла или путь)"
                    },
                    "extension": {
                        "type": "string",
                        "description": "Расширение файла для фильтрации (например, '.py', '.cpp')"
                    }
                },
                "required": ["query"]
            }
        },
        {
            "name": "search_repo_content",
            "description": "Поиск по содержимому файлов в репозитории",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Поисковый запрос по содержимому (например, имя теста или строка ошибки)"
                    },
                    "path": {
                        "type": "string",
                        "description": "Ограничить поиск поддиректорией (опционально)"
                    },
                    "extension": {
                        "type": "string",
                        "description": "Расширение файла для фильтрации (например, '.py', '.cpp')"
                    },
                    "per_page": {
                        "type": "integer",
                        "description": "Максимум результатов (1-100, по умолчанию 20)"
                    }
                },
                "required": ["query"]
            }
        },
        {
            "name": "get_repo_tree",
            "description": "Получить структуру директории в репозитории",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Путь к директории (по умолчанию корень репозитория)"
                    },
                    "recursive": {
                        "type": "boolean",
                        "description": "Рекурсивно получить все файлы (по умолчанию false)"
                    },
                    "ref": {
                        "type": "string",
                        "description": "Ветка, тег или commit SHA (опционально)"
                    }
                },
                "required": []
            }
        },
        {
            "name": "get_file_commits",
            "description": "Получить историю коммитов для файла",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Путь к файлу в репозитории"
                    },
                    "ref": {
                        "type": "string",
                        "description": "Ветка/тег/commit (опционально)"
                    },
                    "per_page": {
                        "type": "integer",
                        "description": "Максимум коммитов (1-100, по умолчанию 20)"
                    }
                },
                "required": ["path"]
            }
        },
        {
            "name": "get_commit_diff",
            "description": "Получить детали и diff по коммиту. Для PR из форка передай repo=head_repo из get_pull_request.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sha": {
                        "type": "string",
                        "description": "SHA коммита"
                    },
                    "repo": {
                        "type": "string",
                        "description": "Репо (owner/repo). Для форка — head_repo из get_pull_request. Иначе ydb-platform/ydb."
                    }
                },
                "required": ["sha"]
            }
        },
        {
            "name": "get_pull_request",
            "description": "Получить информацию о pull request по номеру. Возвращает head_sha, head_repo, is_fork для форков.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "number": {
                        "type": "integer",
                        "description": "Номер pull request"
                    }
                },
                "required": ["number"]
            }
        },
        {
            "name": "get_pull_request_files",
            "description": "Список изменённых файлов и diff по PR. Работает и для PR из форка (не нужен get_commit_diff).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "number": {
                        "type": "integer",
                        "description": "Номер pull request"
                    }
                },
                "required": ["number"]
            }
        },
        {
            "name": "search_pull_requests",
            "description": "Поиск pull request по запросу",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Поисковый запрос (GitHub search syntax)"
                    },
                    "state": {
                        "type": "string",
                        "description": "Состояние PR: open/closed/all (опционально)"
                    },
                    "per_page": {
                        "type": "integer",
                        "description": "Максимум результатов (1-100, по умолчанию 20)"
                    }
                },
                "required": ["query"]
            }
        },
        {
            "name": "get_issue",
            "description": "Получить информацию об issue по номеру",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "number": {
                        "type": "integer",
                        "description": "Номер issue"
                    }
                },
                "required": ["number"]
            }
        },
        {
            "name": "search_issues",
            "description": "Поиск issues по запросу",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Поисковый запрос (GitHub search syntax)"
                    },
                    "state": {
                        "type": "string",
                        "description": "Состояние issue: open/closed/all (опционально)"
                    },
                    "per_page": {
                        "type": "integer",
                        "description": "Максимум результатов (1-100, по умолчанию 20)"
                    }
                },
                "required": ["query"]
            }
        },
        {
            "name": "get_file_info",
            "description": "Получить метаинформацию о файле (размер, SHA, URL)",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Путь к файлу в репозитории"
                    },
                    "ref": {
                        "type": "string",
                        "description": "Ветка, тег или commit SHA (опционально)"
                    }
                },
                "required": ["path"]
            }
        },
        {
            "name": "get_workflow_runs",
            "description": "Получить список последних запусков workflow. branch = ветка запуска (фильтр API). У каждого run есть workflow_branch (head_branch).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workflow": {
                        "type": "string",
                        "description": "Обязательно. Имя workflow: отображаемое (Regression-run_Small_and_Medium, Regression-run_Large) или файл (regression_run_small_medium.yml)."
                    },
                    "branch": {
                        "type": "string",
                        "description": "Ветка запуска workflow (фильтр runs). По умолчанию main."
                    },
                    "status": {
                        "type": "string",
                        "description": "Фильтр по статусу: completed/in_progress/queued (опционально)"
                    },
                    "per_page": {
                        "type": "integer",
                        "description": "Максимум запусков (1-100, по умолчанию 10)"
                    }
                },
                "required": ["workflow"]
            }
        },
        {
            "name": "get_workflow_run_jobs",
            "description": "Получить список jobs для конкретного запуска workflow",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "integer",
                        "description": "ID запуска workflow"
                    },
                    "per_page": {
                        "type": "integer",
                        "description": "Максимум jobs (1-100, по умолчанию 50)"
                    }
                },
                "required": ["run_id"]
            }
        },
        {
            "name": "compare_workflow_failures",
            "description": "Сравнить последние два запуска workflow и найти новые падения тестов по report_t.json",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workflow": {
                        "type": "string",
                        "description": "Имя файла workflow или ID (например, 'regression_run_large.yml')"
                    },
                    "branch": {
                        "type": "string",
                        "description": "Ветка (по умолчанию 'main')"
                    },
                    "artifact_name_contains": {
                        "type": "string",
                        "description": "Подстрока имени артефакта (по умолчанию 'report_t')"
                    },
                    "artifact_file_name": {
                        "type": "string",
                        "description": "Имя файла внутри zip (по умолчанию 'report_t.json')"
                    },
                    "per_page": {
                        "type": "integer",
                        "description": "Сколько последних запусков смотреть (1-100, по умолчанию 10)"
                    }
                },
                "required": ["workflow"]
            }
        },
        {
            "name": "get_run_report",
            "description": "Скачать report.json из storage, распарсить и вернуть только извлечённые failures/muted + report_url, html_url. Полный отчёт не отдаётся.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report_url": {
                        "type": "string",
                        "description": "Полный URL report.json (если известен)"
                    },
                    "run_id": {
                        "type": "integer",
                        "description": "ID workflow run. В путях storage используется **run_id** (не job_id). Предпочтительно."
                    },
                    "job_id": {
                        "type": "integer",
                        "description": "Опционально. Если передан, tool резолвит job_id → run_id и ищет отчёт по run_id."
                    },
                    "suite": {
                        "type": "string",
                        "description": "Имя папки suite, например 'Regression-run_Small_and_Medium'"
                    },
                    "platform": {
                        "type": "string",
                        "description": "Имя платформы, например 'ya-stable-25-2-1-x86-64'"
                    },
                    "try_number": {
                        "type": "integer",
                        "description": "Номер try (1..3). Если не указан, ищет в try_1..try_3"
                    },
                    "base_url": {
                        "type": "string",
                        "description": "Базовый URL логов (по умолчанию ydb-gh-logs/<repo>)"
                    },
                    "max_failures": {
                        "type": "integer",
                        "description": "Максимум падений в выдаче (по умолчанию 200)"
                    }
                },
                "required": []
            }
        },
        {
            "name": "compare_run_reports",
            "description": "Сравнить два report.json по current_run_id и previous_run_id. НЕ вычисляй previous как run_id - 1 (run_id не последовательные). Для «сравни с предыдущим» используй compare_with_previous_run.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "current_report_url": {
                        "type": "string",
                        "description": "URL текущего report.json"
                    },
                    "previous_report_url": {
                        "type": "string",
                        "description": "URL предыдущего report.json"
                    },
                    "current_job_id": {
                        "type": "integer",
                        "description": "ID текущего job (альтернатива current_report_url)"
                    },
                    "previous_job_id": {
                        "type": "integer",
                        "description": "ID предыдущего job (альтернатива previous_report_url)"
                    },
                    "suite": {
                        "type": "string",
                        "description": "Имя папки suite (если используем job_id)"
                    },
                    "platform": {
                        "type": "string",
                        "description": "Имя платформы (если используем job_id)"
                    },
                    "current_try": {
                        "type": "integer",
                        "description": "Номер try для текущего (1..3, по умолчанию ищет 3,2,1)"
                    },
                    "previous_try": {
                        "type": "integer",
                        "description": "Номер try для предыдущего (1..3, по умолчанию ищет 3,2,1)"
                    },
                    "max_new_failures": {
                        "type": "integer",
                        "description": "Максимум новых падений в выдаче (по умолчанию 200)"
                    },
                    "max_fixed_failures": {
                        "type": "integer",
                        "description": "Максимум исправленных падений в выдаче (по умолчанию 200)"
                    },
                    "current_run_id": {
                        "type": "integer",
                        "description": "ID текущего workflow run (предпочтительно; в storage используется run_id)"
                    },
                    "previous_run_id": {
                        "type": "integer",
                        "description": "ID предыдущего workflow run. Берут из get_workflow_runs(per_page=2): runs[0]=current, runs[1]=previous. Не run_id - 1!"
                    }
                },
                "required": []
            }
        },
        {
            "name": "compare_with_previous_run",
            "description": "Сравнить последний и предпоследний run по workflow+branch. По умолчанию main, relwithdebinfo. Если указаны since/until — только запуски за эти даты. Не run_id - 1.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workflow": {
                        "type": "string",
                        "description": "Regression-run_Small_and_Medium, Regression-run_Large, Regression-whitelist-run, nightly_small, whitelist и т.д."
                    },
                    "branch": {
                        "type": "string",
                        "description": "Ветка запуска workflow (фильтр). По умолчанию main."
                    },
                    "test_branch": {
                        "type": "string",
                        "description": "Ветка тестов (platform). Если не задана — branch."
                    },
                    "build_preset": {
                        "type": "string",
                        "description": "relwithdebinfo, asan, tsan, msan. По умолчанию relwithdebinfo."
                    },
                    "since": {
                        "type": "string",
                        "description": "Начало диапазона дат (YYYY-MM-DD). Только запуски с created_at >= since. Используй вместе с until."
                    },
                    "until": {
                        "type": "string",
                        "description": "Конец диапазона дат (YYYY-MM-DD). Только запуски с created_at <= until. Используй вместе с since."
                    },
                    "max_new_failures": {
                        "type": "integer",
                        "description": "Максимум новых падений в выдаче (по умолчанию 200)"
                    },
                    "max_fixed_failures": {
                        "type": "integer",
                        "description": "Максимум исправленных падений в выдаче (по умолчанию 200)"
                    }
                },
                "required": ["workflow"]
            }
        },
        {
            "name": "get_regression_report",
            "description": "Один вызов: получить отчёт по ночным/whitelist/compatibility тестам. Сам находит run_id, скачивает report. Не нужно вызывать get_workflow_runs → get_workflow_run_jobs → get_run_report.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workflow": {
                        "type": "string",
                        "description": "Workflow: Regression-run_Small_and_Medium, Regression-run_Large, Regression-whitelist-run, Regression-run_compatibility (или nightly_small, nightly_large, whitelist, compatibility)"
                    },
                    "branch": {
                        "type": "string",
                        "description": "Ветка запуска workflow (фильтр runs в API). По умолчанию main."
                    },
                    "test_branch": {
                        "type": "string",
                        "description": "Ветка, по которой гоняют тесты (platform в storage). Если не задана — используется branch. Может отличаться при «workflow из ветки A, тесты по ветке B»."
                    },
                    "build_preset": {
                        "type": "string",
                        "description": "relwithdebinfo, asan, tsan, msan (по умолчанию relwithdebinfo)"
                    },
                    "max_failures": {
                        "type": "integer",
                        "description": "Максимум падений в выдаче (по умолчанию 200)"
                    }
                },
                "required": ["workflow"]
            }
        },
        {
            "name": "get_run_errors_matrix",
            "description": "Таблица «ветка × конфигурация»: по run_id — failures_count и report_url/html_url в каждой ячейке. Для «количество ошибок по конфигурации и ветке, число — ссылка».",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "integer", "description": "ID workflow run"},
                    "suite": {"type": "string", "description": "Suite, например Regression-run_Small_and_Medium"},
                    "workflow": {"type": "string", "description": "Альтернатива suite: nightly_small, Regression-run_Small_and_Medium"},
                    "branches": {"type": "array", "items": {"type": "string"}, "description": "Ветки (по умолчанию main, stable-25-2, ...)"},
                    "build_presets": {"type": "array", "items": {"type": "string"}, "description": "Конфигурации (по умолчанию relwithdebinfo, release-asan, ...)"},
                    "base_url": {"type": "string", "description": "Базовый URL логов (опционально)"}
                },
                "required": ["run_id"]
            }
        }
    ]
    
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({
            'jsonrpc': '2.0',
            'result': {'tools': tools},
            'id': request_id
        })
    }


def _normalize_tool_name(name: str) -> str:
    """Нормализует имя инструмента: пробелы → подчёркивания, lowercase."""
    if not name or not isinstance(name, str):
        return ''
    return name.strip().lower().replace(' ', '_')


def _handle_tools_call(body: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Обработка вызова инструмента"""
    params = body.get('params', {})
    raw_name = params.get('name', '')
    tool_name = _normalize_tool_name(raw_name) or raw_name
    arguments = params.get('arguments', {})
    if not isinstance(arguments, dict):
        arguments = {}
    print(f"Tool call: name={tool_name}, arguments={arguments}")
    
    try:
        if tool_name == 'get_repo_file':
            return _get_repo_file(arguments, request_id)
        elif tool_name == 'search_repo_files':
            return _search_repo_files(arguments, request_id)
        elif tool_name == 'search_repo_content':
            return _search_repo_content(arguments, request_id)
        elif tool_name == 'get_repo_tree':
            return _get_repo_tree(arguments, request_id)
        elif tool_name == 'get_file_commits':
            return _get_file_commits(arguments, request_id)
        elif tool_name == 'get_commit_diff':
            return _get_commit_diff(arguments, request_id)
        elif tool_name == 'get_pull_request':
            return _get_pull_request(arguments, request_id)
        elif tool_name == 'get_pull_request_files':
            return _get_pull_request_files(arguments, request_id)
        elif tool_name == 'search_pull_requests':
            return _search_pull_requests(arguments, request_id)
        elif tool_name == 'get_issue':
            return _get_issue(arguments, request_id)
        elif tool_name == 'search_issues':
            return _search_issues(arguments, request_id)
        elif tool_name == 'get_file_info':
            return _get_file_info(arguments, request_id)
        elif tool_name == 'get_workflow_runs':
            return _get_workflow_runs(arguments, request_id)
        elif tool_name == 'get_workflow_run_jobs':
            return _get_workflow_run_jobs(arguments, request_id)
        elif tool_name == 'compare_workflow_failures':
            return _compare_workflow_failures(arguments, request_id)
        elif tool_name == 'get_run_report':
            return _get_run_report(arguments, request_id)
        elif tool_name == 'compare_run_reports':
            return _compare_run_reports(arguments, request_id)
        elif tool_name == 'compare_with_previous_run':
            return _compare_with_previous_run(arguments, request_id)
        elif tool_name == 'get_regression_report':
            return _get_regression_report(arguments, request_id)
        elif tool_name == 'get_run_errors_matrix':
            return _get_run_errors_matrix(arguments, request_id)
        else:
            return _error_response(request_id, -32601, f'Unknown tool: {tool_name}')
    except Exception as e:
        print(f"Internal error in tool call: {str(e)}")
        return _error_response(request_id, -32603, f'Internal error: {str(e)}')


def _get_repo_file(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Получить содержимое файла из репозитория"""
    path = arguments.get('path', '')
    ref = arguments.get('ref', 'main')
    
    if not path:
        return _error_response(request_id, -32602, 'Path parameter is required')
    
    url = f"https://api.github.com/repos/{REPO}/contents/{path}"
    params = {'ref': ref} if ref else {}
    headers = _get_headers()
    
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        if 'content' in data:
            # Декодируем base64 содержимое
            content = base64.b64decode(data['content']).decode('utf-8', errors='ignore')
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'jsonrpc': '2.0',
                    'result': {
                        'content': content,
                        'encoding': data.get('encoding', 'base64'),
                        'size': data.get('size', 0),
                        'sha': data.get('sha', ''),
                        'url': data.get('html_url', '')
                    },
                    'id': request_id
                })
            }
        else:
            return _error_response(request_id, -32602, 'File not found or is not a file')
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return _error_response(request_id, -32602, f'File not found: {path}')
        return _error_response(request_id, -32603, f'GitHub API error: {str(e)}')
    except Exception as e:
        return _error_response(request_id, -32603, f'Error fetching file: {str(e)}')


def _search_repo_files(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Поиск файлов в репозитории"""
    query = arguments.get('query', '')
    extension = arguments.get('extension', '')
    
    if not query:
        return _error_response(request_id, -32602, 'Query parameter is required')
    
    # Используем GitHub Search API
    search_query = f'repo:{REPO} {query}'
    if extension:
        search_query += f' extension:{extension}'
    
    url = "https://api.github.com/search/code"
    params = {'q': search_query}
    headers = _get_headers()
    
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        results = []
        for item in data.get('items', [])[:20]:  # Ограничиваем 20 результатами
            results.append({
                'path': item.get('path', ''),
                'name': item.get('name', ''),
                'url': item.get('html_url', ''),
                'sha': item.get('sha', '')
            })
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'jsonrpc': '2.0',
                'result': {
                    'total_count': data.get('total_count', 0),
                    'items': results
                },
                'id': request_id
            })
        }
    except Exception as e:
        return _error_response(request_id, -32603, f'Error searching files: {str(e)}')


def _search_repo_content(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Поиск по содержимому файлов в репозитории"""
    query = arguments.get('query', '')
    extension = arguments.get('extension', '')
    path = arguments.get('path', '')
    per_page = arguments.get('per_page', 20)

    if not query:
        return _error_response(request_id, -32602, 'Query parameter is required')

    try:
        per_page = max(1, min(int(per_page), 100))
    except (TypeError, ValueError):
        per_page = 20

    search_query = f'repo:{REPO} {query} in:file'
    if extension:
        search_query += f' extension:{extension.lstrip(".")}'
    if path:
        search_query += f' path:{path}'

    url = "https://api.github.com/search/code"
    params = {'q': search_query, 'per_page': per_page}
    headers = _get_headers()

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for item in data.get('items', [])[:per_page]:
            results.append({
                'path': item.get('path', ''),
                'name': item.get('name', ''),
                'url': item.get('html_url', ''),
                'sha': item.get('sha', ''),
                'score': item.get('score', None)
            })

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'jsonrpc': '2.0',
                'result': {
                    'total_count': data.get('total_count', 0),
                    'items': results
                },
                'id': request_id
            })
        }
    except Exception as e:
        return _error_response(request_id, -32603, f'Error searching content: {str(e)}')


def _get_repo_tree(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Получить структуру директории"""
    path = arguments.get('path', '')
    recursive = arguments.get('recursive', False)
    ref = arguments.get('ref', 'main')
    
    url = f"https://api.github.com/repos/{REPO}/git/trees/{ref}"
    if path:
        # Сначала получаем SHA директории
        contents_url = f"https://api.github.com/repos/{REPO}/contents/{path}"
        headers = _get_headers()
        try:
            resp = requests.get(contents_url, headers=headers, params={'ref': ref}, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                # Это директория
                tree_sha = None
                for item in data:
                    if item.get('type') == 'dir':
                        tree_sha = item.get('sha')
                        break
                if tree_sha:
                    url = f"https://api.github.com/repos/{REPO}/git/trees/{tree_sha}"
            elif data.get('type') == 'dir':
                url = f"https://api.github.com/repos/{REPO}/git/trees/{data.get('sha')}"
            else:
                return _error_response(request_id, -32602, f'Path is not a directory: {path}')
        except Exception as e:
            return _error_response(request_id, -32603, f'Error getting directory: {str(e)}')
    
    params = {'recursive': '1'} if recursive else {}
    headers = _get_headers()
    
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        
        tree_items = []
        for item in data.get('tree', []):
            tree_items.append({
                'path': item.get('path', ''),
                'type': item.get('type', ''),
                'size': item.get('size', 0),
                'sha': item.get('sha', '')
            })
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'jsonrpc': '2.0',
                'result': {
                    'path': path or '/',
                    'items': tree_items,
                    'total': len(tree_items)
                },
                'id': request_id
            })
        }
    except Exception as e:
        return _error_response(request_id, -32603, f'Error getting tree: {str(e)}')


def _get_file_commits(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Получить историю коммитов для файла"""
    path = arguments.get('path', '')
    ref = arguments.get('ref', '')
    per_page = arguments.get('per_page', 20)

    if not path:
        return _error_response(request_id, -32602, 'Path parameter is required')

    try:
        per_page = max(1, min(int(per_page), 100))
    except (TypeError, ValueError):
        per_page = 20

    url = f"https://api.github.com/repos/{REPO}/commits"
    params = {'path': path, 'per_page': per_page}
    if ref:
        params['sha'] = ref
    headers = _get_headers()

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        commits = []
        for item in data:
            commit = item.get('commit', {})
            author = commit.get('author', {}) or {}
            committer = commit.get('committer', {}) or {}
            commits.append({
                'sha': item.get('sha', ''),
                'message': commit.get('message', ''),
                'author_name': author.get('name', ''),
                'author_date': author.get('date', ''),
                'committer_name': committer.get('name', ''),
                'committer_date': committer.get('date', ''),
                'url': item.get('html_url', '')
            })

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'jsonrpc': '2.0',
                'result': {
                    'path': path,
                    'commits': commits,
                    'total': len(commits)
                },
                'id': request_id
            })
        }
    except Exception as e:
        return _error_response(request_id, -32603, f'Error getting file commits: {str(e)}')


def _get_commit_diff(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Получить детали и diff по коммиту. Для PR из форка передавай repo=head_repo из get_pull_request."""
    sha = arguments.get('sha', '')
    repo = (arguments.get('repo') or '').strip() or REPO

    if not sha:
        return _error_response(request_id, -32602, 'SHA parameter is required')

    url = f"https://api.github.com/repos/{repo}/commits/{sha}"
    headers = _get_headers()

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        commit = data.get('commit', {}) or {}
        author = commit.get('author', {}) or {}
        files = []
        for f in data.get('files', []) or []:
            files.append({
                'filename': f.get('filename', ''),
                'status': f.get('status', ''),
                'additions': f.get('additions', 0),
                'deletions': f.get('deletions', 0),
                'changes': f.get('changes', 0),
                'patch': f.get('patch', '')
            })

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'jsonrpc': '2.0',
                'result': {
                    'sha': data.get('sha', ''),
                    'message': commit.get('message', ''),
                    'author_name': author.get('name', ''),
                    'author_date': author.get('date', ''),
                    'url': data.get('html_url', ''),
                    'files': files
                },
                'id': request_id
            })
        }
    except Exception as e:
        return _error_response(request_id, -32603, f'Error getting commit diff: {str(e)}')


def _get_pull_request(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Получить информацию о pull request по номеру"""
    number = arguments.get('number')
    if not number:
        return _error_response(request_id, -32602, 'Number parameter is required')

    url = f"https://api.github.com/repos/{REPO}/pulls/{number}"
    headers = _get_headers()

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        base = data.get('base') or {}
        head = data.get('head') or {}
        base_repo = (base.get('repo') or {}).get('full_name', '')
        head_repo = (head.get('repo') or {}).get('full_name', '')
        is_fork = bool(head_repo and base_repo and head_repo != base_repo)

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'jsonrpc': '2.0',
                'result': {
                    'number': data.get('number'),
                    'title': data.get('title', ''),
                    'state': data.get('state', ''),
                    'user': (data.get('user') or {}).get('login', ''),
                    'created_at': data.get('created_at', ''),
                    'updated_at': data.get('updated_at', ''),
                    'merged_at': data.get('merged_at', ''),
                    'url': data.get('html_url', ''),
                    'body': data.get('body', ''),
                    'labels': [lbl.get('name', '') for lbl in data.get('labels', [])],
                    'base_ref': base.get('ref', ''),
                    'head_ref': head.get('ref', ''),
                    'head_sha': head.get('sha', ''),
                    'head_repo': head_repo or None,
                    'is_fork': is_fork,
                },
                'id': request_id
            })
        }
    except Exception as e:
        return _error_response(request_id, -32603, f'Error getting pull request: {str(e)}')


def _get_pull_request_files(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Список изменённых файлов и patch по PR. GET /pulls/{id}/files — работает и для форков."""
    number = arguments.get('number')
    if not number:
        return _error_response(request_id, -32602, 'Number parameter is required')

    url = f"https://api.github.com/repos/{REPO}/pulls/{int(number)}/files"
    headers = _get_headers()

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        files = []
        for f in data or []:
            files.append({
                'filename': f.get('filename', ''),
                'status': f.get('status', ''),
                'additions': f.get('additions', 0),
                'deletions': f.get('deletions', 0),
                'changes': f.get('changes', 0),
                'patch': f.get('patch') or '',
            })

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'jsonrpc': '2.0',
                'result': {'files': files, 'count': len(files)},
                'id': request_id
            })
        }
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 422:
            return _error_response(
                request_id, -32603,
                'PR files unavailable (422). Часто для PR из форка при удалённой/переписанной ветке. '
                'Используй get_pull_request: head_sha, head_repo, is_fork; для diff по коммиту — get_commit_diff(sha, repo=head_repo).'
            )
        return _error_response(request_id, -32603, f'Error getting PR files: {str(e)}')
    except Exception as e:
        return _error_response(request_id, -32603, f'Error getting PR files: {str(e)}')


def _search_pull_requests(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Поиск pull request по запросу"""
    query = arguments.get('query', '')
    state = arguments.get('state', '')
    per_page = arguments.get('per_page', 20)

    if not query:
        return _error_response(request_id, -32602, 'Query parameter is required')

    try:
        per_page = max(1, min(int(per_page), 100))
    except (TypeError, ValueError):
        per_page = 20

    search_query = f'repo:{REPO} type:pr {query}'
    if state:
        search_query += f' state:{state}'

    url = "https://api.github.com/search/issues"
    params = {'q': search_query, 'per_page': per_page}
    headers = _get_headers()

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        items = []
        for item in data.get('items', [])[:per_page]:
            items.append({
                'number': item.get('number'),
                'title': item.get('title', ''),
                'state': item.get('state', ''),
                'user': (item.get('user') or {}).get('login', ''),
                'url': item.get('html_url', '')
            })

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'jsonrpc': '2.0',
                'result': {
                    'total_count': data.get('total_count', 0),
                    'items': items
                },
                'id': request_id
            })
        }
    except Exception as e:
        return _error_response(request_id, -32603, f'Error searching pull requests: {str(e)}')


def _get_issue(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Получить информацию об issue по номеру"""
    number = arguments.get('number')
    if not number:
        return _error_response(request_id, -32602, 'Number parameter is required')

    url = f"https://api.github.com/repos/{REPO}/issues/{number}"
    headers = _get_headers()

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'jsonrpc': '2.0',
                'result': {
                    'number': data.get('number'),
                    'title': data.get('title', ''),
                    'state': data.get('state', ''),
                    'user': (data.get('user') or {}).get('login', ''),
                    'created_at': data.get('created_at', ''),
                    'updated_at': data.get('updated_at', ''),
                    'closed_at': data.get('closed_at', ''),
                    'url': data.get('html_url', ''),
                    'body': data.get('body', ''),
                    'labels': [lbl.get('name', '') for lbl in data.get('labels', [])]
                },
                'id': request_id
            })
        }
    except Exception as e:
        return _error_response(request_id, -32603, f'Error getting issue: {str(e)}')


def _search_issues(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Поиск issues по запросу"""
    query = arguments.get('query', '')
    state = arguments.get('state', '')
    per_page = arguments.get('per_page', 20)

    if not query:
        return _error_response(request_id, -32602, 'Query parameter is required')

    try:
        per_page = max(1, min(int(per_page), 100))
    except (TypeError, ValueError):
        per_page = 20

    search_query = f'repo:{REPO} type:issue {query}'
    if state:
        search_query += f' state:{state}'

    url = "https://api.github.com/search/issues"
    params = {'q': search_query, 'per_page': per_page}
    headers = _get_headers()

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        items = []
        for item in data.get('items', [])[:per_page]:
            items.append({
                'number': item.get('number'),
                'title': item.get('title', ''),
                'state': item.get('state', ''),
                'user': (item.get('user') or {}).get('login', ''),
                'url': item.get('html_url', '')
            })

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'jsonrpc': '2.0',
                'result': {
                    'total_count': data.get('total_count', 0),
                    'items': items
                },
                'id': request_id
            })
        }
    except Exception as e:
        return _error_response(request_id, -32603, f'Error searching issues: {str(e)}')


def _get_file_info(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Получить метаинформацию о файле"""
    path = arguments.get('path', '')
    ref = arguments.get('ref', 'main')
    
    if not path:
        return _error_response(request_id, -32602, 'Path parameter is required')
    
    url = f"https://api.github.com/repos/{REPO}/contents/{path}"
    params = {'ref': ref} if ref else {}
    headers = _get_headers()
    
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'jsonrpc': '2.0',
                'result': {
                    'name': data.get('name', ''),
                    'path': data.get('path', ''),
                    'sha': data.get('sha', ''),
                    'size': data.get('size', 0),
                    'type': data.get('type', ''),
                    'url': data.get('html_url', ''),
                    'download_url': data.get('download_url', ''),
                    'encoding': data.get('encoding', '')
                },
                'id': request_id
            })
        }
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return _error_response(request_id, -32602, f'File not found: {path}')
        return _error_response(request_id, -32603, f'GitHub API error: {str(e)}')
    except Exception as e:
        return _error_response(request_id, -32603, f'Error getting file info: {str(e)}')


def _get_headers() -> Dict[str, str]:
    """Получить заголовки для GitHub API запросов"""
    headers = {'Accept': 'application/vnd.github.v3+json'}
    if GITHUB_PAT:
        headers['Authorization'] = f'token {GITHUB_PAT}'
    return headers


def _github_get(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15) -> Dict[str, Any]:
    """GET запрос к GitHub API с обработкой ошибок"""
    headers = _get_headers()
    resp = requests.get(url, headers=headers, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _is_github_run_id(uid: int) -> bool:
    """Проверить, является ли uid run_id (ID workflow run). Не бросает исключений."""
    url = f"https://api.github.com/repos/{REPO}/actions/runs/{uid}"
    try:
        resp = requests.get(url, headers=_get_headers(), timeout=10)
        return resp.status_code == 200
    except Exception:
        return False


def _get_run_id_from_job_id(job_id: int) -> Optional[int]:
    """Получить run_id по job_id (GET /actions/jobs/{job_id} возвращает run_id)."""
    url = f"https://api.github.com/repos/{REPO}/actions/jobs/{job_id}"
    try:
        data = _github_get(url)
        return data.get('run_id')
    except Exception:
        return None


def _resolve_workflow_id(workflow: str) -> str:
    """Отображаемое имя или алиас → имя файла workflow. Иначе возвращает как есть."""
    if not workflow or not isinstance(workflow, str):
        return workflow or ''
    s = workflow.strip()
    return WORKFLOW_NAME_TO_FILE.get(s, s)


def _get_workflow_runs(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Получить список запусков workflow"""
    workflow = arguments.get('workflow')
    branch = arguments.get('branch', 'main')
    status = arguments.get('status', '')
    per_page = arguments.get('per_page', 10)

    if not workflow:
        valid = ', '.join(f"'{k}'" for k in list(WORKFLOW_NAME_TO_FILE.keys())[:4])
        return _error_response(
            request_id, -32602,
            f"workflow is required. Use display name (e.g. {valid}) or file name (e.g. regression_run_small_medium.yml)."
        )
    workflow = _resolve_workflow_id(workflow)

    try:
        per_page = max(1, min(int(per_page), 100))
    except (TypeError, ValueError):
        per_page = 10

    url = f"https://api.github.com/repos/{REPO}/actions/workflows/{workflow}/runs"
    params: Dict[str, Any] = {'branch': branch, 'per_page': per_page}
    if status:
        params['status'] = status

    try:
        data = _github_get(url, params=params)
        runs = []
        for run in data.get('workflow_runs', []):
            runs.append({
                'id': run.get('id'),
                'name': run.get('name', ''),
                'status': run.get('status', ''),
                'conclusion': run.get('conclusion', ''),
                'event': run.get('event', ''),
                'created_at': run.get('created_at', ''),
                'updated_at': run.get('updated_at', ''),
                'html_url': run.get('html_url', ''),
                'workflow_branch': run.get('head_branch', ''),
                'head_branch': run.get('head_branch', ''),
                'head_sha': run.get('head_sha', '')
            })

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'jsonrpc': '2.0',
                'result': {
                    'total_count': data.get('total_count', 0),
                    'workflow': workflow,
                    'workflow_branch_filter': branch,
                    'branch': branch,
                    'runs': runs
                },
                'id': request_id
            })
        }
    except Exception as e:
        return _error_response(request_id, -32603, f'Error getting workflow runs: {str(e)}')


def _fetch_job_links(run_id: int) -> List[Dict[str, Any]]:
    """Получить прямые ссылки на jobs для run_id. При ошибке — пустой список."""
    try:
        url = f"https://api.github.com/repos/{REPO}/actions/runs/{run_id}/jobs"
        data = _github_get(url, params={'per_page': 100})
        out = []
        for job in data.get('jobs', []):
            u = job.get('html_url') or ''
            if u:
                out.append({'id': job.get('id'), 'name': job.get('name', ''), 'url': u})
        return out
    except Exception as e:
        print(f"_fetch_job_links run_id={run_id}: {e}")
        return []


def _get_workflow_run_jobs(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Получить список jobs для запуска workflow"""
    run_id = arguments.get('run_id')
    per_page = arguments.get('per_page', 50)

    if not run_id:
        return _error_response(request_id, -32602, 'run_id parameter is required')

    try:
        per_page = max(1, min(int(per_page), 100))
    except (TypeError, ValueError):
        per_page = 50

    url = f"https://api.github.com/repos/{REPO}/actions/runs/{run_id}/jobs"
    params = {'per_page': per_page}

    try:
        data = _github_get(url, params=params)
        jobs = []
        for job in data.get('jobs', []):
            jobs.append({
                'id': job.get('id'),
                'name': job.get('name', ''),
                'status': job.get('status', ''),
                'conclusion': job.get('conclusion', ''),
                'started_at': job.get('started_at', ''),
                'completed_at': job.get('completed_at', ''),
                'html_url': job.get('html_url', ''),
                'runner_name': job.get('runner_name', '')
            })

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'jsonrpc': '2.0',
                'result': {
                    'total_count': data.get('total_count', 0),
                    'jobs': jobs
                },
                'id': request_id
            })
        }
    except Exception as e:
        return _error_response(request_id, -32603, f'Error getting workflow run jobs: {str(e)}')


def _resolve_job_id(run_id: Optional[int], job_id: Optional[int], job_name: Optional[str]) -> Tuple[Optional[int], Optional[str]]:
    """Определить job_id по run_id и имени job"""
    if job_id:
        try:
            return int(job_id), None
        except (TypeError, ValueError):
            return None, 'Invalid job_id'

    if not run_id or not job_name:
        return None, None

    jobs_url = f"https://api.github.com/repos/{REPO}/actions/runs/{run_id}/jobs"
    data = _github_get(jobs_url, params={'per_page': 100})
    jobs = data.get('jobs', []) or []
    if not jobs:
        return None, f'No jobs found for run_id={run_id}'

    normalized = str(job_name).strip().lower()
    exact = [j for j in jobs if (j.get('name') or '').strip().lower() == normalized]
    if len(exact) == 1:
        return exact[0].get('id'), None

    contains = [j for j in jobs if normalized in (j.get('name') or '').strip().lower()]
    matches = exact or contains
    if len(matches) == 1:
        return matches[0].get('id'), None
    if not matches:
        available = [j.get('name', '') for j in jobs]
        return None, f'Job name not found. Available jobs: {available}'

    names = [j.get('name', '') for j in matches]
    return None, f'Ambiguous job name. Matches: {names}'


def _build_report_urls(
    base_url: str,
    suite: str,
    run_id: int,
    platform: str,
    try_numbers: List[int]
) -> List[Dict[str, Any]]:
    """Сформировать ссылки на report.json, ya-test.html и platform-level index.html.
    
    В путях storage используется **run_id** (workflow run), не job_id.
    Base всегда ydb-gh-logs (не ydb-public/reports).
    Структура: {base_url}/{suite}/{run_id}/{platform}/try_{N}/report.json, .../ya-test.html
    Platform-level: .../{platform}/index.html — каталог try_1, try_2, try_3, логи и т.д.
    """
    base_url = base_url.rstrip('/')
    platform_prefix = f"{base_url}/{suite}/{run_id}/{platform}"
    index_url = f"{platform_prefix}/index.html"
    urls = []
    for try_number in try_numbers:
        prefix = f"{platform_prefix}/try_{try_number}"
        urls.append({
            'try_number': try_number,
            'report_url': f"{prefix}/report.json",
            'html_url': f"{prefix}/ya-test.html",
            'index_url': index_url,
        })
    return urls


def _fetch_json_url(url: str, timeout: int = 20) -> Dict[str, Any]:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _fetch_html_url(url: str, timeout: int = 20) -> str:
    """Получить HTML контент по URL"""
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def _get_run_report(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Получить report.json/ya-test.html из storage"""
    report_url = arguments.get('report_url', '')
    run_id = arguments.get('run_id')
    job_id = arguments.get('job_id')
    suite = arguments.get('suite', '')
    platform = arguments.get('platform', '')
    try_number = arguments.get('try_number')
    base_url = arguments.get('base_url') or f"https://storage.yandexcloud.net/ydb-gh-logs/{REPO}"
    max_failures = arguments.get('max_failures', 200)

    try:
        max_failures = max(1, min(int(max_failures), 1000))
    except (TypeError, ValueError):
        max_failures = 200

    if report_url:
        try:
            report = _fetch_json_url(report_url)
            failures, muted = _extract_failures_from_report(report)
            html_url = report_url.replace('/report.json', '/ya-test.html')
            result: Dict[str, Any] = {
                'report_url': report_url,
                'html_url': html_url,
                'failures_count': len(failures),
                'muted_count': len(muted),
                'failures': failures[:max_failures],
                'muted': muted[:max_failures]
            }
            if 'workflow_branch' in arguments:
                result['workflow_branch'] = arguments['workflow_branch']
            if 'test_branch' in arguments:
                result['test_branch'] = arguments['test_branch']
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'jsonrpc': '2.0', 'result': result, 'id': request_id})
            }
        except Exception as e:
            return _error_response(request_id, -32603, f'Error fetching report_url: {str(e)}')

    if not suite or not platform:
        return _error_response(request_id, -32602, 'suite and platform are required')

    resolved_run_id: Optional[int] = None
    if run_id is not None:
        try:
            resolved_run_id = int(run_id)
        except (TypeError, ValueError):
            return _error_response(request_id, -32602, 'Invalid run_id')
    elif job_id is not None:
        try:
            jid = int(job_id)
        except (TypeError, ValueError):
            return _error_response(request_id, -32602, 'Invalid job_id')
        resolved_run_id = _get_run_id_from_job_id(jid)
        if not resolved_run_id:
            if _is_github_run_id(jid):
                return _error_response(
                    request_id, -32602,
                    f'The id {jid} is a **run_id**, not job_id. Storage uses run_id. '
                    'Use **run_id** + suite + platform (no job_id).'
                )
            return _error_response(request_id, -32602, f'Could not resolve run_id from job_id={jid}')
    else:
        return _error_response(request_id, -32602, 'run_id or job_id is required')

    if try_number:
        try_numbers = [int(try_number)]
    else:
        try_numbers = [3, 2, 1]

    urls = _build_report_urls(base_url, suite, resolved_run_id, platform, try_numbers)
    print(f"Built URLs: {[u['report_url'] for u in urls]}")
    
    available_tries = []
    for entry in urls:
        url = entry['report_url']
        print(f"Checking if exists: {url}")
        try:
            resp = requests.head(url, timeout=10, allow_redirects=True)
            if resp.status_code == 200:
                available_tries.append(entry)
                print(f"Found: {url}")
        except Exception as e:
            print(f"Error checking {url}: {str(e)}")
            continue
    
    if not available_tries:
        return _error_response(
            request_id, -32602,
            f'No report.json found for suite={suite}, run_id={resolved_run_id}, platform={platform}. '
            f'Tried: {[u["try_number"] for u in urls]}'
        )
    
    entry = available_tries[0]
    url = entry['report_url']
    print(f"Using: {url}")
    
    try:
        report = _fetch_json_url(url)
        failures, muted = _extract_failures_from_report(report)
        run_url = f"https://github.com/{REPO}/actions/runs/{resolved_run_id}"
        job_links = _fetch_job_links(resolved_run_id)
        result = {
            'suite': suite,
            'platform': platform,
            'run_id': resolved_run_id,
            'run_url': run_url,
            'job_links': job_links,
            'used_try': entry['try_number'],
            'available_tries': [e['try_number'] for e in available_tries],
            'report_url': url,
            'html_url': entry['html_url'],
            'index_url': entry['index_url'],
            'failures_count': len(failures),
            'muted_count': len(muted),
            'failures': failures[:max_failures],
            'muted': muted[:max_failures]
        }
        if 'workflow_branch' in arguments:
            result['workflow_branch'] = arguments['workflow_branch']
        if 'test_branch' in arguments:
            result['test_branch'] = arguments['test_branch']
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'jsonrpc': '2.0', 'result': result, 'id': request_id})
        }
    except Exception as e:
        return _error_response(request_id, -32603, f'Error fetching report: {str(e)}')


DEFAULT_MATRIX_BRANCHES = [
    "main", "stable-25-2", "stable-25-2-1", "stable-25-3", "stable-25-3-1",
    "stable-25-4", "stream-nb-25-1",
]
DEFAULT_MATRIX_BUILD_PRESETS = ["relwithdebinfo", "release-asan", "release-tsan", "release-msan"]


def _get_run_errors_matrix(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Матрица (ветка × конфигурация): failures_count + report_url для каждой ячейки. Для таблицы «количество ошибок — ссылка»."""
    run_id = arguments.get("run_id")
    suite = (arguments.get("suite") or "").strip()
    workflow = (arguments.get("workflow") or "").strip()
    base_url = arguments.get("base_url") or f"https://storage.yandexcloud.net/ydb-gh-logs/{REPO}"

    if not run_id:
        return _error_response(request_id, -32602, "run_id is required")
    try:
        run_id = int(run_id)
    except (TypeError, ValueError):
        return _error_response(request_id, -32602, "Invalid run_id")

    if not suite and workflow:
        w = workflow.strip().lower()
        m = {"nightly_small": "Regression-run_Small_and_Medium", "nightly_large": "Regression-run_Large",
             "whitelist": "Regression-whitelist-run", "compatibility": "Regression-run_compatibility"}
        suite = m.get(w, workflow)
    if not suite:
        suite = "Regression-run_Small_and_Medium"

    raw_branches = arguments.get("branches")
    branches = raw_branches if isinstance(raw_branches, list) else DEFAULT_MATRIX_BRANCHES
    raw_presets = arguments.get("build_presets")
    build_presets = raw_presets if isinstance(raw_presets, list) else DEFAULT_MATRIX_BUILD_PRESETS

    run_url = f"https://github.com/{REPO}/actions/runs/{run_id}"
    job_links = _fetch_job_links(run_id)
    matrix: List[Dict[str, Any]] = []

    for branch in branches:
        if not isinstance(branch, str) or not branch.strip():
            continue
        branch = branch.strip()
        for preset in build_presets:
            if not isinstance(preset, str) or not preset.strip():
                continue
            preset = preset.strip().lower()
            suffix = BUILD_PRESET_TO_PLATFORM_SUFFIX.get(preset, "")
            platform = f"ya-{branch}-x86-64{suffix}"
            try:
                report, report_url, html_url, _ = _get_report_by_run_id(
                    run_id, suite, platform, None, base_url
                )
            except Exception as e:
                matrix.append({
                    "branch": branch,
                    "build_preset": preset,
                    "platform": platform,
                    "failures_count": None,
                    "report_url": None,
                    "html_url": None,
                    "error": str(e),
                })
                continue
            if report is None:
                matrix.append({
                    "branch": branch,
                    "build_preset": preset,
                    "platform": platform,
                    "failures_count": None,
                    "report_url": None,
                    "html_url": None,
                    "error": "not_found",
                })
                continue
            failures, _ = _extract_failures_from_report(report)
            matrix.append({
                "branch": branch,
                "build_preset": preset,
                "platform": platform,
                "failures_count": len(failures),
                "report_url": report_url,
                "html_url": html_url,
            })

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "jsonrpc": "2.0",
            "result": {
                "run_id": run_id,
                "run_url": run_url,
                "job_links": job_links,
                "matrix": matrix,
            },
            "id": request_id,
        }),
    }


def _get_report_by_run_id(
    run_id: int,
    suite: str,
    platform: str,
    try_number: Optional[int],
    base_url: str
) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[str], Optional[int]]:
    """Получить report.json по run_id, возвращает (report, report_url, html_url, used_try)"""
    if try_number:
        try_numbers = [int(try_number)]
    else:
        try_numbers = [3, 2, 1]
    urls = _build_report_urls(base_url, suite, run_id, platform, try_numbers)
    for entry in urls:
        try:
            report = _fetch_json_url(entry['report_url'])
            return report, entry['report_url'], entry['html_url'], entry['try_number']
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                continue
            raise
        except Exception:
            raise
    return None, None, None, None


def _get_regression_report(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Один вызов: workflow + branch + build_preset → run_id → report.
    branch = ветка запуска workflow (фильтр API). test_branch = ветка, по которой гоняют тесты (platform в storage)."""
    workflow = (arguments.get('workflow') or '').strip()
    branch = (arguments.get('branch') or 'main').strip()
    test_branch = (arguments.get('test_branch') or '').strip() or branch
    build_preset = (arguments.get('build_preset') or 'relwithdebinfo').strip().lower()
    max_failures = arguments.get('max_failures', 200)

    if not workflow:
        return _error_response(request_id, -32602, 'workflow is required (e.g. Regression-run_Small_and_Medium, nightly_small, whitelist)')

    suite = workflow
    if workflow in ('nightly_small', 'nightly_large', 'whitelist', 'compatibility'):
        m = {'nightly_small': 'Regression-run_Small_and_Medium', 'nightly_large': 'Regression-run_Large',
             'whitelist': 'Regression-whitelist-run', 'compatibility': 'Regression-run_compatibility'}
        suite = m[workflow]
    elif workflow in WORKFLOW_NAME_TO_FILE:
        pass
    else:
        suite = WORKFLOW_NAME_TO_FILE.get(workflow, workflow)

    wf_file = _resolve_workflow_id(suite)
    try:
        runs_data = _github_get(
            f"https://api.github.com/repos/{REPO}/actions/workflows/{wf_file}/runs",
            params={'branch': branch, 'per_page': 1, 'status': 'completed'}
        )
    except Exception as e:
        return _error_response(request_id, -32603, f'Failed to get workflow runs: {str(e)}')

    runs = runs_data.get('workflow_runs', []) or []
    if not runs:
        return _error_response(request_id, -32602, f'No completed runs for workflow={workflow}, branch={branch}')

    run_id = runs[0].get('id')
    if not run_id:
        return _error_response(request_id, -32602, 'No run id in workflow runs')

    suffix = BUILD_PRESET_TO_PLATFORM_SUFFIX.get(build_preset, '')
    platform = f"ya-{test_branch}-x86-64{suffix}"

    return _get_run_report({
        'run_id': run_id,
        'suite': suite,
        'platform': platform,
        'max_failures': max_failures,
        'workflow_branch': branch,
        'test_branch': test_branch,
    }, request_id)


def _compare_with_previous_run(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Сравнить последний и предпоследний run по workflow+branch. По умолчанию main, relwithdebinfo.
    Если since/until — только запуски за эти даты. Берёт run через API (не run_id - 1)."""
    workflow = (arguments.get('workflow') or '').strip()
    branch = (arguments.get('branch') or 'main').strip()
    test_branch = (arguments.get('test_branch') or '').strip() or branch
    build_preset = (arguments.get('build_preset') or 'relwithdebinfo').strip().lower()
    since = (arguments.get('since') or '').strip()
    until = (arguments.get('until') or '').strip()
    max_new_failures = arguments.get('max_new_failures', 200)
    max_fixed_failures = arguments.get('max_fixed_failures', 200)

    if not workflow:
        return _error_response(request_id, -32602, 'workflow is required (e.g. Regression-run_Small_and_Medium, nightly_small, whitelist)')

    suite = workflow
    if workflow in ('nightly_small', 'nightly_large', 'whitelist', 'compatibility'):
        m = {'nightly_small': 'Regression-run_Small_and_Medium', 'nightly_large': 'Regression-run_Large',
             'whitelist': 'Regression-whitelist-run', 'compatibility': 'Regression-run_compatibility'}
        suite = m[workflow]
    elif workflow in WORKFLOW_NAME_TO_FILE:
        pass
    else:
        suite = WORKFLOW_NAME_TO_FILE.get(workflow, workflow)

    wf_file = _resolve_workflow_id(suite)
    params: Dict[str, Any] = {'branch': branch, 'status': 'completed'}
    if since and until:
        params['created'] = f'{since}..{until}'
        params['per_page'] = 100
    else:
        params['per_page'] = 2

    try:
        runs_data = _github_get(
            f"https://api.github.com/repos/{REPO}/actions/workflows/{wf_file}/runs",
            params=params
        )
    except Exception as e:
        return _error_response(request_id, -32603, f'Failed to get workflow runs: {str(e)}')

    runs = runs_data.get('workflow_runs', []) or []
    if len(runs) < 2:
        msg = (
            f'Need at least 2 completed runs for workflow={workflow}, branch={branch}. Found {len(runs)}. '
            'Previous run is taken from API, not run_id - 1.'
        )
        if since and until:
            msg += f' Date range: {since}..{until}.'
        return _error_response(request_id, -32602, msg)

    current_run_id = runs[0].get('id')
    previous_run_id = runs[1].get('id')
    if not current_run_id or not previous_run_id:
        return _error_response(request_id, -32602, 'Missing run id in workflow runs')

    suffix = BUILD_PRESET_TO_PLATFORM_SUFFIX.get(build_preset, '')
    platform = f"ya-{test_branch}-x86-64{suffix}"

    return _compare_run_reports({
        'current_run_id': current_run_id,
        'previous_run_id': previous_run_id,
        'suite': suite,
        'platform': platform,
        'max_new_failures': max_new_failures,
        'max_fixed_failures': max_fixed_failures,
    }, request_id)


def _resolve_run_id(run_id: Optional[Any], job_id: Optional[Any]) -> Tuple[Optional[int], Optional[str]]:
    """Получить run_id: либо напрямую, либо из job_id через API."""
    if run_id is not None:
        try:
            return int(run_id), None
        except (TypeError, ValueError):
            return None, 'Invalid run_id'
    if job_id is not None:
        try:
            jid = int(job_id)
        except (TypeError, ValueError):
            return None, 'Invalid job_id'
        rid = _get_run_id_from_job_id(jid)
        if rid is None:
            return None, f'Could not resolve run_id from job_id={jid}'
        return rid, None
    return None, None


def _compare_run_reports(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Сравнить два report.json и найти новые/исправленные падения. В путях storage используется run_id."""
    current_report_url = arguments.get('current_report_url', '')
    previous_report_url = arguments.get('previous_report_url', '')
    current_run_id = arguments.get('current_run_id')
    previous_run_id = arguments.get('previous_run_id')
    current_job_id = arguments.get('current_job_id')
    previous_job_id = arguments.get('previous_job_id')
    suite = arguments.get('suite', '')
    platform = arguments.get('platform', '')
    current_try = arguments.get('current_try')
    previous_try = arguments.get('previous_try')
    max_new_failures = arguments.get('max_new_failures', 200)
    max_fixed_failures = arguments.get('max_fixed_failures', 200)
    base_url = arguments.get('base_url') or f"https://storage.yandexcloud.net/ydb-gh-logs/{REPO}"

    try:
        max_new_failures = max(1, min(int(max_new_failures), 1000))
        max_fixed_failures = max(1, min(int(max_fixed_failures), 1000))
    except (TypeError, ValueError):
        max_new_failures = 200
        max_fixed_failures = 200

    try:
        if current_report_url:
            current_report = _fetch_json_url(current_report_url)
            current_report_url_used = current_report_url
            current_html_url = current_report_url.replace('/report.json', '/ya-test.html')
        else:
            rid, err = _resolve_run_id(current_run_id, current_job_id)
            if err:
                return _error_response(request_id, -32602, err)
            if not rid or not suite or not platform:
                return _error_response(request_id, -32602, 'current_report_url or (current_run_id or current_job_id) + suite + platform required')
            current_report, current_report_url_used, current_html_url, _ = _get_report_by_run_id(
                rid, suite, platform, current_try, base_url
            )
            if not current_report:
                return _error_response(request_id, -32602, f'Current report not found for run_id={rid}')

        if previous_report_url:
            previous_report = _fetch_json_url(previous_report_url)
            previous_report_url_used = previous_report_url
            previous_html_url = previous_report_url.replace('/report.json', '/ya-test.html')
        else:
            rid, err = _resolve_run_id(previous_run_id, previous_job_id)
            if err:
                return _error_response(request_id, -32602, err)
            if not rid or not suite or not platform:
                return _error_response(request_id, -32602, 'previous_report_url or (previous_run_id or previous_job_id) + suite + platform required')
            previous_report, previous_report_url_used, previous_html_url, _ = _get_report_by_run_id(
                rid, suite, platform, previous_try, base_url
            )
            if not previous_report:
                return _error_response(request_id, -32602, f'Previous report not found for run_id={rid}')

        # Извлекаем failures
        current_failures, current_muted = _extract_failures_from_report(current_report)
        previous_failures, previous_muted = _extract_failures_from_report(previous_report)

        # Создаем множества для быстрого поиска (по id или комбинации path+name+subtest_name)
        def _failure_key(f: Dict[str, Any]) -> str:
            fid = f.get('id')
            if fid:
                return f"id:{fid}"
            return f"path:{f.get('path', '')}|name:{f.get('name', '')}|subtest:{f.get('subtest_name', '')}"

        previous_keys = {_failure_key(f) for f in previous_failures}
        current_keys = {_failure_key(f) for f in current_failures}

        # Новые падения (есть в current, нет в previous)
        new_failures = [f for f in current_failures if _failure_key(f) not in previous_keys]
        # Исправленные падения (есть в previous, нет в current)
        fixed_failures = [f for f in previous_failures if _failure_key(f) not in current_keys]

        result = {
            'current': {
                'report_url': current_report_url_used,
                'html_url': current_html_url,
                'failures_count': len(current_failures),
                'muted_count': len(current_muted)
            },
            'previous': {
                'report_url': previous_report_url_used,
                'html_url': previous_html_url,
                'failures_count': len(previous_failures),
                'muted_count': len(previous_muted)
            },
            'comparison': {
                'new_failures_count': len(new_failures),
                'fixed_failures_count': len(fixed_failures),
                'new_failures': new_failures[:max_new_failures],
                'fixed_failures': fixed_failures[:max_fixed_failures]
            }
        }

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'jsonrpc': '2.0', 'result': result, 'id': request_id})
        }
    except Exception as e:
        return _error_response(request_id, -32603, f'Error comparing reports: {str(e)}')


def _extract_failures_from_report(report: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Вернуть списки падений и muted тестов. Только извлечённые поля, без полного отчёта."""
    failures = []
    muted = []
    failure_statuses = {'FAILED', 'ERROR', 'TIMEOUT', 'BROKEN', 'CRASHED'}

    for item in report.get('results', []) or []:
        status = item.get('status', '')
        is_muted = bool(item.get('muted')) or status == 'MUTE'
        links = item.get('links') or {}
        log_list = links.get('log') or []
        log_url = log_list[0] if log_list else None
        entry = {
            'id': item.get('id') or item.get('hid'),
            'name': item.get('name', ''),
            'subtest_name': item.get('subtest_name', ''),
            'path': item.get('path', ''),
            'status': status,
            'toolchain': item.get('toolchain', ''),
            'rich_snippet': item.get('rich-snippet', ''),
            'log_url': log_url,
        }
        if is_muted:
            muted.append(entry)
        elif status in failure_statuses:
            failures.append(entry)

    return failures, muted


def _download_artifact_json(artifact_url: str, file_name: str) -> Dict[str, Any]:
    """Скачать zip артефакта и извлечь JSON файл"""
    headers = _get_headers()
    resp = requests.get(artifact_url, headers=headers, timeout=30)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open(file_name) as f:
            return json.loads(f.read().decode('utf-8'))


def _compare_workflow_failures(arguments: Dict[str, Any], request_id: int) -> Dict[str, Any]:
    """Сравнить последние два запуска workflow и найти новые падения"""
    workflow = arguments.get('workflow')
    branch = arguments.get('branch', 'main')
    artifact_name_contains = arguments.get('artifact_name_contains', 'report_t')
    artifact_file_name = arguments.get('artifact_file_name', 'report_t.json')
    per_page = arguments.get('per_page', 10)

    if not workflow:
        valid = ', '.join(f"'{k}'" for k in list(WORKFLOW_NAME_TO_FILE.keys())[:4])
        return _error_response(
            request_id, -32602,
            f"workflow is required. Use display name (e.g. {valid}) or file name (e.g. regression_run_small_medium.yml)."
        )
    workflow = _resolve_workflow_id(workflow)

    try:
        per_page = max(1, min(int(per_page), 100))
    except (TypeError, ValueError):
        per_page = 10

    runs_url = f"https://api.github.com/repos/{REPO}/actions/workflows/{workflow}/runs"
    params = {'branch': branch, 'per_page': per_page, 'status': 'completed'}

    try:
        runs_data = _github_get(runs_url, params=params)
        runs = runs_data.get('workflow_runs', []) or []
        if len(runs) < 2:
            return _error_response(request_id, -32602, 'Not enough completed runs to compare')

        current_run = runs[0]
        previous_run = runs[1]

        def _find_artifact(run_id: int) -> Optional[Dict[str, Any]]:
            artifacts_url = f"https://api.github.com/repos/{REPO}/actions/runs/{run_id}/artifacts"
            artifacts_data = _github_get(artifacts_url, params=None)
            for artifact in artifacts_data.get('artifacts', []) or []:
                name = artifact.get('name', '')
                if artifact_name_contains in name:
                    return artifact
            return None

        current_artifact = _find_artifact(current_run.get('id'))
        previous_artifact = _find_artifact(previous_run.get('id'))

        if not current_artifact or not previous_artifact:
            return _error_response(
                request_id,
                -32602,
                f'Artifact not found (expected name containing "{artifact_name_contains}")'
            )

        current_report = _download_artifact_json(current_artifact.get('archive_download_url', ''), artifact_file_name)
        previous_report = _download_artifact_json(previous_artifact.get('archive_download_url', ''), artifact_file_name)

        current_failures, current_muted = _extract_failures_from_report(current_report)
        previous_failures, _ = _extract_failures_from_report(previous_report)

        prev_keys = {f.get('id') for f in previous_failures}
        new_failures = [f for f in current_failures if f.get('id') not in prev_keys]

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'jsonrpc': '2.0',
                'result': {
                    'workflow': workflow,
                    'branch': branch,
                    'current_run': {
                        'id': current_run.get('id'),
                        'html_url': current_run.get('html_url', ''),
                        'created_at': current_run.get('created_at', ''),
                        'conclusion': current_run.get('conclusion', '')
                    },
                    'previous_run': {
                        'id': previous_run.get('id'),
                        'html_url': previous_run.get('html_url', ''),
                        'created_at': previous_run.get('created_at', ''),
                        'conclusion': previous_run.get('conclusion', '')
                    },
                    'current_failures_count': len(current_failures),
                    'current_muted_count': len(current_muted),
                    'previous_failures_count': len(previous_failures),
                    'new_failures_count': len(new_failures),
                    'new_failures': new_failures
                },
                'id': request_id
            })
        }
    except Exception as e:
        return _error_response(request_id, -32603, f'Error comparing workflow failures: {str(e)}')


def _error_response(request_id: int, code: int, message: str) -> Dict[str, Any]:
    """Формирует JSON-RPC ошибку"""
    return {
        'statusCode': 200,  # JSON-RPC ошибки возвращаются с 200 OK
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({
            'jsonrpc': '2.0',
            'error': {
                'code': code,
                'message': message
            },
            'id': request_id
        })
    }
