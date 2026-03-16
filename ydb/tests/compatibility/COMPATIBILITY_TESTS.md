# Compatibility tests — подробная структура (outline)

Ниже — структурированный план/названия блоков руководства по compatibility-тестам. Я оставляю блоки с пометкой «REQUIRES FILL» — на следующих проходах заполняю блоки по очереди.

Цель: сфокусироваться только на специфике compatibility-тестов в этом репозитории — минимальный синтаксис теста, формат взаимодействия между версиями, детали фикстур/кластера, шаблоны надёжности и развитие теста.

Содержимое файла разбито на блоки. Каждый блок содержит краткую метку того, что требуется заполнить.


### Block 1 — Minimal compatibility test (skeleton)

(заполнено)

Ниже — минимальный рабочий тест, подходящий для compatibility-проверки в этом репозитории. Тест использует доступные фикстуры (`MixedClusterFixture`) и демонстрирует ключевые элементы: выбор узлов, HTTP-вызов к viewer-monitoring endpoint и `retry_assertions` для ожидания готовности.

```python
# -*- coding: utf-8 -*-
import json
import urllib.request
import pytest

from ydb.tests.library.common.wait_for import retry_assertions
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture


class TestMinimalCompatibility(MixedClusterFixture):
	 @pytest.fixture(autouse=True, scope="function")
	 def setup(self):
		  # Поднимает кластер, потом отдаёт управление тесту; teardown выполняется внутри setup_cluster()
		  yield from self.setup_cluster()

	 def test_basic_viewer_proxy(self):
		  # Получаем идентификаторы узлов из тестового кластера
		  node_ids = sorted(self.cluster.nodes.keys())
		  assert node_ids, "Expected at least one node in cluster"

		  # Берём самый простой сценарий: запрос к viewer на том же узле
		  node_id = node_ids[0]
		  node = self.cluster.nodes[node_id]
		  url = f"http://{node.host}:{node.mon_port}/node/{node_id}/viewer/json/nodes?timeout=10000"

		  def _check():
				with urllib.request.urlopen(url, timeout=5) as response:
					 status = response.getcode()
					 body = response.read().decode("utf-8", errors="replace")
				assert status == 200
				payload = json.loads(body)
				assert isinstance(payload, dict)
				assert payload.get("Nodes"), "Expected non-empty 'Nodes'"

		  # Ждём ответа с retries — устойчиво к медленному старту сервисов
		  retry_assertions(_check, timeout_seconds=20, step_seconds=1)
```

Построчный синтаксический разбор и пояснения:
- `import json`, `import urllib.request` — модули стандартной библиотеки для HTTP-запроса и парсинга JSON.
- `import pytest` — нужен для фикстур и маркировки теста.
- `from ydb.tests.library.common.wait_for import retry_assertions` — утилита для повторных утверждений с таймаутом; ключевая для стабильности compatibility-тестов.
- `from ydb.tests.library.compatibility.fixtures import MixedClusterFixture` — базовая фикстура/класс, который умеет поднимать тестовый кластер с узлами разной версии/бинарниками.
- `class TestMinimalCompatibility(MixedClusterFixture):` — наследование даёт доступ к `self.cluster`, `self.config` и методам управления окружением.
- `@pytest.fixture(autouse=True, scope="function")
  def setup(self): yield from self.setup_cluster()` — автозапуск подготовки кластера перед каждым тестом. `setup_cluster()` обычно стартует процессы и делает yield для теста; после теста выполняется очистка.
- `node_ids = sorted(self.cluster.nodes.keys())` — получаем список идентификаторов узлов; `self.cluster.nodes` — мапа `{node_id: node_object}`.
- `node.host`, `node.mon_port` — свойства узла: сетевой адрес и порт monitoring/viewer.
- Формирование `url` — стандартный viewer endpoint, ожидаемый в кластере.
- Внутри `_check()` производится реальный HTTP-вызов и базовые assert'ы: статус 200, JSON, наличие непустого поля `Nodes`.
- `retry_assertions(_check, timeout_seconds=20, step_seconds=1)` — повторяет `_check()` каждую секунду до успешного окончания или пока не истечёт 20 секунд.

Почему этот тест минимален, но актуален для compatibility:
- Использует реальные запущенные бинарники в тестовом кластере (через `MixedClusterFixture`), поэтому проверяет взаимодействие между версиями/компонентами, а не только unit-логику.
- Включает wait/retry, что важно для тестов, где сервисы инициализируются асинхронно.
- Проверяет семантику ответа (JSON + непустое поле), а не точную строковую подстроку.

Ограничения и упрощения в минимальном тесте:
- Тест не проверяет cross-node proxy (source != target); это можно добавить в развитии.
- Не делает детальных проверок структуры `Nodes` — минимально валидирует её наличие.

---

### Block 2 — How to evolve the test (development paths)

Ниже — практические пути развития минимального теста: быстрые улучшения, расширения для реальных compatibility-сценариев и идеи для CI-матриц.

- Cross-node / cross-version checks
	- Расширьте тест, чтобы проверять взаимодействие между разными узлами/версиями (source != target). В простейшем виде собирайте список пар `[(s,t), ...]` и выполняйте ассерты для каждой пары.
	- Стратегии матриц: pairwise (представительные пары), full matrix (все комбинации), targeted (бэкпорт-ключевые-версии).

- Параметризация и pytest
	- Используйте `pytest.mark.parametrize` чтобы запускать сценарий для списка пар/конфигураций.
	- Пример:

```python
@pytest.mark.parametrize("source_node,target_node", checks)
def test_cross_node(source_node, target_node):
		# reuse same check body as в минимальном тесте
		...
```

- Глубокая валидация payload
	- Вместо проверки непустого поля `Nodes` добавьте шаблоны/схему: проверка полей ноды, типов (id, state, metrics).
	- Используйте отдельные assertion-helpers (например, `assert_node_shape(node)`), чтобы ошибки были читаемыми.

- Негативные и edge-кейсы
	- Проверяйте поведение при недоступном target (закрытый порт, SIGTERM процесса), при частичных данных или устаревших форматах.
	- Подумайте про версионированные форматы: когда старый/новый компонент возвращает разные структуры, тест должен фиксировать допустимые несовпадения.

- Fault injection / chaos
	- Инъектируйте задержки сети, потерю пакетов или искусственные исключения в зависимые сервисы, чтобы проверить устойчивость proxy/adapter.

- Нагрузочные сценарии и тайминги
	- Добавьте тесты, которые отправляют много параллельных запросов, измеряют latency/throughput и сравнивают с порогами.
	- Поместите такие тесты в отдельную категорию (`slow`/`perf`) и запускайте в CI по расписанию, а не на каждый PR.

- Консистентность и stateful сценарии
	- Тесты миграций/upgrade: стартуйте кластер с версией A, выполните записи/операции, обновите узлы до версии B и проверьте, что viewer/proxy корректно обрабатывает legacy-данные.

- Параллелизм тестов и ресурсы
	- Для heavy/parallel сценариев используйте выделенные CI-агенты с контролем ресурсов; пометьте тесты тегами (`@pytest.mark.skipif`) при недостатке среды.

- Репортинг и метрики
	- Собирайте метрики (latency, error-rate) и сохраняйте артефакты в CI; при регрессии автоматически поднимайте баг.

- Параметры конфигурации и флаги
	- Делайте тесты управляемыми через env-переменные: включение/выключение matrix, таймауты, уровень логирования.

Короткие примеры расширений (псевдо):

1) Cross-node matrix (in-test build):

```python
node_ids = sorted(self.cluster.nodes.keys())
checks = [(s, t) for s in node_ids for t in node_ids if s != t]
for s, t in checks:
		retry_assertions(lambda: check_pair(s, t), timeout_seconds=30)
```

2) Parametrized pytest case (better for test reports):

```python
@pytest.mark.parametrize("pair", checks)
def test_pair(pair):
		s, t = pair
		retry_assertions(lambda: check_pair(s, t), timeout_seconds=30)
```

3) Upgrade/migration scenario outline:

```text
- Start cluster with version A
- Run workloads / write test data
- Upgrade selected nodes to version B
- Verify viewer / proxy responses for both old and new nodes
```

---

(Следующий шаг: заполню Block 3 — разбор фикстур и деталей кластера.)

### Block 3 — Fixtures and cluster specifics

Этот блок описывает типовые фикстуры в `ydb/tests/compatibility`, поведение `MixedClusterFixture` и паттерны написания собственных фикстур для compatibility-тестов.

1) `MixedClusterFixture` — что ожидать
- Назначение: поднять тестовый кластер из нескольких узлов (возможно, с разными бинарниками/версиями) и предоставить удобные атрибуты для теста.
- Основные доступные поля/методы (обычные в реализации подобных фикстур):
	- `self.cluster` — объект-контейнер с полем `nodes` (dict: node_id -> node_object).
	- `self.config` — helper для получения путей к бинарникам/конфигурации (например, `self.config.get_binary_path(node_id)`).
	- `self.setup_cluster()` — метод, который стартует все процессы/узлы и `yield`-ит контроль тесту; после `yield` происходит teardown (останавливает процессы и очищает артефакты).
	- `node_object` обычно имеет свойства: `host`, `mon_port` (monitor/viewer порт), `pid`, `log_file_path`, `data_dir`.

2) Жизненный цикл фикстуры
- `setup_cluster()` чаще всего реализован как генераторная фикстура: подготовка → `yield` → очистка. Тест получает готовый кластер сразу после `yield`.
- Важно: фикстура должна быть детерминированной — одинаковые порты/директории для повторяемости либо выделять динамически и возвращать значения.

3) Как писать собственную фикстуру (шаблон)

```python
import pytest

@pytest.fixture(scope="function")
def my_cluster_fixture(tmp_path):
		# подготовка (создать dirs, конфиги)
		cluster = MyClusterBuilder(tmp_path)
		cluster.start_nodes()
		try:
				# дождаться готовности (health checks)
				retry_assertions(lambda: assert_cluster_ready(cluster), timeout_seconds=30)
				yield cluster
		finally:
				cluster.stop_nodes()
				cluster.cleanup()
```

- Рекомендации:
	- Всегда окружать start/stop try/finally, чтобы предотвратить утечки процессов.
	- Ждать готовности через health endpoint или `retry_assertions`.
	- Использовать `tmp_path`/`tmpdir` для данных, чтобы CI чисто изолировал артефакты.

4) Управление версиями бинарников внутри фикстуры
- Часто `MixedClusterFixture` принимает mapping node_id -> binary_path или config предоставляет `get_binary_path(node_id)`.
- В тесте можно получить путь бинарника и сгруппировать узлы по бинарнику, чтобы затем формировать representative nodes matrix (см. Block 4).

5) Работа с узлами (полезные утилиты)
- `cluster.nodes` — словарь; типичный `node` предоставляет:
	- `node.host` — адрес для HTTP-вызовов
	- `node.mon_port` — порт monitoring/viewer
	- `node.pid` — PID процесса
	- `node.log_file_path` — путь к лог-файлу
	- `node.data_dir` — рабочая директория
- Полезные операции в фикстуре: `start_node(node_id)`, `stop_node(node_id, wait=True)`, `restart_node(node_id)`.

6) Ожидания и health checks
- Нельзя полагаться на мгновенную готовность процесса: всегда делать health-checks. Примеры:
	- HTTP `GET /health` или `GET /node/<id>/viewer/json/nodes` с `retry_assertions`.
	- Проверка, что процесс жив (`ps`/`pid` exists) и порт слушается.

7) Частые ошибки и как их избежать
- Неочищенные процессы и занятые порты: всегда останавливайте процессы в `finally` и используйте динамические порты при параллельных запусках.
- Зависание в `setup`: устанавливайте разумные таймауты и логируйте шаги подготовки.
- Неправильные пути к бинарникам: используйте `self.config`/параметры фикстуры для явного указания версий.

8) Примеры полезных helper-функций

```python
def get_nodes_by_binary(cluster):
		by_bin = {}
		for nid, node in cluster.nodes.items():
				binpath = cluster.config.get_binary_path(nid)
				by_bin.setdefault(binpath, []).append(nid)
		return by_bin

def assert_cluster_ready(cluster):
		for nid, node in cluster.nodes.items():
				url = f"http://{node.host}:{node.mon_port}/node/{nid}/viewer/json/nodes?timeout=1000"
				with urllib.request.urlopen(url, timeout=2) as r:
						if r.getcode() != 200:
								raise AssertionError("node not ready")
		return True
```

---

Block 3 заполнен. Следующий шаг: заполню Block 4 — strategy для version mapping и representative nodes.

### Block 4 — Version mapping and representative nodes

Этот блок описывает стратегии выбора `representative nodes` для cross-version compatibility-проверок и способы контролировать mapping версия->узел в тестах/CI.

1) Задача
- В compatibility-тестах часто нужно проверить взаимодействие между разными версиями бинарников. Запускать полную матрицу для каждого PR дорого, поэтому выбирают небольшое множество *representative nodes* — по одному (или нескольким) узлам на каждую значимую комбинацию бинарников/версий.

2) Стратегии маппинга
- Group-by-binary-path (простая и эффективная): группируете узлы по `binary_path` (или по `self.config.get_binary_path(node_id)`) и берёте по одному узлу из каждой группы.
- Label-based selection: если в фикстуре/конфигурации есть метаданные/теги версии, выбирайте узлы по semantic version tags (например, `v1.2.3`, `latest`, `stable`).
- Targeted selection: вручную указываете пары, которые важны (например, stable vs latest, oldest supported vs latest).
- Canary-based: для сложных изменений добавляете пару только для одного узла в каждой группе, затем расширяете матрицу при проблемах.

3) Пример кода: representative nodes (как в `test_viewer_monitoring_proxy.py`)

```python
representative_node_by_binary = {}
for node_id in node_ids:
		binpath = self.config.get_binary_path(node_id)
		representative_node_by_binary.setdefault(binpath, node_id)
representative_nodes = list(representative_node_by_binary.values())

# representative_nodes содержит по одному node_id для каждого уникального бинарника
```

4) Формирование проверок (checks)
- Если `len(representative_nodes) >= 2`, формируйте пары всех комбинаций между representative nodes (pairwise), иначе используйте первые два реальных узла.
- Альтернативы:
	- Full matrix: все узлы × все узлы (дорого в CI).
	- Pairwise across major versions: перебирайте комбинации major→major (уменьшает матрицу).

5) Как закреплять версии/бинарники для стабильности теста
- Явное указание в тесте или в фикстуре: `fixture` может принимать словарь `node_id -> binary_path` или `version_map`.
- CI-автоматика: в pipeline можно задавать переменные окружения/артефакты, указывающие конкретные билды для тестов.
- Документирование: в шапке теста указывайте, какие версии должны быть доступны в среде CI (или условно пропускать тест, если нужные билды отсутствуют).

6) Примеры расширения матриц
- Тегирование тестов: `@pytest.mark.matrix('smoke','full')` и запускать `smoke` в PR, `full` — nightly.
- Генерация матрицы в CI: на этапе подготовки создавайте `checks` по обнаруженным бинарникам и запускайте параллельные job'ы для каждой пары.

7) Практические советы
- Предпочитайте deterministic selection: выбор representative nodes должен быть одинаковым при повторных запусках.
- Логируйте mapping при старте теста: выводьте, какие бинарники и node_id были сопоставлены, это облегчает дебаг.
- Если в группе несколько узлов одного бинарника используются для load/replica testing, выбирайте узел с наименьшими побочными эффектами (например, без тестовых данных).

---


### Block 5 — Reliability patterns (retry_assertions, health-checks, timeouts, teardown)

Цель блока: дать конкретные практики и шаблоны для надёжных compatibility-тестов — как ждать готовности сервисов, какие таймауты и шаги использовать, и как безопасно убирать ресурсы по завершении.

1) Основная идея
- Compatibility-тесты работают с реальными процессами и сетью — поэтому важно предусмотреть ожидания, retries и аккуратный teardown.
- Не превращайте retries в «маскировку» флаков: используйте retry для естественной асинхронной инициализации, но логируйте и фиксируйте повторы.

2) `retry_assertions` — шаблон использования
- Базовый вызов:

```python
retry_assertions(check_fn, timeout_seconds=30, step_seconds=1)
```

- Рекомендации по параметрам:
	- `step_seconds`: 0.5–2 секунды (обычно 1s) — не ставьте слишком маленькие значения, чтобы не перегрузить систему.
	- `timeout_seconds`: для легких endpoint'ов 20–30s, для полного кластера/upgrade 60–300s (в зависимости от CI-профиля).

3) Health-check patterns
- Небольшие рекомендации:
	- Проверяйте легкие health endpoints (`/health`, `/ready`) или специфичные viewer endpoints с минимальным payload.
	- Комбинируйте проверки: процесс жив (pid), порт слушает, http 200 и семантическая проверка body.
	- Используйте backoff в проверках, если инициализация может быть длительной.

Пример health-check helper:

```python
def wait_node_ready(node, timeout_seconds=30):
		url = f"http://{node.host}:{node.mon_port}/node/{node.id}/viewer/json/nodes?timeout=1000"
		retry_assertions(lambda: assert_node_ok(url), timeout_seconds=timeout_seconds, step_seconds=1)

def assert_node_ok(url):
		with urllib.request.urlopen(url, timeout=2) as r:
				assert r.getcode() == 200
				payload = json.loads(r.read().decode('utf-8', errors='replace'))
				assert 'Nodes' in payload
```

4) Teardown / cleanup
- Всегда останавливайте процессы в блоке `finally` фикстур. Примеры:

```python
try:
		cluster.start_nodes()
		yield cluster
finally:
		cluster.stop_nodes(wait=True)
		cluster.cleanup()
```

- Дополнительно:
	- Заставляйте остановку через SIGTERM с таймаутом, затем SIGKILL, если процесс не завершился.
	- Собирать логи перед удалением директорий.

5) Таймауты и SLA в тестах
- Разделяйте «функциональные» таймауты и «инфраструктурные» таймауты:
	- Функциональные (assert) — короткие, чтобы быстро поймать регрессивные ошибки.
	- Инфраструктурные (стартап/upgrade) — длиннее, зависят от CI-агентов и окружения.

6) Retry anti-patterns
- Не используйте бесконечные retries без лимита.
- Не увеличивайте `timeout_seconds` бессмысленно — фиксируйте и анализируйте флаки.

7) Логирование и видимость
- Логируйте попытки и последний проваленный ответ при окончательном падении `retry_assertions`.
- В CI собирайте stdout/stderr процессов и файлы логов из `node.log_file_path`.

8) Примеры параметров, которые можно хранить в env/config
- `COMPAT_TEST_RETRY_STEP=1` — шаг retry
- `COMPAT_TEST_RETRY_TIMEOUT=30` — базовый timeout
- `COMPAT_TEST_STARTUP_TIMEOUT=120` — для стартапа кластера

9) Контроль flaky-тестов
- При обнаружении flake: сначала увеличить логирование, запустить локально с теми же бинарниками, воспроизвести и исправить причину. Не «размазывайте» flakes бесконечными retry.

---

Block 5 заполнен. Следующий шаг: заполню Block 6 (Debugging and logs).

### Block 6 — Debugging and logs

Этот блок объясняет, какие логи и артефакты собирать при падении compatibility-теста, как быстро воспроизвести проблему локально и какие инструменты применять для глубокого анализа.

1) Какие артефакты собирать (обязательно в CI)
- stdout/stderr процессов (сервисы, queuing runners).
- Логи каждого узла: `node.log_file_path`.
- HTTP request/response тела для упавших проверок.
- Конфигурационные файлы и используемые бинарники (путь и checksum).
- Core dumps / stack traces, если процесс упал с SIGSEGV.
- Метрики (если есть): latency, error-rate, resource usage.

2) Быстрая локальная отладка (шаги)
- Запустить проблемный тест локально:

```bash
pytest ydb/tests/compatibility/test_viewer_monitoring_proxy.py::test_basic_viewer_proxy -q -k test_basic_viewer_proxy
```

- Открыть логи узла в реальном времени:

```bash
tail -n 200 -f /path/to/node.log
```

- Повторять HTTP-вызов ручно, чтобы увидеть поведение на низком уровне:

```bash
curl -v "http://$HOST:$PORT/node/$ID/viewer/json/nodes?timeout=1000"
```

- Если нужно — включить подробный вывод в тесте (увеличить level логирования через env-переменные/флаги).

3) Сбор логов в фикстуре перед teardown

Добавляйте в `finally` фикстуры код, который при падении теста копирует или печатает последние N строк логов узлов:

```python
try:
	yield cluster
finally:
	for nid, node in cluster.nodes.items():
		with open(node.log_file_path, 'r') as f:
			print(f"--- log {nid} (last 200 lines) ---")
			print('\n'.join(deque(f, maxlen=200)))
	cluster.stop_nodes()
```

4) Инструменты для сетевой/системной отладки
- `tcpdump` / `tshark` — ловить сетевой трафик между узлами.
- `strace` — трассировка системных вызовов процесса.
- `gdb` — получить бэктрейс при падении, или подключиться к живому процессу.
- `ss` / `netstat` — проверить слушающие порты.

5) Как прикладывать артефакты к баг-репорту
- Приложите:
  - последние 200 строк логов каждого узла
  - HTTP request/response (curl -v output)
  - mapping node_id -> binary path/sha
  - CI job id + environment variables
  - core dump или stack trace (если есть)

6) Практики повышения полезности логов
- Включайте correlation-id / request-id в запросы, чтобы связывать логи разных сервисов.
- Пишите в лог контекст теста (test name, node_id, run id) при старте фикстуры.
- Используйте структурированные логи (JSON) если CI умеет их парсить.

7) Быстрая диагностика flaky-теста
- Повторите тест в loop локально, соберите статистику failure rate.
- Увеличьте логирование и включите timing/debug flags.
- Постепенно изолируйте внешние зависимости (моки) чтобы понять, где flake возникает.

---

Block 6 заполнен. Следующий шаг: заполню Block 7 (аннотированный пример теста) по `test_viewer_monitoring_proxy.py`.

### Block 7 — Annotated example (the test file walkthrough)

Ниже — построчная аннотация файла `ydb/tests/compatibility/test_viewer_monitoring_proxy.py`. Комментарии объясняют назначение каждой части, возможные подводные камни и варианты расширения.

1) Заголовок и импорты

```python
# -*- coding: utf-8 -*-
import json
import pytest
import urllib.request

from ydb.tests.library.common.wait_for import retry_assertions
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture
```

- `json`, `urllib.request` — стандартная библиотека для HTTP-запросов и парсинга ответа.
- `retry_assertions` — ключевой helper для устойчивости (см. Block 5).
- `MixedClusterFixture` — поднимает кластер разной версии/бинарников (см. Block 3).

2) Класс теста и фикстура setup

```python
class TestViewerMonitoringProxyCompatibility(MixedClusterFixture):
	@pytest.fixture(autouse=True, scope="function")
	def setup(self):
		yield from self.setup_cluster()
```

- Наследование от `MixedClusterFixture` даёт `self.cluster`, `self.config` и методы управления окружением.
- `autouse=True` гарантирует, что кластер поднимается для каждого теста автоматически.
- `yield from self.setup_cluster()` — генератор: setup → тест → teardown.

3) Низкоуровневый HTTP helper

```python
	def _request_node_viewer_nodes(self, source_node_id, target_node_id):
		source_node = self.cluster.nodes[source_node_id]
		url = f"http://{source_node.host}:{source_node.mon_port}/node/{target_node_id}/viewer/json/nodes?timeout=10000"
		with urllib.request.urlopen(url, timeout=10) as response:
			status = response.getcode()
			body = response.read().decode("utf-8", errors="replace")
		return status, body, json.loads(body)
```

- Функция делает запрос с `source_node` к viewer-прокси, указывающему `target_node_id`.
- `timeout` в `url` и `urllib` — разные уровни таймаута; полезно иметь оба.
- `errors="replace"` защищает от бинарных символов в логах.

4) Ассерты ответа

```python
	def _assert_node_viewer_nodes_works(self, source_node_id, target_node_id):
		status, body, payload = self._request_node_viewer_nodes(source_node_id, target_node_id)
		assert status == 200, (
			f"Unexpected HTTP status for /node/{target_node_id}/viewer/json/nodes "
			f"from node {source_node_id}: {status}"
		)
		assert isinstance(payload, dict), (
			f"Expected JSON object for /node/{target_node_id}/viewer/json/nodes "
			f"from node {source_node_id}, got: {body}"
		)
		assert "Nodes" in payload and isinstance(payload["Nodes"], list), (
			f"Expected 'Nodes' list in /node/{target_node_id}/viewer/json/nodes "
			f"from node {source_node_id}, got: {body}"
		)
		assert payload["Nodes"], (
			f"Expected non-empty 'Nodes' list in /node/{target_node_id}/viewer/json/nodes "
			f"from node {source_node_id}, got: {body}"
		)
```

- Сообщения assert'ов содержат контекст (source/target/body) — это облегчает отладку при падении.
- Можно расширить проверки содержания `Nodes` (см. Block 2).

5) Основной тестовый сценарий

```python
	def test_cross_node_viewer_proxy(self):
		node_ids = sorted(self.cluster.nodes.keys())
		assert len(node_ids) >= 2, "Expected at least 2 nodes in compatibility cluster"

		representative_node_by_binary = {}
		for node_id in node_ids:
			representative_node_by_binary.setdefault(self.config.get_binary_path(node_id), node_id)

		representative_nodes = list(representative_node_by_binary.values())
		if len(representative_nodes) >= 2:
			checks = []
			for source_node_id in representative_nodes:
				for target_node_id in representative_nodes:
					if source_node_id != target_node_id:
						checks.append((source_node_id, target_node_id))
		else:
			checks = [(node_ids[0], node_ids[1])]

		for source_node_id, target_node_id in checks:
			retry_assertions(
				lambda source_node_id=source_node_id, target_node_id=target_node_id: self._assert_node_viewer_nodes_works(source_node_id, target_node_id),
				timeout_seconds=30,
				step_seconds=1,
			)
```

- Сначала убеждаемся в наличии как минимум двух узлов — ключевое предположение для cross-node проверок.
- `representative_node_by_binary` — стратегия выборки representative nodes: по одному узлу на каждый бинарник (см. Block 4).
- Если достаточно representative nodes, формируется matrix pairwise; иначе fallback на первые два узла.
- Для каждой пары используется `retry_assertions` — так тест устойчив к временным задержкам старта сервисов (см. Block 5).

6) Потенциальные расширения и тонкости

- Параметризация: можно заменить внутренний цикл на `pytest.mark.parametrize` для лучшей трассируемости в отчетах (см. Block 2).
- Диагностика: при неуспехе распечатать последние строки логов `source_node` и `target_node` (см. Block 6) — полезно в `except` или в фикстуре teardown.
- Security/permissions: если viewer требует auth, добавьте headers/credentials в `_request_node_viewer_nodes`.
- Timeout tuning: `retry_assertions` timeout и шаги стоит вынести в конфиг/env переменные.

7) Частые источники проблем

- Неправильный `mon_port` или конфликт портов при параллельных запусках — используйте динамические порты и логируйте их при старте.
- Бинарник другой версии может возвращать несовместимую структуру JSON — в таких случаях тест должен различать «совместимая семантика» и «точное совпадение формата».

---

Block 7 заполнен. Следующий шаг: заполню Block 8 (чек-лист и примечания для PR).

### Block 8 — Checklist & PR notes for adding compatibility tests

(требует заполнения)


---

Порядок работы (что будет сделано дальше):
- Я заполняю блок 1 (Minimal compatibility test + синтаксический разбор) в следующем сообщении.
- По завершении блока 1 вы получите полный код минимального теста и разбор каждой строки; затем могу переходить к блоку 2 и далее по порядку.

Если хотите поменять порядок блоков или добавить специфические темы (например, работа с контейнерами, специфичные network hooks, или интеграция с конкретным CI), скажите, и я скорректирую порядок.

