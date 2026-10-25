"""Textual dashboard for blobsan-service's local console."""

import json
import shlex
import time

from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.widgets import Button, Collapsible, DataTable, Footer, Header, Input, RichLog, Select, Static, TabbedContent, TabPane, Tabs

from ydb.apps.dstool.lib import write2x2_report
from ydb.apps.dstool.tui.connection import ConsoleConnection, ConsoleError, CommandError


ACTIVE_GC = {'soft_with_keep', 'no_covering_barrier'}
DISCARDED_GC = write2x2_report.GC_STATES - ACTIVE_GC


def age(seconds):
    if seconds is None:
        return '-'
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f'{seconds}s'
    if seconds < 3600:
        return f'{seconds // 60}m'
    if seconds < 86400:
        return f'{seconds // 3600}h {seconds % 3600 // 60}m'
    return f'{seconds // 86400}d {seconds % 86400 // 3600}h'


def since(timestamp):
    return '-' if timestamp is None else age(time.time() - float(timestamp))


def size(value):
    for unit in ('B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'):
        if value < 1024 or unit == 'PiB':
            return f'{value:,.0f} B' if unit == 'B' else f'{value:,.1f} {unit}'
        value /= 1024


def annotation(buckets, key):
    total = sum(b['blobs'] for b in buckets)
    yes = sum(b['blobs'] for b in buckets if b.get(key) is True)
    unknown = sum(b['blobs'] for b in buckets if b.get(key) is None)
    if unknown:
        return 'unknown' if unknown == total else f'{yes:,} yes; {unknown:,} unknown'
    return 'yes' if yes == total else 'no' if not yes else f'mixed ({yes:,}/{total:,})'


def json_rows(response):
    return [] if response == '(empty)' else [json.loads(line) for line in response.splitlines()]


class ConsoleApp(App):
    TITLE = 'blobsan-service'
    BINDINGS = [
        Binding('ctrl+q', 'quit', 'Quit'),
        Binding('f5', 'refresh', 'Refresh'),
        Binding('ctrl+l', 'command', 'Command'),
        Binding('left_square_bracket', 'previous_tab', 'Previous tab', show=False),
        Binding('right_square_bracket', 'next_tab', 'Next tab', show=False),
    ]
    CSS = """
    Screen { background: $surface; }
    #status, #summary { height: auto; max-height: 3; padding: 0 1; }
    #status { color: $success; }
    #status.error { color: $error; }
    #toolbar { height: 3; padding: 0 1; }
    #toolbar Button { min-width: 12; margin-right: 1; }
    #views { height: 1fr; }
    #views Tab { color: $foreground; text-style: not dim; }
    #views Tab.-active { text-style: bold; }
    #views Tabs:focus Tab.-active {
        color: $block-cursor-foreground;
        text-style: $block-cursor-text-style;
    }
    TabPane { padding: 0 1; }
    DataTable, RichLog { height: 1fr; }
    .hint { height: auto; color: $text-muted; }
    #detail { height: auto; max-height: 7; overflow-y: auto; }
    #layout-filter { width: 55; }
    #gc-table { height: 5; }
    #bucket-table { height: 10; }
    #gc-summary, #bucket-details { height: auto; padding: 0; }
    #command { dock: bottom; }
    """
    COLUMNS = {
        'pool-table': ('Pool', 'Key', 'Reports', 'Full', 'Partial', 'Never', 'Outdated', 'Stale',
                       'Failed', 'Blobs', 'Size', 'Oldest'),
        'group-table': ('Group', 'Pool', 'Generation', 'Scan age', 'Candidates', 'Worker', 'Due', 'Error'),
        'layout-table': ('Local2', 'Blobs', 'Size', 'phantom_like', 'write2x2-candidate'),
        'gc-table': ('GC', 'Blobs', 'Size'),
        'bucket-table': ('Local', 'Local3', 'Ingress', 'GC', 'Blobs', 'Size', 'Replica size', 'Missing metadata',
                         'phantom_like', 'write2x2-candidate'),
        'worker-table': ('Worker', 'Last seen', 'Info'),
        'finding-table': ('Group', 'Blob', 'Status', 'First seen', 'Last seen', 'Coverage'),
    }

    def __init__(self, path, refresh=5, timeout=10, *, connection=None):
        super().__init__()
        self.connection = connection or ConsoleConnection(path, timeout)
        self.sub_title = self.connection.path
        self.refresh_seconds = refresh
        self.detail_target = None
        self.stats = {}
        self._refreshing = self._refresh_again = self._command_busy = self._client_closing = False
        self._connected = False
        self._layout_data = None
        self._selected_layout = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True, icon='>')
        yield Static('Connecting…', id='status', markup=False)
        yield Static('', id='summary', markup=False)
        with Horizontal(id='toolbar'):
            yield Button('Refresh (F5)', id='reload')
            yield Button('Pause', id='pause', disabled=True)
            yield Button('Scan all', id='scan', disabled=True)
            yield Button('Inventory', id='inventory', disabled=True)
        with TabbedContent(initial='pools', id='views'):
            with TabPane('Pools', id='pools'):
                yield Static('Enter on a pool opens its histogram.', classes='hint')
                yield DataTable(id='pool-table', cursor_type='row', zebra_stripes=True)
            with TabPane('Groups', id='groups'):
                yield Static('Enter on a group opens its last successful scan.', classes='hint')
                yield DataTable(id='group-table', cursor_type='row', zebra_stripes=True)
            with TabPane('Layouts', id='layouts'):
                yield Static('Select a pool or group with Enter on the corresponding tab.', id='detail', markup=False)
                yield Select([('soft_with_keep + no_covering_barrier', 'active'), ('all GC states', 'all')]
                             + [(gc, gc) for gc in sorted(write2x2_report.GC_STATES)],
                             value='active', allow_blank=False, id='layout-filter')
                yield Static('Local: ? = incomplete realm; 0 = all three VDisks scanned, no part. '
                             'Local3: descending, ? last. Local2: first two of Local3. '
                             'Enter: Local / ingress / GC details. mixed = yes / blobs; unknown = annotation unavailable.', classes='hint')
                yield DataTable(id='layout-table', cursor_type='row', zebra_stripes=True)
                with Collapsible(title='GC', collapsed=True, id='gc-summary'):
                    yield DataTable(id='gc-table', cursor_type='row', zebra_stripes=True)
                with Collapsible(title='Select a Local2 row with Enter for details', collapsed=True, id='bucket-details'):
                    yield DataTable(id='bucket-table', cursor_type='row', zebra_stripes=True)
            with TabPane('Workers', id='workers'):
                yield DataTable(id='worker-table', cursor_type='row', zebra_stripes=True)
            with TabPane('Findings', id='findings'):
                yield Static('Up to 1000 unresolved findings. Enter opens the group histogram.', classes='hint')
                yield DataTable(id='finding-table', cursor_type='row', zebra_stripes=True)
            with TabPane('Console', id='console'):
                yield RichLog(id='log', max_lines=2000, wrap=True, min_width=0, markup=False)
        yield Input(placeholder='Console command (help, layouts GROUP, scan GROUP, interval SEC, …)', id='command')
        yield Footer()

    def on_mount(self):
        for name, columns in self.COLUMNS.items():
            self.query_one('#' + name, DataTable).add_columns(*columns)
        self.set_interval(self.refresh_seconds, self.action_refresh)
        self.action_refresh()
        self.query_one('#pool-table', DataTable).focus()

    async def on_unmount(self):
        self._client_closing = True
        await self.connection.close()

    def action_command(self):
        self.query_one('#command', Input).focus()

    def action_previous_tab(self):
        self._move_tab(-1)

    def action_next_tab(self):
        self._move_tab(1)

    def _move_tab(self, step):
        tabs = self.query_one('#views', TabbedContent)
        order = ('pools', 'groups', 'layouts', 'workers', 'findings', 'console')
        self.set_focus(tabs.query_one(Tabs))
        tabs.active = order[(order.index(tabs.active) + step) % len(order)]

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated):
        if event.tabbed_content.id == 'views':
            self.action_refresh()

    def action_refresh(self):
        if self._client_closing or self._closing:
            return
        if self._refreshing:
            self._refresh_again = True
            return
        self._refreshing = True
        self.run_worker(self.refresh_view(), group='refresh')

    def _status(self, message, connected):
        self._connected = connected
        status = self.query_one('#status', Static)
        status.update(message)
        status.set_class(not connected, 'error')
        self._update_buttons()

    def _update_buttons(self):
        for name in ('pause', 'scan', 'inventory'):
            self.query_one('#' + name, Button).disabled = not self._connected or self._command_busy
        self.query_one('#pause', Button).label = 'Resume' if str(self.stats.get('paused')) == '1' else 'Pause'

    def _table(self, name, rows):
        table = self.query_one('#' + name, DataTable)
        selected = table.ordered_rows[table.cursor_row].key.value if table.row_count else None
        offset = table.scroll_offset
        table.clear()
        keys = []
        for key, values in rows:
            keys.append(key)
            table.add_row(*(Text('-' if value is None else str(value)) for value in values), key=key)
        if selected in keys:
            table.move_cursor(row=keys.index(selected), animate=False, scroll=False)
        table.scroll_to(x=offset.x, y=offset.y, animate=False)

    async def refresh_view(self):
        tabs = self.query_one('#views', TabbedContent)
        view, target = tabs.active, self.detail_target
        try:
            self.stats = json.loads(await self.connection.request('stats'))
            s = self.stats
            self.query_one('#summary', Static).update(
                f"{'PAUSED' if str(s.get('paused')) == '1' else 'RUNNING'}  "
                f"Groups: {s.get('groups', 0)}  Busy: {s.get('busy') or 0}  "
                f"Workers: {s.get('workers_alive', 0)}  Scans: {s.get('scans') or 0}  "
                f"Failed: {s.get('failed') or 0}  Findings: {s.get('unresolved_findings', 0)}  "
                f"Stale inventory: {s.get('stale_inventory_groups', 0)}"
                + (f"\nDiscovery error: {s['discovery_error']}" if s.get('discovery_error') else ''))
            command = {'pools': 'pools', 'groups': 'groups brief', 'workers': 'workers', 'findings': 'findings'}.get(view)
            if view == 'layouts' and target:
                command = ('layouts ' if target[0] == 'group' else 'pools ') + shlex.quote(target[1])
            response = await self.connection.request(command) if command else None
            if tabs.active == view and self.detail_target == target:
                self._render(view, response, target)
            self._status('Connected · updated ' + time.strftime('%H:%M:%S'), True)
        except CommandError as exc:
            self._status(f'Command failed: {exc} · displayed data may be stale', True)
            self._log(str(exc))
        except (ConsoleError, ValueError, KeyError, TypeError) as exc:
            self._status(f'Disconnected / refresh failed: {exc} · displayed data is stale; retrying', False)
        finally:
            self._refreshing = False
            if self._refresh_again:
                self._refresh_again = False
                self.action_refresh()

    def _render(self, view, response, target):
        if response is None:
            return
        if view == 'pools':
            self._table('pool-table', [(p['pool_key'], (
                p.get('pool'), p['pool_key'], f"{p['reported_groups']}/{p['groups']}", p['full_groups'],
                p['partial_groups'], p['never_scanned_groups'], p['outdated_groups'], p['stale_inventory_groups'],
                p['failed_groups'], p['totals']['blobs'], size(p['totals']['logical_bytes']),
                age(p['oldest_scan_age_seconds']))) for p in json.loads(response)])
        elif view == 'groups':
            now = time.time()
            self._table('group-table', [(str(g['id']), (
                g['id'], g.get('pool', g.get('pool_key')), g.get('generation'), since(g['last_scan']),
                g['last_count'], g['owner'] if g['expires'] > now else None,
                'now' if g['due'] <= now else 'in ' + age(g['due'] - now), g['error'])) for g in json_rows(response)])
        elif view == 'workers':
            self._table('worker-table', [(w['id'], (w['id'], since(w['seen']), w['info'])) for w in json_rows(response)])
        elif view == 'findings':
            self._table('finding-table', [(json.dumps([f['gid'], f['blob']]), (
                f['gid'], f['blob'], f['status'], since(f['first_seen']), since(f['last_seen']),
                f.get('coverage'))) for f in json_rows(response)])
        elif view == 'layouts':
            self._render_layout(json.loads(response), target)

    def _render_layout(self, data, target):
        self._layout_data = (data, target)
        if target[0] == 'pool':
            if not data:
                self.query_one('#detail', Static).update(f'Pool {target[1]} is no longer in inventory')
                self._table('layout-table', [])
                self._table('gc-table', [])
                self._table('bucket-table', [])
                return
            p, = data
            buckets, totals = p['buckets'], p['totals']
            text = (f"Pool {p.get('pool')} ({p['pool_key']}) · reports {p['reported_groups']}/{p['groups']} · "
                    f"coverage full {p['full_groups']} · partial {p['partial_groups']} · oldest {age(p['oldest_scan_age_seconds'])}")
            issues = [f'{label} {p[key]}' for key, label in (
                ('never_scanned_groups', 'never scanned'), ('outdated_groups', 'outdated'),
                ('stale_inventory_groups', 'stale inventory'), ('failed_groups', 'latest failures')) if p[key]]
            if issues:
                text += '\n' + ' · '.join(issues)
        else:
            report = data.get('scan_report')
            buckets, totals = (report['buckets'], report['totals']) if report else ([], {})
            missing = ','.join(map(str, data.get('missing_vdisks', []))) or '-'
            text = (f"Group {data['group_id']} · pool {data.get('pool')} · generation {data['generation']} · "
                    f"report generation {data.get('report_generation', '-')} · age {age(data.get('scan_age_seconds'))}\n"
                    f"Completed VDisks: {len(data.get('completed_vdisks', []))}/9 · missing indexes: {missing}")
            if not report:
                text += '\nNo successful scan report'
            if data.get('outdated') or data.get('inventory_stale'):
                text += '\nWARNING: outdated report or stale inventory'
            if data.get('latest_error'):
                text += '\nLatest attempt error: ' + data['latest_error']
            if data.get('stream_errors'):
                text += '\nStream errors: ' + json.dumps(data['stream_errors'], ensure_ascii=False)
        selected_gc = self.query_one('#layout-filter', Select).value
        visible = [b for b in buckets if selected_gc == 'all' or
                   (b['gc'] in ACTIVE_GC if selected_gc == 'active' else b['gc'] == selected_gc)]
        visible_totals = write2x2_report.totals(visible)
        if totals:
            text += (f"\nSelected: {visible_totals['blobs']:,} blobs · {size(visible_totals['logical_bytes'])}"
                     f" · All GC states: {totals['blobs']:,} blobs · {size(totals['logical_bytes'])}")
        self.query_one('#detail', Static).update(text)
        by_local = {}
        for bucket in visible:
            by_local.setdefault(write2x2_report.local2(bucket['local']), []).append(bucket)
        rows = []
        for local, items in by_local.items():
            counts = write2x2_report.totals(items)
            rows.append((local, (local, counts['blobs'], size(counts['logical_bytes']),
                                 *(annotation(items, key) for key in write2x2_report.ANNOTATIONS))))
        rows.sort(key=lambda row: row[0])
        self._table('layout-table', rows)
        gc_rows = []
        for gc in sorted(DISCARDED_GC):
            counts = write2x2_report.totals([b for b in buckets if b['gc'] == gc])
            if counts['blobs']:
                gc_rows.append((gc, (gc, counts['blobs'], size(counts['logical_bytes']))))
        self._table('gc-table', gc_rows)
        discarded = write2x2_report.totals([b for b in buckets if b['gc'] in DISCARDED_GC])
        summary = self.query_one('#gc-summary', Collapsible)
        summary.title = f"GC · {discarded['blobs']:,} blobs · {size(discarded['logical_bytes'])}"
        summary.display = bool(discarded['blobs'])
        self._render_bucket_details(visible)

    def _render_bucket_details(self, visible):
        items = [b for b in visible if write2x2_report.local2(b['local']) == self._selected_layout]
        self.query_one('#bucket-details', Collapsible).title = (
            'Local2 ' + self._selected_layout + ' · Local / ingress / GC details' if items
            else 'Select a Local2 row with Enter for details')
        self._table('bucket-table', [(str(i), (
            b['local'], write2x2_report.local3(b['local']), b['ingress'], b['gc'], b['blobs'],
            size(b['logical_bytes']), size(b['observed_replica_bytes']),
            b['missing_metadata_blobs'], *(annotation([b], key) for key in write2x2_report.ANNOTATIONS)))
            for i, b in enumerate(sorted(items, key=lambda b: (
                write2x2_report.local3(b['local']), b['local'], b['ingress'], b['gc'])))])

    def on_select_changed(self, event: Select.Changed):
        if event.select.id == 'layout-filter' and self._layout_data:
            self._render_layout(*self._layout_data)

    def on_data_table_row_selected(self, event: DataTable.RowSelected):
        key = event.row_key.value
        if event.data_table.id == 'layout-table':
            self._selected_layout = key
            if self._layout_data:
                self._render_layout(*self._layout_data)
            self.query_one('#bucket-details', Collapsible).collapsed = False
            return
        elif event.data_table.id == 'pool-table':
            self.detail_target = ('pool', key)
        elif event.data_table.id == 'group-table':
            self.detail_target = ('group', key)
        elif event.data_table.id == 'finding-table':
            self.detail_target = ('group', str(json.loads(key)[0]))
        else:
            return
        self._selected_layout = None
        self._layout_data = None
        self.query_one('#bucket-details', Collapsible).collapsed = True
        self.query_one('#detail', Static).update('Loading…')
        self._table('layout-table', [])
        self.query_one('#views', TabbedContent).active = 'layouts'
        self.query_one('#layout-table', DataTable).focus()

    def _log(self, message):
        limit = 65536
        if len(message) > limit:
            message = message[:limit] + '\n… response truncated in console'
        self.query_one('#log', RichLog).write(Text(message))

    def submit_command(self, command):
        command = command.strip()
        if not command or self._command_busy:
            return
        if command in ('quit', 'exit'):
            self.exit()
            return
        self._command_busy = True
        self._update_buttons()
        self.run_worker(self.execute_command(command), group='command')

    async def execute_command(self, command):
        self._log('> ' + command)
        try:
            self._log(await self.connection.request(command))
        except CommandError as exc:
            self._log('ERROR: ' + str(exc))
            self.notify(str(exc), severity='error')
        except ConsoleError as exc:
            self._log(f'FAILED: {exc}\nCommand was not retried; check master state before repeating it.')
            self._status(f'Disconnected: {exc} · command was not retried', False)
        finally:
            self._command_busy = False
            self._update_buttons()
            self.action_refresh()

    def on_input_submitted(self, event: Input.Submitted):
        if self._command_busy:
            self.notify('Wait for the previous command to finish')
            return
        self.query_one('#views', TabbedContent).active = 'console'
        self.submit_command(event.value)
        event.input.value = ''

    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == 'reload':
            self.action_refresh()
        elif event.button.id == 'pause':
            self.submit_command('resume' if str(self.stats.get('paused')) == '1' else 'pause')
        elif event.button.id == 'scan':
            self.submit_command('scan all')
        elif event.button.id == 'inventory':
            self.submit_command('refresh')
