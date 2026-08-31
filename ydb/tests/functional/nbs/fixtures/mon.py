# -*- coding: utf-8 -*-
"""Parsers for NBS partition / PBuffer / DDisk mon pages.

HTML shape is locked by ``mon_page/mon_render_ut.cpp``. Fields that the
unit test does not yet assert are parsed best-effort and left empty
rather than failing the parser.
"""

import re
from dataclasses import dataclass, field


_TAG_RE = re.compile(r'<[^>]+>')


def _strip_tags(html):
    return re.sub(r'\s+', ' ', _TAG_RE.sub('', html)).strip()


def parse_html_tables(html):
    """Return a list of (headers, rows) for every HTML table in ``html``."""
    tables = []
    for table_html in re.findall(r'<table[^>]*>(.*?)</table>', html, flags=re.DOTALL | re.IGNORECASE):
        headers = [_strip_tags(h) for h in re.findall(r'<th[^>]*>(.*?)</th>', table_html, flags=re.DOTALL | re.IGNORECASE)]
        rows = []
        for tr in re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, flags=re.DOTALL | re.IGNORECASE):
            cells = [_strip_tags(c) for c in re.findall(r'<td[^>]*>(.*?)</td>', tr, flags=re.DOTALL | re.IGNORECASE)]
            if cells:
                rows.append(cells)
        tables.append((headers, rows))
    return tables


def find_table(html, header_name):
    """Return (headers, rows) of the first table that has ``header_name``."""
    wanted = header_name.lower()
    for headers, rows in parse_html_tables(html):
        if any(wanted == h.lower() for h in headers):
            return headers, rows
    return None, []


def _cell(headers, row, name, default=''):
    name = name.lower()
    for i, header in enumerate(headers):
        if header.lower() == name:
            return row[i] if i < len(row) else default
    return default


def _parse_int(value, default=0):
    if value is None:
        return default
    match = re.search(r'-?\d+', str(value).replace(',', ''))
    if not match:
        return default
    return int(match.group(0))


def _parse_optional_int(value):
    text = (value or '').strip()
    if text in ('', '-', 'nullopt', 'None'):
        return None
    match = re.search(r'-?\d+', text.replace(',', ''))
    return int(match.group(0)) if match else None


@dataclass
class HostSnapshot:
    """One row of the DBG host table plus optional vchunk / connection fields."""

    index: int
    state: str = ''
    health: str = ''
    pbuffer_used: str = ''
    ahead_blocks: str = ''
    behind_blocks: str = ''
    consecutive_errors: int = 0
    consecutive_success: int = 0
    node_id: int = None
    pdisk_id: int = None
    pbuffer_node_id: int = None
    ddisk_id: str = ''
    pbuffer_id: str = ''
    pbuffer_role: str = ''
    ddisk_role: str = ''
    enabled: str = ''
    watermark: object = None
    inflight_by_operation: dict = field(default_factory=dict)


def parse_dbg_hosts(html):
    """Parse the DBG detail host table (F5.1)."""
    headers, rows = find_table(html, 'State')
    if headers is None:
        return []
    hosts = []
    for row in rows:
        label = _cell(headers, row, 'Host')
        match = re.search(r'H(\d+)', label)
        if not match:
            continue
        hosts.append(
            HostSnapshot(
                index=int(match.group(1)),
                state=_cell(headers, row, 'State'),
                health=_cell(headers, row, 'Health'),
                pbuffer_used=_cell(headers, row, 'PBuffer used'),
                ahead_blocks=_cell(headers, row, 'Ahead blocks'),
                behind_blocks=_cell(headers, row, 'Behind blocks'),
                consecutive_errors=_parse_int(_cell(headers, row, 'Consecutive errors')),
                consecutive_success=_parse_int(_cell(headers, row, 'Consecutive success')),
            )
        )
    return hosts


def parse_dbg_connections(html):
    """Parse the DBG detail Connections table (node / PDisk / DDisk ids)."""
    headers, rows = find_table(html, 'DDisk id')
    if headers is None:
        return []
    connections = []
    for row in rows:
        label = _cell(headers, row, 'Host')
        match = re.search(r'H(\d+)', label)
        if not match:
            continue
        ddisk_id = _cell(headers, row, 'DDisk id')
        pbuffer_id = _cell(headers, row, 'PBuffer id')
        node_id = None
        pdisk_id = None
        pbuffer_node_id = None
        parts = ddisk_id.split(':')
        if len(parts) >= 2 and parts[0].isdigit():
            node_id = int(parts[0])
            pdisk_id = int(parts[1])
        pb_parts = pbuffer_id.split(':')
        if len(pb_parts) >= 1 and pb_parts[0].isdigit():
            pbuffer_node_id = int(pb_parts[0])
        connections.append(
            {
                'index': int(match.group(1)),
                'ddisk_id': ddisk_id,
                'pbuffer_id': pbuffer_id,
                'node_id': node_id,
                'pdisk_id': pdisk_id,
                'pbuffer_node_id': pbuffer_node_id,
                'ddisk_session': _cell(headers, row, 'DDisk session'),
                'pbuffer_connected': _cell(headers, row, 'PBuffer connected'),
            }
        )
    return connections


def merge_hosts_with_connections(hosts, connections):
    """Attach node / PDisk ids from the Connections table onto host snapshots."""
    by_index = {c['index']: c for c in connections}
    for host in hosts:
        conn = by_index.get(host.index)
        if conn is None:
            continue
        host.node_id = conn['node_id']
        host.pdisk_id = conn['pdisk_id']
        host.pbuffer_node_id = conn['pbuffer_node_id']
        host.ddisk_id = conn['ddisk_id']
        host.pbuffer_id = conn['pbuffer_id']
    return hosts


@dataclass
class DDiskStateSnapshot:
    """One host from the dirty-map ``DDiskStates:`` line."""

    host_index: int
    membership: str = ''
    state: str = ''
    lagging: object = None
    operational_block_count: int = 0


@dataclass
class InflightDDiskSync:
    """One in-flight copy range from the dirty-map ``DDiskSyncs:`` line."""

    destination_host: int
    start: int = 0
    end: int = 0
    ready: bool = False


# H0*{Operational,32768};H1*{Fresh+,8704};H2-{Disabled,0};
_DDISK_STATE_RE = re.compile(
    r'H(\d+)([-*+])\{([A-Za-z]+)([+-]?),(\d+)\}'
)
# H0[0..255]ready;H1[256..511]wait;
_DDISK_SYNC_RE = re.compile(
    r'H(\d+)\[(\d+)\.\.(\d+)\](ready|wait)'
)


def parse_ddisk_states(html):
    """Parse ``DDiskStates:`` from the VChunk dirty-map dump.

    Returns a dict keyed by host index. ``membership`` is ``-`` disabled,
    ``*`` desired, or ``+`` other. ``lagging`` is set only for ``Fresh``
    (``Fresh-`` / ``Fresh+``).
    """
    states = {}
    for match in _DDISK_STATE_RE.finditer(html):
        host_index = int(match.group(1))
        state = match.group(3)
        suffix = match.group(4)
        lagging = None
        if state == 'Fresh':
            lagging = suffix == '-'
        states[host_index] = DDiskStateSnapshot(
            host_index=host_index,
            membership=match.group(2),
            state=state,
            lagging=lagging,
            operational_block_count=int(match.group(5)),
        )
    return states


def parse_inflight_ddisk_syncs(html):
    """Parse ``DDiskSyncs:`` from the VChunk dirty-map dump."""
    syncs = []
    # Restrict to the DDiskSyncs line so Ahead/Behind ranges are not matched.
    match = re.search(r'DDiskSyncs:\s*(.*)', html)
    if not match:
        return syncs
    for item in _DDISK_SYNC_RE.finditer(match.group(1)):
        syncs.append(
            InflightDDiskSync(
                destination_host=int(item.group(1)),
                start=int(item.group(2)),
                end=int(item.group(3)),
                ready=item.group(4) == 'ready',
            )
        )
    return syncs


def parse_vchunk_hosts(html):
    """Parse the VChunk 'Host roles' table (watermark, Primary / HandOff)."""
    headers, rows = find_table(html, 'Watermark')
    if headers is None:
        return []
    hosts = []
    for row in rows:
        label = _cell(headers, row, 'Host')
        match = re.search(r'H(\d+)', label)
        if not match:
            continue
        hosts.append(
            HostSnapshot(
                index=int(match.group(1)),
                pbuffer_role=_cell(headers, row, 'PBuffer role'),
                ddisk_role=_cell(headers, row, 'DDisk role'),
                enabled=_cell(headers, row, 'Enabled'),
                watermark=_parse_optional_int(_cell(headers, row, 'Watermark')),
            )
        )
    return hosts


def parse_pbuffer_occupancy(html):
    """Best-effort occupancy / tablet-LSN presence from a PBuffer mon page (F5.2)."""
    return {
        'html': html,
        'tablet_ids': sorted(set(re.findall(r'\b(\d{10,})\b', html))),
        'has_free_space': 'free' in html.lower() or 'FreeSpace' in html,
        'overfill': 'OVERFILL' in html,
        'overloaded': 'OVERLOADED' in html,
    }


def parse_vchunk_counters(html):
    """Best-effort TVChunkCounters dump (F5.3).

    The live dump format is discovered by the F5.3 smoke test; this parser
    returns every ``Name: number`` pair so callers can look up Pending / MinLsn
    without hard-coding a layout that may still change.
    """
    counters = {}
    for name, value in re.findall(r'\b([A-Za-z][A-Za-z0-9_.]+)\s*[:=]\s*(\d+)', html):
        counters[name] = int(value)
    return counters


def parse_volume_request_counters(html):
    """Best-effort TVolumeRequestCounters dump (F5.4)."""
    return parse_vchunk_counters(html)


def parse_ddisk_directio(html):
    """Best-effort DDisk DirectIO / PBuffer queue dump (F5.5)."""
    counters = parse_vchunk_counters(html)
    counters['has_directio'] = 'DirectIO' in html or 'QueueSize' in html
    counters['has_pending_events'] = 'PendingEvents' in html or 'PendingEventsQueueSize' in html
    return counters
