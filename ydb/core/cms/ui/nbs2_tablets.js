'use strict';

// NBS 2.0 (Direct Block Group) tablet/DDisk viewer.
//
// The tablet and DDisk lists can be very large in a big cluster, so this UI
// never fetches everything at once. Instead:
//   - the tablet/disk list itself is paginated, filtered and sorted on the
//     CMS backend (cms/api/json/ddisk/tablets and cms/api/json/ddisk/disks);
//   - "Only problems" filtering is also evaluated on the CMS backend (using
//     the latest PDisk availability known to CMS), so it works correctly
//     across the whole cluster and not just within the current page;
//   - per-tablet DDisk layout snapshots (cms/api/json/ddisk/tablet) are only
//     fetched for tablets that are actually shown on the current page, and
//     only when a row is expanded or auto-loaded for the "only problems"
//     tablet view.
//
// All disk availability/unavailability counters shown for the "tablets" and
// "ddisks" views come directly from the backend response; no client-side
// aggregation over the full cluster state is performed.

const Nbs2Tablets = {
    // Current page state.
    tablets: [],        // TTabletRevision[] for the "tablets" view
    disksPage: [],       // TDiskUsage[] for the "ddisks" view
    totalCount: 0,
    offset: 0,

    // Per-tablet DDisk layout snapshots, only kept for tablets on the current page
    // (fetched lazily when a row is expanded).
    snapshots: {},

    // Cache of known DDisk/PDisk availability, keyed by "nodeId:pdiskId"
    // (availability is a property of the underlying PDisk, so it does not
    // depend on the DDisk slot id). Populated from any /ddisk/disks response,
    // either from the "ddisks" view itself or from the per-tablet lookup done
    // when a tablet row is expanded, so that both views show consistent,
    // up-to-date highlighting.
    diskAvailability: {},

    // Cache of the full underlying PDisk state name (e.g. "Normal", "Missing",
    // "Timeout", "NodeDisconnected", one of the Initial*/*Error states, or
    // "Unknown"), keyed the same way as diskAvailability. Populated from the
    // same /ddisk/disks responses and used to show a more detailed status in
    // hints and in the State column than the plain available/unavailable flag.
    diskState: {},

    requestToken: 0,

    init: function() {
        $('#nbs2-tablets-view').on('change', () => this.resetAndLoad());
        $('#nbs2-tablets-sort').on('change', () => this.resetAndLoad());
        $('#nbs2-tablets-sort-desc').on('change', () => this.resetAndLoad());
        $('#nbs2-tablets-page-size').on('change', () => this.resetAndLoad());
        $('#nbs2-tablets-problems').on('change', () => this.resetAndLoad());
        $('#nbs2-tablets-refresh').on('click', () => this.load());
        $('#nbs2-tablets-prev-page').on('click', () => this.changePage(-1));
        $('#nbs2-tablets-next-page').on('click', () => this.changePage(1));

        let filterTimer = null;
        $('#nbs2-tablets-filter').on('input', () => {
            if (filterTimer) clearTimeout(filterTimer);
            filterTimer = setTimeout(() => this.resetAndLoad(), 300);
        });

        $('#nbs2-ddisks-body').on('click', 'tr.nbs2-ddisk-row', (event) => {
            const details = $('#nbs2-ddisk-details-' + $(event.currentTarget).data('ddisk-key').toString().replace(/:/g, '-'));
            details.toggle();
        });
        $('#nbs2-tablets-body').on('click', 'tr.nbs2-tablet-row', (event) => {
            const tabletId = $(event.currentTarget).data('tablet-id').toString();
            const details = $('#nbs2-tablet-details-' + tabletId);
            if (details.length) {
                details.toggle();
                return;
            }
            this.loadSnapshot(tabletId, $(event.currentTarget));
        });
        $('#nbs2-tablets-body').on('click', 'a[href="#"]', (event) => event.preventDefault());
        $('a[href="#nbs2-tablets"]').on('shown.bs.tab', () => {
            if (!this.tablets.length && !this.disksPage.length) this.load();
        });
    },

    isDDisksView: function() {
        return $('#nbs2-tablets-view').val() === 'ddisks';
    },

    pageSize: function() {
        return Number($('#nbs2-tablets-page-size').val()) || 25;
    },

    resetAndLoad: function() {
        this.offset = 0;
        this.load();
    },

    changePage: function(direction) {
        const pageSize = this.pageSize();
        const newOffset = this.offset + direction * pageSize;
        if (newOffset < 0) return;
        if (direction > 0 && this.offset + pageSize >= this.totalCount) return;
        this.offset = newOffset;
        this.load();
    },

    load: function() {
        $('#nbs2-tablets-error').text('Loading...');
        this.snapshots = {};
        if (this.isDDisksView()) {
            this.loadDisksPage();
        } else {
            this.loadTabletsPage();
        }
    },

    commonParams: function() {
        return {
            offset: this.offset,
            limit: this.pageSize(),
            sort_desc: $('#nbs2-tablets-sort-desc').prop('checked') ? '1' : '0',
            only_problems: $('#nbs2-tablets-problems').prop('checked') ? '1' : '0',
        };
    },

    loadTabletsPage: function() {
        const token = ++this.requestToken;
        const sort = $('#nbs2-tablets-sort').val();
        const sortBy = sort === 'last_changed_at' || sort === 'groups_count' ? sort : 'tablet';
        const params = Object.assign(this.commonParams(), {
            filter: $('#nbs2-tablets-filter').val() || '',
            sort_by: sortBy,
        });

        $.getJSON('cms/api/json/ddisk/tablets', params)
            .done((data) => {
                if (token !== this.requestToken) return;
                try {
                    this.checkStatus(data);
                } catch (error) {
                    $('#nbs2-tablets-error').text('Failed to load NBS 2.0 tablets: ' + error.message);
                    return;
                }
                this.tablets = this.field(data, 'Tablets', 'tablets') || [];
                this.totalCount = Number(this.field(data, 'TotalCount', 'totalCount')) || 0;
                $('#nbs2-tablets-error').empty();
                this.render();
            })
            .fail((xhr) => {
                if (token !== this.requestToken) return;
                $('#nbs2-tablets-error').text('Failed to load NBS 2.0 tablets: ' + xhr.statusText);
            });
    },

    loadDisksPage: function() {
        const token = ++this.requestToken;
        const sort = $('#nbs2-tablets-sort').val();
        const sortBy = sort === 'tablets_count' ? 'tablets_count' : 'disk';
        const params = Object.assign(this.commonParams(), {
            filter: $('#nbs2-tablets-filter').val() || '',
            sort_by: sortBy,
        });

        $.getJSON('cms/api/json/ddisk/disks', params)
            .done((data) => {
                if (token !== this.requestToken) return;
                try {
                    this.checkStatus(data);
                } catch (error) {
                    $('#nbs2-tablets-error').text('Failed to load NBS 2.0 disks: ' + error.message);
                    return;
                }
                this.disksPage = this.field(data, 'Disks', 'disks') || [];
                this.totalCount = Number(this.field(data, 'TotalCount', 'totalCount')) || 0;
                this.disksPage.forEach((disk) => this.recordDiskAvailability(disk));
                $('#nbs2-tablets-error').empty();
                this.render();
            })
            .fail((xhr) => {
                if (token !== this.requestToken) return;
                $('#nbs2-tablets-error').text('Failed to load NBS 2.0 disks: ' + xhr.statusText);
            });
    },

    // Remembers the availability and full state name of the disk referenced
    // by a TDiskUsage-like object (as returned by cms/api/json/ddisk/disks),
    // keyed by the underlying PDisk (neither depends on the DDisk slot id).
    recordDiskAvailability: function(diskUsage) {
        const diskIdObj = this.field(diskUsage, 'DiskId', 'diskId');
        if (!diskIdObj) return;
        const key = this.diskId(diskIdObj);
        const available = this.field(diskUsage, 'Available', 'available');
        if (available !== undefined) {
            this.diskAvailability[key] = available !== false;
        }
        const state = this.field(diskUsage, 'State', 'state');
        if (state !== undefined) {
            this.diskState[key] = state;
        }
    },

    // Returns true only if the disk is known (from a previously loaded
    // /ddisk/disks response) to be unavailable; unknown disks are treated as
    // available, consistently with the backend's IsDDiskAvailable default.
    isDiskUnavailable: function(id) {
        const available = this.diskAvailability[this.diskId(id)];
        return available === false;
    },

    // Returns the full underlying PDisk state name (e.g. "Normal", "Missing",
    // "Timeout", "NodeDisconnected", one of the Initial*/*Error states) for a
    // disk, if known from a previously loaded /ddisk/disks response.
    diskStateName: function(id) {
        return this.diskState[this.diskId(id)] || 'Unknown';
    },

    field: function(object, ...names) {
        if (!object) return undefined;
        for (const name of names) {
            if (object[name] !== undefined) return object[name];
        }
        return undefined;
    },

    checkStatus: function(data) {
        const status = this.field(data, 'Status', 'status');
        const code = this.field(status, 'Code', 'code');
        if (code && code !== 'OK') {
            throw new Error(this.field(status, 'Reason', 'reason') || code);
        }
    },

    tabletId: function(tablet) {
        return this.field(tablet, 'TabletId', 'tabletId');
    },

    loadSnapshot: function(tabletId, row) {
        row.after('<tr id="nbs2-tablet-loading-' + tabletId + '"><td colspan="5">Loading snapshot...</td></tr>');
        // Fetch the disks used by this tablet together with the snapshot, so
        // that the expanded DDisk/Persistent buffer layout can correctly
        // highlight the disks that are actually unavailable (the snapshot
        // itself only contains disk ids, not their availability).
        const availability = $.getJSON('cms/api/json/ddisk/disks', {
            filter_tablet_id: String(tabletId),
            limit: 0,
        }).done((data) => {
            (this.field(data, 'Disks', 'disks') || []).forEach((disk) => this.recordDiskAvailability(disk));
        });

        $.when($.getJSON('cms/api/json/ddisk/tablet?tablet_id=' + encodeURIComponent(tabletId)), availability)
            .done((snapshotResult) => {
                this.snapshots[tabletId] = snapshotResult[0];
                $('#nbs2-tablet-loading-' + tabletId).remove();
                this.render();
                $('#nbs2-tablet-details-' + tabletId).show();
            })
            .fail((xhr) => $('#nbs2-tablet-loading-' + tabletId).html('<td colspan="5" class="text-danger">Failed to load snapshot: ' + xhr.statusText + '</td>'));
    },

    diskId: function(id) {
        return (this.field(id, 'NodeId', 'nodeId') || 0) + ':' + (this.field(id, 'PDiskId', 'pdiskId') || 0);
    },

    ddiskKey: function(id) {
        return this.diskId(id) + ':' + (this.field(id, 'DDiskSlotId', 'dDiskSlotId', 'ddiskSlotId') || 0);
    },

    formatDisk: function(id, role, unavailable) {
        const nodeId = this.field(id, 'NodeId', 'nodeId') || 0;
        const pdiskId = this.field(id, 'PDiskId', 'pdiskId') || 0;
        const slotId = this.field(id, 'DDiskSlotId', 'dDiskSlotId', 'ddiskSlotId') || 0;
        const label = nodeId + ':' + pdiskId + ':' + slotId;
        const hint = 'Node: ' + (nodeId || '—') + '\nState: ' + this.diskStateName(id);
        const url = role === 'PersistentBuffer'
            ? this.persistentBufferUrl(nodeId, pdiskId, slotId)
            : this.ddiskUrl(nodeId, pdiskId, slotId);
        return '<span class="nbs2-disk ' + (unavailable ? 'nbs2-disk-unavailable' : '') +
            '" title="' + this.escapeHtml(hint) + '"><a href="' + this.escapeHtml(url) + '">' + label + '</a></span>';
    },

    ddiskUrl: function(nodeId, pdiskId, slotId) {
        return '/node/' + encodeURIComponent(nodeId) + '/actors/ddisks/ddisk_p' + String(pdiskId).padStart(9, '0') +
            '_s' + String(slotId).padStart(9, '0');
    },

    persistentBufferUrl: function(nodeId, pdiskId, slotId) {
        // MakeBlobStoragePersistentBufferId stores "NPB_" + PDiskId + slotId
        // in TActorId's raw fields. TActorId::ToString() therefore renders
        // [node:localId:hint], which is the value accepted by the PB viewer.
        const localId = 0x5f42504en + (BigInt(pdiskId) << 32n);
        const pb = '[' + nodeId + ':' + localId.toString() + ':' + slotId + ']';
        return '/node/' + encodeURIComponent(nodeId) + '/actors/persistent_buffer?formPresent=1&autoRefresh=1&describeFreeSpace=1&showTablets=1&refreshRate=1&pb=' +
            pb;
    },

    escapeHtml: function(value) {
        return String(value).replace(/[&<>\"]/g, (char) => {
            const entities = {
                '&': String.fromCharCode(38) + 'amp;',
                '<': String.fromCharCode(38) + 'lt;',
                '>': String.fromCharCode(38) + 'gt;',
                '"': String.fromCharCode(38) + 'quot;',
            };
            return entities[char];
        });
    },

    updatePagination: function() {
        const pageSize = this.pageSize();
        const totalPages = Math.max(1, Math.ceil(this.totalCount / pageSize));
        const page = Math.floor(this.offset / pageSize) + 1;
        $('#nbs2-tablets-page-info').text('Page ' + page + ' of ' + totalPages + ' (' + this.totalCount + ' total)');
        $('#nbs2-tablets-prev-page').prop('disabled', this.offset <= 0);
        $('#nbs2-tablets-next-page').prop('disabled', this.offset + pageSize >= this.totalCount);
    },

    render: function() {
        this.updatePagination();
        if (this.isDDisksView()) {
            this.renderDDisks();
            return;
        }
        $('#nbs2-tablets-table').show();
        $('#nbs2-ddisks-table').hide();
        const body = $('#nbs2-tablets-body').empty();

        this.tablets.forEach((tablet) => {
            const id = this.tabletId(tablet);
            const groupsCount = this.field(tablet, 'GroupsCount', 'groupsCount');
            const lastChangedAt = this.field(tablet, 'LastChangedAt', 'lastChangedAt');
            const unavailableDDisk = Number(this.field(tablet, 'UnavailableDDiskCount', 'unavailableDDiskCount')) || 0;
            const unavailablePersistentBuffer = Number(this.field(tablet, 'UnavailablePersistentBufferCount', 'unavailablePersistentBufferCount')) || 0;
            body.append('<tr class="nbs2-tablet-row" data-tablet-id="' + id + '"><td><a href="#">' + id + '</a></td><td>' + (groupsCount || '—') + '</td><td class="' + (unavailableDDisk ? 'nbs2-count-unavailable' : '') + '">' + unavailableDDisk + '</td><td class="' + (unavailablePersistentBuffer ? 'nbs2-count-unavailable' : '') + '">' + unavailablePersistentBuffer + '</td><td>' + this.date(lastChangedAt) + '</td></tr>');
            if (this.snapshots[id]) this.renderDetails(body, id);
        });
    },

    renderDDisks: function() {
        $('#nbs2-tablets-table').hide();
        const body = $('#nbs2-ddisks-body').empty();
        $('#nbs2-ddisks-table').show();

        this.disksPage.forEach((disk) => {
            const diskIdObj = this.field(disk, 'DiskId', 'diskId');
            // Deduplicate tablet ids within each role's own list: the backend is
            // expected to report each tablet at most once per (disk, role), but
            // be defensive here too, so that the rendered "DDisk: ... (a, b, c)" /
            // "Persistent Buffer: ... (a, b, c)" lists never show the same tablet
            // id repeated, which would otherwise make the list look much larger
            // than the deduplicated count shown in the "Tablets" column.
            const ddiskTabletIds = Array.from(new Set((this.field(disk, 'DDiskTabletIds', 'dDiskTabletIds', 'ddiskTabletIds') || []).map(String)));
            const persistentBufferTabletIds = Array.from(new Set((this.field(disk, 'PersistentBufferTabletIds', 'persistentBufferTabletIds') || []).map(String)));
            const available = this.field(disk, 'Available', 'available');
            const unavailable = available === false;
            const key = this.ddiskKey(diskIdObj);
            // Show the full underlying PDisk state (e.g. "Normal", "Missing",
            // "Timeout", "NodeDisconnected", one of the Initial*/*Error states)
            // rather than just the coarse available/unavailable flag, falling
            // back to that flag if the backend didn't report a state name.
            const stateName = this.field(disk, 'State', 'state');
            const status = stateName || (unavailable ? 'Unavailable' : 'Available');
            const detailsId = key.replace(/:/g, '-');
            const roles = [];
            const tabletIds = new Set(ddiskTabletIds.concat(persistentBufferTabletIds));
            if (ddiskTabletIds.length) roles.push('<div><b>DDisk:</b> ' + this.formatDisk(diskIdObj, 'DDisk', unavailable) + ' (' + ddiskTabletIds.map((id) => this.escapeHtml(id)).join(', ') + ')</div>');
            if (persistentBufferTabletIds.length) roles.push('<div><b>Persistent Buffer:</b> ' + this.formatDisk(diskIdObj, 'PersistentBuffer', unavailable) + ' (' + persistentBufferTabletIds.map((id) => this.escapeHtml(id)).join(', ') + ')</div>');
            body.append('<tr class="nbs2-ddisk-row" data-ddisk-key="' + this.escapeHtml(key) + '">' +
                '<td>' + this.formatDisk(diskIdObj, ddiskTabletIds.length ? 'DDisk' : 'PersistentBuffer', unavailable) + '</td><td>' + (this.field(diskIdObj, 'NodeId', 'nodeId') || '—') +
                '</td><td>' + (this.field(diskIdObj, 'PDiskId', 'pdiskId') || '—') + '</td><td class="' +
                (unavailable ? 'nbs2-count-unavailable' : '') + '">' + status +
                '</td><td>' + tabletIds.size + '</td></tr>' +
                '<tr id="nbs2-ddisk-details-' + this.escapeHtml(detailsId) + '" style="display: none"><td colspan="5">' +
                (roles.join('') || '—') + '</td></tr>');
        });
    },

    renderDetails: function(body, id) {
        const snapshot = this.snapshots[id];
        const groups = this.field(snapshot, 'Groups', 'groups') || [];
        let html = '<tr id="nbs2-tablet-details-' + id + '"><td colspan="5"><table class="table table-sm mb-0"><thead><tr><th>DBG</th><th>DDisk layout</th><th>Persistent buffer</th></tr></thead><tbody>';
        groups.forEach((group) => {
            const groupId = this.field(group, 'DirectBlockGroupId', 'directBlockGroupId');
            const ddiskIds = this.field(group, 'DDiskId', 'dDiskId', 'ddiskId') || [];
            const persistentBufferIds = this.field(group, 'PersistentBufferDDiskId', 'persistentBufferDDiskId') || [];
            html += '<tr><td>' + (groupId || 0) + '</td><td>' + ddiskIds.map((disk) => this.formatDisk(disk, 'DDisk', this.isDiskUnavailable(disk))).join(' ') + '</td><td>' + persistentBufferIds.map((disk) => this.formatDisk(disk, 'PersistentBuffer', this.isDiskUnavailable(disk))).join(' ') + '</td></tr>';
        });
        body.append(html + '</tbody></table></td></tr>').find('#nbs2-tablet-details-' + id).hide();
    },

    date: function(micros) {
        return micros ? new Date(Number(micros) / 1000).toLocaleString() : '—';
    }
};

function initNbs2TabletsTab() {
    Nbs2Tablets.init();
}
