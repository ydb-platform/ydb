'use strict';

// NBS 2.0 (Direct Block Group) tablet/DDisk viewer.
//
// The tablet and DDisk lists can be very large in a big cluster, so this UI
// never fetches everything at once. Instead:
//   - the tablet/disk list itself is paginated, filtered and sorted on the
//     CMS backend (cms/api/json/ddisk/tablets and cms/api/json/ddisk/disks);
//   - "Only problems" filtering is also evaluated on the CMS backend (using
//     the latest component health known to CMS), so it works correctly
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

    requestToken: 0,

    init: function() {
        $('#nbs2-tablets-view').on('change', () => this.resetAndLoad());
        $('#nbs2-tablets-group-degrade').on('change', () => this.resetAndLoad());
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
        const diskView = this.isDDisksView();
        const sort = $('#nbs2-tablets-sort');
        sort.find('option').each(function() {
            const diskOption = ['disk', 'tablets_count', 'ddisk_occupancy', 'persistent_buffer_occupancy'].includes(this.value);
            $(this).prop('disabled', diskOption !== diskView).prop('hidden', diskOption !== diskView);
        });
        if (sort.find('option:selected').prop('disabled')) {
            sort.val(diskView ? 'disk' : 'degrade');
        }
        $('#nbs2-tablets-error').text('Loading...');
        $('#nbs2-tablets-body').empty();
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
        const sortBy = ['degrade', 'last_changed_at', 'groups_count'].includes(sort) ? sort : 'tablet';
        const params = Object.assign(this.commonParams(), {
            filter: $('#nbs2-tablets-filter').val() || '',
            sort_by: sortBy,
            group_by_degrade: $('#nbs2-tablets-group-degrade').prop('checked') ? '1' : '0',
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
        const sortBy = ['tablets_count', 'ddisk_occupancy', 'persistent_buffer_occupancy'].includes(sort) ? sort : 'disk';
        const params = Object.assign(this.commonParams(), {
            filter: $('#nbs2-tablets-filter').val() || '',
            sort_by: sortBy,
        });

        if (params.only_problems === '1') {
            params.include_component_problems = '1';
        }
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
                $('#nbs2-tablets-error').empty();
                this.render();
            })
            .fail((xhr) => {
                if (token !== this.requestToken) return;
                $('#nbs2-tablets-error').text('Failed to load NBS 2.0 disks: ' + xhr.statusText);
            });
    },

    diskHealth: function(disk) {
        const available = this.field(disk, 'Available', 'available');
        const pdiskState = this.field(disk, 'PDiskState', 'pDiskState', 'pdiskState', 'State', 'state');
        const component = (status, reason) => ({
            unavailable: available === false || (status !== undefined && status !== 'Ready' && status !== 1),
            pdiskState,
            status: status === undefined ? undefined : status + (reason ? ': ' + reason : ''),
        });
        return {
            DDisk: component(this.field(disk, 'DDiskStatus', 'dDiskStatus', 'ddiskStatus'),
                this.field(disk, 'DDiskStatusReason', 'dDiskStatusReason', 'ddiskStatusReason')),
            PersistentBuffer: component(this.field(disk, 'PersistentBufferStatus', 'persistentBufferStatus'),
                this.field(disk, 'PersistentBufferStatusReason', 'persistentBufferStatusReason')),
        };
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
        const code = typeof status === 'string' ? status : this.field(status, 'Code', 'code');
        if (code && code !== 'OK') {
            throw new Error(this.field(status, 'Reason', 'reason') || this.field(data, 'ErrorReason', 'errorReason') || code);
        }
    },

    tabletId: function(tablet) {
        return this.field(tablet, 'TabletId', 'tabletId');
    },

    loadSnapshot: function(tabletId, row) {
        const token = this.requestToken;
        if (row.data('loading-snapshot')) return;
        row.data('loading-snapshot', true);
        $('#nbs2-tablet-loading-' + tabletId).remove();
        const loading = $('<tr id="nbs2-tablet-loading-' + tabletId + '"><td colspan="5">Loading snapshot...</td></tr>').insertAfter(row);
        const showError = (message) => {
            if (token !== this.requestToken) return;
            row.removeData('loading-snapshot');
            loading.children('td').addClass('text-danger').text('Failed to load snapshot: ' + message);
        };
        $.getJSON('cms/api/json/ddisk/tablet', {
            tablet_id: String(tabletId),
            include_health: 1,
        }).done((snapshot) => {
            if (token !== this.requestToken) return;
            try {
                this.checkStatus(snapshot);
            } catch (error) {
                showError(error.message);
                return;
            }
            row.removeData('loading-snapshot');
            loading.replaceWith(this.detailsHtml(tabletId, snapshot));
        }).fail((xhr) => showError(xhr.statusText));
    },

    diskId: function(id) {
        return (this.field(id, 'NodeId', 'nodeId') || 0) + ':' + (this.field(id, 'PDiskId', 'pdiskId') || 0);
    },

    ddiskKey: function(id) {
        return this.diskId(id) + ':' + (this.field(id, 'DDiskSlotId', 'dDiskSlotId', 'ddiskSlotId') || 0);
    },

    formatDisk: function(id, role, component) {
        const nodeId = this.field(id, 'NodeId', 'nodeId') || 0;
        const pdiskId = this.field(id, 'PDiskId', 'pdiskId') || 0;
        const slotId = this.field(id, 'DDiskSlotId', 'dDiskSlotId', 'ddiskSlotId') || 0;
        const label = nodeId + ':' + pdiskId + ':' + slotId;
        const hint = 'Node: ' + (nodeId || '-') + '\nPDisk State: ' + (component?.pdiskState || 'Unknown') +
            '\n' + role + ': ' + (component?.status || 'Unknown');
        const url = role === 'PersistentBuffer'
            ? this.persistentBufferUrl(nodeId, pdiskId, slotId)
            : this.ddiskUrl(nodeId, pdiskId, slotId);
        return '<span class="nbs2-disk ' + (component?.unavailable ? 'nbs2-disk-unavailable' : '') +
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
        return '/node/' + encodeURIComponent(nodeId) + '/actors/persistent_buffer?formPresent=1&autoRefresh=1&describeFreeSpace=1&refreshRate=1&pb=' +
            encodeURIComponent(pb) + '&' + encodeURIComponent('tabletOpen.' + pb) + '=1';
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
        $('#nbs2-tablets-group-degrade').prop('disabled', this.isDDisksView());
        if (this.isDDisksView()) {
            this.renderDDisks();
            return;
        }
        $('#nbs2-tablets-table').show();
        $('#nbs2-ddisks-table').hide();
        const body = $('#nbs2-tablets-body').empty();

        let previousDegrade;
        this.tablets.forEach((tablet) => {
            const degrade = Number(this.field(tablet, 'Degrade', 'degrade')) || 0;
            if ($('#nbs2-tablets-group-degrade').prop('checked') && degrade !== previousDegrade) {
                body.append('<tr class="table-secondary nbs2-degrade-group"><th colspan="5">Degrade ' + degrade + '</th></tr>');
                previousDegrade = degrade;
            }
            const id = this.tabletId(tablet);
            const groupsCount = this.field(tablet, 'GroupsCount', 'groupsCount');
            const lastChangedAt = this.field(tablet, 'LastChangedAt', 'lastChangedAt');
            const unavailableDDisk = Number(this.field(tablet, 'UnavailableDDiskCount', 'unavailableDDiskCount')) || 0;
            const unavailablePersistentBuffer = Number(this.field(tablet, 'UnavailablePersistentBufferCount', 'unavailablePersistentBufferCount')) || 0;
            body.append('<tr class="nbs2-tablet-row" data-tablet-id="' + id + '"><td><a href="#">' + id + '</a></td><td>' + (groupsCount || '-') + '</td><td class="' + (unavailableDDisk ? 'nbs2-count-unavailable' : '') + '">' + unavailableDDisk + '</td><td class="' + (unavailablePersistentBuffer ? 'nbs2-count-unavailable' : '') + '">' + unavailablePersistentBuffer + '</td><td>' + this.date(lastChangedAt) + '</td></tr>');
        });
    },

    formatOccupancy: function(value) {
        if (value === undefined || value === null || value === '') return '-';
        const occupancy = Number(value);
        return Number.isFinite(occupancy) && occupancy >= 0 ? (occupancy * 100).toFixed(1) + '%' : '-';
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
            const health = this.diskHealth(disk);
            const failedDDisk = ddiskTabletIds.length && health.DDisk.unavailable;
            const failedBuffer = persistentBufferTabletIds.length && health.PersistentBuffer.unavailable;
            const linkRole = ddiskTabletIds.length && (failedDDisk || !failedBuffer) ? 'DDisk' : 'PersistentBuffer';
            const key = this.ddiskKey(diskIdObj);
            const problems = [];
            if (failedDDisk) problems.push('DDisk: ' + (health.DDisk.status || health.DDisk.pdiskState || 'Unknown'));
            if (failedBuffer) problems.push('Persistent Buffer: ' + (health.PersistentBuffer.status || health.PersistentBuffer.pdiskState || 'Unknown'));
            const status = problems.join('; ') || this.field(disk, 'State', 'state') || 'Unknown';
            const detailsId = key.replace(/:/g, '-');
            const roles = [];
            const tabletIds = new Set(ddiskTabletIds.concat(persistentBufferTabletIds));
            if (ddiskTabletIds.length) roles.push('<div><b>DDisk:</b> ' + this.formatDisk(diskIdObj, 'DDisk', health.DDisk) + ' (' + ddiskTabletIds.map((id) => this.escapeHtml(id)).join(', ') + ')</div>');
            if (persistentBufferTabletIds.length) roles.push('<div><b>Persistent Buffer:</b> ' + this.formatDisk(diskIdObj, 'PersistentBuffer', health.PersistentBuffer) + ' (' + persistentBufferTabletIds.map((id) => this.escapeHtml(id)).join(', ') + ')</div>');
            body.append('<tr class="nbs2-ddisk-row" data-ddisk-key="' + this.escapeHtml(key) + '">' +
                '<td>' + this.formatDisk(diskIdObj, linkRole, health[linkRole]) + '</td><td>' + (this.field(diskIdObj, 'NodeId', 'nodeId') || '-') +
                '</td><td>' + (this.field(diskIdObj, 'PDiskId', 'pdiskId') || '-') + '</td><td class="' +
                (problems.length ? 'nbs2-count-unavailable' : '') + '">' + this.escapeHtml(status) +
                '</td><td>' + tabletIds.size + '</td><td>' +
                this.formatOccupancy(this.field(disk, 'DDiskOccupancy', 'dDiskOccupancy', 'ddiskOccupancy')) + '</td><td>' +
                this.formatOccupancy(this.field(disk, 'PersistentBufferOccupancy', 'persistentBufferOccupancy')) + '</td></tr>' +
                '<tr id="nbs2-ddisk-details-' + this.escapeHtml(detailsId) + '" style="display: none"><td colspan="7">' +
                (roles.join('') || '-') + '</td></tr>');
        });
    },

    groupDegrade: function(group, health) {
        const countUnavailable = (disks, role) => disks.filter((disk) => health[this.ddiskKey(disk)]?.[role].unavailable).length;
        const ddisks = this.field(group, 'DDiskId', 'dDiskId', 'ddiskId') || [];
        const buffers = this.field(group, 'PersistentBufferDDiskId', 'persistentBufferDDiskId') || [];
        return Math.max(countUnavailable(ddisks, 'DDisk'), countUnavailable(buffers, 'PersistentBuffer'));
    },

    detailsHtml: function(id, snapshot) {
        const health = {};
        for (const slot of this.field(snapshot, 'Slots', 'slots') || []) {
            if (this.diskId(slot) !== '0:0') {
                health[this.ddiskKey(slot)] = this.diskHealth(slot);
            }
        }
        const formatLayout = (disks, role) => disks.map((disk) =>
            this.formatDisk(disk, role, health[this.ddiskKey(disk)]?.[role])).join(' ');
        const groups = (this.field(snapshot, 'Groups', 'groups') || []).map((group) => ({
            group, degrade: this.groupDegrade(group, health),
        })).sort((a, b) => b.degrade - a.degrade);
        let html = '<tr id="nbs2-tablet-details-' + id + '"><td colspan="5"><table class="table table-sm mb-0"><thead><tr><th>DBG</th><th>Degrade</th><th>DDisk layout</th><th>Persistent buffer</th></tr></thead><tbody>';
        groups.forEach(({group, degrade}) => {
            const groupId = this.field(group, 'DirectBlockGroupId', 'directBlockGroupId');
            const ddiskIds = this.field(group, 'DDiskId', 'dDiskId', 'ddiskId') || [];
            const persistentBufferIds = this.field(group, 'PersistentBufferDDiskId', 'persistentBufferDDiskId') || [];
            html += '<tr><td>' + (groupId || 0) + '</td><td>' + degrade + '</td><td>' + formatLayout(ddiskIds, 'DDisk') + '</td><td>' + formatLayout(persistentBufferIds, 'PersistentBuffer') + '</td></tr>';
        });
        return html + '</tbody></table></td></tr>';
    },

    date: function(micros) {
        return micros ? new Date(Number(micros) / 1000).toLocaleString() : '-';
    }
};

function initNbs2TabletsTab() {
    Nbs2Tablets.init();
}
