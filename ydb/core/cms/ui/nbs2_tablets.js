'use strict';

const Nbs2Tablets = {
    tablets: [],
    snapshots: {},
    disks: {},

    init: function() {
        $('#nbs2-tablets-view').on('change', () => this.render());
        $('#nbs2-tablets-sort').on('change', () => this.render());
        $('#nbs2-tablets-refresh').on('click', () => this.load());
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
        $('#nbs2-tablets-body').on('click', 'a', (event) => event.preventDefault());
        $('a[href="#nbs2-tablets"]').on('shown.bs.tab', () => {
            if (!this.tablets.length) this.load();
        });
    },

    load: function() {
        $('#nbs2-tablets-error').text('Loading...');
        $.getJSON('cms/api/json/ddisk/tablets')
            .done((data) => {
                if (data.Status && data.Status !== 'OK') {
                    throw new Error(data.ErrorReason || data.Status);
                }
                this.tablets = data.Tablets || [];
                const snapshotRequests = this.tablets.map((tablet) =>
                    $.getJSON('cms/api/json/ddisk/tablet?tablet_id=' + encodeURIComponent(tablet.TabletId))
                        .done((snapshot) => { this.snapshots[tablet.TabletId] = snapshot; })
                );
                $.when.apply($, snapshotRequests).always(() => {
                    this.loadDiskStatuses().always(() => {
                        $('#nbs2-tablets-error').empty();
                        this.render();
                    });
                });
            })
            .fail((xhr) => $('#nbs2-tablets-error').text('Failed to load NBS 2.0 tablets: ' + xhr.statusText));
    },

    loadDiskStatuses: function() {
        const ids = {};
        const nodes = {};
        const locations = {};
        this.tablets.forEach((tablet) => (this.snapshots[tablet.TabletId] ? this.groups(tablet.TabletId) : []).forEach((group) => {
            (group.DDiskId || []).concat(group.PersistentBufferDDiskId || []).forEach((id) => {
                ids[this.diskId(id)] = id;
                if (!nodes[id.NodeId]) nodes[id.NodeId] = id;
            });
        }));

        // pdiskinfo requires pdisk_id, but its Whiteboard part contains the
        // complete PDiskStateInfo list for the node. One request per node is
        // therefore sufficient; use the first requested PDisk as the query id.
        const nodeListRequest = $.getJSON('viewer/nodelist')
            .done((data) => {
                (Array.isArray(data) ? data : []).forEach((node) => {
                    locations[node.Id] = node.PhysicalLocation || {};
                });
            });
        const requests = Object.keys(nodes).map((nodeId) => {
            const queryId = nodes[nodeId];
            return $.getJSON('viewer/pdiskinfo?node_id=' + encodeURIComponent(nodeId) +
                    '&pdisk_id=' + encodeURIComponent(queryId.PDiskId))
                .done((response) => {
                    Object.keys(ids).forEach((key) => {
                        const id = ids[key];
                        if (String(id.NodeId) === String(nodeId)) {
                            this.disks[key] = {
                                available: this.diskAvailability(response, id),
                                path: this.diskPath(response, id),
                                location: locations[id.NodeId] || {},
                            };
                        }
                    });
                })
                .fail(() => {
                    Object.keys(ids).forEach((key) => {
                        if (String(ids[key].NodeId) === String(nodeId)) {
                            this.disks[key] = {available: false};
                        }
                    });
                });
        });
        return $.when(nodeListRequest, $.when.apply($, requests));
    },

    diskPath: function(data, diskId) {
        const pdisks = this.field(data, 'PDiskStateInfo', 'pdiskStateInfo') || [];
        const pdisk = pdisks.find((item) =>
            Number(this.field(item, 'PDiskId', 'pdiskId')) === Number(diskId.PDiskId));
        return pdisk && this.field(pdisk, 'Path', 'path') || '';
    },

    diskAvailability: function(data, diskId) {
        const pdisks = this.field(data, 'PDiskStateInfo', 'pdiskStateInfo') || [];
        const pdisk = pdisks.find((item) =>
            Number(this.field(item, 'PDiskId', 'pdiskId')) === Number(diskId.PDiskId));
        if (pdisk) {
            // TPDiskState.E::Normal is 10 and means that the PDisk is working.
            return this.isNormalPDiskState(this.field(pdisk, 'State', 'state'));
        }

        const bsc = this.field(data, 'BSC', 'bsc');
        const bscPDisk = bsc && this.field(bsc, 'PDisk', 'pdisk');
        const status = this.field(bscPDisk, 'StatusV2', 'statusV2', 'Status', 'status');
        return status === undefined || status === null ? false : this.isActiveDriveStatus(status);
    },

    field: function(object, ...names) {
        if (!object) return undefined;
        for (const name of names) {
            if (object[name] !== undefined) return object[name];
        }
        return undefined;
    },

    isNormalPDiskState: function(value) {
        if (typeof value === 'number') return value === 10;
        return String(value).toUpperCase() === 'NORMAL';
    },

    isActiveDriveStatus: function(value) {
        if (typeof value === 'number') return value === 1;
        const status = String(value).toUpperCase();
        return status === 'ACTIVE' || status === 'NORMAL' || status === 'OK';
    },

    loadSnapshot: function(tabletId, row) {
        row.after('<tr id="nbs2-tablet-loading-' + tabletId + '"><td colspan="5">Loading snapshot...</td></tr>');
        $.getJSON('cms/api/json/ddisk/tablet?tablet_id=' + encodeURIComponent(tabletId))
            .done((snapshot) => {
                this.snapshots[tabletId] = snapshot;
                $('#nbs2-tablet-loading-' + tabletId).remove();
                this.render();
                $('#nbs2-tablet-details-' + tabletId).show();
            })
            .fail((xhr) => $('#nbs2-tablet-loading-' + tabletId).html('<td colspan="5" class="text-danger">Failed to load snapshot: ' + xhr.statusText + '</td>'));
    },

    diskId: function(id) {
        return (id.NodeId || 0) + ':' + (id.PDiskId || 0);
    },

    isUnavailable: function(id) {
        const key = this.diskId(id);
        return this.disks[key] && this.disks[key].available === false;
    },

    groups: function(tabletId) {
        const snapshot = this.snapshots[tabletId];
        return snapshot && snapshot.Groups || [];
    },

    unavailableCount: function(tabletId) {
        return this.groups(tabletId).reduce((sum, group) => sum + (group.DDiskId || []).filter((id) => this.isUnavailable(id)).length + (group.PersistentBufferDDiskId || []).filter((id) => this.isUnavailable(id)).length, 0);
    },

    formatDisk: function(id) {
        const label = (id.NodeId || 0) + ':' + (id.PDiskId || 0) + ':' + (id.DDiskSlotId || 0);
        const disk = this.disks[this.diskId(id)] || {};
        const location = disk.location || {};
        const locationString = location.Location || '';
        const dc = location.DataCenterId || location.DataCenter || '—';
        const rack = location.RackId || this.locationPart(locationString, 'Rack') || '—';
        const hint = [
            'Node: ' + (id.NodeId || '—'),
            'DC: ' + dc,
            'Rack: ' + rack,
            'Path: ' + (disk.path || '—'),
        ].join('\n');
        return '<span class="nbs2-disk ' + (this.isUnavailable(id) ? 'nbs2-disk-unavailable' : '') +
            '" title="' + this.escapeHtml(hint) + '">' + label + '</span>';
    },

    locationPart: function(location, name) {
        const match = location.match(new RegExp('(?:^|/)' + name + '=([^/]+)', 'i'));
        return match ? match[1] : '';
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

    render: function() {
        if ($('#nbs2-tablets-view').val() === 'ddisks') {
            this.renderDDisks();
            return;
        }
        $('#nbs2-tablets-table').show();
        $('#nbs2-ddisks-table').hide();
        const body = $('#nbs2-tablets-body').empty();
        const tablets = this.tablets.slice().sort((a, b) => {
            if ($('#nbs2-tablets-sort').val() === 'tablet') return Number(a.TabletId) - Number(b.TabletId);
            return this.unavailableCount(b.TabletId) - this.unavailableCount(a.TabletId) || Number(a.TabletId) - Number(b.TabletId);
        });
        tablets.forEach((tablet) => {
            const id = tablet.TabletId;
            const groups = this.groups(id);
            const unavailable = this.unavailableCount(id);
            body.append('<tr class="nbs2-tablet-row" data-tablet-id="' + id + '"><td><a href="#">' + id + '</a></td><td>' + (groups.length || tablet.GroupsCount || '—') + '</td><td class="' + (unavailable ? 'nbs2-count-unavailable' : '') + '">' + unavailable + '</td><td>' + (tablet.Revision || '—') + '</td><td>' + this.date(tablet.LastChangedAt) + '</td></tr>');
            if (this.snapshots[id]) this.renderDetails(body, id, groups);
        });
    },

    renderDDisks: function() {
        $('#nbs2-tablets-table').hide();
        const body = $('#nbs2-ddisks-body').empty();
        $('#nbs2-ddisks-table').show();
        const disks = {};
        this.tablets.forEach((tablet) => {
            const tabletId = tablet.TabletId;
            this.groups(tabletId).forEach((group) => {
                (group.DDiskId || []).forEach((id) => this.addDiskUsage(disks, id, tabletId, 'DDisk'));
                (group.PersistentBufferDDiskId || []).forEach((id) => this.addDiskUsage(disks, id, tabletId, 'PersistentBuffer'));
            });
        });

        Object.keys(disks).map((key) => disks[key]).sort((a, b) => {
            const unavailable = (this.isUnavailable(b.id) ? 1 : 0) - (this.isUnavailable(a.id) ? 1 : 0);
            return unavailable || Number(a.id.NodeId) - Number(b.id.NodeId) ||
                Number(a.id.PDiskId) - Number(b.id.PDiskId) ||
                Number(a.id.DDiskSlotId) - Number(b.id.DDiskSlotId);
        }).forEach((disk) => {
            const key = this.ddiskKey(disk.id);
            const status = this.isUnavailable(disk.id) ? 'Unavailable' : 'Available';
            const detailsId = key.replace(/:/g, '-');
            const roles = [];
            const tabletIds = new Set(disk.ddisk.concat(disk.persistentBuffer));
            if (disk.ddisk.length) roles.push('<div><b>DDisk:</b> ' + disk.ddisk.map((id) => this.escapeHtml(id)).join(', ') + '</div>');
            if (disk.persistentBuffer.length) roles.push('<div><b>Persistent Buffer:</b> ' + disk.persistentBuffer.map((id) => this.escapeHtml(id)).join(', ') + '</div>');
            body.append('<tr class="nbs2-ddisk-row" data-ddisk-key="' + this.escapeHtml(key) + '">' +
                '<td>' + this.formatDisk(disk.id) + '</td><td>' + (disk.id.NodeId || '—') +
                '</td><td>' + (disk.id.PDiskId || '—') + '</td><td class="' +
                (this.isUnavailable(disk.id) ? 'nbs2-count-unavailable' : '') + '">' + status +
                '</td><td>' + tabletIds.size + '</td></tr>' +
                '<tr id="nbs2-ddisk-details-' + this.escapeHtml(detailsId) + '" style="display: none"><td colspan="5">' +
                (roles.join('') || '—') + '</td></tr>');
        });
    },

    ddiskKey: function(id) {
        return this.diskId(id) + ':' + (id.DDiskSlotId || 0);
    },

    addDiskUsage: function(disks, id, tabletId, role) {
        const key = this.ddiskKey(id);
        if (!disks[key]) disks[key] = {id: id, ddisk: [], persistentBuffer: []};
        const target = role === 'DDisk' ? disks[key].ddisk : disks[key].persistentBuffer;
        if (target.indexOf(String(tabletId)) === -1) target.push(String(tabletId));
    },

    renderDetails: function(body, id, groups) {
        let html = '<tr id="nbs2-tablet-details-' + id + '"><td colspan="5"><table class="table table-sm mb-0"><thead><tr><th>DBG</th><th>DDisk layout</th><th>Persistent buffer</th></tr></thead><tbody>';
        groups.forEach((group) => {
            html += '<tr><td>' + (group.DirectBlockGroupId || 0) + '</td><td>' + (group.DDiskId || []).map((disk) => this.formatDisk(disk)).join(' ') + '</td><td>' + (group.PersistentBufferDDiskId || []).map((disk) => this.formatDisk(disk)).join(' ') + '</td></tr>';
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
