'use strict';

const Nbs2Tablets = {
    tablets: [],
    snapshots: {},
    disks: {},

    init: function() {
        $('#nbs2-tablets-view').on('change', () => this.render());
        $('#nbs2-tablets-sort').on('change', () => this.render());
        $('#nbs2-tablets-problems').on('change', () => this.render());
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
        $('#nbs2-tablets-body').on('click', 'a[href="#"]', (event) => event.preventDefault());
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
                this.tablets = this.field(data, 'Tablets', 'tablets') || [];
                const snapshotRequests = this.tablets.map((tablet) => {
                    const tabletId = this.tabletId(tablet);
                    return $.getJSON('cms/api/json/ddisk/tablet?tablet_id=' + encodeURIComponent(tabletId))
                        .done((snapshot) => { this.snapshots[tabletId] = snapshot; });
                });
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
        this.tablets.forEach((tablet) => {
            const tabletId = this.tabletId(tablet);
            (this.snapshots[tabletId] ? this.groups(tabletId) : []).forEach((group) => {
                (this.field(group, 'DDiskId', 'dDiskId', 'ddiskId') || [])
                    .concat(this.field(group, 'PersistentBufferDDiskId', 'persistentBufferDDiskId') || [])
                    .forEach((id) => {
                        const nodeId = this.field(id, 'NodeId', 'nodeId');
                        ids[this.diskId(id)] = id;
                        if (!nodes[nodeId]) nodes[nodeId] = id;
                    });
            });
        });

        // pdiskinfo requires pdisk_id, but its Whiteboard part contains the
        // complete PDiskStateInfo list for the node. One request per node is
        // therefore sufficient; use the first requested PDisk as the query id.
        const nodeListRequest = $.getJSON('viewer/nodelist')
            .done((data) => {
                (Array.isArray(data) ? data : []).forEach((node) => {
                    const nodeId = this.field(node, 'Id', 'id');
                    locations[nodeId] = this.field(node, 'PhysicalLocation', 'physicalLocation') || {};
                });
            });
        const requests = Object.keys(nodes).map((nodeId) => {
            const queryId = nodes[nodeId];
            return $.getJSON('viewer/pdiskinfo?node_id=' + encodeURIComponent(nodeId) +
                    '&pdisk_id=' + encodeURIComponent(this.field(queryId, 'PDiskId', 'pdiskId')))
                .done((response) => {
                    Object.keys(ids).forEach((key) => {
                        const id = ids[key];
                        if (String(this.field(id, 'NodeId', 'nodeId')) === String(nodeId)) {
                            this.disks[key] = {
                                available: this.diskAvailability(response, id),
                                path: this.diskPath(response, id),
                                location: locations[this.field(id, 'NodeId', 'nodeId')] || {},
                            };
                        }
                    });
                })
                .fail(() => {
                    Object.keys(ids).forEach((key) => {
                        if (String(this.field(ids[key], 'NodeId', 'nodeId')) === String(nodeId)) {
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
            Number(this.field(item, 'PDiskId', 'pdiskId')) === Number(this.field(diskId, 'PDiskId', 'pdiskId')));
        return pdisk && this.field(pdisk, 'Path', 'path') || '';
    },

    diskAvailability: function(data, diskId) {
        const pdisks = this.field(data, 'PDiskStateInfo', 'pdiskStateInfo') || [];
        const pdisk = pdisks.find((item) =>
            Number(this.field(item, 'PDiskId', 'pdiskId')) === Number(this.field(diskId, 'PDiskId', 'pdiskId')));
        if (pdisk) {
            // TPDiskState.E::Normal is 10 and means that the PDisk is working.
            return this.isNormalPDiskState(this.field(pdisk, 'State', 'state'));
        }

        const bsc = this.field(data, 'BSC', 'bsc');
        const bscPDisk = bsc && this.field(bsc, 'PDisk', 'pdisk');
        const status = this.field(bscPDisk, 'StatusV2', 'statusV2', 'Status', 'status');
        return status === undefined || status === null ? false : this.isActiveDriveStatus(status);
    },

    tabletId: function(tablet) {
        return this.field(tablet, 'TabletId', 'tabletId');
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
        return (this.field(id, 'NodeId', 'nodeId') || 0) + ':' + (this.field(id, 'PDiskId', 'pdiskId') || 0);
    },

    isUnavailable: function(id) {
        const key = this.diskId(id);
        return this.disks[key] && this.disks[key].available === false;
    },

    groups: function(tabletId) {
        const snapshot = this.snapshots[tabletId];
        return this.field(snapshot, 'Groups', 'groups') || [];
    },

    unavailableCount: function(tabletId, role) {
        return this.groups(tabletId).reduce((sum, group) => {
            const ids = role === 'PersistentBuffer'
                ? this.field(group, 'PersistentBufferDDiskId', 'persistentBufferDDiskId') || []
                : role === 'DDisk'
                    ? this.field(group, 'DDiskId', 'dDiskId', 'ddiskId') || []
                    : (this.field(group, 'DDiskId', 'dDiskId', 'ddiskId') || [])
                        .concat(this.field(group, 'PersistentBufferDDiskId', 'persistentBufferDDiskId') || []);
            return sum + ids.filter((id) => this.isUnavailable(id)).length;
        }, 0);
    },

    formatDisk: function(id, role) {
        const nodeId = this.field(id, 'NodeId', 'nodeId') || 0;
        const pdiskId = this.field(id, 'PDiskId', 'pdiskId') || 0;
        const slotId = this.field(id, 'DDiskSlotId', 'dDiskSlotId', 'ddiskSlotId') || 0;
        const label = nodeId + ':' + pdiskId + ':' + slotId;
        const disk = this.disks[this.diskId(id)] || {};
        const location = disk.location || {};
        const locationString = location.Location || '';
        const hint = [
            'Node: ' + (nodeId || '—'),
            'Location: ' + locationString,
            'Path: ' + (disk.path || '—'),
        ].join('\n');
        const url = role === 'PersistentBuffer'
            ? this.persistentBufferUrl(nodeId, pdiskId, slotId)
            : this.ddiskUrl(nodeId, pdiskId, slotId);
        return '<span class="nbs2-disk ' + (this.isUnavailable(id) ? 'nbs2-disk-unavailable' : '') +
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
        const onlyProblems = $('#nbs2-tablets-problems').prop('checked');
        const tablets = this.tablets.slice().filter((tablet) =>
            !onlyProblems || this.unavailableCount(this.tabletId(tablet)) > 0
        ).sort((a, b) => {
            const aId = this.tabletId(a);
            const bId = this.tabletId(b);
            const sort = $('#nbs2-tablets-sort').val();
            if (sort === 'tablet') return Number(aId) - Number(bId);
            return this.unavailableCount(bId) - this.unavailableCount(aId) || Number(aId) - Number(bId);
        });
        tablets.forEach((tablet) => {
            const id = this.tabletId(tablet);
            const groups = this.groups(id);
            const unavailableDDisk = this.unavailableCount(id, 'DDisk');
            const unavailablePersistentBuffer = this.unavailableCount(id, 'PersistentBuffer');
            const groupsCount = this.field(tablet, 'GroupsCount', 'groupsCount');
            const lastChangedAt = this.field(tablet, 'LastChangedAt', 'lastChangedAt');
            body.append('<tr class="nbs2-tablet-row" data-tablet-id="' + id + '"><td><a href="#">' + id + '</a></td><td>' + (groups.length || groupsCount || '—') + '</td><td class="' + (unavailableDDisk ? 'nbs2-count-unavailable' : '') + '">' + unavailableDDisk + '</td><td class="' + (unavailablePersistentBuffer ? 'nbs2-count-unavailable' : '') + '">' + unavailablePersistentBuffer + '</td><td>' + this.date(lastChangedAt) + '</td></tr>');
            if (this.snapshots[id]) this.renderDetails(body, id, groups);
        });
    },

    renderDDisks: function() {
        $('#nbs2-tablets-table').hide();
        const body = $('#nbs2-ddisks-body').empty();
        $('#nbs2-ddisks-table').show();
        const disks = {};
        this.tablets.forEach((tablet) => {
            const tabletId = this.tabletId(tablet);
            this.groups(tabletId).forEach((group) => {
                (this.field(group, 'DDiskId', 'dDiskId', 'ddiskId') || []).forEach((id) => this.addDiskUsage(disks, id, tabletId, 'DDisk'));
                (this.field(group, 'PersistentBufferDDiskId', 'persistentBufferDDiskId') || []).forEach((id) => this.addDiskUsage(disks, id, tabletId, 'PersistentBuffer'));
            });
        });

        const onlyProblems = $('#nbs2-tablets-problems').prop('checked');
        const sort = $('#nbs2-tablets-sort').val();
        Object.keys(disks).map((key) => disks[key]).filter((disk) =>
            !onlyProblems || this.isUnavailable(disk.id)
        ).sort((a, b) => {
            const unavailable = (this.isUnavailable(b.id) ? 1 : 0) - (this.isUnavailable(a.id) ? 1 : 0);
            return (sort === 'unavailable' ? unavailable : 0) || this.compareDiskIds(a.id, b.id);
        }).forEach((disk) => {
            const key = this.ddiskKey(disk.id);
            const status = this.isUnavailable(disk.id) ? 'Unavailable' : 'Available';
            const detailsId = key.replace(/:/g, '-');
            const roles = [];
            const tabletIds = new Set(disk.ddisk.concat(disk.persistentBuffer));
            if (disk.ddisk.length) roles.push('<div><b>DDisk:</b> ' + this.formatDisk(disk.id, 'DDisk') + ' (' + disk.ddisk.map((id) => this.escapeHtml(id)).join(', ') + ')</div>');
            if (disk.persistentBuffer.length) roles.push('<div><b>Persistent Buffer:</b> ' + this.formatDisk(disk.id, 'PersistentBuffer') + ' (' + disk.persistentBuffer.map((id) => this.escapeHtml(id)).join(', ') + ')</div>');
            body.append('<tr class="nbs2-ddisk-row" data-ddisk-key="' + this.escapeHtml(key) + '">' +
                '<td>' + this.formatDisk(disk.id, disk.ddisk.length ? 'DDisk' : 'PersistentBuffer') + '</td><td>' + (this.field(disk.id, 'NodeId', 'nodeId') || '—') +
                '</td><td>' + (this.field(disk.id, 'PDiskId', 'pdiskId') || '—') + '</td><td class="' +
                (this.isUnavailable(disk.id) ? 'nbs2-count-unavailable' : '') + '">' + status +
                '</td><td>' + tabletIds.size + '</td></tr>' +
                '<tr id="nbs2-ddisk-details-' + this.escapeHtml(detailsId) + '" style="display: none"><td colspan="5">' +
                (roles.join('') || '—') + '</td></tr>');
        });
    },

    compareDiskIds: function(a, b) {
        return Number(this.field(a, 'NodeId', 'nodeId') || 0) - Number(this.field(b, 'NodeId', 'nodeId') || 0) ||
            Number(this.field(a, 'PDiskId', 'pdiskId') || 0) - Number(this.field(b, 'PDiskId', 'pdiskId') || 0) ||
            Number(this.field(a, 'DDiskSlotId', 'dDiskSlotId', 'ddiskSlotId') || 0) - Number(this.field(b, 'DDiskSlotId', 'dDiskSlotId', 'ddiskSlotId') || 0);
    },

    ddiskKey: function(id) {
        return this.diskId(id) + ':' + (this.field(id, 'DDiskSlotId', 'dDiskSlotId', 'ddiskSlotId') || 0);
    },

    addDiskUsage: function(disks, id, tabletId, role) {
        const key = this.ddiskKey(id);
        if (!disks[key]) disks[key] = {id: id, ddisk: [], persistentBuffer: []};
        const target = role === 'DDisk' ? disks[key].ddisk : disks[key].persistentBuffer;
        if (target.indexOf(String(tabletId)) === -1) target.push(String(tabletId));
    },

    renderDetails: function(body, id, groups) {
        let html = '<tr id="nbs2-tablet-details-' + id + '"><td colspan="5"><table class="table table-sm mb-0"><thead><tr><th>DBG</th><th>DDisk layout</th><th>Persistent buffer</th></tr></thead><tbody>';
        const onlyProblems = $('#nbs2-tablets-problems').prop('checked');
        groups.filter((group) => {
            if (!onlyProblems) return true;
            const ddiskIds = this.field(group, 'DDiskId', 'dDiskId', 'ddiskId') || [];
            const persistentBufferIds = this.field(group, 'PersistentBufferDDiskId', 'persistentBufferDDiskId') || [];
            return ddiskIds.concat(persistentBufferIds).some((id) => this.isUnavailable(id));
        }).forEach((group) => {
            const groupId = this.field(group, 'DirectBlockGroupId', 'directBlockGroupId');
            const ddiskIds = this.field(group, 'DDiskId', 'dDiskId', 'ddiskId') || [];
            const persistentBufferIds = this.field(group, 'PersistentBufferDDiskId', 'persistentBufferDDiskId') || [];
            html += '<tr><td>' + (groupId || 0) + '</td><td>' + ddiskIds.map((disk) => this.formatDisk(disk, 'DDisk')).join(' ') + '</td><td>' + persistentBufferIds.map((disk) => this.formatDisk(disk, 'PersistentBuffer')).join(' ') + '</td></tr>';
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
