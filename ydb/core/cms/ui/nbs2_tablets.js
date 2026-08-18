'use strict';

const Nbs2Tablets = {
    tablets: [],
    snapshots: {},
    disks: {},

    init: function() {
        $('#nbs2-tablets-sort').on('change', () => this.render());
        $('#nbs2-tablets-refresh').on('click', () => this.load());
        $('#nbs2-tablets-body').on('click', 'tr.nbs2-tablet-row', (event) => {
            const tabletId = $(event.currentTarget).data('tablet-id').toString();
            const details = $('#nbs2-tablet-details-' + tabletId);
            if (details.length) {
                details.toggle();
                return;
            }
            this.loadSnapshot(tabletId, $(event.currentTarget));
        });
        $('#nbs2-tablets-body').on('click', 'a', (event) => event.stopPropagation());
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
        this.tablets.forEach((tablet) => (this.snapshots[tablet.TabletId] ? this.groups(tablet.TabletId) : []).forEach((group) => {
            (group.DDiskId || []).concat(group.PersistentBufferDDiskId || []).forEach((id) => { ids[this.diskId(id)] = id; });
        }));
        const requests = Object.keys(ids).map((key) => {
            const id = ids[key];
            return $.getJSON('viewer/pdiskinfo?node_id=' + id.NodeId + '&pdisk_id=' + id.PDiskId)
                .done((data) => {
                    const info = data.BSC && data.BSC.PDisk || data.PDisk || data;
                    this.disks[key] = info.StatusV2 === 'ACTIVE' || info.StatusV2 === 'NORMAL';
                })
                .fail(() => { this.disks[key] = false; });
        });
        return $.when.apply($, requests);
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
        return this.disks[key] !== true;
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
        return '<span class="nbs2-disk ' + (this.isUnavailable(id) ? 'nbs2-disk-unavailable' : '') + '">' + label + '</span>';
    },

    render: function() {
        const body = $('#nbs2-tablets-body').empty();
        const tablets = this.tablets.slice().sort((a, b) => {
            if ($('#nbs2-tablets-sort').val() === 'tablet') return Number(a.TabletId) - Number(b.TabletId);
            return this.unavailableCount(b.TabletId) - this.unavailableCount(a.TabletId) || Number(a.TabletId) - Number(b.TabletId);
        });
        tablets.forEach((tablet) => {
            const id = tablet.TabletId;
            const groups = this.groups(id);
            const unavailable = this.unavailableCount(id);
            body.append('<tr class="nbs2-tablet-row" data-tablet-id="' + id + '"><td><a href="javascript:void(0)">' + id + '</a></td><td>' + (groups.length || tablet.GroupsCount || '—') + '</td><td class="' + (unavailable ? 'nbs2-count-unavailable' : '') + '">' + unavailable + '</td><td>' + (tablet.Revision || '—') + '</td><td>' + this.date(tablet.LastChangedAt) + '</td></tr>');
            if (this.snapshots[id]) this.renderDetails(body, id, groups);
        });
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
