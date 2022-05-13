function TabletCell(options) {
    Object.assign(this, options);
    this.Count = 0;
    this.buildDomElement();
}

TabletCell.prototype.tabletTypes = {
    'Coordinator': 'C',
    'Mediator': 'M',
    'DataShard': 'DS',
    'Hive': 'H',
    'BSController': 'BS',
    'SchemeShard': 'SS',
    'Console': 'CN',
    'NodeBroker': 'NB',
    'TenantSlotBroker': 'TSB',
    'Cms': 'CMS',
    'TxAllocator': 'TA',
    'Kesus': 'K',
    'RTMRPartition': 'RP',
    'KeyValue': 'KV',
    'PersQueue': 'PQ',
    'PersQueueReadBalancer': 'PB',
    'BlockStorePartition': 'BP',
    'BlockStoreVolume': 'BV',
    'SequenceShard': 'S',
    'ReplicationController': 'RC',
    'TestShard': 'TS',
    'BlobDepot': 'BD',
};

TabletCell.prototype.getTabletType = function() {
    var t = this.tabletTypes[this.Type];
    if (t) {
        return t;
    }
    return this.Type.substr(0, 2);
}

TabletCell.prototype.buildDomElement = function() {
    var tablet = $('<div>', {class: 'tablet'});
    if (this.Leader) {
        tablet.css('background-color', green);
    } else {
        tablet.css('background-color', lightblue);
    }
    tablet.append($('<div>', {class: 'tablet_label', text: this.type = this.getTabletType()}));
    var counter = $('<div>', {class: 'tablet_counter'});
    tablet.append(counter);
    this.domElementCounter = counter[0];
    var tooltip = this.Type;
    if (!this.Leader) {
        tooltip += ' (follower)';
    }
    tablet.attr('data-original-title', tooltip);
    tablet.tooltip({html: true});
    return this.domElement = tablet[0];
}

TabletCell.prototype.setCount = function(count) {
    if (this.Count !== count) {
        var width = 0;
        if (this.Count === 1) {
            $(this.domElementCounter).show();
        }
        if (count === 1) {
            $(this.domElementCounter).hide();
            width = 20;
        } else if (count < 10) {
            width = 27;
        } else if (count < 100) {
            width = 34;
        } else if (count < 1000) {
            width = 41;
        } else {
            width = 48;
        }
        $(this.domElementCounter).text(count);
        if (this.type.length === 2) {
            width += 7;
        }
        if (this.type.length === 3) {
            width += 14;
        }
        this.Count = count;
        $(this.domElement).css('width', width + 'px');
    }
}

TabletCell.prototype.restoreTooltips = function() {
    $(this.domElement).tooltip({html: true});
}

