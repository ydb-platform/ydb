'use strict';

var ConfigsState = {
    updateIds: [],
    configItems: new Map(),
    itemKinds: new Map(),
    editableItemKinds: new Set(['LogConfigItem', 'CmsConfigItem',
                                'NodeBrokerConfigItem', 'SharedCacheConfigItem',
                                'ImmediateControlsConfigItem']),
    checkInterval: 5000,
    retryInterval: 5000,
}

class ItemKind {
    constructor(kindNo) {
        this.no = kindNo;
        this.name = cmsEnums.ItemKinds.get(kindNo);
        if (!this.name)
            this.name = 'UNKNOWN CONFIG ITEM';

        this.createConfigView = getConfigViewCreator(this.name);
        this.createConfigEditForm = getConfigEditFormCreator(this.name);

        this._createUIElements();

        console.assert(!ConfigsState.itemKinds.has(this.no));
        ConfigsState.itemKinds.set(this.no, this);

        if (ShownElements.has(this.itemsDiv.id))
            $(this.itemsDiv).collapse('show');
    }

    appendView(element) {
        this.itemsDiv.appendChild(element);
        $(this.blockDiv).show();
    }

    createNewItem() {
        this._createItemForm();
    }

    editItem(item) {
        this._createItemForm(item);
    }

    onViewRemoved() {
        if (this.itemsDiv.childNodes.length == 0)
            $(this.blockDiv).hide();
    }

    _createUIElements() {
        this.itemsDiv = document.createElement('div');
        this.itemsDiv.id = 'config-items-' + this.no;
        this.itemsDiv.setAttribute('class', 'collapse');
        this.itemsDiv.setAttribute('aria-expanded', false);

        this.editable = ConfigsState.editableItemKinds.has(this.name);
        if (this.editable) {
            let _this = this;
            this.addBtn = document.createElement('button');
            this.addBtn.setAttribute('class', 'btn btn-outline-secondary btn-block');
            this.addBtn.setAttribute('style', 'margin-top: 5px;');
            this.addBtn.textContent = 'Create new item';
            this.addBtn.addEventListener('click', function() { _this.createNewItem(); });
            this.itemsDiv.appendChild(this.addBtn);
        }

        this.toggleBtn = document.createElement('button');
        this.toggleBtn.setAttribute('class', 'btn btn-light btn-block');
        this.toggleBtn.setAttribute('aria-expanded', false);
        this.toggleBtn.dataset.toggle = "collapse";
        this.toggleBtn.dataset.target = "#" + this.itemsDiv.id;
        this.toggleBtn.textContent = this.name + 's';

        this.blockDiv = document.createElement('div');
        this.blockDiv.appendChild(this.toggleBtn);
        this.blockDiv.appendChild(this.itemsDiv);

        var minKind = Number.MAX_SAFE_INTEGER;
        var insertBefore = null;
        for (var [key, value] of ConfigsState.itemKinds) {
            if (key < minKind && key > this.no) {
                minKind = key;
                insertBefore = value.blockDiv;
            }
        }

        document.getElementById('config-items').insertBefore(this.blockDiv, insertBefore);

        $(this.itemsDiv).on('shown.bs.collapse', getOnShown(this.itemsDiv.id));
        $(this.itemsDiv).on('hidden.bs.collapse', getOnHidden(this.itemsDiv.id));
    }

    _createItemForm(item) {
        var form = this.createConfigEditForm(this, item);

        if (form) {
            var elem = form.getElement();
            if (item !== undefined) {
                this.itemsDiv.insertBefore(elem, item.view.getElement());
                item.hide();
            } else if (this.itemsDiv.childNodes.length > 1)
                this.itemsDiv.insertBefore(elem, this.itemsDiv.childNodes[1]);
            else
                this.itemsDiv.appendChild(elem);
        } else {
            $('#configs-error').html('cannot create ' + this.name + ' edit form');
        }
    }
}

function getOrCreateItemKind(kindNo) {
    var kind = ConfigsState.itemKinds.get(kindNo);
    if (!kind)
        kind = new ItemKind(kindNo);
    return kind;
}

class ConfigItem {
    constructor(data) {
        this._data = data;

        console.assert(!ConfigsState.configItems.has(this.getId()));
        ConfigsState.configItems.set(this.getId(), this);

        this.kind = getOrCreateItemKind(this.getKindNo());
        this.view = this.kind.createConfigView(this);

        this.update();
    }

    update(data) {
        if (data !== undefined) {
            console.assert(this.getId() == data.Id.Id);
            this._data = data;

            this.unmarkAsPendingUpdate();
        }

        this.view.update();
    }

    getConfig() {
        return this._data.Config;
    }

    getCookie() {
        return this._data.Cookie;
    }

    getGeneration() {
        return this._data.Id.Generation;
    }

    getId() {
        return this._data.Id.Id;
    }

    getIdObj() {
        return this._data.Id;
    }

    getKindNo() {
        return this._data.Kind;
    }

    getMergeStrategy() {
        return this._data.MergeStrategy;
    }

    getOrder() {
        return this._data.Order;
    }

    getUsageScope() {
        return this._data.UsageScope;
    }

    getIdString() {
        return this._data.Id.Id + "." + this._data.Id.Generation;
    }

    show() {
        this.view.show();
    }

    hide() {
        this.view.hide();
    }

    edit() {
        this.kind.editItem(this);
    }

    remove() {
        this.view.remove();
        ConfigsState.configItems.delete(this.getId());
    }

    markAsPendingUpdate() {
        this.view.markAsPendingUpdate();
    }

    unmarkAsPendingUpdate() {
        this.view.unmarkAsPendingUpdate();
    }
}

function onConfigItemsLoaded(data) {
    if (data['Status']['Code'] != 'SUCCESS') {
        onConfigItemsFailed(data);
        return;
    }

    $('#configs-error').html('');

    var items = data.ConfigItems;
    if (!items)
        items = [];

    for (var i = 0; i < items.length; ++i) {
        var item = ConfigsState.configItems.get(items[i].Id.Id);

        if (!item) {
            new ConfigItem(items[i]);
        } else {
            item.update(items[i]);
        }
    }

    setTimeout(checkConfigItemUpdates, ConfigsState.checkInterval);
}

function onConfigItemsFailed(data) {
    if (data && data['Status'] && data['Status']['Reason'])
        $('#configs-error').html(data['Status']['Reason']);
    else
        $('#configs-error').html("Cannot get config items");
    setTimeout(loadConfigItems, ConfigsState.retryInterval);
}

function onConfigUpdatesLoaded(data) {
    if (data['Status']['Code'] != 'SUCCESS') {
        onConfigUpdatesFailed(data);
        return;
    }

    $('#configs-error').html('');

    ConfigsState.updateIds = [];

    var added = data.AddedItems;
    if (!added)
        added = [];
    var removed = data.RemovedItems;
    if (!removed)
        removed = [];
    var updated = data.UpdatedItems;
    if (!updated)
        updated = [];

    for (var id of added)
        ConfigsState.updateIds.push(id.Id);
    for (var id of updated)
        ConfigsState.updateIds.push(id.Id);

    for (var id of removed) {
        var item = ConfigsState.configItems.get(id.Id);
        if (!item) {
            console.error('Cannot remove unknown config item ' + id.Id);
            continue;
        }

        item.remove();
    }

    if (ConfigsState.updateIds.length > 0)
        loadConfigItems();
    else
        setTimeout(checkConfigItemUpdates, ConfigsState.checkInterval);
}

function onConfigUpdatesFailed(data) {
    if (data && data['Status'] && data['Status']['Reason'])
        $('#configs-error').html(data['Status']['Reason']);
    else
        $('#configs-error').html("Cannot get config updates");
    setTimeout(checkConfigItemUpdates, ConfigsState.retryInterval);
}

function checkConfigItemUpdates() {
    var url = 'cms/api/json/configupdates';
    var items = Array.from(ConfigsState.configItems.values());
    for (var i = 0; i < items.length; ++i) {
        if (i == 0)
            url += '?base=';
        else
            url += ',';
        url += items[i].getIdString();
    }
    $.get(url).done(onConfigUpdatesLoaded).fail(onConfigUpdatesFailed);
}

function loadConfigItems() {
    var url = 'cms/api/json/configitems';
    if (ConfigsState.updateIds.length > 0)
        url += '?ids=' + ConfigsState.updateIds.toString();
    $.get(url).done(onConfigItemsLoaded).fail(onConfigItemsFailed);
}

function onYamlConfigEnabledFetched(data) {
    if (data) {
        if (data['enabled']) {
            $('#yaml-configs-enabled-warning').show();
        } else if ($('#yaml-config').hasClass('active')) {
            $('#cms-nav a[href="#configs"]').tab('show');
        }
    }
}

function loadConfigsContent() {
    for (var kind of ConfigsState.editableItemKinds)
        getOrCreateItemKind(cmsEnums.parse('ItemKinds', kind));

    loadConfigItems();

    var url = 'cms/yaml-config-enabled';
    $.get(url).done(onYamlConfigEnabledFetched);
}

function loadTypes() {
    cmsProtoTypes
        .add('.NKikimrConfig.TImmediateControlsConfig')
        .done(loadConfigsContent);
}

function initConfigsTab() {
    cmsEnums
        .add('ItemKinds', 'NKikimrConsole::TConfigItem::EKind')
        .add('MergeStrategy', 'NKikimrConsole::TConfigItem::EMergeStrategy')
        .add('ServiceKikimr', 'NKikimrServices::EServiceKikimr')
        .add('CmsLogConfigLevel', 'NKikimrCms::TCmsConfig::TLogConfig::ELevel')
        .add('CmsLogRecordType', 'NKikimrCms::TLogRecordData::EType')
        .add('PDiskStates', 'NCms::EPDiskState')
        .done(loadTypes);
}
