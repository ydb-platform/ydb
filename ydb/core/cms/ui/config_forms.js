'use strict';

function removeConfigItem(item) {
    var modalHTML = `
    <div class="modal fade" tabindex="-1" role="dialog">
        <div class="modal-dialog modal-dialog-centered" role="document">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">Remove config item</h5>
                    <button type="button" class="close" data-dismiss="modal" aria-label="Close">
                        <span aria-hidden="true">&times;</span>
                    </button>
                </div>
                <div class="modal-body">
                    <div class="error"></div>
                    <p>Are you sure you want to remove config item ${item.getIdString()}?</p>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-primary" data-dismiss="modal">Decline</button>
                    <button type="button" class="btn btn-danger" data-action="remove">Remove</button>
                </div>
            </div>
        </div>
    </div>
    `;
    var modal = $(modalHTML);
    modal.find('[data-action="remove"]').click(function() {
        var url = 'cms/api/console/configure';
        var cmd = {
            Actions: [
                {
                    RemoveConfigItem: {
                        ConfigItemId: item.getIdObj()
                    }
                }
            ]
        };

        $(modal).find(".error").html('');

        $.post(url, JSON.stringify(cmd))
            .done(function (data) {
                if (data['Status']['Code'] != 'SUCCESS') {
                    $(modal).find(".error").html(data['Status']['Reason']);
                    return;
                }

                item.remove();
                modal.modal('hide');
            })
            .fail(function () {
                $(modal).find(".error").html("Removal request failed");
            });
    });
    modal.modal();
}

function formatUsageScopeText(scope) {
    var result = '';
    if (scope.NodeFilter) {
        result += 'node(s) ' + scope.NodeFilter.Nodes.join(' ');
    } else if (scope.HostFilter) {
        result += 'host(s) ' + scope.HostFilter.Hosts.join(' ');
    } else if (scope.TenantAndNodeTypeFilter) {
        if (scope.TenantAndNodeTypeFilter.Tenant)
            result += 'tenant ' + scope.TenantAndNodeTypeFilter.Tenant + " ";
        if (scope.TenantAndNodeTypeFilter.NodeType)
            result += 'node ' + scope.TenantAndNodeTypeFilter.NodeType + " ";
    } else {
        result += 'domain';
    }
    return result;
}

function formatCommonItemText(item) {
    var scope = item.getUsageScope();
    var result = "";
    result += "Id: " + item.getIdString() + "\n";
    result += "UsageScope: " + formatUsageScopeText(scope) + "\n";
    result += "Order: " + item.getOrder() + "\n";
    result += "Merge strategy: " + cmsEnums.MergeStrategy.get(item.getMergeStrategy()) + "\n";
    result += "Cookie: " + item.getCookie() + "\n";
    return result;
}

function addEditButtons(div, item) {
    var buttonsHTML = `
    <div style="z-index: 1; position: absolute; top: 0; right: 0; margin: 5px;">
        <button data-action="edit" class="${item.kind.editable ? '' : 'd-none'} btn-cfg-item cfg-item-edit-icon"></button>
        <button data-action="remove" class="btn-cfg-item cfg-item-remove-icon"></button>
    </div>
    `;
    $(div).append(buttonsHTML);

    $(div).find('[data-action="edit"]').click(function() {
        item.edit();
    });
    $(div).find('[data-action="remove"]').click(function() {
        removeConfigItem(item);
    });
}

class GenericConfigView {
    constructor(item) {
        this.item = item;
        this._createUIElements();
    }

    getElement() {
        return this._div;
    }

    update() {
        $(this._pre).html(this._formatItemText());
    }

    show() {
        $(this._div).show();
    }

    hide() {
        $(this._div).hide();
    }

    remove() {
        removeElement(this._div);
        this.item.kind.onViewRemoved();
    }

    markAsPendingUpdate() {
        $(this._pre).addClass('pending-update');
    }

    unmarkAsPendingUpdate() {
        $(this._pre).removeClass('pending-update');
    }

    _createUIElements() {
        this._pre = document.createElement('pre');
        this._pre.setAttribute('class', 'cfg-item');

        this._div = document.createElement('div');
        this._div.setAttribute('style', 'position: relative;');
        this._div.id = 'config-item-' + this.item.getId();
        this._div.appendChild(this._pre);

        addEditButtons(this._div, this.item);

        this.item.kind.appendView(this._div);

        this.update();
    }

    _formatItemText() {
        var result = formatCommonItemText(this.item);
        result += "Config: " + JSON.stringify(this.item.getConfig(), null, 2);

        return result;
    }

}

class LogConfigView {
    constructor(item) {
        this.item = item;
        this._createUIElements();
    }

    getElement() {
        return this._div;
    }

    update() {
        $(this._pre).html(this._formatItemText());

        var tbody = $(this._table).find('tbody');
        tbody.empty();

        var cfg = this.item.getConfig().LogConfig;
        if (cfg === undefined)
            cfg = {};

        if (cfg.DefaultLevel !== undefined
            || cfg.DefaultSamplingLevel !== undefined
            || cfg.DefaultSamplingRate !== undefined)
            this._addLogLevelEntry("_DEFAULT_", cfg.DefaultLevel, cfg.DefaultSamplingLevel,
                                   cfg.DefaultSamplingRate);

        if (cfg.Entry)
            for (var entry of cfg.Entry)
                this._addLogLevelEntry(entry.Component, entry.Level, entry.SamplingLevel,
                                       entry.SamplingRate);

        $(this._table).trigger("update", [true]);
    }

    show() {
        $(this._div).show();
    }

    hide() {
        $(this._div).hide();
    }

    remove() {
        removeElement(this._div);
        this.item.kind.onViewRemoved();
    }

    markAsPendingUpdate() {
        $(this._pre).addClass('pending-update');
    }

    unmarkAsPendingUpdate() {
        $(this._pre).removeClass('pending-update');
    }

    _createUIElements() {
        this._pre = document.createElement('pre');
        this._pre.setAttribute('class', 'cfg-item-hdr');

        this._table = document.createElement('table');

        this._table.innerHTML =
            `<thead>
                 <tr>
                     <th>Component</th>
                     <th>Log level</th>
                     <th>Sampling level</th>
                     <th>Sampling rate</th>
                 </tr>
             </thead>
             <tbody>
             </tbody>`;

        $(this._table).tablesorter({
            theme: 'blue',
            sortList: [[0,0]],
            widgets : ['zebra'],
        });

        this._div = document.createElement('div');
        this._div.setAttribute('class', 'cfg-item');
        this._div.setAttribute('style', 'position: relative;');
        this._div.id = 'config-item-' + this.item.getId();
        this._div.appendChild(this._pre);
        this._div.appendChild(this._table);

        addEditButtons(this._div, this.item);

        this.item.kind.appendView(this._div);

        this.update();
    }

    _addLogLevelEntry(name, level, sampling, rate) {
        if (level === undefined)
            level = '';
        else
            level = this._logLevelToString(level);
        if (sampling === undefined)
            sampling = '';
        else
            sampling = this._logLevelToString(sampling);
        if (rate === undefined)
            rate = '';
        var tbody = $(this._table).find('tbody');
        var tr = `<tr><td>${name}</td><td>${level}</td><td>${sampling}</td><td>${rate}</td>`;
        $(tr).appendTo(tbody);
    }

    _formatItemText() {
        var result = formatCommonItemText(this.item);
        result += '\n';

        var cfg = this.item.getConfig().LogConfig;
        if (cfg !== undefined)
            for (var field of ['SysLog', 'Format', 'ClusterName', 'AllowDropEntries', 'UseLocalTimestamps',
                               'BackendFileName', 'SysLogService', 'SysLogToStdErr']) {
                if (cfg[field] !== undefined)
                    result += field + ': ' + cfg[field];
            }

        return result;
    }

    _logLevelToString(level) {
        if (level === 0)
            return 'EMERG';
        if (level === 1)
            return 'ALERT';
        if (level === 2)
            return 'CRIT';
        if (level === 3)
            return 'ERROR';
        if (level === 4)
            return 'WARN';
        if (level === 5)
            return 'NOTICE';
        if (level === 6)
            return 'INFO';
        if (level === 7)
            return 'DEBUG';
        if (level === 8)
            return 'TRACE';
        return 'UNKNOWN';
    }
}

class NodeBrokerConfigView {
    constructor(item) {
        this.item = item;
        this._createUIElements();
    }

    getElement() {
        return this._div;
    }

    update() {
        $(this._pre).html(this._formatItemText());
    }

    show() {
        $(this._div).show();
    }

    hide() {
        $(this._div).hide();
    }

    remove() {
        removeElement(this._div);
        this.item.kind.onViewRemoved();
    }

    markAsPendingUpdate() {
        $(this._pre).addClass('pending-update');
    }

    unmarkAsPendingUpdate() {
        $(this._pre).removeClass('pending-update');
    }

    _createUIElements() {
        this._pre = document.createElement('pre');
        this._pre.setAttribute('class', 'cfg-item');

        this._div = document.createElement('div');
        this._div.setAttribute('style', 'position: relative;');
        this._div.id = 'config-item-' + this.item.getId();
        this._div.appendChild(this._pre);

        addEditButtons(this._div, this.item);

        this.item.kind.appendView(this._div);

        this.update();
    }

    _formatItemText() {
        var result = formatCommonItemText(this.item);
        result += '\n';

        var cfg = this.item.getConfig().NodeBrokerConfig;
        if (cfg == undefined)
            cfg = {};

        if (cfg['EpochDuration'] !== undefined)
            result += 'EpochDuration: ' + cfg['EpochDuration'] / 1000000 / 60 + ' min.\n';
        if (cfg['BannedNodeIds'] !== undefined) {
            result += 'BannedNodeIds:';
            for (var ids of cfg['BannedNodeIds'])
                result += ' [' + ids.From + ', ' + ids.To + ']';
        }

        return result;
    }
}

class ImmediateControlsConfigView {
    constructor(item) {
        this.item = item;
        this._createUIElements();
    }

    getElement() {
        return this._div;
    }

    update() {
        $(this._pre).html(this._formatItemText());
    }

    show() {
        $(this._div).show();
    }

    hide() {
        $(this._div).hide();
    }

    remove() {
        removeElement(this._div);
        this.item.kind.onViewRemoved();
    }

    markAsPendingUpdate() {
        $(this._pre).addClass('pending-update');
    }

    unmarkAsPendingUpdate() {
        $(this._pre).removeClass('pending-update');
    }

    _createUIElements() {
        this._pre = document.createElement('pre');
        this._pre.setAttribute('class', 'cfg-item');

        this._div = document.createElement('div');
        this._div.setAttribute('style', 'position: relative;');
        this._div.id = 'config-item-' + this.item.getId();
        this._div.appendChild(this._pre);

        addEditButtons(this._div, this.item);

        this.item.kind.appendView(this._div);

        this.update();
    }

    _formatItemText() {
        var result = formatCommonItemText(this.item);
        result += '\n';

        var cfg;
        if (this.item.getConfig() && this.item.getConfig().ImmediateControlsConfig)
            cfg = this.item.getConfig().ImmediateControlsConfig;

        var desc = cmsProtoTypes.get('.NKikimrConfig.TImmediateControlsConfig');
        result += this._printCfg(cfg, desc, "");

        return result;
    }

    _printCfg(cfg, desc, prefix) {
        var res = '';
        for (var field of desc.field) {
            var name = prefix ? (prefix + '.' + field.name) : field.name;
            var value = cfg === undefined ? undefined : cfg[field.name];
            if (field.type_name !== undefined) {
                res += this._printCfg(value,  cmsProtoTypes.get(field.type_name), name) + "\n";
            } else {
                var opts = field.options.ControlOptions;
                res += name + ": ";
                if (value !== undefined)
                    res += value;
                else
                    res += "UNSPECIFIED (default value: " + opts.DefaultValue + ")";
                res += "\n";
            }
        }

        return res;
    }
}

function getConfigViewCreator(kindName) {
    if (kindName === 'LogConfigItem')
        return function (item) {
            return new LogConfigView(item);
        }
    else if (kindName === 'NodeBrokerConfigItem')
        return function (item) {
            return new NodeBrokerConfigView(item);
        }
    else if (kindName === 'ImmediateControlsConfigItem')
        return function (item) {
            return new ImmediateControlsConfigView(item);
        }

    return function (item){
        return new GenericConfigView(item);
    }
}

class CommonEditForm {
    constructor(kind, item) {
        var form = document.createElement('form');
        form.setAttribute('class', 'cfg-form');

        this._kind = kind;
        this._item = item;
        this._form = form;

        var title;
        if (item)
            title = `Modify config item #${item.getIdString()}`;
        else
            title = 'Create new config item';

        form.innerHTML = `
        <div class="error"></div>
        <div class="cfg-form" style="padding-left: 10px;">
            <div class="form-group row">
                <label class="col-form-label form-control-sm col-sm-auto">${title}</label>
            </div>
            <div class="form-group row">
                <label class="col-form-label form-control-sm col-sm-1">Scope:</label>
                <div class="col-sm-auto">
                    <select class="cfg-form scope-select form-control-sm">
                        <option value="">All nodes</option>
                        <option value="nodes">Filter by node ID</option>
                        <option value="hosts">Filter by host</option>
                        <option value="type">Filter by node type</option>
                        <option value="tenant">Filter by tenant</option>
                        <option value="tenant-and-type">Filter by tenant and node type</option>
                    </select>
                </div>
            </div>
            <div class="form-group row scope-nodes">
                <label class="col-form-label form-control-sm col-sm-1">Node ID(s):</label>
                <div class="col-sm-auto">
                    <input type="text" class="form-control-sm cfg-form" placeholder="1 2 3..."/>
                </div>
            </div>
            <div class="form-group row scope-hosts">
                <label class="col-form-label form-control-sm col-sm-1">Host(s):</label>
                <div class="col-sm-auto">
                    <input type="text" class="form-control-sm cfg-form" placeholder="host1 host2 host3..."/>
                </div>
            </div>
            <div class="form-group row scope-tenant">
                <label class="col-form-label form-control-sm col-sm-1">Tenant:</label>
                <div class="col-sm-auto">
                    <input type="text" class="form-control-sm cfg-form" placeholder="Enter tenant name..."/>
                </div>
            </div>
            <div class="form-group row scope-type">
                <label class="col-form-label form-control-sm col-sm-1">Node type:</label>
                <div class="col-sm-auto">
                    <input type="text" class="form-control-sm cfg-form" placeholder="Enter node type..."/>
                </div>
            </div>
        </div>
        <div class="row">
            <div class="col-sm-auto">
                <button class="form-control btn btn-primary" data-action="submit">Submit</button>
            </div>
            <div class="col-sm-auto">
                <button class="form-control btn btn-danger" data-action="discard">Discard</button>
            </div>
        </div>
        `;

        var select = $(form).find('.scope-select');
        if (item) {
            if (item.getUsageScope()) {
                var scope = item.getUsageScope();
                if (scope.NodeFilter) {
                    select.children("[value='nodes']").attr('selected', '');
                    $(form).find('.scope-nodes').find(':input').val(
                        scope.NodeFilter.Nodes.join(' '));
                } else if (scope.HostFilter) {
                    select.children("[value='hosts']").attr('selected', '');
                    $(form).find('.scope-hosts').find(':input').val(
                        scope.HostFilter.Hosts.join(' '));
                } else if (scope.TenantAndNodeTypeFilter) {
                    var filter = scope.TenantAndNodeTypeFilter;
                    if (filter.Tenant) {
                        select.children("[value='tenant']").attr('selected', '');
                        $(form).find('.scope-tenant').find(':input').val(filter.Tenant);
                    }
                    if (filter.NodeType) {
                        $(form).find('.scope-type').find(':input').val(filter.NodeType);
                        if (filter.Tenant)
                            select.children("[value='tenant-and-type']").attr('selected', '');
                        else
                            select.children("[value='type']").attr('selected', '');
                    }
                }
            }
        }

        this._onUsageScopeSelect(select[0]);

        let _this = this;
        $(form).find('[data-action="submit"]').click(function() {
            _this._submit();
        });
        $(form).find('[data-action="discard"]').click(function() {
            _this._discard();
        });
        $(form).find('.scope-select').change(function() {
            _this._onUsageScopeSelect(this);
        });
    }

    getElement() {
        return this._form;
    }

    _prepareData() {
        var data = {};
        var form = $(this._form);
        var val = form.find('.scope-select').children(":selected").val();
        var scope = {};

        if (val === 'nodes') {
            var scope = { NodeFilter: { Nodes: [] } };

            var nodes = form.find('.scope-nodes').find(':input').val();
            if (!nodes.match(/^[0-9 ]*$/))
                throw "invalid nodes list (use only 0-9 and ' ' symbols)";

            nodes = nodes.match(/\d+/g);
            if (!nodes)
                throw "empty nodes list is not allowed";

            scope.NodeFilter.Nodes = nodes.map(Number);
        } else if (val === 'hosts') {
            var scope = { HostFilter: { Hosts: [] } };

            var hosts = form.find('.scope-hosts').find(':input').val();
            scope.HostFilter.Hosts = hosts.match(/[^ ]+/g);

            if (!scope.HostFilter.Hosts)
                throw "empty hosts list is not allowed";
        } else if (val === 'tenant') {
            var tenant = this._extractTenantName();
            var scope = { TenantAndNodeTypeFilter: { Tenant: tenant } };
        } else if (val === 'type') {
            var type = this._extractNodeType();
            var scope = { TenantAndNodeTypeFilter: { NodeType: type } };
        } else if (val === 'tenant-and-type') {
            var type = this._extractNodeType();
            var tenant = this._extractTenantName();
            var scope = { TenantAndNodeTypeFilter: { Tenant: tenant, NodeType: type } };
        }

        data.UsageScope = scope;

        return data;
    }

    _extractTenantName() {
        var tenant = $(this._form).find('.scope-tenant').find(':input').val();

        if (!tenant.match(/^\/[a-zA-Z0-9-_./]+$/))
            throw "invalid tenant path";

        return tenant;
    }

    _extractNodeType() {
        var type = $(this._form).find('.scope-type').find(':input').val();

        if (!type.match(/^[a-zA-Z0-9-_.]+$/))
            throw "invalid node type";

        return type;
    }

    _makeNumericInput(label, cl, placeholder, value, help) {
        return this._makeInput(label, 'number', cl, placeholder, value, help);
    }

    _makeTextInput(label, cl, placeholder, value, help) {
        return this._makeInput(label, 'text', cl, placeholder, value, help);
    }

    _makeInput(label, type, cl, placeholder, value, help) {
        var val = value !== undefined ? value : '';
        var res = `
            <div class="col-3">
                <label class="col-form-label form-control-sm">${label}</label>
            </div>
            <div class="col-auto">
                <input type="${type}" class="form-control-sm cfg-form ${cl}" placeholder="${placeholder}" value="${val}"/>
            </div>`;
        if (help !== undefined)
            res += `<div class="help-icon" data-toggle="tooltip" title="${help}"></div>`;
        return res;
    }

    _setTimeSecValue(data, cl, path) {
        this._setTimeValue(data, 1000000, cl, path);
    }

    _setTimeMinValue(data, cl, path) {
        this._setTimeValue(data, 60 * 1000000, cl, path);
    }

    _setTimeHourValue(data, cl, path) {
        this._setTimeValue(data, 3600 * 1000000, cl, path);
    }

    _setTimeDayValue(data, cl, path) {
        this._setTimeValue(data, 24 * 3600 * 1000000, cl, path);
    }

    _setTimeValue(data, factor, cl, path) {
        var val = $(this._form).find('.' + cl).val();
        if (val === '')
            return;

        val = +val;
        if (val < 0)
            throw `negative duration value (${val}) is not allowed`;
        val = val * factor;

        this._setValue(data, val, path);
    }

    _setNumValue(data, cl, path) {
        var val = $(this._form).find('.' + cl).val();
        if (val === '')
            return;

        val = +val;
        if (val < 0)
            throw `negative value (${val}) is not allowed`;

        this._setValue(data, val, path);
    }

    _setBoolSelectValue(data, cl, path) {
        var val = +$(this._form).find('.' + cl).children(":selected").val();
        if (val === -1)
            return;
        this._setValue(data, val === 1, path);
    }

    _setValue(data, val, path) {
        if (typeof path === 'string') {
            data[path] = val;
            return;
        }

        console.assert(path.length > 0);
        for (var i = 0; i < path.length - 1; ++i) {
            if (data[path[i]] === undefined)
                data[path[i]] = {};
            data = data[path[i]];
        }
        data[path[path.length - 1]] = val;
    }

    _parseInt(val) {
        if (val === undefined)
            return val;

        val.trim();
        if (!val.match(/^\d+$/))
            return undefined;

        return +val;
    }

    _submit() {
        try {
            $(this._form).children(".error").html('');
            $("#configs-success").html('');

            var data = this._prepareData();

            var cmd = { Actions: [] };
            if (this._item) {
                data.Id = this._item.getIdObj();
                data.Kind = this._item.getKindNo();
                data.Order = this._item.getOrder();
                data.MergeStrategy = this._item.getMergeStrategy();
                data.Cookie = this._item.getCookie();

                var modify = { ModifyConfigItem: { ConfigItem: data } };
                cmd.Actions.push(modify);

                this._generation = this._item.getGeneration();
            } else {
                var add = { AddConfigItem: { ConfigItem: data } };
                cmd.Actions.push(add);
            }

            this._data = data;

            $(this._form).find('[data-action="submit"]').attr("disabled", "");
            $(this._form).find('[data-action="discard"]').attr("disabled", "");

            var url = 'cms/api/console/configure';
            let _this = this;
            $.post(url, JSON.stringify(cmd))
                .done(function (data) {
                    _this._onConfigureFinished(data);
                })
                .fail(function () {
                    _this._onConfigureFailed();
                });
        } catch (err) {
            $(this._form).children(".error").html(err);
        }
    }

    _discard() {
        if (this._item)
            this._item.show();
        removeElement(this._form);
    }

    _onConfigureFinished(data) {
        if (data['Status']['Code'] != 'SUCCESS') {
            this._onConfigureFailed(data);
            return;
        }

        $("#configs-success").html('Successfully updated config');
        if (this._item) {
            if (this._item.getGeneration() === this._generation) {
                this._data.Id = this._item.getIdObj();
                this._item.update(this._data);
                this._item.markAsPendingUpdate();
            }
            this._item.show();
        } else {
            if (data.AddedItemIds && data.AddedItemIds.length == 1) {
                this._data.Id = {
                    Id: data.AddedItemIds[0],
                    Generation: 0,
                };
                this._data.Kind = this._kind.no;
                var item = new ConfigItem(this._data);
                item.markAsPendingUpdate();
            }
        }
        removeElement(this._form);
    }

    _onConfigureFailed(data) {
        if (data && data['Status'] && data['Status']['Reason'])
            $(this._form).find('.error').html(data['Status']['Reason']);
        else
            $(this._form).find('.error').html("Cannot update config");

        $(this._form).find('[data-action="submit"]').removeAttr("disabled");
        $(this._form).find('[data-action="discard"]').removeAttr("disabled");
    }

    _onUsageScopeSelect(select) {
        var form = $(this._form);
        var value = $(select).children(":selected").val();

        form.find('.scope-nodes').hide();
        form.find('.scope-hosts').hide();
        form.find('.scope-tenant').hide();
        form.find('.scope-type').hide();

        if (value === 'nodes')
            form.find('.scope-nodes').show();
        else if (value === 'hosts')
            form.find('.scope-hosts').show();
        else if (value === 'tenant')
            form.find('.scope-tenant').show();
        else if (value === 'type')
            form.find('.scope-type').show();
        else if (value === 'tenant-and-type') {
            form.find('.scope-tenant').show();
            form.find('.scope-type').show();
        }
    }
}

class LogConfigEditForm extends CommonEditForm {
    constructor(kind, item) {
        super(kind, item);
        var form = $(this._form);

        var syslog;
        var defLevel;
        var defSampling;
        var defRate;
        var cfg;

        if (item && item.getConfig() && item.getConfig().LogConfig) {
            cfg = item.getConfig().LogConfig;

            syslog = cfg.SysLog;
            defLevel = cfg.DefaultLevel;
            defSampling = cfg.DefaultSamplingLevel;
            defRate = cfg.DefaultSamplingRate;
        }

        var configFormHTML = `
        <div style="padding-left: 10px;">
            <div class="form-group row">
                <label class="col-form-label form-control-sm col-sm-1">Syslog:</label>
                <div class="col-sm-auto">
                    <select class="cfg-form syslog form-control-sm">
                        <option value="-1" ${syslog === undefined ? "selected" : ""}>Default</option>
                        <option value="1" ${syslog !== undefined && syslog == true ? "selected" : ""}>true</option>
                        <option value="0" ${syslog !== undefined && syslog == false ? "selected" : ""}>false</option>
                    </select>
                </div>
            </div>
            <div class="form-group def-log-settings">
                <div class="row">
                    <label class="col-form-label form-control-sm col-sm-auto">Default log settings:</label>
                </div>
                <div class="row">
                    <label class="col-form-label form-control-sm col-sm-auto">Level: </label>
                    <div class="col-sm-auto">
                        ${this._getLevelSelectHTML("def-log-level", defLevel)}
                    </div>
                    <label class="col-form-label form-control-sm col-sm-auto">Sampling: </label>
                    <div class="col-sm-auto">
                        ${this._getLevelSelectHTML("def-log-sampling", defSampling)}
                    </div>
                    <label class="col-form-label form-control-sm col-sm-auto">Sampling rate: </label>
                    <div class="col-sm-auto">
                        <input type="number" class="form-control-sm cfg-form def-log-rate" placeholder="Enter sampling rate..."/>
                    </div>
                </div>
            </div>
            <div class="form-group">
                <div class="row">
                    <label class="col-form-label form-control-sm col-sm-auto">Component log settings:</label>
                </div>
                <div class="row" style="padding: 0 .5rem;">
                    <table class="tablesorter" style="margin-right: 15px;">
                        <thead>
                            <tr>
                                <th>Component</th>
                                <th>Log level</th>
                                <th>Sampling level</th>
                                <th>Sampling rate</th>
                            </tr>
                        </thead>
                        <tbody>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        `;

        $(configFormHTML).insertBefore(form.children("div.row:last-child"));

        if (defRate)
            form.find(".def-log-rate").val(defRate);

        var levels = new Map();
        var samplings = new Map();
        var rates = new Map();
        if (cfg && cfg.Entry) {
            for (var entry of cfg.Entry) {
                if (entry.Level !== undefined)
                    levels.set(entry.Component, entry.Level);
                if (entry.SamplingLevel !== undefined)
                    samplings.set(entry.Component, entry.SamplingLevel);
                if (entry.SamplingRate !== undefined)
                    rates.set(entry.Component, entry.SamplingRate);
            }
        }

        var tbody = form.find("tbody");
        for (var val of cmsEnums['ServiceKikimr'].values()) {
            var rowHTML = `
            <tr class="log-item-settings">
                <td class="log-item-name">${val}</td>
                <td>${this._getLevelSelectHTML("log-item-level", levels.get(val))}</td>
                <td>${this._getLevelSelectHTML("log-item-sampling", samplings.get(val))}</td>
                <td>
                    <input type="number" class="form-control-sm cfg-form log-item-rate" placeholder="Enter sampling rate..." value="${rates.has(val) ? rates.get(val) : ""}"/>
                </td>
            </tr>
            `;
            tbody.append(rowHTML);
        }

        form.find("table").tablesorter({
            theme: 'blue',
            sortList: [[0,0]],
            headers: {
                1: {
                    sorter: 'numeric-ordervalue',
                },
                2: {
                    sorter: 'numeric-ordervalue',
                },
                3: {
                    sorter: false,
                },
            },
            widgets : ['zebra', 'filter'],
        });
    }

    _getLevelSelectHTML(classes, val) {
        if (val === undefined)
            val = -1;
        return `
        <select class="sampling-select form-control-sm ${classes}">
            <option value="-1" ${val == -1 ? "selected" : ""}>Default</option>
            <option value="0" ${val == 0 ? "selected" : ""}>EMERG</option>
            <option value="1" ${val == 1 ? "selected" : ""}>ALERT</option>
            <option value="2" ${val == 2 ? "selected" : ""}>CRIT</option>
            <option value="3" ${val == 3 ? "selected" : ""}>ERROR</option>
            <option value="4" ${val == 4 ? "selected" : ""}>WARN</option>
            <option value="5" ${val == 5 ? "selected" : ""}>NOTICE</option>
            <option value="6" ${val == 6 ? "selected" : ""}>INFO</option>
            <option value="7" ${val == 7 ? "selected" : ""}>DEBUG</option>
            <option value="8" ${val == 8 ? "selected" : ""}>TRACE</option>
        </select>
        `;
    }

    _prepareData() {
        var data = super._prepareData();

        var syslog = +$(this._form).find(".syslog").children(":selected").val();
        var defSettings = $(this._form).find("div.def-log-settings");
        var defLevel = +defSettings.find(".def-log-level").children(":selected").val();
        var defSampling = +defSettings.find(".def-log-sampling").children(":selected").val();
        var defRate = defSettings.find(".def-log-rate").val();

        var config = { LogConfig: {} };
        if (syslog != -1)
            config.LogConfig.SysLog = syslog ? true : false;
        if (defLevel != -1)
            config.LogConfig.DefaultLevel = defLevel;
        if (defSampling != -1)
            config.LogConfig.DefaultSamplingLevel = defSampling;
        if (defRate)
            config.LogConfig.DefaultSamplingRate = +defRate;

        $(this._form).find("tr.log-item-settings").each(function() {
            var name = $(this).find(".log-item-name").text();
            var level = +$(this).find(".log-item-level").val();
            var sampling = +$(this).find(".log-item-sampling").val();
            var rate = $(this).find(".log-item-rate").val();

            if (level != -1 || sampling != -1 || rate) {
                var entry = {};
                entry.Component = name;
                if (level != -1)
                    entry.Level = level;
                if (sampling != -1)
                    entry.SamplingLevel = sampling;
                if (rate)
                    entry.SamplingRate = +rate;

                if (!config.LogConfig.Entry)
                    config.LogConfig.Entry = [];

                config.LogConfig.Entry.push(entry);
            }
        });

        data.Config = config;

        return data;
    }
}

class CmsConfigEditForm extends CommonEditForm {
    constructor(kind, item) {
        super(kind, item);
        var form = $(this._form);

        var infoCollectionTimeout;
        var defRetryTime;
        var defPermissionDuration;
        var clusterNodesLimit;
        var clusterNodesRatioLimit;
        var tenantNodesLimit;
        var tenantNodesRatioLimit;
        var monEnabled;
        var monUpdateInterval;
        var monIgnoredGap;
        var monBrokenTimeout;
        var monBrokenPrepTimeout;
        var monFaultyPrepTimeout;
        var logTTL;

        // sentinel
        var sentinelEnabled;
        var sentinelDryRun;
        var sentinelUpdateConfigInterval;
        var sentinelRetryUpdateConfig;
        var sentinelUpdateStateInterval;
        var sentinelUpdateStateTimeout;
        var sentinelChangeStatusRetries;
        var sentinelRetryChangeStatus;
        var sentinelDefaultStateLimit;
        var sentinelStateLimits = new Map();
        var sentinelDefaultStateLimits = new Map();

        if (item && item.getConfig() && item.getConfig().CmsConfig) {
            var cfg = item.getConfig().CmsConfig;

            if (cfg.InfoCollectionTimeout)
                infoCollectionTimeout = cfg.InfoCollectionTimeout / 1000000;
            if (cfg.DefaultRetryTime)
                defRetryTime = cfg.DefaultRetryTime / 1000000;
            if (cfg.DefaultPermissionDuration)
                defPermissionDuration = cfg.DefaultPermissionDuration / 1000000;
            if (cfg.ClusterLimits) {
                clusterNodesLimit = cfg.ClusterLimits.DisabledNodesLimit;
                clusterNodesRatioLimit = cfg.ClusterLimits.DisabledNodesRatioLimit;
            }
            if (cfg.TenantLimits) {
                tenantNodesLimit = cfg.TenantLimits.DisabledNodesLimit;
                tenantNodesRatioLimit = cfg.TenantLimits.DisabledNodesRatioLimit;
            }

            if (cfg.LogConfig && cfg.LogConfig.TTL)
                logTTL = cfg.LogConfig.TTL / 1000000 / 3600 / 24;

            // sentinel
            if (cfg.SentinelConfig) {
                sentinelEnabled = cfg.SentinelConfig.Enable;
                sentinelDryRun = cfg.SentinelConfig.DryRun;
                if (cfg.SentinelConfig.UpdateConfigInterval)
                    sentinelUpdateConfigInterval = cfg.SentinelConfig.UpdateConfigInterval / 1000000;
                if (cfg.SentinelConfig.RetryUpdateConfig)
                    sentinelRetryUpdateConfig = cfg.SentinelConfig.RetryUpdateConfig / 1000000;
                if (cfg.SentinelConfig.UpdateStateInterval)
                    sentinelUpdateStateInterval = cfg.SentinelConfig.UpdateStateInterval / 1000000;
                if (cfg.SentinelConfig.UpdateStateTimeout)
                    sentinelUpdateStateTimeout = cfg.SentinelConfig.UpdateStateTimeout / 1000000;
                if (cfg.SentinelConfig.ChangeStatusRetries)
                    sentinelChangeStatusRetries = cfg.SentinelConfig.ChangeStatusRetries;
                if (cfg.SentinelConfig.RetryChangeStatus)
                    sentinelRetryChangeStatus = cfg.SentinelConfig.RetryChangeStatus / 1000000;
                if (cfg.SentinelConfig.DefaultStateLimit)
                    sentinelDefaultStateLimit = cfg.SentinelConfig.DefaultStateLimit;
                if (cfg.SentinelConfig.StateLimits) {
                    for (var c of cfg.SentinelConfig.StateLimits) {
                        sentinelStateLimits.set(c.State, c.Limit);
                    }
                }
            }
        }

        for (var e of cmsEnums['PDiskStates'].keys()) {
            var name = cmsEnums.get('PDiskStates', e);
            if (name.match(/Error$/)) {
                sentinelDefaultStateLimits.set(e, 60);
            } else if (e == 255  || name.match(/^Reserved/)) {
                sentinelDefaultStateLimits.set(e, 0);
            } else if (e > 250) {
                sentinelDefaultStateLimits.set(e, 60);
            }
        }

        var configFormHTML = `
        <div style="padding-left: 10px;">
            <div class="form-group">
                <div class="row">
                    <label class="col-form-label form-control-sm col-sm-auto">Permission parameters</label>
                </div>
                <div class="row">
                    ${this._makeNumericInput("Info collection timeout (sec.)", "info-collection-timeout",
                                             "Default is 15 sec.", infoCollectionTimeout,
                                             "Maximum allowed time for cluster state collection")}
                    <div class="w-100 vs-4px"></div>
                    ${this._makeNumericInput("Default retry interval (sec.)", "def-retry-time",
                                             "Default is 300 sec.", defRetryTime,
                                             "If requested permission temporarily cannot be given and appropriate retry interval is unknown then default retry interval is used.")}
                    <div class="w-100 vs-4px"></div>
                    ${this._makeNumericInput("Default permission duration (sec.)", "def-permission-duration",
                                             "Default is 300 sec.", defPermissionDuration,
                                             "This duration is used for permission request with no duration specified")}
                </div>
            </div>
            <div class="form-group">
                <div class="row">
                    ${this._makeNumericInput("Cluster disabled node limit", "cluster-node-limit",
                                             "Unlimited by default", clusterNodesLimit,
                                             "Maximum number of nodes allowed to be down at the same time")}
                    <div class="w-100 vs-4px"></div>
                    ${this._makeNumericInput("Cluster disabled node ratio limit (%)", "cluster-node-ratio-limit",
                                             "Defalut is 10%", clusterNodesRatioLimit,
                                             "Maximum number of nodes allowed to be down at the same time specified in % of total cluster nodes count")}
                </div>
            </div>
            <div class="form-group">
                <div class="row">
                    ${this._makeNumericInput("Tenant disabled node limit", "tenant-node-limit",
                                             "Unlimited by default", tenantNodesLimit,
                                             "Maximum number of nodes allowed to be down at the same time for each tenant.")}
                    <div class="w-100 vs-4px"></div>
                    ${this._makeNumericInput("Tenant disabled node ratio limit (%)", "tenant-node-ratio-limit",
                                             "Defalut is 10%", tenantNodesRatioLimit,
                                             "Maximum number of tenant nodes allowed to be down at the same time specified in % of total tenant nodes count")}
                </div>
            </div>
            <div class="form-group">
                <div class="row">
                    <label class="col-form-label form-control-sm col-sm-auto">Sentinel (self heal) parameters</label>
                </div>
                <div class="row">
                    <div class="col-3">
                        <label class="col-form-label form-control-sm">Status</label>
                    </div>
                    <div class="col-auto">
                        <select class="form-control-sm sentinel-enabled">
                            <option value="0" ${sentinelEnabled ? "" : "selected"}>Disabled</option>
                            <option value="1" ${sentinelEnabled ? "selected" : ""}>Enabled</option>
                        </select>
                    </div>
                </div>
                <div class="w-100 vs-4px"></div>
                <div class="row">
                    <div class="col-3">
                        <label class="col-form-label form-control-sm">Dry run</label>
                    </div>
                    <div class="col-auto">
                        <select class="form-control-sm sentinel-dry-run">
                            <option value="0" ${sentinelDryRun ? "" : "selected"}>Disable</option>
                            <option value="1" ${sentinelDryRun ? "selected" : ""}>Enable</option>
                        </select>
                    </div>
                </div>
            </div>
            <div class="form-group">
                <div class="row">
                    ${this._makeNumericInput("Config update interval (sec.)", "sentinel-update-config-interval",
                                             "Default is 3600 sec.", sentinelUpdateConfigInterval,
                                             "Interval used to update available (known by BSC) PDisks list")}
                    <div class="w-100 vs-4px"></div>
                    ${this._makeNumericInput("Retry interval (sec.)", "sentinel-retry-update-config",
                                             "Default is 60 sec.", sentinelRetryUpdateConfig,
                                             "Used if BSC is temporarily unavailable.")}
                </div>
            </div>
            <div class="form-group">
                <div class="row">
                    ${this._makeNumericInput("State update interval (sec.)", "sentinel-update-state-interval",
                                             "Default is 60 sec.", sentinelUpdateStateInterval,
                                             "Interval used to update states of PDisks")}
                    <div class="w-100 vs-4px"></div>
                    ${this._makeNumericInput("Timeout (sec.)", "sentinel-update-state-timeout",
                                             "Default is 45 sec.", sentinelUpdateStateTimeout,
                                             "Maximum allowed time for PDisks states collection")}
                </div>
            </div>
            <div class="form-group">
                <div class="row">
                    ${this._makeNumericInput("Change status retries", "sentinel-change-status-retries",
                                             "Default is 5", sentinelChangeStatusRetries,
                                             "Number of retries if status of PDisk cannot be changed due to BSC unavailability")}
                    <div class="w-100 vs-4px"></div>
                    ${this._makeNumericInput("Change status retry interval (sec.)", "sentinel-change-status-retry-interval",
                                             "Default is 10 sec.", sentinelRetryChangeStatus,
                                             "Used if BSC is temporarily unavailable.")}
                </div>
            </div>
            <div class="form-group">
                <div class="row">
                    ${this._makeNumericInput("Default state limit", "sentinel-default-state-limit",
                                             "Default is 60", sentinelDefaultStateLimit,
                                             "Number of 'state update' cycles before changing status")}
                    ${this._makeSentinelStateLimits(sentinelStateLimits, sentinelDefaultStateLimits)}
                </div>
            </div>
            <div class="form-group">
                <div class="row">
                    <label class="col-form-label form-control-sm col-sm-auto">Persistent log settings</label>
                </div>
                <div class="row">
                    ${this._makeLogLevelSelects()}
                    <div class="w-100 vs-4px"></div>
                    ${this._makeNumericInput("Time to live (days)", "log-ttl",
                                             "Default is 14 days", logTTL)}
                </div>
            </div>
        </div>
        `;

        $(configFormHTML).insertBefore(form.children("div.row:last-child"));
        form.find('[data-toggle="tooltip"]').tooltip();
    }

    _makeLogLevelSelects() {
        var item = this._item;
        var defLogLevel = 0;
        var componentLevels = new Map();

        if (item && item.getConfig() && item.getConfig().CmsConfig && item.getConfig().CmsConfig.LogConfig) {
            var cfg = item.getConfig().CmsConfig.LogConfig;

            if (cfg.DefaultLevel)
                defLogLevel = cmsEnums.parse('CmsLogConfigLevel', cfg.DefaultLevel);
            if (cfg.ComponentLevels) {
                for (var c of cfg.ComponentLevels) {
                    if (c.RecordType === undefined)
                        continue;
                    var level = c.Level === undefined ? 0 : cmsEnums.parse('CmsLogConfigLevel', c.Level);
                    componentLevels.set(c.RecordType, level);
                }
            }
        }

        var result = `
            <div class="col-3">
                <label class="col-form-label form-control-sm">Default log level</label>
            </div>
            <div class="col-auto">
                ${this._makeLogLevelSelect(-1, false, defLogLevel)}
            </div>
        `;

        for (var e of cmsEnums['CmsLogRecordType'].keys()) {
            if (e === 0)
                continue;

            var name = cmsEnums.get('CmsLogRecordType', e);
            result += `
            <div class="w-100 vs-4px"></div>
            <div class="col-3">
                <label class="col-form-label form-control-sm">${name} log level</label>
            </div>
            <div class="col-auto">
                ${this._makeLogLevelSelect(e, true, componentLevels.get(e))}
            </div>
            `;
        }

        return result;
    }

    _makeLogLevelSelect(type, allowDefault, val) {
        if (val === undefined) {
            console.assert(allowDefault);
            val = -1;
        }

        var res = `<select class="form-control-sm log-level" data-recordtype="${type}">`;
        if (allowDefault)
            res += `<option value="-1" ${val == -1 ? "selected" : ""}>Default</option>`;
        for (var e of cmsEnums['CmsLogConfigLevel'].keys()) {
            var name = cmsEnums.get('CmsLogConfigLevel', e);
            res += `<option value="${e}" ${val == e ? "selected" : ""}>${name}</option>`;
        }
        res += '</select>';

        return res;
    }

    _makeSentinelStateLimits(stateLimits, defaultStateLimits) {
        var res = '';

        for (var e of cmsEnums['PDiskStates'].keys()) {
            var name = cmsEnums.get('PDiskStates', e);
            var placeholder = "Use default if empty";
            if (defaultStateLimits.has(e)) {
                placeholder = "Default is " + defaultStateLimits.get(e) + " cycles";
            }

            var val = stateLimits.has(e) ? stateLimits.get(e) : '';
            res += `
                <div class="w-100 vs-4px"></div>
                <div class="col-3">
                    <label class="col-form-label form-control-sm"><i>${name}</i></label>
                </div>
                <div class="col-auto">
                    <input type="number" class="form-control-sm cfg-form sentinel-state-limit" placeholder="${placeholder}" value="${val}" data-recordtype="${e}"/>
                </div>
                <div class="help-icon" data-toggle="tooltip" title="Number of 'state update' cycles before changing status"></div>
            `;
        }

        return res;
    }

    _prepareData() {
        var data = super._prepareData();
        var cfg = {};

        this._setTimeSecValue(cfg, 'info-collection-timeout', 'InfoCollectionTimeout');
        this._setTimeSecValue(cfg, 'def-retry-time', 'DefaultRetryTime');
        this._setTimeSecValue(cfg, 'def-permission-duration', 'DefaultPermissionDuration');
        this._setNumValue(cfg, 'cluster-node-limit', ['ClusterLimits', 'DisabledNodesLimit']);
        this._setNumValue(cfg, 'cluster-node-ratio-limit', ['ClusterLimits', 'DisabledNodesRatioLimit']);
        this._setNumValue(cfg, 'tenant-node-limit', ['TenantLimits', 'DisabledNodesLimit']);
        this._setNumValue(cfg, 'tenant-node-ratio-limit', ['TenantLimits', 'DisabledNodesRatioLimit']);

        cfg.LogConfig = {};
        this._setTimeDayValue(cfg, 'log-ttl', ['LogConfig', 'TTL']);
        $(this._form).find('select.log-level').each(function() {
            var recType = +this.dataset.recordtype;
            var val = +$(this).children(":selected").val();

            if (recType === -1) {
                console.assert(val !== -1);
                cfg.LogConfig.DefaultLevel = cmsEnums.get('CmsLogConfigLevel', val);
            } else if (val !== -1) {
                if (cfg.LogConfig.ComponentLevels === undefined)
                    cfg.LogConfig.ComponentLevels = [];
                var comp = {
                    RecordType: recType,
                    Level: cmsEnums.get('CmsLogConfigLevel', val),
                };
                cfg.LogConfig.ComponentLevels.push(comp);
            }
        });

        // sentinel
        this._setBoolSelectValue(cfg, 'sentinel-enabled', ['SentinelConfig', 'Enable']);
        this._setBoolSelectValue(cfg, 'sentinel-dry-run', ['SentinelConfig', 'DryRun']);
        this._setTimeSecValue(cfg, 'sentinel-update-config-interval', ['SentinelConfig', 'UpdateConfigInterval']);
        this._setTimeSecValue(cfg, 'sentinel-retry-update-config', ['SentinelConfig', 'RetryUpdateConfig']);
        this._setTimeSecValue(cfg, 'sentinel-update-state-interval', ['SentinelConfig', 'UpdateStateInterval']);
        this._setTimeSecValue(cfg, 'sentinel-update-state-timeout', ['SentinelConfig', 'UpdateStateTimeout']);
        this._setNumValue(cfg, 'sentinel-change-status-retries', ['SentinelConfig', 'ChangeStatusRetries']);
        this._setTimeSecValue(cfg, 'sentinel-change-status-retry-interval', ['SentinelConfig', 'RetryChangeStatus']);
        this._setNumValue(cfg, 'sentinel-default-state-limit', ['SentinelConfig', 'DefaultStateLimit']);
        $(this._form).find('input.sentinel-state-limit').each(function() {
            var val = $(this).val();
            if (val) {
                if (cfg.SentinelConfig.StateLimits === undefined)
                    cfg.SentinelConfig.StateLimits = [];
                cfg.SentinelConfig.StateLimits.push({
                    State: +this.dataset.recordtype,
                    Limit: +val,
                });
            }
        });

        data.Config = { CmsConfig: cfg };

        return data;
    }
}

class NodeBrokerConfigEditForm extends CommonEditForm {
    constructor(kind, item) {
        super(kind, item);
        var form = $(this._form);

        var epochDuration;
        var bannedIds;

        if (item && item.getConfig() && item.getConfig().NodeBrokerConfig) {
            var cfg = item.getConfig().NodeBrokerConfig;

            if (cfg.EpochDuration)
                epochDuration = cfg.EpochDuration / 1000000 / 60;
            if (cfg.BannedNodeIds) {
                bannedIds = '';
                for (var ids of cfg.BannedNodeIds) {
                    if (bannedIds !== '')
                        bannedIds += ',';
                    bannedIds += ids.From;
                    if (ids.To != ids.From)
                        bannedIds += '-' + ids.To;
                }
            }
        }

        var configFormHTML = `
        <div style="padding-left: 10px;">
            <div class="form-group">
                <div class="row">
                    ${this._makeNumericInput("Epoch duration (min.)", "epoch-duration",
                                             "Default is 60 min.", epochDuration)}
                    <div class="w-100 vs-4px"></div>
                    ${this._makeTextInput("Banned node IDs", "banned-ids",
                                          "E.g. 1000-2999,4000,5000,6000-6500", bannedIds)}
                </div>
            </div>
        </div>
        `;

        $(configFormHTML).insertBefore(form.children("div.row:last-child"));
        form.find('[data-toggle="tooltip"]').tooltip();
    }

    _prepareData() {
        var data = super._prepareData();
        var cfg = {};

        this._setTimeMinValue(cfg, 'epoch-duration', 'EpochDuration');
        this._parseNodeIds(cfg);

        data.Config = { NodeBrokerConfig: cfg };

        return data;
    }

    _parseNodeIds(cfg) {
        var val = $(this._form).find('.banned-ids').val().trim();
        if (val === '')
            return;

        cfg.BannedNodeIds = [];
        var ranges = val.split(',');
        for (var range of ranges) {
            if (range.search('-') != -1) {
                var vals = range.split('-');
                if (vals.length != 2)
                    throw 'incorrect banned IDs range: ' + range;

                var from = this._parseInt(vals[0]);
                var to = this._parseInt(vals[1]);
                if (from === undefined || to === undefined)
                    throw 'incorrect banned IDs range: ' + range;

                cfg.BannedNodeIds.push({From: from, To: to});
            } else {
                var val = this._parseInt(range);
                if (val === undefined)
                    throw 'incorrect banned IDs range: ' + range;

                cfg.BannedNodeIds.push({From: val, To: val});
            }
        }
    }
}

class SharedCacheConfigEditForm extends CommonEditForm {
    constructor(kind, item) {
        super(kind, item);
        var form = $(this._form);

        var memoryLimit;
        var scanQueueLimit;
        var asyncQueueLimit;

        if (item && item.getConfig() && item.getConfig().SharedCacheConfig) {
            var cfg = item.getConfig().SharedCacheConfig;
            if (cfg.MemoryLimit) {
                memoryLimit = cfg.MemoryLimit / 1048576;
            }
            if (cfg.ScanQueueInFlyLimit) {
                scanQueueLimit = cfg.ScanQueueInFlyLimit / 1048576;
            }
            if (cfg.AsyncQueueInFlyLimit) {
                asyncQueueLimit = cfg.AsyncQueueInFlyLimit / 1048576;
            }
        }

        var configFormHTML = `
        <div style="padding-left: 10px;">
            <div class="form-group">
                <div class="row">
                    ${this._makeNumericInput("Memory limit (MB)", "memory-limit",
                                             "Default is 512MB", memoryLimit)}
                    <div class="w-100 vs-4px"></div>
                    ${this._makeNumericInput("Scan queue in-fly (MB)", "scan-queue-infly",
                                             "Default is 512MB", scanQueueLimit)}
                    <div class="w-100 vs-4px"></div>
                    ${this._makeNumericInput("Async queue in-fly (MB)", "async-queue-infly",
                                             "Default is 512MB", asyncQueueLimit)}
                </div>
            </div>
        </div>
        `;

        $(configFormHTML).insertBefore(form.children("div.row:last-child"));
        form.find('[data-toggle="tooltip"]').tooltip();
    }

    _prepareData() {
        var data = super._prepareData();
        var cfg = {};

        var form = $(this._form);
        var memoryLimit = this._parseInt(form.find('.memory-limit').val());
        var scanQueueLimit = this._parseInt(form.find('.scan-queue-infly').val());
        var asyncQueueLimit = this._parseInt(form.find('.async-queue-infly').val());

        var cfg = {};
        if (memoryLimit) {
            cfg.MemoryLimit = memoryLimit * 1048576;
        }
        if (scanQueueLimit) {
            cfg.ScanQueueInFlyLimit = scanQueueLimit * 1048576;
        }
        if (asyncQueueLimit) {
            cfg.AsyncQueueInFlyLimit = asyncQueueLimit * 1048576;
        }

        data.Config = { SharedCacheConfig: cfg };

        return data;
    }
}

class ImmediateControlsConfigEditForm extends CommonEditForm {
    constructor(kind, item) {
        super(kind, item);
        var form = $(this._form);
        var cfg;

        if (item && item.getConfig() && item.getConfig().ImmediateControlsConfig)
            cfg = item.getConfig().ImmediateControlsConfig;

        var configFormHTML = `
        <div style="padding-left: 10px;">
            <div class="form-group">
                ${this._makeControlInputs(cfg)}
            </div>
        </div>
        `;

        $(configFormHTML).insertBefore(form.children("div.row:last-child"));
        form.find('[data-toggle="tooltip"]').tooltip({html: true});
    }

    _makeControlInputs(item) {
        var desc = cmsProtoTypes.get('.NKikimrConfig.TImmediateControlsConfig');
        return this._makeControlInputsForMessage(item, desc, undefined);
    }

    _makeControlInputsForMessage(cfg, desc, cl) {
        var res = '';
        for (var field of desc.field) {
            var fieldCl = cl ? (cl + '-' + field.name) : field.name;
            var value = cfg === undefined ? undefined : cfg[field.name];
            if (field.type_name !== undefined) {
                res += `<div class="row vs-4px">
                           <label class="col-form-label form-control-sm">${field.name}:</label>
                        </div>`;

                res += '<div style="padding-left: 10px;">'
                    + this._makeControlInputsForMessage(value,
                                                        cmsProtoTypes.get(field.type_name),
                                                        fieldCl)
                    + '</div>';
            } else {
                var opts = field.options.ControlOptions;
                var help = `${opts.Description}<br/>
                            Allowed range: [${opts.MinValue}-${opts.MaxValue}]<br/>
                            Default value: ${opts.DefaultValue}<br/>`;
                res += '<div class="row">'
                    + this._makeNumericInput(field.name, fieldCl,
                                             "Default is " + opts.DefaultValue,
                                             value, help)
                    + '<div class="w-100 vs-4px"></div>'
                    + '</div>';
            }
        }
        return res;
    }

    _prepareData() {
        var data = super._prepareData();

        var cfg = {};
        this._parseControlInputs(cfg);

        data.Config = { ImmediateControlsConfig: cfg };

        return data;
    }

    _parseControlInputs(cfg) {
        var desc = cmsProtoTypes.get('.NKikimrConfig.TImmediateControlsConfig');
        this._parseControlInputsForMessage(cfg, desc, undefined);
    }

    _parseControlInputsForMessage(cfg, desc, cl) {
        var res = '';
        for (var field of desc.field) {
            var fieldCl = cl ? (cl + '-' + field.name) : field.name;
            if (field.type_name !== undefined) {
                cfg[field.name] = {};
                this._parseControlInputsForMessage(cfg[field.name],
                                                   cmsProtoTypes.get(field.type_name),
                                                   fieldCl);
            } else {
                var value = this._parseInt($(this._form).find("." + fieldCl).val());
                if (value !== undefined)
                    cfg[field.name] = value;
            }
        }
    }
}

function getConfigEditFormCreator(kindName) {
    if (kindName === 'LogConfigItem') {
        return function (kind, item) {
            return new LogConfigEditForm(kind, item);
        }
    } else if (kindName === 'CmsConfigItem') {
        return function (kind, item) {
            return new CmsConfigEditForm(kind, item);
        }
    } else if (kindName === 'NodeBrokerConfigItem') {
        return function (kind, item) {
            return new NodeBrokerConfigEditForm(kind, item);
        }
    } else if (kindName == 'SharedCacheConfigItem') {
        return function (kind, item) {
            return new SharedCacheConfigEditForm(kind, item);
        }
    } else if (kindName == 'ImmediateControlsConfigItem') {
        return function (kind, item) {
            return new ImmediateControlsConfigEditForm(kind, item);
        }
    }

    return function (item) {
        return undefined;
    }
}
