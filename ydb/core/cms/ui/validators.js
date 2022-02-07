var ValidatorsState = {
    validators: new Map(),
    retryInterval: 5000,
    updateInterval: 5000,
}

class Validator {
    constructor(info) {
        this.name = info.Name;
        this.description = info.Description;
        this.checkedKinds = info.CheckedItemKinds;
        if (!this.checkedKinds)
            this.checkedKinds = [];
        this.enabled = info.Enabled;
        this.toggling = false;

        var cell1 = document.createElement('td');
        cell1.textContent = this.name;

        var cell2 = document.createElement('td');
        cell2.textContent = this.description;

        var cell3 = document.createElement('td');
        for (var kind of this.checkedKinds) {
            var p = document.createElement('p');
            p.textContent = cmsEnums.get('ItemKinds', kind);
            cell3.appendChild(p);
        }
        cell3.dataset.ordervalue = 0;
        if (this.checkedKinds.length > 0)
            cell3.dataset.ordervalue = this.checkedKinds[0];

        this.enableBtn = document.createElement('input');
        this.enableBtn.type = 'radio';
        this.enableBtn.name = 'options';
        this.enableBtn.id = this.name + '-validator-enable';
        this.enableBtn.autocomplete = 'off';
        $(this.enableBtn).change(this._getOnChange(true));
        var enableLabel = document.createElement('label');
        enableLabel.setAttribute('class', 'btn btn-sm btn-outline-success');
        enableLabel.textContent = 'Enabled';
        enableLabel.appendChild(this.enableBtn);

        this.disableBtn = document.createElement('input');
        this.disableBtn.id = this.name + '-validator-disable';
        this.disableBtn.type = 'radio';
        this.disableBtn.name = 'options';
        this.disableBtn.autocomplete = 'off';
        $(this.disableBtn).change(this._getOnChange(false));
        var disableLabel = document.createElement('label');
        disableLabel.setAttribute('class', 'btn btn-sm btn-outline-danger');
        disableLabel.textContent = 'Disabled';
        disableLabel.appendChild(this.disableBtn);

        var divBtns = document.createElement('div');
        divBtns.setAttribute('class', 'btn-group btn-group-toggle');
        divBtns.dataset.toggle = 'buttons';
        divBtns.appendChild(enableLabel);
        divBtns.appendChild(disableLabel);

        this.enableCell = document.createElement('td');
        this.enableCell.setAttribute('style', 'vertical-align: middle; text-align: center;');
        this.enableCell.appendChild(divBtns);

        var line = document.createElement('tr');
        line.appendChild(cell1);
        line.appendChild(cell2);
        line.appendChild(cell3);
        line.appendChild(this.enableCell);

        document.getElementById('validators-body').appendChild(line);

        this.update(info);
    }

    update(info) {
        if (this.toggling)
            return;

        this.enabled = info.Enabled;

        if (this.enableCell.dataset.ordervalue == Number(this.enabled))
            return;

        if (this.enabled) {
            this.enableBtn.parentNode.classList.add('active');
            this.enableBtn.setAttribute('checked', '');
            this.disableBtn.removeAttribute('checked');
            this.disableBtn.parentNode.classList.remove('active');
        } else {
            this.disableBtn.parentNode.classList.add('active');
            this.disableBtn.setAttribute('checked', '');
            this.enableBtn.removeAttribute('checked');
            this.enableBtn.parentNode.classList.remove('active');
        }
        this.enableCell.dataset.ordervalue = Number(this.enabled);

        this._updateTable();
    }

    _getOnChange(enabled) {
        var _this = this;
        return function() {
            _this._toggle(enabled);
        };
    }

    _toggle(enabled) {
        if (this.toggling || this.enabled == enabled)
            return;

        this.toggling = true;
        this.enabled = enabled;

        if (enabled) {
            this.disableBtn.classList.add('disabled');
            this.disableBtn.parentNode.classList.add('disabled');
        } else {
            this.enableBtn.parentNode.classList.add('disabled');
            this.enableBtn.classList.add('disabled');
        }

        this._sendToggle();
    }

    _sendToggle() {
        var url = 'cms/api/json/toggleconfigvalidator?';
        url += 'name=' + this.name;
        url += '&enable=' + (+this.enabled);
        var _this = this;
        $.get(url)
            .done(function(data) {
                _this._onToggleFinished(data);
            })
            .fail(function() {
                _this._onToggleFailed();
            });
    }

    _onToggleFinished(data) {
        if (data['Status']['Code'] != 'SUCCESS') {
            this._onToggleFailed(data);
            return;
        }

        $('#update-validator-error').html('');

        this.enableBtn.parentNode.classList.remove('disabled');
        this.enableBtn.classList.remove('disabled');
        this.disableBtn.parentNode.classList.remove('disabled');
        this.disableBtn.classList.remove('disabled');

        this.toggling = false;
        this.enableCell.dataset.ordervalue = Number(this.enabled);

        this._updateTable();
    }

    _onToggleFailed(data) {
        console.log('_onToggleFailed');
        console.log(data);
        if (data && data['Status'] && data['Status']['Reason'])
            $('#update-validator-error').html(data['Status']['Reason']);
        else
            $('#update-validator-error').html("Cannot update validator state");

        var _this = this;
        setTimeout(function(){ _this._sendToggle() }, ValidatorsState.retryInterval);
    }

    _updateTable() {
        $("#validators-table").trigger("update", [true]);
    }
}

function onValidatorsLoaded(data) {
    if (data['Status']['Code'] != 'SUCCESS') {
        onValidatorsFailed(data);
        return;
    }

    $('#validators-error').html('');

    var validators = data.Validators;
    if (!validators)
        validators = [];

    for (var info of validators) {
        if (ValidatorsState.validators.has(info.Name)) {
            ValidatorsState.validators.get(info.Name).update(info);
        } else {
            ValidatorsState.validators.set(info.Name, new Validator(info));
        }
    }

    setTimeout(loadValidators, ValidatorsState.updateInterval);
}

function onValidatorsFailed(data) {
    if (data && data['Status'] && data['Status']['Reason'])
        $('#validators-error').html(data['Status']['Reason']);
    else
        $('#validators-error').html("Cannot get validators state");
    setTimeout(loadValidators, ValidatorsState.retryInterval);
}

function loadValidators() {
    var url = 'cms/api/json/configvalidators';
    $.get(url).done(onValidatorsLoaded).fail(onValidatorsFailed);
}

function initValidatorsTab() {
    $("#validators-table").tablesorter({
        theme: 'blue',
        sortList: [[0,0]],
        headers: {
            1: {
                sorter: false,
            },
            2: {
                sorter: 'numeric-ordervalue',
            },
            3: {
                sorter: 'numeric-ordervalue',
            },
        },
        widgets : ['zebra'],
    });

    cmsEnums
        .add('ItemKinds', 'NKikimrConsole::TConfigItem::EKind')
        .done(loadValidators);
}
