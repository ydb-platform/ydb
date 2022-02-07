'use strict';

class Enums {

    constructor(errorElemId) {
        this._requested = new Map();
        this._requests = 0;
        this._retryInterval = 5000;
        this._errorElemId = errorElemId;
        this._onDone = [];
    }

    add(enumName, enumClass) {
        if (this._requested.has(enumName)) {
            console.assert(this._requested.get(enumName) === enumClass);
        } else {
            this._requested.set(enumName, enumClass);
            ++this._requests;
            this._load(enumName, enumClass);
        }

        return this;
    }

    done(cb) {
        this._onDone.push(cb);
        return this;
    }

    get(name, val) {
        if (!this[name])
            return '<unknown>';

        if (this[name].has(val))
            return this[name].get(val);

        return '<unknown>';
    }

    parse(name, val) {
        name += 'Names';
        if (!this[name])
            return -1;

        if (this[name].has(val))
            return this[name].get(val);

        return -1;
    }

    _load(enumName, enumClass) {
        var _this = this;
        $.get('cms/api/json/proto?enum=' + enumClass)
            .done((data) => {
                _this._onLoaded(enumName, data);
            })
            .fail(function(){
                _this._onLoadFailed(enumName, enumClass);
            });
    }

    _onLoaded(enumName, data) {
        var items = new Map();
        var names = new Map();
        for (var entry of data.value) {
            items.set(entry.number, entry.name);
            names.set(entry.name, entry.number);
        }

        this[enumName] = items;
        this[enumName + 'Names'] = names;

        --this._requests;
        if (this._requests == 0) {
            $('#' + this._errorElemId).html('');
            for (var cb of this._onDone)
                cb();
        }
    }

    _onLoadFailed(enumName, enumClass) {
        var error = 'Cannot load values for enum ' + enumName + ' (' + enumClass + ')';
        $('#' + this._errorElemId).html(error);
        var _this = this;
        setTimeout(function() { _this._load(enumName, enumClass) }, this._retryInterval);
    }
}

var cmsEnums = new Enums('enums-error');
