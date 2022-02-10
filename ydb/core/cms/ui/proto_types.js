'use strict';

class ProtoTypes {

    constructor(errorElemId) {
        this._requested = new Set();
        this._requests = 0;
        this._retryInterval = 5000;
        this._errorElemId = errorElemId;
        this._onDone = [];
    }

    add(typeName) {
        if (!this._requested.has(typeName)) {
            this._requested.add(typeName);
            ++this._requests;
            this._load(typeName);
        }

        return this;
    }

    done(cb) {
        this._onDone.push(cb);
        return this;
    }

    get(name) {
        if (!this[name])
            return undefined;
        return this[name];
    }

    _load(typeName) {
        var _this = this;
        $.get('cms/api/json/proto?type=' + typeName)
            .done((data) => {
                _this._onLoaded(typeName, data);
            })
            .fail(function(){
                _this._onLoadFailed(typeName);
            });
    }

    _onLoaded(typeName, data) {
        this[typeName] = data;

        for (var field of data.field) {
            if (field.type_name !== undefined) {
                this.add(field.type_name);
            }
        }

        --this._requests;
        if (this._requests == 0) {
            $('#' + this._errorElemId).html('');
            for (var cb of this._onDone)
                cb();
        }
    }

    _onLoadFailed(typeName) {
        var error = 'Cannot load description of proto type ' + typeName;
        $('#' + this._errorElemId).html(error);
        var _this = this;
        setTimeout(function() { _this._load(typeName) }, this._retryInterval);
    }
}

var cmsProtoTypes = new ProtoTypes('proto-types-error');
