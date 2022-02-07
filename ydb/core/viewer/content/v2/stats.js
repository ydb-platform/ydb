function Stat(options) {
    Object.assign(this,
                  options);
    this.buildDomElement();
}

Stat.prototype.buildDomElement = function() {
    var cell = $('<div>', {class: 'stats_cell'});
    var name = $('<div>', {class: 'stats_name', text: this.name});
    var value = $('<div>', {class: 'stats_value', text: '-'});
    cell.append(name);
    cell.append(value);
    this.domElementValue = value[0];
    this.domElement = cell[0];
}

Stat.prototype.setValue = function(value) {
    $(this.domElementValue).text(value);
}


function Stats(options) {
    Object.assign(this,
                  {
                      stats: {}
                  },
                  options);
    this.buildDomElement();
}

Stats.prototype.buildDomElement = function() {
    this.domElement = $('<div>', {class: 'stats_container'})[0];
}

Stats.prototype.addStat = function(name) {
    var stat = this.stats[name];
    if (!stat) {
        stat = this.stats[name] = new Stat({name: name});
        $(this.domElement).append(stat.domElement);
    }
    return stat;
}
