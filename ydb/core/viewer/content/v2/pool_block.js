function PoolBlock() {
    this.buildDomElement();
}

PoolBlock.prototype.poolColors = new PoolColors();

PoolBlock.prototype.buildDomElement = function() {
    var pooldiv = $('<div>', {class: 'pool_block', style: 'width:39px;background-color:#90ee90'});
    var innerdiv = $('<div>', {class: 'pool_div', style: 'height:100%;background-color:' + lightgrey});
    var innerspan = $('<span>', {class: 'pool_span'});
    pooldiv.append(innerdiv);
    pooldiv.append(innerspan);
    this.domElementDiv = innerdiv[0];
    this.domElementSpan = innerspan[0];
    this.domElement = pooldiv[0];
    pooldiv.tooltip({html: true});
}

PoolBlock.prototype.setText = function(text) {
    $(this.domElementSpan).text(text);
}

PoolBlock.prototype.setUsage = function(value) {
    var usage = (value * 100).toFixed();
    if (usage < 0) {
        usage = 0;
    }
    if (usage > 100) {
        usage = 100;
    }
    var height = 100 - usage;
    this.domElement.style.backgroundColor = this.poolColors.getPoolColor(value);
    this.domElementDiv.style.height = height + '%';
}

PoolBlock.prototype.restoreTooltips = function() {
    $(this.domElement).tooltip({html: true});
}
