'use strict';
const assert = require('assert');
const fs = require('fs');
const vm = require('vm');
const context = {};
vm.createContext(context);
vm.runInContext(fs.readFileSync(process.argv[2], 'utf8'), context);

for (const species of [19, 'block-8-2']) {
    assert.strictEqual(JSON.stringify(context.getErasureInfo(species)),
        JSON.stringify({Name: 'Block8+2', Min: 10, Total: 12}));
    for (let failed = 0; failed <= 3; ++failed) {
        const cells = Array.from({length: 6}, () => ({}));
        context.DiskStatsDomElement = {rows: [null, null, null, {cells}]};
        context.updateStatsCell = (cell, value) => { cell.value = value; };
        context.Groups = {1: {ErasureSpecies: species, VDisks: Object.fromEntries(
            Array.from({length: 12}, (_, disk) => [disk, {Color: disk < 12 - failed ? context.green : context.grey}])
        )}};
        context.refreshBSGroupStats();
        const expectedCell = [5, 4, 3, 2][failed]; // Green, Yellow, Orange, Red
        for (let cell = 1; cell <= 5; ++cell) {
            assert.strictEqual(cells[cell].value, Number(cell === expectedCell));
        }
    }
}
assert.strictEqual(context.getErasureInfo(4).Total, 8);
assert.strictEqual(context.getErasureInfo('block-4-2').Min, 6);
assert.strictEqual(context.MaxWideVDisks, 4); // VSlots per PDisk, independent of group width.
console.log('Legacy viewer: id/name mapping, all 12 VDisks, failure colors and Block42 regression passed.');
