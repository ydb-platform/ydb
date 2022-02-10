var cfg = [{
    "label": "mygen1",
    "startTime": 0,
    "period": 50,
    "periodCount": 0,
    "reqSizeBytes": 1000000,
    "reqCount": 1,
    "reqInterval": 1,
    "user": "vdisk0",
    "desc": "write-log"
},{
    "label": "mygen2",
    "startTime": 0,
    "period": 50,
    "periodCount": 0,
    "reqSizeBytes": 1000000,
    "reqCount": 5,
    "reqInterval": 1,
    "user": "vdisk1",
    "desc": "write-chunk"
}];

var scharm = d3.scharm().height(350).width(800);

scharm.tableSelector('#cfg-table');
scharm.chartSelector('#cfg-chart');
scharm.applyUrl('?mode=setconfig');
scharm.timeDomain([0, 1000]);
scharm(cfg);
