d3.json("schviz-test0.json", function(hist) {
    var linesCount = 100;
    var lineHeight = 50;
    for (var i = 4; i < linesCount; i++) {
        hist[0].cbs.push({
            "idx": i,
            "name": "vdisk" + i,
            "max_budget": 250000000,
            "period": 1000000000,
            "cur_budget": 250000000,
            "deadline": Math.random()*1000000000,
            "state": "ACTIVE",
            "req": [{
                "id": 1,
                "cost": 125000000,
                "state": "RUNNING",
                "seqno": 1
            }]
        });
    }

    var schviz = d3.schviz().height(lineHeight*linesCount).width(800);

    schviz.timeDomain([0, 1000]);
    schviz(hist);
});
