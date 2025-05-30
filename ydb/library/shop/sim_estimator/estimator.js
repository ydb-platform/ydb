d3.estimator = function (tableData) {
    function estimator() { }

    function createAll() {
        // create empty table
        table.append("table")
            .attr("class", "table data-table")
            ;

        table.append("thead").append("tr").attr("class", "data-table-head")
            .selectAll("td")
            .data(attributes)
            .enter().append("th")
            .text(function (d) { return d.name; })
            ;

        table.append("tbody");

        // initialize initial state editor
        //d3.select("input#throughput").on('keypress', onKeyPress);
    }

    function createTableData(tableData) {
        var data = [];
        for (var i = 0; i < tableData.length; i++) {
            var row = [];
            for (var a = 0; a < attributes.length; a++) {
                if (attributes[a].type) {
                    row.push(tableData[i][attributes[a].name]);
                } else {
                    row.push(i); // save row index for remove operation
                }
            }
            data.push(row);
        }

        var tr = table.select("tbody").selectAll("tr.data-table-row")
            .data(data)
            ;
        tr.exit().remove();
        var trEnterUpd = tr.enter().append("tr")
            .attr("class", "data-table-row")
            .merge(tr) // enter + update
            ;

        var td = trEnterUpd.selectAll("td")
            .data(function (d) { return d; })
            ;

        td.enter().append("td")
            .merge(td) // enter + update
            .attr("contenteditable", function (d, i) { return attributes[i].type ? "true" : null; })
            .text(function (d, i) { return attributes[i].type ? d : null; })
            //.on('keypress', onKeyPress)
            .attr("class", function (d, i) {
                return "data-cell data-cell-" +
                    (attributes[i].type ? attributes[i].type : attributes[i].class)
                    ;
            })
            ;
    }

    estimator.GetAverage = function () {
        return GoalMean;
    }

    estimator.GetEstimation = function (feature) {
        return GoalMean + GetSlope() * (feature - FeatureMean);
    }

    estimator.GetSlope = function () {
        var disp = FeatureSqMean - FeatureMean * FeatureMean;
        if (disp > 1e-10) {
            return (GoalFeatureMean - GoalMean * FeatureMean) / disp;
        } else {
            return GetAverage();
        }
    }

    estimator.Update = function (goal, feature) {
        GoalMean = OldFactor * GoalMean + NewFactor * goal;
        FeatureMean = OldFactor * FeatureMean + NewFactor * feature;
        FeatureSqMean = OldFactor * FeatureSqMean + NewFactor * feature * feature;
        GoalFeatureMean = OldFactor * GoalFeatureMean + NewFactor * goal * feature;
    }

    var GoalMean = 1,
        FeatureMean = 1,
        FeatureSqMean = 1,
        GoalFeatureMean = 1,
        NewFactor = 0.3,
        OldFactor = 1 - NewFactor
        ;

    var attributes = [
        { name: "feature", type: "integer" },
        { name: "goal", type: "integer" },
        //{ name: "estimate", type: "integer" },
        //{ name: "error", type: "integer" },

        // Remove button
        { name: "", class: "btn-remove" }
    ];

    var margin = {
        top: 20,
        right: 40,
        bottom: 20,
        left: 80,
        footer: 100,
    };

    var emptyRow = {
        goal: 1,
        feature: 1
    };

    var table = d3.select("#data-table");

    createAll();
    createTableData(tableData);

    return estimator;
}