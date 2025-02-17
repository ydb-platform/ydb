define("ace/mode/sql_highlight_rules",["require","exports","module","ace/lib/oop","ace/mode/text_highlight_rules"], function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var TextHighlightRules = require("./text_highlight_rules").TextHighlightRules;

var SqlHighlightRules = function() {

    var keywords = (
        "abort|action|all|and|as|asc|ast|attach|auth|avg|begin|between|case|cli|update|clusters|columns|compile|cross|cube|default|define|in|" +
        "delete|desc|describe|distinct|discard|do|drop|else|end|exclusion|explain|fields|file|from|full|join|get|if|for|evaluate|" +
        "group|by|grouping|having|help|history|inner|insert|into|is|join|left|limitmeta|" +
        "not|null|offset|on|only|open|operations|optimize|or|order|by|parallel|parse|pragma|preview|process|" +
        "progress|put|queries|query|quickstart|reduce|remove|replace|into|restart|result|results|right|join|rollup|run|running|" +
        "schema|scheme|select|semi|set|sets|show|status|stream|subquery|table|tables|then|union|use|using|validate|values|version|when|where|limit|" +
        "with|yamr|yt|null|revert|ignore|upsert|erase|presort|assume|any|without|window|partition|rows|range|groups|" +
        "unbounded|following|preceding|current|row|sample|tablesample|flatten|view|bernoulli|system|repeatable|over|return"
    );

    var builtinConstants = (
        "true|false"
    );

    var builtinFunctions = (
        "avg|cast|coalesce|likely|random|randomnumber|filecontent|filepath|length|max|median|count|count_if|" +
        "grouping|min|percentile|sum|min_by|max_by|min_of|max_of|stddev|variance|" +
        "stddev_sample|stddev_population|variance_sample|variance_population|" +
        "bool_and|bool_or|bit_and|bit_or|bit_xor|some|list|unique|sakura|betula|banach|smith|hegel|aristotle|plato|quine|marx|freud|hahn|cedar"
    );

    var dataTypes = (
        "string|byte|double|float|int32|uint32|int64|uint64|bool"
    );

    var keywordMapper = this.createKeywordMapper({
        "keyword.operator": builtinFunctions,
        "keyword": keywords,
        "constant.language": builtinConstants,
        "storage.type": dataTypes
    }, "identifier", true);

    this.$rules = {
        "start" : [ {
            token : "comment",
            regex : "--.*$"
        },  {
            token : "comment",
            start : "/\\*",
            end : "\\*/"
        }, {
            token : "string",           // " string
            regex : '".*?"'
        }, {
            token : "string",           // ' string
            regex : "'.*?'"
        }, {
            token : "keyword.operator",
            start: "\\[",
            end: "\\][:a-zA-Z0-9_]*"
        }, {
            token : "constant.numeric", // float
            regex : "[+-]?\\d+(?:(?:\\.\\d*)?(?:[eE][+-]?\\d+)?)?\\b"
        }, {
            token : "support.function",
            regex : "[a-zA-Z0-9_]+::[a-zA-Z0-9_]+"
        }, {
            token : keywordMapper,
            regex : "[a-zA-Z_][a-zA-Z0-9_$]*\\b"
        }, {
            token : "keyword.operator",
            regex : "\\+|\\-|\\/|\\/\\/|%|<@>|@>|<@|&|\\^|~|<|>|<=|=>|==|!=|<>|="
        }, {
            token : "paren.lparen",
            regex : "[\\(]"
        }, {
            token : "paren.rparen",
            regex : "[\\)]"
        }, {
            token : "text",
            regex : "\\s+"
        }, {
            token : "variable",
            regex : "[$][a-zA-Z0-9_$]*\\b"
        }, {
            token : "string",       // multiline string
            regex : '@@',
            next  : "multiline"
        } ],
        "multiline": [ {
            token : "string",
            regex: "[^@]+"
        }, {
            token : "string",
            regex : "[@]{2}",
            next  : "start"
        } ]
    };
    this.normalizeRules();
};

oop.inherits(SqlHighlightRules, TextHighlightRules);

exports.SqlHighlightRules = SqlHighlightRules;
});

define("ace/mode/sql",["require","exports","module","ace/lib/oop","ace/mode/text","ace/mode/sql_highlight_rules","ace/range"], function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var TextMode = require("./text").Mode;
var SqlHighlightRules = require("./sql_highlight_rules").SqlHighlightRules;
var Range = require("../range").Range;

var Mode = function() {
    this.HighlightRules = SqlHighlightRules;
};
oop.inherits(Mode, TextMode);

(function() {

    this.lineCommentStart = "--";

    this.$id = "ace/mode/sql";
}).call(Mode.prototype);

exports.Mode = Mode;

});
