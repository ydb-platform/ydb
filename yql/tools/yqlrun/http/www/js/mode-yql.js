define("ace/mode/yql_highlight_rules",
        ["require","exports","module","ace/lib/oop","ace/mode/text_highlight_rules"],
        function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var TextHighlightRules = require("./text_highlight_rules").TextHighlightRules;

var YqlHighlightRules = function() {
    var keywordControl = "let|return|quote|lambda|block|import|export|bind";
    var keywordMapper = this.createKeywordMapper({
        "keyword.control": keywordControl,
        "keyword.operator": window.yql.builtinFunctions.join("|"),
        "variable": window.yql.supportedFunctions.join("|")
    }, "identifier", false);

    this.$rules = {
        "start": [
            {
                token : "comment",
                regex : "#.*$"
            },
            {
                token: ["storage.type.function-type.yql", "text", "entity.name.function.lisp"],
                regex: "(?:\\b(?:(defun|defmethod|defmacro))\\b)(\\s+)((?:\\w|\\-|\\?)*)"
            },
            {
                token: ["punctuation.definition.constant.character.lisp", "constant.character.lisp"],
                regex: "(#)((?:\\w|[\\\\+-=<>'\"&#])+)"
            },
            {
                token: ["punctuation.definition.variable.lisp", "variable.other.global.lisp", "punctuation.definition.variable.lisp"],
                regex: "(\\*)(\\S*)(\\*)"
            },
            {
                token : "string",
                regex : '\'?"(?=.)',
                next  : "qqstring"
            },
            {
                token : "string",       // multiline atom
                regex : '\'?@@',
                next  : "multiline"
            },
            {
                token : "string",           // binary atom
                regex : '\'?x"(?:[0-9A-Fa-f]{2})*"',
            },
            {
                token : "variable.parameter", // short quote
                regex : "'[^#\\s)(]+"
            },
            {
                token : keywordMapper,
                regex : "(?:(?:<=?|>=?|==|!=|[-+*/%])|[a-zA-Z][a-zA-Z0-9!]*)"
            }
        ],
        "qqstring": [
            {
                token: "string.regexp",
                regex: "\\\\(?:[0-3][0-7][0-7]|x[0-9A-Fa-f]{2}|[\"tnrbfav\\\\])"
            },
            {
                token : "string",
                regex : '[^"\\\\]+'
            },
            {
                token : "string",
                regex : '"|$',
                next  : "start"
            }
        ],
        "multiline": [
            {
                token : "string",
                regex: "[^@]+"
            },
            {
                token : "string",
                regex : "[@]{2}",
                next  : "start"
            },
        ]
    }
};

oop.inherits(YqlHighlightRules, TextHighlightRules);
exports.YqlHighlightRules = YqlHighlightRules;
});

define("ace/mode/yql",
        ["require","exports","module","ace/lib/oop","ace/mode/text","ace/mode/yql_highlight_rules"],
        function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var TextMode = require("./text").Mode;
var YqlHighlightRules = require("./yql_highlight_rules").YqlHighlightRules;

var Mode = function() {
    this.HighlightRules = YqlHighlightRules;
};
oop.inherits(Mode, TextMode);

(function() {
    this.lineCommentStart = "#";
    this.$id = "ace/mode/yql";
}).call(Mode.prototype);

exports.Mode = Mode;
});
