'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();
function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

/**
 * Encodes text into diff-codes to prepare for Myers diff.
 */
var Encoder = function () {
    function Encoder() {
        _classCallCheck(this, Encoder);

        this.code = 0;
        this.diff_codes = {};
    }

    _createClass(Encoder, [{
        key: 'encode',
        value: function encode(text, settings) {
            return new EncodeContext(this, text, settings);
        }
    }, {
        key: 'getCode',
        value: function getCode(line) {
            return this.diff_codes[line];
        }
    }, {
        key: 'hasKey',
        value: function hasKey(line) {
            return this.diff_codes.hasOwnProperty(line);
        }
    }, {
        key: 'getCodes',
        value: function getCodes() {
            return this.diff_codes;
        }
    }, {
        key: 'newCode',
        value: function newCode(line) {
            this.code = this.code + 1;
            this.diff_codes[line] = this.code;
            return this.code;
        }
    }]);

    return Encoder;
}();

/**
 * Encoder context
 */

var EncodeContext = function () {
    function EncodeContext(encoder, text, settings) {
        _classCallCheck(this, EncodeContext);

        var lines = void 0,
            re = void 0;
        if (text && text.length) {
            if (encoder === undefined) {
                throw new Error('illegal argument \'encoder\'');
            }
            if (text === undefined) {
                throw new Error('illegal argument \'text\'');
            }
            if (settings === undefined) {
                throw new Error('illegal argument \'settings\'');
            }
            if (settings.compare === 'chars') {
                // split all chars
                re = new RegExp(settings.splitCharsRegex, "g");
            } else if (settings.compare === 'words') {
                // split all of the text on spaces
                re = new RegExp(settings.splitWordsRegex, "g");
            } else {
                // lines (default)
                re = new RegExp(settings.splitLinesRegex, "g");
            }
            lines = text.split(re);
        } else {
            // line is empty
            lines = [];
        }
        this._init(encoder, lines, settings);
    }

    _createClass(EncodeContext, [{
        key: 'getLine',
        value: function getLine(n) {
            if (!this._codes.hasOwnProperty(n)) {
                return;
            }
            var key = void 0,
                ckey = this._codes[n];
            var keyCodes = this.encoder.getCodes();
            for (key in keyCodes) {
                if (keyCodes.hasOwnProperty(key)) {
                    if (keyCodes[key] === ckey) {
                        return key;
                    }
                }
            }
        }
    }, {
        key: '_init',
        value: function _init(encoder, lines, settings) {
            this.encoder = encoder;
            this._codes = {};
            this._modified = {};

            // for each line, if it exists in 'diff_codes', then 'codes' will
            // be assgined the existing value from 'diff_codes'.  if the line does
            // not existin 'diff_codes', then a new diff_code will be generated
            // (EncodeContext.code) and stored in 'codes' for the line.
            for (var i = 0; i < lines.length; ++i) {
                var line = lines[i];
                if (settings.ignoreWhitespace) {
                    line = line.replace(/\s+/g, '');
                }
                var aCode = encoder.getCode(line);
                if (aCode !== undefined) {
                    this._codes[i] = aCode;
                } else {
                    this._codes[i] = encoder.newCode(line);
                }
            }
        }
    }, {
        key: 'codes',
        get: function get() {
            return this._codes;
        }
    }, {
        key: 'length',
        get: function get() {
            return Object.keys(this._codes).length;
        }
    }, {
        key: 'modified',
        get: function get() {
            return this._modified;
        }
    }]);

    return EncodeContext;
}();

var Myers = function () {
    function Myers() {
        _classCallCheck(this, Myers);
    }

    _createClass(Myers, null, [{
        key: 'compare_lcs',
        value: function compare_lcs(lhs_modified, lhs_codes, lhs_codes_length, rhs_modified, rhs_codes, rhs_codes_length, callback) {
            var lhs_start = 0,
                rhs_start = 0,
                lhs_line = 0,
                rhs_line = 0,
                item = void 0;

            while (lhs_line < lhs_codes_length || rhs_line < rhs_codes_length) {
                if (lhs_line < lhs_codes_length && !lhs_modified[lhs_line] && rhs_line < rhs_codes_length && !rhs_modified[rhs_line]) {
                    // equal lines
                    lhs_line++;
                    rhs_line++;
                } else {
                    // maybe deleted and/or inserted lines
                    lhs_start = lhs_line;
                    rhs_start = rhs_line;
                    while (lhs_line < lhs_codes_length && (rhs_line >= rhs_codes_length || lhs_modified[lhs_line])) {
                        lhs_line++;
                    }
                    while (rhs_line < rhs_codes_length && (lhs_line >= lhs_codes_length || rhs_modified[rhs_line])) {
                        rhs_line++;
                    }
                    if (lhs_start < lhs_line || rhs_start < rhs_line) {
                        item = {
                            lhs: {
                                at: Math.min(lhs_start, lhs_codes_length ? lhs_codes_length - 1 : 0),
                                del: lhs_line - lhs_start
                            },
                            rhs: {
                                at: Math.min(rhs_start, rhs_codes_length ? rhs_codes_length - 1 : 0),
                                add: rhs_line - rhs_start
                            }
                        };
                        callback(item);
                    }
                }
            }
        }
    }, {
        key: 'getShortestMiddleSnake',
        value: function getShortestMiddleSnake(lhs_codes, lhs_codes_length, lhs_lower, lhs_upper, rhs_codes, rhs_codes_length, rhs_lower, rhs_upper, vector_u, vector_d) {
            var max = lhs_codes_length + rhs_codes_length + 1;
            if (max === undefined) {
                throw new Error('unexpected state');
            }
            var kdown = lhs_lower - rhs_lower,
                kup = lhs_upper - rhs_upper,
                delta = lhs_upper - lhs_lower - (rhs_upper - rhs_lower),
                odd = (delta & 1) != 0,
                offset_down = max - kdown,
                offset_up = max - kup,
                maxd = (lhs_upper - lhs_lower + rhs_upper - rhs_lower) / 2 + 1,
                ret = { x: 0, y: 0 },
                d = void 0,
                k = void 0,
                x = void 0,
                y = void 0;

            vector_d[offset_down + kdown + 1] = lhs_lower;
            vector_u[offset_up + kup - 1] = lhs_upper;
            for (d = 0; d <= maxd; ++d) {
                for (k = kdown - d; k <= kdown + d; k += 2) {
                    if (k === kdown - d) {
                        x = vector_d[offset_down + k + 1]; //down
                    } else {
                        x = vector_d[offset_down + k - 1] + 1; //right
                        if (k < kdown + d && vector_d[offset_down + k + 1] >= x) {
                            x = vector_d[offset_down + k + 1]; //down
                        }
                    }
                    y = x - k;
                    // find the end of the furthest reaching forward D-path in diagonal k.
                    while (x < lhs_upper && y < rhs_upper && lhs_codes[x] === rhs_codes[y]) {
                        x++;y++;
                    }
                    vector_d[offset_down + k] = x;
                    // overlap ?
                    if (odd && kup - d < k && k < kup + d) {
                        if (vector_u[offset_up + k] <= vector_d[offset_down + k]) {
                            ret.x = vector_d[offset_down + k];
                            ret.y = vector_d[offset_down + k] - k;
                            return ret;
                        }
                    }
                }
                // Extend the reverse path.
                for (k = kup - d; k <= kup + d; k += 2) {
                    // find the only or better starting point
                    if (k === kup + d) {
                        x = vector_u[offset_up + k - 1]; // up
                    } else {
                        x = vector_u[offset_up + k + 1] - 1; // left
                        if (k > kup - d && vector_u[offset_up + k - 1] < x) x = vector_u[offset_up + k - 1]; // up
                    }
                    y = x - k;
                    while (x > lhs_lower && y > rhs_lower && lhs_codes[x - 1] === rhs_codes[y - 1]) {
                        // diagonal
                        x--;
                        y--;
                    }
                    vector_u[offset_up + k] = x;
                    // overlap ?
                    if (!odd && kdown - d <= k && k <= kdown + d) {
                        if (vector_u[offset_up + k] <= vector_d[offset_down + k]) {
                            ret.x = vector_d[offset_down + k];
                            ret.y = vector_d[offset_down + k] - k;
                            return ret;
                        }
                    }
                }
            }
            // should never get to this state
            throw new Error('unexpected state');
        }
    }, {
        key: 'getLongestCommonSubsequence',
        value: function getLongestCommonSubsequence(lhs_modified, lhs_codes, lhs_codes_length, lhs_lower, lhs_upper, rhs_modified, rhs_codes, rhs_codes_length, rhs_lower, rhs_upper, vector_u, vector_d) {
            // trim off the matching items at the beginning
            while (lhs_lower < lhs_upper && rhs_lower < rhs_upper && lhs_codes[lhs_lower] === rhs_codes[rhs_lower]) {
                ++lhs_lower;
                ++rhs_lower;
            }
            // trim off the matching items at the end
            while (lhs_lower < lhs_upper && rhs_lower < rhs_upper && lhs_codes[lhs_upper - 1] === rhs_codes[rhs_upper - 1]) {
                --lhs_upper;
                --rhs_upper;
            }
            if (lhs_lower === lhs_upper) {
                while (rhs_lower < rhs_upper) {
                    rhs_modified[rhs_lower++] = true;
                }
            } else if (rhs_lower === rhs_upper) {
                while (lhs_lower < lhs_upper) {
                    lhs_modified[lhs_lower++] = true;
                }
            } else {
                var sms = Myers.getShortestMiddleSnake(lhs_codes, lhs_codes_length, lhs_lower, lhs_upper, rhs_codes, rhs_codes_length, rhs_lower, rhs_upper, vector_u, vector_d);
                Myers.getLongestCommonSubsequence(lhs_modified, lhs_codes, lhs_codes_length, lhs_lower, sms.x, rhs_modified, rhs_codes, rhs_codes_length, rhs_lower, sms.y, vector_u, vector_d);
                Myers.getLongestCommonSubsequence(lhs_modified, lhs_codes, lhs_codes_length, sms.x, lhs_upper, rhs_modified, rhs_codes, rhs_codes_length, sms.y, rhs_upper, vector_u, vector_d);
            }
        }
    }, {
        key: 'optimize',
        value: function optimize(ctx) {
            var start = 0,
                end = 0;
            while (start < ctx._codes.length) {
                while (start < ctx._codes.length && (ctx._modified[start] === undefined || ctx._modified[start] === false)) {
                    start++;
                }
                end = start;
                while (end < ctx._codes.length && ctx._modified[end] === true) {
                    end++;
                }
                if (end < ctx._codes.length && ctx._codes[start] === ctx._codes[end]) {
                    ctx._modified[start] = false;
                    ctx._modified[end] = true;
                } else {
                    start = end;
                }
            }
        }
    }, {
        key: 'LCS',
        value: function LCS(lhsModified, lhsCodes, lhsLength, rhsModified, rhsCodes, rhsLength) {
            var vector_u = [],
                vector_d = [];
            return Myers.getLongestCommonSubsequence(lhsModified, lhsCodes, lhsLength, 0, lhsLength, rhsModified, rhsCodes, rhsLength, 0, rhsLength, vector_u, vector_d);
        }
    }, {
        key: 'CompareLCS',
        value: function CompareLCS(lhsModified, lhsCodes, lhsLength, rhsModified, rhsCodes, rhsLength, callback) {
            return Myers.compare_lcs(lhsModified, lhsCodes, lhsLength, rhsModified, rhsCodes, rhsLength, callback);
        }

        /**
         * Compare {@code lhs} to {@code rhs}.  Changes are compared from left
         * to right such that items are deleted from left, or added to right,
         * or just otherwise changed between them.
         *
         * @param   {String} lhs        The left-hand source text.
         * @param   {String} rhs        The right-hand source text.
         * @param   {Object} options    Optional settings.
         */

    }, {
        key: 'diff',
        value: function diff(lhs, rhs, options) {
            var settings = Myers._getDefaultSettings(),
                encoder = new Encoder();

            if (lhs === undefined) {
                throw new Error('illegal argument \'lhs\'');
            }
            if (rhs === undefined) {
                throw new Error('illegal argument \'rhs\'');
            }

            Object.assign(settings, options);

            var lhsCtx = encoder.encode(lhs, settings),
                rhsCtx = encoder.encode(rhs, settings);

            Myers.LCS(lhsCtx.modified, lhsCtx.codes, lhsCtx.length, rhsCtx.modified, rhsCtx.codes, rhsCtx.length);

            Myers.optimize(lhsCtx);
            Myers.optimize(rhsCtx);

            // compare lhs/rhs codes and build a list of comparisons
            var items = void 0;
            Myers.CompareLCS(lhsCtx.modified, lhsCtx.codes, lhsCtx.length, rhsCtx.modified, rhsCtx.codes, rhsCtx.length, function (item) {
                // add context information
                item.lhs.ctx = lhsCtx;
                item.rhs.ctx = rhsCtx;
                if (items === undefined) {
                    items = [];
                }
                items.push(item);
            });
            if (items === undefined) {
                return [];
            }
            return items;
        }
    }, {
        key: '_getDefaultSettings',
        value: function _getDefaultSettings() {
            return {
                compare: 'lines', // lines|words|chars
                ignoreWhitespace: false,
                splitLinesRegex: '\n',
                splitWordsRegex: '[ ]{1}',
                splitCharsRegex: ''
            };
        }
    }]);

    return Myers;
}();
