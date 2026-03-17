(function ($) {
// Ok, let's do eeet

    ACTIVE_NODE_BG_COLOR = '#B7D7E8';
    RECENTLY_MOVED_COLOR = '#FFFF00';
    RECENTLY_MOVED_FADEOUT = '#FFFFFF';
    ABORT_COLOR = '#EECCCC';
    DRAG_LINE_COLOR = '#AA00AA';
    MOVE_NODE_ENDPOINT = 'move/';

    RECENTLY_FADE_DURATION = 2000;

    CSRF_TOKEN = document.currentScript.dataset.csrftoken;

    // Add jQuery util for disabling selection
    // Originally taken from jquery-ui (where it is deprecated)
    // https://api.jqueryui.com/disableSelection/
    $.fn.extend( {
        disableSelection: ( function() {
            var eventType = "onselectstart" in document.createElement( "div" ) ? "selectstart" : "mousedown";
            return function() {
                return this.on( eventType + ".ui-disableSelection", function( event ) {
                    event.preventDefault();
                } );
            };
        } )(),
    
        enableSelection: function() {
            return this.off( ".ui-disableSelection" );
        }
    } );

    // This is the basic Node class, which handles UI tree operations for each 'row'
    var Node = function (elem) {
        const $elem = $(elem);
        var node_id = $elem.data('node-id');
        var parent_id = $elem.data('parent-id');
        var level = parseInt($elem.data('level'));

        return {
            elem: elem,
            $elem: $elem,
            node_id: node_id,
            parent_id: parent_id,
            level: level,
            is_collapsed: function () {
                return $elem.find('a.treebeard-collapse').hasClass('treebeard-collapsed');
            },
            children: function () {
                return $('tr[data-parent-id="' + node_id + '"]');
            },
            collapse: function () {
                // For each children, hide it's children and so on...
                $.each(this.children(),function () {
                    new Node(this).collapse();
                }).hide();
                // Switch class to set the property expand/collapse icon
                $elem.find('a.treebeard-collapse').removeClass('treebeard-expanded').addClass('treebeard-collapsed');
            },
            expand: function () {
                // Display each kid (will display in collapsed state)
                this.children().show();
                // Swicth class to set the property expand/collapse icon
                $elem.find('a.treebeard-collapse').removeClass('treebeard-collapsed').addClass('treebeard-expanded');
            },
            toggle: function () {
                if (this.is_collapsed()) {
                    this.expand();
                } else {
                    this.collapse();
                }
            },
            clone: function () {
                return $elem.clone();
            }
        }
    };

    $(document).ready(function () {
        $(document).ajaxSend(function (event, xhr, settings) {
            if (!(/^http:.*/.test(settings.url) || /^https:.*/.test(settings.url))) {
                // Only send the token to relative URLs i.e. locally.
                xhr.setRequestHeader("X-CSRFToken", CSRF_TOKEN);
            }
        });

        const $resultList = $('#result_list tbody tr');

        // Read in JSON context and link it to each row in the table
        const contextList = JSON.parse(document.getElementById('tree-context').textContent);
        $resultList.each(function (index, el) {
            Object.entries(contextList[index]).forEach(function ([key, val]) {
                $(el).attr(`data-${key}`, val);     // Must use attr() to set the HTML5 attribute, not data()
            })
        });

        // Add drag handler and spacers to each node
        $resultList.each(function () {
            // Inject spacer and collapse buttons into the first table cell that isn't an action checkbox or drag handler
            const $firstCell = $(this).find("td,th").not(".action-checkbox").first();
            if (!$firstCell.length) {
                return;
            }

            const hasChildren = parseInt($(this).data("has-children"));
            if (hasChildren) {
                $firstCell.prepend("<a href='#' class='treebeard-collapse treebeard-expanded'>-</a>");
            }

            const level = parseInt($(this).data("level"));
            if (level > 1) {
                $firstCell.prepend("<span class='spacer'>&nbsp;</span>".repeat(level - 1));
            }

            $firstCell.prepend("<span class='drag-handler'></span>")
        })

        // Don't activate drag or collapse if GET filters are set on the page, or if user has no change permission
        if ($('#has-filters').val() === "1" || $('#has-change-permission').val() === "0") {
            return;
        }

        $body = $('body');

        // Activate all rows for drag & drop
        // then bind mouse down event
        $('.drag-handler').addClass('active').bind('mousedown', function (evt) {
            $ghost = $('<div id="ghost"></div>');
            $drag_line = $('<div id="drag_line"><span></span></div>');
            $ghost.appendTo($body);
            $drag_line.appendTo($body);

            var stop_drag = function () {
                $ghost.remove();
                $drag_line.remove();
                $body.enableSelection().unbind('mousemove').unbind('mouseup');
                node.elem.removeAttribute('style');
            };

            // Create a clone create the illusion that we're moving the node
            var node = new Node($(this).closest('tr')[0]);
            cloned_node = node.clone();
            node.$elem.css({
                'background': ACTIVE_NODE_BG_COLOR
            });

            $targetRow = null;
            as_child = false;

            // Now make the new clone move with the mouse
            $body.disableSelection().bind('mousemove',function (evt2) {
                $ghost.html(cloned_node).css({  // from FeinCMS :P
                    'opacity': .8,
                    'position': 'absolute',
                    'top': evt2.pageY,
                    'left': evt2.pageX - 30,
                    'width': 600
                });
                // Iterate through all rows and see where am I moving so I can place
                // the drag line accordingly
                rowHeight = node.$elem.height();
                $('tr', node.$elem.parent()).each(function (index, element) {
                    $row = $(element);
                    rtop = $row.offset().top;
                    // The tooltip will display whether I'm dropping the element as
                    // child or sibling
                    $tooltip = $drag_line.find('span');
                    $tooltip.css({
                        'left': node.$elem.width() - $tooltip.width(),
                        'height': rowHeight,
                    });
                    node_top = node.$elem.offset().top;
                    // Check if you are dragging over the same node
                    if (evt2.pageY >= node_top && evt2.pageY <= node_top + rowHeight) {
                        $targetRow = null;
                        $tooltip.text(gettext('Abort'));
                        $drag_line.css({
                            'top': node_top,
                            'height': rowHeight,
                            'borderWidth': 0,
                            'opacity': 0.8,
                            'backgroundColor': ABORT_COLOR
                        });
                    } else
                    // Check if mouse is over this row
                    if (evt2.pageY >= rtop && evt2.pageY <= rtop + rowHeight / 2) {
                        // The mouse is positioned on the top half of a $row
                        $targetRow = $row;
                        as_child = false;
                        $drag_line.css({
                            'left': node.$elem.offset().left,
                            'width': node.$elem.width(),
                            'top': rtop,
                            'borderWidth': '5px',
                            'height': 0,
                            'opacity': 1
                        });
                        $tooltip.text(gettext('As Sibling'));
                    } else if (evt2.pageY >= rtop + rowHeight / 2 && evt2.pageY <= rtop + rowHeight) {
                        // The mouse is positioned on the bottom half of a row
                        $targetRow = $row;
                        target_node = new Node($targetRow[0]);
                        if (target_node.is_collapsed()) {
                            target_node.expand();
                        }
                        as_child = true;
                        $drag_line.css({
                            'top': rtop,
                            'left': node.$elem.offset().left,
                            'height': rowHeight,
                            'opacity': 0.4,
                            'width': node.$elem.width(),
                            'borderWidth': 0,
                            'backgroundColor': DRAG_LINE_COLOR
                        });
                        $tooltip.text(gettext('As child'));
                    }
                });
            }).bind('mouseup',function () {
                    if ($targetRow !== null) {
                        target_node = new Node($targetRow[0]);
                        if (target_node.node_id !== node.node_id) {
                            // Call $.ajax so we can handle the error
                            // On Drop, make an XHR call to perform the node move
                            $.ajax({
                                url: MOVE_NODE_ENDPOINT,
                                type: 'POST',
                                data: {
                                    node_id: node.node_id,
                                    parent_id: target_node.parent_id,
                                    sibling_id: target_node.node_id,
                                    as_child: as_child ? 1 : 0
                                },
                                complete: function (req, status) {
                                    // http://stackoverflow.com/questions/1439895/add-a-hash-with-javascript-to-url-without-scrolling-page/1439910#1439910
                                    node.$elem.remove();
                                    window.location.hash = 'node-' + node.node_id;
                                    window.location.reload();
                                },
                                error: function (req, status, error) {
                                    // On error (!200) also reload to display
                                    // the message
                                    node.$elem.remove();
                                    window.location.hash = 'node-' + node.node_id;
                                    window.location.reload();
                                }
                            });
                        }
                    }
                    stop_drag();
                }).bind('keyup', function (kbevt) {
                    // Cancel drag on escape
                    if (kbevt.keyCode === 27) {
                        stop_drag();
                    }
                });
        });

        $('a.treebeard-collapse').click(function () {
            var node = new Node($(this).closest('tr')[0]); // send the DOM node, not jQ
            node.toggle();
            return false;
        });
        var hash = window.location.hash;
        // This is a hack, the actual element's id ends in '-id' but the url's hash
        // doesn't, I'm doing this to avoid scrolling the page... is that a good thing?
        if (hash) {
            $(hash + '-id').animate({
                backgroundColor: RECENTLY_MOVED_COLOR
            }, RECENTLY_FADE_DURATION, function () {
                $(this).animate({
                    backgroundColor: RECENTLY_MOVED_FADEOUT
                }, RECENTLY_FADE_DURATION, function () {
                    this.removeAttribute('style');
                });
            });
        }
    });
})(django.jQuery);

// http://stackoverflow.com/questions/190560/jquery-animate-backgroundcolor/2302005#2302005
(function (d) {
    d.each(["backgroundColor", "borderBottomColor", "borderLeftColor", "borderRightColor", "borderTopColor", "color", "outlineColor"], function (f, e) {
        d.fx.step[e] = function (g) {
            if (!g.colorInit) {
                g.start = c(g.elem, e);
                g.end = b(g.end);
                g.colorInit = true
            }
            g.elem.style[e] = "rgb(" + [Math.max(Math.min(parseInt((g.pos * (g.end[0] - g.start[0])) + g.start[0]), 255), 0), Math.max(Math.min(parseInt((g.pos * (g.end[1] - g.start[1])) + g.start[1]), 255), 0), Math.max(Math.min(parseInt((g.pos * (g.end[2] - g.start[2])) + g.start[2]), 255), 0)].join(",") + ")"
        }
    });
    function b(f) {
        var e;
        if (f && f.constructor == Array && f.length == 3) {
            return f
        }
        if (e = /rgb\(\s*([0-9]{1,3})\s*,\s*([0-9]{1,3})\s*,\s*([0-9]{1,3})\s*\)/.exec(f)) {
            return[parseInt(e[1]), parseInt(e[2]), parseInt(e[3])]
        }
        if (e = /rgb\(\s*([0-9]+(?:\.[0-9]+)?)\%\s*,\s*([0-9]+(?:\.[0-9]+)?)\%\s*,\s*([0-9]+(?:\.[0-9]+)?)\%\s*\)/.exec(f)) {
            return[parseFloat(e[1]) * 2.55, parseFloat(e[2]) * 2.55, parseFloat(e[3]) * 2.55]
        }
        if (e = /#([a-fA-F0-9]{2})([a-fA-F0-9]{2})([a-fA-F0-9]{2})/.exec(f)) {
            return[parseInt(e[1], 16), parseInt(e[2], 16), parseInt(e[3], 16)]
        }
        if (e = /#([a-fA-F0-9])([a-fA-F0-9])([a-fA-F0-9])/.exec(f)) {
            return[parseInt(e[1] + e[1], 16), parseInt(e[2] + e[2], 16), parseInt(e[3] + e[3], 16)]
        }
        if (e = /rgba\(0, 0, 0, 0\)/.exec(f)) {
            return a.transparent
        }
        return a[d.trim(f).toLowerCase()]
    }

    function c(g, e) {
        var f;
        do {
            f = d.css(g, e);
            if (f != "" && f != "transparent" || d.nodeName(g, "body")) {
                break
            }
            e = "backgroundColor"
        } while (g = g.parentNode);
        return b(f)
    }

    var a = {aqua: [0, 255, 255], azure: [240, 255, 255], beige: [245, 245, 220], black: [0, 0, 0], blue: [0, 0, 255], brown: [165, 42, 42], cyan: [0, 255, 255], darkblue: [0, 0, 139], darkcyan: [0, 139, 139], darkgrey: [169, 169, 169], darkgreen: [0, 100, 0], darkkhaki: [189, 183, 107], darkmagenta: [139, 0, 139], darkolivegreen: [85, 107, 47], darkorange: [255, 140, 0], darkorchid: [153, 50, 204], darkred: [139, 0, 0], darksalmon: [233, 150, 122], darkviolet: [148, 0, 211], fuchsia: [255, 0, 255], gold: [255, 215, 0], green: [0, 128, 0], indigo: [75, 0, 130], khaki: [240, 230, 140], lightblue: [173, 216, 230], lightcyan: [224, 255, 255], lightgreen: [144, 238, 144], lightgrey: [211, 211, 211], lightpink: [255, 182, 193], lightyellow: [255, 255, 224], lime: [0, 255, 0], magenta: [255, 0, 255], maroon: [128, 0, 0], navy: [0, 0, 128], olive: [128, 128, 0], orange: [255, 165, 0], pink: [255, 192, 203], purple: [128, 0, 128], violet: [128, 0, 128], red: [255, 0, 0], silver: [192, 192, 192], white: [255, 255, 255], yellow: [255, 255, 0], transparent: [255, 255, 255]}
})(django.jQuery);
