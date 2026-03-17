/**
 * GRAPPELLI GROUP COLLAPSIBLES
 * handles open/closing of all elements
 * with tabular- and stacked-inlines.
 */

(function($) {
    $.fn.grp_collapsible_group = function(options){
        var defaults = {
            open_handler_slctr: ".grp-open-handler",
            close_handler_slctr: ".grp-close-handler",
            collapsible_container_slctr: ".grp-collapse",
            closed_css: "grp-closed",
            open_css: "grp-open",
            on_init: function() {},
            on_open: function() {},
            on_close: function() {}
        };
        options = $.extend(defaults, options);
        return this.each(function() {
            _initialize($(this), options);
        });
    };
    var _initialize = function(elem, options) {
        options.on_init(elem, options);
        _register_handlers(elem, options);
    };
    var _register_handlers = function(elem, options) {
        _register_open_handler(elem, options);
        _register_close_handler(elem, options);
    };
    var _register_open_handler = function(elem, options) {
        elem.find(options.open_handler_slctr).each(function() {
            $(this).on("click", function() {
                options.on_open(elem, options);
                elem.find(options.collapsible_container_slctr)
                    .removeClass(options.closed_css)
                    .addClass(options.open_css);
                elem.removeClass(options.closed_css)
                    .addClass(options.open_css);
            });
        });
    };
    var _register_close_handler = function(elem, options) {
        elem.find(options.close_handler_slctr).each(function() {
            $(this).on("click", function() {
                options.on_close(elem, options);
                elem.find(options.collapsible_container_slctr)
                    .removeClass(options.open_css)
                    .addClass(options.closed_css);
            });
        });
    };
})(grp.jQuery);
