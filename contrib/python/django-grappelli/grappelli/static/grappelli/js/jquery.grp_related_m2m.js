/**
 * GRAPPELLI RELATED M2M
 * m2m lookup
 */

(function($){

    var methods = {
        init: function(options) {
            options = $.extend({}, $.fn.grp_related_m2m.defaults, options);
            return this.each(function() {
                var $this = $(this);
                // add placeholder
                $this.parent().find('a.related-lookup').after(options.placeholder);
                // change lookup class
                $this.next().addClass("grp-m2m");
                // add related class
                $this.addClass('grp-has-related-lookup');
                // lookup
                lookup_id($this, options); // lookup when loading page
                $this.on("change focus keyup", function() { // id-handler
                    lookup_id($this, options);
                });
            });
        }
    };

    $.fn.grp_related_m2m = function(method) {
        if (methods[method]) {
            return methods[method].apply(this, Array.prototype.slice.call(arguments, 1));
        } else if (typeof method === 'object' || ! method) {
            return methods.init.apply(this, arguments);
        } else {
            $.error('Method ' +  method + ' does not exist on jQuery.grp_related_m2m');
        }
        return false;
    };

    var lookup_id = function(elem, options) {
        $.getJSON(options.lookup_url, {
            object_id: elem.val(),
            app_label: grappelli.get_app_label(elem),
            model_name: grappelli.get_model_name(elem),
            query_string: grappelli.get_query_string(elem)
        }, function(data) {
            values = $.map(data, function (a, i) {
                if (data.length === i + 1 && !a.safe) {
                    return $('<span class="grp-placeholder-label"></span>').text(a.label + '\u200E');
                } else if (data.length === i + 1 && a.safe) {
                        return $('<span class="grp-placeholder-label"></span>').html(a.label + '\u200E');
                } else if (a.safe) {
                    return $('<span class="grp-placeholder-label"></span>').html(a.label + '\u200E').append($('<span class="grp-separator"></span>'));
                } else {
                    return $('<span class="grp-placeholder-label"></span>').text(a.label + '\u200E').append($('<span class="grp-separator"></span>'));
                }
            });
            if (values === "") {
                elem.parent().find('.grp-placeholder-related-m2m').hide();
            } else {
                elem.parent().find('.grp-placeholder-related-m2m').show();
            }
            elem.parent().find('.grp-placeholder-related-m2m').html(values);
        });
    };

    $.fn.grp_related_m2m.defaults = {
        placeholder: '<div class="grp-placeholder-related-m2m"></div>',
        repr_max_length: 30,
        lookup_url: ''
    };

})(grp.jQuery);
