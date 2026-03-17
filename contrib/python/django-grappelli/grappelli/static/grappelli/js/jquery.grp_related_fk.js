/**
 * GRAPPELLI RELATED FK
 * foreign-key lookup
 */

(function($){

    var methods = {
        init: function(options) {
            options = $.extend({}, $.fn.grp_related_fk.defaults, options);
            return this.each(function() {
                var $this = $(this);
                var $parent = $this.parent();
                // remove djangos object representation
                if ($parent.find('a.related-lookup').next().is('strong')) {
                    $parent.find('a.related-lookup').get(0).nextSibling.nodeValue="";
                    $parent.find('a.related-lookup').next('strong').remove();
                }
                // add placeholder
                $parent.find('a.related-lookup').after(options.placeholder);
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

    $.fn.grp_related_fk = function(method) {
        if (methods[method]) {
            return methods[method].apply(this, Array.prototype.slice.call(arguments, 1));
        } else if (typeof method === 'object' || ! method) {
            return methods.init.apply(this, arguments);
        } else {
            $.error('Method ' +  method + ' does not exist on jQuery.grp_related_fk');
        }
        return false;
    };

    var lookup_id = function(elem, options) {
        var text = elem.parent().find('.grp-placeholder-related-fk');
        $.getJSON(options.lookup_url, {
            object_id: elem.val(),
            app_label: grappelli.get_app_label(elem),
            model_name: grappelli.get_model_name(elem),
            query_string: grappelli.get_query_string(elem),
            to_field: grappelli.get_to_field(elem)
        }, function(data) {
            if (data[0].label === "") {
                text.hide();
            } else {
                text.show();
            }
            if (data[0].safe) {
                text.html($('<span class="grp-placeholder-label"></span>').html(data[0].label + '\u200E'));
            } else {
                text.html($('<span class="grp-placeholder-label"></span>').text(data[0].label + '\u200E'));
            }
        });
    };

    $.fn.grp_related_fk.defaults = {
        placeholder: '<div class="grp-placeholder-related-fk"></div>',
        repr_max_length: 30,
        lookup_url: ''
    };

})(grp.jQuery);
