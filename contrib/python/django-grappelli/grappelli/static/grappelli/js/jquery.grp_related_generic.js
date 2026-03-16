/**
 * GRAPPELLI RELATED FK
 * generic lookup
 */

(function($){

    var methods = {
        init: function(options) {
            options = $.extend({}, $.fn.grp_related_generic.defaults, options);
            return this.each(function() {
                var $this = $(this);
                // add placeholder
                var val = $(options.content_type).val() || $(options.content_type).find(':checked').val();
                if (val) {
                    $this.after(options.placeholder).after(lookup_link($this.attr('id'),val));
                }
                // add related class
                $this.addClass('grp-has-related-lookup');
                // lookup
                if (val) {
                    lookup_id($this, options); // lookup when loading page
                }
                $this.on("change focus keyup", function() { // id-handler
                    lookup_id($this, options);
                });
                $(options.content_type).on("change", function() { // content-type-handler
                    update_lookup($(this), options);
                });
            });
        }
    };

    $.fn.grp_related_generic = function(method) {
        if (methods[method]) {
            return methods[method].apply(this, Array.prototype.slice.call(arguments, 1));
        } else if (typeof method === 'object' || ! method) {
            return methods.init.apply(this, arguments);
        } else {
            $.error('Method ' +  method + ' does not exist on jQuery.grp_related_generic');
        }
        return false;
    };

    var lookup_link = function(id, val) {
        var lookuplink = $('<a class="related-lookup"></a>');
        lookuplink.attr('id', 'lookup_'+id);
        lookuplink.attr('href', window.ADMIN_URL + MODEL_URL_ARRAY[val].app + "/" + MODEL_URL_ARRAY[val].model + '/?');
        lookuplink.attr('onClick', 'return showRelatedObjectLookupPopup(this);');
        return lookuplink;
    };

    var update_lookup = function(elem, options) {
        var obj = $(options.object_id);
        obj.val('');
        obj.parent().find('a.related-lookup').remove();
        obj.parent().find('.grp-placeholder-related-generic').remove();
        var val = $(elem).val() || $(elem).find(':checked').val();
        if (val) {
            obj.after(options.placeholder).after(lookup_link(obj.attr('id'),val));
        }
    };

    var lookup_id = function(elem, options) {
        var text = elem.next().next();
        $.getJSON(options.lookup_url, {
            object_id: elem.val(),
            app_label: grappelli.get_app_label(elem),
            model_name: grappelli.get_model_name(elem),
            query_string: grappelli.get_query_string(elem)
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

    $.fn.grp_related_generic.defaults = {
        placeholder: '<div class="grp-placeholder-related-generic" style="display:none"></div>',
        repr_max_length: 30,
        lookup_url: '',
        content_type: '',
        object_id: ''
    };

})(grp.jQuery);
