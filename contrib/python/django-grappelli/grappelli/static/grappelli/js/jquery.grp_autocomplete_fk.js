/**
 * GRAPPELLI AUTOCOMPLETE FK
 * foreign-key lookup with autocomplete
 */

(function($){

    var methods = {
        init: function(options) {
            options = $.extend({}, $.fn.grp_autocomplete_fk.defaults, options);
            return this.each(function() {
                var $this = $(this);
                // assign attributes
                $this.attr({
                    "tabindex": "-1",
                    "readonly": "readonly"
                }).addClass("grp-autocomplete-hidden-field");
                // remove djangos object representation (if given)
                if ($this.next().next() && $this.next().next().attr("class") != "errorlist" && $this.next().next().attr("class") != "grp-help") $this.next().next().remove();
                // build autocomplete wrapper
                $this.next().after(loader).after(remove_link($this.attr('id')));
                $this.parent().wrapInner("<div class='grp-autocomplete-wrapper-fk'></div>");
                $this.parent().prepend("<input id='" + $this.attr("id") + "-autocomplete' type='text' class='vTextField' value='' />");
                // extend options
                options = $.extend({
                    wrapper_autocomplete: $this.parent(),
                    input_field: $this.prev(),
                    remove_link: $this.next().next().hide(),
                    loader: $this.next().next().next().hide()
                }, $.fn.grp_autocomplete_fk.defaults, options);
                // lookup
                lookup_id($this, options); // lookup when loading page
                lookup_autocomplete($this, options); // autocomplete-handler
                $this.on("change focus keyup", function() { // id-handler
                    lookup_id($this, options);
                });
                // labels
                $("label[for='"+$this.attr('id')+"']").each(function() {
                    $(this).attr("for", $this.attr("id")+"-autocomplete");
                });
            });
        }
    };

    $.fn.grp_autocomplete_fk = function(method) {
        if (methods[method]) {
            return methods[method].apply(this, Array.prototype.slice.call(arguments, 1));
        } else if (typeof method === 'object' || ! method) {
            return methods.init.apply(this, arguments);
        } else {
            $.error('Method ' +  method + ' does not exist on jQuery.grp_autocomplete_fk');
        }
        return false;
    };

    var loader = function() {
        var loader = $('<div class="grp-loader">loader</div>');
        return loader;
    };

    var remove_link = function(id) {
        var removelink = $('<a class="grp-related-remove"></a>');
        removelink.attr('id', 'remove_'+id);
        removelink.attr('href', 'javascript://');
        removelink.attr('onClick', 'return removeRelatedObject(this);');
        removelink.hover(function() {
            $(this).parent().toggleClass("grp-autocomplete-preremove");
        });
        return removelink;
    };

    var lookup_autocomplete = function(elem, options) {
        options.wrapper_autocomplete.find("input:first")
            .on("focus", function() {
                options.wrapper_autocomplete.addClass("grp-state-focus");
            })
            .on("blur", function() {
                options.wrapper_autocomplete.removeClass("grp-state-focus");
            })
            .autocomplete({
                minLength: 1,
                autoFocus: true,
                delay: 1000,
                source: function(request, response) {
                    $.ajax({
                        url: options.autocomplete_lookup_url,
                        dataType: 'json',
                        data: "term=" + encodeURIComponent(request.term) + "&app_label=" + grappelli.get_app_label(elem) + "&model_name=" + grappelli.get_model_name(elem) + "&query_string=" + grappelli.get_query_string(elem) + "&to_field=" + grappelli.get_to_field(elem),
                        beforeSend: function (XMLHttpRequest) {
                            options.loader.show();
                        },
                        success: function(data){
                            response($.map(data, function(item) {
                                return {label: item.label, value: item.value};
                            }));
                        },
                        complete: function (XMLHttpRequest, textStatus) {
                            options.loader.hide();
                        }
                    });
                },
                focus: function() { // prevent value inserted on focus
                    return false;
                },
                select: function(event, ui) {
                    options.input_field.val(ui.item.label);
                    elem.val(ui.item.value);
                    elem.trigger('change');
                    elem.val() ? $(options.remove_link).show() : $(options.remove_link).hide();
                    return false;
                }
            })
            .data("ui-autocomplete")._renderItem = function(ul,item) {
                if (!item.value) {
                    return $("<li class='ui-state-disabled'></li>")
                        .data( "item.autocomplete", item )
                        .append($("<span class='error'></span>").text(item.label))
                        .appendTo(ul);
                } else {
                    return $("<li></li>")
                        .data( "item.autocomplete", item )
                        .append($("<a></a>").text(item.label))
                        .appendTo(ul);
                }
            };
    };

    var lookup_id = function(elem, options) {
        $.getJSON(options.lookup_url, {
            object_id: elem.val(),
            app_label: grappelli.get_app_label(elem),
            model_name: grappelli.get_model_name(elem),
            query_string: grappelli.get_query_string(elem),
            to_field: grappelli.get_to_field(elem)
        }, function(data) {
            $.each(data, function(index) {
                options.input_field.val(data[index].label);
                elem.val() ? $(options.remove_link).show() : $(options.remove_link).hide();
            });
        });
    };

    $.fn.grp_autocomplete_fk.defaults = {
        autocomplete_lookup_url: '',
        lookup_url: ''
    };

})(grp.jQuery);
