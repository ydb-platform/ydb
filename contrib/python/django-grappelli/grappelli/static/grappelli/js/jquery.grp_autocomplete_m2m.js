/**
 * GRAPPELLI AUTOCOMPLETE M2M
 * many-to-many lookup with autocomplete
 */


(function($){

    var methods = {
        init: function(options) {
            options = $.extend({}, $.fn.grp_autocomplete_m2m.defaults, options);
            return this.each(function() {
                var $this = $(this);
                // assign attributes
                $this.attr({
                    "tabindex": "-1",
                    "readonly": "readonly"
                }).addClass("grp-autocomplete-hidden-field");
                // build autocomplete wrapper
                $this.next().after(loader).after(remove_link($this.attr('id')));
                $this.parent().wrapInner("<div class='grp-autocomplete-wrapper-m2m'></div>");
                //$this.parent().prepend("<ul class='search'><li class='search'><input id='" + $this.attr("id") + "-autocomplete' type='text' class='vTextField' value='' /></li></ul>").prepend("<ul class='repr'></ul>");
                $this.parent().prepend("<ul class='grp-repr'><li class='grp-search'><input id='" + $this.attr("id") + "-autocomplete' type='text' class='vTextField' value='' /></li></ul>");
                // defaults
                options = $.extend({
                    wrapper_autocomplete: $this.parent(),
                    wrapper_repr: $this.parent().find("ul.grp-repr"),
                    wrapper_search: $this.parent().find("li.grp-search"),
                    remove_link: $this.next().next().hide(),
                    loader: $this.next().next().next().hide()
                }, $.fn.grp_autocomplete_m2m.defaults, options);
                // move errorlist outside the wrapper
                if ($this.parent().find("ul.errorlist")) {
                    $this.parent().find("ul.errorlist").detach().appendTo($this.parent().parent());
                }
                // move helptext outside the wrapper
                if ($this.parent().find("p.grp-help")) {
                    $this.parent().find("p.grp-help").detach().appendTo($this.parent().parent());
                }
                // lookup
                lookup_id($this, options);  // lookup when loading page
                lookup_autocomplete($this, options);  // autocomplete-handler
                $this.on("change focus keyup", function() { // id-handler
                    lookup_id($this, options);
                });
                // labels
                $("label[for='"+$this.attr('id')+"']").each(function() {
                    $(this).attr("for", $this.attr("id")+"-autocomplete");
                });
                // click on div > focus input
                options.wrapper_autocomplete.on("click", function(e) {
                    // prevent focus when clicking on remove/select
                    if (!$(e.target).hasClass("related-lookup") && !$(e.target).hasClass("grp-related-remove")) {
                        options.wrapper_search.find("input:first").trigger("focus");
                    }
                });
            });
        }
    };

    $.fn.grp_autocomplete_m2m = function(method) {
        if (methods[method]) {
            return methods[method].apply(this, Array.prototype.slice.call(arguments, 1));
        } else if (typeof method === 'object' || ! method) {
            return methods.init.apply(this, arguments);
        } else {
            $.error('Method ' +  method + ' does not exist on jQuery.grp_autocomplete_m2m');
        }
        return false;
    };

    var value_add = function(elem, value, options) {
        var values = [];
        if (elem.val()) values = elem.val().split(",");
        values.push(value);
        elem.val(values.join(","));
        elem.trigger('change');
        return values.join(",");
    };

    var value_remove = function(elem, position, options) {
        var values = [];
        if (elem.val()) values = elem.val().split(",");
        values.splice(position,1);
        elem.val(values.join(","));
        elem.trigger('change');
        return values.join(",");
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

    var repr_add = function(elem, label, options) {
        var repr = $('<li class="grp-repr"></li>');
        var removelink = $('<a class="grp-m2m-remove" href="javascript://"></a>').text(label);
        repr.append(removelink);
        repr.insertBefore(options.wrapper_search);
        removelink.on("click", function(e) { // remove-handler
            var pos = $(this).parent().parent().children("li").index($(this).parent());
            value_remove(elem, pos, options);
            $(this).parent().remove();
            elem.val() ? $(options.remove_link).show() : $(options.remove_link).hide();
            e.stopPropagation(); // prevent focus on input
        });
        removelink.hover(function() {
            $(this).parent().toggleClass("grp-autocomplete-preremove");
        });
    };

    var lookup_autocomplete = function(elem, options) {
        options.wrapper_search.find("input:first")
            .on("keydown", function(event) { // don't navigate away from the field on tab when selecting an item
                if (event.keyCode === $.ui.keyCode.TAB && $(this).data("uiAutocomplete").menu.active) {
                    event.preventDefault();
                }
            })
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
                position: {my: "left top", at: "left bottom", of: options.wrapper_autocomplete},
                open: function(event, ui) {
                    $(".ui-menu").width(options.wrapper_autocomplete.outerWidth()-6);
                },
                source: function(request, response) {
                    $.ajax({
                        url: options.autocomplete_lookup_url,
                        dataType: 'json',
                        data: "term=" + encodeURIComponent(request.term) + "&app_label=" + grappelli.get_app_label(elem) + "&model_name=" + grappelli.get_model_name(elem) + "&query_string=" + grappelli.get_query_string(elem),
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
                select: function(event, ui) { // add repr, add value
                    repr_add(elem, ui.item.label, options);
                    value_add(elem, ui.item.value, options);
                    elem.val() ? $(options.remove_link).show() : $(options.remove_link).hide();
                    $(this).val("").trigger("focus");
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
            model_name: grappelli.get_model_name(elem)
        }, function(data) {
            options.wrapper_repr.find("li.grp-repr").remove();
            options.wrapper_search.find("input").val("");
            $.each(data, function(index) {
                if (data[index].value) repr_add(elem, data[index].label, options);
            });
            elem.val() ? $(options.remove_link).show() : $(options.remove_link).hide();
        });
    };

})(grp.jQuery);
