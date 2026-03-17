var DjangoSelect2 = window.DjangoSelect2 || {};

(function() {
    var $ = DjangoSelect2.jQuery;
    if (!$) {
        $ = DjangoSelect2.jQuery = (window.django || {}).jQuery || window.jQuery;
    }

    DjangoSelect2.init = function(input) {
        var $input = $(input);
        var ajaxOptions = {
            ajax: {
                data: function(term, page) {
                    return {
                        q: term,
                        page: page,
                        page_limit: 10
                    };
                },
                results: function(data, page) {
                    return data;
                }
            },
            initSelection: function (element, callback) {
                var inputVal = $input.val();
                if (!inputVal) {
                    return;
                }
                var data = {
                    q: inputVal
                };
                var select2 = $input.data('select2') || {};
                if (typeof select2.opts == 'object' && select2.opts !== null) {
                    data.multiple = (select2.opts.multiple) ? 1 : 0;
                }
                $.ajax({
                    url: $input.data('initSelectionUrl'),
                    dataType: 'json',
                    data: data,
                    success: function(data) {
                        if (typeof(data) == 'object' && typeof(data.results) == 'object' && data.results) {
                            callback(data.results);
                        }
                    }
                });
            }
        };
        var options =  $input.data('select2Options') || {};
        if (typeof options.ajax == 'object') {
            options = $.extend(true, ajaxOptions, options);
        }
        $input.select2(options);
        if ($input.data('sortable') && typeof $.fn.djs2sortable == 'function') {
            $input.select2("container").find("ul.select2-choices").djs2sortable({
                containment: 'parent',
                start: function() { $input.select2("onSortStart"); },
                update: function() { $input.select2("onSortEnd"); }
            });
        }
        $input.data('select2').selection.addClass('vTextField');
    };

    $(document).ready(function() {
        $('.django-select2:not([name*="__prefix__"])').each(function() {
            DjangoSelect2.init(this);
        });
        $(document).on('formset:added', function(event, $form) {
            if (typeof $form === "undefined") {
                $form = $(event.target);
            }
            $form.find('.django-select2:not([name*="__prefix__"])').each(function() {
                DjangoSelect2.init(this);
            });
        });
    });
})();
