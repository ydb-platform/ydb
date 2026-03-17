if (typeof jQuery === 'undefined') {
    var jQuery = django.jQuery;
}

(function ($) {
    $(function () {
        $('.sortedm2m-container').find('.sortedm2m-items').addClass('hide');
        function prepareUl(ul) {
            ul.addClass('sortedm2m');
            var checkboxes = ul.find('input[type=checkbox]');
            var id;
            var name;

            if (checkboxes.length) {
                id = checkboxes.first().attr('id').match(/^(.*)_\d+$/)[1];
                name = checkboxes.first().attr('name');
                checkboxes.removeAttr('name');
            } else {
                var label, labelFor;
                var currentElement = ul;

                // Look for a <label> that is a sibling of an
                // ancestor, if there is one, extract the for
                // attribute as name variable
                while (currentElement[0].tagName != "BODY") {
                    currentElement = currentElement.parent();
                    label = currentElement.siblings('label');
                    if(label) {
                        labelFor = label.attr('for');
                        if (!labelFor) {
                            break;
                        }
                        id = labelFor.match(/^(.*)_\d+$/)[1];
                        name = id.replace(/^id_/, '');
                        // we found the label, and a name!
                        break;
                    }
                }

            }
            if (name) {
                ul.before('<input type="hidden" id="' + id + '" name="' + name + '" />');
            } else {
                ul.before('<input type="hidden" id="' + id + '" />');
            }

            var recalculate_value = function () {
                var values = [];
                ul.find(':checked').each(function () {
                    values.push($(this).val());
                });
                $('#' + id).val(values.join(','));
            };
            recalculate_value();
            ul.on('change','input[type=checkbox]',recalculate_value);
            ul.sortable({
                axis: 'y',
                //containment: 'parent',
                update: recalculate_value
            });
            return ul;
        }

        function iterateUl() {
            $('.sortedm2m-items').each(function () {
                var ul = $(this);

                prepareUl(ul);
                ul.removeClass('hide');
            });
        }

        $(".add-row a").click(iterateUl);

        iterateUl();

        $('.sortedm2m-container .selector-filter input').each(function () {
            $(this).bind('input', function() {
                var search = $(this).val().toLowerCase();
                var $el = $(this).closest('.selector-filter');
                var $container = $el.siblings('.sortedm2m-items').each(function() {
                    // walk over each child list el and do name comparisons
                    $(this).children().each(function() {
                        var curr = $(this).find('label').text().toLowerCase();
                        if (curr.indexOf(search) === -1) {
                            $(this).css('display', 'none');
                        } else {
                            $(this).css('display', 'inherit');
                        }
                    });
                });
            });
        });

        var dismissPopupFnName = 'dismissAddAnotherPopup';
        // django 1.8+
        if (window.dismissAddRelatedObjectPopup) {
            dismissPopupFnName = 'dismissAddRelatedObjectPopup';
        }

        function html_unescape(text) {
            // Unescape a string that was escaped using django.utils.html.escape.
            text = text.replace(/&lt;/g, '<');
            text = text.replace(/&gt;/g, '>');
            text = text.replace(/&quot;/g, '"');
            text = text.replace(/&#39;/g, "'");
            text = text.replace(/&amp;/g, '&');
            return text;
        }

        function windowname_to_id(text) {
            // django32 has removed windowname_to_id function.
            text = text.replace(/__dot__/g, '.');
            text = text.replace(/__dash__/g, '-');
            return text;
        }

        if (window.showAddAnotherPopup) {
            var django_dismissAddAnotherPopup = window[dismissPopupFnName];
            window[dismissPopupFnName] = function (win, newId, newRepr) {
                // newId and newRepr are expected to have previously been escaped by
                // django.utils.html.escape.
                newId = html_unescape(newId);
                newRepr = html_unescape(newRepr);
                var name = windowname_to_id(win.name);
                var elem = $('#' + name);
                var sortedm2m = elem.siblings('.sortedm2m-items.sortedm2m');
                if (sortedm2m.length == 0) {
                    // no sortedm2m widget, fall back to django's default
                    // behaviour
                    return django_dismissAddAnotherPopup.apply(this, arguments);
                }

                if (elem.val().length > 0) {
                    elem.val(elem.val() + ',');
                }
                elem.val(elem.val() + newId);

                var id_template = '';
                var maxid = 0;
                sortedm2m.find('.sortedm2m-item input').each(function () {
                    var match = this.id.match(/^(.+)_(\d+)$/);
                    id_template = match[1];
                    id = parseInt(match[2]);
                    if (id > maxid) maxid = id;
                });

                var id = id_template + '_' + (maxid + 1);
                var new_li = $('<li/>').append(
                    $('<label/>').attr('for', id).append(
                        $('<input class="sortedm2m" type="checkbox" checked="checked" />').attr('id', id).val(newId)
                    ).append($('<span/>').text(' ' + newRepr))
                );
                sortedm2m.append(new_li);

                win.close();
            };
        }
    });
})(jQuery);
