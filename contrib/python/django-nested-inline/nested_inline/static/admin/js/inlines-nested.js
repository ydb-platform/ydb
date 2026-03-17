/**
 * Django admin inlines
 *
 * Based on jQuery Formset 1.1
 * @author Stanislaus Madueke (stan DOT madueke AT gmail DOT com)
 * @requires jQuery 1.2.6 or later
 *
 * Copyright (c) 2009, Stanislaus Madueke
 * All rights reserved.
 *
 * Spiced up with Code from Zain Memon's GSoC project 2009
 * and modified for Django by Jannis Leidel
 *
 * Licensed under the New BSD License
 * See: http://www.opensource.org/licenses/bsd-license.php
 */
(function($) {
    $.fn.formset = function(opts) {
        var options = $.extend({}, $.fn.formset.defaults, opts);
        var $this = $(this);
        var $parent = $this.parent();
        var updateElementIndex = function(el, prefix, ndx) {
            var id_regex = new RegExp("(" + prefix + "-(\\d+|__prefix__))");
            var replacement = prefix + "-" + ndx;
            if ($(el).attr("for")) {
                $(el).attr("for", $(el).attr("for").replace(id_regex, replacement));
            }
            if (el.id) {
                el.id = el.id.replace(id_regex, replacement);
            }
            if (el.name) {
                el.name = el.name.replace(id_regex, replacement);
            }
        };
        var nextIndex = get_no_forms(options.prefix);
        // Add form classes for dynamic behaviour
        $this.each(function(i) {
            $(this).not("." + options.emptyCssClass).addClass(options.formCssClass);
        });
        // Only show the add button if we are allowed to add more items,
        // note that max_num = None translates to a blank string.
        var showAddButton = get_max_forms(options.prefix) === '' || (get_max_forms(options.prefix) - get_no_forms(options.prefix)) > 0;
        if ($this.length && showAddButton) {
            var addButton;
            if ($this.prop("tagName") == "TR") {
                // If forms are laid out as table rows, insert the
                // "add" button in a new table row:
                var numCols = this.eq(-1).children().length;
                $parent.append('<tr class="' + options.addCssClass + '"><td colspan="' + numCols + '"><a href="javascript:void(0)">' + options.addText + "</a></tr>");
                addButton = $parent.find("tr:last a");
            } else {
                // Otherwise, insert it immediately after the last form:
                $this.filter(":last").after('<div class="' + options.addCssClass + '"><a href="javascript:void(0)">' + options.addText + "</a></div>");
                addButton = $this.filter(":last").next().find("a");
            }
            addButton.click(function(e) {
                e.preventDefault();
                var nextIndex = get_no_forms(options.prefix);
                var template = $("#" + options.prefix + "-empty");
                var row = template.clone(true);
                row.removeClass(options.emptyCssClass).addClass(options.formCssClass).attr("id", options.prefix + "-" + nextIndex);
                if (row.is("tr")) {
                    // If the forms are laid out in table rows, insert
                    // the remove button into the last table cell:
                    row.children(":last").append('<div><a class="' + options.deleteCssClass + '" href="javascript:void(0)">' + options.deleteText + "</a></div>");
                } else if (row.is("ul") || row.is("ol")) {
                    // If they're laid out as an ordered/unordered list,
                    // insert an <li> after the last list item:
                    row.append('<li><a class="' + options.deleteCssClass + '" href="javascript:void(0)">' + options.deleteText + "</a></li>");
                } else {
                    // Otherwise, just insert the remove button as the
                    // last child element of the form's container:
                    row.children(":first").append('<span><a class="' + options.deleteCssClass + '" href="javascript:void(0)">' + options.deleteText + "</a></span>");
                }
                row.find("*").each(function() {
                    updateElementIndex(this, options.prefix, nextIndex);
                });
                // when adding something from a cloned formset the id is the same

                // Insert the new form when it has been fully edited
                row.insertBefore($(template));
                // Update number of total forms
                change_no_forms(options.prefix, true);
                // Hide add button in case we've hit the max, except we want to add infinitely
                if ((get_max_forms(options.prefix) !== '') && (get_max_forms(options.prefix) - get_no_forms(options.prefix)) <= 0) {
                    addButton.parent().hide();
                }
                // The delete button of each row triggers a bunch of other things
                row.find("a." + options.deleteCssClass).click(function(e) {
                    e.preventDefault();
                    // Find the row that will be deleted by this button
                    var row = $(this).parents("." + options.formCssClass);
                    // Remove the parent form containing this button:
                    var formset_to_update = row.parent();
                    while (row.next().hasClass('nested-inline-row')) {
                        row.next().remove();
                    }
                    row.remove();
                    change_no_forms(options.prefix, false);
                    // If a post-delete callback was provided, call it with the deleted form:
                    if (options.removed) {
                        options.removed(formset_to_update);
                    }

                    $(document).trigger('formset:removed', [row, options.prefix]);
                });
                if (row.is("tr")) {
                    // If the forms are laid out in table rows, insert
                    // the remove button into the last table cell:
                    // Insert the nested formsets into the new form
                    nested_formsets = create_nested_formset(options.prefix, nextIndex, options, false);
                    if (nested_formsets.length) {
                        row.addClass("no-bottom-border");
                    }
                    nested_formsets.each(function() {
                        if (!$(this).next()) {
                            border_class = "";
                        } else {
                            border_class = " no-bottom-border";
                        }
                        ($('<tr class="nested-inline-row' + border_class + '">').html(($('<td>', {
                            colspan : '100%'
                        }).html($(this))))).insertBefore($(template));
                    });
                } else {
                    // stacked
                    // Insert the nested formsets into the new form
                    nested_formsets = create_nested_formset(options.prefix, nextIndex, options, true);
                    nested_formsets.each(function() {
                        row.append($(this));
                    });
                }
                // If a post-add callback was supplied, call it with the added form:
                if (options.added) {
                    options.added(row);
                }
                nextIndex = nextIndex + 1;

                $(document).trigger('formset:added', [row, options.prefix]);
            });
        }
        return this;
    };
    /* Setup plugin defaults */
    $.fn.formset.defaults = {
        prefix : "form", // The form prefix for your django formset
        addText : "add another", // Text for the add link
        deleteText : "remove", // Text for the delete link
        addCssClass : "add-row", // CSS class applied to the add link
        deleteCssClass : "delete-row", // CSS class applied to the delete link
        emptyCssClass : "empty-row", // CSS class applied to the empty row
        formCssClass : "dynamic-form", // CSS class applied to each form in a formset
        added : null, // Function called each time a new form is added
        removed : null // Function called each time a form is deleted
    };
    // Tabular inlines ---------------------------------------------------------
    $.fn.tabularFormset = function(options) {
        var $rows = $(this);
        var alternatingRows = function(row) {
            row_number = 0;
            $($rows.selector).not(".add-row").removeClass("row1 row2").each(function() {
                $(this).addClass('row' + ((row_number%2)+1));
                next = $(this).next();
                while (next.hasClass('nested-inline-row')) {
                    next.addClass('row' + ((row_number%2)+1));
                    next = next.next();
                }
                row_number = row_number + 1;
            });
        };
        var reinitDateTimeShortCuts = function() {
            // Reinitialize the calendar and clock widgets by force
            if ( typeof DateTimeShortcuts != "undefined") {
                $(".datetimeshortcuts").remove();
                DateTimeShortcuts.init();
            }
        };
        var updateSelectFilter = function() {
            // If any SelectFilter widgets are a part of the new form,
            // instantiate a new SelectFilter instance for it.
            if ( typeof SelectFilter != 'undefined') {
                $('.selectfilter').each(function(index, value) {
                    var namearr = value.name.split('-');
                    SelectFilter.init(value.id, namearr[namearr.length - 1], false, options.adminStaticPrefix);
                });
                $('.selectfilterstacked').each(function(index, value) {
                    var namearr = value.name.split('-');
                    SelectFilter.init(value.id, namearr[namearr.length - 1], true, options.adminStaticPrefix);
                });
            }
        };
        var initPrepopulatedFields = function(row) {
            row.find('.prepopulated_field').each(function() {
                var field = $(this), input = field.find('input, select, textarea'), dependency_list = input.data('dependency_list') || [], dependencies = [];
                $.each(dependency_list, function(i, field_name) {
                    dependencies.push('#' + row.find('.field-' + field_name).find('input, select, textarea').attr('id'));
                });
                if (dependencies.length) {
                    input.prepopulate(dependencies, input.attr('maxlength'));
                }
            });
        };

        $rows.formset({
            prefix : options.prefix,
            addText : options.addText,
            formCssClass : "dynamic-" + options.prefix,
            deleteCssClass : "inline-deletelink",
            deleteText : options.deleteText,
            emptyCssClass : "empty-form",
            removed : alternatingRows,
            added : function(row) {
                initPrepopulatedFields(row);
                reinitDateTimeShortCuts();
                updateSelectFilter();
                alternatingRows(row);
            }
        });

        return $rows;
    };

    // Stacked inlines ---------------------------------------------------------
    $.fn.stackedFormset = function(options) {
        var $rows = $(this);

        var update_inline_labels = function(formset_to_update) {
            formset_to_update.children('.inline-related').not('.empty-form').children('h3').find('.inline_label').each(function(i) {
                var count = i + 1;
                $(this).html($(this).html().replace(/(#\d+)/g, "#" + count));
            });
        };

        var reinitDateTimeShortCuts = function() {
            // Reinitialize the calendar and clock widgets by force, yuck.
            if ( typeof DateTimeShortcuts != "undefined") {
                $(".datetimeshortcuts").remove();
                DateTimeShortcuts.init();
            }
        };

        var updateSelectFilter = function() {
            // If any SelectFilter widgets were added, instantiate a new instance.
            if ( typeof SelectFilter != "undefined") {
                $(".selectfilter").each(function(index, value) {
                    var namearr = value.name.split('-');
                    SelectFilter.init(value.id, namearr[namearr.length - 1], false, options.adminStaticPrefix);
                });
                $(".selectfilterstacked").each(function(index, value) {
                    var namearr = value.name.split('-');
                    SelectFilter.init(value.id, namearr[namearr.length - 1], true, options.adminStaticPrefix);
                });
            }
        };

        var initPrepopulatedFields = function(row) {
            row.find('.prepopulated_field').each(function() {
                var field = $(this), input = field.find('input, select, textarea'), dependency_list = input.data('dependency_list') || [], dependencies = [];
                $.each(dependency_list, function(i, field_name) {
                    dependencies.push('#' + row.find('.form-row .field-' + field_name).find('input, select, textarea').attr('id'));
                });
                if (dependencies.length) {
                    input.prepopulate(dependencies, input.attr('maxlength'));
                }
            });
        };

        $rows.formset({
            prefix : options.prefix,
            addText : options.addText,
            formCssClass : "dynamic-" + options.prefix,
            deleteCssClass : "inline-deletelink",
            deleteText : options.deleteText,
            emptyCssClass : "empty-form",
            removed : update_inline_labels,
            added : (function(row) {
                initPrepopulatedFields(row);
                reinitDateTimeShortCuts();
                updateSelectFilter();
                update_inline_labels(row.parent());
            })
        });

        return $rows;
    };

    function create_nested_formset(parent_formset_prefix, next_form_id, options, add_bottom_border) {
        var formsets = $(false);
        // update options
        // Normalize prefix to something we can rely on
        var normalized_parent_formset_prefix = parent_formset_prefix.replace(/[-][0-9][-]/g, "-0-");
        // Check if the form should have nested formsets
        var nested_inlines = $('#' + normalized_parent_formset_prefix + "-group ." + normalized_parent_formset_prefix + "-0-nested-inline").not('.cloned');
        if (!nested_inlines.length) {
          nested_inlines = $('#' + normalized_parent_formset_prefix + "-group ." + normalized_parent_formset_prefix + "-nested-inline").not('.cloned');
        }
        nested_inlines.each(function(key, formsetToClone) {
            // prefixes for the nested formset
            var normalized_formset_prefix = $(this).attr('id').split('-group')[0];
            var formset_prefix = normalized_formset_prefix.replace(normalized_parent_formset_prefix + "-0", parent_formset_prefix + "-" + next_form_id);
            // Find the normalized formset and clone it
            // I have changed the line below which seems to work better
            //var template = $("#" + normalized_formset_prefix + "-group").clone();
            var template = $(formsetToClone).clone();
            template.addClass('cloned');
            if (template.children().first().hasClass('tabular')) {
                // Template is tabular
                template.find(".form-row").not(".empty-form").remove();
                template.find(".nested-inline-row").remove();
                // Make a new form
                template_form = template.find("#" + normalized_formset_prefix + "-empty")
                new_form = template_form.clone().removeClass(options.emptyCssClass).addClass("dynamic-" + formset_prefix);

                new_form.insertBefore(template_form);

                var inputs = new_form.find('input');
                inputs.each(function () {
                    var $input = $(this)
                    if ($input.val()) {
                        $input.removeAttr('value')
                    }
                });

                // Update Form Properties
                update_props(template, normalized_formset_prefix, formset_prefix);
                template.find('#id_' + formset_prefix + '-TOTAL_FORMS').val(1);
                template.find('#id_' + formset_prefix + '-INITIAL_FORMS').val(0);
                var add_text = template.find('.add-row').text();
                template.find('.add-row').remove();
                template.find('.tabular.inline-related tbody tr.' + formset_prefix + '-not-nested').tabularFormset({
                    prefix : formset_prefix,
                    adminStaticPrefix : options.adminStaticPrefix,
                    addText : add_text,
                    deleteText : options.deleteText
                });
                // Create the nested formset
                var nested_formsets = create_nested_formset(formset_prefix, 0, options, false);
                if (nested_formsets.length) {
                    template.find(".form-row").addClass('no-bottom-border');
                }
                // Insert nested formsets
                nested_formsets.each(function() {
                    if (!$(this).next()) {
                        border_class = "";
                    } else {
                        border_class = " no-bottom-border";
                    }
                    template.find("#" + formset_prefix + "-empty").before(($('<tr class="nested-inline-row' + border_class + '">').html(($('<td>', {
                        colspan : '100%'
                    }).html($(this))))));
                });
            } else {
                // Template is stacked
                // Create the nested formset
                var nested_formsets = create_nested_formset(formset_prefix, 0, options, true);
                template.find(".inline-related").not(".empty-form").remove();
                // Make a new form
                template_form = template.find("#" + normalized_formset_prefix + "-empty")
                new_form = template_form.clone().removeClass(options.emptyCssClass).addClass("dynamic-" + formset_prefix);

                new_form.insertBefore(template_form);

                var inputs = new_form.find('input');
                inputs.each(function () {
                    var $input = $(this)
                    if ($input.val()) {
                        $input.removeAttr('value')
                    }
                });

                // Update Form Properties
                new_form.find('.inline_label').text('#1');
                update_props(template, normalized_formset_prefix, formset_prefix);
                template.find('#id_' + formset_prefix + '-TOTAL_FORMS').val(1);
                template.find('#id_' + formset_prefix + '-INITIAL_FORMS').val(0);

                var add_text = template.find('.add-row').text();
                template.find('.add-row').remove();
                template.find(".inline-related").stackedFormset({
                    prefix : formset_prefix,
                    adminStaticPrefix : options.adminStaticPrefix,
                    addText : add_text,
                    deleteText : options.deleteText
                });
                nested_formsets.each(function() {
                    new_form.append($(this));
                });
            }
            if (add_bottom_border) {
                template = template.add($('<div class="nested-inline-bottom-border">'));
            }
            if (formsets.length) {
                formsets = formsets.add(template);
            } else {
                formsets = template;
            }
        });
        return formsets;
    };

    function update_props(template, normalized_formset_prefix, formset_prefix) {
        // Fix template id
        template.attr('id', template.attr('id').replace(normalized_formset_prefix, formset_prefix));
        template.find('*').each(function() {
            if ($(this).attr("for")) {
                $(this).attr("for", $(this).attr("for").replace(normalized_formset_prefix, formset_prefix));
            }
            if ($(this).attr("class")) {
                $(this).attr("class", $(this).attr("class").replace(normalized_formset_prefix, formset_prefix));
            }
            if (this.id) {
                this.id = this.id.replace(normalized_formset_prefix, formset_prefix);
            }
            if (this.name) {
                this.name = this.name.replace(normalized_formset_prefix, formset_prefix);
            }
        });
        // fix __prefix__ where needed
        prefix_fix = template.find(".inline-related").first();

        nextIndex = get_no_forms(formset_prefix);
        if (prefix_fix.hasClass('tabular')) {
            // tabular
            prefix_fix = prefix_fix.find('.form-row').first();
            prefix_fix.attr('id', prefix_fix.attr('id').replace('-empty', '-' + nextIndex));
        } else {
            // stacked
            prefix_fix.attr('id', prefix_fix.attr('id').replace('-empty', '-' + nextIndex));
        }
        prefix_fix.find('*').each(function() {
            if ($(this).attr("for")) {
                $(this).attr("for", $(this).attr("for").replace('__prefix__', '0'));
            }
            if ($(this).attr("class")) {
                $(this).attr("class", $(this).attr("class").replace('__prefix__', '0'));
            }
            if (this.id) {
                this.id = this.id.replace('__prefix__', '0');
            }
            if (this.name) {
                this.name = this.name.replace('__prefix__', '0');
            }
        });
    };

    // This returns the amount of forms in the given formset
    function get_no_forms(formset_prefix) {
        formset_prop = $("#id_" + formset_prefix + "-TOTAL_FORMS")
        if (!formset_prop.length) {
            return 0;
        }
        return parseInt(formset_prop.attr("autocomplete", "off").val());
    }

    function change_no_forms(formset_prefix, increase) {
        var no_forms = get_no_forms(formset_prefix);
        if (increase) {
            $("#id_" + formset_prefix + "-TOTAL_FORMS").attr("autocomplete", "off").val(parseInt(no_forms) + 1);
        } else {
            $("#id_" + formset_prefix + "-TOTAL_FORMS").attr("autocomplete", "off").val(parseInt(no_forms) - 1);
        }
    };

    // This return the maximum amount of forms in the given formset
    function get_max_forms(formset_prefix) {
        var max_forms = $("#id_" + formset_prefix + "-MAX_NUM_FORMS").attr("autocomplete", "off").val();
        if ( typeof max_forms == 'undefined') {
            return '';
        }
        return parseInt(max_forms);
    };
})(django.jQuery);


// TODO:
// Remove border between tabular fieldset and nested inline
// Fix alternating rows
