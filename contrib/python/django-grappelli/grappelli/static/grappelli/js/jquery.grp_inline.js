/**
 * GRAPPELLI INLINES
 * jquery-plugin for inlines (stacked and tabular)
 */


(function($) {
    $.fn.grp_inline = function(options) {
        var defaults = {
            prefix: "form",                         // The form prefix for your django formset
            addText: "add another",                 // Text for the add link
            deleteText: "remove",                   // Text for the delete link
            addCssClass: "grp-add-handler",             // CSS class applied to the add link
            removeCssClass: "grp-remove-handler",       // CSS class applied to the remove link
            deleteCssClass: "grp-delete-handler",       // CSS class applied to the delete link
            emptyCssClass: "grp-empty-form",            // CSS class applied to the empty row
            formCssClass: "grp-dynamic-form",           // CSS class applied to each form in a formset
            predeleteCssClass: "grp-predelete",
            onBeforeInit: function(form) {},        // Function called before a form is initialized
            onBeforeAdded: function(inline) {},     // Function called before a form is added
            onBeforeRemoved: function(form) {},     // Function called before a form is removed
            onBeforeDeleted: function(form) {},     // Function called before a form is deleted
            onAfterInit: function(form) {},         // Function called after a form has been initialized
            onAfterAdded: function(form) {},        // Function called after a form has been added
            onAfterRemoved: function(inline) {},    // Function called after a form has been removed
            onAfterDeleted: function(form) {}       // Function called after a form has been deleted
        };
        options = $.extend(defaults, options);

        return this.each(function() {
            var inline = $(this); // the current inline node
            var totalForms = inline.find("#id_" + options.prefix + "-TOTAL_FORMS");
            // set autocomplete to off in order to prevent the browser from keeping the current value after reload
            totalForms.attr("autocomplete", "off");
            // init inline and add-buttons
            initInlineForms(inline, options);
            initAddButtons(inline, options);
            // button handlers
            addButtonHandler(inline.find("a." + options.addCssClass), options);
            removeButtonHandler(inline.find("a." + options.removeCssClass), options);
            deleteButtonHandler(inline.find("a." + options.deleteCssClass), options);
        });
    };

    getFormIndex = function(elem, options, regex) {
        var formIndex = elem.find("[id^='id_" + options.prefix + "']").attr('id');
        if (!formIndex) { return -1; }
        return parseInt(regex.exec(formIndex)[1], 10);
    };

    updateFormIndex = function(elem, options, replace_regex, replace_with) {
        elem.find(':input,span,table,iframe,label,a,ul,p,img,div').each(function() {
            var node = $(this),
                node_id = node.attr('id'),
                node_name = node.attr('name'),
                node_for = node.attr('for'),
                node_href = node.attr("href"),
                node_class = node.attr("class"),
                node_onclick = node.attr("onclick");
            if (node_id) { node.attr('id', node_id.replace(replace_regex, replace_with)); }
            if (node_name) { node.attr('name', node_name.replace(replace_regex, replace_with)); }
            if (node_for) { node.attr('for', node_for.replace(replace_regex, replace_with)); }
            if (node_href) { node.attr('href', node_href.replace(replace_regex, replace_with)); }
            if (node_class) { node.attr('class', node_class.replace(replace_regex, replace_with)); }
            if (node_onclick) { node.attr('onclick', node_onclick.replace(replace_regex, replace_with)); }
        });
        // update prepopulate ids for function initPrepopulatedFields
        elem.find('.prepopulated_field').each(function() {
            var dependency_ids = $(this).data('dependency_ids') || [],
                dependency_ids_updated = [];
            $.each(dependency_ids, function(i, id) {
                dependency_ids_updated.push(id.replace(replace_regex, replace_with));
            });
            $(this).data('dependency_ids', dependency_ids_updated);
        });
    };

    var initPrepopulatedFields = function(elem, options) {
        elem.find('.prepopulated_field').each(function() {
            var dependency_ids = $(this).data('dependency_ids') || [];
            $(this).prepopulate(dependency_ids, $(this).attr('maxlength'));
        });
    };

    initInlineForms = function(elem, options) {
        elem.find("div.grp-module").each(function() {
            var form = $(this);
            // callback
            options.onBeforeInit(form);
            // add options.formCssClass to all forms in the inline
            // except table/theader/add-item
            if (form.attr('id') !== "") {
                form.not("." + options.emptyCssClass).not(".grp-table").not(".grp-thead").not(".add-item").addClass(options.formCssClass);
            }
            // add options.predeleteCssClass to forms with the delete checkbox checked
            form.find("li.grp-delete-handler-container input").each(function() {
                if ($(this).is(":checked") && form.hasClass("has_original")) {
                    form.toggleClass(options.predeleteCssClass);
                }
            });
            // callback
            options.onAfterInit(form);
        });
    };

    initAddButtons = function(elem, options) {
        var totalForms = elem.find("#id_" + options.prefix + "-TOTAL_FORMS");
        var maxForms = elem.find("#id_" + options.prefix + "-MAX_NUM_FORMS");
        var addButtons = elem.find("a." + options.addCssClass);
        // hide add button in case we've hit the max, except we want to add infinitely
        if ((maxForms.val() !== '') && (maxForms.val()-totalForms.val()) <= 0) {
            hideAddButtons(elem, options);
        }
    };

    addButtonHandler = function(elem, options) {
        elem.on("click", function(e) {
            var inline = elem.parents(".grp-group"),
                totalForms = inline.find("#id_" + options.prefix + "-TOTAL_FORMS"),
                maxForms = inline.find("#id_" + options.prefix + "-MAX_NUM_FORMS"),
                addButtons = inline.find("a." + options.addCssClass),
                empty_template = inline.find("#" + options.prefix + "-empty");
            // callback
            options.onBeforeAdded(inline);
            // create new form
            var index = parseInt(totalForms.val(), 10),
                form = empty_template.clone(true);
            form.removeClass(options.emptyCssClass)
                .attr("id", empty_template.attr('id').replace("-empty", index));
            // update form index
            var re = /__prefix__/g;
            updateFormIndex(form, options, re, index);
            // after "__prefix__" strings has been substituted with the number
            // of the inline, we can add the form to DOM, not earlier.
            // This way we can support handlers that track live element
            // adding/removing, like those used in django-autocomplete-light
            form.insertBefore(empty_template)
                .addClass(options.formCssClass);
            // update total forms
            totalForms.val(index + 1);
            // hide add button in case we've hit the max, except we want to add infinitely
            if ((maxForms.val() !== 0) && (maxForms.val() !== "") && (maxForms.val() - totalForms.val()) <= 0) {
                hideAddButtons(inline, options);
            }
            // prepopulate fields
            initPrepopulatedFields(form, options);
            // select2: we need to use the django namespace here
            django.jQuery(document).trigger('formset:added', [django.jQuery(form), options.prefix]);
            form.get(0).dispatchEvent(new CustomEvent("formset:added", {
                bubbles: true,
                detail: {
                    formsetName: options.prefix
                }
            }));
            // callback
            options.onAfterAdded(form);
        });
    };

    removeButtonHandler = function(elem, options) {
        elem.on("click", function() {
            var inline = elem.parents(".grp-group"),
                form = $(this).parents("." + options.formCssClass).first(),
                totalForms = inline.find("#id_" + options.prefix + "-TOTAL_FORMS"),
                maxForms = inline.find("#id_" + options.prefix + "-MAX_NUM_FORMS"),
                re = /-(\d+)-/,
                removedFormIndex = getFormIndex(form, options, re);
            // callback
            options.onBeforeRemoved(form);
            // remove form
            form.remove();
            // update total forms
            totalForms.val(parseInt(totalForms.val(), 10) - 1);
            // show add button in case we've dropped below max
            if ((maxForms.val() !== 0) && (maxForms.val() - totalForms.val()) > 0) {
                showAddButtons(inline, options);
            }
            // update form index (only forms with a higher index than the removed form)
            inline.find("." + options.formCssClass).each(function() {
                var form = $(this),
                    formIndex = getFormIndex(form, options, re);
                if (formIndex > removedFormIndex) {
                    updateFormIndex(form, options, re, "-" + (formIndex - 1) + "-");
                }
            });
            // callback
            options.onAfterRemoved(inline);
        });
    };

    deleteButtonHandler = function(elem, options) {
        elem.on("click", function() {
            var deleteInput = $(this).prev(),
                form = $(this).parents("." + options.formCssClass).first();
            // callback
            options.onBeforeDeleted(form);
            // toggle options.predeleteCssClass and toggle checkbox
            if (form.hasClass("has_original")) {
                form.toggleClass(options.predeleteCssClass);
                if (deleteInput.prop("checked")) {
                    deleteInput.removeAttr("checked");
                } else {
                    deleteInput.prop("checked", true);
                }
            }
            // callback
            options.onAfterDeleted(form);
        });
    };

    hideAddButtons = function(elem, options) {
        var addButtons = elem.find("a." + options.addCssClass);
        addButtons.hide().parents('.grp-add-item').hide();
        // last row with stacked/tabular
        addButtons.closest('.grp-module.grp-transparent').hide();
    };

    showAddButtons = function(elem, options) {
        var addButtons = elem.find("a." + options.addCssClass);
        addButtons.show().parents('.grp-add-item').show();
        // last row with stacked/tabular
        addButtons.closest('.grp-module.grp-transparent').show();
    };

})(grp.jQuery);
