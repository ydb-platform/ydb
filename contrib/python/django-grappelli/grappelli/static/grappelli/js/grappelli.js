/**
 * GRAPPELLI UTILS
 * functions needed for Grappelli
 */

// grp jQuery namespace
var grp = {
    jQuery: jQuery.noConflict(true)
};

// django jQuery namespace
var django = {
    jQuery: grp.jQuery
};

// general jQuery instance
var jQuery = grp.jQuery;

var inputTypes = [
    "[type='search']",
    "[type='email']",
    "[type='url']",
    "[type='tel']",
    "[type='number']",
    "[type='range']",
    "[type='date']",
    "[type='month']",
    "[type='week']",
    "[type='time']",
    "[type='datetime']",
    "[type='datetime-local']",
    "[type='color']"].join(",");

(function($) {

    // dateformat
    grappelli.getFormat = function(type) {
        if (type == "date") {
            var format = DATE_FORMAT.toLowerCase().replace(/%\w/g, function(str) {
                str = str.replace(/%/, '');
                return str + str;
            });
            return format;
        }
    };

    // remove types: search, email, url, tel, number, range, date
    // month, week, time, datetime, datetime-local, color
    // because of browser inconsistencies
    /*jshint multistr: true */
    grappelli.cleanInputTypes = function() {
        $("form").each(function(){
            $(this).find(':input').filter(inputTypes).each(function(){
                $(this).attr("type", "text");
            });
        });
    };

    // datepicker, timepicker init
    grappelli.initDateAndTimePicker = function() {

        // HACK: get rid of text after DateField (hardcoded in django.admin)
        $('p.datetime').each(function() {
            var text = $(this).html();
            text = text.replace(/\w*: /, "");
            text = text.replace(/<br>[^<]*: /g, "<br>");
            $(this).html(text);
        });

        $('span.datetimeshortcuts').each(function() {
            var text = $(this).html();
            text = text.replace(/\w*: /, "");
            text = text.replace(/<br>[^<]*: /g, "<br>");
            $(this).html(text);
        });

        var options = {
            //appendText: '(mm/dd/yyyy)',
            constrainInput: false,
            showOn: 'button',
            buttonImageOnly: false,
            buttonText: '',
            dateFormat: grappelli.getFormat('date'),
            showButtonPanel: true,
            showAnim: '',
            // HACK: sets the current instance to a global var.
            // needed to actually select today if the today-button is clicked.
            // see onClick handler for ".ui-datepicker-current"
            beforeShow: function(year, month, inst) {
                grappelli.datepicker_instance = this;
            }
        };
        var dateFields = $("input[class*='vDateField']:not([id*='__prefix__'])");
        dateFields.datepicker(options);

        if (typeof IS_POPUP != "undefined" && IS_POPUP) {
            dateFields.datepicker('disable');
        }

        // HACK: adds an event listener to the today button of datepicker
        // if clicked today gets selected and datepicker hides.
        // use on() because couldn't find hook after datepicker generates it's complete dom.
        $(document).on('click', '.ui-datepicker-current', function() {
            $.datepicker._selectDate(grappelli.datepicker_instance);
            grappelli.datepicker_instance = null;
        });

        // init timepicker
        $("input[class*='vTimeField']:not([id*='__prefix__'])").grp_timepicker();

    };

    // changelist: filter
    grappelli.initFilter = function(method) {
        $("a.grp-pulldown-handler").on("click", function() {
            var pulldownContainer = $(this).closest(".grp-pulldown-container");
            $(pulldownContainer).toggleClass("grp-pulldown-state-open").children(".grp-pulldown-content").toggle();
        });
        $("a.grp-pulldown-handler").on('mouseout', function() {
            $(this).blur();
        });
        if (!method) {
            $(".grp-filter-choice").change(function(){
                location.href = $(this).val();
            });
        }
        if (method === 'confirm') {
            // Construct windowQueryDict from current window.location.search
            var windowQuery = window.location.search.replace('?', '').split('&');
            var windowQueryDict = [];
            if (windowQuery[0] !== undefined && windowQuery[0] !== '') {
                windowQuery.map(param => {
                    // Split query param to get the fieldName
                    var fieldName = param.split('=')[0];
                    if (fieldName.search('__') != -1) {
                        fieldName = param.split('__')[0];
                    }
                    // Check if fieldName already exists in searchStringDict and add it resp. its values
                    var fieldNameIndex = windowQueryDict.findIndex(el => el.fieldName === fieldName);
                    if (fieldNameIndex === -1) {
                        windowQueryDict.push({
                            fieldName: fieldName,
                            values: [param]
                        });
                    } else {
                        windowQueryDict.find(obj => obj.fieldName === fieldName).values.push(param);
                    }
                });
            }
            // Manipulate windowQueryDict based on changes of filter choices
            $(".grp-filter-choice").change(function(){
                // Get the choice's fieldName and isolate its distinctive query params
                var fieldName = $(this).data('field-name');
                var value = $(this).val() !== '?' ? $(this).val().replace('?', '') : false;
                var values = value && value.split('&');
                var filterQueryParams = values && values.filter(el => el.includes(fieldName));
                // Check if fieldName already exists in filterQueryDict and add it resp. its values
                var filterWindowIndex = windowQueryDict.findIndex(el => el.fieldName === fieldName);
                var isFilterPartOfWindow = filterWindowIndex < 0 ? false : true;
                if (isFilterPartOfWindow) {
                    if (filterQueryParams.length > 0) {
                        // Update query params
                        windowQueryDict.find(el => el.fieldName === fieldName).values = filterQueryParams;
                    } else {
                        // Remove filter
                        windowQueryDict.splice(filterWindowIndex, 1);
                    }
                } else {
                    if (filterQueryParams.length > 0) {
                        // Add filter
                        windowQueryDict.push({
                            fieldName: fieldName,
                            values: filterQueryParams,
                        });
                    }
                }
                // Construct queryString from windowQueryDict
                var queryString = windowQueryDict.flatMap(el => el.values).join('&');
                // Assiqn query string to "Apply" button
                var applyFilter = $(this).closest('.grp-filter').find('#grp-filter-apply');
                applyFilter.attr('href', '?' + queryString);
            });
        }
    };

    // changelist: searchbar
    grappelli.initSearchbar = function() {
        var searchbar = $("input.grp-search-field");
        searchbar.trigger("focus");
    };

    grappelli.updateSelectFilter = function(form) {
        if (typeof SelectFilter != "undefined"){
            form.find(".selectfilter").each(function(index, value){
                var namearr = value.name.split('-');
                SelectFilter.init(value.id, namearr[namearr.length-1], false, "{% admin_media_prefix %}");
            });
            form.find(".selectfilterstacked").each(function(index, value){
                var namearr = value.name.split('-');
                SelectFilter.init(value.id, namearr[namearr.length-1], true, "{% admin_media_prefix %}");
            });
        }
    };

    grappelli.reinitDateTimeFields = function(form) {
        form.find(".vDateField").datepicker({
            constrainInput: false,
            showOn: 'button',
            buttonImageOnly: false,
            buttonText: '',
            dateFormat: grappelli.getFormat('date')
        });
        form.find(".vTimeField").grp_timepicker();
    };

    // autocomplete helpers
    grappelli.get_app_label = function(elem) {
        var link = elem.next("a");
        if (link.length > 0) {
            var url = link.attr('href').split('?')[0].split('/');
            return url[url.length-3];
        }
        return false;
    };
    grappelli.get_model_name = function(elem) {
        var link = elem.next("a");
        if (link.length > 0) {
            var url = link.attr('href').split('?')[0].split('/');
            return url[url.length-2];
        }
        return false;
    };
    grappelli.get_query_string = function(elem) {
        var link = elem.next("a");
        if (link.length > 0) {
            var url = link.attr('href').split('/');
            pairs = url[url.length-1].replace('?', '').split("&");
            return pairs.join(":");
        }
        return false;
    };
    grappelli.get_to_field = function(elem) {
        var link = elem.next("a");
        if (link.length > 0 && link.attr('href').indexOf("_to_field") !== -1) {
            var url = link.attr('href').split('/');
            var pairs = url[url.length-1].replace('?', '').split("&");
            for (var i = 0; i < pairs.length; i++) {
                v = pairs[i].split('=');
                if (v[0] == "_to_field") {
                    return v[1];
                }
            }
        }
        return false;
    };

})(grp.jQuery);
