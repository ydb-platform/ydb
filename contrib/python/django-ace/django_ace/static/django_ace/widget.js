(function() {
    function getDocHeight() {
        var D = document;
        return Math.max(
            Math.max(D.body.scrollHeight, D.documentElement.scrollHeight),
            Math.max(D.body.offsetHeight, D.documentElement.offsetHeight),
            Math.max(D.body.clientHeight, D.documentElement.clientHeight)
        );
    }

    function getDocWidth() {
        var D = document;
        return Math.max(
            Math.max(D.body.scrollWidth, D.documentElement.scrollWidth),
            Math.max(D.body.offsetWidth, D.documentElement.offsetWidth),
            Math.max(D.body.clientWidth, D.documentElement.clientWidth)
        );
    }

    function next(elem) {
        // Credit to John Resig for this function
        // taken from Pro JavaScript techniques
        do {
            elem = elem.nextSibling;
        } while (elem && elem.nodeType != 1);
        return elem;
    }

    function prev(elem) {
        // Credit to John Resig for this function
        // taken from Pro JavaScript techniques
        do {
            elem = elem.previousSibling;
        } while (elem && elem.nodeType != 1);
        return elem;
    }

    function redraw(element){
        element = $(element);
        var n = document.createTextNode(' ');
        element.appendChild(n);
        (function(){n.parentNode.removeChild(n)}).defer();
        return element;
    }

    function minimizeMaximize(widget, main_block, editor) {
        if (window.fullscreen == true) {
            main_block.className = 'django-ace-editor';

            widget.style.width = window.ace_widget.width + 'px';
            widget.style.height = window.ace_widget.height + 'px';
            widget.style.zIndex = 1;
            window.fullscreen = false;
        }
        else {
            window.ace_widget = {
                'width': widget.offsetWidth,
                'height': widget.offsetHeight,
            }

            main_block.className = 'django-ace-editor-fullscreen';

            widget.style.height = getDocHeight() + 'px';
            widget.style.width = getDocWidth() + 'px';
            widget.style.zIndex = 999;

            window.scrollTo(0, 0);
            window.fullscreen = true;
            editor.resize();
        }
    }

    function apply_widget(widget) {
        var div = widget.firstChild,
            textarea = next(widget),
            mode = widget.getAttribute('data-mode'),
            theme = widget.getAttribute('data-theme'),
            wordwrap = widget.getAttribute('data-wordwrap'),
            minlines = widget.getAttribute('data-minlines'),
            maxlines = widget.getAttribute('data-maxlines'),
            showprintmargin = widget.getAttribute('data-showprintmargin'),
            showinvisibles = widget.getAttribute('data-showinvisibles'),
            tabsize = widget.getAttribute('data-tabsize'),
            fontsize = widget.getAttribute('data-fontsize'),
            usesofttabs = widget.getAttribute('data-usesofttabs'),
            readonly = widget.getAttribute('data-readonly'),
            showgutter = widget.getAttribute('data-showgutter'),
            behaviours = widget.getAttribute('data-behaviours'),
            useworker = widget.getAttribute('data-useworker'),
            basicautocompletion = widget.getAttribute('data-basicautocompletion'),
            liveautocompletion = widget.getAttribute('data-liveautocompletion'),
            vimkeybinding = widget.getAttribute('data-vimkeybinding'),
            highlightactiveline = widget.getAttribute('data-highlightactiveline'),
            toolbar = prev(widget);

        // initialize editor and attach to widget element (for use in formset:removed)
        var editor = widget.editor = ace.edit(div);

        var main_block = div.parentNode.parentNode;
        if (toolbar != null) {
            // Toolbar maximize/minimize button
            var min_max = toolbar.getElementsByClassName('django-ace-max_min');
            min_max[0].onclick = function() {
                minimizeMaximize(widget, main_block, editor);
                return false;
            };
        }

        // load initial data
        editor.getSession().setValue(textarea.value);

        // the editor is initially absolute positioned
        textarea.style.display = "none";

        // options
        if (mode) {
            var Mode = ace.require("ace/mode/" + mode).Mode;
            editor.getSession().setMode(new Mode());
        }
        if (basicautocompletion == "true" || liveautocompletion == "true") {
            ace.require("ace/ext/language_tools");
        }
        if (theme) {
            editor.setTheme("ace/theme/" + theme);
        }
        if (wordwrap == "true") {
            editor.getSession().setUseWrapMode(true);
        }
        if (!!minlines) {
            editor.setOption("minLines", minlines);
        }
        if (!!maxlines) {
            editor.setOption("maxLines", maxlines=="-1" ? Infinity : maxlines);
        }
        if (showprintmargin == "false") {
            editor.setShowPrintMargin(false);
        }
        if (showinvisibles == "true") {
            editor.setShowInvisibles(true);
        }
        if (!!tabsize) {
            editor.setOption("tabSize", tabsize);
        }
        if (!!fontsize) {
            editor.setOption("fontSize", fontsize);
        }
        if (readonly == "true") {
            editor.setOption("readOnly", readonly);
        }
        if (usesofttabs == "false") {
            editor.getSession().setUseSoftTabs(false);
        }
        if (showgutter == "false") {
            editor.setOption("showGutter", false);
        }
        if (behaviours == "false") {
            editor.setOption("behavioursEnabled", false);
        }
        if (useworker == "false") {
            editor.setOption("useWorker", false);
        }
        if (basicautocompletion == "true") {
            // Avoid calling setOption("enableBasicAutocompletion", false) without
            // loading ext-language_tools as it gives a warning.
            editor.setOption("enableBasicAutocompletion", true);
        }
        if (basicautocompletion == "true") {
            // Avoid calling setOption("enableBasicAutocompletion", true) without
            // loading ext-language_tools as it gives a warning.
            editor.setOption("enableLiveAutocompletion", true);
        }
        if (vimkeybinding == "true") {
            editor.setKeyboardHandler("ace/keyboard/vim");
        }
        if (highlightactiveline == "false") {
            editor.setHighlightActiveLine(false);
        }

        // write data back to original textarea
        editor.getSession().on('change', function() {
            textarea.value = editor.getSession().getValue();
        });

        editor.commands.addCommand({
            name: 'Full screen',
            bindKey: {win: 'Ctrl-F11',  mac: 'Command-F11'},
            exec: function(editor) {
                minimizeMaximize(widget, main_block, editor);
            },
            readOnly: true // false if this command should not apply in readOnly mode
        });
    }

    /**
     * Determine if the given element is within the element that holds the template
     * for dynamically added forms for an InlineModelAdmin.
     *
     * @param {*} widget - The element to check.
     */
    function is_empty_form(widget) {
        var empty_forms = document.querySelectorAll('.empty-form, .grp-empty-form');
        for (empty_form of empty_forms) {
            if (empty_form.contains(widget)) {
                return true
            }
        }
        return false
    }

    function init() {
        var widgets = document.getElementsByClassName('django-ace-widget');
        var useStrictCSP = undefined;

        for (widget of widgets) {
            if (useStrictCSP == undefined) { // Set it
                useStrictCSP = widget.getAttribute('data-usestrictcsp') == "true" ? true : false;
            } else { // Warn if a 2nd widget have a different CSP policy
                if ((widget.getAttribute('data-useStrictCSP') == "true" ? true : false) != useStrictCSP) {
                    console.warn("Two AceWidget on this page have a different useStrictCSP values, it may not work properly. The kept value for useStrictCSP is ", useStrictCSP)
                }
            }
        }

        if (useStrictCSP)
            ace.config.set("useStrictCSP", true);

        for (widget of widgets) {

            // skip the widget in the admin inline empty-form
            if (is_empty_form(widget)) {
                continue;
            }

            // skip already loaded widgets
            if (!widget.classList.contains("loading")) {
                continue;
            }

            widget.className = "django-ace-widget"; // remove `loading` class

            apply_widget(widget);
        }
    }

    // Django's jQuery instance is available, we are probably in the admin
    if (typeof django == 'object' && typeof django.jQuery == 'function') {
        django.jQuery(document).on('formset:added', function (event, $row, formsetName) {
            // Row added to InlineModelAdmin, initialize new widgets
            init();
        });
        django.jQuery(document).on('formset:removed', function (event, $row, formsetName) {
            // Row removed from InlineModelAdmin, destroy attached editor
            $row.find('div.django-ace-widget')[0].editor.destroy()
        });
    }

    if (window.addEventListener) { // W3C
        window.addEventListener('load', init);
    } else if (window.attachEvent) { // Microsoft
        window.attachEvent('onload', init);
    }
})();
