/**
 * GRAPPELLI TIMEPICKER
 * works pretty similar to ui.datepicker:
 * adds a button to the element
 * creates a node (div) at the bottom called ui-timepicker
 * element.onClick fills the ui-timepicker node with the time_list (all times you can select)
 */

(function($) {
    $.widget("ui.grp_timepicker", {
        // default options
        options: {
            // template for the container of the timepicker
            template: '<div id="ui-timepicker" class="module" style="position: absolute; display: none;"></div>',
            // selector to get the ui-timepicker once it's added to the dom
            timepicker_selector: "#ui-timepicker",
            // needed offset of the container from the element
            offset: {
                top: 0
            },
            // if time_list wasn't sent when calling the timepicker we use this
            default_time_list: [
                'now',
                '00:00',
                '01:00',
                '02:00',
                '03:00',
                '04:00',
                '05:00',
                '06:00',
                '07:00',
                '08:00',
                '09:00',
                '10:00',
                '11:00',
                '12:00',
                '13:00',
                '14:00',
                '15:00',
                '16:00',
                '17:00',
                '18:00',
                '19:00',
                '20:00',
                '21:00',
                '22:00',
                '23:00'
            ],
            // leave this empty!!!
            // NOTE: you can't set a default for time_list because if you call:
            // $("node").timepicker({time_list: ["01:00", "02:00"]})
            // ui.widget will extend/merge the options.time_list with the one you sent.
            time_list: []
        },

        // init timepicker for a specific element
        _create: function() {
            // for the events
            var self = this;

            // to close timpicker if you click somewhere in the document
            $(document).on("mousedown", function(evt) {
                if (self.timepicker.is(":visible")) {
                    var $target = $(evt.target);
                    if ($target[0].id != self.timepicker[0].id && $target.parents(self.options.timepicker_selector).length === 0 && !$target.hasClass('hasTimepicker') && !$target.hasClass('ui-timepicker-trigger')) {
                        self.timepicker.hide();
                    }
                }
            });
            // close on esc
            $(document).on("keyup", function(e) {
                if (e.keyCode == 27) {
                    self.timepicker.hide();
                }
            });

            // get/create timepicker's container
            if ($(this.options.timepicker_selector).length === 0) {
                $(this.options.template).appendTo('body');
            }
            this.timepicker = $(this.options.timepicker_selector);
            this.timepicker.hide();

            // modify the element and create the button
            this.element.addClass("hasTimepicker");
            this.button = $('<button type="button" class="ui-timepicker-trigger"></button>');
            this.element.after(this.button);

            // disable button if element is disabled
            if (this.element.prop("disabled")) {
                this.button.prop("disabled", true);
            } else {
                // register event
                this.button.on("click", function() {
                    self._toggleTimepicker();
                });
            }
        },

        // called when button is clicked
        _toggleTimepicker: function() {
            if (this.timepicker.is(":visible")) {
                this.timepicker.hide();
            } else {
                this.element.trigger("focus");
                this._generateTimepickerContents();
                this._showTimepicker();
            }
        },

        // fills timepicker with time_list of element and shows it.
        // called by _toggleTimepicker
        _generateTimepickerContents: function() {
            var self = this,
                template_str = "<ul>";

            // there is no time_list for this instance so use the default one
            if (this.options.time_list.length === 0) {
                this.options.time_list = this.options.default_time_list;
            }

            for (var i = 0; i < this.options.time_list.length; i++) {
                if (this.options.time_list[i] == "now") {
                    var now = new Date(),
                        hours = now.getHours(),
                        minutes = now.getMinutes();

                    hours = ((hours < 10) ? "0" + hours : hours);
                    minutes = ((minutes < 10) ? "0" + minutes : minutes);

                    template_str += '<li class="ui-state-active row">' + hours + ":" + minutes + '</li>';
                } else {
                    template_str += '<li class="ui-state-default row">' + this.options.time_list[i] + '</li>';
                }
            }
            template_str += "</ul>";

            // fill timepicker container
            this.timepicker.html(template_str);

            // click handler for items (times) in timepicker
            this.timepicker.find('li').on("click", function() {
                // remove active class from all items
                $(this).parent().children('li').removeClass("ui-state-active");
                // mark clicked item as active
                $(this).addClass("ui-state-active");
                // set the new value and hide the timepicker
                self.element.val($(this).html());
                self.timepicker.hide();
            });
        },

        // sets offset and shows timepicker container
        _showTimepicker: function() {
            var browserHeight = document.documentElement.clientHeight;
            var scrollY = document.documentElement.scrollTop || document.body.scrollTop;
            var tpInputHeight = this.element.outerHeight();
            var tpDialogHeight = this.timepicker.outerHeight() + tpInputHeight;
            var offsetTop = this.element.offset().top;
            var offsetLeft = this.element.offset().left;

            // check the remaining space within the viewport
            // 60 counts for fixed header/footer
            var checkSpace = offsetTop - scrollY + tpDialogHeight + 60;

            // position timepicker below or above the input
            if (checkSpace < browserHeight) {
                // place the timepicker below input
                var below = offsetTop + tpInputHeight;
                this.timepicker.css('left', offsetLeft + 'px').css('top', below + 'px');
            } else {
                // place timepicker above input
                var above = offsetTop - tpDialogHeight + tpInputHeight;
                this.timepicker.css('left', offsetLeft + 'px').css('top', above + 'px');
            }
            // show timepicker
            this.timepicker.show();
        },

        destroy: function() {
            $.Widget.prototype.destroy.apply(this, arguments); // default destroy
            // now do other stuff particular to this widget
        }

    });
})(grp.jQuery);
