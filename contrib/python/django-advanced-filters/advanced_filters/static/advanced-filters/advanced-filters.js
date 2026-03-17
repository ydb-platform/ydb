var _af_handlers = window._af_handlers || null;
var OperatorHandlers = function($) {
	var self = this;
	self.value = null;
	self.val_input = null;
	self.selected_field_elm = null;

	self.add_datepickers = function() {
		var form_id = self.val_input.parents('tr').attr('id');
		var form_num = parseInt(form_id.replace('form-', ''), 10);

		var $from = $('<input type="text">');
		$from.attr("name", "form-" + form_num + "-value_from");
		$from.attr("id", "id_form-" + form_num + "-value_from");
		$from.attr("placeholder", gettext('Start date (YYYY-MM-DD)'));
		$from.addClass('query-dt-from');
		var $to = $('<input type="text">');
		$to.attr("name", "form-" + form_num + "-value_to");
		$to.attr("id", "id_form-" + form_num + "-value_to");
		$to.attr("placeholder", gettext('End date (YYYY-MM-DD)'));
		$to.addClass('query-dt-to');

		self.val_input.parent().prepend($to);
		self.val_input.parent().prepend($from);
		var val = self.val_input.val();
		if (!val || val == 'null') {
			self.val_input.val("-");
		} else {
			from_to = val.split(',');
			if (from_to.length == 2) {
				$from.val(from_to[0])
				$to.val(from_to[1])
			}
		}
		self.val_input.css({display: 'none'});

		$(".hasDatepicker").datepicker("destroy");
		$from.addClass('vDateField');
		$to.addClass('vDateField');
		grappelli.initDateAndTimePicker();
	};

	self.remove_datepickers = function() {
		self.val_input.css({display: 'block'});
		if (self.val_input.parent().find('input.vDateField').length > 0) {
			var datefields = self.val_input.parent().find('input.vDateField');
			datefields.each(function() {
				$(this).datepicker("destroy");
			});
			datefields.remove();
		}
	};

	self.modify_widget = function(elm) {
		// pick a widget for the value field according to operator
		self.value = $(elm).val();
		self.val_input = $(elm).parents('tr').find('.query-value');
		console.log("selected operator: " + self.value);
		if (self.value == "range") {
			self.add_datepickers();
		} else {
			self.remove_datepickers();
		}
	};

	self.initialize_select2 = function(elm) {
		// initialize select2 widget and populate field choices
		var field = $(elm).val();
		var choices_url = ADVANCED_FILTER_CHOICES_LOOKUP_URL + (FORM_MODEL ||
						  MODEL_LABEL) + '/' + field;
		var input = $(elm).parents('tr').find('input.query-value');
		input.select2("destroy");
		$.get(choices_url, function(data) {
			input.select2({'data': data, 'createSearchChoice': function(term) {
                return { 'id': term, 'text': term };
            }});
		});
	};

	self.field_selected = function(elm) {
		self.selected_field_elm = elm;
		var row = $(elm).parents('tr');
		var op = row.find('.query-operator');
		var value = row.find('.query-value');
		if ($(elm).val() == "_OR") {
			op.val("iexact").prop("disabled", true);
			value.val("null").prop("disabled", true);
			op.after('<input type="hidden" value="' + op.val() +
				'" name="' + op.attr("name") + '">');
			value.after('<input type="hidden" value="' + value.val() +
				'" name="' + value.attr("name") + '">');
		} else {
			op.prop("disabled", false);
			op.siblings('input[type="hidden"]').remove();
			value.prop("disabled", false);
			value.siblings('input[type="hidden"]').remove();
			if (!value.val() == "null") {
				value.val("");
			}
			op.val("iexact").change();
			self.initialize_select2(elm);
		}
	};

	self.init = function() {
		var rows = $('[data-rules-formset] tr.form-row');
		if (rows.length == 1 && rows.eq(0).hasClass('empty-form')) {
			// if only 1 form and it's empty, add first extra formset
			$('[data-rules-formset] .add-row a').click();
		}
		$('.form-row select.query-operator').each(function() {
			$(this).off("change");
			$(this).data('pre_change', $(this).val());
			$(this).on("change", function() {
				var before_change = $(this).data('pre_change');
				if ($(this).val() != before_change) self.modify_widget(this);
				$(this).data('pre_change', $(this).val());
			}).change();
			self.modify_widget(this);
		});
		$('.form-row select.query-field').each(function() {
			$(this).off("change");
			$(this).data('pre_change', $(this).val());
			$(this).on("change", function() {
				var before_change = $(this).data('pre_change');
				if ($(this).val() != before_change) self.field_selected(this);
				$(this).data('pre_change', $(this).val());
			}).change();
		});
		self.field_selected($('.form-row select.query-field').first());

	};

	self.destroy = function() {
		$('.form-row select.query-operator').each(function() {
			$(this).off("change");
		});
		$('.form-row select.query-field').each(function() {
			$(this).off("change");
		});
		$('.form-row input.query-value').each(function() {
			$(this).select2("destroy");
		});
	};
};

// using Grappelli's jquery if available
(function($) {
	$(document).ready(function() {
		if (!_af_handlers) {
			_af_handlers = new OperatorHandlers($);
			_af_handlers.destroy()
			_af_handlers.init();
		}
	});
})(window._jq || jQuery);
