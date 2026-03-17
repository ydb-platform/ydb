// Handle delete modal
$(document).on('shown.bs.modal', '#modal-delete', function (event) {
  var element = $(event.relatedTarget);

  var name = element.data("name");
  var pk = element.data("pk");
  $("#modal-delete-text").text("This will permanently delete " + name + " " + pk + " ?");

  $("#modal-delete-button").attr("data-url", element.data("url"));
});

$(document).on('click', '#modal-delete-button', function () {
  $.ajax({
    url: $(this).attr('data-url'),
    method: 'DELETE',
    success: function (result) {
      window.location.href = result;
    },
    error: function (request, status, error) {
      alert(request.responseText);
    }
  });
});

// Search
$(document).on('click', '#search-button', function () {
  var searchTerm = encodeURIComponent($("#search-input").val());

  newUrl = "";
  if (window.location.search && window.location.search.indexOf('search=') != -1) {
    newUrl = window.location.search.replace(/search=[^&]*/, "search=" + searchTerm);
  } else if (window.location.search) {
    newUrl = window.location.search + "&search=" + searchTerm;
  } else {
    newUrl = window.location.search + "?search=" + searchTerm;
  }
  window.location.href = newUrl;
});

// Reset search
$(document).on('click', '#search-reset', function () {
  if (window.location.search && window.location.search.indexOf('search=') != -1) {
    window.location.href = window.location.search.replace(/search=[^&]*/, "");
  }
});

// Press enter to search
$(document).on('keypress', '#search-input', function (e) {
  if (e.which === 13) {
    $('#search-button').click();
  }
});

// Init a timeout variable to be used below
var timeout = null;
// Search
$(document).on('keyup', '#search-input', function (e) {
  clearTimeout(timeout);
  // Make a new timeout set to go off in 1000ms (1 second)
  timeout = setTimeout(function () {
    $('#search-button').click();
  }, 1000);
});

// Date picker
$(':input[data-role="datepicker"]:not([readonly])').each(function () {
  $(this).flatpickr({
    enableTime: false,
    allowInput: true,
    dateFormat: "Y-m-d",
  });
});

// DateTime picker
$(':input[data-role="datetimepicker"]:not([readonly])').each(function () {
  $(this).flatpickr({
    enableTime: true,
    allowInput: true,
    enableSeconds: true,
    time_24hr: true,
    dateFormat: "Y-m-d H:i:s",
  });
});

// Ajax Refs
$(':input[data-role="select2-ajax"]').each(function () {
  $(this).select2({
    minimumInputLength: 1,
    ajax: {
      url: $(this).data("url"),
      dataType: 'json',
      data: function (params) {
        var query = {
          name: $(this).attr("name"),
          term: params.term,
        }
        return query;
      }
    }
  });

  existing_data = $(this).data("json") || [];
  for (var i = 0; i < existing_data.length; i++) {
    data = existing_data[i];
    var option = new Option(data.text, data.id, true, true);
    $(this).append(option).trigger('change');
  }
});

// Checkbox select
$("#select-all").click(function () {
  $('input.select-box:checkbox').prop('checked', this.checked);
});

// Bulk delete
$("#action-delete").click(function () {
  var pks = [];
  $('.select-box').each(function () {
    if ($(this).is(':checked')) {
      pks.push($(this).siblings().get(0).value);
    }
  });

  $('#action-delete').data("pk", pks);
  $('#action-delete').data("url", $(this).data('url') + '?pks=' + pks.join(","));
  $('#modal-delete').modal('show');
});

$("[id^='action-custom-']").click(function () {
  var pks = [];
  $('.select-box').each(function () {
    if ($(this).is(':checked')) {
      pks.push($(this).siblings().get(0).value);
    }
  });

  window.location.href = $(this).attr('data-url') + '?pks=' + pks.join(",");
});

// Select2 Tags
$(':input[data-role="select2-tags"]').each(function () {
  $(this).select2({
    tags: true,
    multiple: true,
  });

  existing_data = $(this).data("json") || [];
  for (var i = 0; i < existing_data.length; i++) {
    var option = new Option(existing_data[i], existing_data[i], true, true);
    $(this).append(option).trigger('change');
  }
});
