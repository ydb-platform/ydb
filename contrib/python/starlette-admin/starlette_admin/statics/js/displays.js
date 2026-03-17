$("div.field-json").each(function () {
    $(this).append(pretty_print_json(JSON.parse(JSON.stringify($(this).text()))));
});
