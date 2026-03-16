var jQuery, $, django;

(function () {
  "use strict";
  (jQuery || $ || django.jQuery)(function ($) {
    $(".clearable-input").each(function () {
      var clear = $(this).children().last();
      $(this)
        .find("input, select, textarea")
        .not(clear)
        .change(function () {
          if (typeof clear.prop == "undefined")
            // older jQuery
            clear.removeAttr("checked");
          else clear.prop("checked", false);
        });
    });
  });
})();
