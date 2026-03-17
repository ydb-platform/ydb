(function ($, djangoJQuery) {
  $.fn.autocompleteselect = function (options) {
    return this.each(function () {
      var id = this.id,
        $this = $(this),
        $text = $("#" + id + "_text"),
        $deck = $("#" + id + "_on_deck");

      function receiveResult(event, ui) {
        if ($this.val()) {
          removeItem();
        }
        $this.val(ui.item.pk);
        $text.val("");
        addItemOnDeck(ui.item.repr, ui.item.pk);
        $deck.trigger("added", [ui.item.pk, ui.item]);
        $this.trigger("change");

        return false;
      }

      function addItemOnDeck(repr, pk) {
        var trashId = "trash_" + pk + id,
          killButton = `<span class="ui-icon ui-icon-trash" id="${trashId}">X</span>`;
        if (repr) {
          $deck.empty();
          $deck.append(`<div>${killButton}${repr}</div>`);
        } else {
          $(`#${id}_on_deck > div`).prepend(killButton);
        }
        $("#" + trashId).click(function () {
          removeItem();
          $deck.trigger("killed", [pk]);
        });
      }

      function removeItem() {
        $this.val("");
        $deck.children().fadeOut(1.0).remove();
      }

      options.select = receiveResult;
      $text.autocomplete(options);

      function reset() {
        if (options.initial) {
          addItemOnDeck(options.initial[0], options.initial[1]);
          $this.val(options.initial[1]);
        } else {
          removeItem();
        }
      }

      if (!$this.attr("data-changed")) {
        reset();
        $this.attr("data-changed", true);
      }

      $this.closest("form").on("reset", reset);

      $this.bind("didAddPopup", function (event, pk, repr) {
        receiveResult(null, { item: { pk: pk, repr: repr } });
      });
    });
  };

  $.fn.autocompleteselectmultiple = function (options) {
    return this.each(function () {
      var id = this.id,
        $this = $(this),
        $text = $(`#${id}_text`),
        $deck = $(`#${id}_on_deck`);

      function receiveResult(event, ui) {
        var pk = ui.item.pk,
          prev = $this.val();

        if (prev.indexOf("|" + pk + "|") === -1) {
          $this.val((prev ? prev : "|") + pk + "|");
          addKiller(ui.item.repr, pk);
          $text.val("");
          $deck.trigger("added", [ui.item.pk, ui.item]);
          $this.trigger("change");
        }
        return false;
      }

      function addKiller(repr, pk) {
        var killId = "kill_" + pk + id;
        $deck.append(
          `<div id="${id}_on_deck_${pk}" class="item_on_deck">
            <span class="ui-icon ui-icon-trash" id="${killId}">X</span>
            <div class="repr">${repr}</div>
          </div>`
        );

        $("#" + killId).click(function () {
          kill(pk);
          $deck.trigger("killed", [pk]);
        });
      }

      function kill(pk) {
        $this.val($this.val().replace("|" + pk + "|", "|"));
        $(`#${id}_on_deck_${pk}`).fadeOut().remove();
      }

      options.select = receiveResult;
      $text.autocomplete(options);

      function reset() {
        $deck.empty();
        var query = "|";
        if (options.initial) {
          $.each(options.initial, function (i, its) {
            addKiller(its[0], its[1]);
            query += its[1] + "|";
          });
        }
        $this.val(query);
      }

      if (!$this.attr("data-changed")) {
        reset();
        $this.attr("data-changed", true);
      }

      $this.closest("form").on("reset", reset);

      $this.bind("didAddPopup", function (event, pk, repr) {
        receiveResult(null, { item: { pk: pk, repr: repr } });
      });
    });
  };

  function addAutoComplete(inp, callback) {
    var $inp = $(inp),
      opts = JSON.parse($inp.attr("data-plugin-options"));
    // Do not activate empty-form inline rows.
    // These are cloned into the form when adding another row and will be activated at that time.
    if ($inp.attr("id").indexOf("__prefix__") !== -1) {
      return;
    }
    if ($inp.data("_ajax_select_inited_")) {
      return;
    }
    callback($inp, opts);
    $inp.data("_ajax_select_inited_", true);
  }

  // allow html in the results menu
  // https://github.com/scottgonzalez/jquery-ui-extensions
  var proto = $.ui.autocomplete.prototype,
    initSource = proto._initSource;

  function filter(array, term) {
    var matcher = new RegExp($.ui.autocomplete.escapeRegex(term), "i");
    return $.grep(array, function (value) {
      return matcher.test(
        $("<div>")
          .html(value.label || value.value || value)
          .text()
      );
    });
  }

  $.extend(proto, {
    _initSource: function () {
      if (this.options.html && $.isArray(this.options.source)) {
        this.source = function (request, response) {
          response(filter(this.options.source, request.term));
        };
      } else {
        initSource.call(this);
      }
    },
    _renderItem: function (ul, item) {
      var body = this.options.html ? item.match : item.label;
      return $("<li></li>")
        .data("item.autocomplete", item)
        .append($("<a></a>")[this.options.html ? "html" : "text"](body))
        .appendTo(ul);
    },
  });

  // activate any on page
  $(window).bind("init-autocomplete", function () {
    $("input[data-ajax-select=autocomplete]").each(function (i, inp) {
      addAutoComplete(inp, function ($inp, opts) {
        opts.select = function (event, ui) {
          $inp.val(ui.item.value).trigger("added", [ui.item.pk, ui.item]);
          return false;
        };
        $inp.autocomplete(opts);
      });
    });

    $("input[data-ajax-select=autocompleteselect]").each(function (i, inp) {
      addAutoComplete(inp, function ($inp, opts) {
        $inp.autocompleteselect(opts);
      });
    });

    $("input[data-ajax-select=autocompleteselectmultiple]").each(function (
      i,
      inp
    ) {
      addAutoComplete(inp, function ($inp, opts) {
        $inp.autocompleteselectmultiple(opts);
      });
    });
  });

  $(document).ready(function () {
    // if dynamically injecting forms onto a page
    // you can trigger them to be ajax-selects-ified:
    $(window).trigger("init-autocomplete");
    // When adding new rows in inline forms, reinitialize and activate newly added rows.
    $(document).on(
      "click",
      "._inline-group ul.tools a.add, ._inline-group div.add-row a, ._inline-group .tabular tr.add-row td a, .inline-group ul.tools a.add, .inline-group div.add-row a, .inline-group .tabular tr.add-row td a",
      function () {
        $(window).trigger("init-autocomplete");
      }
    );
  });

  /* Called by the popup create object when it closes.
   * For the popup this is opener.dismissAddRelatedObjectPopup
   * Django implements this in RelatedObjectLookups.js
   * In django >= 1.10 we could try to rely on input.trigger('change')
   * and avoid this hijacking, but the id and repr isn't passed.
   */
  if (djangoJQuery) {
    // only for Django Admin
    djangoJQuery(document).ready(function () {
      var djangoDismissAddRelatedObjectPopup =
          window.dismissAddRelatedObjectPopup;
      if (!djangoDismissAddRelatedObjectPopup) {
        throw new Error("dismissAddAnotherPopup not found");
      }

      window.dismissAddRelatedObjectPopup = function (win, newId, newRepr) {
        // Iff this is an ajax-select input then close the window and
        // trigger didAddPopup
        var input = $("#" + win.name.replace("__1", ""));
        if (input.length && input.data("ajax-select")) {
          win.close();
          // note: newRepr is django's repr of object, not the Lookup's formatting of it.
          input.trigger("didAddPopup", [newId, newRepr]);
        } else {
          // Call the normal django set and close function.
          djangoDismissAddRelatedObjectPopup(win, newId, newRepr);
        }
      };
    });
  }
})(window.jQuery, window.django && window.django.jQuery);
