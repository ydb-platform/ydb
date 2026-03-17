$(function () {
  moment.locale(model.locale);
  $.fn.DataTable.DateTime.defaults.locale = model.locale;

  /* list of primary keys of selected rows */
  var selectedRows = [];

  /*
    contains all fields including nested fields inside all CollectionField.
    Each nested field name is prefixed by it parent CollectionField name (ex: 'category.name')
    */
  var dt_fields = [];

  /* datatables columns generated from model.fields */
  var dt_columns = [];

  (function () {
    let fringe = structuredClone(model.fields);
    while (fringe.length > 0) {
      let field = fringe.shift(0);
      if (field.type === "CollectionField")
        fringe = field.fields
          .map((f) => {
            // Produce nested name (ex: category.name)
            f.name = field.name + "." + f.name;
            f.label = field.label + "." + f.label;
            return f;
          })
          .concat(fringe);
      else if (field.type === "ListField") {
        // To reduce complexity, List of CollectionField will render as json
        if (field.field.type == "CollectionField") {
          $("#table-header").append(`<th>${field.label}</th>`);
          dt_columns.push({
            name: field.name,
            data: field.name,
            orderable: field.field.orderable,
            searchBuilderType: field.search_builder_type,
            render: function (data, type, full, meta) {
              return render[field.field.render_function_key](
                data,
                type,
                full,
                meta,
                field
              );
            },
          });
        } else {
          field.field.name = field.name;
          field.field.label = field.label;
          fringe.unshift(field.field);
        }
      } else if (!field.exclude_from_list) {
        $("#table-header").append(`<th>${field.label}</th>`);
        dt_columns.push({
          name: field.name,
          data: field.name,
          orderable: field.orderable,
          searchBuilderType: field.search_builder_type,
          render: function (data, type, full, meta) {
            return render[field.render_function_key](
              data,
              type,
              full,
              meta,
              field
            );
          },
        });
        dt_fields.push(field);
      }
    }
  })();

  // Actions

  var actionManager = new ActionManager(
    model.actionUrl,
    model.rowActionUrl,
    function (query, element) {
      // appendQueryParams
      if (element.data("is-row-action") === true)
        query.append(
          "pk",
          element.closest(".row-actions-container").data("id")
        );
      else
        selectedRows.forEach((s) => {
          query.append("pks", s);
        });
    },
    function (actionName, element, msg) {
      // onSuccess
      if (!element.data("is-row-action")) table.rows().deselect();
      table.ajax.reload();
      successAlert(msg);
    },
    function (actionName, element, error) {
      //onError
      dangerAlert(error);
    }
  );

  // Buttons declarations

  buttons = [];
  export_buttons = [];
  if (model.exportTypes.includes("csv"))
    export_buttons.push({
      extend: "csv",
      text: function (dt) {
        return `<i class="fa-solid fa-file-csv"></i> ${dt.i18n("buttons.csv")}`;
      },
      exportOptions: {
        columns: model.exportColumns,
        orthogonal: "export-csv",
      },
    });
  if (model.exportTypes.includes("excel"))
    export_buttons.push({
      extend: "excel",
      text: function (dt) {
        return `<i class="fa-solid fa-file-excel"></i> ${dt.i18n(
          "buttons.excel"
        )}`;
      },
      exportOptions: {
        columns: model.exportColumns,
        orthogonal: "export-excel",
      },
    });
  if (model.exportTypes.includes("pdf"))
    export_buttons.push({
      extend: "pdf",
      text: function (dt) {
        return `<i class="fa-solid fa-file-pdf"></i> ${dt.i18n("buttons.pdf")}`;
      },
      exportOptions: {
        columns: model.exportColumns,
        orthogonal: "export-pdf",
      },
    });
  if (model.exportTypes.includes("print"))
    export_buttons.push({
      extend: "print",
      text: function (dt) {
        return `<i class="fa-solid fa-print"></i> ${dt.i18n("buttons.print")}`;
      },
      exportOptions: {
        columns: model.exportColumns,
        orthogonal: "export-print",
      },
    });
  if (export_buttons.length > 0)
    buttons.push({
      extend: "collection",
      text: function (dt) {
        return `<i class="fa-solid fa-file-export"></i> ${dt.i18n(
          "starlette-admin.buttons.export"
        )}`;
      },
      className: "",
      buttons: export_buttons,
    });
  noInputCondition = function (cn) {
    return {
      conditionName: function (t, i) {
        return t.i18n(cn);
      },
      init: function (a) {
        a.s.dt.one("draw.dtsb", function () {
          a.s.topGroup.trigger("dtsb-redrawLogic");
        });
      },
      inputValue: function () {},
      isInputValid: function () {
        return !0;
      },
    };
  };
  if (model.columnVisibility)
    buttons.push({
      extend: "colvis",
      text: function (dt) {
        return `<i class="fa-solid fa-eye"></i> ${dt.i18n("buttons.colvis")}`;
      },
    });

  if (model.searchBuilder)
    buttons.push({
      extend: "searchBuilder",
      config: {
        columns: model.searchColumns,
        conditions: {
          bool: {
            false: noInputCondition("starlette-admin.conditions.false"),
            true: noInputCondition("starlette-admin.conditions.true"),
            null: noInputCondition("starlette-admin.conditions.empty"),
            "!null": noInputCondition("starlette-admin.conditions.notEmpty"),
          },
          default: {
            null: noInputCondition("starlette-admin.conditions.empty"),
            "!null": noInputCondition("starlette-admin.conditions.notEmpty"),
          },
        },
        greyscale: true,
      },
    });

  // End Buttons declarations

  /*
    Convert datatables searchBuilder conditions into custom dict
    with custom operators before send it to the backend.
    */
  function extractCriteria(c) {
    var d = {};
    if ((c.logic && c.logic == "OR") || c.logic == "AND") {
      d[c.logic.toLowerCase()] = [];
      c.criteria.forEach((v) => {
        d[c.logic.toLowerCase()].push(extractCriteria(v));
      });
    } else {
      if (c.type.startsWith("moment-")) {
        searchFormat = dt_fields.find(
          (f) => f.name == c.origData
        )?.search_format;
        if (!searchFormat) searchFormat = moment.defaultFormat;
        c.value = [];
        if (c.value1) {
          c.value1 = moment(c.value1).format(searchFormat);
          c.value.push(c.value1);
        }
        if (c.value2) {
          c.value2 = moment(c.value2).format(searchFormat);
          c.value.push(c.value2);
        }
      } else if (c.type == "num") {
        c.value = [];
        if (c.value1) {
          c.value1 = Number(c.value1);
          c.value.push(c.value1);
        }
        if (c.value2) {
          c.value2 = Number(c.value2);
          c.value.push(c.value2);
        }
      }
      cnd = {};
      c_map = {
        "=": "eq",
        "!=": "neq",
        ">": "gt",
        ">=": "ge",
        "<": "lt",
        "<=": "le",
        contains: "contains",
        starts: "startswith",
        ends: "endswith",
        "!contains": "not_contains",
        "!starts": "not_startswith",
        "!ends": "not_endswith",
        null: "is_null",
        "!null": "is_not_null",
        false: "is_false",
        true: "is_true",
      };
      if (c.condition == "between") {
        cnd["between"] = c.value;
      } else if (c.condition == "!between") {
        cnd["not_between"] = c.value;
      } else if (c_map[c.condition]) {
        cnd[c_map[c.condition]] = c.value1 ?? "";
      }
      d[c.origData] = cnd;
    }
    return d;
  }

  // End Search builder

  // Datatable instance
  var table = $("#dt").DataTable({
    dom: "r<'table-responsive't><'card-footer d-flex align-items-center'<'m-0'i><'m-0 ms-auto'p>>",
    stateSave: model.stateSave,
    paging: true,
    lengthChange: true,
    searching: true,
    info: true,
    colReorder: true,
    searchHighlight: true,
    responsive: model.responsiveTable,
    serverSide: true,
    scrollX: false,
    lengthMenu: model.lengthMenu,
    pagingType: "full_numbers",
    pageLength: model.pageSize,
    select: {
      style: "multi",
      selector: "td:first-child .form-check-input",
      className: "row-selected",
    },
    language: {
      url: model.dt_i18n_url,
      infoFiltered: "",
      select: {
        rows: {
          0: "",
        },
      },
      searchBuilder: {
        delete: `<svg xmlns="http://www.w3.org/2000/svg" class="icon icon-tabler icon-tabler-trash" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"></path><line x1="4" y1="7" x2="20" y2="7"></line><line x1="10" y1="11" x2="10" y2="17"></line><line x1="14" y1="11" x2="14" y2="17"></line><path d="M5 7l1 12a2 2 0 0 0 2 2h8a2 2 0 0 0 2 -2l1 -12"></path><path d="M9 7v-3a1 1 0 0 1 1 -1h4a1 1 0 0 1 1 1v3"></path></svg>`,
        left: `<svg xmlns="http://www.w3.org/2000/svg" class="icon icon-tabler icon-tabler-chevron-left" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"></path><polyline points="15 6 9 12 15 18"></polyline></svg>`,
        right: `<svg xmlns="http://www.w3.org/2000/svg" class="icon icon-tabler icon-tabler-chevron-right" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"></path><polyline points="9 6 15 12 9 18"></polyline></svg>`,
      },
    },
    ajax: function (data, callback, settings) {
      // console.log(data);
      order = [];
      data.order.forEach((o) => {
        const { column, dir } = o;
        order.push(`${data.columns[column].data} ${dir}`);
      });
      where = null;
      if (data.searchBuilder && !jQuery.isEmptyObject(data.searchBuilder)) {
        where = extractCriteria(data.searchBuilder);
        // console.log(where);
      }
      query = {
        skip: settings._iDisplayStart,
        limit: settings._iDisplayLength,
        order_by: order,
      };
      if (data.search.value != "") query.where = data.search.value;
      else if (where) query.where = JSON.stringify(where);
      $.ajax({
        url: model.apiUrl,
        type: "get",
        data: query,
        traditional: true,
        dataType: "json",
        success: function (data, status, xhr) {
          total = data.total;
          data = data.items;
          data.forEach((d) => {
            d.DT_RowId = d[model.pk];
          });
          callback({
            recordsFiltered: total,
            data: data,
          });
        },
      });
    },
    columns: [
      {
        data: "DT_RowId",
        orderable: false,
        checkboxes: {
          selectRow: true,
          selectAllRender:
            '<input class="form-check-input dt-checkboxes" type="checkbox">',
        },
        render: render.col_0,
      },
      {
        data: "DT_RowId",
        orderable: false,
        render: render.col_1,
      },
      ...dt_columns,
    ],
    order: (function defaultSort() {
      let order = [];
      for (const [col, desc] of Object.entries(model.fieldsDefaultSort)) {
        let idx = dt_columns.findIndex((it) => col === it.name);
        if (idx > -1) order.push([idx + 2, desc === true ? "desc" : "asc"]);
      }
      return order;
    })(),
    initComplete: function () {
      new $.fn.dataTable.Buttons(table, {
        name: "main",
        buttons: buttons,
        dom: {
          button: {
            className: "btn btn-secondary",
          },
        },
      });
      new $.fn.dataTable.Buttons(table, {
        name: "pageLength",
        buttons: [
          {
            extend: "pageLength",
            className: "btn",
          },
        ],
        dom: {
          button: {
            className: "",
          },
        },
      });

      table.buttons("main", null).container().appendTo("#btn_container");
      table
        .buttons("pageLength", null)
        .container()
        .appendTo("#pageLength_container");
    },
    drawCallback: function (settings) {
      actionManager.initNoConfirmationActions();
    },

    stateSaveCallback: function (settings, data) {
      let page = 0;
      try {
        page = (data?.page ?? data?.start / data?.length ?? 0) + 1;
      } catch (e) {}
      const params = {
        page: page,
        page_size: data?.length,
        search: data?.search?.search,
        order: data?.order
          ? data.order.map(
              ([col, dir]) =>
                `${dir === "asc" ? "" : "-"}${dt_columns[col - 2].name}`
            )
          : undefined,
        searchBuilder:
          data?.searchBuilder && !$.isEmptyObject(data?.searchBuilder)
            ? JSON.stringify(data?.searchBuilder)
            : undefined,
      };

      const query = Qs.stringify(params, { encode: false, indices: false });

      history.replaceState(
        null,
        "",
        location.pathname + (query ? "?" + query : "")
      );
    },

    stateLoadCallback: function (settings, callback) {
      const params = Qs.parse(location.search, { ignoreQueryPrefix: true });
      if (!Object.keys(params).length) return null;

      let length = isNaN(parseInt(params?.page_size))
        ? model.pageSize
        : parseInt(params?.page_size);
      let page = isNaN(parseInt(params?.page)) ? 0 : parseInt(params?.page) - 1;
      let order = [];
      if (params?.order) {
        if (typeof params.order === "string") params.order = [params.order];
        params.order.forEach((o) => {
          const isDesc = o.startsWith("-");
          const colName =
            o.startsWith("-") || o.startsWith("+") ? o.substring(1) : o;
          let idx = dt_columns.findIndex((it) => colName === it.name);
          if (idx > -1) order.push([idx + 2, isDesc ? "desc" : "asc"]);
        });
      }

      var state = {
        time: Date.now(),
        length: length,
        order: order.length > 0 ? order : undefined,
        search: {
          search: params?.search ?? undefined,
          smart: true,
          regex: false,
          caseInsensitive: true,
        },
        searchBuilder: params?.searchBuilder
          ? JSON.parse(params?.searchBuilder)
          : undefined,
        page: page,
        start: length * page,
      };
      if (params?.search) $("#searchInput").val(params.search);
      callback(state);
    },
    ...model.datatablesOptions,
  });

  $("#searchInput").on("keyup", function () {
    table.search($(this).val()).draw();
  });

  function onSelectChange() {
    selectedRows = table.rows({ selected: true }).ids().toArray();
    if (table.rows({ selected: true }).count() == 0)
      $("#actions-dropdown").hide();
    else $("#actions-dropdown").show();
    $(".actions-selected-counter").text(table.rows({ selected: true }).count());
  }

  table
    .on("select", function (e, dt, type, indexes) {
      onSelectChange();
    })
    .on("deselect", function (e, dt, type, indexes) {
      onSelectChange();
    });

  actionManager.initNoConfirmationActions();
  actionManager.initActionModal();

  $('[data-toggle="tooltip"]').tooltip();
});
