'use strict';

var yamlConfigState;

function showSmallModal(header, body, footer) {
    var headerElem = $('#small-modal-header');
    headerElem.empty();
    if (header !== undefined) {
        headerElem.append(header);
    }

    var bodyElem = $('#small-modal-body');
    bodyElem.empty();
    if (body !== undefined) {
        bodyElem.append(body);
    }

    var footerElem = $('#small-modal-footer');
    footerElem.empty();
    if (footer !== undefined) {
        footerElem.append(footer);
    }

    $("#small-modal").modal("show");
}

function showAck(headerText, bodyText, okBtn, cancelBtn, cb) {
    var header = $('<span></span>');
    header.text(headerText);

    var body = $('<span></span>');
    body.text(bodyText);

    var okBtn = $(`
        <button type="button" class="btn btn-primary" data-dismiss="modal">${okBtn}</button>
        `)
    okBtn.click(cb);

    var cancelBtn = $(`
        <button type="button" class="btn btn-default" data-dismiss="modal">${cancelBtn}</button>
        `)

    showSmallModal(header, body, [okBtn, cancelBtn]);
}

function showToast(header, text, timeout) {

    if (!$("#yaml-config").hasClass("active")) {
        return;
    }

    var toast = $(`
      <div class="toast bg-danger text-white" role="alert" aria-live="assertive" aria-atomic="true" data-delay="${timeout}">
        <div class="toast-header">
          <strong class="mr-auto text-black">${header}</strong>
          <button type="button" class="ml-2 mb-1 close" data-dismiss="toast" aria-label="Close">
            <span aria-hidden="true">&times;</span>
          </button>
        </div>
        <div class="toast-body">
          ${text}
        </div>
      </div>
    `);

    $("#toaster").append(toast);
    toast.toast('show');
    setTimeout(function() { toast.remove(); }, timeout);
}

function hideOnClickOutside(selector) {
  const outsideClickListener = (event) => {
    const target = $(event.target);
    if (!target.closest(selector).length && $(selector).is(':visible')) {
        $('.yaml-submenu').removeClass("active");
        $('.yaml-btn').removeClass("active");
        removeClickListener();
    }
  }

  const removeClickListener = () => {
    document.removeEventListener('click', outsideClickListener);
  }

  document.addEventListener('click', outsideClickListener);
}

function addLabel() {
   var label = $(`
        <tr class="yaml-label-row">
            <th scope="col" style="width:1%;">
            <button type="button" class="btn btn-danger" id="yaml-labels-clear">
                <span aria-hidden="true">&times;</span>
            </button>
            </th>
            <td>
            <input type="text" class="form-control yaml-label-name">
            </td>
            <td>
            <input type="text" class="form-control yaml-label-value">
            </td>
        </tr>
    `);
  label.find('.btn').click((event) => {
      event.stopPropagation();
      label.remove();
  });
  label.insertBefore('#yaml-label-controls');
}

function clearLabels() {
    $(".yaml-label-row").remove();
}

function yamlCollectConfig() {
    let current = $("#yaml-current-config-tab").hasClass("active");
    var config;
    if (current) {
        config = yamlConfigState.codeMirror.getValue();
    } else {
        config = yamlConfigState.arbitraryResolveCodeMirror.getValue();
    }
    return config;
}

function yamlCollectVolatileConfigs() {
    let current = $("#yaml-current-config-tab").hasClass("active");
    var configs = [];
    var id = 1;
    if (current) {
        if ($("#yaml-resolve-include-volatile").is(':checked')) {
            for (let codeMirror of yamlConfigState.codeMirrors) {
                configs.push({id: id, config: codeMirror.getValue()});
                ++id;
            }
        }
        if (yamlConfigState.additionalResolveCodeMirror.getValue() !== "") {
            configs.push({id: id, config: yamlConfigState.additionalResolveCodeMirror.getValue()});
        }
    }
    return configs;
}


function yamlCollectLabels() {
    var labels = {};
    $(".yaml-label-row").each(function() {
        var label = $(this).find('.yaml-label-name').val();
        var value = $(this).find('.yaml-label-value').val();
        labels[label] = value;
    });
    return labels;
}

class YamlConfigState {
    constructor() {
        this.fetchInterval = 5000;
        this.url = 'cms/api/console/yamlconfig';
        this.readOnlyUrl = 'cms/api/console/readonly';
        this.resolveUrl = 'cms/api/console/resolveyamlconfig';
        this.resolveAllUrl = 'cms/api/console/resolveallyamlconfig';
        this.removeVolatileUrl = 'cms/api/console/removevolatileyamlconfig';
        this.applyUrl = 'cms/api/console/configureyamlconfig';
        this.applyVolatileUrl = 'cms/api/console/configurevolatileyamlconfig';
        this.readOnly = true;
        this.maxVolatileId = -1;
        this.volatileConfigs = [];
        this.codeMirrors = [];
        this.initTab();
    }

    changeReadOnlyState(state) {
        if (this.readOnly == state)
            return;

        this.readOnly = state;
        var btn = $('#yaml-apply-button');

        if (this.readOnly) {
            btn.addClass("disabled");
            btn.prop("onclick", null).off("click");
            btn.attr("title", "Set config field 'allow_edit_yaml_in_ui' to 'true' to enable this button");
        } else {
            btn.removeClass("disabled");
            btn.removeAttr("title");
            var self = this;
            btn.on('click', function(event) {
                event.preventDefault();
                showAck("Apply new config?", " ", "Yes", "No", self.setConfig.bind(self));
            });
        }

        if (this.codeMirror) {
            this.codeMirror.updateOptions({ readOnly: this.readOnly });
        }
    }

    setConfig() {
        var cmd = {
            Request: {
                config: this.codeMirror.getValue(),
            },
        };

        $.post(this.applyUrl, JSON.stringify(cmd))
         .done(this.onSetConfig.bind(this, true))
         .fail(this.onSetConfig.bind(this, false));
    }

    onSetConfig(success, data) {
        if (success) {
            // ok, do nothing
        } else {
            var message = "";
            if (data.hasOwnProperty('responseJSON')) {
                message = data.responseJSON.issues;
            } else {
                message = data.responseText;
            }
            showToast("Error", "Can't set config\n" + message, 15000);
        }
    }

    loadYaml() {
        clearTimeout(this.loadYamlTimeout);
        $.get(this.url).done(this.onYamlLoaded.bind(this, true)).fail(this.onYamlLoaded.bind(this, false));
        $.get(this.readOnlyUrl).done(this.onReadOnlyLoaded.bind(this, true)).fail(this.onReadOnlyLoaded.bind(this, false));
    }

    onReadOnlyLoaded(success, data) {
        if (success && data.hasOwnProperty('ReadOnly')) {
            this.changeReadOnlyState(data.ReadOnly);
        }
    }

    onYamlLoaded(success, data) {
        if (success) {
            this.cluster = data.Response.identity.cluster;
            $('#yaml-cluster').text(data.Response.identity.cluster);

            this.version = data.Response.identity.version;
            $('#yaml-version').text(data.Response.identity.version);

            if (this.config !== data.Response.config) {
                this.codeMirror.setValue((data.Response.config !== undefined) ? data.Response.config : "");
                this.codeMirror.trigger('fold', 'editor.foldLevel2');
                this.config = data.Response.config;
            }

            if (data.Response.volatile_configs === undefined) {
                data.Response.volatile_configs = [];
            }

            if (JSON.stringify(this.volatileConfigs) !== JSON.stringify(data.Response.volatile_configs)) {
                this.volatileConfigs = data.Response.volatile_configs;
                var configsRoot = $("#yaml-volatile-configs");
                configsRoot.empty();
                this.codeMirrors = [];
                for (let volatileConfig of this.volatileConfigs) {
                    var remove = $(`
                        <button type="button" class="close float-left text-danger mx-2" aria-label="Close" title="Remove this volatile config">
                            <span aria-hidden="true">&times;</span>
                        </button>
                        `);
                    var self = this;
                    let id = volatileConfig.id;
                    remove.on('click', function(event) {
                        event.preventDefault();
                        showAck(`Remove volatile config #${id}?`, " ", "Yes", "No", self.removeVolatileConfig.bind(self, id));
                    });
                    var form = $(`<form style="flex:1 1 auto;"></form>`);
                    var copy = $(`
                        <div class="yaml-sticky-btn-wrap copy-yaml-config yaml-sticky-btn-wrap-volatile">
                            <div class="yaml-sticky-btn"></div>
                        </div>
                        `);

                    var div = $("<div></div>");
                    var head = $(`<h5 class="float-left">Volatile Config, id ${volatileConfig.id}:</h5>`);
                    div.attr('name', `volatile-yaml-config-${volatileConfig.id}`);
                    div.attr('id', `volatile-yaml-config-${volatileConfig.id}`);
                    form.append(copy);
                    form.append(div);
                    configsRoot.append(remove);
                    configsRoot.append(head);
                    configsRoot.append(form);
                    let cm = createEditor(div.get(0), true, 1068);
                    cm.setValue(volatileConfig.config);
                    copy.click(function() {
                        copyToClipboard(cm.getValue());
                    });

                    this.codeMirrors.push(cm);
                }
            }
        } else {
            showToast("Error", "Can't update config", 5000);
        }

        if (this.volatileConfigs.length) {
            this.maxVolatileId = this.volatileConfigs.slice(-1)[0].id;
        } else {
            this.maxVolatileId = -1;
        }

        $("#yaml-volatile-id").text(this.maxVolatileId + 1);

        this.loadYamlTimeout = setTimeout(this.loadYaml.bind(this), this.fetchInterval);
    }

    onVolatileConfigChanged(success, data) {
        if (success) {
            this.loadYaml();
        } else {
            showToast("Error", "Invalid volatile config\n" + data.responseJSON.issues, 15000);
        }
    }

    addVolatileConfig() {
        var cmd = {
            Request: {
                id: this.maxVolatileId + 1,
                cluster: this.cluster,
                version: this.version,
                config: this.volatileCodeMirror.getValue(),
            },
        };

        $.post(this.applyVolatileUrl, JSON.stringify(cmd))
         .done(this.onVolatileConfigChanged.bind(this, true))
         .fail(this.onVolatileConfigChanged.bind(this, false));

        this.volatileCodeMirror.setValue("");

        ++this.maxVolatileId;

        $("#yaml-volatile-id").text(this.maxVolatileId + 1);
    }

    removeAllVolatileConfigs() {
        var cmd = {
            Request: {
                identity: {
                    cluster: this.cluster,
                    version: this.version,
                },
                all: true
            },
        };

        $.post(this.removeVolatileUrl, JSON.stringify(cmd))
         .done(this.onVolatileConfigChanged.bind(this, true))
         .fail(this.onVolatileConfigChanged.bind(this, false));
    }

    removeVolatileConfig(id) {
        var cmd = {
            Request: {
                identity: {
                    cluster: this.cluster,
                    version: this.version,
                },
                ids: { ids: [id], },
            }
        };

        $.post(this.removeVolatileUrl, JSON.stringify(cmd))
         .done(this.onVolatileConfigChanged.bind(this, true))
         .fail(this.onVolatileConfigChanged.bind(this, false));
    }

    onResolved(success, data) {
        if (success) {
            this.resolvedCodeMirror.setValue(data.Response.config);
        } else {
            showToast("Error", "Config resolution error\n" + data.responseJSON.issues, 15000);
        }

    }

    resolveAll() {
        var cmd = {
            Request: {
                config: yamlCollectConfig(),
                volatile_configs: yamlCollectVolatileConfigs(),
            },
        };

        $.post(this.resolveAllUrl, JSON.stringify(cmd))
         .done(this.onResolved.bind(this, true))
         .fail(this.onResolved.bind(this, false));
    }

    resolveForLabels() {
        var labels = [];
        var rawLabels = yamlCollectLabels();
        for (let label in rawLabels) {
            labels.push({
                Label: label,
                Value: rawLabels[label],
            });
        }
        var cmd = {
            Request: {
                config: yamlCollectConfig(),
                volatile_configs: yamlCollectVolatileConfigs(),
                labels: labels,
            },
        };

        $.post(this.resolveUrl, JSON.stringify(cmd))
         .done(this.onResolved.bind(this, true))
         .fail(this.onResolved.bind(this, false));
    }

    initTab() {
        var self = this;
        this.codeMirror = createEditor($("#main-editor-container").get(0), this.readOnly, 1068);
        this.config = "";
        this.codeMirror.setValue(this.config);

        this.volatileCodeMirror = createEditor(document.getElementById("volatile-yaml-config-item"), false, 1068);

        this.volatileConfig = "";
        this.volatileCodeMirror.setValue(this.volatileConfig);

        this.additionalResolveCodeMirror = createEditor($("#additional-yaml-config-resolve").get(0), false, 1034);
        this.additionalResolveCodeMirror.setValue("");

        $("#copy-additional-yaml-config").click(function() {
            copyToClipboard(self.additionalResolveCodeMirror.getValue());
        });

        $("#link-additional-yaml-config").click(function() {
            $(".yaml-btn").first().click();
            self.volatileCodeMirror.setValue(self.additionalResolveCodeMirror.getValue());
        });

        this.arbitraryResolveCodeMirror = createEditor($("#arbitrary-yaml-config-resolve").get(0), false, 1034);
        this.arbitraryResolveCodeMirror.setValue("");

        $("#copy-arbitrary-yaml-config").click(function() {
            copyToClipboard(self.arbitraryResolveCodeMirror.getValue());
        });

        $("#fold-arbitrary-yaml-config").click(function() {
            self.arbitraryResolveCodeMirror.trigger('fold', 'editor.foldLevel2');
        });

        $("#unfold-arbitrary-yaml-config").click(function() {
            self.arbitraryResolveCodeMirror.trigger('fold', 'editor.unfoldAll');
        });

        var elemResolved = $("#yaml-config-resolved");

        var copyResolved = $(`
            <div class="yaml-sticky-btn-wrap copy-yaml-config" style="margin-top: 5px">
                <div class="yaml-sticky-btn"></div>
            </div>
            `);

        elemResolved.parent().prepend(copyResolved);

        this.resolvedCodeMirror = createEditor(elemResolved.get(0), true, 1050);
        this.resolvedCodeMirror.setValue("");

        copyResolved.click(function() {
            copyToClipboard(this.resolvedCodeMirror.getValue());
        });

        $("#yaml-label-add").click(() => addLabel());
        $("#yaml-label-clear").click(() => clearLabels());

        $(".yaml-floater").children().click(function() {
            var floater = $(this);
            var submenu = $(floater.attr('data-target'));
            if (!$(".yaml-floater").children().hasClass("active")) {
                floater.addClass("active");
                submenu.addClass("active");
                setTimeout(() => hideOnClickOutside(".yaml-submenu"), 100);
            }
        });

        $("#link-yaml-config").click(function() {
            $(".yaml-btn").first().click();
            $("#yaml-arbitrary-config-tab").click();
            self.arbitraryResolveCodeMirror.setValue(self.codeMirror.getValue());
        });

        $("#copy-yaml-config").click(function() {
            copyToClipboard(self.codeMirror.getValue());
        });

        $("#fold-yaml-config").click(function() {
            self.codeMirror.trigger('fold', 'editor.foldLevel2');
        });

        $("#unfold-yaml-config").click(function() {
            self.codeMirror.trigger('fold', 'editor.unfoldAll');
        });

        $("#copy-volatile-yaml-config").click(function() {
            copyToClipboard(self.volatileCodeMirror.getValue());
        });

        $("#link-volatile-yaml-config").click(function() {
            $(".yaml-btn").first().click();
            $("#yaml-current-config-tab").click();
            $("#yaml-resolve-include-volatile").prop('checked', true);
            self.additionalResolveCodeMirror.setValue(self.volatileCodeMirror.getValue());
        });

        $("#volatile-yaml-resolve-all").click(() => self.resolveAll());
        $("#volatile-yaml-resolve-for-labels").click(() => self.resolveForLabels());

        $('#volatile-yaml-apply-button').on('click', function(event) {
            event.preventDefault();
            showAck("Add volatile config?", " ", "Yes", "No", self.addVolatileConfig.bind(self));
        });

        $('#volatile-yaml-remove-all').on('click', function(event) {
            event.preventDefault();
            showAck("Remove all volatile configs?", " ", "Yes", "No", self.removeAllVolatileConfigs.bind(self));
        });

        $('.codeeditor-yaml').each(function () {
            var self = $(this);
            var copy = $(`
                <div class="yaml-sticky-btn-wrap copy-yaml-config">
                    <div class="yaml-sticky-btn"></div>
                </div>
                `);

            self.parent().prepend(copy);

            var value = self.text();
            self.text("");

            let cm = createEditor(self.get(0), true, 500);

            cm.setValue(value);

            copy.click(function() {
                copyToClipboard(cm.getValue());
            });
        });

        this.loadYaml();
    }
}

function initYamlConfigTab() {
    yamlConfigState = new YamlConfigState();
}
