(function() {
    
    tinymce.PluginManager.requireLangPack('grappelli');
    var DOM = tinymce.DOM;
    
    tinymce.create("tinymce.plugins.Grappelli", {
        init: function(ed, url) {
            var t = this;
            tb = ed.getParam("grappelli_adv_toolbar", "toolbar2");
            documentstructure_css = url + "../../../themes/advanced/skins/grappelli/content_documentstructure_" + ed.settings.language + ".css";
            cookie_date = new Date();
            var year = cookie_date.getFullYear();
            cookie_date.setYear(year + 1);
            
            // get/set cookie
            cookie_grappelli_show_documentstructure = tinymce.util.Cookie.get('grappelli_show_documentstructure');
            if (cookie_grappelli_show_documentstructure != null) {
                ed.settings.grappelli_show_documentstructure = cookie_grappelli_show_documentstructure;
            } else {
                tinymce.util.Cookie.set('grappelli_show_documentstructure', ed.settings.grappelli_show_documentstructure, cookie_date, '/');
            }
            
            ed.onInit.add(function() {
                if ("mce_fullscreen" == ed.id) {
                    ed.dom.addClass(ed.dom.select('body'), 'fullscreen');
                }
                if (ed.settings.grappelli_adv_hidden) {
                    t._hide_adv_menu(ed);
                } else {
                    t._show_adv_menu(ed);
                }
                if (ed.settings.grappelli_show_documentstructure == "on") {
                    t._show_documentstructure(ed);
                } else {
                    t._hide_documentstructure(ed);
                }
            });
            
            // ADD COMMAND for SHOW/HIDE ADVANCED MENU
            ed.addCommand("Grappelli_Adv", function() {
                if (DOM.isHidden(ed.controlManager.get(tb).id)) {
                    t._show_adv_menu(ed);
                } else {
                    t._hide_adv_menu(ed);
                }
            });
            
            // ADD COMMAND for SHOW/HIDE DOCUMENTSTRUCTURE
            ed.addCommand("Grappelli_DocumentStructure", function() {
                i = ed.controlManager;
                if (ed.settings.grappelli_show_documentstructure == "on") {
                    t._hide_documentstructure(ed);
                } else {
                    t._show_documentstructure(ed);
                }
            });
            
            // ADD BUTTON: ADVANCED MENU
            ed.addButton("grappelli_adv", {
                title: "grappelli.grappelli_adv_desc",
                cmd: "Grappelli_Adv"
            });
            
            // ADD BUTTON: DOCUMENT STRUCTURE
            ed.addButton("grappelli_documentstructure", {
                title: "grappelli.grappelli_documentstructure_desc",
                cmd: "Grappelli_DocumentStructure"
            });
            
            /// FULLSCREEN
            ed.onBeforeExecCommand.add(function(ed, cmd, ui, val) {
                if ("mceFullScreen" != cmd) {
                    return;
                };
                if ("mce_fullscreen" == ed.id) {
                    base_ed = tinyMCE.get(ed.settings.fullscreen_editor_id);
                    
                    /// ADVANCED MENU
                    if (!ed.settings.grappelli_adv_hidden) {
                        t._show_adv_menu(base_ed);
                    } else {
                        t._hide_adv_menu(base_ed);
                    }
                    
                    /// DOCUMENT STRUCTURE
                    if (ed.settings.grappelli_show_documentstructure == "on") {
                        t._show_documentstructure(base_ed);
                    } else {
                        t._hide_documentstructure(base_ed);
                    }
                    
                }
            });
            
            ed.addShortcut("alt+shift+z", ed.getLang("grappelli_adv_desc"), "Grappelli_Adv");
            
            // CONTENT HAS TO BE WITHIN A BLOCK-LEVEL-ELEMENT
            // ed.onNodeChange.add(function(ed, cm, e) {
            //     if (e.nodeName == "TD" || e.nodeName == "BODY") {
            //         cn = e.childNodes;
            //         for (i = cn.length - 1; i >= 0; i--) {
            //             c_cn = cn[i];
            //             if (c_cn.nodeType == 3 || (!ed.dom.isBlock(c_cn) && c_cn.nodeType != 8)) {
            //                 if (c_cn.nodeType != 3 || /[^\s]/g.test(c_cn.nodeValue)) {
            //                     bl = ed.dom.create('p');
            //                     bl.appendChild(c_cn.cloneNode(1));
            //                     new_cn = c_cn.parentNode.replaceChild(bl, c_cn);
            //                     // move caret
            //                     r = ed.getDoc().createRange();
            //                     r.setStart(bl, bl.nodeValue ? bl.nodeValue.length : 0);
            //                     r.setEnd(bl, bl.nodeValue ? bl.nodeValue.length : 0);
            //                     ed.selection.setRng(r);
            //                 }
            //             }
            //         }
            //     }
            // });
            
        },
        
        // INTERNAL: RESIZE
        _resizeIframe: function(ed, tb, b) {
            var iframe = ed.getContentAreaContainer().firstChild;
            DOM.setStyle(iframe, "height", iframe.clientHeight + b);
            ed.theme.deltaHeight += b
        },
        
        // INTERNAL: SHOW/HIDE DOCUMENT STRUCTURE
        _show_documentstructure: function(ed) {
            head = ed.getBody().previousSibling;
            var headChilds = head.childNodes;
            
            for (var i = 0; i < headChilds.length; i++) {
                if (headChilds[i].nodeName == "LINK" 
                    && headChilds[i].getAttribute('href') == documentstructure_css) {
                    // documentstructure_css is already set so...
                    return;
                }
            }
            var vs_link = document.createElement("link");
            vs_link.rel="stylesheet";
            vs_link.mce_href = documentstructure_css;
            vs_link.href = documentstructure_css;
            head.appendChild(vs_link);
            ed.settings.grappelli_show_documentstructure = 'on';
            tinymce.util.Cookie.set('grappelli_show_documentstructure', 'on', cookie_date, '/');
            ed.controlManager.setActive('grappelli_documentstructure', true);
        },
        _hide_documentstructure: function(ed) {
            head = ed.getBody().previousSibling;
            vs_link = null;
            
            var headChilds = head.childNodes;
            
            for (var i = 0; i < headChilds.length; i++) {
                if (headChilds[i].nodeName == "LINK" 
                    && headChilds[i].getAttribute('href') == documentstructure_css) {
                    // found the node with documentstructure_css
                    vs_link = headChilds[i];
                    break;
                }
            }
            
            if (vs_link !== null) {
                // if we found the node with documentstructure_css, delete it
                head.removeChild(vs_link);
                ed.settings.grappelli_show_documentstructure = 'off';
                tinymce.util.Cookie.set('grappelli_show_documentstructure', 'off', cookie_date, '/');
                ed.controlManager.setActive('grappelli_documentstructure', false);
            }
        },
        
        // INTERNAL: SHOW/HIDE ADVANCED MENU
        _show_adv_menu: function(ed) {
            if (ed.controlManager.get(tb, false)) {
                ed.controlManager.setActive("grappelli_adv", 1);
                DOM.show(ed.controlManager.get(tb).id);
                this._resizeIframe(ed, tb, -28);
                ed.settings.grappelli_adv_hidden = 0;
            }
        },
        _hide_adv_menu: function(ed) {
            if (ed.controlManager.get(tb, false)) {
                ed.controlManager.setActive("grappelli_adv", 0);
                DOM.hide(ed.controlManager.get(tb).id);
                this._resizeIframe(ed, tb, 28);
                ed.settings.grappelli_adv_hidden = 1;
            }
        },
        
        // GET INFO
        getInfo: function() {
            return {
                longname: "Grappelli Plugin",
                author: "vonautomatisch (patrick kranzlmueller)",
                authorurl: "http://vonautomatisch.at",
                infourl: "http://code.google.com/p/django-grappelli/",
                version: "1.1"
            }
        }
        
    });
    
    tinymce.PluginManager.add("grappelli", tinymce.plugins.Grappelli)
    
})();
