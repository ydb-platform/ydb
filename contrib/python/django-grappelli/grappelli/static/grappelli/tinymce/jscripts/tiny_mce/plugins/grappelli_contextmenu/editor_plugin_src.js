(function() {
    
    tinymce.PluginManager.requireLangPack('grappelli_contextmenu');
    var Event = tinymce.dom.Event, each = tinymce.each, DOM = tinymce.DOM;
    
    tinymce.create('tinymce.plugins.ContextMenu', {
        init : function(ed) {
            var t = this;
            
            t.editor = ed;
            t.onContextMenu = new tinymce.util.Dispatcher(this);
            
            ed.onContextMenu.add(function(ed, e) {
                if (!e.ctrlKey) {
                    t._getMenu(ed).showMenu(e.clientX, e.clientY);
                    Event.add(ed.getDoc(), 'click', hide);
                    Event.cancel(e);
                }
            });
            
            function hide() {
                if (t._menu) {
                    t._menu.removeAll();
                    t._menu.destroy();
                    Event.remove(ed.getDoc(), 'click', hide);
                }
            };
            
            ed.onMouseDown.add(hide);
            ed.onKeyDown.add(hide);
            
            // Register commands
            // INSERT ELEMENTS
            ed.addCommand('mcePBefore', function() {
                ce = ed.selection.getNode();
                pe = ed.dom.getParent(ce, function(n) {
                    nn = n.nodeName;
                    if (nn == 'P' || nn == 'H1' || nn == 'H2' || nn == 'H3' || nn == 'H4' || nn == 'H5' || nn == 'H6' || nn == 'UL' || nn == 'OL' || nn == 'BLOCKQUOTE') {
                        return n;
                    }
                }, ed.dom.getRoot());
                if (pe) {
                    new_p = ed.dom.create('p', {}, '<br />');
                    pe.parentNode.insertBefore(new_p, pe);
                }
            });
            ed.addCommand('mcePAfter', function() {
                ce = ed.selection.getNode();
                pe = ed.dom.getParent(ce, function(n) {
                    nn = n.nodeName;
                    if (nn == 'P' || nn == 'H1' || nn == 'H2' || nn == 'H3' || nn == 'H4' || nn == 'H5' || nn == 'H6' || nn == 'UL' || nn == 'OL' || nn == 'BLOCKQUOTE') {
                        return n;
                    }
                }, ed.dom.getRoot());
                if (pe) {
                    new_p = ed.dom.create('p', {}, '<br />');
                    ed.dom.insertAfter(new_p, pe);
                }
            });
            
            // INSERT ROOT ELEMENTS
            ed.addCommand('mcePBeforeRoot', function() {
                ce = ed.selection.getNode();
                pe = ed.dom.getParent(ce, function(n) {
                    nn = n.nodeName;
                    nn_p = n.parentNode.nodeName;
                    if ((nn == 'P' || nn == 'H1' || nn == 'H2' || nn == 'H3' || nn == 'H4' || nn == 'H5' || nn == 'H6' || nn == 'UL' || nn == 'OL' || nn == 'BLOCKQUOTE') && nn_p == 'BODY') {
                        return n;
                    }
                }, ed.dom.getRoot());
                if (pe) {
                    new_p = ed.dom.create('p', {}, '<br />');
                    pe.parentNode.insertBefore(new_p, pe);
                }
            });
            ed.addCommand('mcePAfterRoot', function() {
                ce = ed.selection.getNode();
                pe = ed.dom.getParent(ce, function(n) {
                    nn = n.nodeName;
                    nn_p = n.parentNode.nodeName;
                    if ((nn == 'P' || nn == 'H1' || nn == 'H2' || nn == 'H3' || nn == 'H4' || nn == 'H5' || nn == 'H6' || nn == 'UL' || nn == 'OL' || nn == 'BLOCKQUOTE')  && nn_p == 'BODY') {
                        return n;
                    }
                }, ed.dom.getRoot());
                if (pe) {
                    new_p = ed.dom.create('p', {}, '<br />');
                    ed.dom.insertAfter(new_p, pe);
                }
            });
            
            // DELETE
            ed.addCommand('mceDelete', function() {
                ce = ed.selection.getNode();
                pe = ed.dom.getParent(ce, function(n) {
                    nn = n.nodeName;
                    if (nn == 'P' || nn == 'H1' || nn == 'H2' || nn == 'H3' || nn == 'H4' || nn == 'H5' || nn == 'H6' || nn == 'UL' || nn == 'OL' || nn == 'BLOCKQUOTE') {
                        return n;
                    }
                }, ed.dom.getRoot());
                if (pe) {
                    ed.dom.remove(pe);
                }
            });
            
            ed.addCommand('mceDeleteRoot', function() {
                ce = ed.selection.getNode();
                pe = ed.dom.getParent(ce, function(n) {
                    nn = n.nodeName;
                    nn_p = n.parentNode.nodeName;
                    if ((nn == 'P' || nn == 'H1' || nn == 'H2' || nn == 'H3' || nn == 'H4' || nn == 'H5' || nn == 'H6' || nn == 'UL' || nn == 'OL' || nn == 'BLOCKQUOTE')  && nn_p == 'BODY') {
                        return n;
                    }
                }, ed.dom.getRoot());
                if (pe) {
                    ed.dom.remove(pe);
                }
            });
            
            // MOVE
            ed.addCommand('mceMoveUp', function() {
                ce = ed.selection.getNode();
                pe = ed.dom.getParent(ce, function(n) {
                    nn = n.nodeName;
                    if (nn == 'P' || nn == 'H1' || nn == 'H2' || nn == 'H3' || nn == 'H4' || nn == 'H5' || nn == 'H6' || nn == 'UL' || nn == 'OL' || nn == 'BLOCKQUOTE') {
                        return n;
                    }
                }, ed.dom.getRoot());
                if (pe) {
                    pre_prev = t._getPreviousSibling(pe);
                    if (pre_prev) {
                        pre_prev.parentNode.insertBefore(pe, pre_prev);
                    }
                }
            });
            ed.addCommand('mceMoveUpRoot', function() {
                ce = ed.selection.getNode();
                pe = ed.dom.getParent(ce, function(n) {
                    nn = n.nodeName;
                    nn_p = n.parentNode.nodeName;
                    if ((nn == 'P' || nn == 'H1' || nn == 'H2' || nn == 'H3' || nn == 'H4' || nn == 'H5' || nn == 'H6' || nn == 'UL' || nn == 'OL' || nn == 'BLOCKQUOTE')  && nn_p == 'BODY') {
                        return n;
                    }
                }, ed.dom.getRoot());
                if (pe) {
                    pre_prev = t._getPreviousSibling(pe);
                    if (pre_prev) {
                        pre_prev.parentNode.insertBefore(pe, pre_prev);
                    }
                }
            });
            
        },
        
        getInfo : function() {
            return {
                longname : 'Grappelli (Contextmenu)',
                author : 'Patrick Kranzlmueller',
                authorurl : 'http://vonautomatisch.at',
                infourl : 'http://code.google.com/p/django-grappelli/',
                version : '0.1'
            };
        },
        
        _getMenu : function(ed) {
            var t = this, m = t._menu, se = ed.selection, col = se.isCollapsed(), el = se.getNode() || ed.getBody(), am, p1, p2;
            
            if (m) {
                m.removeAll();
                m.destroy();
            }
            
            p1 = DOM.getPos(ed.getContentAreaContainer());
            p2 = DOM.getPos(ed.getContainer());
            
            m = ed.controlManager.createDropMenu('contextmenu', {
                offset_x : p1.x + ed.getParam('contextmenu_offset_x', 0),
                offset_y : p1.y + ed.getParam('contextmenu_offset_y', 0),
                constrain : 1
            });
            
            t._menu = m;
            
            // parent element
            pe = ed.dom.getParent(el, function(n) {
                nn = n.nodeName;
                if (nn == 'P' || nn == 'H1' || nn == 'H2' || nn == 'H3' || nn == 'H4' || nn == 'H5' || nn == 'H6' || nn == 'UL' || nn == 'OL' || nn == 'BLOCKQUOTE') {
                    return n;
                }
            }, ed.dom.getRoot());
            // root element
            re = ed.dom.getParent(el, function(n) {
                nn = n.nodeName;
                nn_p = n.parentNode.nodeName;
                if (nn == 'P' || nn == 'H1' || nn == 'H2' || nn == 'H3' || nn == 'H4' || nn == 'H5' || nn == 'H6' || nn == 'UL' || nn == 'OL' || nn == 'BLOCKQUOTE'  && nn_p == 'BODY') {
                    return n;
                }
            }, ed.dom.getRoot());
            
            title_prefix = pe.nodeName;
            title_prefix_root = re.nodeName;
            
            title_b_before = 'grappelli_contextmenu.' + title_prefix + '_grappelli_contextmenu_insertpbefore_desc';
            title_b_after = 'grappelli_contextmenu.' + title_prefix + '_grappelli_contextmenu_insertpafter_desc';
            title_b_before_root = 'grappelli_contextmenu.' + title_prefix + '_grappelli_contextmenu_insertpbeforeroot_desc';
            title_b_after_root = 'grappelli_contextmenu.' + title_prefix + '_grappelli_contextmenu_insertpafterroot_desc';
            title_b_delete = 'grappelli_contextmenu.' + title_prefix + '_grappelli_contextmenu_delete_desc';
            title_b_delete_root = 'grappelli_contextmenu.' + title_prefix + '_grappelli_contextmenu_deleteroot_desc';
            title_b_moveup = 'grappelli_contextmenu.' + title_prefix + '_grappelli_contextmenu_moveup_desc';
            title_b_moveup_root = 'grappelli_contextmenu.' + title_prefix + '_grappelli_contextmenu_moveuproot_desc';
            
            m.add({title : title_b_before, icon : '', cmd : 'mcePBefore'});
            m.add({title : title_b_after, icon : '', cmd : 'mcePAfter'});
            
            if (pe.parentNode.nodeName != "BODY") {
                m.addSeparator();
                m.add({title : title_b_before_root, icon : '', cmd : 'mcePBeforeRoot'});
                m.add({title : title_b_after_root, icon : '', cmd : 'mcePAfterRoot'});
            }
            
            m.addSeparator();
            m.add({title : title_b_delete, icon : '', cmd : 'mceDelete'});
            if (pe.parentNode.nodeName != "BODY") {
                m.add({title : title_b_delete_root, icon : '', cmd : 'mceDeleteRoot'});
            }
            
            m.addSeparator();
            m.add({title : title_b_moveup, icon : '', cmd : 'mceMoveUp'});
            if (pe.parentNode.nodeName != "BODY") {
                m.add({title : title_b_moveup_root, icon : '', cmd : 'mceMoveUpRoot'});
            }
            
            t.onContextMenu.dispatch(t, m, el, col);
            
            return m;
            
        },
        
        _getPreviousSibling: function(obj) {
            var prevNode = obj.previousSibling;
            while(prevNode && (prevNode.nodeType == document.TEXT_NODE || prevNode.nodeType == document.CDATA_NODE) && prevNode.nodeValue.match(/^\s*$/)) {
                prevNode = prevNode.previousSibling;
            }
            return prevNode;
        }
        
    });
    
    // Register plugin
    tinymce.PluginManager.add('grappelli_contextmenu', tinymce.plugins.ContextMenu);
})();