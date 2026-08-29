<script type="text/ecmascript">
<![CDATA[
    var selectedGroups = [];

    function shift_nodes(node, delta) {
        while(node && node.tagName == "svg") {
            node.setAttribute("y", Number(node.getAttribute("y")) + delta);
            node = node.nextElementSibling;
        }
    }

    function resize_nodes(node, delta) {
        while(node && node.tagName == "svg") {
            node.setAttribute("height", Number(node.getAttribute("height")) + delta);
            shift_nodes(node.nextElementSibling, delta);
            node = node.parentElement;
        }
    }

    function calc_delta(node) {
        var delta = 0;
        while(node && node.tagName == "svg") {
            delta += Number(node.getAttribute("height"));
            node = node.nextElementSibling;
        }
        return delta;
    }

    function find_parent_svg(node) {
        while (node && node.tagName != "svg") {
            node = node.parentElement;
        }
        return node;
    }

    function toggle_fold(node) {
        if (node) {
            var delta = calc_delta(node.nextElementSibling);
            if (delta) {
                if (node.classList.contains("folded")) {
                    resize_nodes(node.parentElement, delta);
                    node.classList.remove("folded");
                } else {
                    resize_nodes(node.parentElement, -delta);
                    node.classList.add("folded");
                }
            }
        }
    }

    function toggle_slim_on(node) {
        if (node && node.classList.contains("slimable") && !node.classList.contains("slim")) {
            node.classList.add("slim");
            var delta = 18 - Number(node.getAttribute("height"));
            if (delta) {
                resize_nodes(node, delta);
            }
        }
    }

    function toggle_slim_off(node) {
        if (node && node.classList.contains("slim")) {
            node.classList.remove("slim");
            var delta = Number(node.getAttribute("data-height")) - Number(node.getAttribute("height"));
            if (delta) {
                resize_nodes(node, delta);
            }
        }
    }

    function expand_tree(node) {
        if (node) {
            for (var i = 0; i < node.children.length; i++) {
                var child = node.children[i];
                if (child.tagName == "svg") {
                    toggle_slim_off(child);
                    if (child.classList.contains("folded")) {
                        toggle_fold(child);
                    }
                    expand_tree(child)
                }
            }
        }
    }

    function tree_slim_on(node) {
        if (node) {
            for (var i = 0; i < node.children.length; i++) {
                var child = node.children[i];
                if (child.tagName == "svg") {
                    toggle_slim_on(child);
                    if (child.classList.contains("folded")) {
                        break;
                    }
                    tree_slim_on(child);
                }
            }
        }
    }

    function deselect_selected() {
        for (const group of selectedGroups) {
            var nodes = document.querySelectorAll("[data-group='" + group + "']");
            for (const node of nodes) {
                node.classList.remove("selected");
            }
        }
    }

    function select_selected() {
        for (const group of selectedGroups) {
            var nodes = document.querySelectorAll("[data-group='" + group + "']");
            for (const node of nodes) {
                node.classList.add("selected");
                toggle_slim_off(find_parent_svg(node));
            }
        }
    }

    function select_cpu_path(groups) {
        deselect_selected();
        selectedGroups = groups.split(',');
        select_selected();
    }

    function select_time_path(groups) {
        deselect_selected();
        selectedGroups = groups.split(',');
        select_selected();
    }

    function toggle_selection(node) {
        var group = node.getAttribute("data-group");
        if (group) {
            deselect_selected();
            if (selectedGroups.length == 1 && selectedGroups[0] == group) {
                selectedGroups = [];
            } else {
                selectedGroups = [group];
                select_selected();
            }
        }
    }

    window.onload = function() {
        var nodes = document.querySelectorAll(".selected");
        if (nodes.length > 0) {
            selectedGroups = [];
            for (const node of nodes) {
                selectedGroups.push(node.getAttribute("data-group"));
            }
        } else {
            select_selected();
        }
    }

    window.addEventListener("click", function(e) {
        var node = e.target;
        while (node) {
            if (node.classList.contains("button") && node.classList.contains("plus")) {
                toggle_fold(find_parent_svg(node));
                return;
            }
            if (node.classList.contains("button") && node.classList.contains("arup")) {
                toggle_slim_on(find_parent_svg(node));
                return;
            }
            if (node.classList.contains("button") && node.classList.contains("ardn")) {
                expand_tree(find_parent_svg(node));
                return;
            }
            if (node.classList.contains("button") && node.classList.contains("aruu")) {
                tree_slim_on(find_parent_svg(node));
                return;
            }
            if (node.classList.contains("selectable")) {
                toggle_slim_off(find_parent_svg(node));
                toggle_selection(node);
                return;
            }
            if (node.classList.contains("cpupath")) {
                select_cpu_path(node.getAttribute("data-groups"));
                return;
            }
            if (node.classList.contains("timepath")) {
                select_time_path(node.getAttribute("data-groups"));
                return;
            }
            node = node.parentElement;
        }
	}, false)
]]>
</script>
