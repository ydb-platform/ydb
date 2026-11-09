<script type="text/ecmascript">
<![CDATA[
    var selectedGroups = [];

window.addEventListener("load", function() {
    const root = document.querySelector("svg");
    if (!root || !root.querySelector("[data-cs-panel]")) {
        return;
    }
    const ns = "http://www.w3.org/2000/svg";
    const clip = document.createElementNS(ns, "clipPath");
    clip.id = "cs-plan-overlay-clip";
    clip.setAttribute("clipPathUnits", "userSpaceOnUse");
    const path = document.createElementNS(ns, "path");
    path.setAttribute("clip-rule", "evenodd");
    clip.appendChild(path);
    root.appendChild(clip);
    // Query-wide highlights must leave holes for the stage-local CS surfaces.
    Array.from(root.children).filter(node => node.tagName === "rect").forEach(node => {
        const group = document.createElementNS(ns, "g");
        group.setAttribute("clip-path", "url(#cs-plan-overlay-clip)");
        root.insertBefore(group, node);
        group.appendChild(node);
    });
    function update() {
        const matrix = root.getScreenCTM();
        if (!matrix) {
            return;
        }
        const inverse = matrix.inverse();
        let shape = `M0 0H${root.width.baseVal.value}V${root.height.baseVal.value}H0Z`;
        root.querySelectorAll("[data-cs-panel]").forEach(panel => {
            const width = Number(panel.getAttribute("width"));
            const height = Number(panel.getAttribute("height"));
            if (!width || !height || getComputedStyle(panel).display === "none") {
                return;
            }
            const screen = panel.getScreenCTM();
            if (!screen) {
                return;
            }
            const transform = inverse.multiply(screen);
            const begin = new DOMPoint(0, 0).matrixTransform(transform);
            const end = new DOMPoint(width, height).matrixTransform(transform);
            shape += `M${begin.x} ${begin.y}H${end.x}V${end.y}H${begin.x}Z`;
        });
        path.setAttribute("d", shape);
    }
    let pending = false;
    new MutationObserver(function() {
        if (!pending) {
            pending = true;
            requestAnimationFrame(function() {
                pending = false;
                update();
            });
        }
    }).observe(root, {subtree: true, attributes: true, attributeFilter: ["height", "y", "style", "class"]});
    update();
});

function cs_content_height(svg, minimum) {
    let height = minimum;
    svg.querySelectorAll("foreignObject").forEach(cell => {
        const content = cell.firstElementChild;
        if (content) {
            height = Math.max(height, content.scrollHeight, content.offsetHeight);
        }
    });
    svg.querySelectorAll("text").forEach(text => {
        const bounds = text.getBBox();
        height = Math.max(height, bounds.y + bounds.height + 2);
    });
    return Math.ceil(height);
}

function cs_set_cell_height(svg, height) {
    svg.querySelectorAll("foreignObject").forEach(cell => cell.setAttribute("height", height));
}

function cs_layout_row(row) {
    const children = Array.from(row.children).filter(node => node.localName === "svg");
    const header = children.find(node => node.hasAttribute("data-cs-header")) || children[0];
    const body = children.find(node => node.hasAttribute("data-cs-children")) || children[1];
    const minimum = Number(row.getAttribute("data-cs-row-min-height")) || 24;
    const ownHeight = cs_content_height(header, minimum);
    header.setAttribute("height", ownHeight);
    row.setAttribute("data-cs-row-height", ownHeight);
    cs_set_cell_height(header, ownHeight);
    const background = header.querySelector(".cs-row-background");
    if (background) {
        background.setAttribute("height", ownHeight);
    }
    let bodyHeight = 0;
    if (body) {
        Array.from(body.children).filter(node => ["block", "meta", "step"].includes(node.getAttribute("data-cs-row"))).forEach(child => {
            child.setAttribute("y", bodyHeight);
            bodyHeight += cs_layout_row(child);
        });
        body.setAttribute("y", ownHeight);
        body.setAttribute("height", bodyHeight);
    }
    const height = ownHeight + (header.classList.contains("folded") ? 0 : bodyHeight);
    row.setAttribute("height", height);
    return height;
}

function cs_layout_read(read) {
    const firstRow = read.querySelector("[data-cs-row='block'],[data-cs-row='meta'],[data-cs-row='step']");
    if (!firstRow) {
        return Number(read.getAttribute("height"));
    }
    const body = read.querySelector("[data-cs-read-body]") || firstRow.parentElement;
    const title = Array.from(read.children).find(node => node.localName === "svg" && node !== body);
    const titleHeight = title ? cs_content_height(title, 16) : Number(body.getAttribute("y"));
    if (title) {
        title.setAttribute("height", titleHeight);
        cs_set_cell_height(title, titleHeight);
        const background = title.querySelector(":scope > g > rect");
        if (background) {
            background.setAttribute("height", titleHeight);
        }
    }
    const rows = Array.from(body.children).filter(node => ["block", "meta", "step"].includes(node.getAttribute("data-cs-row")));
    let height = Number(body.getAttribute("data-cs-content-top"));
    if (!body.hasAttribute("data-cs-content-top")) {
        height = Math.min(...rows.map(row => Number(row.getAttribute("y"))));
        body.setAttribute("data-cs-content-top", height);
    }
    rows.forEach(row => {
        row.setAttribute("y", height);
        height += cs_layout_row(row);
    });
    for (let footer = rows[rows.length - 1].nextElementSibling; footer; footer = footer.nextElementSibling) {
        if (footer.localName === "svg") {
            footer.setAttribute("y", height);
            height += Number(footer.getAttribute("height"));
        }
    }
    body.setAttribute("y", titleHeight);
    body.setAttribute("height", height);
    read.setAttribute("height", titleHeight + height);
    if (read.hasAttribute("data-height")) {
        read.setAttribute("data-height", titleHeight + height);
    }
    return titleHeight + height;
}

function cs_layout_storage(storage) {
    const rows = Array.from(storage.children).filter(node => node.hasAttribute("data-cs-storage-row"));
    if (!rows.length) {
        return Number(storage.getAttribute("height"));
    }
    let height = Number(storage.getAttribute("data-cs-content-top"));
    if (!storage.hasAttribute("data-cs-content-top")) {
        height = Math.min(...rows.map(row => Number(row.getAttribute("y"))));
        storage.setAttribute("data-cs-content-top", height);
    }
    rows.forEach(row => {
        const rowHeight = cs_content_height(row, 20);
        row.setAttribute("y", height);
        row.setAttribute("height", rowHeight);
        cs_set_cell_height(row, rowHeight);
        const background = row.querySelector(":scope > g > rect");
        if (background) {
            background.setAttribute("height", rowHeight);
        }
        height += rowHeight;
    });
    storage.setAttribute("height", height);
    return height;
}

function layout_cs_stats() {
    document.querySelectorAll("[data-cs-panel]").forEach(panel => {
        const open = getComputedStyle(panel).display !== "none";
        const oldHeight = Number(panel.getAttribute("height"));
        const display = panel.style.display;
        const visibility = panel.style.visibility;
        // Closed panes still need normal HTML layout before their first expansion.
        panel.style.visibility = "hidden";
        panel.style.display = "inline";
        let height = 0;
        Array.from(panel.children).filter(node => node.localName === "svg").forEach(child => {
            child.setAttribute("y", height);
            height += child.hasAttribute("data-cs-storage") ? cs_layout_storage(child) : cs_layout_read(child);
        });
        panel.style.display = display;
        panel.style.visibility = visibility;
        if (open) {
            resize_nodes(panel, height - oldHeight);
        } else {
            panel.setAttribute("height", 0);
        }
        panel.setAttribute("data-height", height);
    });
}

let cs_layout_frame = 0;
function schedule_cs_layout() {
    if (!cs_layout_frame) {
        cs_layout_frame = requestAnimationFrame(function() {
            cs_layout_frame = 0;
            layout_cs_stats();
        });
    }
}
window.addEventListener("load", schedule_cs_layout);
window.addEventListener("resize", schedule_cs_layout);
if (document.fonts) {
    document.fonts.ready.then(schedule_cs_layout);
}

function shift_nodes(node, delta) {
        while(node && node.tagName == "svg") {
            node.setAttribute("y", Number(node.getAttribute("y")) + delta);
            node = node.nextElementSibling;
        }
    }

    function resize_nodes(node, delta) {
    var origin = node;
        while(node && node.tagName == "svg") {
        if (node.hasAttribute("data-cs-panel") && getComputedStyle(node).display === "none") {
            node.setAttribute("data-height", Number(node.getAttribute("data-height")) + delta);
            return;
        }
            node.setAttribute("height", Number(node.getAttribute("height")) + delta);
        if (node !== origin && node.classList.contains("slimable") &&
            Array.from(node.children).some(function(child) { return child.hasAttribute("data-cs-panel"); })) {
            node.setAttribute("data-height", Number(node.getAttribute("data-height")) + delta);
        }
            shift_nodes(node.nextElementSibling, delta);
            node = node.parentElement;
        }
    }

function toggle_cs_stats(button) {
    layout_cs_stats();
    var stage = find_parent_svg(button);
    var panel = Array.from(stage.children).find(function(child) { return child.hasAttribute("data-cs-panel"); });
    if (!panel) return;
    toggle_slim_off(stage);
    var open = button.getAttribute("aria-expanded") === "true";
    if (open) {
        var height = Number(panel.getAttribute("height"));
        panel.setAttribute("data-height", height);
        resize_nodes(panel, -height);
        panel.style.display = "none";
    } else {
        panel.style.display = "inline";
        resize_nodes(panel, Number(panel.getAttribute("data-height")));
    }
    button.setAttribute("aria-expanded", String(!open));
    button.querySelector("path").setAttribute("transform", open ? "" : "rotate(90 4 5)");
}

window.addEventListener("keydown", function(e) {
    if ((e.key === "Enter" || e.key === " ") && e.target.classList.contains("cs-stage-toggle")) {
        e.preventDefault();
        toggle_cs_stats(e.target);
    }
});

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

    // The column header strip is an ordinary SVG element, so CSS positioning
    // does not apply to it: it is moved by hand to the top of the viewport on
    // every scroll instead. It is the last element of the document, so it paints
    // over the plans it slides across.
    function update_column_headers() {
        var headers = document.getElementById("columnHeaders");
        if (headers) {
            var y = window.scrollY || window.pageYOffset || 0;
            headers.setAttribute("transform", "translate(0," + y + ")");
        }
    }

    window.addEventListener("scroll", update_column_headers, false);

    window.onload = function() {
        update_column_headers();
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
        if (node.hasAttribute("data-cs-panel")) {
            return;
        }
        if (node.classList.contains("cs-stage-toggle")) {
            toggle_cs_stats(node);
            return;
        }
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
