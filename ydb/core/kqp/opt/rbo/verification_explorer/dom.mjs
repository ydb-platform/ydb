// Text-only DOM helpers. Imported evidence is never interpreted as HTML.
export const json = value => JSON.stringify(value, null, 2);
export const own = (record, key, fallback) => Object.hasOwn(record, key) ? record[key] : fallback;
export const title = text => String(text || '').replaceAll('_', ' ').replace(/\b\w/g, c => c.toUpperCase());

export function h(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else if (key === 'class') node.className = value;
    else if (key.startsWith('aria-') && value != null) node.setAttribute(key, String(value));
    else if (value != null && value !== false) node.setAttribute(key, value === true ? '' : value);
  }
  for (const child of children.flat(Infinity))
    if (child != null && child !== false)
      node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  return node;
}

export function svg(tag, attrs, ...children) {
  const node = document.createElementNS('http://www.w3.org/2000/svg', tag);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, String(value));
  for (const child of children)
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  return node;
}

export function button(text, action, className = '', attrs = {}) {
  return h('button', { class: className, onclick: action, type: 'button', ...attrs }, text);
}

export function empty(message, detail = '') {
  return h('div', { class: 'empty' }, h('p', {}, message), detail && h('p', { class: 'small-note' }, detail));
}

export function badge(label, tone = '') {
  return h('span', { class: `badge ${tone}` }, label);
}

export function cell(value) {
  if (value === null) return h('span', { class: 'null-value' }, 'NULL');
  if (value === undefined) return h('span', { class: 'null-value' }, 'not recorded');
  return typeof value === 'object' ? json(value) : String(value);
}

export function tableView(columns, rows) {
  if (!rows.length) return h('div', { class: 'empty-table' }, '∅ No rows');
  return h('div', { class: 'table-scroll' }, h('table', {},
    h('thead', {}, h('tr', {}, columns.map(c => h('th', {}, c)))),
    h('tbody', {}, rows.map(row => h('tr', {}, row.map(value => h('td', {}, cell(value))))))));
}
