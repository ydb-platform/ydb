// Presentation of the exported typed AST, never a second SQL encoding.
export function formulaIndex(document) {
  if (document?.format !== 'ydb-rbo-operator-formulas' || document.version !== 1
    || !Array.isArray(document.terms) || document.terms.length > 200000)
    throw new Error('Unsupported operator formula document or display limit exceeded');
  const terms = new Map();
  let edges = 0;
  const ref = id => { if (!terms.has(id)) throw new Error(`Unknown formula reference ${id}`); };
  for (const term of document.terms) {
    if (!term || typeof term.id !== 'string' || terms.has(term.id) || typeof term.op !== 'string'
      || typeof term.sort !== 'string' || !Array.isArray(term.args)) throw new Error('Invalid formula term');
    if ((edges += term.args.length) > 1000000) throw new Error('Formula dependency display limit exceeded');
    term.args.forEach(ref); // Child-first order also rules out cycles.
    if (['symbol', 'int', 'bool'].includes(term.op)
      && (term.args.length || typeof term.atom !== (term.op === 'bool' ? 'boolean' : 'string')))
      throw new Error('Invalid formula atom');
    if (term.op === 'int' && (term.sort !== 'Int' || !/^(?:0|-?[1-9]\d*)$/.test(term.atom)))
      throw new Error('Invalid integer formula atom');
    if (term.op === 'bool' && term.sort !== 'Bool') throw new Error('Invalid Boolean formula atom');
    if ((term.op === 'not' && term.args.length !== 1) || (term.op === 'ite' && term.args.length !== 3))
      throw new Error('Invalid formula operator arity');
    if (['forall', 'exists'].includes(term.op) && (term.args.length < 2 || term.sort !== 'Bool'
      || term.args.slice(0, -1).some(id => terms.get(id).op !== 'symbol')
      || terms.get(term.args.at(-1)).sort !== 'Bool')) throw new Error('Invalid formula quantifier');
    terms.set(term.id, term);
  }
  if (!Array.isArray(document.assertions) || !Array.isArray(document.declarations))
    throw new Error('Formula declarations/assertions missing');
  document.assertions.forEach(ref);
  for (const declaration of document.declarations) {
    if (!declaration || typeof declaration.name !== 'string'
      || !['function', 'product', 'definition'].includes(declaration.kind))
      throw new Error('Invalid formula declaration');
    if (declaration.kind === 'product' && (!Array.isArray(declaration.fields)
      || declaration.fields.some(field => !field || typeof field.selector !== 'string' || typeof field.sort !== 'string')))
      throw new Error('Invalid product declaration fields');
    if (declaration.kind === 'definition') {
      if (!Array.isArray(declaration.parameters)) throw new Error('Invalid definition parameters');
      declaration.parameters.forEach(ref); ref(declaration.body);
    }
  }
  for (const side of ['before', 'after']) {
    if (!Array.isArray(document[side]?.operators)) throw new Error(`Missing ${side} operator formulas`);
    for (const event of document[side].operators) {
      if (typeof event.node !== 'string' || !event.scope || typeof event.scope !== 'object')
        throw new Error('Invalid formula operator event');
      for (const field of formulaFields(event.result)) ref(field.ref);
    }
  }
  ref(document.comparison?.counterexample);
  if (document.comparison.soundness_exclusion != null) ref(document.comparison.soundness_exclusion);
  return terms;
}

export function formulaFields(family) {
  if (!Array.isArray(family?.outcomes)) throw new Error('Missing symbolic outcome family');
  const fields = [];
  const add = (label, ref) => { if (typeof ref !== 'string') throw new Error(`Invalid term for ${label}`);
    fields.push({label, ref}); };
  for (const outcome of family.outcomes) {
    const prefix = `Outcome ${outcome.index}`;
    add(`${prefix} · enabled`, outcome.enabled);
    add(`${prefix} · error`, outcome.error);
    for (const choice of outcome.choices || []) add(`${prefix} · choice [0, ${choice.bound})`, choice.term);
    if (!Array.isArray(outcome.rows)) throw new Error('Missing symbolic row slots');
    for (const row of outcome.rows) {
      const slot = `${prefix} · slot ${row.slot}`;
      add(`${slot} · present`, row.present);
      if (row.ordinal != null) add(`${slot} · ordinal`, row.ordinal);
      if (!Array.isArray(row.values)) throw new Error('Missing symbolic row values');
      for (const value of row.values) {
        const cell = `${slot} · ${value.column}`;
        add(`${cell} · NULL?`, value.is_null);
        add(`${cell} · payload (${value.type})`, value.value);
        for (const metadata of value.metadata || [])
          for (const [name, term] of Object.entries(metadata.terms || {}))
            add(`${cell} · ${metadata.kind}/${name} (${metadata.role})`, term);
      }
    }
  }
  return fields;
}

// A bounded abbreviation. References mark every omitted subtree explicitly.
export function readableTerm(terms, id, depth = 3, budget = 480) {
  let remaining = budget;
  const format = (key, level) => {
    const term = terms.get(key);
    if (!term) return `?${key}`;
    remaining -= 12;
    if (remaining < 0 || level < 0) return `@${key}`;
    if (['symbol', 'int', 'bool'].includes(term.op)) {
      const atom = String(term.atom);
      remaining -= atom.length;
      return remaining < 0 ? `@${key}` : atom;
    }
    if (term.args.length > 8) return `${term.op}(${term.args.slice(0, 6).map(child =>
      format(child, level - 1)).join(', ')}, … ${term.args.length - 6} more arguments at @${key})`;
    const args = term.args.map(child => format(child, level - 1));
    const infix = {'and': ' ∧ ', 'or': ' ∨ ', '=': ' = ', '<': ' < ', '+': ' + ', '*': ' × '};
    if (Object.hasOwn(infix, term.op) && args.length >= 2) return `(${args.join(infix[term.op])})`;
    if (term.op === 'not') return `¬${args[0]}`;
    if (term.op === 'ite') return `if ${args[0]} then ${args[1]} else ${args[2]}`;
    if (['forall', 'exists'].includes(term.op)) return `${term.op === 'forall' ? '∀' : '∃'} ${
      term.args.slice(0, -1).map(child => `${format(child, 0)}:${terms.get(child).sort}`).join(', ')} · ${args.at(-1)}`;
    return `${term.op}(${args.join(', ')})`;
  };
  return format(id, depth);
}
