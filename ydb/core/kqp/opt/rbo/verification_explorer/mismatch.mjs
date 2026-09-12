// Explain saved normalized root observations, not operator correspondence or
// verifier semantics. A recorded descriptor is mandatory; a difference between
// two arbitrary schedules is not evidence of a counterexample.

const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const integerType = /^(?:Int(?:8|16|32|64)|Uint(?:8|16|32|64)|Date|Decimal\(\d+,\d+\))$/;
const own = (value, key) => Object.hasOwn(value, key);

// Display only: keep the typed raw payload available to the caller and leave
// comparison keys unchanged. Decimal markers are global, not precision-scaled
// (rbo_verifier/decimal.py); finite payloads are signed scaled integers.
export function formatCell(cell) {
  const value = cell?.value;
  if (value === undefined) return 'Not recorded';
  if (value === null) return 'NULL';
  const raw = typeof value === 'object' ? JSON.stringify(value) : String(value);
  if (typeof cell.type !== 'string' || !cell.type.startsWith('Decimal(')) return raw;
  const type = /^Decimal\(([1-9]|[12][0-9]|3[0-5]),(0|[1-9]|[12][0-9]|3[0-5])\)$/.exec(cell.type);
  if (typeof value === 'number' && !Number.isSafeInteger(value)) return 'Unreliable Decimal payload (already rounded)';
  if (!type || Number(type[2]) > Number(type[1]) || !['number', 'string'].includes(typeof value)
    || !/^(?:0|-?[1-9][0-9]*)$/.test(raw)) return `Raw Decimal payload: ${raw}`;
  const scaled = BigInt(raw), infinity = 10n ** 35n;
  if (scaled === infinity + 1n) return 'NaN';
  if (scaled === infinity) return '+∞';
  if (scaled === -infinity) return '−∞';
  const absolute = scaled < 0n ? -scaled : scaled;
  if (absolute >= 10n ** BigInt(type[1])) return `Raw Decimal payload: ${raw}`;
  const scale = Number(type[2]), digits = absolute.toString().padStart(scale + 1, '0');
  return (scaled < 0n ? '-' : '') + (scale ? `${digits.slice(0, -scale)}.${digits.slice(-scale)}` : digits);
}

function cellKey(cell) {
  if (cell.value === null) return JSON.stringify([cell.type, null]);
  if (integerType.test(cell.type)) {
    const value = cell.value;
    if ((typeof value === 'number' && !Number.isSafeInteger(value))
      || !['number', 'string'].includes(typeof value) || !/^-?\d+$/.test(String(value))) {
      throw new Error('An integer/Decimal value is unavailable or already rounded; use the lossless artifact decoder.');
    }
    return JSON.stringify([cell.type, BigInt(value).toString()]);
  }
  if ((['String', 'Utf8'].includes(cell.type) && typeof cell.value === 'string')
    || (cell.type === 'Bool' && typeof cell.value === 'boolean')) return JSON.stringify([cell.type, cell.value]);
  throw new Error(`Exact display comparison for non-NULL ${cell.type} is unavailable.`);
}

function rowsOf(selection) {
  const {family, outcome} = selection;
  if (!Array.isArray(family.columns) || !Array.isArray(outcome.rows)) throw new Error('Columns or candidate rows are not recorded.');
  const names = new Set();
  for (const column of family.columns) {
    if (!object(column) || typeof column.name !== 'string' || typeof column.type !== 'string' || names.has(column.name)) {
      throw new Error('Recorded output columns are incomplete or ambiguous.');
    }
    names.add(column.name);
  }
  const rows = [];
  for (const row of outcome.rows) {
    if (!object(row) || typeof row.present !== 'boolean') throw new Error('A candidate row has no Boolean presence value.');
    if (!row.present) continue;
    if (!Number.isSafeInteger(row.slot) || row.slot < 0 || !Array.isArray(row.values)) throw new Error('A present row has no recorded slot or cells.');
    const values = family.columns.map(column => {
      const matching = row.values.filter(cell => object(cell) && cell.column === column.name);
      if (matching.length !== 1 || !own(matching[0], 'value') || matching[0].type !== column.type) {
        throw new Error(`Typed value for ${column.name} is missing or ambiguous; missing is not NULL.`);
      }
      const cell = matching[0];
      if (own(cell, 'average_state') || own(cell, 'integral_average_certificate')) {
        throw new Error('AVG state/certificate semantics cannot be reconstructed from the displayed payload; inspect the exact outcome.');
      }
      return {column: column.name, type: cell.type, value: cell.value};
    });
    rows.push({slot: row.slot, values, key: JSON.stringify(values.map(cellKey))});
  }
  return rows;
}

function bag(rows) {
  const result = new Map();
  for (const row of rows) {
    if (!result.has(row.key)) result.set(row.key, {values: row.values, slots: []});
    result.get(row.key).slots.push(row.slot);
  }
  return result;
}

function multiplicities(sourceRows, targetRows) {
  const source = bag(sourceRows), target = bag(targetRows);
  const differences = [];
  for (const key of new Set([...source.keys(), ...target.keys()])) {
    const a = source.get(key), b = target.get(key);
    const sourceSlots = a?.slots || [], targetSlots = b?.slots || [];
    if (sourceSlots.length !== targetSlots.length) differences.push({kind: 'multiplicity',
      values: (a || b).values, sourceCount: sourceSlots.length, targetCount: targetSlots.length,
      sourceSlots, targetSlots});
  }
  return differences;
}

/**
 * Index arguments are recorded outcome.index values, not array offsets.
 * source/target include original family/outcome refs for read-only UI tables.
 * differences: status; language; multiplicity(values/counts/slots); order;
 * row(position/source/target); cell(position/slots/columnIndex/typed values).
 * Bag differences never invent a pairing between unequal rows.
 */
export function explainMismatch(trace, sourceSide, outcomeIndex, targetOutcomeIndex) {
  const result = {status: 'unavailable', descriptor: null, semantics: null, source: null, target: null,
    targets: [], summary: '', differences: [], issues: [],
    basis: 'Recorded mismatch descriptor; pairwise annotations explain saved modeled values, not a runtime bug or all schedules.'};
  const fail = message => { result.issues.push(message); result.summary = message; return result; };
  if (!['before', 'after'].includes(sourceSide)) return fail('Choose an initial or final recorded mismatch.');
  if (trace?.format !== 'ydb-rbo-concrete-trace' || trace.version !== 1 || trace.status !== 'COUNTEREXAMPLE') {
    return fail('A v1 concrete counterexample trace is required.');
  }
  const comparison = trace.trace?.comparison;
  if (!object(comparison) || !['bag', 'sequence'].includes(comparison.semantics)) {
    return fail('Normalized root comparison is not recorded; boundary families are not a substitute.');
  }
  result.semantics = comparison.semantics;
  const targetSide = sourceSide === 'before' ? 'after' : 'before';
  const sourceFamily = comparison[sourceSide], targetFamily = comparison[targetSide];
  for (const family of [sourceFamily, targetFamily]) {
    if (!object(family) || !Array.isArray(family.outcomes)) return fail('Enabled root outcomes are not recorded.');
    const ids = new Set();
    for (const outcome of family.outcomes) {
      if (!object(outcome) || !Number.isSafeInteger(outcome.index) || outcome.index < 0 || ids.has(outcome.index)
        || !['success', 'error'].includes(outcome.status)) return fail('Recorded outcome indices/statuses are missing or ambiguous.');
      ids.add(outcome.index);
    }
  }
  const descriptors = Array.isArray(trace.mismatches) ? trace.mismatches.filter(item => object(item)
    && item.source === sourceSide && (outcomeIndex == null ? item.outcome == null : item.outcome === outcomeIndex)) : [];
  if (descriptors.length !== 1) return fail('This source outcome has no unique recorded mismatch descriptor.');
  result.descriptor = descriptors[0];
  result.targets = targetFamily.outcomes.map(outcome => ({index: outcome.index, status: outcome.status}));
  const select = (side, family, outcome) => ({side, index: outcome?.index ?? null,
    status: outcome?.status ?? 'no_enabled_outcomes', family, outcome});
  if (outcomeIndex == null && result.descriptor.reason === 'no_enabled_outcomes') {
    result.source = select(sourceSide, sourceFamily, null);
    if (sourceFamily.outcomes.length) {
      result.status = 'inconsistent'; return fail('The empty-language descriptor conflicts with recorded enabled outcomes.');
    }
    result.status = 'explained';
    result.summary = `The recorded ${sourceSide} family has no enabled outcomes.`;
    result.differences.push({kind: 'language', sourceCount: 0, targetCount: targetFamily.outcomes.length});
    return result;
  }
  if (!Array.isArray(result.descriptor.matching_outcomes) || result.descriptor.matching_outcomes.length) {
    return fail('The selected descriptor does not record an unmatched outcome.');
  }
  const sourceOutcome = sourceFamily.outcomes.find(outcome => outcome.index === outcomeIndex);
  if (!sourceOutcome) return fail('The descriptor source is not among the recorded enabled outcomes.');
  result.source = select(sourceSide, sourceFamily, sourceOutcome);
  if (!targetFamily.outcomes.length) {
    result.status = 'explained';
    result.summary = `No enabled ${targetSide} outcome is recorded to match ${sourceSide} outcome ${outcomeIndex}.`;
    result.differences.push({kind: 'language', sourceCount: sourceFamily.outcomes.length, targetCount: 0});
    return result;
  }
  if (targetOutcomeIndex === undefined && targetFamily.outcomes.length === 1) targetOutcomeIndex = targetFamily.outcomes[0].index;
  if (targetOutcomeIndex === undefined) return fail('Choose an enabled opposite outcome to inspect; no arbitrary schedule is selected.');
  const targetOutcome = targetFamily.outcomes.find(outcome => outcome.index === targetOutcomeIndex);
  if (!targetOutcome) return fail('The selected opposite outcome is not recorded as enabled.');
  result.target = select(targetSide, targetFamily, targetOutcome);
  if (sourceOutcome.status !== targetOutcome.status) {
    result.status = 'explained';
    result.summary = `Recorded ${sourceSide} outcome ${outcomeIndex} is ${sourceOutcome.status}; ${targetSide} outcome ${targetOutcomeIndex} is ${targetOutcome.status}.`;
    result.differences.push({kind: 'status', sourceStatus: sourceOutcome.status, targetStatus: targetOutcome.status});
    return result;
  }
  if (sourceOutcome.status === 'error') {
    result.status = 'inconsistent';
    return fail('Both recorded outcomes are query errors; the buffered error contract does not distinguish their payloads.');
  }
  try {
    if (!Array.isArray(sourceFamily.columns) || !Array.isArray(targetFamily.columns)
      || sourceFamily.columns.length !== targetFamily.columns.length) return fail('Comparable output-column positions are unavailable.');
    const sourceRows = rowsOf(result.source), targetRows = rowsOf(result.target);
    if (result.semantics === 'bag') result.differences = multiplicities(sourceRows, targetRows);
    else {
      if (sourceOutcome.sequence !== true || targetOutcome.sequence !== true) return fail('Normalized sequence observations are unavailable.');
      // trace.py emits present rows in recorded ordinal order. Do not sort by
      // slot, payload or a guessed cross-plan key; slot is only an annotation.
      for (let position = 0; position < Math.max(sourceRows.length, targetRows.length); position++) {
        const source = sourceRows[position], target = targetRows[position];
        if (!source || !target) {
          const row = item => item ? {slot: item.slot, values: item.values} : null;
          result.differences.push({kind: 'row', position, source: row(source), target: row(target)});
          continue;
        }
        source.values.forEach((cell, columnIndex) => {
          const opposite = target.values[columnIndex];
          if (cellKey(cell) !== cellKey(opposite)) result.differences.push({kind: 'cell', position,
            sourceSlot: source.slot, targetSlot: target.slot, columnIndex,
            sourceColumn: cell.column, targetColumn: opposite.column,
            source: {type: cell.type, value: cell.value}, target: {type: opposite.type, value: opposite.value}});
        });
      }
      if (result.differences.length && !multiplicities(sourceRows, targetRows).length) {
        result.differences.unshift({kind: 'order', sourceCount: sourceRows.length, targetCount: targetRows.length});
      }
    }
  } catch (error) { result.differences = []; return fail(error.message); }
  if (!result.differences.length) {
    result.status = 'inconsistent';
    return fail('The selected recorded observations agree; they do not explain the unmatched descriptor.');
  }
  result.status = 'explained';
  result.summary = result.differences[0].kind === 'order'
    ? 'The same recorded rows occur in a different sequence order.'
    : result.semantics === 'bag' ? 'Recorded whole-row multiplicities differ; unequal rows are not implicitly paired.'
      : 'Recorded values or row counts differ at the shown sequence positions.';
  return result;
}
