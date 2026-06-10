#!/usr/bin/env python3
"""Analyze a bank-statement CSV.

Reports:
  * Income  (credit operations) grouped by income origin.
  * Outcome (debit  operations) grouped by reason of the outcome.

Both the origin and the reason are taken from the "Operation details" column.
All amounts are assumed to be in RUB.

CSV format (semicolon-separated):
  Transaction date;Issued by the bank;Document number;
  Amount in operation currency (credit);Amount in operation currency (debit);
  Operation currency;
  Amount in account currency (credit);Amount in account currency (debit);
  Account currency;Operation details;Card number

Amounts use a space as the thousands separator and a comma as the decimal
separator, e.g. "253 936,00".

Usage:
  python3 analyze_operations.py statement.csv
  python3 analyze_operations.py statement.csv --normalize   # merge similar details
  python3 analyze_operations.py statement.csv --csv out.csv # also dump groups to CSV
"""

import argparse
import csv
import re
import sys
from collections import defaultdict

# Column header names as they appear in the statement.
COL_CREDIT_ACC = "Amount in account currency (credit)"
COL_DEBIT_ACC = "Amount in account currency (debit)"
COL_CREDIT_OP = "Amount in operation currency (credit)"
COL_DEBIT_OP = "Amount in operation currency (debit)"
COL_DETAILS = "Operation details"

# Positional fallback (0-indexed) if the headers don't match exactly.
POS = {
    COL_CREDIT_OP: 3,
    COL_DEBIT_OP: 4,
    COL_CREDIT_ACC: 6,
    COL_DEBIT_ACC: 7,
    COL_DETAILS: 9,
}


def parse_amount(raw):
    """Turn '253 936,00' (or '253\xa0936,00') into the float 253936.0."""
    if raw is None:
        return 0.0
    s = raw.strip()
    if not s:
        return 0.0
    # Drop every kind of space used as a thousands separator.
    for space in (" ", "\xa0", " ", "\t"):
        s = s.replace(space, "")
    s = s.replace(",", ".")
    # Keep only digits, sign and the decimal dot.
    s = re.sub(r"[^0-9.\-]", "", s)
    if s in ("", "-", ".", "-."):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def fmt_amount(value):
    """Render 253936.0 back as '253 936,00 RUB' to match the source style."""
    s = f"{value:,.2f}"            # '253,936.00'
    s = s.replace(",", " ")   # group separator -> non-breaking space
    s = s.replace(".", ",")        # decimal point   -> comma
    return f"{s} RUB"


def normalize_key(text):
    """Collapse near-duplicate descriptions into one bucket.

    Removes dates, long digit runs (card/document/account numbers) and squashes
    whitespace so that e.g. two transfers that differ only by document number
    land in the same group.
    """
    if not text:
        return "(no details)"
    t = text
    t = re.sub(r"\d{1,2}[./-]\d{1,2}[./-]\d{2,4}", " ", t)  # dates
    t = re.sub(r"\d[\d\s./-]{4,}\d", " ", t)                # long number runs
    t = re.sub(r"\s+", " ", t).strip(" ,.;:-")
    return t or "(no details)"


def pick(row, header_name, headers):
    """Read a cell by header name, falling back to a fixed position."""
    if header_name in row:
        return row[header_name]
    idx = POS.get(header_name)
    if idx is not None and idx < len(headers):
        return row.get(headers[idx])
    return None


def read_rows(path):
    """Read the CSV, trying the encodings Russian bank exports commonly use."""
    last_err = None
    for encoding in ("utf-8-sig", "cp1251", "utf-8", "latin-1"):
        try:
            with open(path, newline="", encoding=encoding) as fh:
                reader = csv.DictReader(fh, delimiter=";")
                headers = reader.fieldnames or []
                rows = list(reader)
            return rows, headers, encoding
        except UnicodeDecodeError as err:
            last_err = err
    raise SystemExit(f"Could not decode {path!r}: {last_err}")


def group_key(details, normalize, substrings):
    """Decide which bucket a row belongs to.

    If any of ``substrings`` occurs in the details, the row is grouped under
    that substring (first match wins). Otherwise the row keeps its normal
    per-details grouping (optionally normalized).
    """
    low = details.lower()
    for sub in substrings:
        if sub.lower() in low:
            return sub
    if normalize:
        return normalize_key(details)
    return details or "(no details)"


def analyze(path, normalize, substrings=()):
    rows, headers, encoding = read_rows(path)
    if not headers:
        raise SystemExit(f"{path!r} looks empty or is not a valid CSV.")

    income = defaultdict(lambda: [0.0, 0])   # key -> [total, count]
    outcome = defaultdict(lambda: [0.0, 0])
    skipped = 0

    for row in rows:
        details = (pick(row, COL_DETAILS, headers) or "").strip()
        key = group_key(details, normalize, substrings)

        credit = parse_amount(pick(row, COL_CREDIT_ACC, headers)) \
            or parse_amount(pick(row, COL_CREDIT_OP, headers))
        debit = parse_amount(pick(row, COL_DEBIT_ACC, headers)) \
            or parse_amount(pick(row, COL_DEBIT_OP, headers))

        if credit > 0:
            income[key][0] += credit
            income[key][1] += 1
        if debit > 0:
            outcome[key][0] += debit
            outcome[key][1] += 1
        if credit <= 0 and debit <= 0:
            skipped += 1

    return {
        "income": income,
        "outcome": outcome,
        "skipped": skipped,
        "encoding": encoding,
        "total_rows": len(rows),
    }


def print_section(title, groups):
    print(f"\n{title}")
    print("=" * len(title))
    if not groups:
        print("  (none)")
        return 0.0
    ordered = sorted(groups.items(), key=lambda kv: kv[1][0], reverse=True)
    total = sum(total for total, _ in groups.values())
    label_width = min(max(len(k) for k in groups), 70)
    for key, (amount, count) in ordered:
        label = key if len(key) <= label_width else key[: label_width - 1] + "…"
        share = (amount / total * 100) if total else 0.0
        print(f"  {label:<{label_width}}  {fmt_amount(amount):>20}  "
              f"x{count:<4} {share:5.1f}%")
    print("-" * (label_width + 36))
    print(f"  {'TOTAL':<{label_width}}  {fmt_amount(total):>20}")
    return total


def dump_csv(out_path, result):
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter=";")
        writer.writerow(["Direction", "Group", "Total RUB", "Count"])
        for direction, groups in (("income", result["income"]),
                                   ("outcome", result["outcome"])):
            ordered = sorted(groups.items(), key=lambda kv: kv[1][0], reverse=True)
            for key, (amount, count) in ordered:
                writer.writerow([direction, key, f"{amount:.2f}", count])
    print(f"\nWrote grouped results to {out_path!r}")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Analyze a bank-statement CSV.")
    parser.add_argument("csv_path", help="path to the semicolon-separated CSV")
    parser.add_argument("--normalize", action="store_true",
                        help="merge descriptions that differ only by numbers/dates")
    parser.add_argument("--group", action="append", default=[], metavar="SUBSTR",
                        help="bucket operations whose details contain SUBSTR "
                             "(repeatable); non-matching rows keep normal grouping")
    parser.add_argument("--csv", dest="out_csv", metavar="FILE",
                        help="also write the grouped totals to this CSV file")
    args = parser.parse_args(argv)

    result = analyze(args.csv_path, args.normalize, args.group)

    print(f"Read {result['total_rows']} rows from {args.csv_path!r} "
          f"(encoding: {result['encoding']}).")
    if result["skipped"]:
        print(f"Note: {result['skipped']} row(s) had no credit/debit amount and "
              f"were ignored.")

    income_total = print_section("INCOME by origin", result["income"])
    outcome_total = print_section("OUTCOME by reason", result["outcome"])

    print("\nSUMMARY")
    print("=======")
    print(f"  Total income : {fmt_amount(income_total)}")
    print(f"  Total outcome: {fmt_amount(outcome_total)}")
    print(f"  Net          : {fmt_amount(income_total - outcome_total)}")

    if args.out_csv:
        dump_csv(args.out_csv, result)


if __name__ == "__main__":
    sys.exit(main())
