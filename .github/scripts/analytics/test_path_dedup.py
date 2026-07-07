"""Collapse rows that share full_name but differ in (suite_folder, test_name)."""


def _suite_folder_len(row, suite_folder_key):
    return len(str(row.get(suite_folder_key) or ''))


def dedupe_rows_by_full_name(rows, full_name_key='full_name', suite_folder_key='suite_folder'):
    """Keep one dict row per full_name (deepest suite_folder wins)."""
    if not rows:
        return rows
    best = {}
    for row in rows:
        full_name = row[full_name_key]
        if (
            full_name not in best
            or _suite_folder_len(row, suite_folder_key)
            > _suite_folder_len(best[full_name], suite_folder_key)
        ):
            best[full_name] = row
    return list(best.values())


def dedupe_dataframe_by_full_name(df, full_name_col='full_name', suite_folder_col='suite_folder'):
    """Keep one DataFrame row per full_name (deepest suite_folder wins)."""
    if df is None or df.empty or full_name_col not in df.columns:
        return df
    suite_len = df[suite_folder_col].astype(str).str.len()
    return (
        df.assign(_suite_len=suite_len)
        .sort_values([full_name_col, '_suite_len'])
        .drop_duplicates(full_name_col, keep='last')
        .drop(columns='_suite_len')
    )
