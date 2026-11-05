//! Print SDK Core changelog and commit notes for a Git revision range.

use std::{collections::BTreeMap, env, process::Command};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
    lines: Vec<String>,
    introduced_header: Option<String>,
}

type Entries = BTreeMap<String, Vec<Entry>>;

fn git(args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .output()
        .map_err(|err| format!("failed to run git: {err}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn changelog_entries(text: &str) -> BTreeMap<String, Vec<Vec<String>>> {
    let mut entries = BTreeMap::new();
    let mut header: Option<String> = None;
    let mut entry: Option<Vec<String>> = None;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("### ") {
            if let Some(entry) = entry.take() {
                entries
                    .entry(header.clone().unwrap_or_else(|| "Other".into()))
                    .or_insert_with(Vec::new)
                    .push(entry);
            }
            header = Some(value.trim().to_owned());
        } else if line.starts_with("* ") || line.starts_with("- ") {
            if let Some(entry) = entry.take() {
                entries
                    .entry(header.clone().unwrap_or_else(|| "Other".into()))
                    .or_insert_with(Vec::new)
                    .push(entry);
            }
            entry = Some(vec![line.to_owned()]);
        } else if line.trim().is_empty() {
            if let Some(entry) = entry.take() {
                entries
                    .entry(header.clone().unwrap_or_else(|| "Other".into()))
                    .or_insert_with(Vec::new)
                    .push(entry);
            }
        } else if let Some(entry) = entry.as_mut() {
            entry.push(line.to_owned());
        }
    }
    if let Some(entry) = entry.filter(|entry| !entry.is_empty()) {
        entries
            .entry(header.unwrap_or_else(|| "Other".into()))
            .or_insert_with(Vec::new)
            .push(entry);
    }
    entries
}

fn similarity(left: &[String], right: &[String]) -> f64 {
    let left = left.join("\n");
    let right = right.join("\n");
    let mut row = vec![0; right.len() + 1];
    for a in left.bytes() {
        let mut next = vec![0];
        for (index, b) in right.bytes().enumerate() {
            next.push(if a == b {
                row[index] + 1
            } else {
                row[index + 1].max(next[index])
            });
        }
        row = next;
    }
    let length = left.len() + right.len();
    if length == 0 {
        1.0
    } else {
        2.0 * row[right.len()] as f64 / length as f64
    }
}

fn update_entries(previous: &Entries, current: BTreeMap<String, Vec<Vec<String>>>) -> Entries {
    let previous_flat: Vec<Entry> = previous.values().flatten().cloned().collect();
    let current_flat: Vec<(String, Vec<String>)> = current
        .into_iter()
        .flat_map(|(header, entries)| {
            entries
                .into_iter()
                .map(move |entry| (header.clone(), entry))
        })
        .collect();
    let mut matches: Vec<Option<usize>> = vec![None; current_flat.len()];
    let mut used = vec![false; previous_flat.len()];
    for (index, (_, lines)) in current_flat.iter().enumerate() {
        if let Some(previous_index) = previous_flat
            .iter()
            .enumerate()
            .find_map(|(i, entry)| (!used[i] && entry.lines == *lines).then_some(i))
        {
            matches[index] = Some(previous_index);
            used[previous_index] = true;
        }
    }
    let mut candidates = Vec::new();
    for (current_index, (_, lines)) in current_flat
        .iter()
        .enumerate()
        .filter(|(i, _)| matches[*i].is_none())
    {
        for (previous_index, entry) in previous_flat.iter().enumerate().filter(|(i, _)| !used[*i]) {
            let score = similarity(&entry.lines, lines);
            if score >= 0.6 {
                candidates.push((score, current_index, previous_index));
            }
        }
    }
    candidates.sort_by(|a, b| b.0.total_cmp(&a.0));
    for (_, current_index, previous_index) in candidates {
        if matches[current_index].is_none() && !used[previous_index] {
            matches[current_index] = Some(previous_index);
            used[previous_index] = true;
        }
    }
    let mut updated = Entries::new();
    for ((header, lines), previous_index) in current_flat.into_iter().zip(matches) {
        let introduced_header = match previous_index {
            Some(index) => previous_flat[index].introduced_header.clone(),
            None => Some(header.clone()),
        };
        updated.entry(header).or_default().push(Entry {
            lines,
            introduced_header,
        });
    }
    updated
}

fn changelog_notes(from: &str, to: &str) -> Result<Vec<String>, String> {
    let mut entries: Entries = changelog_entries(&git(&["show", &format!("{from}:CHANGELOG.md")])?)
        .into_iter()
        .map(|(header, entries)| {
            (
                header,
                entries
                    .into_iter()
                    .map(|lines| Entry {
                        lines,
                        introduced_header: None,
                    })
                    .collect(),
            )
        })
        .collect();
    let commits = git(&[
        "log",
        "--format=%H",
        "--reverse",
        &format!("{from}..{to}"),
        "--",
        "CHANGELOG.md",
    ])?;
    for commit in commits.lines().filter(|commit| !commit.is_empty()) {
        entries = update_entries(
            &entries,
            changelog_entries(&git(&["show", &format!("{commit}:CHANGELOG.md")])?),
        );
    }
    let mut categorized: Entries = Entries::new();
    for entries in entries.into_values() {
        for entry in entries {
            if let Some(header) = entry.introduced_header.clone() {
                categorized.entry(header).or_default().push(entry);
            }
        }
    }
    let mut output = Vec::new();
    for (header, entries) in categorized {
        if !entries.is_empty() {
            output.extend([format!("#### {header}"), String::new()]);
            for entry in entries {
                output.extend(entry.lines);
            }
            output.push(String::new());
        }
    }
    Ok(output)
}

fn clean_subject(subject: &str) -> String {
    let subject = subject.chars().filter(char::is_ascii).collect::<String>();
    let subject = subject.split_whitespace().collect::<Vec<_>>().join(" ");
    subject
        .strip_prefix(':')
        .and_then(|value| value.split_once(": "))
        .map_or(subject.clone(), |(_, value)| value.to_owned())
        .replace(" : ", ": ")
}

fn link_prs(subject: &str) -> String {
    let mut output = String::new();
    let mut remaining = subject;
    while let Some(start) = remaining.find("(#") {
        output.push_str(&remaining[..start]);
        let suffix = &remaining[start + 2..];
        if let Some(end) = suffix.find(')') {
            let number = &suffix[..end];
            if number.chars().all(|c| c.is_ascii_digit()) {
                output.push_str(&format!(
                    "([#{number}](https://github.com/temporalio/sdk-rust/pull/{number}))"
                ));
                remaining = &suffix[end + 1..];
                continue;
            }
        }
        output.push_str("(#");
        remaining = suffix;
    }
    output.push_str(remaining);
    output
}

fn release_notes(from: &str, to: &str) -> Result<Vec<String>, String> {
    let log = git(&[
        "log",
        "--format=%H%x00%h%x00%s",
        "--reverse",
        &format!("{from}..{to}"),
    ])?;
    if log.is_empty() {
        return Ok(Vec::new());
    }
    let mut output = changelog_notes(from, to)?;
    if !output.is_empty() {
        output.insert(0, String::new());
        output.insert(0, "#### Changelog".into());
    }
    output.extend(["#### Commits".into(), String::new()]);
    for line in log.lines() {
        let parts: Vec<_> = line.split('\0').collect();
        if let [full, short, subject] = parts.as_slice() {
            output.push(format!(
                "- [`{short}`](https://github.com/temporalio/sdk-rust/commit/{full}) {}",
                link_prs(&clean_subject(subject))
            ));
        }
    }
    Ok(output)
}

fn main() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let from = args
        .next()
        .filter(|arg| arg == "--from")
        .and_then(|_| args.next())
        .ok_or("expected --from <sha>")?;
    let to = args
        .next()
        .filter(|arg| arg == "--to")
        .and_then(|_| args.next())
        .ok_or("expected --to <sha>")?;
    println!("{}", release_notes(&from, &to)?.join("\n"));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(lines: &[&str], header: Option<&str>) -> Entry {
        Entry {
            lines: lines.iter().map(|line| (*line).into()).collect(),
            introduced_header: header.map(Into::into),
        }
    }

    #[test]
    fn preserves_original_heading_after_move_and_edit() {
        let previous = Entries::from([(
            "Added".into(),
            vec![entry(&["* Initial feature."], Some("Added"))],
        )]);
        let updated = update_entries(
            &previous,
            BTreeMap::from([("Changed".into(), vec![vec!["* Updated feature.".into()]])]),
        );
        assert_eq!(
            updated["Changed"][0].introduced_header.as_deref(),
            Some("Added")
        );
    }

    #[test]
    fn formats_commit_links() {
        assert_eq!(
            link_prs("Change (#12)"),
            "Change ([#12](https://github.com/temporalio/sdk-rust/pull/12))"
        );
        assert_eq!(clean_subject(":boom: Change"), "Change");
    }

    #[test]
    fn keeps_final_wording_for_introduced_entry() {
        let previous = Entries::from([(
            "Added".into(),
            vec![entry(&["* Initial wording."], Some("Added"))],
        )]);
        let updated = update_entries(
            &previous,
            BTreeMap::from([("Added".into(), vec![vec!["* Final wording.".into()]])]),
        );
        assert_eq!(
            updated["Added"][0],
            entry(&["* Final wording."], Some("Added"))
        );
    }

    #[test]
    fn excludes_modified_old_entry_and_keeps_new_entry() {
        let previous =
            Entries::from([("Added".into(), vec![entry(&["* Existing feature."], None)])]);
        let updated = update_entries(
            &previous,
            BTreeMap::from([(
                "Added".into(),
                vec![
                    vec!["* Corrected existing feature.".into()],
                    vec!["* New feature.".into()],
                ],
            )]),
        );
        assert_eq!(
            updated["Added"]
                .iter()
                .map(|entry| entry.introduced_header.as_deref())
                .collect::<Vec<_>>(),
            [None, Some("Added")]
        );
    }

    #[test]
    fn includes_unrelated_replacement() {
        let previous = Entries::from([("Added".into(), vec![entry(&["* Old feature."], None)])]);
        let updated = update_entries(
            &previous,
            BTreeMap::from([(
                "Added".into(),
                vec![vec!["* New capability for another API.".into()]],
            )]),
        );
        assert_eq!(
            updated["Added"][0].introduced_header.as_deref(),
            Some("Added")
        );
    }

    #[test]
    fn excludes_multiline_old_entry_modification() {
        let previous = Entries::from([(
            "Fixed".into(),
            vec![entry(&["* Existing fix.", "  Old detail."], None)],
        )]);
        let updated = update_entries(
            &previous,
            BTreeMap::from([(
                "Fixed".into(),
                vec![vec!["* Existing fix.".into(), "  New detail.".into()]],
            )]),
        );
        assert_eq!(updated["Fixed"][0].introduced_header, None);
    }

    #[test]
    fn keeps_introduced_entry_when_heading_changes() {
        let previous = Entries::from([(
            "Added".into(),
            vec![entry(&["* New feature."], Some("Added"))],
        )]);
        let updated = update_entries(
            &previous,
            BTreeMap::from([(
                "Released additions".into(),
                vec![vec!["* New feature.".into()]],
            )]),
        );
        assert_eq!(
            updated["Released additions"][0]
                .introduced_header
                .as_deref(),
            Some("Added")
        );
    }
}
