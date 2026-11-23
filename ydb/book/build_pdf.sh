#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Ensure TeX binaries are in PATH (basictex / mactex standard location)
if [[ -d /Library/TeX/texbin ]]; then
    export PATH="/Library/TeX/texbin:$PATH"
fi

OUTPUT="ydb_book.pdf"
METADATA="metadata.yaml"
MERGED="_merged_book.md"
LATEX_PACKAGES=(unicode-math framed fvextra upquote lineno adjustbox collectbox booktabs fancyhdr float placeins)

# --- dependency checks ---

missing=()
if ! command -v pandoc &>/dev/null; then
    missing+=("pandoc: brew install pandoc")
fi
PDF_ENGINE=""
if command -v xelatex &>/dev/null; then
    PDF_ENGINE="xelatex"
elif command -v tectonic &>/dev/null; then
    PDF_ENGINE="tectonic"
else
    missing+=("xelatex or tectonic: brew install tectonic, or brew install --cask basictex && eval \"\$(/usr/libexec/path_helper)\"")
fi
if [[ ${#missing[@]} -gt 0 ]]; then
    echo "ERROR: missing dependencies:"
    for dep in "${missing[@]}"; do
        echo "  - $dep"
    done
    echo ""
    echo "After installing basictex, install required LaTeX packages:"
    echo "  sudo tlmgr update --self"
    echo "  sudo tlmgr install ${LATEX_PACKAGES[*]}"
    exit 1
fi

if [[ ! -f "$METADATA" ]]; then
    echo "ERROR: $METADATA not found in $SCRIPT_DIR"
    exit 1
fi

# Verify critical LaTeX packages are available (tectonic downloads them on demand)
if [[ "$PDF_ENGINE" == "xelatex" ]]; then
    missing_tex=()
    for package in "${LATEX_PACKAGES[@]}"; do
        if ! kpsewhich "$package.sty" &>/dev/null; then
            missing_tex+=("$package")
        fi
    done
    if [[ ${#missing_tex[@]} -gt 0 ]]; then
        echo "ERROR: missing LaTeX packages: ${missing_tex[*]}"
        echo "Install them with:"
        echo "  sudo tlmgr update --self"
        echo "  sudo tlmgr install ${missing_tex[*]}"
        exit 1
    fi
fi

# --- build file list automatically ---
# Collect unique two-digit chapter prefixes from top-level md files and
# directories, then for each prefix emit the chapter intro file (if it
# exists) followed by sorted section files from the matching directory
# (if it exists).  00-* is skipped (table of contents / meta).

chapter_nums=()
for entry in [0-9][0-9]-*.md; do
    if [[ -f "$entry" ]]; then
        num="${entry:0:2}"
        [[ "$num" == "00" ]] && continue
        chapter_nums+=("$num")
    fi
done
for entry in [0-9][0-9]-*/; do
    if [[ -d "$entry" ]]; then
        num="${entry:0:2}"
        [[ "$num" == "00" ]] && continue
        chapter_nums+=("$num")
    fi
done
chapter_nums=($(printf '%s\n' "${chapter_nums[@]}" | sort -u))

FILES=()
for num in "${chapter_nums[@]}"; do
    for top in "${num}"-*.md; do
        if [[ -f "$top" ]]; then
            FILES+=("$top")
        fi
    done
    for dir in "${num}"-*/; do
        if [[ -d "$dir" ]]; then
            for sub in "${dir}"*.md; do
                if [[ -f "$sub" ]]; then
                    FILES+=("$sub")
                fi
            done
        fi
    done
done

if [[ ${#FILES[@]} -eq 0 ]]; then
    echo "ERROR: no chapter .md files found"
    exit 1
fi

# --- preprocess: merge all files with transformations ---

preprocess() {
    local file="$1"
    perl -pe '
        s{\.\./assets/}{assets/}g;
        s{<img\s+src="([^"]+)"\s+width="([^"]+)"\s*/?>}{![]($1){width=$2}}gi;
        s{\[([^\]]+)\]\(<?\./[^)]+\.md>?\)}{$1}g;
    ' "$file" | perl -CSD -pe 's/\$\\color\{#([0-9A-Fa-f]{6})\}\{\\textsf\{[^}]+\}\}\$/`\\textcolor[HTML]{$1}{\\rule{0.7em}{0.7em}}`{=latex}/g'
}

echo "Merging ${#FILES[@]} files..."

rm -f "$MERGED"

for f in "${FILES[@]}"; do
    if [[ "$f" != */* ]]; then
        printf '\n\\newpage\n\n' >> "$MERGED"
    fi
    preprocess "$f" >> "$MERGED"
    printf '\n\n' >> "$MERGED"
done

echo "Running pandoc..."

pandoc "$MERGED" \
    --metadata-file="$METADATA" \
    --pdf-engine="$PDF_ENGINE" \
    --syntax-highlighting=tango \
    --top-level-division=chapter \
    -V linkcolor=blue \
    -V urlcolor=blue \
    -o "$OUTPUT"

rm -f "$MERGED"

echo "Done: $OUTPUT"
