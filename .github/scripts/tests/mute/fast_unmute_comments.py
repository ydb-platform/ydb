"""User-visible GitHub comments for the manual fast-unmute flow."""


def format_bullet_list(tests):
    return '\n'.join(f"- `{name}`" for name in sorted(set(tests)))


COMMENT_ENTER = """🚀 **Fast-unmute started**

{closer_mention_line}You closed this issue as completed. The issue stays **closed**; automation registered fast-unmute in YDB and added the `manual-fast-unmute` label.

The listed tests are still muted in the repo, but CI now evaluates them on a **shorter unmute window** ({window_days} calendar days, min {min_runs} clean runs) until they qualify for automatic unmute:

{tests_bullet_list}

**What happens next**

- **While time runs** (``manual_unmute_ttl_calendar_days`` calendar days per row from registration): if a test is already **unmuted in CI data**, only its fast-unmute row is removed; you may see a short **progress** comment if other tests on this issue are still tracked.
- **All tests unmuted in CI before anyone hits the deadline** → success comment, project **Status → Unmuted**, fast-unmute label removed (this issue stays **closed**).
- **After the deadline**, if any test is **still muted** in CI → this issue is **reopened**, **Status → Muted**, all fast-unmute rows for this issue are cleared, one summary comment, label removed.
- No action needed from you. Please do not edit `muted_ya.txt` manually.

🔗 Workflow run: {workflow_run_url}
"""


COMMENT_SUCCESS = """✅ **Fast-unmute completed**

All tests from this issue are **no longer muted** in CI data before the fast-unmute deadline. They will be removed from `muted_ya` on the next mute automation run that updates the repo — no action needed from you.

Project **Status** → **Unmuted**. The `manual-fast-unmute` label is removed.

🔗 Workflow run: {workflow_run_url}
"""


COMMENT_PROGRESS = """📌 **Fast-unmute: progress**

While the deadline is still running for other tests on this issue, these are **already unmuted** in CI data — the fast-unmute row was removed only for them:

{unmuted_bullets}

🔗 Workflow run: {workflow_run_url}
"""


COMMENT_TTL_INCOMPLETE = """⏱️ **Fast-unmute: deadline passed**

The fast-unmute calendar limit (**{ttl_days}** days from row registration) has passed, but at least one test is **still muted** in CI. Fast-unmute tracking for **this whole issue** is cleared, the issue is **reopened**, project **Status** → **Muted**, and the `manual-fast-unmute` label is removed.

**Already unmuted in CI (pending `muted_ya` update):**
{graduated_bullets}

**Still muted after the deadline:**
{stuck_bullets}

**Tracking cleared early (same issue, deadline not reached for that row):**
{cleared_other_bullets}

🔗 Workflow run: {workflow_run_url}
"""
