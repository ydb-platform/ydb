"""User-visible GitHub comments for the manual fast-unmute flow."""


def format_bullet_list(tests):
    return '\n'.join(f"- `{name}`" for name in sorted(set(tests)))


COMMENT_ENTER = """🚀 **Fast-unmute started**

{closer_mention_line}
**Status** → **Observation**

These tests stay in `muted_ya` until the mute automation removes the line:

- **Fast-unmute:** in the last **{window_days}** days, at least **{min_runs}** runs with no failures or mutes
- **Fast-delete:** in the last **{fast_delete_window_days}** days, zero pass/fail/mute outcomes

{tests_bullet_list}

**What to expect**

- Wait — nothing for you to do; do not edit `muted_ya.txt`.
- Unmute checks use **{window_days}** days of history here instead of the usual **{unmute_window_days}** for the normal mute job (faster path). Still need **{min_runs}** clean runs in that window for fast-unmute.
- If the test had no CI activity, fast-delete can remove the line after **{fast_delete_window_days}** days with zero outcomes.
- Miss the deadline with failures → this issue may reopen.

🔗 Workflow run: {workflow_run_url}
"""


COMMENT_SUCCESS = """✅ **Fast-unmute completed**

All tests finished tracking (runs went green and/or the mute job dropped idle lines).

**Status** → **Unmuted**. Label `manual-fast-unmute` removed.

🔗 Workflow run: {workflow_run_url}
"""


COMMENT_PROGRESS = """⏱️ **Fast-unmute: progress**

Done for:

{unmuted_bullets}

Other tests on this issue are still in the window.

🔗 Workflow run: {workflow_run_url}
"""


COMMENT_ABANDON_NOT_COMPLETED = """🛑 **Fast-unmute stopped**

Tracking for fast-unmute on this issue was stopped: **Status** → **Muted**.

🔗 Workflow run: {workflow_run_url}
"""


COMMENT_TTL_INCOMPLETE = """❌ **Fast-unmute: deadline passed**

At least one **(test, branch, build)** on this issue stayed muted past **its** **{ttl_days}**-day window. This issue is **reopened**, **Status** → **Muted**

**Green in CI** (already unmuted there, or will be shortly once the mute list in the repo updates):
{graduated_bullets}

**Still on the CI mute list** when the window closed — at least one of these stayed muted and led to the reopen:
{stuck_bullets}

**Other rows cleared in the same shutdown:**
{cleared_other_bullets}

🔗 Workflow run: {workflow_run_url}
"""
