"""User-visible GitHub comments for the manual fast-unmute flow."""


def format_bullet_list(tests):
    return '\n'.join(f"- `{name}`" for name in sorted(set(tests)))


COMMENT_ENTER = """🚀 **Fast-unmute started**

{closer_mention_line}Issue stays **closed**. Label `manual-fast-unmute` added.

These tests are still listed in `muted_ya`, but CI will try to unmute them sooner ({window_days} days window, at least {min_runs} clean runs):

{tests_bullet_list}

**What to expect**

- If tests go green in CI before the limit → board **Unmuted**, label removed.
- If the limit passes and something is still red → this issue **reopens**, board **Muted**, label removed.

You do not need to do anything. Please do not edit `muted_ya.txt` by hand.

🔗 Workflow run: {workflow_run_url}
"""


COMMENT_SUCCESS = """✅ **Fast-unmute completed**

Every test on this issue is green in CI before the time limit. The mute list in the repo will catch up on the next routine update.

**Status** → **Unmuted**. Label `manual-fast-unmute` removed.

🔗 Workflow run: {workflow_run_url}
"""


COMMENT_PROGRESS = """⏱️ **Fast-unmute: progress**

These already show as unmuted in CI. Other tests on this issue are still in the shorter window:

{unmuted_bullets}

🔗 Workflow run: {workflow_run_url}
"""


COMMENT_TTL_INCOMPLETE = """❌ **Fast-unmute: deadline passed**

At least one **(test, branch, build)** on this issue stayed muted past **its** **{ttl_days}**-day window. This issue is **reopened**, board **Muted**, label `manual-fast-unmute` removed.

**Green in CI** (already unmuted there, or will be shortly once the mute list in the repo updates):
{graduated_bullets}

**Still on the CI mute list** when the window closed — at least one of these stayed muted and led to the reopen:
{stuck_bullets}


🔗 Workflow run: {workflow_run_url}
"""
