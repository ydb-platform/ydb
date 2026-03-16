Perform comprehensive analysis of a single Windows crash dump with detailed metadata extraction, call stack analysis, and structured markdown reporting.

## WORKFLOW - Execute in this exact sequence:

### Step 1: Dump File Identification
**If no dump file path provided:**
- Ask user to provide the specific crash dump file path, use `list_windbg_dumps` to help them find available dumps.

### Step 2: Comprehensive Dump Analysis
**Analyze the specified dump file:**

**Tool:** `open_windbg_dump`
- **Parameters:**
  - `dump_path`: Provided dump file path
  - `include_stack_trace`: true
  - `include_modules`: true
  - `include_threads`: true

The `open_windbg_dump` tool automatically runs `!analyze -v` and provides initial analysis output.

If the dump is very large (>5GB) or analysis takes too long we eventually timeout. In that case, inform the user about the timeout
and tell him to wait since the analysis keeps running in the background and will complete. The most obvious reason is downloading symbols for running into a timeout here.

**Then extract additional metadata with:** `run_windbg_cmd`
- **Command 1:** `vertarget` (OS version and platform details)
- **Command 2:** `lm` (loaded modules list)
- **Command 3:** `k` (call stack)
- **Command 4:** `.time` (dump creation time)
- **Command 5:** `!peb` (process environment details)
- **Command 6:** `r` (registers)

**Then cleanup:** `close_windbg_dump`

### Step 3: Generate Structured Analysis Report

## REQUIRED OUTPUT FORMAT:

```markdown
# Crash Dump Analysis Report
**Analysis Date:** [Current Date]
**Dump File:** [filename.dmp]
**File Path:** [Full path to dump file]

## Executive Summary
- **Crash Type:** [Exception type - Access Violation, Heap Corruption, etc.]
- **Severity:** [Critical/High/Medium/Low]
- **Root Cause:** [Brief description of the identified issue]
- **Recommended Action:** [Immediate next steps]

## Dump Metadata
- **File Size:** [MB]
- **Creation Time:** [Date/Time from .time command]
- **OS Build:** [Windows version and build from vertarget]
- **Platform:** [x86/x64/ARM64]
- **Process Name:** [Process name and PID]
- **Process Path:** [Full executable path]
- **Command Line:** [Process command line arguments]
- **Working Directory:** [Process working directory]

## Crash Analysis
**Exception Details:**
- **Exception Code:** [0xC0000005, etc.]
- **Exception Address:** [0x12345678]
- **Faulting Module:** [module.dll or module.exe]
- **Module Version:** [File version]
- **Module Timestamp:** [Build timestamp]
- **Module Base Address:** [0x12345678]

**Call Stack Analysis:**

Frame Module!Function+Offset                               Parameters
===== ================================================== ==================
[0]   module!function+0x123                              param1, param2, param3
[1]   module!function+0x456                              param1, param2
[2]   module!function+0x789                              param1
[3]   module!function+0xabc
[4]   module!function+0xdef
[5]   [Continue with full stack...]

**Thread Information:**
- **Crashing Thread ID:** [Thread ID]
- **Thread Count:** [Total threads]
- **Other Notable Threads:** [List any threads of interest]

## Technical Details

**Memory Information:**
- **Virtual Size:** [Size from !peb]
- **Working Set:** [Size]
- **Heap Information:** [Heap details if relevant]

**Loaded Modules Summary:**
| Module | Base Address | Size | Path |
|--------|--------------|------|------|
| [Primary modules of interest] | | | |

**Environment Details:**
- **Current Directory:** [From !peb]
- **Environment Variables:** [Key variables if relevant]
- **Command Line:** [Full command line from !peb]

## Root Cause Analysis
[Detailed explanation of what caused the crash, including:]
- **What happened:** [Technical description of the failure]
- **Why it happened:** [Analysis of contributing factors]
- **Code location:** [Specific function/line if identifiable]
- **Memory state:** [Description of memory corruption, null pointers, etc.]

## Recommendations

### Immediate Actions
1. [Specific action item 1]
2. [Specific action item 2]
3. [Specific action item 3]

### Investigation Steps
1. [Follow-up analysis steps]
2. [Code review recommendations]
3. [Testing scenarios to reproduce]

### Prevention Measures
1. [Code changes to prevent recurrence]
2. [Additional validation/checks needed]
3. [Process improvements]

## Priority Assessment
**Severity:** [Critical/High/Medium/Low]

**Justification:** [Explanation of why this severity was assigned based on:]
- Impact on users/system stability
- Frequency of occurrence (if known)
- Ease of reproduction
- Potential for data loss or security issues

## Additional Notes
[Any other relevant observations, similar crashes seen, or context that might be helpful for developers]
```

## METADATA EXTRACTION REQUIREMENTS:
Extract and include ALL of the following for the dump file:
- **OS Information:** Build number, version, platform (x86/x64/ARM64) from `vertarget`
- **Process Details:** Name, PID, command line, working directory from `!peb`
- **Exception Information:** Code, type, address, parameters from initial analysis
- **Module Information:** Name, base address, size from `lm`
- **Call Stack:** Stack with frame numbers, modules, functions, offsets from `k`
- **Timing:** Dump creation time from `.time`
- **Memory Information:** Virtual size, working set, commit charge from `!peb`
- **Thread Information:** Thread count and details from initial analysis

## ANALYSIS DEPTH:
Provide comprehensive technical details suitable for developers to:
- Understand the exact failure mechanism
- Identify the root cause in source code
- Implement appropriate fixes
- Prevent similar issues in the future

**Always ask follow-up questions if:**
- Dump file path is not provided or unclear
- User wants specific focus areas for analysis
- Additional investigation commands are needed
- Source code context would be helpful
