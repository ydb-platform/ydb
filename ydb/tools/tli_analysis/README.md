# TLI Chain Finder

A tool for analyzing TLI (Transaction Lock Invalidation) logs to find the chain of events when a transaction's locks are broken by another transaction.

## Overview

When a transaction (victim) has its locks broken by another transaction (breaker), YDB logs TLI records that contain:
- **VictimQuerySpanId**: Identifier of the victim query that had its locks broken
- **BreakerQuerySpanId**: Identifier of the breaker query that broke the locks
- **Query texts**: The actual SQL queries involved in both transactions

This tool extracts and displays this information in a human-readable format.

## Usage

```bash
python3 find_tli_chain.py <VictimQuerySpanId> <logfile> [options]
```

### Arguments

- `VictimQuerySpanId` (required): The numeric ID of the victim query span (found in error messages or logs)
- `logfile` (required): Path to the log file
- `--window-sec`: Time window in seconds around the anchor event (default: 10)
- `--no-color`: Disable ANSI color output

### Example

```bash
python3 find_tli_chain.py 39941355799977664 tli.log
```

## Output

The tool displays:

1. **TLI Chain Summary**
   - VictimQuerySpanId
   - VictimQueryText (the query that was victimized)
   - BreakerQuerySpanId
   - BreakerQueryText (the query that broke the locks)

2. **VictimTx** - All queries in the victim transaction

3. **BreakerTx** - All queries in the breaker transaction

## How It Works

1. Scans the log file for lines containing TLI-related patterns
2. Finds the anchor timestamp (first occurrence of the VictimQuerySpanId)
3. Collects all relevant log entries within the time window
4. Extracts and correlates:
   - Victim SessionActor logs ("was a victim of broken locks")
   - Breaker DataShard logs ("broke other locks")
   - Breaker SessionActor logs ("had broken other locks")

## Enabling TLI Logs

TLI logging must be enabled on the YDB server to generate the log entries this tool analyzes.

### Configuration

Set the log priority for the `TLI` service to `INFO` level:

```yaml
log_config:
  entry:
    - component: TLI
      level: INFO
```

### Log Format

TLI log entries contain structured information about lock conflicts:
- **Component**: `SessionActor` (KQP layer) or `DataShard` (storage layer)
- **Message types**:
  - `"(Query|Commit) had broken other locks"` - breaker SessionActor
  - `"(Query|Commit) was a victim of broken locks"` - victim SessionActor
  - `"Write transaction broke other locks"` - breaker DataShard
  - `"(Write|Read) transaction was a victim of broken locks"` - victim DataShard

## Notes

- The tool handles unsorted/merged log files correctly by filtering based on timestamp values, not line positions
- The `--window-sec` parameter defines how far before and after the anchor event to search for related entries
- Query texts are automatically unescaped and formatted for readability