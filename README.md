# Exchange Migrations Companion

Exchange Migrations Companion is a PowerShell-first toolkit for Exchange migration operations.

The project centers on one script:

- `MigrationAnalysisV3.ps1`

It collects move statistics, builds an interactive report, and gives operational guidance for triage and remediation planning.

## Project Analysis (What This Project Contains)

- A single main automation script (`MigrationAnalysisV3.ps1`) with reporting, monitoring, and troubleshooting workflows.
- A generated HTML dashboard for interactive analysis.
- CSV and XML outputs for sharing results and replaying past incidents offline.
- An embedded Solution Center with issue signatures, runbook steps, and command plans.

## Main Features

### 1) Snapshot Reporting

Create a one-time migration report for the current state of your environment.

- Overall health score and grade
- Throughput, latency, and stall indicators
- Mailbox-level visibility for slow/failed moves

### 2) Watch Mode (Live Monitoring)

Run continuous refresh during migration windows.

- Auto-refresh dashboard
- Scope changes from the UI without restarting
- Trend tracking over multiple refresh cycles

### 3) Offline Replay

Rebuild reports from previously exported XML without reconnecting to Exchange.

- Useful for incident retrospectives
- Useful for sharing evidence with support teams

### 4) Solution Center

Built-in troubleshooting library inside the report.

- Signature-based issue catalog
- Action-focused runbook flow
- Search across issue and content sections

### 5) MRS Explorer

Deep-dive panel for move request diagnostics.

- Mailbox-level statistics exploration
- Diagnostic context for troubleshooting escalations

### 6) Operational Automation

- Alerts for failure, completion, and stall conditions
- Optional auto-retry for retryable failures
- Scheduled reporting support (hourly, daily, weekly)
- Email and Teams webhook notification options

## Report Experience (UI Tabs)

The HTML report includes these major work areas:

- Performance Analysis
- Mailbox Detail
- Cohort Analysis
- MRS Explorer
- Solution Center
- Migration Trends (watch mode context)
- Batch Analysis (watch mode context)
- Auto-Retry status (when enabled)

## Prerequisites

- Windows PowerShell 5.1 or PowerShell 7+
- Exchange Online PowerShell connectivity
- `ExchangeOnlineManagement` module

Typical setup:

```powershell
Install-Module ExchangeOnlineManagement -Scope CurrentUser
Import-Module ExchangeOnlineManagement
Connect-ExchangeOnline
```

## Quick Start

### 1) Standard snapshot report

```powershell
.\MigrationAnalysisV3.ps1 -ReportPath "C:\Reports"
```

### 2) Snapshot with deeper troubleshooting detail

```powershell
.\MigrationAnalysisV3.ps1 -ReportPath "C:\Reports" -IncludeDetailReport
```

### 3) Watch mode for active migration windows

```powershell
.\MigrationAnalysisV3.ps1 -WatchMode -RefreshIntervalSeconds 300 -ListenerPort 8787
```

### 4) Scope to one migration batch

```powershell
.\MigrationAnalysisV3.ps1 -MigrationBatchName "batch001-50GB" -IncludeDetailReport
```

### 5) Scope to specific mailboxes

```powershell
.\MigrationAnalysisV3.ps1 -Mailbox "jsmith","alex@contoso.com" -IncludeDetailReport
```

### 6) Offline replay from previously exported XML

```powershell
.\MigrationAnalysisV3.ps1 -ImportXmlPath "C:\Reports\Sprint1_RawStats.xml" -ReportPath "C:\Reports"
```

## Common Usage Flows

### Daily operations check

- Run one snapshot report.
- Review health score and failed/slow mailbox list.
- Share HTML and CSV with operations.

### Migration war room

- Run watch mode with a short refresh interval.
- Enable stall/failure alerts.
- Keep Solution Center open for triage support.

Example:

```powershell
.\MigrationAnalysisV3.ps1 `
  -WatchMode `
  -RefreshIntervalSeconds 300 `
  -AlertOnFailure -AlertOnStall `
  -StallThresholdMinutes 45 `
  -TeamsWebhookUrl "https://outlook.office.com/webhook/..."
```

### Post-incident analysis

- Run with `-IncludeDetailReport -ExportDetailXml`.
- Save XML artifacts.
- Re-open later using `-ImportXmlPath` for replay without live dependencies.

## Output Files

Typical generated files:

- `<ReportName>_Report.html` - interactive dashboard
- `<ReportName>_Summary.csv` - summary KPIs
- `<ReportName>_PerMailbox.csv` - mailbox-level rows
- `<ReportName>_RawStats.xml` - optional raw export for replay
- `<ReportName>_SkippedMailboxes.csv` - identities where stats retrieval failed

## High-Value Parameters

- `-IncludeDetailReport`: richer troubleshooting data in report.
- `-ExportDetailXml`: save raw stats for offline replay.
- `-WatchMode`: continuous refresh and live operations mode.
- `-Mailbox` or `-MigrationBatchName`: narrow scope quickly.
- `-AlertOnFailure`, `-AlertOnStall`, `-AlertOnComplete`: event-driven alerting.
- `-AutoRetryFailed`: optional automatic retry logic for retryable failures.
- `-ScheduledReports`: periodic reporting workflow.

## Constraints and Behavior Notes

- `-Mailbox` and `-MigrationBatchName` cannot be used together.
- `-ExportDetailXml` requires `-IncludeDetailReport`.
- `-ExportDetailXml` is not allowed in watch mode.
- Watch mode suppresses CSV generation during live loop for performance.
- Watch mode local API binds to `127.0.0.1` on `-ListenerPort`.
- If another process uses the selected listener port, the script clears that port on startup.
- First run may prompt for open-source license acceptance (console and report UI flows).

## File Extensions in This Project

| Extension | Purpose |
|---|---|
| `.ps1` | Main automation and report generation logic |
| `.html` | Interactive dashboard/report output |
| `.csv` | Summary and mailbox-level exports |
| `.xml` | Raw move statistics export for replay and evidence |

## Main File

- `MigrationAnalysisV3.ps1`
