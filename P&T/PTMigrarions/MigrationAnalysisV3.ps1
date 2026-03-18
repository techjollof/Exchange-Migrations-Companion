#Requires -Version 5.1
<#
.SYNOPSIS
    Analyzes Exchange mailbox migration move requests and generates detailed reports.

.DESCRIPTION
    Collects move request statistics from Exchange Online (live mode) or a previously
    exported XML file (offline mode), evaluates health metrics, identifies bottlenecks,
    and produces HTML + CSV reports with actionable recommendations.

.PARAMETER StatusFilter
    Filter move requests by status.
    Accepts: All, Queued, InProgress, AutoSuspended, CompletedWithWarning, Completed, Failed
    Default: All

.PARAMETER Mailbox
    One or more mailbox identifiers to scope the analysis to specific mailboxes.
    Each value is matched (case-insensitive, wildcard supported) against:
      Alias, DisplayName, EmailAddress, ExchangeGuid, MailboxGuid
    Example: -Mailbox "jsmith","pthkit@domain.com","11fba157-f711-4ae6-a7c0-8c9ac6e4e10f"
    Cannot be combined with -MigrationBatchName.

.PARAMETER MigrationBatchName
    Filter to a specific migration batch name, e.g. "batch001-50GB".
    Cannot be combined with -Mailbox.

.PARAMETER SinceDate
    Only include move requests with a QueuedTimestamp on or after this date.
    Example: -SinceDate "2026-03-01"

.PARAMETER IncludeCompleted
    Include already-completed move requests in the analysis.

.PARAMETER IncludeDetailReport
    Runs Pass 2 with -IncludeReport against active mailboxes (~1-2 min extra).
    Enables: SourceSideDuration%, DestSideDuration%, AverageSourceLatency,
             WordBreakingDuration%, LastFailure messages.
    Without this switch the script runs in fast mode (~10s Pass 1 only).

.PARAMETER ExportDetailXml
    Requires -IncludeDetailReport. Exports the raw statistics array to CLIXML
    alongside the HTML/CSV reports for later offline replay.
    Output file: <ReportName>_RawStats.xml

.PARAMETER WatchMode
    Continuously regenerates the report every -RefreshIntervalSeconds seconds.
    Starts a local HTTP API on -ListenerPort so the browser control panel can
    switch batches, filter mailboxes, and trigger refreshes without restarting.
    Press Ctrl+C to stop. Only valid in Live mode.

.PARAMETER RefreshIntervalSeconds
    Interval between report refreshes in watch mode. Default: 60 seconds. Range: 10–3600.

.PARAMETER ListenerPort
    TCP port for the local HTTP API used by the browser control panel in watch mode.
    Default: 8787. Only binds to 127.0.0.1 — never exposed to the network.
    Any existing process using this port will be automatically killed on startup.

.PARAMETER ImportXmlPath
    Path to a previously exported <ReportName>_RawStats.xml file.
    Skips all EXO connections and re-renders the report from saved data.
    Cannot be combined with live-mode parameters.

.PARAMETER ReportPath
    Directory where reports are saved. Default: current directory.

.PARAMETER ReportName
    Base filename for all output files. Default: MigrationReport_<timestamp>

.PARAMETER SkipHtml
    Suppress HTML report output.

.PARAMETER SkipCsv
    Suppress CSV report output.

.PARAMETER BatchSize
    Number of mailboxes per Get-MoveRequestStatistics EXO call. Default: 500

.PARAMETER Percentile
    Percentile of mailboxes (by transfer rate) used for aggregate metrics.
    Microsoft default is 90 — the slowest 10% are excluded to prevent outliers
    skewing the batch averages. Use 100 to include all mailboxes. Default: 90

.EXAMPLE
    # Full live analysis with detail report exported for later use
    .\MigrationAnalysis.ps1 -IncludeDetailReport -ExportDetailXml -ReportPath "C:\Reports"

.EXAMPLE
    # Fast mode — no detail report, just transfer rates and stalls
    .\MigrationAnalysis.ps1 -ReportPath "C:\Reports"

.EXAMPLE
    # Check specific mailboxes only
    .\MigrationAnalysis.ps1 -Mailbox "jsmith","pthkit@p-t-group.com" -IncludeDetailReport

.EXAMPLE
    # Filter to a specific batch since March 2026
    .\MigrationAnalysis.ps1 -MigrationBatchName "batch001-50GB" -SinceDate "2026-03-01"

.EXAMPLE
    # Offline replay from previously saved XML
    .\MigrationAnalysis.ps1 -ImportXmlPath "C:\Reports\Sprint1_RawStats.xml" -ReportPath "C:\Reports"
#>

[CmdletBinding(DefaultParameterSetName = "Live")]
param (
    # ── Live mode — filtering ────────────────────────────────────────────────
    [Parameter(ParameterSetName = "Live")]
    [ValidateSet("All","Queued","InProgress","AutoSuspended","CompletedWithWarning","Completed","Failed")]
    [string]$StatusFilter = "All",

    [Parameter(ParameterSetName = "Live")]
    [string[]]$Mailbox,

    [Parameter(ParameterSetName = "Live")]
    [string]$MigrationBatchName,

    [Parameter(ParameterSetName = "Live")]
    [datetime]$SinceDate,

    [Parameter(ParameterSetName = "Live")]
    [switch]$IncludeCompleted,

    # ── Live mode — report depth ─────────────────────────────────────────────
    [Parameter(ParameterSetName = "Live")]
    [switch]$IncludeDetailReport,

    [Parameter(ParameterSetName = "Live")]
    [switch]$ExportDetailXml,

    [Parameter(ParameterSetName = "Live")]
    [ValidateRange(1,1000)]
    [int]$BatchSize = 500,

    [Parameter(ParameterSetName = "Live")]
    [Parameter(ParameterSetName = "FromXml")]
    [ValidateRange(1,100)]
    [int]$Percentile = 90,

    # Mailboxes smaller than this threshold (GB) skip Rate and Efficiency health scoring
    # — those metrics are meaningless for tiny mailboxes where fixed MRS overhead dominates.
    # Default: 0.1 GB (100 MB). Set to 0 to score all mailboxes regardless of size.
    [Parameter(ParameterSetName = "Live")]
    [Parameter(ParameterSetName = "FromXml")]
    [double]$MinSizeGBForScoring = 0.1,

    # ── Watch mode (Live only) ───────────────────────────────────────────────
    # Continuously regenerates the report every N seconds.
    # Press Ctrl+C to stop. Only valid in Live mode.
    [Parameter(ParameterSetName = "Live")]
    [switch]$WatchMode,

    [Parameter(ParameterSetName = "Live")]
    [ValidateRange(10,3600)]
    [int]$RefreshIntervalSeconds = 60,

    [Parameter(ParameterSetName = "Live")]
    [ValidateRange(1024,65535)]
    [int]$ListenerPort = 8787,

    # ── Offline / XML replay mode ────────────────────────────────────────────
    [Parameter(ParameterSetName = "FromXml", Mandatory)]
    [ValidateScript({ Test-Path $_ -PathType Leaf })]
    [string]$ImportXmlPath,

    # ── Common output parameters ─────────────────────────────────────────────
    [Parameter(ParameterSetName = "Live")]
    [Parameter(ParameterSetName = "FromXml")]
    [string]$ReportPath = (Get-Location).Path,

    [Parameter(ParameterSetName = "Live")]
    [Parameter(ParameterSetName = "FromXml")]
    [string]$ReportName = "MigrationReport_$(Get-Date -Format 'yyyyMMdd_HHmmss')",

    [Parameter(ParameterSetName = "Live")]
    [Parameter(ParameterSetName = "FromXml")]
    [switch]$SkipHtml,

    [Parameter(ParameterSetName = "Live")]
    [Parameter(ParameterSetName = "FromXml")]
    [switch]$SkipCsv
)

#region ── Helpers ──────────────────────────────────────────────────────────────

function Write-Log {
    param([string]$Message, [ValidateSet("INFO","WARN","ERROR","SUCCESS")]$Level = "INFO")
    $color = @{ INFO="Cyan"; WARN="Yellow"; ERROR="Red"; SUCCESS="Green" }[$Level]
    Write-Host "[$Level] $(Get-Date -Format 'HH:mm:ss')  $Message" -ForegroundColor $color
}

# ── Byte / size helpers (ported from Microsoft's official MRS perf script) ───────

function ByteStrToBytes {
    # Handles three formats:
    #   1. Plain integer/long  — REST API returns raw bytes as JSON number
    #   2. ByteQuantifiedSize string — "30.42 GB (32,658,835,018 bytes)"
    #   3. Live ByteQuantifiedSize object — has .ToBytes() method
    param($val)
    if ($null -eq $val) { return [int64]0 }
    # Plain numeric — REST JSON path, already bytes
    if ($val -is [int64] -or $val -is [int32] -or $val -is [double] -or $val -is [long]) {
        return [int64]$val
    }
    $str = $val.ToString()
    # Pure numeric string
    if ($str -match '^[0-9]+$') { return [int64]$str }
    # "X.XX GB (N,NNN,NNN bytes)"
    if ($str -match '\(([0-9,]+)\s+bytes\)') {
        return [int64]($Matches[1] -replace ',','')
    }
    # Live object .ToBytes()
    try { return [int64]$val.ToBytes() } catch {}
    return [int64]0
}

function ConvertTo-GB {
    param($Value)
    if ($null -eq $Value) { return 0 }
    return [math]::Round((ByteStrToBytes $Value) / 1GB, 4)
}

function ToMB {
    param($Value)
    if ($null -eq $Value) { return 0 }
    return (ByteStrToBytes $Value) / 1MB
}

function ToKB {
    param($Value)
    if ($null -eq $Value) { return 0 }
    return (ByteStrToBytes $Value) / 1KB
}

function GetArchiveSize {
    # Returns null if this is an archive-only move (don't double-count)
    param($size, $flags)
    if ($flags -and $flags.ToString().Contains('MoveOnlyArchiveMailbox')) { return $null }
    return $size
}

function SafeTicks {
    # Handles three formats:
    #   1. Plain integer/long  — REST JSON returns ticks as a number
    #   2. ISO 8601 / .NET TimeSpan string — "20:51:16.4533872" or "P1DT2H3M4S"
    #   3. Live or deserialized TimeSpan — has .Ticks property
    param($Value)
    if ($null -eq $Value) { return [int64]0 }
    # Plain numeric — REST JSON path
    if ($Value -is [int64] -or $Value -is [int32] -or $Value -is [double] -or $Value -is [long]) {
        return [int64]$Value
    }
    # Try .Ticks (live or deserialized TimeSpan)
    try { return [int64]$Value.Ticks } catch {}
    # Try parsing as TimeSpan string ("d.hh:mm:ss.fffffff" or "hh:mm:ss.fffffff")
    try {
        $ts = [TimeSpan]::Parse($Value.ToString())
        return [int64]$ts.Ticks
    } catch {}
    return [int64]0
}

function ConvertTo-TotalMs {
    param($Value)
    if ($null -eq $Value) { return 0 }
    try { return [double]$Value.TotalMilliseconds } catch { return 0 }
}

function ConvertTo-TotalHours {
    param($Value)
    if ($null -eq $Value) { return 0 }
    try { return [double]$Value.TotalHours } catch { return 0 }
}

# ── Health thresholds — script-scoped, built once ───────────────────────────
#
# Each metric has a Direction and threshold values:
#   Direction "High"  → only penalise when value is TOO HIGH (lower is better)
#   Direction "Low"   → only penalise when value is TOO LOW  (higher is better)
#
# Source: Microsoft MRS perf reference (aka.ms/MailboxMigrationPerfScript)
#
# SourceSideDuration / DestinationSideDuration:
#   MS says >80% source (or >40% dest) = bottleneck.  Being LOW is fine —
#   it means a cloud-only or efficient move.  Do NOT penalise low values.
#
# MoveEfficiencyPercent:
#   MS says healthy 75-100%.  Values slightly above 100% occur on small
#   mailboxes due to metadata overhead — not a problem.  Only penalise
#   when below 75% (excessive retransmission / transient failures).
#
# AvgPerMoveTransferRateGBPerHour:
#   MS says >0.5 GB/h healthy, 0.3-1 normal range.  Only penalise LOW.
#
# "Too high" metrics (stalls, latency, failures):
#   Penalise when above ceiling.  Being at zero is perfect.

$script:HealthThresholds = @{
    # Direction=Low  — healthy floor, warning floor (penalise below these)
    AvgPerMoveTransferRateGBPerHour = @{ Direction="Low";  HealthyFloor=0.5; WarningFloor=0.3  }
    MoveEfficiencyPercent           = @{ Direction="Low";  HealthyFloor=75;  WarningFloor=60   }

    # Direction=High — healthy ceiling, warning ceiling (penalise above these)
    SourceSideDuration              = @{ Direction="High"; HealthyCeil=80;   WarningCeil=85    }
    DestinationSideDuration         = @{ Direction="High"; HealthyCeil=40;   WarningCeil=50    }
    WordBreakingDuration            = @{ Direction="High"; HealthyCeil=15;   WarningCeil=20    }
    TransientFailureDurations       = @{ Direction="High"; HealthyCeil=5;    WarningCeil=10    }
    OverallStallDurations           = @{ Direction="High"; HealthyCeil=15;   WarningCeil=20    }
    AverageSourceLatency            = @{ Direction="High"; HealthyCeil=100;  WarningCeil=150   }
}

function Get-HealthStatus {
    param([string]$Metric, [double]$Value)
    if (-not $script:HealthThresholds.ContainsKey($Metric)) { return "N/A" }
    $t = $script:HealthThresholds[$Metric]

    if ($t.Direction -eq "Low") {
        # Good = high value. Penalise when too LOW.
        if ($Value -ge $t.HealthyFloor) { return "Healthy" }
        if ($Value -ge $t.WarningFloor) { return "Warning" }
        return "Critical"
    }
    else {
        # Direction = "High". Good = low value. Penalise when too HIGH.
        if ($Value -le $t.HealthyCeil) { return "Healthy" }
        if ($Value -le $t.WarningCeil) { return "Warning" }
        return "Critical"
    }
}

function Get-BottleneckAnalysis {
    # Identifies migration bottleneck from SourceSideDuration and DestinationSideDuration.
    # Causes and recommendations sourced from Microsoft MRS perf reference:
    # https://techcommunity.microsoft.com/blog/exchange/mailbox-migration-performance-analysis/587134
    param([double]$SourcePct, [double]$DestPct)

    $result = [PSCustomObject]@{
        Bottleneck      = "Balanced"
        Severity        = "None"
        Explanation     = "SourceSideDuration ($SourcePct%) and DestinationSideDuration ($DestPct%) are within normal ranges. The migration is progressing efficiently."
        Causes          = @()
        Recommendations = @()
    }

    if ($SourcePct -gt 80) {
        $result.Bottleneck  = "Source Side"
        $result.Severity    = if ($SourcePct -gt 90) { "Critical" } else { "Warning" }
        $result.Explanation = "SourceSideDuration ($SourcePct%) exceeds the healthy ceiling of 80%. Time is dominated by the on-premises MRSProxy service. A higher average latency and transient failure rate will increase this value."
        $result.Causes      = @(
            "High transient failures — The most common cause is connectivity issues to the on-premises MRSProxy web service. Check TransientFailureDurations and MailboxLockedStall values. The source mailbox may get locked when a transient failure occurs, lowering performance.",
            "Misconfigured network load balancers — If load balancing MRS requests, the load balancer must direct all calls for a specific migration request to the same server hosting MRSProxy. Incorrect routing causes calls to hit the wrong MRSProxy instance and fail.",
            "High network latency — Office 365 MRS makes periodic no-op WCF calls to the on-premises MRSProxy and measures their duration as AverageSourceLatency. Values above 100ms indicate high latency between Office 365 and on-premises MRSProxy.",
            "Source servers too busy — CPU, Memory, or Disk IO on the on-premises Mailbox or Client Access servers may be high, causing MRSProxy to respond slowly to web service calls.",
            "Scale issues — Migration requests may not be load balanced across servers, or other services are running on the same servers as MRSProxy."
        )
        $result.Recommendations = @(
            "Check TransientFailureDurations — if elevated, inspect the failures log and review MRSProxy connectivity.",
            "Check MailboxLockedStall — elevated values confirm transient failure-related mailbox locking.",
            "Verify load balancer configuration: all migration calls for a given request must route to the same MRSProxy server (use ExchangeCookie affinity).",
            "If AverageSourceLatency > 100ms: increase the ExportBufferSizeOverrideKB parameter in MSExchangeMailboxReplication.exe.config (e.g. 7500) to reduce the number of migration calls — requires Exchange 2013 SP1+.",
            "Consider migrating from servers geographically closer to Office 365 datacenters if latency is network-distance related.",
            "Reduce empty or excessive mailbox folders — high folder counts amplify the impact of network latency.",
            "Release system resources (CPU, Memory, Disk IO) on the Mailbox and Client Access servers.",
            "Distribute source mailboxes across multiple Mailbox servers and databases on separate physical drives."
        )
    }
    elseif ($DestPct -gt 40) {
        $result.Bottleneck  = "Destination Side"
        $result.Severity    = if ($DestPct -gt 55) { "Critical" } else { "Warning" }
        $result.Explanation = "DestinationSideDuration ($DestPct%) exceeds the healthy ceiling of 40%. Time is dominated by the Office 365 MRSProxy service. Target stalls (CPU, ContentIndexing, HighAvailability) increase this value."
        $result.Causes      = @(
            "Office 365 system resources — The destination Office 365 servers may be too busy handling other service requests for your organisation.",
            "Word breaking stalls (WordBreakingDuration > 15%) — Content migrated to Office 365 is tokenised for indexing by the search service, coordinated by MRS. High values indicate the content indexing service on the target server is busy.",
            "Content Indexing stalls — The content indexing service on the Office 365 target servers is backlogged.",
            "High Availability stalls — The HA service responsible for replicating data to multiple Office 365 servers is busy.",
            "Target CPU stalls — The Office 365 server CPU consumption is too high."
        )
        $result.Recommendations = @(
            "Review ContentIndexingStalls — if elevated, the Office 365 search service is backlogged on the target server.",
            "Review WordBreakingDuration — values above 15% confirm content indexing pressure on the destination.",
            "Review HighAvailabilityStalls — elevated values mean database replication is causing MRS to wait.",
            "Review TargetCPUStalls — elevated values confirm destination CPU is the constraint.",
            "Remove and re-insert migration requests — this reassigns them to less busy Office 365 servers.",
            "Spread migrations across more batches to reduce simultaneous load on the same destination servers.",
            "Open a Microsoft support request if TargetCPUStalls or ContentIndexingStalls are persistently high."
        )
    }
    return $result
}

#endregion

#region ── Core Processing ──────────────────────────────────────────────────────

function Get-MoveRequests {
    param(
        [string]$StatusFilter     = "All",
        [bool]$IncludeCompleted,
        [string[]]$Mailbox,
        [string]$MigrationBatchName,
        [datetime]$SinceDate
    )

    $filterDesc = "Status=$StatusFilter"
    if ($Mailbox)             { $filterDesc += ", Mailbox=$($Mailbox -join ',')" }
    if ($MigrationBatchName)  { $filterDesc += ", Batch=$MigrationBatchName" }
    if ($SinceDate)           { $filterDesc += ", Since=$($SinceDate.ToString('yyyy-MM-dd'))" }
    Write-Log "Retrieving move requests ($filterDesc)..."

    try {
        $all = Get-MoveRequest -ErrorAction Stop

        # Status filter
        $moves = switch ($StatusFilter) {
            "All"   { if ($IncludeCompleted) { $all } else { $all | Where-Object { $_.Status -ne 'Queued' } } }
            default { $all | Where-Object { $_.Status -eq $StatusFilter } }
        }
        if (-not $IncludeCompleted -and $StatusFilter -eq "All") {
            $moves = @($moves) | Where-Object { $_.Status -ne 'Queued' }
        }

        # MigrationBatchName filter
        if ($MigrationBatchName) {
            # EXO prefixes batch names with "MigrationService:" internally.
            # Match against both the raw value and the unprefixed version so
            # both "keplerip-Aaron" and "MigrationService:keplerip-Aaron" work.
            $moves = @($moves) | Where-Object {
                $bn = "$($_.BatchName)" -replace '^MigrationService:',''
                $bn -like $MigrationBatchName -or
                "$($_.BatchName)" -like $MigrationBatchName -or
                "$($_.BatchName)" -like "*$MigrationBatchName*"
            }
        }

        # SinceDate filter — QueuedTimestamp or StartTimestamp
        if ($SinceDate) {
            $moves = @($moves) | Where-Object {
                $ts = if ($_.QueuedTimestamp) { $_.QueuedTimestamp } else { $_.StartTimestamp }
                $ts -and $ts -ge $SinceDate
            }
        }

        # Mailbox filter — alias, display name, email, ExchangeGuid, MailboxGuid
        # NOTE: Get-MoveRequest objects may not have ExternalEmailAddress populated.
        # Also try direct EXO resolution: pass each filter to Get-MoveRequest -Identity
        # to let EXO resolve email addresses natively.
        if ($Mailbox -and $Mailbox.Count -gt 0) {

            # First try property-based matching on already-retrieved moves
            $propMatched = @($moves) | Where-Object {
                $mr = $_
                $matched = $false
                foreach ($filter in $Mailbox) {
                    # ExternalEmailAddress may be absent on Get-MoveRequest objects —
                    # also check Identity string which often contains the email
                    $emailStr    = "$($mr.ExternalEmailAddress)" -replace '^(?:SMTP|smtp):',''
                    $identityStr = "$($mr.Identity)"
                    if (
                        ($mr.ExchangeGuid  -and "$($mr.ExchangeGuid)"  -like $filter) -or
                        ($mr.MailboxGuid   -and "$($mr.MailboxGuid)"   -like $filter) -or
                        ($mr.Alias         -and $mr.Alias              -like $filter) -or
                        ($mr.DisplayName   -and $mr.DisplayName        -like $filter) -or
                        ($emailStr         -and $emailStr              -like $filter) -or
                        ($identityStr      -and $identityStr           -like "*$filter*")
                    ) { $matched = $true; break }
                }
                $matched
            }

            # If property match found nothing, try fetching each filter directly
            # via Get-MoveRequest -Identity — EXO resolves email addresses natively
            if (@($propMatched).Count -eq 0) {
                Write-Log "  Property match found nothing — trying direct EXO identity lookup..." -Level INFO
                $directMatched = [System.Collections.Generic.List[object]]::new()
                foreach ($filter in $Mailbox) {
                    try {
                        $direct = Get-MoveRequest -Identity $filter -ErrorAction Stop
                        if ($direct) {
                            @($direct) | ForEach-Object { $directMatched.Add($_) }
                            Write-Log "  Direct lookup '$filter' — found $(@($direct).Count) move(s)." -Level INFO
                        }
                    } catch {}
                }
                $moves = if ($directMatched.Count -gt 0) { $directMatched.ToArray() } else { @() }
            } else {
                $moves = $propMatched
            }
        }

        $count = @($moves).Count
        if ($count -eq 0) {
            Write-Log "No move requests matched the specified filters." -Level WARN
        } else {
            Write-Log "Found $count move request(s)." -Level SUCCESS
        }
        return $moves
    }
    catch {
        Write-Log "Failed to retrieve move requests: $_" -Level ERROR
        throw
    }
}

function Resolve-MoveGuid {
    param($Move)
    $e = [Guid]::Empty
    if ($Move.ExchangeGuid -and [Guid]$Move.ExchangeGuid -ne $e) { return "$($Move.ExchangeGuid)" }
    if ($Move.MailboxGuid  -and [Guid]$Move.MailboxGuid  -ne $e) { return "$($Move.MailboxGuid)"  }
    if ($Move.Guid         -and [Guid]$Move.Guid         -ne $e) { return "$($Move.Guid)"         }
    # No GUID available — return the Alias or Identity so EXO can resolve by name
    if ($Move.Alias) { return "$($Move.Alias)" }
    return "$($Move.Identity)"
}

function Test-IsGuid {
    param([string]$Value)
    $g = [System.Guid]::Empty
    return [System.Guid]::TryParse($Value, [ref]$g)
}


function Get-MoveStats {
    <#
    .SYNOPSIS
        Two-pass retrieval strategy to minimise -IncludeReport overhead.

    .NOTES
        WHY -IncludeReport IS SLOW:
          EXO must assemble the full Report object server-side — session statistics,
          provider durations, latency samples, failure logs. This is the dominant
          cost (~35s per mailbox). Without -IncludeReport the same call takes <1s.

        WHAT ACTUALLY NEEDS -IncludeDetailReport:
          Only 5 things require the Report object:
            - SourceSideDuration %      (Report.SessionStatistics.SourceProviderInfo)
            - DestinationSideDuration % (Report.SessionStatistics.DestinationProviderInfo)
            - WordBreakingDuration %    (Report.SessionStatistics.TotalTimeProcessingMessages)
            - AverageSourceLatency      (Report.SessionStatistics.SourceLatencyInfo)
            - LastFailure message       (Report.Failures)

          Everything else — all stall metrics, sizes, transfer rates, timestamps,
          item counts — is a top-level property available WITHOUT -IncludeReport.

        TWO-PASS STRATEGY:
          Pass 1 — Fast (seconds, ALL mailboxes, no -IncludeReport)
            Gets all stalls, rates, sizes, items, timestamps.

          Pass 2 — Slow (only ACTIVE mailboxes, with -IncludeReport)
            Active = InProgress or AutoSuspended — mailboxes currently moving.
            These are the only ones where the Report object is current and meaningful.
            Synced/Completed mailboxes have a static report — we skip them entirely.

        EXPECTED SAVINGS (your environment):
          16 mailboxes, 2 InProgress:
            Before: 16 × -IncludeReport = ~10 min
            After:  Fast pass (all 16, no report) ~10s
                  + Slow pass (2 InProgress, with report) ~1-2 min
                  = ~1.5 min total  (~85% faster)
    #>
    param(
        $Moves,
        [ValidateRange(1,1000)]
        [int]$BatchSize = 500,
        [bool]$IncludeDetailReport = $false,
        # When set, skip two-pass and call Get-MoveRequestStatistics per identity directly.
        # Used when -Mailbox is specified — EXO resolves by email/alias natively.
        [string[]]$DirectIdentities = @()
    )

    # Statuses considered "active" — report is current and meaningful
    $activeStatuses = @('InProgress','AutoSuspended','Suspended')

    $moveArr = @($Moves)
    $total   = $moveArr.Count
    $results = [System.Collections.Generic.List[object]]::new()
    $failed  = [System.Collections.Generic.List[object]]::new()

    # Pre-resolve GUIDs
    $resolvedMoves = foreach ($move in $moveArr) {
        $guid = Resolve-MoveGuid $move
        if ($guid -eq "$($move.Identity)") {
            Write-Log "  [WARN] No GUID for '$($move.DisplayName)' — using Identity string." -Level WARN
        }
        $statusStr = "$($move.Status)"
        # Normalise status integer (deserialized) to string if needed
        if ($statusStr -match '^\d+$') {
            $statusStr = switch ([int]$statusStr) {
                2 { 'InProgress' } 3 { 'AutoSuspended' } 9 { 'Suspended' }
                default { $statusStr }
            }
        }
        [PSCustomObject]@{
            DisplayName = $move.DisplayName
            Alias       = $move.Alias
            StatusStr   = $statusStr
            IsActive    = ($statusStr -in $activeStatuses)
            Guid        = $guid
        }
    }

    $activeCount   = @($resolvedMoves | Where-Object { $_.IsActive }).Count
    $inactiveCount = $total - $activeCount

    Write-Log "Two-pass fetch: $total mailboxes ($activeCount active, $inactiveCount static)." -Level INFO

    # ════════════════════════════════════════════════════════════════════
    # DIRECT MODE — when specific identities are supplied (e.g. -Mailbox email)
    # skip two-pass entirely and call Get-MoveRequestStatistics per identity
    # ════════════════════════════════════════════════════════════════════
    $fastStatMap = @{}   # guid → fast stat object

    if ($DirectIdentities.Count -gt 0) {
        $includeReport = $IncludeDetailReport
        Write-Log "  Direct fetch ($($DirectIdentities.Count) identity/identities, IncludeReport=$includeReport)..." -Level INFO
        foreach ($identity in $DirectIdentities) {
            try {
                $fs = if ($includeReport) {
                    Get-MoveRequestStatistics -Identity $identity -IncludeReport -ErrorAction Stop
                } else {
                    Get-MoveRequestStatistics -Identity $identity -ErrorAction Stop
                }
                if ($fs) {
                    $key = "$($fs.ExchangeGuid)"
                    $fastStatMap[$key] = $fs
                    Write-Log "    OK [$($fs.DisplayName)] via '$identity'" -Level INFO
                }
            } catch {
                $itemErr = $_.Exception.Message -replace "`r`n"," "
                Write-Log "    FAILED [$identity]: $itemErr" -Level WARN
                $failed.Add([PSCustomObject]@{
                    DisplayName = $identity
                    Alias       = $identity
                    GuidUsed    = $identity
                    Status      = ""
                    Error       = $itemErr
                })
            }
        }
        Write-Log "  Direct fetch complete — $($fastStatMap.Count) stats retrieved." -Level INFO

        # Skip both Pass 1 and Pass 2 — jump straight to results
        $results.AddRange([object[]]($fastStatMap.Values))
        return [PSCustomObject]@{
            Stats  = $results.ToArray()
            Failed = $failed.ToArray()
        }
    }

    # ════════════════════════════════════════════════════════════════════
    # PASS 1 — Fast: all mailboxes, NO -IncludeReport
    # Builds a lookup by GUID for merging with Pass 2 results
    # ════════════════════════════════════════════════════════════════════
    Write-Log "  Pass 1 — Fast stats (all $total mailboxes, no report)..." -Level INFO

    $batchCount = [math]::Ceiling($total / $BatchSize)
    for ($b = 0; $b -lt $batchCount; $b++) {
        $s = $b * $BatchSize
        $e = [math]::Min($s + $BatchSize - 1, $total - 1)
        $slice = @($resolvedMoves[$s..$e])

        # Separate true GUIDs from alias/identity fallbacks
        # EXO batch pipe works best with GUIDs; name-based items go per-mailbox directly
        $guidItems  = @($slice | Where-Object { Test-IsGuid $_.Guid })
        $nameItems  = @($slice | Where-Object { -not (Test-IsGuid $_.Guid) })

        # Batch call for GUID-identified mailboxes
        if ($guidItems.Count -gt 0) {
            try {
                $fastStats = $guidItems.Guid | Get-MoveRequestStatistics -ErrorAction Stop
                foreach ($fs in @($fastStats)) {
                    if ($fs) { $fastStatMap["$($fs.ExchangeGuid)"] = $fs }
                }
                Write-Log "    Batch $($b+1)/$batchCount — $(@($fastStats).Count) returned." -Level INFO
            }
            catch {
                Write-Log "    Batch $($b+1)/$batchCount failed: $($_.Exception.Message -replace '`r`n',' ') — retrying per-mailbox..." -Level WARN
                foreach ($item in $guidItems) {
                    try {
                        $fs = Get-MoveRequestStatistics -Identity $item.Guid -ErrorAction Stop
                        if ($fs) { $fastStatMap["$($fs.ExchangeGuid)"] = $fs }
                    } catch {}
                }
            }

            # EXO silently returns 0 results for some completed mailboxes when piped by GUID.
            # Detect any that were missed and retry by Alias directly.
            $missedItems = @($guidItems | Where-Object {
                $g = $_.Guid
                -not ($fastStatMap.Keys | Where-Object { $_ -eq $g })
            })
            foreach ($item in $missedItems) {
                Write-Log "    Retrying by alias: $($item.Alias) (GUID batch returned nothing)" -Level INFO
                try {
                    $fs = Get-MoveRequestStatistics -Identity $item.Alias -ErrorAction Stop
                    if ($fs) {
                        $key = "$($fs.ExchangeGuid)"
                        if (-not $key -or $key -eq [Guid]::Empty.ToString()) { $key = $item.Guid }
                        $fastStatMap[$key] = $fs
                        $item.Guid = $key
                    }
                }
                catch {
                    Write-Log "    Retrying by Identity: $($item.Guid)" -Level INFO
                    try {
                        $fs = Get-MoveRequestStatistics -Identity $item.Guid -ErrorAction Stop
                        if ($fs) { $fastStatMap["$($fs.ExchangeGuid)"] = $fs }
                    } catch {}
                }
            }
        }

        # Per-mailbox call for name/alias-identified mailboxes (no GUID available)
        foreach ($item in $nameItems) {
            Write-Log "    Fetching by alias: $($item.Alias) (no GUID available)" -Level INFO
            try {
                $fs = Get-MoveRequestStatistics -Identity $item.Guid -ErrorAction Stop
                if ($fs) {
                    # Key by ExchangeGuid now that we have it
                    $key = "$($fs.ExchangeGuid)"
                    $fastStatMap[$key] = $fs
                    # Update the resolvedMove so Pass 2 can find it by real GUID
                    $item.Guid = $key
                }
            }
            catch {
                $itemErr = $_.Exception.Message -replace "`r`n"," "
                Write-Log "    FAILED [$($item.DisplayName)]: $itemErr" -Level WARN
            }
        }
    }

    # ── Final sweep — catch any mailbox still missing from the map ─────────────
    # Regardless of status or GUID availability, if a mailbox didn't make it
    # into $fastStatMap after all batches, fetch it individually by Alias.
    $stillMissing = @($resolvedMoves | Where-Object {
        $g = $_.Guid
        -not ($fastStatMap.Keys | Where-Object { $_ -eq $g })
    })
    if ($stillMissing.Count -gt 0) {
        Write-Log "  Fetching $($stillMissing.Count) missing mailbox(es) individually..." -Level INFO
        foreach ($item in $stillMissing) {
            $fetched = $false
            # Try alias first — most reliable across all statuses in EXO
            foreach ($identity in @($item.Alias, $item.Guid, $item.DisplayName) | Where-Object { $_ }) {
                try {
                    $fs = Get-MoveRequestStatistics -Identity $identity -ErrorAction Stop
                    if ($fs) {
                        $key = "$($fs.ExchangeGuid)"
                        if (-not $key -or $key -eq [System.Guid]::Empty.ToString()) { $key = $item.Guid }
                        $fastStatMap[$key] = $fs
                        $item.Guid = $key
                        Write-Log "    OK [$($item.DisplayName)] via '$identity'" -Level INFO
                        $fetched = $true
                        break
                    }
                } catch {}
            }
            if (-not $fetched) {
                Write-Log "    FAILED [$($item.DisplayName)] — could not retrieve via alias, GUID, or display name." -Level WARN
            }
        }
    }

    Write-Log "  Pass 1 complete — $($fastStatMap.Count) stats retrieved." -Level INFO

    # ════════════════════════════════════════════════════════════════════
    # PASS 2 — Slow: active mailboxes only, WITH -IncludeReport
    # Merges Report fields into the fast stat objects
    # ════════════════════════════════════════════════════════════════════
    # Pass 2 target: when -IncludeDetailReport is set, fetch Report for ALL mailboxes
    # (not just active ones). Completed/Synced mailboxes have a valid historical Report
    # with SourceSideDuration, latency, WordBreaking, LastFailure — all useful for analysis.
    # Active-only was a bandwidth optimisation but breaks single/completed mailbox runs.
    $activeGuids = @($resolvedMoves | Where-Object { $_.IsActive } | Select-Object -ExpandProperty Guid)
    # Build report GUIDs — simply use the keys already in $fastStatMap (those are the
    # real ExchangeGuids EXO returned). Wrap in @() to force array — a single-key
    # hashtable returns a bare string from .Keys, and Select-Object -First 1 on a
    # bare string returns the first CHARACTER, not the string itself.
    $reportGuids = if ($IncludeDetailReport) {
        @($fastStatMap.Keys)
    } else { @() }

    if (-not $IncludeDetailReport) {
        Write-Log "  Pass 2 SKIPPED (-IncludeDetailReport not set) — SourceSideDuration%, DestSideDuration%, Latency and LastFailure will be N/A." -Level WARN
    } elseif ($reportGuids.Count -gt 0) {
        Write-Log "  Pass 2 — Full report ($($reportGuids.Count) mailbox(es) including completed)..." -Level INFO

        $reportBatchCount = [math]::Ceiling($reportGuids.Count / $BatchSize)
        for ($b = 0; $b -lt $reportBatchCount; $b++) {
            $s     = $b * $BatchSize
            $e     = [math]::Min($s + $BatchSize - 1, $reportGuids.Count - 1)
            $slice = @($reportGuids[$s..$e])
            try {
                $reportStats = $slice | Get-MoveRequestStatistics -IncludeReport -ErrorAction Stop
                foreach ($rs in @($reportStats)) {
                    if ($rs) {
                        $key = "$($rs.ExchangeGuid)"
                        if ($fastStatMap.ContainsKey($key)) {
                            # Graft Report onto the fast stat object so downstream
                            # processing gets both top-level and report fields
                            $fastStatMap[$key] | Add-Member -NotePropertyName Report `
                                                            -NotePropertyValue $rs.Report `
                                                            -Force
                        } else {
                            # Fallback: use the full report stat directly
                            $fastStatMap[$key] = $rs
                        }
                    }
                }
                Write-Log "    Report batch $($b+1)/$reportBatchCount — $(@($reportStats).Count) returned." -Level INFO
            }
            catch {
                $errMsg = $_.Exception.Message -replace "`r`n"," "
                Write-Log "    Report batch $($b+1)/$reportBatchCount failed ($errMsg) — retrying per-mailbox..." -Level WARN
                foreach ($guid in $slice) {
                    try {
                        $rs = if ($IncludeDetailReport) {
                            Get-MoveRequestStatistics -Identity $guid -IncludeReport -ErrorAction Stop
                        } else {
                            Get-MoveRequestStatistics -Identity $guid -ErrorAction Stop
                        }
                        if ($rs) {
                            $key = "$($rs.ExchangeGuid)"
                            if ($fastStatMap.ContainsKey($key)) {
                                $fastStatMap[$key] | Add-Member -NotePropertyName Report `
                                                                -NotePropertyValue $rs.Report `
                                                                -Force
                            } else {
                                $fastStatMap[$key] = $rs
                            }
                        }
                    }
                    catch {
                        $itemErr = $_.Exception.Message -replace "`r`n"," "
                        $item = $resolvedMoves | Where-Object { $_.Guid -eq $guid } | Select-Object -First 1
                        Write-Log "    FAILED [$($item.DisplayName)] ($guid): $itemErr" -Level WARN
                        $failed.Add([PSCustomObject]@{
                            DisplayName = $item.DisplayName
                            Alias       = $item.Alias
                            GuidUsed    = $guid
                            Status      = $item.StatusStr
                            Error       = $itemErr
                        })
                    }
                }
            }
        }
        Write-Log "  Pass 2 complete." -Level INFO
    } else {
        Write-Log "  Pass 2 skipped — no mailboxes to report on." -Level INFO
    }

    # Collect final results
    foreach ($entry in $fastStatMap.Values) {
        $results.Add($entry)
    }

    Write-Log "Statistics retrieved: $($results.Count) succeeded, $($failed.Count) failed." -Level SUCCESS

    if ($failed.Count -gt 0) {
        $failed | ForEach-Object {
            Write-Log "  • $($_.DisplayName) | $($_.GuidUsed) | $($_.Error)" -Level WARN
        }
    }

    return [PSCustomObject]@{
        Stats  = $results.ToArray()
        Failed = $failed.ToArray()
    }
}

function Invoke-ProcessStats {
    <#
    .SYNOPSIS
        Core aggregation engine — aligned with Microsoft's official MRS perf script.
        Uses TotalInProgressDuration as the stall denominator (not SyncDuration),
        correct property names for every stall type, Report.SessionStatistics for
        source/destination provider durations and latency, and Ticks-based math
        throughout to avoid floating-point issues with deserialized TimeSpans.
    #>
    param(
        [Parameter(Mandatory)][array]$Stats,
        [Parameter(Mandatory)][string]$Name,
        [int]$Percentile = 90,   # top N% by transfer rate, matching Microsoft's default

        [double]$MinSizeGBForScoring = 0.1

    )

    Write-Log "Processing statistics for batch: $Name (percentile filter: top $Percentile%)"

    #── Time boundaries — linear scan, no Sort-Object allocation ────────────────
    $startTime = $null
    $endTime   = $null
    $lastSuspended = $null

    foreach ($s in $Stats) {
        # Earliest queued (or start) time
        $qt = if ($s.QueuedTimestamp)    { $s.QueuedTimestamp }
              elseif ($s.StartTimestamp) { $s.StartTimestamp  }
              else                       { $null }
        if ($qt -and ($null -eq $startTime -or $qt -lt $startTime)) { $startTime = $qt }

        # Latest completion time
        if ($s.CompletionTimestamp -and ($null -eq $endTime -or $s.CompletionTimestamp -gt $endTime)) {
            $endTime = $s.CompletionTimestamp
        }
        if ($s.SuspendedTimestamp  -and ($null -eq $lastSuspended -or $s.SuspendedTimestamp -gt $lastSuspended)) {
            $lastSuspended = $s.SuspendedTimestamp
        }
    }
    if (-not $endTime) { $endTime = if ($lastSuspended) { $lastSuspended } else { Get-Date } }
    if (-not $startTime) { $startTime = $endTime }

    $duration           = $endTime - $startTime
    $moveDurationTicks  = [math]::Truncate($duration.Ticks)

    #── Per-mailbox detail (Microsoft property names + archive-aware sizing) ────
    $perMailbox = foreach ($s in $Stats) {
        # Archive-aware sizing — separate primary and archive for display,
        # combined total used for efficiency / rate calculations
        $archSize       = GetArchiveSize -size $s.TotalArchiveSize -flags $s.Flags
        $primaryGB      = [math]::Round((ToMB $s.TotalMailboxSize) / 1024, 4)
        $archiveGB      = if ($archSize) { [math]::Round((ToMB $archSize) / 1024, 4) } else { 0 }
        $mbxGB          = [math]::Round($primaryGB + $archiveGB, 4)  # combined for efficiency math
        $xferGB         = ConvertTo-GB $s.BytesTransferred

        # Microsoft uses TotalInProgressDuration.TotalSeconds for rate (not SyncDuration)
        $inProgressSec = try {
            [double]$s.TotalInProgressDuration.TotalSeconds
        } catch { 0 }
        $overallDurStr = try {

            $ts = $s.OverallDuration

            if ($ts) {

                $h  = [math]::Floor([double]$ts.TotalHours)

                $ms = $ts.Minutes

                $ss = $ts.Seconds

                "$($h):$($ms.ToString('00')):$($ss.ToString('00'))"

            } else { "—" }

        } catch { "—" }

        $mbxRateGBph   = if ($inProgressSec -gt 0 -and $xferGB -gt 0) {
            [math]::Round(($xferGB / $inProgressSec) * 3600, 4)
        } else { 0 }

        # Efficiency: logical portion moved vs actual bytes transferred
        $logicalGB = $mbxGB * ($s.PercentComplete / 100)
        $eff = if ($xferGB -gt 0) { [math]::Round($logicalGB / $xferGB * 100, 2) } else { 0 }

        # Latency from Report.SessionStatistics (Microsoft's source)
        $srcLatency = try {
            $lc = $s.Report.SessionStatistics.SourceLatencyInfo
            if ($lc -and $lc.NumberOfLatencySamplingCalls -gt 0) { [math]::Round([double]$lc.Average, 2) } else { $null }
        } catch { $null }

        $dstLatency = try {
            $lc = $s.Report.SessionStatistics.DestinationLatencyInfo
            if ($lc -and $lc.NumberOfLatencySamplingCalls -gt 0) { [math]::Round([double]$lc.Average, 2) } else { $null }
        } catch { $null }

        # Last failure message
        $lastFail = try {
            if ($s.Report.Failures) {
                $last = $s.Report.Failures | Select-Object -Last 1
                # CLIXML may deserialize failures as exception objects (.Message)
                # or as plain strings — handle both
                if ($last -is [string]) { $last -replace '^[^:]+:\s*','' }
                elseif ($last.Message)  { $last.Message }
                else                    { "$last" }
            } else { "" }
        } catch { "" }

        # Pre-compute tick values — try/catch blocks cannot be used
        # directly as PSCustomObject property values in PowerShell
        $tInProgress    = SafeTicks $s.TotalInProgressDuration
        $tCI            = SafeTicks $s.TotalStalledDueToContentIndexingDuration
        $tHA            = SafeTicks $s.TotalStalledDueToMdbReplicationDuration
        $tTargetCPU     = SafeTicks $s.TotalStalledDueToWriteCpu
        $tSourceCPU     = SafeTicks $s.TotalStalledDueToReadCpu
        $tMbxLocked     = SafeTicks $s.TotalStalledDueToMailboxLockedDuration
        $tProxySrc      = SafeTicks $s.TotalStalledDueToReadUnknown
        $tProxyDst      = SafeTicks $s.TotalStalledDueToWriteUnknown
        $tReadThrottle  = SafeTicks $s.TotalStalledDueToReadThrottle
        $tWriteThrottle = SafeTicks $s.TotalStalledDueToWriteThrottle
        $tTransient     = SafeTicks $s.TotalTransientFailureDuration
        $tIdle          = SafeTicks $s.TotalIdleDuration
        $tWordBreak          = try {
            SafeTicks $s.Report.SessionStatistics.TotalTimeProcessingMessages
        } catch { [int64]0 }
        # Pre-compute string fields — try/catch inside @{} PSCustomObject causes parse errors
        # .Value on the array returns all Value strings directly — no ForEach-Object needed
        $dataConsistencyFactors = try {
            $s.DataConsistencyScoringFactors.Value -join ", "
        } catch { "" }
        # .Value extracts the string from the deserialized enum {value:N; Value:"Good"}
        # Confirmed: $s.DataConsistencyScore.Value returns "Good" directly
        $dataConsistencyScoreStr = try {
            "$($s.DataConsistencyScore.Value)"
        } catch { "" }
        $tSrcProvider   = try {
            (SafeTicks $s.Report.SessionStatistics.SourceProviderInfo.TotalDuration) +
            (SafeTicks $s.Report.ArchiveSessionStatistics.SourceProviderInfo.TotalDuration)
        } catch { [int64]0 }
        $tDstProvider   = try {
            (SafeTicks $s.Report.SessionStatistics.DestinationProviderInfo.TotalDuration) +
            (SafeTicks $s.Report.ArchiveSessionStatistics.DestinationProviderInfo.TotalDuration)
        } catch { [int64]0 }

        $emailAddress = try {
            # ExternalEmailAddress is a ProxyAddress object — .ToString() includes "SMTP:" prefix
            # Try .Address first (clean SMTP string), fall back to stripping the prefix
            $ea = $s.ExternalEmailAddress
            if ($null -eq $ea -or "$ea" -eq "") {
                ""
            } elseif ($ea.Address) {
                "$($ea.Address)"
            } else {
                "$ea" -replace '^(?:SMTP|smtp):',''
            }
        } catch { "" }
        $syncStageStr = try {

            "$($s.SyncStage.Value)"

        } catch { "" }


        [PSCustomObject]@{
            Alias                 = $s.Alias
            DisplayName           = $s.DisplayName
            EmailAddress          = $emailAddress
            Status                = $(
                                      # Over remote PS session Status is a deserialized int;
                                      # over a live session it is already a string.
                                      # Try string first — if it looks like a name, use it.
                                      $statusVal = "$($s.Status)"
                                      if ($statusVal -match '^\d+$') {
                                          switch ([int]$statusVal) {
                                              0  { "None" }           1  { "Queued" }
                                              2  { "InProgress" }     3  { "AutoSuspended" }
                                              4  { "CompletedWithWarning" }
                                              5  { "Synced" }         6  { "Completed" }
                                              7  { "CompletedWithSkippedItems" }
                                              8  { "Failed" }         9  { "Suspended" }
                                              10 { "Completed" }      default { $statusVal }
                                          }
                                      } else { $statusVal })
            PercentComplete       = $s.PercentComplete
            MailboxSizeGB         = $mbxGB
            PrimaryMailboxSizeGB  = $primaryGB
            ArchiveMailboxSizeGB  = $archiveGB
            TransferredGB         = $xferGB
            TransferRateGBph      = $mbxRateGBph
            EfficiencyPct         = $eff
            InProgressDuration    = $s.TotalInProgressDuration
            QueuedDuration        = $s.TotalQueuedDuration


            OverallDuration       = $s.OverallDuration
            ItemsTransferred      = $s.ItemsTransferred
            ItemsSkipped          = $s.ItemsSkippedDueToLocalFailure

            LargeItems            = $s.LargeItemsEncountered

            MissingItems          = $s.MissingItemsEncountered
            BadItems              = $s.BadItemsEncountered
            SourceLatencyMs       = $srcLatency
            DestLatencyMs         = $dstLatency
            StartTime             = $s.StartTimestamp
            QueuedTime            = $s.QueuedTimestamp
            CompletionTime        = $s.CompletionTimestamp
            LastFailure           = $lastFail
            TickInProgress        = $tInProgress
            TickCI                = $tCI
            TickHA                = $tHA
            TickTargetCPU         = $tTargetCPU
            TickSourceCPU         = $tSourceCPU
            TickMbxLocked         = $tMbxLocked
            TickProxySrc          = $tProxySrc
            TickProxyDst          = $tProxyDst
            TickTransient         = $tTransient
            TickIdle              = $tIdle
            TickWordBreak         = $tWordBreak
            TickSrcProvider       = $tSrcProvider
            TickDstProvider       = $tDstProvider
            TickReadThrottle      = $tReadThrottle
            TickWriteThrottle     = $tWriteThrottle
            # New fields from actual EXO data
            DataConsistencyScore  = $dataConsistencyScoreStr
            DataConsistencyFactors= $dataConsistencyFactors
            SyncStage             = $syncStageStr
            LastSuccessfulSync    = $s.LastSuccessfulSyncTimestamp
            InitialSeedingDone    = $s.InitialSeedingCompletedTimestamp
            SourceVersion         = $s.SourceVersion
            TargetVersion         = $s.TargetVersion
            RemoteHostName        = $s.RemoteHostName
            MRSServerName         = $s.MRSServerName
            BatchName             = $s.BatchName
        }
    }

    #── Percentile filter (Microsoft default: top 90% by transfer rate) ─────────
    $sorted   = @($perMailbox | Sort-Object TransferRateGBph -Descending)
    $topN     = [math]::Max(1, [math]::Truncate($sorted.Count * ($Percentile / 100)))
    $filtered = $sorted | Select-Object -First $topN
    # Slowest = mailboxes excluded by the percentile filter (sorted ascending by rate)
    $slowest  = if ($topN -lt $sorted.Count) {
        $sorted | Select-Object -Skip $topN | Sort-Object TransferRateGBph
    } else { @() }

    Write-Log "  Using top $topN of $($sorted.Count) mailboxes ($Percentile% percentile) — $($slowest.Count) slowest excluded" -Level INFO

    #── Tick-based aggregates — single foreach loop, one pass over $filtered ────
    # Replaces 12 separate SumTicks pipeline calls (each a full enumeration).
    $totalInProgressTicks  = [int64]0
    $totalCITicks          = [int64]0
    $totalHATicks          = [int64]0
    $totalTargetCPUTicks   = [int64]0
    $totalSourceCPUTicks   = [int64]0
    $totalMbxLockedTicks   = [int64]0
    $totalProxySrcTicks    = [int64]0
    $totalProxyDstTicks    = [int64]0
    $totalReadThrottleTicks  = [int64]0
    $totalWriteThrottleTicks = [int64]0
    $totalTransientTicks   = [int64]0
    $totalIdleTicks        = [int64]0
    $totalWordBreakTicks   = [int64]0
    $totalSrcProviderTicks = [int64]0
    $totalDstProviderTicks = [int64]0

    foreach ($row in $filtered) {
        $totalInProgressTicks  += $row.TickInProgress
        $totalCITicks          += $row.TickCI
        $totalHATicks          += $row.TickHA
        $totalTargetCPUTicks   += $row.TickTargetCPU
        $totalSourceCPUTicks   += $row.TickSourceCPU
        $totalMbxLockedTicks   += $row.TickMbxLocked
        $totalProxySrcTicks      += $row.TickProxySrc
        $totalProxyDstTicks      += $row.TickProxyDst
        $totalReadThrottleTicks  += $row.TickReadThrottle
        $totalWriteThrottleTicks += $row.TickWriteThrottle
        $totalTransientTicks     += $row.TickTransient
        $totalIdleTicks        += $row.TickIdle
        $totalWordBreakTicks   += $row.TickWordBreak
        $totalSrcProviderTicks += $row.TickSrcProvider
        $totalDstProviderTicks += $row.TickDstProvider
    }

    $totalProxyTicks    = $totalProxySrcTicks + $totalProxyDstTicks
    $totalThrottleTicks = $totalReadThrottleTicks + $totalWriteThrottleTicks
    $totalStallTicks    = $totalCITicks + $totalHATicks + $totalTargetCPUTicks +
                          $totalSourceCPUTicks + $totalMbxLockedTicks +
                          $totalProxyTicks + $totalThrottleTicks

    function PctOf {
        param([int64]$Num, [int64]$Den)
        if ($null -eq $Num -or $null -eq $Den -or $Den -eq 0) { return [double]0 }
        return [math]::Round(([double]$Num / [double]$Den) * 100, 2)
    }

    $idlePct          = PctOf $totalIdleTicks        $totalInProgressTicks
    $sourceSidePct    = PctOf $totalSrcProviderTicks  $totalInProgressTicks
    $destSidePct      = PctOf $totalDstProviderTicks  $totalInProgressTicks
    $wordBreakPct     = PctOf $totalWordBreakTicks    $totalInProgressTicks
    $transientFailPct = PctOf $totalTransientTicks    $totalInProgressTicks
    $stallPct         = PctOf $totalStallTicks        $totalInProgressTicks
    $contentIdxPct    = PctOf $totalCITicks           $totalInProgressTicks
    $haPct            = PctOf $totalHATicks           $totalInProgressTicks
    $targetCPUPct     = PctOf $totalTargetCPUTicks    $totalInProgressTicks
    $sourceCPUPct     = PctOf $totalSourceCPUTicks    $totalInProgressTicks
    $mbxLockedPct     = PctOf $totalMbxLockedTicks    $totalInProgressTicks
    $proxyUnknownPct  = PctOf $totalProxyTicks        $totalInProgressTicks
    $throttlePct      = PctOf $totalThrottleTicks     $totalInProgressTicks

    #── Transfer metrics — single pass over $filtered accumulates all size values ──
    # Microsoft definition of PercentComplete (New-Rec):
    #   TotalTransferredMailboxSizeInGB / TotalMailboxSizeInGB * 100
    #   i.e. size-weighted, not a simple average of per-mailbox pct values.
    #   A 1 GB mailbox 100% + a 100 GB mailbox 0% = 0.99%, not 50%.
    #
    # Microsoft definition of MoveEfficiencyPercent (New-Rec):
    #   TotalTransferredMailboxSizeInGB / TotalGBTransferred * 100
    #   = logical portion moved / actual bytes on the wire.
    #
    # Microsoft definition of TotalThroughputGBPerHour:
    #   totalTransferredMailboxSizeInMB / MoveDuration.TotalHours / 1024
    #   = batch-level throughput over wall-clock migration duration.
    #   Different from AvgPerMoveTransferRate which is per-mailbox averaged.

    $sumXferGB    = [double]0   # actual bytes on wire (GB)
    $sumSrcGB     = [double]0   # full source mailbox size (GB)
    $sumLogicalGB = [double]0   # logical portion moved = srcGB * pctComplete/100

    foreach ($row in $filtered) {
        $sumXferGB    += $row.TransferredGB
        $sumSrcGB     += $row.MailboxSizeGB
        $sumLogicalGB += $row.MailboxSizeGB * ($row.PercentComplete / 100)
    }

    $totalGBXfer = [math]::Round($sumXferGB,    4)
    $totalSrcGB  = [math]::Round($sumSrcGB,     4)

    # Size-weighted percent complete (Microsoft formula)
    $pctComplete = if ($sumSrcGB -gt 0) {
        [math]::Round($sumLogicalGB / $sumSrcGB * 100, 1)
    } else { 0 }

    # Move efficiency (Microsoft formula): logical moved / wire bytes
    $moveEfficiency = if ($totalGBXfer -gt 0) {
        [math]::Round($sumLogicalGB / $totalGBXfer * 100, 2)
    } else { 0 }

    # Batch throughput over wall-clock time (Microsoft TotalThroughputGBPerHour)
    $totalThroughputGBph = if ($duration.TotalHours -gt 0) {
        [math]::Round($sumLogicalGB / $duration.TotalHours, 4)
    } else { 0 }

    #── Per-move transfer rates + latency — single pass each ─────────────────────
    # Measure-Object with all flags in one call avoids re-enumerating the collection.
    # [math]::Round() cannot take an inline if-expression as an argument —
    # resolve to a variable first, then round.
    $ratesMeasure = $filtered |
                    Where-Object   { $_.TransferRateGBph -gt 0 } |
                    Measure-Object -Property TransferRateGBph -Maximum -Minimum -Average

    $maxRaw  = if ($null -ne $ratesMeasure.Maximum) { $ratesMeasure.Maximum } else { 0 }
    $minRaw  = if ($null -ne $ratesMeasure.Minimum) { $ratesMeasure.Minimum } else { 0 }
    $avgRaw  = if ($null -ne $ratesMeasure.Average) { $ratesMeasure.Average } else { 0 }
    $maxRate = [math]::Round($maxRaw, 4)
    $minRate = [math]::Round($minRaw, 4)
    $avgRate = [math]::Round($avgRaw, 4)

    #── Latency — single Measure-Object call per metric ──────────────────────────
    # Where-Object + Measure-Object combined so the pipeline runs once per metric.
    $srcLatMeasure = $filtered |
                     Where-Object   { $null -ne $_.SourceLatencyMs -and $_.SourceLatencyMs -gt 0 } |
                     Measure-Object -Property SourceLatencyMs -Average
    $dstLatMeasure = $filtered |
                     Where-Object   { $null -ne $_.DestLatencyMs -and $_.DestLatencyMs -gt 0 } |
                     Measure-Object -Property DestLatencyMs -Average

    $avgSrcLatency = if ($srcLatMeasure.Count -gt 0) { [math]::Round($srcLatMeasure.Average, 2) } else { $null }
    $avgDstLatency = if ($dstLatMeasure.Count -gt 0) { [math]::Round($dstLatMeasure.Average, 2) } else { $null }

    #── Status breakdown ──────────────────────────────────────────────────────────
    $statusGroups = $Stats | Group-Object Status | Sort-Object Count -Descending

    #── Bottleneck ────────────────────────────────────────────────────────────────
    $bottleneck = Get-BottleneckAnalysis -SourcePct $sourceSidePct -DestPct $destSidePct

    #── Build summary object ──────────────────────────────────────────────────────
    $summary = [PSCustomObject]@{
        BatchName                        = $Name
        GeneratedAt                      = Get-Date
        StartTime                        = $startTime
        EndTime                          = $endTime
        MigrationDuration                = if ($duration.Days -gt 0) {
                                               "$($duration.Days) day(s) $("{0:00}:{1:00}:{2:00}" -f $duration.Hours,$duration.Minutes,$duration.Seconds)"
                                           } else {
                                               "$("{0:00}:{1:00}:{2:00}" -f $duration.Hours,$duration.Minutes,$duration.Seconds)"
                                           }
        MailboxCount                     = @($Stats).Count
        PercentileUsed                   = $Percentile

        MinSizeGBForScoring              = $MinSizeGBForScoring
        TotalSourceSizeGB                = $totalSrcGB
        TotalGBTransferred               = $totalGBXfer
        PercentComplete                  = $pctComplete
        MaxPerMoveTransferRateGBPerHour  = $maxRate
        MinPerMoveTransferRateGBPerHour  = $minRate
        TotalThroughputGBPerHour         = $totalThroughputGBph
        AvgPerMoveTransferRateGBPerHour  = $avgRate
        MoveEfficiencyPercent            = $moveEfficiency
        AverageSourceLatencyMs           = $avgSrcLatency
        AverageDestinationLatencyMs      = $avgDstLatency
        IdleDurationPct                  = $idlePct
        SourceSideDurationPct            = $sourceSidePct
        DestinationSideDurationPct       = $destSidePct
        WordBreakingDurationPct          = $wordBreakPct
        TransientFailureDurationsPct     = $transientFailPct
        OverallStallDurationsPct         = $stallPct
        ContentIndexingStallsPct         = $contentIdxPct
        HighAvailabilityStallsPct        = $haPct
        TargetCPUStallsPct               = $targetCPUPct
        SourceCPUStallsPct               = $sourceCPUPct
        MailboxLockedStallPct            = $mbxLockedPct
        ProxyUnknownStallPct             = $proxyUnknownPct
        ThrottleStallsPct                = $throttlePct
        StatusBreakdown                  = $statusGroups
        Bottleneck                       = $bottleneck
        PerMailboxDetail                 = $perMailbox   # all mailboxes, not just filtered
        SlowestMailboxes                 = $slowest      # excluded by percentile filter, sorted by rate asc
    }

    # Calculate ETA for incomplete migrations
    $etaText = "—"
    $etaDateTime = $null
    $remainingGB = $totalSrcGB - $totalGBXfer
    if ($remainingGB -gt 0 -and $totalThroughputGBph -gt 0 -and $pctComplete -lt 100) {
        $hoursRemaining = $remainingGB / $totalThroughputGBph
        $etaDateTime = (Get-Date).AddHours($hoursRemaining)
        if ($hoursRemaining -lt 1) {
            $etaText = "{0:N0} min" -f ($hoursRemaining * 60)
        } elseif ($hoursRemaining -lt 24) {
            $etaText = "{0:N1} hours" -f $hoursRemaining
        } elseif ($hoursRemaining -lt 168) {
            $etaText = "{0:N1} days" -f ($hoursRemaining / 24)
        } else {
            $etaText = "{0:N0} weeks" -f ($hoursRemaining / 168)
        }
    } elseif ($pctComplete -ge 100) {
        $etaText = "Complete"
    }
    $summary | Add-Member -NotePropertyName EstimatedTimeRemaining -NotePropertyValue $etaText -Force
    $summary | Add-Member -NotePropertyName EstimatedCompletionTime -NotePropertyValue $etaDateTime -Force
    $summary | Add-Member -NotePropertyName RemainingGB -NotePropertyValue ([math]::Round($remainingGB, 2)) -Force

    # Throttling detection
    $isThrottled = $false
    $throttleReason = @()
    if ($throttlePct -gt 5) { $isThrottled = $true; $throttleReason += "Throttle stalls: $throttlePct%" }
    if ($transientFailPct -gt 10) { $isThrottled = $true; $throttleReason += "Transient failures: $transientFailPct%" }
    if ($avgRate -lt 0.3 -and $avgRate -gt 0) { $isThrottled = $true; $throttleReason += "Low transfer rate: $avgRate GB/h" }
    $summary | Add-Member -NotePropertyName IsThrottled -NotePropertyValue $isThrottled -Force
    $summary | Add-Member -NotePropertyName ThrottleReasons -NotePropertyValue ($throttleReason -join "; ") -Force

    return $summary
}

#endregion

#region ── Health Scoring ───────────────────────────────────────────────────────

function Get-OverallHealthScore {
    param($Summary)

    # All 8 metrics — 4 require -IncludeDetailReport (Report object), 4 are always available
    $allChecks = @(
        # Always available (Pass 1 — no -IncludeReport needed)
        @{ Metric="AvgPerMoveTransferRateGBPerHour"; Value=$Summary.AvgPerMoveTransferRateGBPerHour; Weight=20; RequiresDetail=$false }
        @{ Metric="MoveEfficiencyPercent";            Value=$Summary.MoveEfficiencyPercent;           Weight=20; RequiresDetail=$false }
        @{ Metric="TransientFailureDurations";        Value=$Summary.TransientFailureDurationsPct;    Weight=10; RequiresDetail=$false }
        @{ Metric="OverallStallDurations";            Value=$Summary.OverallStallDurationsPct;        Weight=10; RequiresDetail=$false }
        # Requires -IncludeDetailReport (Report.SessionStatistics)
        @{ Metric="SourceSideDuration";               Value=$Summary.SourceSideDurationPct;           Weight=15; RequiresDetail=$true  }
        @{ Metric="DestinationSideDuration";          Value=$Summary.DestinationSideDurationPct;      Weight=15; RequiresDetail=$true  }
        @{ Metric="WordBreakingDuration";             Value=$Summary.WordBreakingDurationPct;         Weight=5;  RequiresDetail=$true  }
        @{ Metric="AverageSourceLatency";             Value=$Summary.AverageSourceLatencyMs;          Weight=5;  RequiresDetail=$true  }
    )

    # Mailboxes below size floor — Rate and Efficiency are noise, not signal
    $isTinyMailbox = ($Summary.MinSizeGBForScoring -gt 0 -and
                      $Summary.TotalSourceSizeGB -gt 0 -and
                      ($Summary.TotalSourceSizeGB / [math]::Max(1,$Summary.MailboxCount)) -lt $Summary.MinSizeGBForScoring)

    # Filter to available metrics
    $activeChecks = $allChecks | Where-Object {
        (-not $_.RequiresDetail -or $Summary.HasDetailReport) -and
        -not ($isTinyMailbox -and $_.Metric -in @('AvgPerMoveTransferRateGBPerHour','MoveEfficiencyPercent'))
    }

    # Rescale weights so they always sum to 100
    $rawWeightTotal = ($activeChecks | Measure-Object -Property Weight -Sum).Sum
    $weightedScore  = 0

    $checkResults = foreach ($c in $activeChecks) {
        $scaledWeight = [math]::Round(($c.Weight / $rawWeightTotal) * 100, 1)
        $status = Get-HealthStatus -Metric $c.Metric -Value $c.Value
        $points = switch ($status) { "Healthy" { 1.0 } "Warning" { 0.5 } "Critical" { 0.0 } default { 0.75 } }
        $weightedScore += ($points * $scaledWeight)
        [PSCustomObject]@{
            Metric         = $c.Metric
            Value          = $c.Value
            Status         = $status
            Weight         = $scaledWeight
            RequiresDetail = $c.RequiresDetail
        }
    }

    # N/A placeholders for excluded metrics (shown as greyed out in HTML)
    $naResults = if (-not $Summary.HasDetailReport -or $isTinyMailbox) {
        $allChecks | Where-Object {
            ($_.RequiresDetail -and -not $Summary.HasDetailReport) -or
            ($isTinyMailbox -and $_.Metric -in @('AvgPerMoveTransferRateGBPerHour','MoveEfficiencyPercent'))
        } | ForEach-Object {
            [PSCustomObject]@{
                Metric         = $_.Metric
                Value          = $null
                Status         = "N/A"
                Weight         = $_.Weight
                RequiresDetail = $true
            }
        }
    } else { @() }

    $score = [math]::Round($weightedScore, 1)
    $grade = if ($score -ge 85) { "A – Excellent" }
             elseif ($score -ge 70) { "B – Good" }
             elseif ($score -ge 55) { "C – Fair" }
             elseif ($score -ge 40) { "D – Poor" }
             else                  { "F – Critical" }

    $metricCount   = @($activeChecks).Count
    $sizeNote    = if ($isTinyMailbox) { " Rate & Efficiency excluded (mailbox < $($Summary.MinSizeGBForScoring*1024) MB)." } else { "" }
    $partialNote = if (-not $Summary.HasDetailReport -and $isTinyMailbox) {
        "Score based on $metricCount of 8 metrics. Run with -IncludeDetailReport for full analysis.$sizeNote"
    } elseif (-not $Summary.HasDetailReport) {
        "Score based on $metricCount of 8 metrics. Run with -IncludeDetailReport for full analysis."
    } elseif ($isTinyMailbox) {
        "Rate & Efficiency N/A — mailbox below $($Summary.MinSizeGBForScoring*1024) MB scoring threshold."
    } else { "" }

    return [PSCustomObject]@{
        Score       = $score
        Grade       = $grade
        Checks      = $checkResults   # active metrics (scored)
        NaChecks    = $naResults      # excluded metrics (N/A)
        IsPartial   = (-not $Summary.HasDetailReport)
        PartialNote = $partialNote
        MetricCount = $metricCount
    }
}

#endregion

#region ── Report Writers ───────────────────────────────────────────────────────

function Export-CsvReport {
    param($Summary, [string]$Path)

    # Summary sheet
    $summaryData = $Summary | Select-Object BatchName, GeneratedAt, StartTime, EndTime,
        MigrationDuration, MailboxCount, TotalSourceSizeGB, TotalGBTransferred,
        PercentComplete, TotalThroughputGBPerHour,
        MaxPerMoveTransferRateGBPerHour, MinPerMoveTransferRateGBPerHour,
        AvgPerMoveTransferRateGBPerHour, MoveEfficiencyPercent,
        AverageSourceLatencyMs, AverageDestinationLatencyMs,
        IdleDurationPct, SourceSideDurationPct, DestinationSideDurationPct,
        WordBreakingDurationPct, TransientFailureDurationsPct, OverallStallDurationsPct,
        ContentIndexingStallsPct, HighAvailabilityStallsPct, TargetCPUStallsPct,
        SourceCPUStallsPct, MailboxLockedStallPct, ProxyUnknownStallPct

    $csvSummary  = Join-Path $Path "$($Summary.BatchName)_Summary.csv"
    $csvMailbox  = Join-Path $Path "$($Summary.BatchName)_PerMailbox.csv"

    $summaryData | Export-Csv -Path $csvSummary -NoTypeInformation -Force
    $Summary.PerMailboxDetail | Export-Csv -Path $csvMailbox -NoTypeInformation -Force

    Write-Log "CSV reports saved: $csvSummary, $csvMailbox" -Level SUCCESS
    return @($csvSummary, $csvMailbox)
}

function Export-HtmlReport {
    param(
        $Summary,
        $Health,
        [string]$Path,
        [int]$AutoRefreshSeconds = 0,
        [int]$ListenerPort = 0,
        [string]$ListenerBaseUrl = ""
    )

    $apiBaseUrl = if ($ListenerBaseUrl) { $ListenerBaseUrl } elseif ($ListenerPort -gt 0) { "http://127.0.0.1:$ListenerPort" } else { "" }


    $scoreColor = switch -Wildcard ($Health.Grade) {
        "A*" { "#22c55e" } "B*" { "#84cc16" } "C*" { "#f59e0b" }
        "D*" { "#f97316" } default { "#ef4444" }
    }

    $bottleneckColor = switch ($Summary.Bottleneck.Severity) {
        "None"     { "#22c55e" } "Warning" { "#f59e0b" }
        "Critical" { "#ef4444" } default   { "#94a3b8" }
    }

    function Get-StatusBadge { param([string]$Status)
        $bg = switch ($Status) {
            "Healthy"  { "#dcfce7"; $fc="#166534" }
            "Warning"  { "#fef9c3"; $fc="#854d0e" }
            "Critical" { "#fee2e2"; $fc="#991b1b" }
            default    { "#f1f5f9"; $fc="#475569"  }
        }
        "<span style='background:$bg;color:$fc;padding:2px 10px;border-radius:999px;font-size:0.78rem;font-weight:600'>$Status</span>"
    }

    # Health check table — metric, value, healthy range (from MS reference), status, weight
    $metricDefs = @{
        AvgPerMoveTransferRateGBPerHour = "Target >0.5 GB/h. Normal range 0.3–1 GB/h."
        MoveEfficiencyPercent           = "SourceSize / BytesTransferred. Healthy 75–100%. Lower = excess retransmission."
        SourceSideDuration              = "Time on source MRSProxy. Healthy 60–80%. >80% = source bottleneck."
        DestinationSideDuration         = "Time on dest MRSProxy. Healthy 20–40%. >40% = destination bottleneck."
        WordBreakingDuration            = "Time tokenising content for indexing. Healthy 0–15%. >15% = dest indexing busy."
        TransientFailureDurations       = "Time in intermittent failures. Healthy 0–5%. Check connectivity and load balancers."
        OverallStallDurations           = "Time waiting for system resources. Healthy 0–15%."
        AverageSourceLatency            = "No-op WCF call duration to source MRSProxy. Target ≤100ms."
    }
    $healthRows = ($Health.Checks | ForEach-Object {
        $def = if ($metricDefs.ContainsKey($_.Metric)) { $metricDefs[$_.Metric] } else { "" }
        "<tr>
          <td><strong>$($_.Metric)</strong><br><span style='font-size:.75rem;color:#64748b'>$def</span></td>
          <td style='font-family:monospace'>$($_.Value)</td>
          <td>$(Get-StatusBadge $_.Status)</td>
          <td style='text-align:center'>$($_.Weight)%</td>
        </tr>"
    }) -join "`n"

    # Per-mailbox table rows
    # Status badge colours for HTML table
    $statusBadgeMap = @{
        "InProgress"               = @{ bg="#dbeafe"; fc="#1e40af" }
        "Synced"                   = @{ bg="#dcfce7"; fc="#166534" }
        "Completed"                = @{ bg="#dcfce7"; fc="#166534" }
        "CompletedWithWarning"     = @{ bg="#fef9c3"; fc="#854d0e" }
        "CompletedWithSkippedItems"= @{ bg="#fef9c3"; fc="#854d0e" }
        "AutoSuspended"            = @{ bg="#fef9c3"; fc="#854d0e" }
        "Suspended"                = @{ bg="#fef9c3"; fc="#854d0e" }
        "Failed"                   = @{ bg="#fee2e2"; fc="#991b1b" }
        "Queued"                   = @{ bg="#f1f5f9"; fc="#475569" }
    }

    $mbxRows = ($Summary.PerMailboxDetail | ForEach-Object {
        $effColor  = if ($_.EfficiencyPct -lt 60) { "color:#ef4444" }
                     elseif ($_.EfficiencyPct -lt 75) { "color:#f59e0b" }
                     else { "color:#22c55e" }
        $rateColor = if ($_.TransferRateGBph -lt 0.3) { "color:#ef4444" }
                     elseif ($_.TransferRateGBph -lt 0.5) { "color:#f59e0b" }
                     else { "color:#22c55e" }

        # Status badge
        $sc  = if ($statusBadgeMap.ContainsKey($_.Status)) { $statusBadgeMap[$_.Status] } else { @{bg="#f1f5f9";fc="#475569"} }
        $statusBadge = "<span style='background:$($sc.bg);color:$($sc.fc);padding:2px 9px;border-radius:999px;font-size:.76rem;font-weight:600;white-space:nowrap'>$($_.Status)</span>"

        # InProgress duration — format as h:mm:ss, trim leading zeros
        $durStr = try {
            $ts = $_.InProgressDuration
            if ($ts) {
                $h  = [math]::Floor([double]$ts.TotalHours)
                $ms = $ts.Minutes
                $ss = $ts.Seconds
                "$($h):$($ms.ToString('00')):$($ss.ToString('00'))"
            } else { "—" }
        } catch { "—" }
        $overallDurStr = try {

            $ts = $_.OverallDuration

            if ($ts) {

                $h  = [math]::Floor([double]$ts.TotalHours)

                $ms = $ts.Minutes

                $ss = $ts.Seconds

                "$($h):$($ms.ToString('00')):$($ss.ToString('00'))"

            } else { "—" }

        } catch { "—" }


        $queuedDurStr = try {

            $ts = $_.QueuedDuration

            if ($ts -and $ts.TotalSeconds -gt 0) {

                $h  = [math]::Floor([double]$ts.TotalHours)

                $ms = $ts.Minutes

                $ss = $ts.Seconds

                "$($h):$($ms.ToString('00')):$($ss.ToString('00'))"

            } else { "0:00:00" }

        } catch { "0:00:00" }

        $gapDurStr = try {

            $overallSec = [double]$_.OverallDuration.TotalSeconds

            $activeSec  = [double]$_.InProgressDuration.TotalSeconds

            $queuedSec  = if ($_.QueuedDuration) { [double]$_.QueuedDuration.TotalSeconds } else { 0 }

            $gapSec     = [math]::Max(0, $overallSec - $activeSec - $queuedSec)

            $h  = [math]::Floor($gapSec / 3600)

            $ms = [math]::Floor(($gapSec % 3600) / 60)

            $ss = [math]::Floor($gapSec % 60)

            "$($h):$($ms.ToString('00')):$($ss.ToString('00'))"

        } catch { "0:00:00" }

        # Items transferred with thousands separator
        $itemStr = if ($_.ItemsTransferred) { "{0:N0}" -f [int]$_.ItemsTransferred } else { "—" }
        $totalBadStr = [int]($_.BadItems) + [int]($_.ItemsSkipped) + [int]($_.LargeItems) + [int]($_.MissingItems)



        $_tsQueued = if ($_.QueuedTime) { $_.QueuedTime.ToString("yyyy-MM-dd HH:mm") } else { "" }

        $_tsStart = if ($_.StartTime) { $_.StartTime.ToString("yyyy-MM-dd HH:mm") } else { "" }

        $_tsComplete = if ($_.CompletionTime) { $_.CompletionTime.ToString("yyyy-MM-dd HH:mm") } else { "" }

        $_tsLastSync = if ($_.LastSuccessfulSync) { $_.LastSuccessfulSync.ToString("yyyy-MM-dd HH:mm") } else { "" }

        $_tsSeeding = if ($_.InitialSeedingDone) { $_.InitialSeedingDone.ToString("yyyy-MM-dd HH:mm") } else { "" }

        $_srcLat = if ($_.SourceLatencyMs) { "$($_.SourceLatencyMs)" } else { "" }

        $_dstLat = if ($_.DestLatencyMs)   { "$($_.DestLatencyMs)" }   else { "" }

        $rowData = "data-status='$($_.Status)'" +
            " data-dn='$($_.DisplayName -replace "'","&#39;")'" +
            " data-alias='$($_.Alias)'" +
            " data-email='$($_.EmailAddress)'" +
            " data-pct='$($_.PercentComplete)'" +
            " data-primary='$($_.PrimaryMailboxSizeGB)'" +
            " data-archive='$($_.ArchiveMailboxSizeGB)'" +
            " data-xfer='$($_.TransferredGB)'" +
            " data-rate='$($_.TransferRateGBph)'" +
            " data-eff='$($_.EfficiencyPct)'" +
            " data-overall='$overallDurStr'" +
            " data-active='$durStr'" +

            " data-queueddur='$queuedDurStr'" +

            " data-gapdur='$gapDurStr'" +
            " data-items='$($_.ItemsTransferred)'" +
            " data-baditems='$($_.BadItems)'" +
            " data-skipped='$($_.ItemsSkipped)'" +
                " data-large='$($_.LargeItems)'" +

                " data-missing='$($_.MissingItems)'" +

            " data-consistency='$($_.DataConsistencyScore)'" +
            " data-factors='$($_.DataConsistencyFactors)'" +
            " data-syncstage='$($_.SyncStage)'" +
            " data-queued='$_tsQueued'" +
            " data-start='$_tsStart'" +
            " data-complete='$_tsComplete'" +
            " data-lastsync='$_tsLastSync'" +
            " data-seeding='$_tsSeeding'" +
            " data-srclatency='$_srcLat'" +
            " data-dstlatency='$_dstLat'" +
            " data-lastfail='$($_.LastFailure -replace "'","&#39;" -replace '"','&quot;')'" +
            " data-batch='$($_.BatchName)'" +
            " data-srcver='$(if($_.SourceVersion){"$($_.SourceVersion.Major).$($_.SourceVersion.Minor) (Build $($_.SourceVersion.Build))"})'" +
            " data-tgtver='$(if($_.TargetVersion){"$($_.TargetVersion.Major).$($_.TargetVersion.Minor) (Build $($_.TargetVersion.Build))"})'" +
            " data-mrssrv='$($_.MRSServerName)'" +
            " data-remote='$($_.RemoteHostName)'" +

            " data-tickprogress='$($_.TickInProgress)'" +

            " data-ticktransient='$($_.TickTransient)'" +

            " data-tickci='$($_.TickCI)'" +

            " data-tickha='$($_.TickHA)'" +

            " data-ticktargetcpu='$($_.TickTargetCPU)'" +

            " data-ticksourcecpu='$($_.TickSourceCPU)'" +

            " data-tickmbxlocked='$($_.TickMbxLocked)'" +

            " data-tickreadthrottle='$($_.TickReadThrottle)'" +

            " data-tickwritethrottle='$($_.TickWriteThrottle)'" +

            " data-tickproxysrc='$($_.TickProxySrc)'" +

            " data-tickproxyDst='$($_.TickProxyDst)'" +

            " data-tickwordbreak='$($_.TickWordBreak)'"
        "<tr $rowData style='cursor:pointer'>
            <td><strong>$($_.DisplayName)</strong></td>
            <td style='font-size:.8rem;color:#64748b'>$($_.Alias)</td>
            <td>$statusBadge</td>
            <td>$($_.PercentComplete)%</td>
            <td>$($_.PrimaryMailboxSizeGB)</td>
            <td>$(if($_.ArchiveMailboxSizeGB -gt 0){"$($_.ArchiveMailboxSizeGB)"}else{"—"})</td>
            <td>$($_.TransferredGB)</td>
            <td style='$rateColor;font-weight:600'>$($_.TransferRateGBph)</td>
            <td style='$effColor;font-weight:600'>$($_.EfficiencyPct)%</td>
            <td style='font-family:monospace'>$overallDurStr</td>

            <td style='font-family:monospace'>$durStr</td>
            <td>$itemStr</td>
            <td>$(if($totalBadStr -gt 0){"<span style='color:#ef4444;font-weight:600'>$totalBadStr</span>"}else{'0'})</td>



            <td>$(
                $score = $_.DataConsistencyScore
                $sc = switch($score) {
                    "Good"  { "background:#dcfce7;color:#166534" }
                    "Poor"  { "background:#fee2e2;color:#991b1b" }
                    default { "background:#fef9c3;color:#854d0e" }
                }
                if ($score -and $score -ne "{}") { "<span style='$sc;padding:2px 8px;border-radius:999px;font-size:.74rem;font-weight:600'>$score</span>" } else { "—" }
            )</td>
            <td style='font-size:.78rem;color:#64748b'>$(if($_.SyncStage){$_.SyncStage}else{"—"})</td>
        </tr>"
    }) -join "`n"

    # Slowest mailboxes — excluded by percentile filter, sorted rate ascending
    $slowestRows = ""
    if ($Summary.SlowestMailboxes -and @($Summary.SlowestMailboxes).Count -gt 0) {
        $slowestRows = ($Summary.SlowestMailboxes | ForEach-Object {
            $effColor  = if ($_.EfficiencyPct -lt 60) { "color:#ef4444" }
                         elseif ($_.EfficiencyPct -lt 75) { "color:#f59e0b" }
                         else { "color:#22c55e" }
            $rateColor = if ($_.TransferRateGBph -lt 0.3) { "color:#ef4444" }
                         elseif ($_.TransferRateGBph -lt 0.5) { "color:#f59e0b" }
                         else { "color:#22c55e" }
            $sc  = if ($statusBadgeMap.ContainsKey($_.Status)) { $statusBadgeMap[$_.Status] } else { @{bg="#f1f5f9";fc="#475569"} }
            $statusBadge = "<span style='background:$($sc.bg);color:$($sc.fc);padding:2px 9px;border-radius:999px;font-size:.76rem;font-weight:600;white-space:nowrap'>$($_.Status)</span>"
            $durStr = try {
                $ts = $_.InProgressDuration
                if ($ts) {
                    $h  = [math]::Floor([double]$ts.TotalHours)
                    $ms = $ts.Minutes
                    $ss = $ts.Seconds
                    "$($h):$($ms.ToString('00')):$($ss.ToString('00'))"
                } else { "—" }
            } catch { "—" }
            $itemStr = if ($_.ItemsTransferred) { "{0:N0}" -f [int]$_.ItemsTransferred } else { "—" }
            $totalBadStr = [int]($_.BadItems) + [int]($_.ItemsSkipped) + [int]($_.LargeItems) + [int]($_.MissingItems)


            $_tsQueued = if ($_.QueuedTime) { $_.QueuedTime.ToString("yyyy-MM-dd HH:mm") } else { "" }

            $_tsStart = if ($_.StartTime) { $_.StartTime.ToString("yyyy-MM-dd HH:mm") } else { "" }

            $_tsComplete = if ($_.CompletionTime) { $_.CompletionTime.ToString("yyyy-MM-dd HH:mm") } else { "" }

            $_tsLastSync = if ($_.LastSuccessfulSync) { $_.LastSuccessfulSync.ToString("yyyy-MM-dd HH:mm") } else { "" }

            $_tsSeeding = if ($_.InitialSeedingDone) { $_.InitialSeedingDone.ToString("yyyy-MM-dd HH:mm") } else { "" }

            $_srcLat = if ($_.SourceLatencyMs) { "$($_.SourceLatencyMs)" } else { "" }

            $_dstLat = if ($_.DestLatencyMs)   { "$($_.DestLatencyMs)" }   else { "" }

            $rowData = "data-status='$($_.Status)' data-slowest='true'" +
                " data-dn='$($_.DisplayName -replace "'","&#39;")'" +
                " data-alias='$($_.Alias)'" +
                " data-email='$($_.EmailAddress)'" +
                " data-pct='$($_.PercentComplete)'" +
                " data-primary='$($_.PrimaryMailboxSizeGB)'" +
                " data-archive='$($_.ArchiveMailboxSizeGB)'" +
                " data-xfer='$($_.TransferredGB)'" +
                " data-rate='$($_.TransferRateGBph)'" +
                " data-eff='$($_.EfficiencyPct)'" +
                " data-overall='$overallDurStr'" +
                " data-active='$durStr'" +

                " data-queueddur='$queuedDurStr'" +

                " data-gapdur='$gapDurStr'" +


                " data-items='$($_.ItemsTransferred)'" +
                " data-baditems='$($_.BadItems)'" +
                " data-skipped='$($_.ItemsSkipped)'" +
                " data-large='$($_.LargeItems)'" +

                " data-missing='$($_.MissingItems)'" +

                " data-consistency='$($_.DataConsistencyScore)'" +
                " data-factors='$($_.DataConsistencyFactors)'" +
                " data-syncstage='$($_.SyncStage)'" +
                " data-queued='$_tsQueued'" +
                " data-start='$_tsStart'" +
                " data-complete='$_tsComplete'" +
                " data-lastsync='$_tsLastSync'" +
                " data-seeding='$_tsSeeding'" +
                " data-srclatency='$_srcLat'" +
                " data-dstlatency='$_dstLat'" +
                " data-lastfail='$($_.LastFailure -replace "'","&#39;" -replace '"','&quot;')'" +
                " data-batch='$($_.BatchName)'" +
                " data-srcver='$(if($_.SourceVersion){"$($_.SourceVersion.Major).$($_.SourceVersion.Minor) (Build $($_.SourceVersion.Build))"})'" +
                " data-tgtver='$(if($_.TargetVersion){"$($_.TargetVersion.Major).$($_.TargetVersion.Minor) (Build $($_.TargetVersion.Build))"})'" +
                " data-mrssrv='$($_.MRSServerName)'" +
                " data-remote='$($_.RemoteHostName)'" +

                " data-tickprogress='$($_.TickInProgress)'" +

                " data-ticktransient='$($_.TickTransient)'" +

                " data-tickci='$($_.TickCI)'" +

                " data-tickha='$($_.TickHA)'" +

                " data-ticktargetcpu='$($_.TickTargetCPU)'" +

                " data-ticksourcecpu='$($_.TickSourceCPU)'" +

                " data-tickmbxlocked='$($_.TickMbxLocked)'" +

                " data-tickreadthrottle='$($_.TickReadThrottle)'" +

                " data-tickwritethrottle='$($_.TickWriteThrottle)'" +

                " data-tickproxysrc='$($_.TickProxySrc)'" +

                " data-tickproxyDst='$($_.TickProxyDst)'" +

                " data-tickwordbreak='$($_.TickWordBreak)'" +












            "<tr $rowData style='cursor:pointer'>
            <td style='text-align:center'><button class='pin-btn' onclick='event.stopPropagation();togglePin(this)' title='Pin to top'>📌</button></td>
            <td><strong>$($_.DisplayName)</strong></td>
            <td style='font-size:.8rem;color:#64748b'>$($_.Alias)</td>
            <td>$statusBadge</td>
            <td>$($_.PercentComplete)%</td>
            <td>$($_.PrimaryMailboxSizeGB)</td>
            <td>$(if($_.ArchiveMailboxSizeGB -gt 0){`"$($_.ArchiveMailboxSizeGB)`"}else{`"—`"})</td>
            <td>$($_.TransferredGB)</td>
            <td style='$rateColor;font-weight:600'>$($_.TransferRateGBph)</td>
            <td style='$effColor;font-weight:600'>$($_.EfficiencyPct)%</td>
            <td style='font-family:monospace'>$overallDurStr</td>

            <td style='font-family:monospace'>$durStr</td>
            <td>$itemStr</td>
            <td>$(if($totalBadStr -gt 0){"<span style='color:#ef4444;font-weight:600'>$totalBadStr</span>"}else{'0'})</td>



            <td>$(
                $score = $_.DataConsistencyScore
                $sc2 = switch($score) {
                    `"Good`"  { `"background:#dcfce7;color:#166534`" }
                    `"Poor`"  { `"background:#fee2e2;color:#991b1b`" }
                    default { `"background:#fef9c3;color:#854d0e`" }
                }
                if ($score -and $score -ne `"{}") { `"<span style='$sc2;padding:2px 8px;border-radius:999px;font-size:.74rem;font-weight:600'>$score</span>`" } else { `"—`" }
            )</td>
            <td style='font-size:.78rem;color:#64748b'>$(if($_.SyncStage){$_.SyncStage}else{`"—`"})</td>
        </tr>"
        }) -join "`n"
    }

    # Recommendations list
    $recommendations = ($Summary.Bottleneck.Recommendations | ForEach-Object {
        "<li>$_</li>"
    }) -join "`n"

    # Status breakdown pills
    $statusPills = ($Summary.StatusBreakdown | ForEach-Object {
        "<span style='background:#e0f2fe;color:#0369a1;padding:4px 14px;border-radius:999px;font-size:0.85rem;margin:3px;display:inline-block'><strong>$($_.Name)</strong>: $($_.Count)</span>"
    }) -join " "

    $html = @"
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
$(if($AutoRefreshSeconds -gt 0){"<meta http-equiv='refresh' content='$AutoRefreshSeconds'>"})
<title>Migration Report – $($Summary.BatchName)</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: #f8fafc; color: #1e293b; transition: background .3s, color .3s; }
  .container { max-width: 1300px; margin: 0 auto; padding: 24px; }

  /* Dark Mode */
  body.dark-mode { background: #0f172a; color: #e2e8f0; }
  body.dark-mode .header { background: linear-gradient(135deg, #1e293b 0%, #334155 100%); }
  body.dark-mode .score-card, body.dark-mode .kpi, body.dark-mode .card { background: #1e293b; box-shadow: 0 1px 4px rgba(0,0,0,.3); }
  body.dark-mode .score-grade, body.dark-mode .kpi .value, body.dark-mode .card h2 { color: #f1f5f9; }
  body.dark-mode .score-desc, body.dark-mode .kpi .sub, body.dark-mode .kpi .label { color: #94a3b8; }
  body.dark-mode thead th { background: #334155; color: #cbd5e1; }
  body.dark-mode tbody td { border-color: #334155; color: #cbd5e1; }
  body.dark-mode tbody tr:hover { background: #334155; }
  body.dark-mode .ent-panel { background: #1e293b; border-color: #334155; }
  body.dark-mode .ent-btn { background: #334155; border-color: #475569; color: #e2e8f0; }
  body.dark-mode .ent-btn:hover { background: #475569; }
  body.dark-mode .tb-search { background: #1e293b; border-color: #475569; color: #e2e8f0; }
  body.dark-mode .bottleneck-banner { background: #1e293b; }
  body.dark-mode .mbx-modal { background: #1e293b; }
  body.dark-mode .mbx-modal-header { background: #1e293b; border-color: #334155; }
  body.dark-mode .mbx-modal-title { color: #f1f5f9; }
  body.dark-mode .mbx-section { background: #334155 !important; }
  body.dark-mode .mbx-label { color: #94a3b8; }
  body.dark-mode .mbx-value { color: #e2e8f0; }
  body.dark-mode .hc-card { background: #334155; }
  body.dark-mode .hc-metric { color: #e2e8f0; }
  body.dark-mode .dur-label { color: #cbd5e1; }

  /* Dark mode toggle button */
  .dark-toggle { position: fixed; top: 20px; right: 20px; z-index: 1000; background: #1e293b; color: #f8fafc;
                 border: none; border-radius: 50%; width: 44px; height: 44px; cursor: pointer;
                 font-size: 1.2rem; box-shadow: 0 2px 8px rgba(0,0,0,.2); transition: all .2s; }
  .dark-toggle:hover { transform: scale(1.1); }
  body.dark-mode .dark-toggle { background: #f8fafc; color: #1e293b; }

  /* Header */
  .header { background: linear-gradient(135deg, #1e3a5f 0%, #0f6cbd 100%);
            color: white; padding: 32px 36px; border-radius: 12px; margin-bottom: 24px; }
  .header h1 { font-size: 1.8rem; font-weight: 700; }
  .header .meta { font-size: 0.85rem; opacity: 0.8; margin-top: 6px; }

  /* Score card */
  .score-card { background: white; border-radius: 12px; box-shadow: 0 1px 4px rgba(0,0,0,.08);
                padding: 24px 32px; margin-bottom: 20px; display:flex; align-items:center; gap:24px; }
  .score-circle { width:90px; height:90px; border-radius:50%; display:flex; flex-direction:column;
                  align-items:center; justify-content:center; font-weight:700;
                  background:$scoreColor; color:white; flex-shrink:0; }
  .score-circle .num { font-size:1.8rem; line-height:1; }
  .score-circle .lbl { font-size:0.65rem; text-transform:uppercase; letter-spacing:.05em; }
  .score-grade { font-size:1.1rem; font-weight:600; color:#1e293b; }
  .score-desc  { font-size:0.9rem; color:#64748b; margin-top:4px; }

  /* KPI grid */
  .kpi-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:16px; margin-bottom:20px; }
  .kpi { background:white; border-radius:12px; padding:18px 20px;
         box-shadow:0 1px 4px rgba(0,0,0,.06); border-top:4px solid #e2e8f0; }
  .kpi.blue  { border-color:#3b82f6; }
  .kpi.green { border-color:#22c55e; }
  .kpi.amber { border-color:#f59e0b; }
  .kpi.red   { border-color:#ef4444; }
  .kpi .label  { font-size:0.75rem; text-transform:uppercase; letter-spacing:.08em; color:#64748b; font-weight:600; }
  .kpi .value  { font-size:1.6rem; font-weight:700; color:#0f172a; margin-top:4px; }
  .kpi .sub    { font-size:0.78rem; color:#94a3b8; margin-top:2px; }

  /* Cards */
  .card { background:white; border-radius:12px; box-shadow:0 1px 4px rgba(0,0,0,.06); padding:24px; margin-bottom:20px; }
  .card h2 { font-size:1rem; font-weight:700; color:#0f172a; border-bottom:1px solid #e2e8f0;
             padding-bottom:10px; margin-bottom:16px; }



  /* Bottleneck */
  .bottleneck-banner { border-left:5px solid $bottleneckColor; padding:14px 18px; background:#f8fafc;
                       border-radius:0 8px 8px 0; margin-bottom:16px; }
  .bottleneck-title  { font-weight:700; color:#0f172a; font-size:0.95rem; }
  .bottleneck-body   { font-size:0.85rem; color:#475569; margin-top:6px; }
  .rec-list { list-style:disc; padding-left:1.4rem; }
  .rec-list li { font-size:0.85rem; color:#475569; margin-bottom:6px; }

  /* Table */
  .tbl-wrap { overflow-x:auto; }
  table { width:100%; border-collapse:collapse; font-size:0.82rem; }
  thead th { background:#f1f5f9; color:#475569; font-weight:600; text-transform:uppercase;
             font-size:0.72rem; letter-spacing:.06em; padding:10px 12px; text-align:left; }
  thead th { position:relative; white-space:nowrap; }

  thead th .th-tip {

    display:none; position:absolute; top:100%; left:50%; transform:translateX(-50%);

    background:#1e293b; color:#f8fafc; font-size:.75rem; font-weight:400;

    text-transform:none; letter-spacing:0; line-height:1.5;

    padding:8px 12px; border-radius:8px; white-space:normal; width:230px;

    z-index:99; box-shadow:0 4px 12px rgba(0,0,0,.25); pointer-events:none;

    margin-top:4px; text-align:left;

  }

  thead th:hover .th-tip { display:block; }

  thead th .th-tip::before {

    content:''; position:absolute; bottom:100%; left:50%; transform:translateX(-50%);

    border:5px solid transparent; border-bottom-color:#1e293b;

  }

  tbody td { padding:10px 12px; border-bottom:1px solid #f1f5f9; color:#334155; vertical-align:top; }
  tbody tr:hover { background:#f8fafc; }

  /* Section notes */
  .section-note { font-size:.82rem; color:#64748b; margin-bottom:16px; line-height:1.6; }

  /* Duration rows */
  .dur-group-label { font-size:.68rem; font-weight:700; text-transform:uppercase; letter-spacing:.1em;
                     color:#94a3b8; margin-bottom:10px; margin-top:4px; }
  .dur-row  { display:grid; grid-template-columns:10px 200px 1fr 52px; align-items:center;
              gap:10px; margin-bottom:12px; }
  .dur-dot  { width:10px; height:10px; border-radius:50%; flex-shrink:0; }
  .dur-label { display:flex; flex-direction:column; gap:2px; }
  .dur-name  { font-size:.84rem; font-weight:600; color:#1e293b; }
  .dur-def   { font-size:.72rem; color:#94a3b8; line-height:1.4; }
  .dur-bar-wrap { display:flex; flex-direction:column; gap:3px; }
  .dur-track { position:relative; height:10px; background:#e2e8f0; border-radius:6px; overflow:hidden; }
  .dur-fill  { height:100%; border-radius:6px; transition:width .4s; }
  .dur-range { font-size:.68rem; color:#94a3b8; }
  .dur-val   { font-size:.88rem; font-weight:700; color:#334155; text-align:right; }

  /* Health check cards */
  .hc-group-label { font-size:.68rem; font-weight:700; text-transform:uppercase; letter-spacing:.1em;
                    color:#94a3b8; margin-bottom:10px; }
  .hc-grid  { display:grid; grid-template-columns:repeat(4,1fr); gap:14px; }
  .hc-card  { background:#f8fafc; border-radius:10px; padding:16px 18px;
              border:1px solid #e2e8f0; display:flex; flex-direction:column; gap:6px; }
  .hc-top   { display:flex; justify-content:space-between; align-items:flex-start; gap:6px; }
  .hc-name  { font-size:.78rem; font-weight:700; color:#475569; text-transform:uppercase;
              letter-spacing:.04em; line-height:1.3; }
  .hc-badge { font-size:.7rem; font-weight:700; padding:2px 8px; border-radius:999px;
              white-space:nowrap; flex-shrink:0; }
  .hc-value { font-size:1.5rem; font-weight:800; color:#0f172a; line-height:1.1; }
  .hc-range { font-size:.72rem; color:#64748b; font-weight:600; }
  .hc-def   { font-size:.75rem; color:#64748b; line-height:1.5; flex:1; }
  .hc-score-row { display:flex; justify-content:space-between; align-items:center;
                  border-top:1px solid #e2e8f0; padding-top:8px; margin-top:2px; }
  .hc-weight { font-size:.72rem; color:#94a3b8; }
  .hc-pts    { font-size:.78rem; }

  /* ── Main page tabs ── */
  .main-tab-bar {
    display:flex; gap:4px; margin-bottom:24px;
    border-bottom:2px solid #e2e8f0; padding-bottom:0;
  }
  .main-tab {
    padding:10px 22px; font-size:.9rem; font-weight:600; color:#64748b;
    border:none; background:none; cursor:pointer; border-radius:8px 8px 0 0;
    border-bottom:3px solid transparent; margin-bottom:-2px;
    transition:all .15s; white-space:nowrap;
  }
  .main-tab:hover { color:#1e40af; background:#f1f5f9; }
  .main-tab.active { color:#1e40af; border-bottom-color:#1e40af; background:#fff; }
  .main-panel { display:none; }
  .main-panel.active { display:block; }

  /* Mailbox tabs */
  .tab-bar { display:flex; flex-wrap:wrap; gap:6px; margin-bottom:14px; }
  .mbx-tab {
    display:inline-flex; align-items:center; gap:5px;
    padding:6px 14px; border-radius:8px; border:1px solid #e2e8f0;
    background:#f8fafc; color:#475569; font-size:.82rem; font-weight:600;
    cursor:pointer; transition:all .15s; white-space:nowrap;
  }
  .mbx-tab:hover { background:#f1f5f9; border-color:#cbd5e1; }
  .mbx-tab.active {
    background:var(--tab-active-bg, #1e40af);
    color:var(--tab-active-fc, #fff);
    border-color:transparent;
  }
  .mbx-tab.active .tab-count {
    background:rgba(255,255,255,.25) !important;
    color:inherit !important;
  }
  .tab-count {
    font-size:.72rem; font-weight:700; padding:1px 7px;
    border-radius:999px; background:#e2e8f0; color:#475569;
  }

  /* Footer */
  .footer { text-align:center; font-size:0.78rem; color:#94a3b8; margin-top:28px; padding-bottom:16px; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
  /* ── Enterprise toolbar ── */
  .ent-toolbar { display:flex; align-items:center; flex-wrap:wrap; gap:8px;
                 padding:10px 16px; background:#fff; border:1px solid #e2e8f0;
                 border-radius:10px; margin-bottom:16px; }
  .ent-toolbar .tb-group { display:flex; align-items:center; gap:6px; }
  .ent-toolbar .tb-sep { width:1px; height:24px; background:#e2e8f0; margin:0 4px; }
  .ent-btn { display:inline-flex; align-items:center; gap:5px; padding:6px 13px;
             border:1px solid #e2e8f0; border-radius:7px; background:#f8fafc;
             color:#475569; font-size:.8rem; font-weight:600; cursor:pointer;
             white-space:nowrap; transition:all .15s; }
  .ent-btn:hover { background:#f1f5f9; border-color:#cbd5e1; color:#0f172a; }
  .ent-btn.active { background:#dbeafe; border-color:#93c5fd; color:#1e40af; }
  .ent-btn.green { background:#dcfce7; border-color:#86efac; color:#166534; }
  .tb-search { flex:1; min-width:180px; padding:6px 12px; border:1px solid #e2e8f0;
               border-radius:7px; font-size:.82rem; outline:none; }
  .tb-search:focus { border-color:#93c5fd; }
  .ent-panel { background:#fff; border:1px solid #e2e8f0; border-radius:10px;
               padding:16px 20px; margin-bottom:14px; display:none; }
  .ent-panel.open { display:block; }
  .ent-panel h4 { font-size:.8rem; font-weight:700; color:#475569; text-transform:uppercase;
                  letter-spacing:.08em; margin-bottom:12px; }
  .filter-row  { display:flex; flex-wrap:wrap; gap:14px; align-items:flex-end; }
  .filter-field { display:flex; flex-direction:column; gap:4px; }
  .filter-field label { font-size:.72rem; font-weight:600; color:#64748b; }
  .filter-field input[type=number], .filter-field input[type=date], .filter-field select {
    padding:5px 10px; border:1px solid #e2e8f0; border-radius:6px; font-size:.82rem; width:120px; }
  .filter-field input[type=checkbox] { width:16px; height:16px; }
  .col-grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(140px,1fr)); gap:6px; }
  .col-toggle { display:flex; align-items:center; gap:6px; padding:5px 10px;
                border:1px solid #e2e8f0; border-radius:6px; cursor:pointer;
                font-size:.78rem; background:#f8fafc; user-select:none; }
  .col-toggle.hidden-col { opacity:.4; text-decoration:line-through; }
  .summary-bar { font-size:.78rem; color:#64748b; padding:4px 0 8px; }
  .summary-bar strong { color:#1e293b; }
  .kpi-clickable { cursor:pointer; transition:transform .1s; }
  .kpi-clickable:hover { transform:translateY(-2px); box-shadow:0 4px 12px rgba(0,0,0,.1); }
  .kpi-clickable.kpi-active { outline:3px solid #3b82f6; }
  /* Watch panel */
  .watch-panel {
    position:fixed; bottom:20px; right:20px; width:300px;
    background:#1e293b; color:#e2e8f0; border-radius:14px;
    box-shadow:0 8px 32px rgba(0,0,0,.4); z-index:500; font-size:.82rem;
  }
  .watch-panel-hdr {
    display:flex; align-items:center; justify-content:space-between;
    padding:11px 15px; background:#0f172a; border-radius:14px 14px 0 0; cursor:pointer;
  }
  .watch-panel-title { font-weight:700; font-size:.85rem; display:flex; align-items:center; gap:8px; }
  .watch-dot { width:8px; height:8px; border-radius:50%; background:#4ade80; animation:pulse 2s infinite; }
  .watch-dot.stale { background:#f59e0b; } .watch-dot.err { background:#ef4444; animation:none; }
  .watch-panel-body { padding:12px 15px; display:flex; flex-direction:column; gap:9px; }
  .watch-panel.collapsed .watch-panel-body { display:none; }
  .watch-stat { display:flex; justify-content:space-between; font-size:.78rem; }
  .watch-stat .wl { color:#94a3b8; } .watch-stat .wv { font-weight:600; color:#f8fafc; }
  .watch-sec { font-size:.68rem; font-weight:700; text-transform:uppercase; letter-spacing:.1em; color:#64748b; margin-bottom:3px; }
  .watch-inp { width:100%; padding:5px 9px; background:#334155; border:1px solid #475569;
               border-radius:6px; color:#e2e8f0; font-size:.79rem; box-sizing:border-box; }
  .watch-inp:focus { border-color:#60a5fa; outline:none; }
  .watch-btn-row { display:flex; gap:5px; }
  .wbtn { padding:6px 12px; border-radius:6px; border:none; cursor:pointer; font-size:.79rem; font-weight:600; }
  .wbtn-p { background:#3b82f6; color:#fff; } .wbtn-p:hover { background:#2563eb; }
  .wbtn-s { background:#334155; color:#e2e8f0; border:1px solid #475569; } .wbtn-s:hover { background:#475569; }
  .watch-prog { height:3px; background:#0f172a; border-radius:2px; overflow:hidden; margin-top:4px; }
  .watch-prog-fill { height:100%; background:#3b82f6; border-radius:2px; transition:width 1s linear; }

  /* Keyboard Help Modal */
  .keyboard-help { position:fixed; top:50%; left:50%; transform:translate(-50%,-50%) scale(0.9);
                   background:#fff; border-radius:16px; padding:28px 32px; z-index:10000;
                   box-shadow:0 25px 50px rgba(0,0,0,.25); opacity:0; pointer-events:none;
                   transition:all .2s ease; min-width:380px; max-width:90vw; }
  .keyboard-help.open { opacity:1; transform:translate(-50%,-50%) scale(1); pointer-events:auto; }
  .keyboard-help h3 { font-size:1.1rem; font-weight:700; color:#0f172a; margin-bottom:16px;
                      padding-bottom:12px; border-bottom:1px solid #e2e8f0; }
  .keyboard-help .kb-row { display:flex; justify-content:space-between; align-items:center;
                           padding:8px 0; border-bottom:1px solid #f1f5f9; }
  .keyboard-help .kb-row:last-child { border-bottom:none; }
  .keyboard-help .kb-key { display:inline-block; background:#f1f5f9; color:#334155; font-family:monospace;
                           font-size:.85rem; font-weight:600; padding:4px 10px; border-radius:6px;
                           border:1px solid #e2e8f0; min-width:32px; text-align:center; }
  .keyboard-help .kb-desc { color:#64748b; font-size:.88rem; }
  .keyboard-help .kb-close { position:absolute; top:12px; right:12px; background:none; border:none;
                             font-size:1.4rem; color:#94a3b8; cursor:pointer; }
  .keyboard-help .kb-close:hover { color:#475569; }
  body.dark-mode .keyboard-help { background:#1e293b; }
  body.dark-mode .keyboard-help h3 { color:#f1f5f9; border-color:#334155; }
  body.dark-mode .keyboard-help .kb-row { border-color:#334155; }
  body.dark-mode .keyboard-help .kb-key { background:#334155; color:#e2e8f0; border-color:#475569; }
  body.dark-mode .keyboard-help .kb-desc { color:#94a3b8; }

  /* Sound toggle button */
  .sound-toggle { position:fixed; top:20px; right:72px; z-index:1000; background:#1e293b; color:#f8fafc;
                  border:none; border-radius:50%; width:44px; height:44px; cursor:pointer;
                  font-size:1.2rem; box-shadow:0 2px 8px rgba(0,0,0,.2); transition:all .2s; }
  .sound-toggle:hover { transform:scale(1.1); }
  .sound-toggle.muted { opacity:0.5; }
  body.dark-mode .sound-toggle { background:#f8fafc; color:#1e293b; }

  /* Pin/Bookmark button */
  .pin-btn { background:none; border:none; cursor:pointer; font-size:1.1rem; padding:2px 6px;
             opacity:0.3; transition:all .15s; }
  .pin-btn:hover { opacity:0.7; transform:scale(1.15); }
  .pin-btn.pinned { opacity:1; color:#f59e0b; }
  tr.pinned-row { background:#fef9c3 !important; }
  tr.pinned-row:hover { background:#fef3c7 !important; }
  body.dark-mode tr.pinned-row { background:#422006 !important; }
  body.dark-mode tr.pinned-row:hover { background:#4a2106 !important; }
  .pin-header { width:40px; text-align:center; }

</style>
</head>
<body>
<!-- Dark Mode Toggle -->
<button class="dark-toggle" onclick="toggleDarkMode()" title="Toggle Dark Mode (D)">🌙</button>
<!-- Sound Toggle -->
<button class="sound-toggle" id="sound-toggle" onclick="toggleSound()" title="Toggle Sound Alerts (S)">🔔</button>

<div class="container">

  <!-- Header -->
  <div class="header">
    <h1>📊 Exchange Mailbox Migration Report</h1>
    <div class="meta">
      Batch: <strong>$($Summary.BatchName)</strong> &nbsp;|&nbsp;
      Generated: <strong>$($Summary.GeneratedAt.ToString("yyyy-MM-dd HH:mm:ss"))</strong> &nbsp;|&nbsp;
      Duration: <strong>$($Summary.MigrationDuration)</strong>
    </div>
  </div>
  $(if($AutoRefreshSeconds -gt 0){
    "<div style='background:rgba(255,255,255,.15);border-radius:8px;padding:6px 14px;margin-top:10px;font-size:.8rem;display:inline-flex;align-items:center;gap:8px'>
      <span style='display:inline-block;width:8px;height:8px;border-radius:50%;background:#4ade80;animation:pulse 2s infinite'></span>
      Auto-refreshing every $AutoRefreshSeconds seconds &nbsp;|&nbsp; Last updated: $($Summary.GeneratedAt.ToString("HH:mm:ss"))
    </div>"
  })

  <!-- Health Score -->
  <div class="score-card">
    <div class="score-circle">
      <span class="num">$($Health.Score)</span>
      <span class="lbl">Health</span>
    </div>
    <div>
      <div class="score-grade">$($Health.Grade)</div>
      <div class="score-desc">$(if($Health.IsPartial){"<span style='color:#f59e0b;font-weight:600'>Partial score</span> — $($Health.PartialNote)"}else{"Overall health score based on all 8 weighted metrics."})</div>
      <div style="margin-top:10px">$statusPills</div>
    </div>
  </div>

  <!-- Main navigation tabs -->
  <div class="main-tab-bar">
    <button class="main-tab active" onclick="switchMain('perf',this)">📊 Migration Performance Analysis</button>
    <button class="main-tab"        onclick="switchMain('mbx', this)">📬 Mailbox Migration Detail</button>
  </div>

  <!-- Panel 1: Performance Analysis -->
  <div id="panel-perf" class="main-panel active">
  <!-- KPIs — 12 cards, 4 columns × 3 rows -->
  <div class="kpi-grid" id="kpi-grid">

    <!-- Row 1: Overview -->
    <div class="kpi blue">
      <div class="label">Mailboxes</div>
      <div class="value">$($Summary.MailboxCount)</div>
      <div class="sub">Total in scope</div>
    </div>
    <div class="kpi blue">
      <div class="label">% Complete</div>
      <div class="value">$($Summary.PercentComplete)%</div>
      <div class="sub">Size-weighted completion</div>
    </div>
    <div class="kpi $(if($Summary.EstimatedTimeRemaining -eq 'Complete'){'green'}else{'blue'})">
      <div class="label">ETA</div>
      <div class="value" style="font-size:1.1rem">$($Summary.EstimatedTimeRemaining)</div>
      <div class="sub">$(if($Summary.EstimatedCompletionTime){"~" + $Summary.EstimatedCompletionTime.ToString("MM/dd HH:mm")}else{"Based on current rate"})</div>
    </div>
    <div class="kpi $(if($Summary.IsThrottled){'red'}else{'green'})">
      <div class="label">Throttling</div>
      <div class="value">$(if($Summary.IsThrottled){"⚠ Detected"}else{"✓ None"})</div>
      <div class="sub" title="$($Summary.ThrottleReasons)">$(if($Summary.IsThrottled){$Summary.ThrottleReasons.Substring(0, [Math]::Min(30, $Summary.ThrottleReasons.Length)) + $(if($Summary.ThrottleReasons.Length -gt 30){"..."}else{""})}else{"Performance normal"})</div>
    </div>

    <!-- Row 2: Data transfer -->
    <div class="kpi green">
      <div class="label">Total Transferred</div>
      <div class="value">$($Summary.TotalGBTransferred) GB</div>
      <div class="sub">of $($Summary.TotalSourceSizeGB) GB source</div>
    </div>
    <div class="kpi green">
      <div class="label">Batch Throughput</div>
      <div class="value">$($Summary.TotalThroughputGBPerHour)</div>
      <div class="sub">GB/h wall-clock rate</div>
    </div>
    <div class="kpi green">
      <div class="label">Avg Transfer Rate</div>
      <div class="value">$($Summary.AvgPerMoveTransferRateGBPerHour)</div>
      <div class="sub">GB/h per mailbox (≥0.5)</div>
    </div>
    <div class="kpi $(if ($Summary.MoveEfficiencyPercent -ge 75){'green'}elseif($Summary.MoveEfficiencyPercent-ge60){'amber'}else{'red'})">
      <div class="label">Move Efficiency</div>
      <div class="value">$($Summary.MoveEfficiencyPercent)%</div>
      <div class="sub">Healthy 75–100%</div>
    </div>

    <!-- Row 3: Performance indicators -->
    <div class="kpi $(if ($Summary.AverageSourceLatencyMs -le 100){'green'}elseif($Summary.AverageSourceLatencyMs-le150){'amber'}else{'red'})">
      <div class="label">Avg Source Latency</div>
      <div class="value">$(if($Summary.AverageSourceLatencyMs){"$($Summary.AverageSourceLatencyMs) ms"}else{"N/A"})</div>
      <div class="sub">Target ≤100 ms</div>
    </div>
    <div class="kpi amber">
      <div class="label">Max Per-Move Rate</div>
      <div class="value">$($Summary.MaxPerMoveTransferRateGBPerHour)</div>
      <div class="sub">GB/h fastest mailbox</div>
    </div>
    <div class="kpi amber">
      <div class="label">Min Per-Move Rate</div>
      <div class="value">$($Summary.MinPerMoveTransferRateGBPerHour)</div>
      <div class="sub">GB/h slowest mailbox</div>
    </div>
    <div class="kpi $(if(($Summary.PerMailboxDetail|Where-Object{$_.BadItems -gt 0}).Count -gt 0){'red'}else{'green'})">
      <div class="label">Bad Items</div>
      <div class="value">$(($Summary.PerMailboxDetail | Measure-Object -Property BadItems -Sum).Sum)</div>
      <div class="sub">Across all mailboxes</div>
    </div>

  </div>

  <!-- Health Check Cards — 8 cards, 4 columns × 2 rows -->
  <div class="card">
    <h2>🩺 Metric Health Checks</h2>
    <p class="section-note">
      Each metric contributes its weight × points (Healthy=100%, Warning=50%, Critical=0%) toward the overall health score.
    </p>
    <div class="hc-grid">
      $(
        function New-HcCard {
          param($metric, $value, $displayValue, $status, $weight, $range, $def)
          $isNA     = ($status -eq "N/A")
          $statusBg = switch($status) { "Healthy"{"#dcfce7"} "Warning"{"#fef9c3"} "Critical"{"#fee2e2"} "N/A"{"#f1f5f9"} default{"#f1f5f9"} }
          $statusFc = switch($status) { "Healthy"{"#166534"} "Warning"{"#854d0e"} "Critical"{"#991b1b"} "N/A"{"#94a3b8"} default{"#475569"} }
          $borderC  = switch($status) { "Healthy"{"#22c55e"} "Warning"{"#f59e0b"} "Critical"{"#ef4444"} "N/A"{"#e2e8f0"} default{"#e2e8f0"} }
          $pts      = switch($status) { "Healthy"{$weight} "Warning"{[math]::Round($weight * 0.5,0)} "N/A"{"-"} default{0} }
          $ptsStr   = if($isNA){"N/A — needs -IncludeDetailReport"}else{"$pts/$weight pts"}
          $opacity  = if($isNA){"opacity:0.5;"}else{""}
          "<div class='hc-card' style='border-top:3px solid $borderC;$opacity'>
            <div class='hc-top'>
              <span class='hc-name'>$metric</span>
              <span class='hc-badge' style='background:$statusBg;color:$statusFc'>$status</span>
            </div>
            <div class='hc-value' style='$(if($isNA){"color:#94a3b8;font-size:1rem"})'>$(if($isNA){"Not collected"}else{$displayValue})</div>
            <div class='hc-range'>Healthy: $range</div>
            <div class='hc-def'>$def</div>
            <div class='hc-score-row'>
              <span class='hc-weight'>Weight $weight%</span>
              <span class='hc-pts' style='color:$statusFc;font-weight:700'>$ptsStr</span>
            </div>
          </div>"
        }

        $s = $Summary
        $chk = @{}

        $Health.Checks   | ForEach-Object { $chk[$_.Metric] = $_ }

        if ($Health.NaChecks) { $Health.NaChecks | ForEach-Object { $chk[$_.Metric] = $_ } }


        # Row 1 — Performance (weights rescaled dynamically)
        (New-HcCard "Avg Transfer Rate"        $s.AvgPerMoveTransferRateGBPerHour  "$($s.AvgPerMoveTransferRateGBPerHour) GB/h"  $chk["AvgPerMoveTransferRateGBPerHour"].Status  $chk["AvgPerMoveTransferRateGBPerHour"].Weight ">0.5 GB/h"  "Per-mailbox average rate. >0.5 GB/h healthy. Normal range 0.3–1 GB/h.") +
        (New-HcCard "Move Efficiency"          $s.MoveEfficiencyPercent            "$($s.MoveEfficiencyPercent)%"                $chk["MoveEfficiencyPercent"].Status            $chk["MoveEfficiencyPercent"].Weight           "75–100%"    "Logical data moved ÷ actual wire bytes. Below 75% = excessive retransmission.") +
        (New-HcCard "Source Side Duration"     $s.SourceSideDurationPct            "$($s.SourceSideDurationPct)%"                $chk["SourceSideDuration"].Status               $chk["SourceSideDuration"].Weight               "60–80%"     "Time on source MRSProxy. Above 80% = source bottleneck.") +
        (New-HcCard "Destination Side"         $s.DestinationSideDurationPct       "$($s.DestinationSideDurationPct)%"           $chk["DestinationSideDuration"].Status          $chk["DestinationSideDuration"].Weight          "20–40%"     "Time on dest MRSProxy. Above 40% = destination bottleneck.") +

        # Row 2 — Stability
        (New-HcCard "Transient Failures"       $s.TransientFailureDurationsPct     "$($s.TransientFailureDurationsPct)%"         $chk["TransientFailureDurations"].Status        $chk["TransientFailureDurations"].Weight        "0–5%"       "Intermittent connectivity failures. Check MRSProxy and load balancer config.") +
        (New-HcCard "Overall Stalls"           $s.OverallStallDurationsPct         "$($s.OverallStallDurationsPct)%"             $chk["OverallStallDurations"].Status            $chk["OverallStallDurations"].Weight            "0–15%"      "Total time waiting for CPU, Content Indexing, and HA resources.") +
        (New-HcCard "Word Breaking"            $s.WordBreakingDurationPct          "$($s.WordBreakingDurationPct)%"              $chk["WordBreakingDuration"].Status             $chk["WordBreakingDuration"].Weight             "0–15%"      "Content tokenisation for Office 365 search. Above 15% = dest indexing busy.") +
        (New-HcCard "Source Latency"           $s.AverageSourceLatencyMs           "$(if($s.AverageSourceLatencyMs){"$($s.AverageSourceLatencyMs) ms"}else{"N/A"})" $chk["AverageSourceLatency"].Status $chk["AverageSourceLatency"].Weight "≤100 ms" "No-op WCF call duration to source MRSProxy. Above 100ms = latency issue.")
      )
    </div>
  </div>

  <!-- Duration Breakdown -->
  <div class="card">
    <h2>⏱ Duration Breakdown</h2>
    <p class="section-note">
      All values are percentages of <strong>TotalInProgressDuration</strong> — the time each move was actively transferring data.
      SourceSide + DestinationSide do not always sum to 100%; the remainder is relinquished or unaccounted time.
    </p>

    <!-- Group 1: Time distribution -->
    <div class="dur-group-label">TIME DISTRIBUTION</div>
    $(
      function New-DurRow {
        param($label, $value, $color, $range, $def, $warnAt, $critAt, $direction, $indent=$false)
        # Status dot
        $dot = "#22c55e"  # green default
        if ($direction -eq "High") {
          if ($value -gt $critAt)      { $dot="#ef4444" }
          elseif ($value -gt $warnAt)  { $dot="#f59e0b" }
        } else {
          if ($value -lt $critAt)      { $dot="#ef4444" }
          elseif ($value -lt $warnAt)  { $dot="#f59e0b" }
        }
        $ind = if ($indent) { "margin-left:20px;border-left:2px solid #e2e8f0;padding-left:10px;" } else { "" }
        $pct = [math]::Min($value, 100)

        # Threshold marker for source/dest (show where healthy zone ends)
        $marker = if ($critAt -gt 0 -and $direction -eq "High") {
          "<div style='position:absolute;left:$($warnAt)%;top:0;height:100%;width:2px;background:rgba(0,0,0,.15);'></div>"
        } else { "" }

        "<div class='dur-row' style='$ind' title='$def'>
          <div class='dur-dot' style='background:$dot'></div>
          <div class='dur-label'>
            <span class='dur-name'>$label</span>
            <span class='dur-def'>$def</span>
          </div>
          <div class='dur-bar-wrap'>
            <div class='dur-track'>
              $marker
              <div class='dur-fill' style='width:$($pct)%;background:$color'></div>
            </div>
            <span class='dur-range'>$range</span>
          </div>
          <div class='dur-val'>$($value)%</div>
        </div>"
      }

      (New-DurRow "Source Side Duration"      $Summary.SourceSideDurationPct      "#3b82f6" "60–80%"  "Time on the on-premises MRSProxy. Healthy 60–80%. Above 80% = source bottleneck." 80 90 "High") +
      (New-DurRow "Destination Side Duration" $Summary.DestinationSideDurationPct "#8b5cf6" "20–40%"  "Time on the Office 365 MRSProxy. Healthy 20–40%. Above 40% = destination bottleneck." 40 55 "High") +
      (New-DurRow "Word Breaking Duration"    $Summary.WordBreakingDurationPct    "#06b6d4" "0–15%"   "Time tokenising content for Office 365 search indexing. Above 15% = dest indexing busy." 15 20 "High") +
      (New-DurRow "Idle Duration"             $Summary.IdleDurationPct            "#94a3b8" "N/A on EXO" "Not reported by Exchange Online — this metric is on-premises Exchange only. Always shows 0% in EXO migrations." 5 10 "High")
    )

    <!-- Group 2: Problem indicators -->
    <div class="dur-group-label" style="margin-top:20px">PROBLEM INDICATORS</div>
    $(
      (New-DurRow "Transient Failures"     $Summary.TransientFailureDurationsPct "#f59e0b" "0–5%"    "Time in intermittent connectivity failures between MRS and MRSProxy. Check load balancer config." 5 10 "High") +
      (New-DurRow "Overall Stalls"         $Summary.OverallStallDurationsPct     "#ef4444" "0–15%"   "Total time waiting for system resources. Sum of all stall subcategories below." 15 20 "High") +
      (New-DurRow "↳ Content Indexing"     $Summary.ContentIndexingStallsPct     "#f97316" "subset"  "Waiting for the Office 365 Content Indexing service to catch up." 5 10 "High" $true) +
      (New-DurRow "↳ High Availability"    $Summary.HighAvailabilityStallsPct    "#ec4899" "subset"  "Waiting for HA replication of data to passive database copies." 5 10 "High" $true) +
      (New-DurRow "↳ Target CPU"           $Summary.TargetCPUStallsPct           "#dc2626" "subset"  "Waiting for CPU availability on the destination Office 365 server." 5 10 "High" $true) +
      (New-DurRow "↳ Source CPU"           $Summary.SourceCPUStallsPct           "#b45309" "subset"  "Waiting for CPU availability on the source on-premises server." 5 10 "High" $true) +
      (New-DurRow "↳ Read/Write Throttle"  $Summary.ThrottleStallsPct         "#0284c7" "subset"  "EXO throttled the migration — TotalStalledDueToReadThrottle + WriteThrottle. Check migration throttling policies." 5 10 "High" $true) +
      (New-DurRow "↳ Mailbox Locked"       $Summary.MailboxLockedStallPct        "#7c3aed" "subset"  "Mailbox locked — usually caused by transient failures. Check TransientFailureDurations." 3 8 "High" $true) +
      (New-DurRow "↳ Proxy Unknown"        $Summary.ProxyUnknownStallPct         "#0ea5e9" "subset"  "Waiting for unknown remote on-premises resources. Review failures log to identify." 3 8 "High" $true)
    )
  </div>

  <!-- Bottleneck Analysis -->
  <div class="card">
    <h2>🔍 Bottleneck Analysis
      <span style='font-size:.78rem;font-weight:400;color:#64748b;margin-left:8px'>
        Source healthy: 60–80% &nbsp;|&nbsp; Destination healthy: 20–40%
      </span>
    </h2>
    <div class="bottleneck-banner">
      <div class="bottleneck-title">$($Summary.Bottleneck.Bottleneck) &nbsp;
        <span style='background:$bottleneckColor;color:white;padding:2px 10px;border-radius:999px;font-size:.75rem'>
          $($Summary.Bottleneck.Severity)
        </span>
      </div>
      <div class="bottleneck-body" style='margin-top:6px'>$($Summary.Bottleneck.Explanation)</div>
    </div>
    $(
      $causesHtml = ""
      if ($Summary.Bottleneck.Causes -and $Summary.Bottleneck.Causes.Count -gt 0) {
          $causeItems = ($Summary.Bottleneck.Causes | ForEach-Object { "<li>$_</li>" }) -join "`n"
          $causesHtml = "<div style='margin-bottom:14px'>
            <strong style='font-size:.88rem'>Possible Causes:</strong>
            <ul class='rec-list' style='margin-top:6px'>$causeItems</ul>
          </div>"
      }
      $recsHtml = ""
      if ($Summary.Bottleneck.Recommendations -and $Summary.Bottleneck.Recommendations.Count -gt 0) {
          $recItems = ($Summary.Bottleneck.Recommendations | ForEach-Object { "<li>$_</li>" }) -join "`n"
          $recsHtml = "<div>
            <strong style='font-size:.88rem'>Recommended Actions:</strong>
            <ul class='rec-list' style='margin-top:6px'>$recItems</ul>
          </div>"
      }
      $causesHtml + $recsHtml
    )
  </div>


  </div><!-- /panel-perf -->

  <!-- Panel 2: Mailbox Detail -->
  <div id="panel-mbx" class="main-panel">

  <!-- ── Enterprise toolbar ───────────────────────────────────────────── -->
  <div class="ent-toolbar">
    <div class="tb-group">
      <button class="ent-btn" onclick="exportCSV()">&#x2B07; CSV</button>
      <button class="ent-btn" onclick="exportExcel()">&#x2B07; Excel</button>
      <button class="ent-btn" onclick="printReport()">&#x1F5A8; Print</button>
      <button class="ent-btn" onclick="exportPDF()">&#x1F4C4; PDF</button>
    </div>
    <div class="tb-sep"></div>
    <div class="tb-group">
      <button class="ent-btn" id="btn-filters" onclick="togglePanel('adv-filter-panel','btn-filters')">&#x2699; Filters</button>
      <button class="ent-btn" id="btn-cols"    onclick="togglePanel('col-panel','btn-cols')">&#x25A6; Columns</button>
      <button class="ent-btn" onclick="showKeyboardHelp()" title="Keyboard Shortcuts (?)">&#x2328; Keys</button>
    </div>
    <div class="tb-sep"></div>
    <input class="tb-search" id="ent-search" type="text" placeholder="Search mailboxes..." oninput="applyFilters()">
  </div>

  <!-- Advanced filter panel -->
  <div class="ent-panel" id="adv-filter-panel">
    <h4>Advanced Filters</h4>
    <div class="filter-row">
      <div class="filter-field"><label>Min Rate (GB/h)</label><input type="number" id="f-rate-min" min="0" step="0.1" placeholder="0" oninput="applyFilters()"></div>
      <div class="filter-field"><label>Max Rate (GB/h)</label><input type="number" id="f-rate-max" min="0" step="0.1" placeholder="any" oninput="applyFilters()"></div>
      <div class="filter-field"><label>Min Size (GB)</label><input type="number" id="f-size-min" min="0" step="0.1" placeholder="0" oninput="applyFilters()"></div>
      <div class="filter-field"><label>Max Size (GB)</label><input type="number" id="f-size-max" min="0" step="0.1" placeholder="any" oninput="applyFilters()"></div>
      <div class="filter-field"><label>Min % Done</label><input type="number" id="f-pct-min" min="0" max="100" placeholder="0" oninput="applyFilters()"></div>
      <div class="filter-field"><label>Max % Done</label><input type="number" id="f-pct-max" min="0" max="100" placeholder="100" oninput="applyFilters()"></div>
      <div class="filter-field" style="justify-content:flex-end;padding-bottom:2px"><label><input type="checkbox" id="f-baditems" onchange="applyFilters()"> Bad Items only</label></div>
      <div class="filter-field" style="justify-content:flex-end;padding-bottom:2px"><button class="ent-btn" onclick="resetFilters()">Reset</button></div>
    </div>
  </div>

  <!-- Column visibility panel -->
  <div class="ent-panel" id="col-panel">
    <h4>Column Visibility</h4>
    <div class="col-grid" id="col-grid"></div>
  </div>

  <!-- Per-Mailbox Detail with status tabs -->
  <div class="card" id="mbx-card">
    <div style="display:flex;align-items:baseline;justify-content:space-between;flex-wrap:wrap;gap:8px;margin-bottom:14px">
      <h2 style="margin:0;border:none;padding:0">📬 Per-Mailbox Detail</h2>
      <div style="font-size:.78rem;color:#64748b">$($Summary.MailboxCount) mailboxes &nbsp;|&nbsp; top $($Summary.PercentileUsed)% percentile used for aggregates$(if(@($Summary.SlowestMailboxes).Count -gt 0){" &nbsp;|&nbsp; <span style='color:#f59e0b;font-weight:600'>$(@($Summary.SlowestMailboxes).Count) slowest excluded</span>"})</div>
    </div>

    <!-- Tab bar — tabs injected by JS based on statuses present in data -->
    <div id="mbx-summary" class="summary-bar"></div>
    <div class="tab-bar" id="mbx-tabs"></div>

    <!-- Search -->


    <div class="tbl-wrap">
    <table id="mbx-table">
      <thead>
        <tr>
          <th class="pin-header">📌<span class='th-tip'>Pin/bookmark mailboxes to keep them at the top of the list.</span></th><th>Display Name<span class='th-tip'>Full display name of the mailbox owner.</span></th><th>Alias<span class='th-tip'>Mail alias (short logon name).</span></th><th>Status<span class='th-tip'>Current move request status: InProgress, Synced, Completed, etc.</span></th><th>% Done<span class='th-tip'>Percentage of mailbox data copied to the destination.</span></th>

          <th>Primary (GB)<span class='th-tip'>Total size of the primary mailbox on the source server.</span></th><th>Archive (GB)<span class='th-tip'>Total size of the online archive mailbox, if present.</span></th><th>Transferred (GB)<span class='th-tip'>Actual bytes sent over the wire — may exceed source size due to retransmissions.</span></th><th>Rate GB/h<span class='th-tip'>Average transfer rate in GB/h = Transferred / TotalInProgressDuration. Healthy: &gt;0.5 GB/h.</span></th><th>Efficiency<span class='th-tip'>Logical data moved as a % of actual wire bytes. Below 75% = excessive retransmissions. Formula: (SourceSize &times; %Done) / Transferred.</span></th>

          <th>Total Duration<span class='th-tip'>Wall-clock time since queued — includes queued, suspended, and active time. This is what the user experiences.</span></th><th>Active Duration<span class='th-tip'>Time actively transferring data (TotalInProgressDuration). Excludes queued &amp; suspended time. Used for rate calculations.</span></th><th>Items<span class='th-tip'>Number of mail items successfully transferred.</span></th><th>Bad Items<span class='th-tip'>Sum of all problem items: BadItemsEncountered + ItemsSkippedDueToLocalFailure + LargeItemsEncountered + MissingItemsEncountered. Red when &gt; 0. Click row for breakdown.</span></th><th>Consistency<span class='th-tip'>Data consistency score: Good, Fair, or Poor. Poor may indicate data integrity issues.</span></th><th>Sync Stage<span class='th-tip'>Current internal MRS stage (e.g. IncrementalSync, CopyingMessages, CreatingFolderHierarchy).</span></th>

        </tr>
      </thead>
      <tbody id="mbx-tbody">$mbxRows</tbody>
      <!-- SLOWEST_ROWS_START -->
      <tbody id="slowest-tbody" style="display:none">$slowestRows</tbody>
      <!-- SLOWEST_ROWS_END -->
    </table>
    </div>
    <div id="mbx-empty" style="display:none;text-align:center;padding:24px;color:#94a3b8;font-size:.88rem">
      No mailboxes match the current filter.
    </div>
  </div>


  </div><!-- /panel-mbx -->

  <div class="footer">
    Exchange Migration Analyzer &nbsp;•&nbsp; Generated $($Summary.GeneratedAt.ToString("R"))
  </div>
</div>

<script>
(function () {
  var activeTab = 'All';

  // ── Build tabs from unique statuses in the table rows ─────────────────────
  var tbody        = document.getElementById('mbx-tbody');
  var slowestTbody = document.getElementById('slowest-tbody');
  var tabBar       = document.getElementById('mbx-tabs');
  var rows         = Array.from(tbody.querySelectorAll('tr'));
  var slowestRows  = Array.from(slowestTbody ? slowestTbody.querySelectorAll('tr') : []);
  var inSlowestTab = false;

  // Collect statuses and counts
  var counts = { All: rows.length };
  rows.forEach(function (r) {
    var s = r.getAttribute('data-status') || 'Unknown';
    counts[s] = (counts[s] || 0) + 1;
  });

  // Status order priority — active/problem first, completed last
  var priority = ['InProgress','Synced','AutoSuspended','Suspended',
                  'Failed','CompletedWithWarning','CompletedWithSkippedItems',
                  'Completed','Queued','None'];

  var statuses = Object.keys(counts).filter(function(s){ return s !== 'All'; });
  statuses.sort(function(a, b) {
    var ia = priority.indexOf(a), ib = priority.indexOf(b);
    if (ia === -1) ia = 99;
    if (ib === -1) ib = 99;
    return ia - ib;
  });

  // Status badge colours matching the table rows
  var badgeColors = {
    InProgress:                { bg:'#dbeafe', fc:'#1e40af' },
    Synced:                    { bg:'#dcfce7', fc:'#166534' },
    Completed:                 { bg:'#dcfce7', fc:'#166534' },
    CompletedWithWarning:      { bg:'#fef9c3', fc:'#854d0e' },
    CompletedWithSkippedItems: { bg:'#fef9c3', fc:'#854d0e' },
    AutoSuspended:             { bg:'#fef9c3', fc:'#854d0e' },
    Suspended:                 { bg:'#fef9c3', fc:'#854d0e' },
    Failed:                    { bg:'#fee2e2', fc:'#991b1b' },
    Queued:                    { bg:'#f1f5f9', fc:'#475569' }
  };

  function makeTab(label, count, customBg, customFc) {
    var tab = document.createElement('button');
    tab.className = 'mbx-tab' + (label === 'All' ? ' active' : '');
    tab.setAttribute('data-tab', label);

    var badge = document.createElement('span');
    badge.className = 'tab-count';
    badge.textContent = count;

    if (customBg) {
      tab.style.setProperty('--tab-active-bg', customBg);
      tab.style.setProperty('--tab-active-fc', customFc);
      badge.style.background = customBg;
      badge.style.color      = customFc;
    } else if (label !== 'All' && badgeColors[label]) {
      var c = badgeColors[label];
      tab.style.setProperty('--tab-active-bg', c.bg);
      tab.style.setProperty('--tab-active-fc', c.fc);
      badge.style.background = c.bg;
      badge.style.color      = c.fc;
    }

    tab.appendChild(document.createTextNode(label + '\u00a0'));
    tab.appendChild(badge);
    tab.onclick = function () { setTab(label); };
    return tab;
  }

  // Render All tab first, then status tabs
  tabBar.appendChild(makeTab('All', counts['All']));
  statuses.forEach(function (s) { tabBar.appendChild(makeTab(s, counts[s])); });

  // Add Slowest tab if there are excluded mailboxes
  if (slowestRows.length > 0) {
    tabBar.appendChild(makeTab('Slowest \u26a0', slowestRows.length, '#fff7ed', '#9a3412'));
  }

  // ── Filter logic ─────────────────────────────────────────────────────────
  function setTab(label) {
    activeTab = label;
    inSlowestTab = (label === 'Slowest \u26a0');
    document.querySelectorAll('.mbx-tab').forEach(function (t) {
      t.classList.toggle('active', t.getAttribute('data-tab') === label);
    });
    // Show the correct tbody
    tbody.style.display        = inSlowestTab ? 'none' : '';
    if (slowestTbody) slowestTbody.style.display = inSlowestTab ? '' : 'none';
    applyFilters();
  }

  window.applyFilters = function () {
    // Unified search: check ent-search (toolbar) or legacy mbx-search
    var entSearch = document.getElementById('ent-search');
    var legSearch = document.getElementById('mbx-search');
    var q = ((entSearch ? entSearch.value : '') || (legSearch ? legSearch.value : '') || '').toLowerCase();

    // Read advanced filters if available
    if (typeof readAdvFilters === 'function') readAdvFilters();

    var visible = 0;
    var total   = 0;
    var activeRows = inSlowestTab ? slowestRows : rows;

    // Hide the inactive tbody entirely
    tbody.style.display        = inSlowestTab ? 'none' : '';
    if (slowestTbody) slowestTbody.style.display = inSlowestTab ? '' : 'none';

    activeRows.forEach(function (r) {
      total++;
      var statusMatch = inSlowestTab || (activeTab === 'All') || (r.getAttribute('data-status') === activeTab);
      var textMatch   = !q || r.textContent.toLowerCase().indexOf(q) !== -1;
      var advMatch    = (typeof rowMatchesAdvFilters === 'function') ? rowMatchesAdvFilters(r) : true;
      var show = statusMatch && textMatch && advMatch;
      r.style.display = show ? '' : 'none';
      if (show) visible++;
    });
    document.getElementById('mbx-empty').style.display = visible === 0 ? '' : 'none';
    if (typeof updateSummaryBar === 'function') updateSummaryBar(visible, total);
  };

  // ── Sort columns on header click ──────────────────────────────────────────
  var sortState = { col: -1, asc: true };
  document.querySelectorAll('#mbx-table thead th').forEach(function (th, idx) {
    th.style.cursor = 'pointer';
    th.title = '';  // tooltip shown via CSS .th-tip span
    th.onclick = function () {
      var asc = sortState.col === idx ? !sortState.asc : true;
      sortState = { col: idx, asc: asc };
      var activeBody = inSlowestTab ? slowestTbody : tbody;
      var allRows = Array.from(activeBody.querySelectorAll('tr'));
      allRows.sort(function (a, b) {
        var ta = a.cells[idx] ? a.cells[idx].textContent.trim() : '';
        var tb = b.cells[idx] ? b.cells[idx].textContent.trim() : '';
        var na = parseFloat(ta), nb = parseFloat(tb);
        var cmp = (!isNaN(na) && !isNaN(nb)) ? na - nb : ta.localeCompare(tb);
        return asc ? cmp : -cmp;
      });
      allRows.forEach(function (r) { activeBody.appendChild(r); });
      // Re-sync rows reference after sort
      if (inSlowestTab) {
        slowestRows.length = 0;
        Array.from(slowestTbody.querySelectorAll('tr')).forEach(function(r){ slowestRows.push(r); });
      } else {
        rows.length = 0;
        Array.from(tbody.querySelectorAll('tr')).forEach(function(r){ rows.push(r); });
      }
      applyFilters();
      // Update sort indicators
      document.querySelectorAll('#mbx-table thead th').forEach(function(h,i){
        h.textContent = h.textContent.replace(/ [\u25b2\u25bc]$/,'');
        if (i === idx) h.textContent += asc ? ' \u25b2' : ' \u25bc';
      });
    };
  });
})();

  // ── Main tab switcher ────────────────────────────────────────────────────
  function switchMain(id, btn) {
    document.querySelectorAll('.main-panel').forEach(function(p){ p.classList.remove('active'); });
    document.querySelectorAll('.main-tab').forEach(function(b){ b.classList.remove('active'); });
    document.getElementById('panel-' + id).classList.add('active');
    btn.classList.add('active');
  }
</script>


<!-- ── Mailbox Detail Modal ─────────────────────────────────────────── -->
<style>
  .mbx-modal-overlay {
    display:none; position:fixed; inset:0; background:rgba(0,0,0,.45);
    z-index:1000; align-items:center; justify-content:center;
    backdrop-filter:blur(2px);
  }
  .mbx-modal-overlay.open { display:flex; }
  .mbx-modal {
    background:#fff; border-radius:16px; box-shadow:0 20px 60px rgba(0,0,0,.25);
    width:min(780px,95vw); max-height:88vh; overflow-y:auto;
    animation:modalIn .18s ease;
  }
  @keyframes modalIn { from{opacity:0;transform:translateY(-16px)} to{opacity:1;transform:none} }
  .mbx-modal-header {
    display:flex; align-items:flex-start; justify-content:space-between;
    padding:24px 28px 16px; border-bottom:1px solid #e2e8f0; position:sticky; top:0;
    background:#fff; border-radius:16px 16px 0 0; z-index:1;
  }
  .mbx-modal-title  { font-size:1.15rem; font-weight:700; color:#0f172a; }
  .mbx-modal-sub    { font-size:.82rem; color:#64748b; margin-top:3px; }
  .mbx-modal-close  {
    background:none; border:none; cursor:pointer; padding:4px 8px;
    font-size:1.3rem; color:#94a3b8; line-height:1; border-radius:6px;
  }
  .mbx-modal-close:hover { background:#f1f5f9; color:#475569; }
  .mbx-modal-body   { padding:20px 28px 28px; }
  .mbx-section      { margin-bottom:12px; }
  .mbx-section-title{
    font-size:.68rem; font-weight:700; text-transform:uppercase;
    letter-spacing:.1em; color:#94a3b8; margin-bottom:10px;
    padding-bottom:6px; border-bottom:1px solid #f1f5f9;
  }
  .mbx-grid         { display:grid; grid-template-columns:1fr 1fr; gap:8px 20px; }
  .mbx-grid-3       { display:grid; grid-template-columns:1fr 1fr 1fr; gap:8px 20px; }
  .mbx-field        { display:flex; flex-direction:column; gap:2px; }
  .mbx-label        { font-size:.7rem; font-weight:600; color:#94a3b8; text-transform:uppercase; letter-spacing:.05em; }
  .mbx-value        { font-size:.88rem; color:#1e293b; font-weight:500; word-break:break-all; }
  .mbx-value.mono   { font-family:monospace; font-size:.83rem; }
  .mbx-value.warn   { color:#ef4444; font-weight:600; }
  .mbx-value.good   { color:#22c55e; font-weight:600; }
  .mbx-value.na     { color:#94a3b8; font-style:italic; }
  .mbx-failbox {
    background:#fff7f7; border:1px solid #fecaca; border-radius:8px;
    padding:10px 14px; font-size:.8rem; font-family:monospace;
    color:#991b1b; line-height:1.5; word-break:break-all; max-height:100px; overflow-y:auto;
  }
  .mbx-badge {
    display:inline-block; padding:2px 10px; border-radius:999px;
    font-size:.76rem; font-weight:600;
  }
</style>

<div class="mbx-modal-overlay" id="mbxModal" onclick="if(event.target===this)closeMbxModal()">
  <div class="mbx-modal" id="mbxModalContent">
    <div class="mbx-modal-header">
      <div>
        <div class="mbx-modal-title" id="mdTitle"></div>
        <div class="mbx-modal-sub"  id="mdSub"></div>
      </div>
      <div style="display:flex;gap:8px;align-items:center;">
        <button class="ent-btn" onclick="saveModalAsImage()" title="Save as Image">&#x1F4F7; Save Image</button>
        <button class="mbx-modal-close" onclick="closeMbxModal()">&#x2715;</button>
      </div>
    </div>
    <div class="mbx-modal-body" id="mdBody"></div>
  </div>
</div>

<script>
(function(){
  var statusColors = {
    InProgress:                {bg:'#dbeafe',fc:'#1e40af'},
    Synced:                    {bg:'#dcfce7',fc:'#166534'},
    Completed:                 {bg:'#dcfce7',fc:'#166534'},
    CompletedWithWarning:      {bg:'#fef9c3',fc:'#854d0e'},
    CompletedWithSkippedItems: {bg:'#fef9c3',fc:'#854d0e'},
    AutoSuspended:             {bg:'#fef9c3',fc:'#854d0e'},
    Suspended:                 {bg:'#fef9c3',fc:'#854d0e'},
    Failed:                    {bg:'#fee2e2',fc:'#991b1b'},
    Queued:                    {bg:'#f1f5f9',fc:'#475569'}
  };

  function consistencyColor(s){
    if(s==='Good') return 'good';
    if(s==='Poor') return 'warn';
    return '';
  }

  function field(label, val, cls){
    var c = cls || '';
    var display = (val===''||val===null||val===undefined||val==='0'&&label==='Archive')?
      "<span class='mbx-value na'>—</span>" :
      "<span class='mbx-value "+c+"'>"+val+"</span>";
    return "<div class='mbx-field'><span class='mbx-label'>"+label+"</span>"+display+"</div>";
  }

  function statusBadge(s){
    var c = statusColors[s] || {bg:'#f1f5f9',fc:'#475569'};
    return "<span class='mbx-badge' style='background:"+c.bg+";color:"+c.fc+"'>"+s+"</span>";
  }

  function openMbxModal(row){
    var d = row.dataset;

    // Header
    document.getElementById('mdTitle').textContent = d.dn || d.alias;
    var sub = d.alias;
    if(d.email) sub += '  ·  ' + d.email;
    if(d.batch) sub += '  ·  Batch: ' + d.batch;
    document.getElementById('mdSub').textContent = sub;

    var body = '';

    // ── Status & Progress ──────────────────────────────────────────────
    body += "<div class='mbx-section' style='background:#eff6ff;border-radius:10px;padding:14px 16px;'>";
    body += "<div class='mbx-section-title'>Status &amp; Progress</div>";
    body += "<div class='mbx-grid'>";
    body += field('Status',       statusBadge(d.status));
    body += field('Sync Stage',   d.syncstage, 'mono');
    body += field('% Complete',   d.pct + '%', d.pct>=95?'good':d.pct>=50?'':'warn');
    body += field('Consistency',  d.consistency, consistencyColor(d.consistency));
    body += "</div>";
    if(d.factors){
      body += "<div style='margin-top:8px'>";
      body += field('Consistency Factors', d.factors);
      body += "</div>";
    }
    body += "</div>";

    // ── Mailbox Sizes ──────────────────────────────────────────────────
    body += "<div class='mbx-section' style='background:#f0fdf4;border-radius:10px;padding:14px 16px;'>";
    body += "<div class='mbx-section-title'>Mailbox Size</div>";
    body += "<div class='mbx-grid-3'>";
    body += field('Primary (GB)',     d.primary,  'mono');
    body += field('Archive (GB)',     d.archive,  'mono');
    body += field('Transferred (GB)', d.xfer,     'mono');
    body += "</div></div>";

    // ── Transfer Performance ───────────────────────────────────────────
    body += "<div class='mbx-section' style='background:#fefce8;border-radius:10px;padding:14px 16px;'>";
    body += "<div class='mbx-section-title'>Transfer Performance</div>";
    body += "<div class='mbx-grid'>";
    body += field('Rate GB/h',       d.rate,  parseFloat(d.rate)>=0.5?'good':parseFloat(d.rate)>=0.3?'':'warn');
    body += field('Efficiency',      d.eff+'%', parseFloat(d.eff)>=75?'good':parseFloat(d.eff)>=60?'':'warn');
    body += field('Items Transferred', parseInt(d.items||0).toLocaleString());
    body += field('Bad Items',       d.baditems, parseInt(d.baditems||0)>0?'warn':'good');
    body += field('Skipped Items',  d.skipped||'0',  parseInt(d.skipped||0)>0?'warn':'');

    body += field('Large Items',    d.large||'0',    parseInt(d.large||0)>0?'warn':'');

    body += field('Missing Items',  d.missing||'0',  parseInt(d.missing||0)>0?'warn':'');
    var srcLat = d.srclatency && d.srclatency !== 'N/A' ? d.srclatency : null;
    var dstLat = d.dstlatency && d.dstlatency !== 'N/A' ? d.dstlatency : null;
    body += field('Source Latency', srcLat ? srcLat+' ms' : '—',
                   srcLat ? (parseFloat(srcLat)<=100?'good':'warn') : 'na');
    body += field('Dest Latency',   dstLat ? dstLat+' ms' : '—', dstLat ? '' : 'na');
    body += "</div></div>";

    // ── Duration Breakdown ─────────────────────────────────────────────────────────

    body += "<div class='mbx-section' style='background:#faf5ff;border-radius:10px;padding:14px 16px;'>";

    body += "<div class='mbx-section-title'>Duration</div>";

    body += "<div class='mbx-grid-3'>";

    body += field('Total (wall-clock)',    d.overall||'—', 'mono');

    body += field('Active (transferring)', d.active||'—',  'mono');

    body += field('Queue Wait',            d.queueddur && d.queueddur!=='0:00:00' ? d.queueddur : '—', 'mono');

    body += "</div>";

    var gapSecs = d.gapdur ? (function(t){

      var p=t.split(':'); return (+p[0])*3600+(+p[1])*60+(+(p[2]||0));

    })(d.gapdur) : 0;

    if(gapSecs > 5){

      body += "<div style='margin-top:8px'>";

      body += field('Suspended / Stall Gap',

        "<span style='color:#f59e0b;font-weight:600'>"+d.gapdur+"</span> "

        +"<span style='font-size:.75rem;color:#64748b'>(Total − Active − Queue ≠ 0 &mdash; time in suspended, failed, or stall state)</span>");

      body += "</div>";

    }

    body += "</div>";


    // ── Timeline ───────────────────────────────────────────────────────
    body += "<div class='mbx-section' style='background:#fff7ed;border-radius:10px;padding:14px 16px;'>";
    body += "<div class='mbx-section-title'>Timeline</div>";
    body += "<div class='mbx-grid'>";
    body += field('Queued',          d.queued  ||'—');
    body += field('Started',         d.start   ||'—');
    body += field('Initial Seeding', d.seeding ||'—');
    body += field('Last Sync',       d.lastsync||'—');
    body += field('Completed',       d.complete||'—');
    body += "</div></div>";

    // ── Infrastructure ─────────────────────────────────────────────────
    body += "<div class='mbx-section' style='background:#f8fafc;border-radius:10px;padding:14px 16px;'>";
    body += "<div class='mbx-section-title'>Infrastructure</div>";
    body += "<div class='mbx-grid'>";
    body += field('Source Version',  d.srcver, 'mono');
    body += field('Target Version',  d.tgtver, 'mono');
    body += field('Source Server',   d.remote, 'mono');
    body += field('MRS Server',      d.mrssrv, 'mono');
    body += "</div></div>";

    // ── Stall Breakdown ────────────────────────────────────────────────
    var tickProgress = parseInt(d.tickprogress||0);
    var stallItems = [
      { label:'Transient Failures',  tick: parseInt(d.ticktransient||0),      color:'#f59e0b' },
      { label:'Content Indexing',    tick: parseInt(d.tickci||0),             color:'#f97316' },
      { label:'High Availability',   tick: parseInt(d.tickha||0),             color:'#ec4899' },
      { label:'Target CPU',          tick: parseInt(d.ticktargetcpu||0),      color:'#dc2626' },
      { label:'Source CPU',          tick: parseInt(d.ticksourcecpu||0),      color:'#b45309' },
      { label:'Mailbox Locked',      tick: parseInt(d.tickmbxlocked||0),      color:'#7c3aed' },
      { label:'Read Throttle',       tick: parseInt(d.tickreadthrottle||0),   color:'#0284c7' },
      { label:'Write Throttle',      tick: parseInt(d.tickwritethrottle||0),  color:'#0369a1' },
      { label:'Proxy Unknown',       tick: parseInt((parseInt(d.tickproxysrc||0)+parseInt(d.tickproxydst||0))||0), color:'#0ea5e9' },
      { label:'Word Breaking',       tick: parseInt(d.tickwordbreak||0),      color:'#06b6d4' },
    ];
    var hasStalls = stallItems.some(function(s){ return s.tick > 0; });
    if(hasStalls && tickProgress > 0){
      body += "<div class='mbx-section' style='background:#fafafa;border:1px solid #e2e8f0;border-radius:10px;padding:14px 16px;'>";
      body += "<div class='mbx-section-title'>Stall Breakdown</div>";
      stallItems.forEach(function(s){
        if(s.tick === 0) return;
        var pct = Math.min(100, Math.round(s.tick / tickProgress * 1000) / 10);
        var barW = Math.min(100, pct);
        var warnCls = pct > 10 ? 'warn' : pct > 5 ? '' : '';
        body += "<div style='margin-bottom:8px'>";
        body += "<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:3px'>";
        body += "<span style='font-size:.78rem;font-weight:600;color:#475569'>"+s.label+"</span>";
        body += "<span style='font-size:.78rem;font-weight:700;color:"+(pct>10?'#ef4444':pct>5?'#f59e0b':'#64748b')+"'>"+pct+"%</span>";
        body += "</div>";
        body += "<div style='height:8px;background:#f1f5f9;border-radius:4px;overflow:hidden'>";
        body += "<div style='height:100%;width:"+barW+"%;background:"+s.color+";border-radius:4px;transition:width .3s'></div>";
        body += "</div></div>";
      });
      body += "</div>";
    }

    // ── Last Failure ───────────────────────────────────────────────────
    if(d.lastfail && d.lastfail.trim()){
      body += "<div class='mbx-section' style='background:#fff1f2;border-radius:10px;padding:14px 16px;'>";
      body += "<div class='mbx-section-title'>Last Failure</div>";
      body += "<div class='mbx-failbox'>"+d.lastfail+"</div>";
      body += "</div>";
    }

    document.getElementById('mdBody').innerHTML = body;
    document.getElementById('mbxModal').classList.add('open');
  }

  window.closeMbxModal = function(){
    document.getElementById('mbxModal').classList.remove('open');
  };

  // Keyboard close
  document.addEventListener('keydown', function(e){
    if(e.key==='Escape') closeMbxModal();
  });

  // Wire up click on all current and future rows via event delegation
  document.addEventListener('click', function(e){
    var row = e.target.closest('tr[data-status]');
    if(row && !e.target.closest('button')) openMbxModal(row);
  });
})();
</script>

<script>
// ═══════════════════════════════════════════════════════════════════
// ENTERPRISE FEATURES
// ═══════════════════════════════════════════════════════════════════

// ── Panel toggle ─────────────────────────────────────────────────
function togglePanel(id, btnId) {
  var p = document.getElementById(id);
  var b = document.getElementById(btnId);
  var open = p.classList.toggle('open');
  if (b) b.classList.toggle('active', open);
}

// ── Column visibility ─────────────────────────────────────────────
var hiddenCols = {};
var colHeaders = [];

function initColumns() {
  var ths = document.querySelectorAll('#mbx-table thead th');
  var grid = document.getElementById('col-grid');
  if (!grid) return;
  ths.forEach(function(th, i) {
    colHeaders[i] = th.textContent.replace(/[▲▼]/g,'').trim().split('\n')[0].trim();
    var tog = document.createElement('div');
    tog.className = 'col-toggle';
    tog.id = 'coltog-' + i;
    tog.innerHTML = '<input type="checkbox" checked> ' + colHeaders[i];
    tog.onclick = function() { toggleCol(i); };
    grid.appendChild(tog);
  });
}

function toggleCol(idx) {
  hiddenCols[idx] = !hiddenCols[idx];
  applyColVisibility();
  var tog = document.getElementById('coltog-' + idx);
  if (tog) tog.classList.toggle('hidden-col', !!hiddenCols[idx]);
  var cb = tog ? tog.querySelector('input') : null;
  if (cb) cb.checked = !hiddenCols[idx];
}

function applyColVisibility() {
  document.querySelectorAll('#mbx-table thead th').forEach(function(th,i){
    th.style.display = hiddenCols[i] ? 'none' : '';
  });
  document.querySelectorAll('#mbx-table tbody tr').forEach(function(tr){
    Array.from(tr.cells).forEach(function(td,i){ td.style.display = hiddenCols[i] ? 'none' : ''; });
  });
}

// ── Advanced filters ──────────────────────────────────────────────
var advF = { rateMin:0, rateMax:Infinity, sizeMin:0, sizeMax:Infinity,
             pctMin:0, pctMax:100, badOnly:false };

function readAdvFilters() {
  function v(id) { var el = document.getElementById(id); return el ? el.value : ''; }
  advF.rateMin = parseFloat(v('f-rate-min')) || 0;
  advF.rateMax = parseFloat(v('f-rate-max')) || Infinity;
  advF.sizeMin = parseFloat(v('f-size-min')) || 0;
  advF.sizeMax = parseFloat(v('f-size-max')) || Infinity;
  advF.pctMin  = parseFloat(v('f-pct-min'))  || 0;
  advF.pctMax  = parseFloat(v('f-pct-max'))  || 100;
  var cb = document.getElementById('f-baditems');
  advF.badOnly = cb ? cb.checked : false;
}

function resetFilters() {
  ['f-rate-min','f-rate-max','f-size-min','f-size-max','f-pct-min','f-pct-max'].forEach(function(id){
    var el = document.getElementById(id); if(el) el.value = '';
  });
  var cb = document.getElementById('f-baditems'); if(cb) cb.checked = false;
  var es = document.getElementById('ent-search'); if(es) es.value = '';
  advF = { rateMin:0, rateMax:Infinity, sizeMin:0, sizeMax:Infinity,
           pctMin:0, pctMax:100, badOnly:false };
  applyFilters();
}

function rowMatchesAdvFilters(r) {
  var d = r.dataset;
  if (parseFloat(d.rate  ||0) < advF.rateMin || parseFloat(d.rate  ||0) > advF.rateMax) return false;
  if (parseFloat(d.primary||0) < advF.sizeMin || parseFloat(d.primary||0) > advF.sizeMax) return false;
  if (parseFloat(d.pct   ||0) < advF.pctMin  || parseFloat(d.pct   ||0) > advF.pctMax ) return false;
  if (advF.badOnly && parseInt(d.baditems||0) === 0) return false;
  return true;
}

// ── KPI click-to-filter ───────────────────────────────────────────
var kpiStatusFilter = null;
function initKpiFilter() {
  document.querySelectorAll('.kpi').forEach(function(kpi) {
    kpi.classList.add('kpi-clickable');
    kpi.addEventListener('click', function() {
      // Find status from label text
      var label = (kpi.querySelector('.label')||{}).textContent || '';
      // Map KPI labels to status filters
      var map = {
        'Bad Items': '_baditems',
        'Move Efficiency': '_efficiency',
      };
      // For status breakdown KPIs injected by PS - check sub text
      var sub = (kpi.querySelector('.sub')||{}).textContent || '';
      var statusMatch = null;
      ['InProgress','Synced','Completed','Failed','AutoSuspended','Suspended'].forEach(function(s){
        if(label.indexOf(s)!==-1||sub.indexOf(s)!==-1) statusMatch = s;
      });
      if (statusMatch) {
        // Switch to that status tab
        document.querySelectorAll('.mbx-tab').forEach(function(t){
          if(t.getAttribute('data-tab')===statusMatch) t.click();
        });
        // Switch to mailbox detail panel
        document.querySelectorAll('.main-tab').forEach(function(b){
          if(b.getAttribute('onclick') && b.getAttribute('onclick').indexOf("'mbx'")!==-1) b.click();
        });
        kpi.classList.toggle('kpi-active');
      }
    });
  });
}

// ── Summary bar ───────────────────────────────────────────────────
function updateSummaryBar(visible, total) {
  var bar = document.getElementById('mbx-summary');
  if (!bar) return;
  bar.innerHTML = 'Showing <strong>' + visible + '</strong> of <strong>' + total + '</strong> mailboxes';
}

// ══════════════════════════════════════════════════════════════════
// PIN/BOOKMARK MAILBOXES
// ══════════════════════════════════════════════════════════════════
var pinnedMailboxes = JSON.parse(localStorage.getItem('migrationPinnedMailboxes') || '[]');

function togglePin(btn) {
  var row = btn.closest('tr');
  var alias = row.getAttribute('data-alias') || row.cells[2].textContent.trim();

  var idx = pinnedMailboxes.indexOf(alias);
  if (idx === -1) {
    pinnedMailboxes.push(alias);
    btn.classList.add('pinned');
    row.classList.add('pinned-row');
  } else {
    pinnedMailboxes.splice(idx, 1);
    btn.classList.remove('pinned');
    row.classList.remove('pinned-row');
  }

  localStorage.setItem('migrationPinnedMailboxes', JSON.stringify(pinnedMailboxes));
  sortPinnedToTop();
}

function sortPinnedToTop() {
  var tbody = document.getElementById('mbx-tbody');
  if (!tbody) return;

  var rows = Array.from(tbody.querySelectorAll('tr'));
  var pinned = [];
  var unpinned = [];

  rows.forEach(function(row) {
    var alias = row.getAttribute('data-alias') || row.cells[2].textContent.trim();
    if (pinnedMailboxes.indexOf(alias) !== -1) {
      pinned.push(row);
    } else {
      unpinned.push(row);
    }
  });

  // Re-append in order: pinned first, then unpinned
  pinned.forEach(function(row) { tbody.appendChild(row); });
  unpinned.forEach(function(row) { tbody.appendChild(row); });
}

function initPinnedMailboxes() {
  var tbody = document.getElementById('mbx-tbody');
  if (!tbody) return;

  var rows = Array.from(tbody.querySelectorAll('tr'));
  rows.forEach(function(row) {
    var alias = row.getAttribute('data-alias') || (row.cells[2] ? row.cells[2].textContent.trim() : '');
    var btn = row.querySelector('.pin-btn');
    if (pinnedMailboxes.indexOf(alias) !== -1) {
      if (btn) btn.classList.add('pinned');
      row.classList.add('pinned-row');
    }
  });

  sortPinnedToTop();
}

// ── Export CSV ────────────────────────────────────────────────────
function getVisibleRows() {
  var activeBody = (typeof inSlowestTab !== 'undefined' && inSlowestTab) ?
    document.getElementById('slowest-tbody') : document.getElementById('mbx-tbody');
  if (!activeBody) return [];
  return Array.from(activeBody.querySelectorAll('tr')).filter(function(r){
    return r.style.display !== 'none';
  });
}

function exportCSV() {
  var ths = Array.from(document.querySelectorAll('#mbx-table thead th'));
  var headers = ths.map(function(th,i){
    return hiddenCols[i] ? null : '"' + th.textContent.replace(/[▲▼]/g,'').trim().replace(/"/g,'""') + '"';
  }).filter(Boolean);

  var vrows = getVisibleRows();
  var csvRows = vrows.map(function(r){
    return Array.from(r.cells).map(function(c,i){
      return hiddenCols[i] ? null : '"' + c.textContent.trim().replace(/"/g,'""') + '"';
    }).filter(Boolean).join(',');
  });

  var csv = '\uFEFF' + [headers.join(',')].concat(csvRows).join('\r\n');
  downloadBlob(csv, 'MigrationReport_' + new Date().toISOString().slice(0,10) + '.csv', 'text/csv');
}

// ── Export Excel ──────────────────────────────────────────────────
function exportExcel() {
  var ths = Array.from(document.querySelectorAll('#mbx-table thead th'));
  var headerRow = '<tr>' + ths.map(function(th,i){
    return hiddenCols[i] ? '' : '<th>' + th.textContent.replace(/[▲▼]/g,'').trim() + '</th>';
  }).join('') + '</tr>';

  var vrows = getVisibleRows();
  var dataRows = vrows.map(function(r){
    return '<tr>' + Array.from(r.cells).map(function(c,i){
      return hiddenCols[i] ? '' : '<td>' + c.textContent.trim() + '</td>';
    }).join('') + '</tr>';
  }).join('');

  var html = '<html xmlns:o="urn:schemas-microsoft-com:office:office"' +
    ' xmlns:x="urn:schemas-microsoft-com:office:excel">' +
    '<head><meta charset="UTF-8"></head>' +
    '<body><table>' + headerRow + dataRows + '</table></body></html>';

  downloadBlob(html, 'MigrationReport_' + new Date().toISOString().slice(0,10) + '.xls',
    'application/vnd.ms-excel');
}

// ── Print ─────────────────────────────────────────────────────────
function printReport() {
  window.print();
}

// ── Download helper ───────────────────────────────────────────────
function downloadBlob(data, filename, type) {
  var blob = new Blob([data], {type: type + ';charset=utf-8;'});
  var url  = URL.createObjectURL(blob);
  var a    = document.createElement('a');
  a.href = url; a.download = filename;
  document.body.appendChild(a); a.click();
  document.body.removeChild(a);
  setTimeout(function(){ URL.revokeObjectURL(url); }, 1000);
}

// ── Save Modal as Image ───────────────────────────────────────────
function saveModalAsImage() {
  var modal = document.getElementById('mbxModalContent');
  var title = document.getElementById('mdTitle').textContent || 'MailboxDetail';
  var filename = title.replace(/[^a-zA-Z0-9]/g, '_') + '_' + new Date().toISOString().slice(0,10) + '.png';

  // Check if html2canvas is loaded
  if (typeof html2canvas === 'undefined') {
    // Load html2canvas dynamically
    var script = document.createElement('script');
    script.src = 'https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js';
    script.onload = function() {
      captureModal(modal, filename);
    };
    script.onerror = function() {
      alert('Failed to load image capture library. Please check your internet connection.');
    };
    document.head.appendChild(script);
  } else {
    captureModal(modal, filename);
  }
}

function captureModal(modal, filename) {
  // Temporarily hide the save button and close button for cleaner capture
  var buttons = modal.querySelectorAll('.ent-btn, .mbx-modal-close');
  buttons.forEach(function(btn) { btn.style.visibility = 'hidden'; });

  // Store original styles and expand modal for full capture
  var originalStyle = modal.style.cssText;
  modal.style.overflow = 'visible';
  modal.style.maxHeight = 'none';
  modal.style.height = 'auto';
  modal.style.boxShadow = '0 4px 20px rgba(0,0,0,0.15)';
  modal.style.border = '1px solid #cbd5e1';

  // Darken section backgrounds for better print/image visibility
  var sections = modal.querySelectorAll('.mbx-section');
  var sectionStyles = [];
  var colorMap = {
    'rgb(239, 246, 255)': '#dbeafe', // blue - darker
    'rgb(240, 253, 244)': '#dcfce7', // green - darker
    'rgb(254, 252, 232)': '#fef08a', // yellow - darker
    'rgb(250, 245, 255)': '#f3e8ff', // purple - darker
    'rgb(255, 247, 237)': '#fed7aa', // orange - darker
    'rgb(248, 250, 252)': '#e2e8f0', // gray - darker
    'rgb(250, 250, 250)': '#e5e5e5', // neutral - darker
    'rgb(255, 241, 242)': '#fecdd3'  // red - darker
  };
  sections.forEach(function(s) {
    sectionStyles.push(s.style.cssText);
    var bg = window.getComputedStyle(s).backgroundColor;
    if (colorMap[bg]) {
      s.style.backgroundColor = colorMap[bg];
    }
    s.style.border = '1px solid #cbd5e1';
  });

  html2canvas(modal, {
    backgroundColor: '#ffffff',
    scale: 3,
    useCORS: true,
    logging: false,
    allowTaint: true,
    imageTimeout: 0,
    windowWidth: modal.scrollWidth,
    windowHeight: modal.scrollHeight,
    width: modal.scrollWidth,
    height: modal.scrollHeight
  }).then(function(canvas) {
    // Restore buttons and original styles
    buttons.forEach(function(btn) { btn.style.visibility = ''; });
    modal.style.cssText = originalStyle;
    sections.forEach(function(s, i) { s.style.cssText = sectionStyles[i]; });

    // Download the image with high quality
    var link = document.createElement('a');
    link.download = filename;
    link.href = canvas.toDataURL('image/png', 1.0);
    link.click();
  }).catch(function(err) {
    // Restore buttons and styles on error
    buttons.forEach(function(btn) { btn.style.visibility = ''; });
    modal.style.cssText = originalStyle;
    sections.forEach(function(s, i) { s.style.cssText = sectionStyles[i]; });
    alert('Failed to capture image: ' + err.message);
  });
}

// ══════════════════════════════════════════════════════════════════
// DARK MODE
// ══════════════════════════════════════════════════════════════════
function toggleDarkMode() {
  document.body.classList.toggle('dark-mode');
  var isDark = document.body.classList.contains('dark-mode');
  localStorage.setItem('migrationReportDarkMode', isDark ? '1' : '0');
  document.querySelector('.dark-toggle').textContent = isDark ? '☀️' : '🌙';
}

function initDarkMode() {
  var saved = localStorage.getItem('migrationReportDarkMode');
  if (saved === '1') {
    document.body.classList.add('dark-mode');
    document.querySelector('.dark-toggle').textContent = '☀️';
  }
}

// ══════════════════════════════════════════════════════════════════
// KEYBOARD SHORTCUTS
// ══════════════════════════════════════════════════════════════════
var currentRowIndex = -1;
var tableRows = [];

function initKeyboardShortcuts() {
  tableRows = Array.from(document.querySelectorAll('#mbx-tbody tr'));

  document.addEventListener('keydown', function(e) {
    // Ignore if typing in input
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

    switch(e.key.toLowerCase()) {
      case 'd': // Toggle dark mode
        toggleDarkMode();
        break;
      case 's': // Toggle sound
        toggleSound();
        break;
      case 'j': // Next row
        navigateRow(1);
        break;
      case 'k': // Previous row
        navigateRow(-1);
        break;
      case 'enter': // Open selected row
        if (currentRowIndex >= 0 && tableRows[currentRowIndex]) {
          tableRows[currentRowIndex].click();
        }
        break;
      case 'escape': // Close modal
        if (typeof closeMbxModal === 'function') closeMbxModal();
        break;
      case 'r': // Refresh (if in watch mode)
        if (e.ctrlKey || e.metaKey) return; // Allow browser refresh
        if (typeof triggerRefresh === 'function') triggerRefresh();
        break;
      case 'f': // Focus search
        e.preventDefault();
        var search = document.getElementById('ent-search');
        if (search) search.focus();
        break;
      case 'p': // Export PDF
        if (e.ctrlKey || e.metaKey) return; // Allow browser print
        exportPDF();
        break;
      case '?': // Show help
        showKeyboardHelp();
        break;
    }
  });
}

function navigateRow(direction) {
  var visibleRows = tableRows.filter(function(r) { return r.style.display !== 'none'; });
  if (visibleRows.length === 0) return;

  // Remove highlight from current
  if (currentRowIndex >= 0 && tableRows[currentRowIndex]) {
    tableRows[currentRowIndex].style.outline = '';
  }

  // Find current in visible rows
  var visibleIndex = visibleRows.indexOf(tableRows[currentRowIndex]);
  visibleIndex = Math.max(-1, Math.min(visibleRows.length - 1, visibleIndex + direction));
  if (visibleIndex < 0) visibleIndex = 0;

  currentRowIndex = tableRows.indexOf(visibleRows[visibleIndex]);

  // Highlight and scroll
  if (tableRows[currentRowIndex]) {
    tableRows[currentRowIndex].style.outline = '2px solid #3b82f6';
    tableRows[currentRowIndex].scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }
}

function showKeyboardHelp() {
  var help = document.getElementById('keyboard-help');
  if (help) {
    help.classList.toggle('open');
  } else {
    var div = document.createElement('div');
    div.id = 'keyboard-help';
    div.className = 'keyboard-help open';
    div.innerHTML = '<button class="kb-close" onclick="this.parentElement.classList.remove(\'open\')">&times;</button>' +
      '<h3>⌨️ Keyboard Shortcuts</h3>' +
      '<div class="kb-row"><span class="kb-key">D</span><span class="kb-desc">Toggle dark mode</span></div>' +
      '<div class="kb-row"><span class="kb-key">S</span><span class="kb-desc">Toggle sound alerts</span></div>' +
      '<div class="kb-row"><span class="kb-key">J</span><span class="kb-desc">Next row</span></div>' +
      '<div class="kb-row"><span class="kb-key">K</span><span class="kb-desc">Previous row</span></div>' +
      '<div class="kb-row"><span class="kb-key">Enter</span><span class="kb-desc">Open mailbox detail</span></div>' +
      '<div class="kb-row"><span class="kb-key">Esc</span><span class="kb-desc">Close modal</span></div>' +
      '<div class="kb-row"><span class="kb-key">F</span><span class="kb-desc">Focus search</span></div>' +
      '<div class="kb-row"><span class="kb-key">R</span><span class="kb-desc">Refresh (watch mode)</span></div>' +
      '<div class="kb-row"><span class="kb-key">P</span><span class="kb-desc">Export PDF</span></div>' +
      '<div class="kb-row"><span class="kb-key">?</span><span class="kb-desc">Show this help</span></div>';
    document.body.appendChild(div);
  }
}

// ══════════════════════════════════════════════════════════════════
// SOUND ALERTS
// ══════════════════════════════════════════════════════════════════
var soundEnabled = localStorage.getItem('migrationReportSound') !== '0';
var lastCompletedCount = -1;
var lastFailedCount = -1;

function playSound(type) {
  if (!soundEnabled) return;
  var ctx = new (window.AudioContext || window.webkitAudioContext)();
  var osc = ctx.createOscillator();
  var gain = ctx.createGain();
  osc.connect(gain);
  gain.connect(ctx.destination);

  if (type === 'complete') {
    osc.frequency.value = 800;
    gain.gain.value = 0.1;
    osc.start();
    osc.frequency.linearRampToValueAtTime(1200, ctx.currentTime + 0.1);
    osc.stop(ctx.currentTime + 0.2);
  } else if (type === 'fail') {
    osc.frequency.value = 400;
    gain.gain.value = 0.15;
    osc.start();
    osc.frequency.linearRampToValueAtTime(200, ctx.currentTime + 0.3);
    osc.stop(ctx.currentTime + 0.4);
  }
}

function checkForAlerts() {
  var completed = document.querySelectorAll('tr[data-status="Completed"], tr[data-status="Synced"]').length;
  var failed = document.querySelectorAll('tr[data-status="Failed"]').length;

  if (lastCompletedCount >= 0 && completed > lastCompletedCount) {
    playSound('complete');
  }
  if (lastFailedCount >= 0 && failed > lastFailedCount) {
    playSound('fail');
  }

  lastCompletedCount = completed;
  lastFailedCount = failed;
}

function toggleSound() {
  soundEnabled = !soundEnabled;
  localStorage.setItem('migrationReportSound', soundEnabled ? '1' : '0');
  var btn = document.getElementById('sound-toggle');
  if (btn) {
    btn.textContent = soundEnabled ? '🔔' : '🔕';
    btn.classList.toggle('muted', !soundEnabled);
    btn.title = soundEnabled ? 'Sound Alerts ON (S)' : 'Sound Alerts OFF (S)';
  }
}

function initSoundButton() {
  var btn = document.getElementById('sound-toggle');
  if (btn) {
    btn.textContent = soundEnabled ? '🔔' : '🔕';
    btn.classList.toggle('muted', !soundEnabled);
    btn.title = soundEnabled ? 'Sound Alerts ON (S)' : 'Sound Alerts OFF (S)';
  }
}

// ══════════════════════════════════════════════════════════════════
// PDF EXPORT
// ══════════════════════════════════════════════════════════════════
function exportPDF() {
  // Use browser print with PDF-friendly styles
  var style = document.createElement('style');
  style.id = 'pdf-print-style';
  style.textContent = '@media print { ' +
    '.dark-toggle, .sound-toggle, .watch-panel, .ent-toolbar, .keyboard-help { display: none !important; } ' +
    'body { background: white !important; color: black !important; } ' +
    '.kpi, .card, .score-card { break-inside: avoid; } ' +
    '.container { max-width: 100%; padding: 10px; } ' +
    '}';
  document.head.appendChild(style);

  window.print();

  setTimeout(function() {
    var s = document.getElementById('pdf-print-style');
    if (s) s.remove();
  }, 1000);
}

// ── Init on load ──────────────────────────────────────────────────
window.addEventListener('load', function() {
  initColumns();
  initKpiFilter();
  initDarkMode();
  initSoundButton();
  initKeyboardShortcuts();
  checkForAlerts();
  updateSummaryBar(
    document.querySelectorAll('#mbx-tbody tr').length, 0
  );
});
</script>




$(if($ListenerPort -gt 0){

"<!-- Watch Mode Control Panel -->

<div class='watch-panel' id='watchPanel'>

  <div class='watch-panel-hdr' onclick='toggleWatchPanel()'>

    <span class='watch-panel-title'><span class='watch-dot' id='watchDot'></span> Live Dashboard</span>

    <span id='watchChevron' style='color:#94a3b8'>&#x25B2;</span>

  </div>

  <div class='watch-panel-body' id='watchBody'>

    <div class='watch-stat'><span class='wl'>Last refresh</span><span class='wv' id='wLastRefresh'>&#x2014;</span></div>

    <div class='watch-stat'><span class='wl'>Iteration</span><span class='wv' id='wIter'>&#x2014;</span></div>

    <div class='watch-stat'><span class='wl'>Mailboxes</span><span class='wv' id='wCount'>&#x2014;</span></div>

    <div class='watch-stat'><span class='wl'>Scope</span><span class='wv' id='wScope' style='max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap'>&#x2014;</span></div>

    <div class='watch-prog'><div class='watch-prog-fill' id='wProg' style='width:0%'></div></div>

    <div class='watch-btn-row'>

      <button class='wbtn wbtn-p' style='flex:1' onclick='apiRefresh()'>&#x21BA; Refresh Now</button>

    </div>

    <div>

      <div class='watch-sec'>Switch Batch</div>

      <select class='watch-inp' id='wBatchSel'><option value=''>All Batches</option></select>

    </div>

    <div>

      <div class='watch-sec'>Filter Mailbox</div>

      <input class='watch-inp' id='wMailboxInput' placeholder='alias, email or GUID...' type='text'>

    </div>

    <div>

      <label style='display:flex;align-items:center;gap:6px;color:#94a3b8;font-size:.78rem'>

        <input type='checkbox' id='wIncludeCompleted' style='width:14px;height:14px'> Include Completed

      </label>

    </div>

    <div class='watch-btn-row'>

      <button class='wbtn wbtn-p' style='flex:1' onclick='apiSwitch()'>Apply &amp; Refresh</button>

      <button class='wbtn wbtn-s' onclick='apiSwitchAll()'>All</button>

    </div>

  </div>

</div>"

})

<script>
// ═══════════════════════════════════════════════════════════════════
// WATCH MODE API CLIENT
// ═══════════════════════════════════════════════════════════════════
(function(){
  var API_BASE = '$($apiBaseUrl)';  // injected by PS at report generation time

  if (!API_BASE) return;


  // When served via HTTP listener, use relative URLs (same origin, no CORS/ad-blocker issues)




  // Always use relative paths — the listener serves both HTML and API
  var watchInterval = $AutoRefreshSeconds;
  var pollTimer = null;
  var countdownTimer = null;
  var nextRefreshAt = null;
  var collapsed = false;

  function apiCall(endpoint, method, body) {
    return fetch(API_BASE + endpoint, {

      method: method || 'GET',
      headers: {'Content-Type':'application/json'},
      body: body ? JSON.stringify(body) : undefined
    }).then(function(r){ return r.json(); });
  }

  window.apiRefresh = function() {
    setDot('stale');
    apiCall('/api/refresh','POST').then(function(){
      nextRefreshAt = Date.now() + watchInterval * 1000;
    }).catch(function(){ setDot('err'); });
  };

  window.apiSwitch = function() {
    var batch   = (document.getElementById('wBatchSel')||{}).value || '';
    var mailbox = (document.getElementById('wMailboxInput')||{}).value || '';
    var incComp = (document.getElementById('wIncludeCompleted')||{}).checked || false;
    setDot('stale');
    apiCall('/api/switch','POST',{
      batch: batch, mailbox: mailbox, includeCompleted: incComp
    }).then(function(){
      nextRefreshAt = Date.now() + 2000;
    }).catch(function(){ setDot('err'); });
  };

  window.apiSwitchAll = function() {
    var incComp = (document.getElementById('wIncludeCompleted')||{}).checked || false;
    setDot('stale');
    apiCall('/api/switch','POST',{batch:'', mailbox:'', includeCompleted: incComp})
      .then(function(){ nextRefreshAt = Date.now() + 2000; })
      .catch(function(){ setDot('err'); });
  };

  window.toggleWatchPanel = function() {
    collapsed = !collapsed;
    var panel = document.getElementById('watchPanel');
    var chev  = document.getElementById('watchChevron');
    if (panel) panel.classList.toggle('collapsed', collapsed);
    if (chev)  chev.textContent = collapsed ? '▼' : '▲';
  };

  function setDot(state) {
    var dot = document.getElementById('watchDot');
    if (!dot) return;
    dot.className = 'watch-dot' + (state !== 'ok' ? ' ' + state : '');
  }

  function setText(id, val) {
    var el = document.getElementById(id);
    if (el) el.textContent = val;
  }

  function setProgress(pct) {
    var el = document.getElementById('wProg');
    if (el) el.style.width = Math.min(100, Math.max(0, pct)) + '%';
  }

  function pollStatus() {
    apiCall('/api/status').then(function(data) {
      if (!data.ok) { setDot('err'); return; }
      setDot(data.isRefreshing ? 'stale' : 'ok');
      setText('wLastRefresh', data.lastRefresh || '--');
      setText('wIter',  data.iteration || '--');
      setText('wCount', data.mailboxCount || '--');
      setText('wScope', data.currentScope || 'All');

      if (!data.isRefreshing && data.nextIn > 0) {
        var pct = ((watchInterval - data.nextIn) / watchInterval) * 100;
        setProgress(pct);
        nextRefreshAt = Date.now() + data.nextIn * 1000;
      }
    }).catch(function() { setDot('err'); });
  }

  function loadBatches() {
    apiCall('/api/batches').then(function(batches) {
      var sel = document.getElementById('wBatchSel');
      if (!sel || !batches || !batches.length) return;
      sel.innerHTML = '<option value="">All Batches</option>';
      batches.forEach(function(b) {
        var opt = document.createElement('option');
        opt.value = b.Name;
        opt.textContent = b.Name + ' (' + b.Count + ')';
        sel.appendChild(opt);
      });
    }).catch(function(){});
  }

  // Smooth countdown progress between polls
  function tickCountdown() {
    if (nextRefreshAt) {
      var remaining = (nextRefreshAt - Date.now()) / 1000;
      var pct = ((watchInterval - remaining) / watchInterval) * 100;
      setProgress(pct);
    }
  }

  // Start polling
  window.addEventListener('load', function() {
    loadBatches();
    pollStatus();
    setInterval(pollStatus, 3000);         // status poll every 3s
    setInterval(tickCountdown, 500);       // smooth progress every 0.5s
  });
})();
</script>

</body>
</html>
"@

    $htmlPath = Join-Path $Path "$($Summary.BatchName)_Report.html"
    $html | Out-File -FilePath $htmlPath -Encoding UTF8 -Force
    Write-Log "HTML report saved: $htmlPath" -Level SUCCESS
    return $htmlPath
}

#endregion

#region ── Console Summary ──────────────────────────────────────────────────────

function Write-ConsoleSummary {
    param($Summary, $Health)

    $sep  = "─" * 60
    $sep2 = "═" * 60

    Write-Host "`n$sep2" -ForegroundColor Cyan
    Write-Host "  MIGRATION REPORT — $($Summary.BatchName)" -ForegroundColor Cyan
    Write-Host $sep2 -ForegroundColor Cyan

    Write-Host "`n  Overall Health : " -NoNewline
    $hColor = switch -Wildcard ($Health.Grade) {
        "A*" {"Green"} "B*" {"Green"} "C*" {"Yellow"} "D*" {"Red"} default {"Red"}
    }
    Write-Host "$($Health.Grade)  ($($Health.Score)/100)" -ForegroundColor $hColor

    Write-Host "`n$sep" -ForegroundColor DarkGray
    Write-Host "  TIMING" -ForegroundColor White
    Write-Host $sep -ForegroundColor DarkGray
    Write-Host ("  StartTime            : {0}" -f $Summary.StartTime)
    Write-Host ("  EndTime              : {0}" -f $Summary.EndTime)
    Write-Host ("  MigrationDuration    : {0}" -f $Summary.MigrationDuration)

    Write-Host "`n$sep" -ForegroundColor DarkGray
    Write-Host "  DATA TRANSFER" -ForegroundColor White
    Write-Host $sep -ForegroundColor DarkGray
    Write-Host ("  MailboxCount         : {0}" -f $Summary.MailboxCount)
    Write-Host ("  TotalGBTransferred   : {0} GB" -f $Summary.TotalGBTransferred)
    Write-Host ("  TotalThroughput      : {0} GB/h" -f $Summary.TotalThroughputGBPerHour)
    Write-Host ("  PercentComplete      : {0}% (size-weighted)" -f $Summary.PercentComplete)
    Write-Host ("  MaxTransferRate      : {0} GB/h" -f $Summary.MaxPerMoveTransferRateGBPerHour)
    Write-Host ("  MinTransferRate      : {0} GB/h" -f $Summary.MinPerMoveTransferRateGBPerHour)
    Write-Host ("  AvgTransferRate      : {0} GB/h" -f $Summary.AvgPerMoveTransferRateGBPerHour) -ForegroundColor $(
        if ($Summary.AvgPerMoveTransferRateGBPerHour -ge 0.5) {"Green"} else {"Yellow"})
    Write-Host ("  MoveEfficiency       : {0}%" -f $Summary.MoveEfficiencyPercent) -ForegroundColor $(
        if ($Summary.MoveEfficiencyPercent -ge 75) {"Green"} elseif($Summary.MoveEfficiencyPercent-ge60) {"Yellow"} else {"Red"})

    Write-Host "`n$sep" -ForegroundColor DarkGray
    Write-Host "  DURATION BREAKDOWN" -ForegroundColor White
    Write-Host $sep -ForegroundColor DarkGray
    Write-Host ("  IdleDuration         : {0}%" -f $Summary.IdleDurationPct)
    Write-Host ("  SourceSideDuration   : {0}%" -f $Summary.SourceSideDurationPct)
    Write-Host ("  DestinationSide      : {0}%" -f $Summary.DestinationSideDurationPct)
    Write-Host ("  WordBreaking         : {0}%" -f $Summary.WordBreakingDurationPct)
    Write-Host ("  TransientFailures    : {0}%" -f $Summary.TransientFailureDurationsPct)
    Write-Host ("  OverallStalls        : {0}%" -f $Summary.OverallStallDurationsPct)
    Write-Host ("  ContentIndexing      : {0}%" -f $Summary.ContentIndexingStallsPct)
    Write-Host ("  HighAvailability     : {0}%" -f $Summary.HighAvailabilityStallsPct)
    Write-Host ("  TargetCPU            : {0}%" -f $Summary.TargetCPUStallsPct)
    Write-Host ("  SourceCPU            : {0}%" -f $Summary.SourceCPUStallsPct)
    Write-Host ("  MailboxLocked        : {0}%" -f $Summary.MailboxLockedStallPct)
    Write-Host ("  ProxyUnknown         : {0}%" -f $Summary.ProxyUnknownStallPct)
    Write-Host ("  Throttle Stalls      : {0}%" -f $Summary.ThrottleStallsPct) -ForegroundColor $(
        if ($Summary.ThrottleStallsPct -gt 5) {"Yellow"} else {"White"})

    Write-Host "`n$sep" -ForegroundColor DarkGray
    Write-Host "  BOTTLENECK ANALYSIS" -ForegroundColor White
    Write-Host $sep -ForegroundColor DarkGray
    $bColor = switch ($Summary.Bottleneck.Severity) { "None"{"Green"} "Warning"{"Yellow"} default{"Red"} }
    Write-Host ("  Bottleneck           : {0}  [{1}]" -f $Summary.Bottleneck.Bottleneck, $Summary.Bottleneck.Severity) -ForegroundColor $bColor
    Write-Host ("  {0}" -f $Summary.Bottleneck.Explanation) -ForegroundColor Gray
    if ($Summary.Bottleneck.Recommendations) {
        Write-Host "`n  Recommendations:"
        $Summary.Bottleneck.Recommendations | ForEach-Object { Write-Host "   • $_" -ForegroundColor Yellow }
    }

    Write-Host "`n$sep2`n" -ForegroundColor Cyan
}

#endregion


#region ── Watch Mode HTTP Listener ─────────────────────────────────────────────

function Start-WatchListener {
    <#
    .SYNOPSIS
        Starts a local HTTP API on 127.0.0.1:PORT in a background runspace.
        Allows the browser control panel to trigger refreshes, switch batches,
        and fetch status without restarting the script.
    #>
    param(
        [int]$Port,
        [System.Collections.Hashtable]$State
    )

    # Kill any existing process using this port
    Write-Host "  Checking for processes using port $Port..." -ForegroundColor DarkGray
    try {
        # Method 1: Use Get-NetTCPConnection (more reliable on Windows)
        $connections = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue
        foreach ($conn in $connections) {
            if ($conn.OwningProcess -and $conn.OwningProcess -ne 0) {
                try {
                    $proc = Get-Process -Id $conn.OwningProcess -ErrorAction SilentlyContinue
                    if ($proc) {
                        Write-Host "  Killing process '$($proc.ProcessName)' (PID $($conn.OwningProcess)) using port $Port" -ForegroundColor Yellow
                        Stop-Process -Id $conn.OwningProcess -Force -ErrorAction SilentlyContinue
                        Start-Sleep -Milliseconds 500
                    }
                } catch {}
            }
        }
    } catch {
        # Method 2: Fallback to netstat if Get-NetTCPConnection fails
        try {
            $netstat = netstat -ano 2>$null | Select-String "[:.]$Port\s" | ForEach-Object {
                if ($_ -match '\s+(\d+)\s*$') { $Matches[1] }
            } | Where-Object { $_ -and $_ -ne '0' } | Select-Object -Unique

            foreach ($procId in $netstat) {
                try {
                    $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
                    if ($proc) {
                        Write-Host "  Killing process '$($proc.ProcessName)' (PID $procId) using port $Port" -ForegroundColor Yellow
                        Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
                        Start-Sleep -Milliseconds 500
                    }
                } catch {}
            }
        } catch {}
    }

    # Also try to delete any HTTP URL reservation that might be blocking
    try {
        $null = netsh http delete urlacl url="http://127.0.0.1:$Port/" 2>$null
        $null = netsh http delete urlacl url="http://localhost:$Port/" 2>$null
        $null = netsh http delete urlacl url="http://+:$Port/" 2>$null
    } catch {}

    # Small delay to ensure port is released
    Start-Sleep -Milliseconds 300

    $listenerScript = {
        param([int]$Port, [System.Collections.Hashtable]$State)
        try {
            $hl = $null
            $prefixesToTry = @(
                "http://127.0.0.1:$Port/",
                "http://localhost:$Port/"
            )
            $started = $false
            $lastStartError = ''
            foreach ($prefix in $prefixesToTry) {
                try {
                    if ($hl) { try { $hl.Close() } catch {} }
                    $hl = New-Object System.Net.HttpListener
                    $hl.Prefixes.Add($prefix)
                    $hl.Start()
                    $State['ListenerUrl'] = $prefix.TrimEnd('/')
                    $started = $true
                    break
                } catch {
                    $lastStartError = $_.Exception.Message
                }
            }
            if (-not $started) {
                throw "Failed to start HTTP listener on loopback. Last error: $lastStartError"
            }
            $State['ListenerReady'] = $true
            $State['ListenerError'] = ''

            while ($State['Running']) {
                $ctx = $null
                try {
                    $result = $hl.BeginGetContext($null, $null)
                    while (-not $result.AsyncWaitHandle.WaitOne(500)) {
                        if (-not $State['Running']) { break }
                    }
                    if (-not $State['Running']) { break }
                    $ctx = $hl.EndGetContext($result)
                } catch {
                    Start-Sleep -Milliseconds 100
                    continue
                }

                if ($null -eq $ctx) { continue }

                # Process request in isolated try block
                try {
                    $req  = $ctx.Request
                    $resp = $ctx.Response
                    $path = $req.Url.AbsolutePath

                    $responseBytes = $null
                    $contentType = 'text/html; charset=utf-8'

                    if ($path -eq '/' -or $path -eq '/index.html') {
                        $htmlFile = $State['ReportFile']
                        if ($htmlFile -and (Test-Path $htmlFile)) {
                            try {
                                $responseBytes = [System.IO.File]::ReadAllBytes($htmlFile)
                            } catch {
                                $fallback = "<html><body style='font-family:sans-serif;padding:40px'><h2>&#9200; Report is being written...</h2><p>Could not read report file yet.</p><meta http-equiv='refresh' content='2'></body></html>"
                                $responseBytes = [System.Text.Encoding]::UTF8.GetBytes($fallback)
                            }
                        } else {
                            $iter = [int]$State['Iteration']
                            $isRefreshing = [bool]$State['IsRefreshing']
                            if ($iter -gt 0 -and -not $isRefreshing) {
                                $scope = "$($State['CurrentScope'])"
                                $waitMsg = "<html><body style='font-family:sans-serif;padding:40px'><h2>&#9888; Report not generated</h2><p>No HTML report file was produced on the last refresh.</p><p><strong>Scope:</strong> $scope</p></body></html>"
                            } else {
                                $waitMsg = '<html><body style="font-family:sans-serif;padding:40px"><h2>&#9200; Generating report...</h2><p>The first report is being generated. This page will refresh automatically.</p><meta http-equiv="refresh" content="3"></body></html>'
                            }
                            $responseBytes = [System.Text.Encoding]::UTF8.GetBytes($waitMsg)
                        }
                    }
                    elseif ($path -eq '/api/status') {
                        $contentType = 'application/json; charset=utf-8'
                        $json = [PSCustomObject]@{
                            ok           = $true
                            lastRefresh  = if ($State['LastRefresh']) { $State['LastRefresh'].ToString('HH:mm:ss') } else { '--' }
                            iteration    = [int]$State['Iteration']
                            mailboxCount = [int]$State['MailboxCount']
                            currentScope = "$($State['CurrentScope'])"
                            isRefreshing = [bool]$State['IsRefreshing']
                            interval     = [int]$State['Interval']
                            nextIn       = [int]$State['NextIn']
                        } | ConvertTo-Json -Compress
                        $responseBytes = [System.Text.Encoding]::UTF8.GetBytes($json)
                    }
                    elseif ($path -eq '/api/batches') {
                        $contentType = 'application/json; charset=utf-8'
                        $b = $State['Batches']
                        $json = if ($b -and $b.Count -gt 0) { $b | ConvertTo-Json -Compress } else { '[]' }
                        $responseBytes = [System.Text.Encoding]::UTF8.GetBytes($json)
                    }
                    elseif ($path -eq '/api/refresh') {
                        $contentType = 'application/json; charset=utf-8'
                        $State['PendingCommand'] = @{ Action = 'refresh' }
                        $responseBytes = [System.Text.Encoding]::UTF8.GetBytes('{"ok":true,"message":"Refresh queued"}')
                    }
                    elseif ($path -eq '/api/switch') {
                        $contentType = 'application/json; charset=utf-8'
                        try {
                            $reqBody = ''
                            if ($req.HasEntityBody) {
                                $reader = New-Object System.IO.StreamReader($req.InputStream, [System.Text.Encoding]::UTF8)
                                $reqBody = $reader.ReadToEnd()
                                $reader.Close()
                            }
                            $d = $reqBody | ConvertFrom-Json
                            $State['PendingCommand'] = @{
                                Action           = 'switch'
                                Batch            = "$($d.batch)"
                                Mailbox          = "$($d.mailbox)"
                                SinceDate        = "$($d.sincedate)"
                                IncludeCompleted = [bool]$d.includeCompleted
                            }
                            $responseBytes = [System.Text.Encoding]::UTF8.GetBytes('{"ok":true,"message":"Switch queued"}')
                        } catch {
                            $responseBytes = [System.Text.Encoding]::UTF8.GetBytes('{"ok":false,"error":"Invalid request"}')
                        }
                    }
                    else {
                        $contentType = 'application/json; charset=utf-8'
                        $resp.StatusCode = 404
                        $responseBytes = [System.Text.Encoding]::UTF8.GetBytes('{"ok":false,"error":"Not found"}')
                    }

                    # Send response
                    $resp.ContentType = $contentType
                    $resp.Headers.Add('Access-Control-Allow-Origin', '*')
                    $resp.ContentLength64 = $responseBytes.Length
                    $resp.OutputStream.Write($responseBytes, 0, $responseBytes.Length)
                } catch {
                    # Ignore request processing errors
                } finally {
                    try { $resp.OutputStream.Flush() } catch {}
                    try { $resp.OutputStream.Close() } catch {}
                    try { $resp.Close() } catch {}
                }
            }
            try { $hl.Stop() } catch {}
            try { $hl.Close() } catch {}
        } catch {
            $State['ListenerError'] = $_.Exception.Message
            $State['ListenerReady'] = $false
        }
    }

    $rs = [runspacefactory]::CreateRunspace()
    $rs.ApartmentState = 'MTA'
    $rs.ThreadOptions  = 'UseNewThread'
    $rs.Open()

    $ps = [powershell]::Create()
    $ps.Runspace = $rs
    [void]$ps.AddScript($listenerScript).AddArgument($Port).AddArgument($State)
    $handle = $ps.BeginInvoke()

    return [PSCustomObject]@{ Runspace=$rs; PS=$ps; Handle=$handle }
}

#endregion

#region ── Entry Point ──────────────────────────────────────────────────────────

function Invoke-MigrationReport {
    <#
    .SYNOPSIS
        Main entry point. Orchestrates retrieval, processing, and report generation.
        Supports live EXO mode and offline XML replay mode.
    #>
    [CmdletBinding(DefaultParameterSetName = "Live")]
    param(
        # Live mode — filtering
        [Parameter(ParameterSetName = "Live")]
        [ValidateSet("All","Queued","InProgress","AutoSuspended","CompletedWithWarning","Completed","Failed")]
        [string]$StatusFilter = "All",

        [Parameter(ParameterSetName = "Live")]
        [string[]]$Mailbox,

        [Parameter(ParameterSetName = "Live")]
        [string]$MigrationBatchName,

        [Parameter(ParameterSetName = "Live")]
        [datetime]$SinceDate,

        [Parameter(ParameterSetName = "Live")]
        [switch]$IncludeCompleted,

        # Live mode — report depth
        [Parameter(ParameterSetName = "Live")]
        [switch]$IncludeDetailReport,

        [Parameter(ParameterSetName = "Live")]
        [switch]$ExportDetailXml,

        [Parameter(ParameterSetName = "Live")]
        [ValidateRange(1,1000)]
        [int]$BatchSize = 500,

        [Parameter(ParameterSetName = "Live")]
        [Parameter(ParameterSetName = "FromXml")]
        [ValidateRange(1,100)]
        [int]$Percentile = 90,

        [Parameter(ParameterSetName = "Live")]
        [Parameter(ParameterSetName = "FromXml")]
        [double]$MinSizeGBForScoring = 0.1,

        # Watch mode
        [Parameter(ParameterSetName = "Live")]
        [switch]$WatchMode,

        [Parameter(ParameterSetName = "Live")]
        [ValidateRange(10,3600)]
        [int]$RefreshIntervalSeconds = 60,

        [Parameter(ParameterSetName = "Live")]
        [ValidateRange(1024,65535)]
        [int]$ListenerPort = 8787,

        [Parameter(ParameterSetName = "Live")]
        [string]$ListenerBaseUrl = "",

        [Parameter(ParameterSetName = "Live")]
        [Parameter(ParameterSetName = "FromXml")]
        [int]$AutoRefreshSeconds = 0,

        # Offline XML replay
        [Parameter(ParameterSetName = "FromXml", Mandatory)]
        [string]$ImportXmlPath,

        # Common output
        [Parameter(ParameterSetName = "Live")]
        [Parameter(ParameterSetName = "FromXml")]
        [string]$ReportPath = (Get-Location).Path,

        [Parameter(ParameterSetName = "Live")]
        [Parameter(ParameterSetName = "FromXml")]
        [string]$ReportName = "MigrationReport_$(Get-Date -Format 'yyyyMMdd_HHmmss')",

        [Parameter(ParameterSetName = "Live")]
        [Parameter(ParameterSetName = "FromXml")]
        [switch]$SkipHtml,

        [Parameter(ParameterSetName = "Live")]
        [Parameter(ParameterSetName = "FromXml")]
        [switch]$SkipCsv
    )

    # Ensure output directory exists
    if (-not (Test-Path $ReportPath)) {
        New-Item -ItemType Directory -Path $ReportPath | Out-Null
        Write-Log "Created report directory: $ReportPath"
    }

    # ── Determine mode ───────────────────────────────────────────────────────
    $goodStats = $null
    $failedMbx = @()

    if ($PSCmdlet.ParameterSetName -eq "FromXml") {
        # ── OFFLINE MODE — load raw stats from XML ───────────────────────────
        Write-Log "Offline mode — loading stats from: $ImportXmlPath" -Level INFO
        try {
            $goodStats = @(Import-Clixml -Path $ImportXmlPath -ErrorAction Stop)
            Write-Log "Loaded $($goodStats.Count) mailbox record(s) from XML." -Level SUCCESS
        }
        catch {
            Write-Log "Failed to load XML: $_" -Level ERROR
            return
        }
    }
    else {
        # ── LIVE MODE — retrieve from EXO ────────────────────────────────────

        # Validate ExportDetailXml dependency
        if ($ExportDetailXml -and -not $IncludeDetailReport) {
            Write-Log "-ExportDetailXml requires -IncludeDetailReport. ExportDetailXml will be skipped." -Level WARN
            $ExportDetailXml = $false
        }

        # Step 1 – Retrieve move requests
        # -Mailbox and -MigrationBatchName are mutually exclusive
        if ($Mailbox -and $MigrationBatchName) {
            Write-Log "-Mailbox and -MigrationBatchName cannot be used together. Use one or the other." -Level ERROR
            return
        }

        $getMoveParams = @{
            StatusFilter    = $StatusFilter
            IncludeCompleted = $IncludeCompleted.IsPresent
        }
        if ($Mailbox)            { $getMoveParams.Mailbox            = $Mailbox }
        if ($MigrationBatchName) { $getMoveParams.MigrationBatchName = $MigrationBatchName }
        if ($SinceDate)          { $getMoveParams.SinceDate          = $SinceDate }

        $moves = Get-MoveRequests @getMoveParams
        if (-not $moves -or @($moves).Count -eq 0) {
            Write-Log "No move requests found matching the specified filters." -Level WARN
            return
        }

        # Step 2 – Retrieve statistics
        # When -Mailbox is specified use direct per-identity calls.
        # Use ExchangeGuid from the resolved move request — avoids "matches multiple entries"
        # errors when an email address resolves to both active and soft-deleted objects.
        $directIds = if ($Mailbox) {
            @($moves | ForEach-Object {
                $g = if ($_.ExchangeGuid -and "$($_.ExchangeGuid)" -ne [Guid]::Empty.ToString()) {
                    "$($_.ExchangeGuid)"
                } elseif ($_.MailboxGuid -and "$($_.MailboxGuid)" -ne [Guid]::Empty.ToString()) {
                    "$($_.MailboxGuid)"
                } elseif ($_.Alias) {
                    "$($_.Alias)"
                } else {
                    "$($_.Identity)"
                }
                $g
            } | Where-Object { $_ })
        } else { @() }
        $statsResult = Get-MoveStats -Moves $moves -BatchSize $BatchSize `
                                     -IncludeDetailReport $IncludeDetailReport.IsPresent `
                                     -DirectIdentities $directIds
        $goodStats   = $statsResult.Stats
        $failedMbx   = $statsResult.Failed

        if (-not $goodStats -or $goodStats.Count -eq 0) {
            Write-Log "No statistics could be retrieved. Check move request GUIDs and permissions." -Level ERROR
            return
        }

        # Step 2b – Export raw stats to XML if requested
        if ($ExportDetailXml) {
            $xmlPath = Join-Path $ReportPath "$($ReportName)_RawStats.xml"
            try {
                $goodStats | Export-Clixml -Path $xmlPath -Force
                Write-Log "Raw stats exported to XML: $xmlPath" -Level SUCCESS
            }
            catch {
                Write-Log "Failed to export XML: $_" -Level WARN
            }
        }
    }

    # Step 3 – Process statistics
    $summary = Invoke-ProcessStats -Stats @($goodStats) -Name $ReportName -Percentile $Percentile -MinSizeGBForScoring $MinSizeGBForScoring
    $summary | Add-Member -NotePropertyName FailedMailboxes  -NotePropertyValue $failedMbx                          -Force
    # Detect whether Report data was actually collected.
    # For FromXml mode: the XML may have been exported with -IncludeDetailReport.
    # For Live mode: Pass 2 may have been skipped (e.g. all Completed) even with the flag.
    # Best signal: TickSrcProvider or TickDstProvider > 0 means provider durations were recorded.
    # Fallback: check if Report property is a non-empty object.
    # Detect whether Report data was collected — avoid try-as-expression (PS5.1 parse issue)
    $detectedDetailReport = $false
    $detSample = $null
    if ($goodStats -and @($goodStats).Count -gt 0) {
        foreach ($gs in @($goodStats)) { if ($null -ne $gs) { $detSample = $gs; break } }
    }
    if ($null -ne $detSample) {
        if     ([int64]$detSample.TickSrcProvider -gt 0 -or [int64]$detSample.TickDstProvider -gt 0) { $detectedDetailReport = $true }
        elseif ($detSample.SourceLatencyMs -gt 0 -or $detSample.DestLatencyMs -gt 0)                 { $detectedDetailReport = $true }
        elseif ($null -ne $detSample.Report -and "$($detSample.Report)" -notin @('','{}'))            { $detectedDetailReport = $true }
        elseif ($PSCmdlet.ParameterSetName -ne 'FromXml')                                             { $detectedDetailReport = $IncludeDetailReport.IsPresent }
    } elseif ($PSCmdlet.ParameterSetName -ne 'FromXml') {
        $detectedDetailReport = $IncludeDetailReport.IsPresent
    }
    $summary | Add-Member -NotePropertyName HasDetailReport  -NotePropertyValue $detectedDetailReport -Force

    # Step 4 – Health scoring
    $health = Get-OverallHealthScore -Summary $summary

    # Step 5 – Console output
    Write-ConsoleSummary -Summary $summary -Health $health

    # Surface skipped mailboxes in console (live mode only)
    if ($failedMbx -and $failedMbx.Count -gt 0) {
        Write-Host "`n  SKIPPED MAILBOXES ($($failedMbx.Count)) - Could not retrieve statistics:" -ForegroundColor Yellow
        $failedMbx | ForEach-Object {
            Write-Host ("     * {0,-35} GUID: {1}" -f $_.DisplayName, $_.GuidUsed) -ForegroundColor Yellow
            Write-Host ("       Error: {0}" -f $_.Error) -ForegroundColor DarkYellow
        }
        $skippedCsv = Join-Path $ReportPath "$($ReportName)_SkippedMailboxes.csv"
        $failedMbx | Export-Csv -Path $skippedCsv -NoTypeInformation -Force
        Write-Log "Skipped mailboxes exported: $skippedCsv" -Level WARN
    }

    # Step 6 – Export reports
    if (-not $SkipHtml) {
        Export-HtmlReport -Summary $summary -Health $health -Path $ReportPath -AutoRefreshSeconds $AutoRefreshSeconds -ListenerPort $ListenerPort -ListenerBaseUrl $ListenerBaseUrl | Out-Null
    }
    if (-not $SkipCsv)  { Export-CsvReport  -Summary $summary -Path $ReportPath | Out-Null }

    Write-Log "All reports generated successfully." -Level SUCCESS
    Write-Log "Output directory: $ReportPath"       -Level SUCCESS

    return $summary
}
#endregion

#── Auto-run when executed directly (not dot-sourced) ────────────────────────
if ($MyInvocation.InvocationName -ne '.') {

    # Build invoke params once — reused in watch loop
    if ($PSCmdlet.ParameterSetName -eq 'FromXml') {
        # Offline replay mode
        $invokeParams = @{
            ImportXmlPath = $ImportXmlPath
            ReportPath    = $ReportPath
            ReportName    = $ReportName
        }
        if ($SkipHtml) { $invokeParams.SkipHtml = $true }
        if ($SkipCsv)  { $invokeParams.SkipCsv  = $true }
        if ($Percentile -ne 90) { $invokeParams.Percentile = $Percentile }

        if ($MinSizeGBForScoring -ne 0.1) { $invokeParams.MinSizeGBForScoring = $MinSizeGBForScoring }

    }
    else {
        # Live mode
        $invokeParams = @{
            StatusFilter  = $StatusFilter
            ReportPath    = $ReportPath
            ReportName    = $ReportName
            BatchSize     = $BatchSize
            Percentile    = $Percentile
            MinSizeGBForScoring = $MinSizeGBForScoring


        }
        if ($Mailbox)             { $invokeParams.Mailbox             = $Mailbox }
        if ($MigrationBatchName)  { $invokeParams.MigrationBatchName  = $MigrationBatchName }
        if ($SinceDate)           { $invokeParams.SinceDate           = $SinceDate }
        if ($IncludeCompleted)    { $invokeParams.IncludeCompleted    = $true }
        if ($IncludeDetailReport) { $invokeParams.IncludeDetailReport = $true }
        if ($ExportDetailXml)     { $invokeParams.ExportDetailXml     = $true }
        if ($SkipHtml)            { $invokeParams.SkipHtml            = $true }
        if ($SkipCsv)             { $invokeParams.SkipCsv             = $true }
        $invokeParams.ListenerPort = $ListenerPort

    }

    if ($WatchMode -and $PSCmdlet.ParameterSetName -ne 'FromXml') {
        # ── Watch mode — loop until Ctrl+C ───────────────────────────────────
        $invokeParams.WatchMode          = $false  # prevent recursion
        $invokeParams.AutoRefreshSeconds = $RefreshIntervalSeconds

        # Fixed report name — always overwrite same file
        $baseName  = ($invokeParams.ReportName -replace '_\d{8}_\d{6}$','')
        $watchName = "${baseName}_Watch"
        $invokeParams.ReportName = $watchName
        $reportFile = Join-Path $ReportPath "${watchName}_Report.html"




        # ── Shared state for listener <-> main loop communication ─────────────
        $watchState = [hashtable]::Synchronized(@{
            Running       = $true
            ListenerReady = $false
            ListenerError = ''
            PendingCommand= $null
            LastRefresh   = $null
            Iteration     = 0
            MailboxCount  = 0
            IsRefreshing  = $false
            CurrentScope  = if ($MigrationBatchName) { $MigrationBatchName } elseif ($Mailbox) { $Mailbox -join ',' } else { 'All' }
            Interval      = $RefreshIntervalSeconds
            NextIn        = $RefreshIntervalSeconds
            Batches       = @()

            ReportFile    = $reportFile

        })

        # ── Start HTTP listener in background runspace ────────────────────────
        $listenerJob = $null
        $apiUrl = "http://127.0.0.1:$ListenerPort"
        try {
            $listenerJob = Start-WatchListener -Port $ListenerPort -State $watchState
            # Wait up to 3s for listener to be ready
            $sw = [System.Diagnostics.Stopwatch]::StartNew()
            while (-not $watchState['ListenerReady'] -and $sw.ElapsedMilliseconds -lt 3000) {
                Start-Sleep -Milliseconds 100
            }
            if ($watchState['ListenerReady']) {
                if ($watchState['ListenerUrl']) { $apiUrl = "$($watchState['ListenerUrl'])" }
                Write-Host "  API listener ready: $apiUrl" -ForegroundColor Green
            } else {
                Write-Host "  API listener failed to start (port $ListenerPort may be in use). Watch mode will still work without browser API." -ForegroundColor Yellow
                if ($watchState['ListenerError']) { Write-Host "  Error: $($watchState['ListenerError'])" -ForegroundColor DarkYellow }
            }
        } catch {
            Write-Host "  Could not start API listener: $_" -ForegroundColor Yellow
        }

        # Pass API endpoint into HTML for the control panel JS when listener is available.
        if ($watchState['ListenerReady']) {
            $invokeParams.ListenerPort    = $ListenerPort
            $invokeParams.ListenerBaseUrl = $apiUrl
        } else {
            $invokeParams.Remove('ListenerPort')
            if ($invokeParams.ContainsKey('ListenerBaseUrl')) { [void]$invokeParams.Remove('ListenerBaseUrl') }
        }

        Write-Host ""
        Write-Host "  ████████████████████████████████████████████████████████" -ForegroundColor Cyan
        Write-Host "  WATCH MODE  —  refreshing every $RefreshIntervalSeconds seconds" -ForegroundColor Cyan
        Write-Host "  Report : $reportFile" -ForegroundColor Cyan
        Write-Host "  API    : $apiUrl" -ForegroundColor Cyan
        Write-Host "  Ctrl+C : stop" -ForegroundColor Cyan
        Write-Host "  ████████████████████████████████████████████████████████" -ForegroundColor Cyan
        Write-Host ""

        # ── Pre-fetch batch list from EXO for the control panel ───────────────
        try {
            $allMoves = Get-MoveRequest -ErrorAction Stop
            $watchState['Batches'] = @(
                $allMoves | Group-Object { "$($_.BatchName)" -replace '^MigrationService:','' } |
                    Where-Object { $_.Name } |
                    Sort-Object Name |
                    ForEach-Object { @{ Name=$_.Name; Count=$_.Count } }
            )
            Write-Host "  Loaded $($watchState['Batches'].Count) batch(es) for browser control panel." -ForegroundColor DarkCyan
        } catch {
            Write-Host "  Could not pre-load batch list: $_" -ForegroundColor DarkYellow
        }

        $iteration = 0

        try {
            while ($true) {
                $iteration++
                $watchState['Iteration']    = $iteration
                $watchState['IsRefreshing'] = $true

                Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Iteration $iteration — $($watchState['CurrentScope'])..." -ForegroundColor DarkCyan

                $result = Invoke-MigrationReport @invokeParams

                $watchState['LastRefresh']  = Get-Date
                $watchState['IsRefreshing'] = $false
                if ($result) { $watchState['MailboxCount'] = $result.MailboxCount }

                # Open in browser on first run.
                # Prefer opening the generated HTML file directly for reliable rendering.
                # Keep API listener running in background for live control panel calls.
                if ($iteration -eq 1 -and -not $SkipHtml) {

                    if ($watchState['ListenerReady']) {
                        Write-Host "  Opening report in browser: $apiUrl" -ForegroundColor Cyan
                        Start-Process $apiUrl
                    }
                    elseif (Test-Path $reportFile) {
                        Write-Host "  Listener unavailable. Opening report file directly: $reportFile" -ForegroundColor Yellow
                        Start-Process $reportFile
                    }
                    else {
                        Write-Host "  Browser auto-open skipped: listener unavailable and report file not found yet." -ForegroundColor Yellow
                    }

                }




                Write-Host "  Next refresh in $RefreshIntervalSeconds s. API: $apiUrl  |  Ctrl+C to stop`n" -ForegroundColor DarkGray

                # ── Countdown — check for pending API commands every second ──────
                for ($i = $RefreshIntervalSeconds; $i -gt 0; $i--) {
                    $watchState['NextIn'] = $i
                    Write-Progress -Activity "Watch Mode" `
                                   -Status "Next refresh in ${i}s  |  Iter $iteration  |  $($watchState['CurrentScope'])  |  API $apiUrl" `
                                   -PercentComplete ([math]::Round((($RefreshIntervalSeconds - $i) / $RefreshIntervalSeconds) * 100))
                    Start-Sleep -Seconds 1

                    # Check for command from browser API
                    if ($watchState['PendingCommand']) {
                        $cmd = $watchState['PendingCommand']
                        $watchState['PendingCommand'] = $null

                        Write-Host "  [API] Command received: $($cmd.Action)" -ForegroundColor Magenta

                        if ($cmd.Action -eq 'switch') {
                            # Update invoke params based on what was requested
                            if ($cmd.Batch -and $cmd.Batch -ne '') {
                                $invokeParams.Remove('Mailbox')
                                $invokeParams.MigrationBatchName = $cmd.Batch
                                $watchState['CurrentScope'] = "Batch: $($cmd.Batch)"
                            } elseif ($cmd.Mailbox -and $cmd.Mailbox -ne '') {
                                $invokeParams.Remove('MigrationBatchName')
                                $invokeParams.Mailbox = @($cmd.Mailbox -split ',')
                                $watchState['CurrentScope'] = "Mailbox: $($cmd.Mailbox)"
                            } else {
                                # All — clear filters
                                $invokeParams.Remove('Mailbox')
                                $invokeParams.Remove('MigrationBatchName')
                                $watchState['CurrentScope'] = 'All'
                            }
                            if ($cmd.IncludeCompleted) { $invokeParams.IncludeCompleted = $true }
                            if ($cmd.SinceDate -and $cmd.SinceDate -ne '') {
                                try { $invokeParams.SinceDate = [datetime]$cmd.SinceDate } catch {}
                            }
                        }
                        # Break countdown to refresh immediately
                        break
                    }
                }
                Write-Progress -Activity "Watch Mode" -Completed
            }
        }
        finally {
            $watchState['Running'] = $false
            Write-Progress -Activity "Watch Mode" -Completed
            if ($listenerJob) {
                try { $listenerJob.PS.Stop() } catch {}
                try { $listenerJob.Runspace.Close() } catch {}
            }
            Write-Host "`n[$(Get-Date -Format 'HH:mm:ss')] Watch mode stopped." -ForegroundColor Yellow
        }
    }
    else {
        Invoke-MigrationReport @invokeParams
    }
}
