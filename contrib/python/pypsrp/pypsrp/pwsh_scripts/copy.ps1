# Copyright: (c) 2019, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

begin {
    $ErrorActionPreference = "Stop"
    $WarningPreference = "Continue"
    $path = [System.IO.Path]::GetTempFileName()
    $fd = [System.IO.File]::Create($path)
    $algo = [System.Security.Cryptography.SHA1CryptoServiceProvider]::Create()
    $bytes = $null
    $expected_hash = ""

    $binding_flags = [System.Reflection.BindingFlags]'NonPublic, Instance'
    Function Get-Property {
        <#
        .SYNOPSIS
        Gets the private/internal property specified of the object passed in.
        #>
        Param (
            [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
            [System.Object]
            $Object,

            [Parameter(Mandatory=$true, Position=1)]
            [System.String]
            $Name
        )

        $Object.GetType().GetProperty($Name, $binding_flags).GetValue($Object, $null)
    }

    Function Set-Property {
        <#
        .SYNOPSIS
        Sets the private/internal property specified on the object passed in.
        #>
        Param (
            [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
            [System.Object]
            $Object,

            [Parameter(Mandatory=$true, Position=1)]
            [System.String]
            $Name,

            [Parameter(Mandatory=$true, Position=2)]
            [AllowNull()]
            [System.Object]
            $Value
        )

        $Object.GetType().GetProperty($Name, $binding_flags).SetValue($Object, $Value, $null)
    }

    Function Get-Field {
        <#
        .SYNOPSIS
        Gets the private/internal field specified of the object passed in.
        #>
        Param (
            [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
            [System.Object]
            $Object,

            [Parameter(Mandatory=$true, Position=1)]
            [System.String]
            $Name
        )

        $Object.GetType().GetField($Name, $binding_flags).GetValue($Object)
    }

    # MaximumAllowedMemory is required to be set to so we can send input data that exceeds the limit on a PS
    # Runspace. We use reflection to access/set this property as it is not accessible publicly. This is not ideal
    # but works on all PowerShell versions I've tested with. We originally used WinRS to send the raw bytes to the
    # host but this falls flat if someone is using a custom PS configuration name so this is a workaround. This
    # isn't required for smaller files so if it fails we just want to return the warning back to the user.
    # https://github.com/PowerShell/PowerShell/blob/c8e72d1e664b1ee04a14f226adf655cced24e5f0/src/System.Management.Automation/engine/serialization.cs#L325
    try {
        $Host | Get-Property 'ExternalHost' | `
            Get-Field '_transportManager' | `
            Get-Property 'Fragmentor' | `
            Get-Property 'DeserializationContext' | `
            Set-Property 'MaximumAllowedMemory' $null
    } catch {
        $version_info = $PSVersionTable | Out-String
        $msg = "Failed to disable MaximumAllowedMemory input size: $($_.Exception.Message)`r`n"
        $msg += "Server PS Info:`r`n$version_info"
        Write-Warning -Message $msg
    }
} process {
    # On the first input $bytes will be $null so this isn't run. This shifts each input to the next run until
    # the final input is reach (checksum of the file) which is processed in enc.
    if ($null -ne $bytes) {
        $algo.TransformBlock($bytes, 0, $bytes.Length, $bytes, 0) > $null
        $fd.Write($bytes, 0, $bytes.Length)
    }
    $bytes = [System.Convert]::FromBase64String($input)
} end {
    $fd.Close()

    try {
        # Makes sure relative paths are resolved to an absolute path based on the current location.
        # Cannot rely on $args[0] as this isn't set in PowerShell v2, MyInvocation works just fine though.
        $raw_out_path = $MyInvocation.UnboundArguments[0]
        $expand_variables = $MyInvocation.UnboundArguments[1]
        if ($expand_variables -eq $true) {
            $raw_out_path = [System.Environment]::ExpandEnvironmentVariables($raw_out_path)
        }
        $output_path = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($raw_out_path)
        $dest = New-Object -TypeName System.IO.FileInfo -ArgumentList $output_path

        $expected_hash = [System.Text.Encoding]::UTF8.GetString($bytes)
        $algo.TransformFinalBlock($bytes, 0, 0) > $null
        $actual_hash = [System.BitConverter]::ToString($algo.Hash)
        $actual_hash = $actual_hash.Replace("-", "").ToLowerInvariant()

        if ($actual_hash -ne $expected_hash) {
            throw "Transport failure, hash mismatch`r`nActual: $actual_hash`r`nExpected: $expected_hash"
        }

        # Move the temp file to the actual dest location and return the absolute path back to the client.
        # Note that we always attempt a delete first since the move operation can fail if the target is
        # located on a different volume and already exists.
        [System.IO.File]::Delete($output_path)
        [System.IO.File]::Move($path, $output_path)
        $dest.FullName
    } finally {
        # Note: If the file to be deleted does not exist, no exception is thrown.
        [System.IO.File]::Delete($path)
    }
}
