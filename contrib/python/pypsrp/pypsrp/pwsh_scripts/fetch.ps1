# Copyright: (c) 2019, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

# Need to output as a base64 string as PS Runspaces will create individual byte objects for each byte in a byte
# array which has way more overhead than a single base64 string. I also wanted to output in chunks and have the
# local side process the output in parallel for large files but it seems like the base64 stream is getting sent
# in one chunk when in a loop so scratch that idea.

$ErrorActionPreference = 'Stop'
$raw_src_path = $MyInvocation.UnboundArguments[0]
$expand_variables = $MyInvocation.UnboundArguments[1]
if ($expand_variables -eq $true) {
    $raw_src_path = [System.Environment]::ExpandEnvironmentVariables($raw_src_path)
}
$src_path = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($raw_src_path)

if (Test-Path -LiteralPath $src_path -PathType Container) {
    throw "The path at '$src_path' is a directory, src must be a file"
} elseif (-not (Test-Path -LiteralPath $src_path)) {
    throw "The path at '$src_path' does not exist"
}

$algo = [System.Security.Cryptography.SHA1CryptoServiceProvider]::Create()
$src = New-Object -TypeName System.IO.FileInfo -ArgumentList $src_path
$offset = 0
$fs = $src.OpenRead()
$bytes_to_read = $fs.Length
try {
    while ($bytes_to_read -ne 0) {
        $bytes = New-Object -TypeName byte[] -ArgumentList $bytes_to_read
        $read = $fs.Read($bytes, $offset, $bytes_to_read)

        Write-Output -InputObject ([System.Convert]::ToBase64String($bytes))
        $bytes_to_read -= $read
        $offset += $read

        $algo.TransformBlock($bytes, 0, $bytes.Length, $bytes, 0) > $null
    }
} finally {
    $fs.Dispose()
}

# Output the actual hash of the file so the client can validate the fetch data isn't mangled.
$algo.TransformFinalBlock($bytes, 0, 0) > $Null
$hash = [System.BitConverter]::ToString($algo.Hash)
$hash.Replace("-", "").ToLowerInvariant()