self: super: with self; rec {
  name = "fast_float";
  version = "8.0.0";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-shP+me3iqTRrsPGYrvcbnJNRZouQbW62T24xfkEgGSE=";
  };
}
