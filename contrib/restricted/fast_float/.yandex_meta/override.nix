self: super: with self; rec {
  name = "fast_float";
  version = "8.3.0";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-Hfnoet/FMCqm5Qx7A6/2NnSlmWgTFJrOqHvXh1TDd6U=";
  };
}
