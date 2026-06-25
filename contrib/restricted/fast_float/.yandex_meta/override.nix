self: super: with self; rec {
  name = "fast_float";
  version = "8.2.7";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-Ks3ChZ54PnVb1NOyXRMXB9GUUQs4zAN6HL2aRFrYGbY=";
  };
}
