self: super: with self; rec {
  name = "fast_float";
  version = "8.1.0";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-kTLmk+mxfdN/+vKdyohcaNSeoAkhJf3uesaBOz123ag=";
  };
}
