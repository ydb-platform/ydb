self: super: with self; rec {
  name = "fast_float";
  version = "8.0.2";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-lKEzRYKdpjsqixC9WBoILccqB2ZkUtPUzT4Q4+j0oac=";
  };
}
