self: super: with self; rec {
  name = "fast_float";
  version = "8.2.6";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-hzoB+Mmok3oe6B494uLc5ReWpUcB89zCGPYw4gvanK0=";
  };
}
