self: super: with self; rec {
  name = "fast_float";
  version = "8.2.9";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-2abtYCIvvVxA5drzxq1pkNJUIdprLNczEDlnQX4mZh0=";
  };
}
