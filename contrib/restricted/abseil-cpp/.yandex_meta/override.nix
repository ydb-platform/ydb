self: super: with self; rec {
  version = "20250127.0";

  src = fetchFromGitHub {
    owner = "abseil";
    repo = "abseil-cpp";
    rev = version;
    hash = "sha256-Tt4F0VT7koEARWNjL/L2E8jrPZbSsPb/Y32Kn86sb+k=";
  };

  patches = [];
}
