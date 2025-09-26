self: super: with self; rec {
  version = "20250814.1";

  src = fetchFromGitHub {
    owner = "abseil";
    repo = "abseil-cpp";
    rev = version;
    hash = "sha256-SCQDORhmJmTb0CYm15zjEa7dkwc+lpW2s1d4DsMRovI=";
  };

  patches = [];
}
