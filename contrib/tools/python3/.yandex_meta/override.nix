pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.13.12";

  src = fetchFromGitHub {
    owner = "python";
    repo = "cpython";
    rev = "v${version}";
    hash = "sha256-m1j3U3QVsISgENCkxoNZPbTJWW8IO86/s5KRWNoY2DA=";
  };

  patches = [];
  postPatch = "";
}
