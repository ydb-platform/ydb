pkgs: attrs: with pkgs; with attrs; rec {
  version = "5.46";

  src = fetchFromGitHub {
    owner = "file";
    repo = "file";
    rev = "FILE${lib.replaceStrings ["."] ["_"] version}";
    hash = "sha256-Ur+1QV6CrPpjYF+8sK7RFSqQ7OF1aS/VNrOtLodp1x4=";
  };

  patches = [];

  nativeBuildInputs = [ autoreconfHook ];

  configureFlags = [
    "--disable-dependency-tracking"
    "--build=x86_64-unknown-linux-gnu"
  ];
}
