pkgs: attrs: with pkgs; with attrs; rec {
  version = "16.0.6";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-fspqSReX+VD+Nl/Cfq+tDcdPtnQPV1IRopNDfd5VtUs=";
  };

  patches = [
    # https://reviews.llvm.org/D132298, Allow building libcxxabi alone
    (fetchpatch {
      url = "https://github.com/llvm/llvm-project/commit/e6a0800532bb409f6d1c62f3698bdd6994a877dc.patch";
      sha256 = "1xyjd56m4pfwq8p3xh6i8lhkk9kq15jaml7qbhxdf87z4jjkk63a";
      stripLen = 1;
    })
  ];

  NIX_CFLAGS_COMPILE = [
    # See DTCC-589 for the details
    "-fno-integrated-cc1"
  ];

  sourceRoot = "source/libcxxabi";
}
